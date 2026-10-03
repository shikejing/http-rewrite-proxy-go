// Package upstream 实现上游实例池：负载选择、健康检查摘除/恢复、并发限制。
package upstream

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shikejing/http-rewrite-proxy-go/config"
	"github.com/shikejing/http-rewrite-proxy-go/limiter"
)

// Pool 是一个上游集群。
type Pool struct {
	name      string
	client    *http.Client
	transport *http.Transport

	limiterMu sync.Mutex
	limiter   *limiter.Limiter

	maxConns     int
	queueTimeout time.Duration
	healthCfg    config.HealthConfig

	state  atomic.Pointer[poolState]
	rr     atomic.Uint64
	stopCh chan struct{}
	stopOnce sync.Once
}

// poolState 是不可变的实例视图，更新时整体替换。
type poolState struct {
	instances []*Instance
}

// Instance 是一个上游实例。
type Instance struct {
	rawURL   string
	URL      *url.URL
	healthy  atomic.Bool
	inflight atomic.Int64

	fails  atomic.Int32
	passes atomic.Int32
}

// InstanceStatus 是实例的可观测状态快照。
type InstanceStatus struct {
	URL      string `json:"url"`
	Healthy  bool   `json:"healthy"`
	InFlight int64  `json:"in_flight"`
}

// NewPool 创建池（不启动健康检查）。
func NewPool(name string, urls []string, maxConns, queueTimeoutMS int, settings config.Settings, healthCfg config.HealthConfig) (*Pool, error) {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          settings.MaxIdleConns,
		MaxIdleConnsPerHost:   settings.MaxIdleConnsPerHost,
		IdleConnTimeout:       time.Duration(settings.IdleConnTimeoutMS) * time.Millisecond,
		ForceAttemptHTTP2:     true,
	}
	p := &Pool{
		name:         name,
		client:       &http.Client{Transport: transport},
		transport:    transport,
		maxConns:     maxConns,
		queueTimeout: time.Duration(queueTimeoutMS) * time.Millisecond,
		healthCfg:    healthCfg,
		stopCh:       make(chan struct{}),
	}
	p.limiter = limiter.New(maxConns, p.queueTimeout)
	state := &poolState{}
	for _, raw := range urls {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, err
		}
		inst := &Instance{rawURL: raw, URL: u}
		inst.healthy.Store(true)
		state.instances = append(state.instances, inst)
	}
	p.state.Store(state)
	return p, nil
}

// Name 返回池名。
func (p *Pool) Name() string { return p.name }

// Client 返回共享的 HTTP 客户端。
func (p *Pool) Client() *http.Client { return p.client }

// Limiter 返回并发限流器。
func (p *Pool) Limiter() *limiter.Limiter {
	p.limiterMu.Lock()
	defer p.limiterMu.Unlock()
	return p.limiter
}

// Select 用轮询方式选择一个健康实例；全部不健康返回 nil。
// 轮询计数器在所有实例间递增并对健康实例取模，保证健康实例不会被持续绕过。
func (p *Pool) Select() *Instance {
	state := p.state.Load()
	n := len(state.instances)
	if n == 0 {
		return nil
	}
	start := int(p.rr.Add(1) - 1)
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if state.instances[idx].healthy.Load() {
			return state.instances[idx]
		}
	}
	return nil
}

// MarkResult 根据转发结果反馈健康状态（连续失败/成功达到阈值才翻转，避免抖动）。
func (p *Pool) MarkResult(inst *Instance, success bool) {
	if success {
		inst.fails.Store(0)
		passes := inst.passes.Add(1)
		if !inst.healthy.Load() && int(passes) >= p.healthyThreshold() {
			inst.healthy.Store(true)
			log.Printf("upstream[%s] %s 恢复健康", p.name, inst.rawURL)
		}
		return
	}
	inst.passes.Store(0)
	fails := inst.fails.Add(1)
	if inst.healthy.Load() && int(fails) >= p.unhealthyThreshold() {
		inst.healthy.Store(false)
		log.Printf("upstream[%s] %s 被摘除（连续失败 %d 次）", p.name, inst.rawURL, fails)
	}
}

func (p *Pool) healthyThreshold() int {
	if p.healthCfg.HealthyThreshold > 0 {
		return p.healthCfg.HealthyThreshold
	}
	return 1
}

func (p *Pool) unhealthyThreshold() int {
	if p.healthCfg.UnhealthyThreshold > 0 {
		return p.healthCfg.UnhealthyThreshold
	}
	return 1
}

// Update 用新 URL 列表更新实例视图：保留已有实例的健康状态，新增实例默认健康，移除消失实例。
// 并发上限配置变化时重建限流器。
func (p *Pool) Update(urls []string, maxConns, queueTimeoutMS int) error {
	state := p.state.Load()
	byURL := make(map[string]*Instance, len(state.instances))
	for _, inst := range state.instances {
		byURL[inst.rawURL] = inst
	}
	fresh := make([]*Instance, 0, len(urls))
	for _, raw := range urls {
		if inst, ok := byURL[raw]; ok {
			fresh = append(fresh, inst)
			continue
		}
		u, err := url.Parse(raw)
		if err != nil {
			return err
		}
		inst := &Instance{rawURL: raw, URL: u}
		inst.healthy.Store(true)
		fresh = append(fresh, inst)
	}
	p.state.Store(&poolState{instances: fresh})

	qt := time.Duration(queueTimeoutMS) * time.Millisecond
	if maxConns != p.maxConns || qt != p.queueTimeout {
		p.limiterMu.Lock()
		p.limiter = limiter.New(maxConns, qt)
		p.maxConns = maxConns
		p.queueTimeout = qt
		p.limiterMu.Unlock()
	}
	return nil
}

// Status 返回池状态快照。
func (p *Pool) Status() []InstanceStatus {
	state := p.state.Load()
	out := make([]InstanceStatus, 0, len(state.instances))
	for _, inst := range state.instances {
		out = append(out, InstanceStatus{
			URL:      inst.rawURL,
			Healthy:  inst.healthy.Load(),
			InFlight: inst.inflight.Load(),
		})
	}
	return out
}

// Stop 停止后台任务并关闭空闲连接。
func (p *Pool) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
	p.transport.CloseIdleConnections()
}

var _ = context.Background

// AddInflight 调整在途请求计数。
func (inst *Instance) AddInflight(delta int64) {
	inst.inflight.Add(delta)
}
