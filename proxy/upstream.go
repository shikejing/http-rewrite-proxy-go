package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNoHealthyUpstream 表示集群内没有健康实例。
var ErrNoHealthyUpstream = errors.New("no healthy upstream endpoint")

// endpoint 是上游集群中的一个实例。
type endpoint struct {
	addr string

	mu       sync.RWMutex
	healthy  bool  // 当前是否可用
	failSeq  int   // 连续失败计数
	okSeq    int   // 连续成功计数
	inflight int64 // 在途请求数（观测用）
}

func (e *endpoint) isHealthy() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.healthy
}

// record 根据一次探活结果更新状态机：
// 连续失败达到 FailThreshold 摘除；连续成功达到 RecoverThreshold 恢复。
func (e *endpoint) record(ok bool, hc HealthCheckConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ok {
		e.okSeq++
		e.failSeq = 0
		if !e.healthy && e.okSeq >= hc.RecoverThreshold {
			e.healthy = true
		}
	} else {
		e.failSeq++
		e.okSeq = 0
		if e.healthy && e.failSeq >= hc.FailThreshold {
			e.healthy = false
		}
	}
}

// Pool 是一个上游集群，负责负载选择与健康状态维护。
type Pool struct {
	name      string
	endpoints []*endpoint
	rr        atomic.Uint64
	hc        HealthCheckConfig
	client    *http.Client
}

// NewPool 创建上游集群，初始时所有实例视为健康。
func NewPool(cfg UpstreamConfig, hc HealthCheckConfig) *Pool {
	p := &Pool{name: cfg.Name, hc: hc}
	for _, addr := range cfg.Endpoints {
		p.endpoints = append(p.endpoints, &endpoint{addr: addr, healthy: true})
	}
	return p
}

// Next 在健康实例间轮询选择，保证健康实例不会被持续绕过；
// 没有健康实例时返回 ErrNoHealthyUpstream。
func (p *Pool) Next() (string, error) {
	n := len(p.endpoints)
	if n == 0 {
		return "", ErrNoHealthyUpstream
	}
	start := p.rr.Add(1) - 1
	for i := 0; i < n; i++ {
		e := p.endpoints[(int(start)+i)%n]
		if e.isHealthy() {
			atomic.AddInt64(&e.inflight, 1)
			return e.addr, nil
		}
	}
	return "", ErrNoHealthyUpstream
}

// Done 释放一次 Next 占用的在途计数。
func (p *Pool) Done(addr string) {
	for _, e := range p.endpoints {
		if e.addr == addr {
			atomic.AddInt64(&e.inflight, -1)
			return
		}
	}
}

// RunHealthChecks 启动后台探活循环，ctx 取消时退出（不泄漏后台任务）。
func (p *Pool) RunHealthChecks(ctx context.Context, client *http.Client) {
	t := time.NewTicker(p.hc.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, e := range p.endpoints {
				p.probe(ctx, client, e)
			}
		}
	}
}

func (p *Pool) probe(ctx context.Context, client *http.Client, e *endpoint) {
	cctx, cancel := context.WithTimeout(ctx, p.hc.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, e.addr+"/", nil)
	if err != nil {
		e.record(false, p.hc)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		e.record(false, p.hc)
		return
	}
	resp.Body.Close()
	// 能建立连接且返回非 5xx 视为存活。
	e.record(resp.StatusCode < 500, p.hc)
}

// HealthyCount 返回当前健康实例数（用于测试与观测）。
func (p *Pool) HealthyCount() int {
	n := 0
	for _, e := range p.endpoints {
		if e.isHealthy() {
			n++
		}
	}
	return n
}

// Name 返回集群名。
func (p *Pool) Name() string { return p.name }

func (p *Pool) String() string {
	return fmt.Sprintf("pool(%s, healthy=%d/%d)", p.name, p.HealthyCount(), len(p.endpoints))
}
