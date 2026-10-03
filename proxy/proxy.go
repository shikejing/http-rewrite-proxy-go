package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// FailReason 是可区分的失败原因，通过 X-Proxy-Fail-Reason 响应头暴露。
type FailReason string

const (
	ReasonNoRule           FailReason = "no-matching-rule"
	ReasonNoHealthy        FailReason = "no-healthy-upstream"
	ReasonOverload         FailReason = "overload"
	ReasonSignedDenied     FailReason = "signed-rewrite-denied"
	ReasonUpstreamTimeout  FailReason = "upstream-timeout"
	ReasonUpstreamError    FailReason = "upstream-error"
	ReasonRetriesExhausted FailReason = "retries-exhausted"
)

// Proxy 是转发层主对象，持有原子配置视图。
type Proxy struct {
	view      atomic.Pointer[runtimeView]
	transport *http.Transport
}

// runtimeView 是某一时刻自洽的运行时视图：规则、上游、并发原语同属一份快照。
type runtimeView struct {
	cfg    *Config
	rules  *RuleSet
	pools  map[string]*Pool
	sem    *semaphore
	cancel context.CancelFunc // 停止本视图的后台健康检查
}

// semaphore 是有排队上限的计数信号量。
type semaphore struct {
	ch      chan struct{}
	maxWait int
	waiting atomic.Int64
}

func newSemaphore(capacity int64, maxWait int) *semaphore {
	if capacity <= 0 {
		return nil // 不限流
	}
	return &semaphore{ch: make(chan struct{}, capacity), maxWait: maxWait}
}

// acquire 尝试获取许可；排队数超过上限时立即返回 false。
func (s *semaphore) acquire(ctx context.Context) bool {
	if s == nil {
		return true
	}
	select {
	case s.ch <- struct{}{}:
		return true
	default:
	}
	if s.maxWait <= 0 || s.waiting.Add(1) > int64(s.maxWait) {
		s.waiting.Add(-1)
		return false
	}
	defer s.waiting.Add(-1)
	select {
	case s.ch <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *semaphore) release() {
	if s == nil {
		return
	}
	<-s.ch
}

// NewProxy 以初始配置创建 Proxy。
func NewProxy(cfg *Config) (*Proxy, error) {
	c := *cfg
	c.setDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	p := &Proxy{
		transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	v, err := p.buildView(&c)
	if err != nil {
		return nil, err
	}
	p.view.Store(v)
	return p, nil
}

func (p *Proxy) buildView(cfg *Config) (*runtimeView, error) {
	rules, err := NewRuleSet(cfg.Rules)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	v := &runtimeView{
		cfg:    cfg,
		rules:  rules,
		pools:  map[string]*Pool{},
		sem:    newSemaphore(cfg.Limits.MaxConcurrent, cfg.Limits.QueueLimit),
		cancel: cancel,
	}
	hcClient := &http.Client{Timeout: cfg.HealthCheck.Timeout}
	for _, uc := range cfg.Upstreams {
		pool := NewPool(uc, cfg.HealthCheck)
		v.pools[uc.Name] = pool
		go pool.RunHealthChecks(ctx, hcClient)
	}
	return v, nil
}

// UpdateConfig 热更新配置：先完整构建新视图，校验或构建失败时保留旧视图；
// 成功后原子切换，在途请求继续使用旧视图快照，不受中断。
func (p *Proxy) UpdateConfig(cfg *Config) error {
	c := *cfg
	c.setDefaults()
	if err := c.Validate(); err != nil {
		return err
	}
	v, err := p.buildView(&c)
	if err != nil {
		v.cancel()
		return err
	}
	old := p.view.Swap(v)
	if old != nil {
		old.cancel() // 停止旧视图的后台健康检查，不泄漏任务
	}
	return nil
}

// Close 释放后台资源。
func (p *Proxy) Close() {
	if v := p.view.Load(); v != nil {
		v.cancel()
	}
	p.transport.CloseIdleConnections()
}

// ServeHTTP 实现 http.Handler。
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	v := p.view.Load()
	if v == nil {
		http.Error(w, "proxy not ready", http.StatusServiceUnavailable)
		return
	}
	if !v.sem.acquire(r.Context()) {
		writeFail(w, ReasonOverload, http.StatusServiceUnavailable, "too many in-flight requests")
		return
	}
	defer v.sem.release()

	rule := v.rules.Match(r.URL.Path)
	if rule == nil {
		writeFail(w, ReasonNoRule, http.StatusNotFound, "no matching rule")
		return
	}
	pool := v.pools[rule.Upstream()]
	if pool == nil {
		writeFail(w, ReasonNoHealthy, http.StatusServiceUnavailable, "upstream pool missing")
		return
	}

	if err := applyAuth(r, v.cfg.Auth, rule.hasRewrite()); err != nil {
		writeFail(w, ReasonSignedDenied, http.StatusForbidden, err.Error())
		return
	}

	if isUpgrade(r) {
		p.serveUpgrade(w, r, pool, rule)
		return
	}
	p.serveHTTP(w, r, v, pool, rule)
}

// serveHTTP 处理普通 HTTP 请求：重试 + 超时 + 流式响应转发。
func (p *Proxy) serveHTTP(w http.ResponseWriter, r *http.Request, v *runtimeView, pool *Pool, rule *Rule) {
	canRetry := replayable(r)
	first := true

	status, err := doWithRetry(r.Context(), v.cfg.Retry, v.cfg.Timeouts.Overall, canRetry,
		func(ctx context.Context) (int, error) {
			addr, err := pool.Next()
			if err != nil {
				return 0, err
			}
			defer pool.Done(addr)

			actx, cancel := context.WithTimeout(ctx, v.cfg.Timeouts.PerAttempt)
			defer cancel()

			out, err := p.buildOutbound(r, addr, actx, rule, first)
			if err != nil {
				return 0, err
			}
			first = false

			resp, err := p.transport.RoundTrip(out)
			if err != nil {
				return 0, err
			}
			// 5xx 且可重试：丢弃本次响应，交给重试循环
			if resp.StatusCode >= 500 && canRetry {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				return resp.StatusCode, nil
			}
			// 写回响应头（应用响应头改写）并流式转发 body
			copyHeaders(w.Header(), resp.Header)
			rule.ApplyResponseHeaders(w.Header())
			w.Header().Set("X-Proxy-Rule", rule.Name())
			w.WriteHeader(resp.StatusCode)
			defer resp.Body.Close()
			streamCopy(w, resp.Body) // 客户端断开时写失败，defer 关闭上游 body
			return resp.StatusCode, nil
		})

	if err != nil {
		reason := ReasonUpstreamError
		code := http.StatusBadGateway
		switch {
		case errors.Is(err, ErrNoHealthyUpstream):
			reason, code = ReasonNoHealthy, http.StatusServiceUnavailable
		case errors.Is(err, ErrRetriesExhausted):
			reason, code = ReasonRetriesExhausted, http.StatusBadGateway
		case errors.Is(err, ErrOverallTimeout):
			reason, code = ReasonUpstreamTimeout, http.StatusGatewayTimeout
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded) && r.Context().Err() != nil:
			return // 客户端已取消，无需再写响应
		case status >= 500:
			reason, code = ReasonRetriesExhausted, http.StatusBadGateway
		}
		writeFail(w, reason, code, err.Error())
	}
}

// buildOutbound 构造发往上游的请求副本，并应用规则改写。
func (p *Proxy) buildOutbound(r *http.Request, addr string, ctx context.Context, rule *Rule, useOriginalBody bool) (*http.Request, error) {
	out := r.Clone(ctx)
	if strings.HasPrefix(addr, "https://") {
		out.URL.Scheme = "https"
		out.URL.Host = strings.TrimPrefix(addr, "https://")
	} else {
		out.URL.Scheme = "http"
		out.URL.Host = strings.TrimPrefix(addr, "http://")
	}
	out.RequestURI = ""
	if !useOriginalBody && r.GetBody != nil {
		b, err := r.GetBody()
		if err != nil {
			return nil, err
		}
		out.Body = b
	}
	rule.ApplyRequest(out)
	return out, nil
}

// serveUpgrade 透传长连接/双向流式协议（如 WebSocket）。
func (p *Proxy) serveUpgrade(w http.ResponseWriter, r *http.Request, pool *Pool, rule *Rule) {
	addr, err := pool.Next()
	if err != nil {
		writeFail(w, ReasonNoHealthy, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer pool.Done(addr)

	host := strings.TrimPrefix(strings.TrimPrefix(addr, "https://"), "http://")
	upConn, err := net.DialTimeout("tcp", host, 10*time.Second)
	if err != nil {
		writeFail(w, ReasonUpstreamError, http.StatusBadGateway, err.Error())
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		upConn.Close()
		writeFail(w, ReasonUpstreamError, http.StatusInternalServerError, "hijack unsupported")
		return
	}
	clientConn, clientRW, err := hj.Hijack()
	if err != nil {
		upConn.Close()
		return
	}

	// 构造出站请求（含路径/查询/请求头改写）并原样写入上游连接
	out := r.Clone(r.Context())
	out.RequestURI = ""
	if strings.HasPrefix(addr, "https://") {
		out.URL.Scheme = "https"
	} else {
		out.URL.Scheme = "http"
	}
	out.URL.Host = host
	rule.ApplyRequest(out)
	if err := out.Write(upConn); err != nil {
		upConn.Close()
		clientConn.Close()
		return
	}

	// 双向流式拷贝：任一方向结束即关闭两侧连接，两个拷贝 goroutine 随之退出，
	// 客户端中断或上游提前关闭都不会泄漏连接与后台任务。
	var once sync.Once
	closeBoth := func() {
		once.Do(func() {
			upConn.Close()
			clientConn.Close()
		})
	}
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(upConn, clientRW) // clientRW 含 hijack 时缓冲的未读数据
		closeBoth()
		done <- struct{}{}
	}()
	go func() {
		io.Copy(clientConn, upConn)
		closeBoth()
		done <- struct{}{}
	}()
	<-done
	<-done
}

func isUpgrade(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") &&
		r.Header.Get("Upgrade") != ""
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		dst.Del(k)
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

// streamCopy 边读边写边 flush，大响应不整体缓冲进内存。
func streamCopy(w http.ResponseWriter, src io.Reader) {
	fw := &flushWriter{w: w}
	buf := make([]byte, 32*1024)
	io.CopyBuffer(fw, src, buf)
}

type flushWriter struct{ w http.ResponseWriter }

func (f *flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if fl, ok := f.w.(http.Flusher); ok {
		fl.Flush()
	}
	return n, err
}

func writeFail(w http.ResponseWriter, reason FailReason, code int, msg string) {
	w.Header().Set("X-Proxy-Fail-Reason", string(reason))
	http.Error(w, fmt.Sprintf("%s: %s", reason, msg), code)
}
