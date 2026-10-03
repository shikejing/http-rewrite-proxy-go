package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func baseCfg(upstreamName, upstreamURL, prefix string) *Config {
	return &Config{
		Rules: []RuleConfig{{
			Name: "r1", Priority: 1, PathPrefix: prefix, Upstream: upstreamName,
		}},
		Upstreams: []UpstreamConfig{{Name: upstreamName, Endpoints: []string{upstreamURL}}},
		Retry:     RetryConfig{MaxAttempts: 2, Backoff: time.Millisecond},
		Timeouts:  TimeoutConfig{PerAttempt: 2 * time.Second, Overall: 5 * time.Second},
	}
}

func TestEndToEndRewrite(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server", "upstream")
		fmt.Fprintf(w, "path=%s query=%s xenv=%s", r.URL.Path, r.URL.RawQuery, r.Header.Get("X-Env"))
	}))
	defer up.Close()

	cfg := baseCfg("u", up.URL, "/old")
	cfg.Rules[0].ReplacePath = "/new"
	cfg.Rules[0].AddQuery = map[string]string{"token": "t1"}
	cfg.Rules[0].RemoveQuery = []string{"debug"}
	cfg.Rules[0].RequestHeaders = HeaderOps{Set: map[string]string{"X-Env": "prod"}}
	cfg.Rules[0].ResponseHeaders = HeaderOps{
		Set: map[string]string{"X-Cache": "hit"}, Remove: []string{"Server"},
	}
	p, err := NewProxy(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/old/page?debug=1&keep=2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	want := "path=/new/page query=keep=2&token=t1 xenv=prod"
	if string(body) != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
	if resp.Header.Get("Server") != "" || resp.Header.Get("X-Cache") != "hit" {
		t.Fatalf("response headers not rewritten: %v", resp.Header)
	}
}

func TestStreamingNotBuffered(t *testing.T) {
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("chunk1"))
		w.(http.Flusher).Flush()
		<-release // 在客户端收到 chunk1 之前，上游不发送后续数据
		w.Write([]byte("chunk2"))
	}))
	defer up.Close()
	defer close(release)

	p, _ := NewProxy(baseCfg("u", up.URL, "/"))
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/big")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 6)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "chunk1" {
		t.Fatalf("got %q", buf)
	}
	// 若整体缓冲，客户端在上游解除阻塞前不可能读到 chunk1 —— 到此即证明流式
}

func TestClientAbortReleasesUpstream(t *testing.T) {
	upstreamDone := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("chunk1"))
		w.(http.Flusher).Flush()
		<-r.Context().Done() // 客户端断开应传播到上游
		close(upstreamDone)
	}))
	defer up.Close()

	p, _ := NewProxy(baseCfg("u", up.URL, "/"))
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 6)
	io.ReadFull(resp.Body, buf)
	cancel() // 客户端中断
	resp.Body.Close()

	select {
	case <-upstreamDone:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream request context not canceled after client abort")
	}
}

func TestRetryOnlyIdempotent(t *testing.T) {
	var calls atomic.Int64
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusBadGateway) // 第一次失败
			return
		}
		w.Write([]byte("ok"))
	}))
	defer up.Close()

	cfg := baseCfg("u", up.URL, "/")
	p, _ := NewProxy(cfg)
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	// GET 可重试：第一次 502，第二次成功
	resp, err := http.Get(srv.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("GET retry failed: %q", body)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}

	// POST 不可重试：直接失败，不产生第二次副作用
	calls.Store(0)
	resp, err = http.Post(srv.URL+"/x", "text/plain", strings.NewReader("data"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("POST status = %d", resp.StatusCode)
	}
	if calls.Load() != 1 {
		t.Fatalf("POST was retried, calls = %d", calls.Load())
	}
}

func TestRetryExhaustedAndNoHealthy(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer up.Close()
	p, _ := NewProxy(baseCfg("u", up.URL, "/"))
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("X-Proxy-Fail-Reason") != string(ReasonRetriesExhausted) {
		t.Fatalf("reason = %s", resp.Header.Get("X-Proxy-Fail-Reason"))
	}
}

func TestSignedRequestRewriteDenied(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer up.Close()
	cfg := baseCfg("u", up.URL, "/old")
	cfg.Rules[0].ReplacePath = "/new"
	cfg.Auth.SignedHeaders = []string{"Authorization"}
	p, _ := NewProxy(cfg)
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/old/x", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 ...")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	if resp.Header.Get("X-Proxy-Fail-Reason") != string(ReasonSignedDenied) {
		t.Fatalf("reason = %s", resp.Header.Get("X-Proxy-Fail-Reason"))
	}
}

func TestSensitiveHeaderStripAndInject(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "cookie=%q internal=%q", r.Header.Get("Cookie"), r.Header.Get("X-Internal-Token"))
	}))
	defer up.Close()
	cfg := baseCfg("u", up.URL, "/")
	cfg.Auth.StripHeaders = []string{"Cookie"}
	cfg.Auth.InjectHeaders = map[string]string{"X-Internal-Token": "secret"}
	p, _ := NewProxy(cfg)
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/x", nil)
	req.Header.Set("Cookie", "session=abc")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != `cookie="" internal="secret"` {
		t.Fatalf("body = %s", body)
	}
}

func TestHotReloadInFlightNotInterrupted(t *testing.T) {
	release := make(chan struct{})
	up1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Write([]byte("from-up1"))
	}))
	defer up1.Close()
	up2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("from-up2"))
	}))
	defer up2.Close()

	p, _ := NewProxy(baseCfg("u1", up1.URL, "/"))
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	// 在途请求使用旧视图（指向 up1）
	type result struct {
		body string
		err  error
	}
	inflight := make(chan result, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/x")
		if err != nil {
			inflight <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		inflight <- result{body: string(b)}
	}()
	time.Sleep(50 * time.Millisecond) // 确保请求已进入 up1 并阻塞

	// 热更新：新规则指向 up2
	if err := p.UpdateConfig(baseCfg("u2", up2.URL, "/")); err != nil {
		t.Fatal(err)
	}
	// 新请求立即走新配置
	resp, err := http.Get(srv.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "from-up2" {
		t.Fatalf("new request should hit up2, got %q", b)
	}
	// 在途请求不被中断，仍由 up1 完成
	close(release)
	r := <-inflight
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.body != "from-up1" {
		t.Fatalf("in-flight request got %q, want from-up1", r.body)
	}
}

func TestHotReloadInvalidKeepsOld(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer up.Close()
	p, _ := NewProxy(baseCfg("u", up.URL, "/"))
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	bad := baseCfg("u", up.URL, "/")
	bad.Rules[0].Upstream = "nonexistent" // 引用不存在的上游
	if err := p.UpdateConfig(bad); err == nil {
		t.Fatal("invalid config should be rejected")
	}
	// 旧配置仍然可用
	resp, err := http.Get(srv.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("old config broken after failed reload, status = %d", resp.StatusCode)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Write([]byte("ok"))
	}))
	defer up.Close()

	cfg := baseCfg("u", up.URL, "/")
	cfg.Limits.MaxConcurrent = 1
	cfg.Limits.QueueLimit = 0 // 不排队，超限直接拒绝
	p, _ := NewProxy(cfg)
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	first := make(chan struct{})
	go func() {
		resp, err := http.Get(srv.URL + "/slow")
		if err == nil {
			resp.Body.Close()
		}
		close(first)
	}()
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get(srv.URL + "/slow")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable ||
		resp.Header.Get("X-Proxy-Fail-Reason") != string(ReasonOverload) {
		t.Fatalf("status = %d reason = %s", resp.StatusCode, resp.Header.Get("X-Proxy-Fail-Reason"))
	}
	close(release) // 放行第一个请求，验证慢请求不阻塞拒绝路径
	<-first
}

func TestUpgradeTunnel(t *testing.T) {
	// 上游：完成 101 握手后做回声
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj := w.(http.Hijacker)
		conn, rw, err := hj.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: echo\r\nConnection: Upgrade\r\n\r\n")
		rw.Flush()
		io.Copy(conn, conn) // 回声
	}))
	defer up.Close()

	p, _ := NewProxy(baseCfg("u", up.URL, "/"))
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: echo\r\n\r\n")
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("status line = %q", status)
	}
	// 读完响应头
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	// 双向流式：发什么收什么
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(br, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("echo got %q", buf)
	}
}

func TestHotReloadUnderConcurrency(t *testing.T) {
	up1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("up1"))
	}))
	defer up1.Close()
	up2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("up2"))
	}))
	defer up2.Close()

	p, _ := NewProxy(baseCfg("u1", up1.URL, "/"))
	defer p.Close()
	srv := httptest.NewServer(p)
	defer srv.Close()

	stop := make(chan struct{})
	errCh := make(chan error, 64)
	// 并发请求与并发热更新交叉进行，任一时刻规则视图必须自洽
	for i := 0; i < 8; i++ {
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				resp, err := http.Get(srv.URL + "/x")
				if err != nil {
					errCh <- err
					continue
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if s := string(b); s != "up1" && s != "up2" {
					errCh <- fmt.Errorf("inconsistent view: %q", s)
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		cfg := baseCfg("u1", up1.URL, "/")
		if i%2 == 1 {
			cfg = baseCfg("u2", up2.URL, "/")
		}
		if err := p.UpdateConfig(cfg); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}
