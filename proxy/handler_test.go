package proxy_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shikejing/http-rewrite-proxy-go/config"
	"github.com/shikejing/http-rewrite-proxy-go/runtime"
)

// ---------- 测试上游 ----------

type upstreamState struct {
	flakyHits atomic.Int64
	slowHit   chan struct{}
	slowDone  chan struct{}
}

func newTestUpstream() (*httptest.Server, *upstreamState) {
	st := &upstreamState{slowHit: make(chan struct{}, 8), slowDone: make(chan struct{}, 8)}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/flaky", func(w http.ResponseWriter, r *http.Request) {
		n := st.flakyHits.Add(1)
		if n < 3 {
			hj, _ := w.(http.Hijacker)
			conn, _, err := hj.Hijack()
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			conn.Close()
			return
		}
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "ok")
	})
	mux.HandleFunc("/early-close", func(w http.ResponseWriter, r *http.Request) {
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		conn.Close()
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		st.slowHit <- struct{}{}
		select {
		case <-r.Context().Done():
			st.slowDone <- struct{}{}
		case <-time.After(30 * time.Second):
		}
	})
	mux.HandleFunc("/delay", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		f := w.(http.Flusher)
		for i := 0; i < 5; i++ {
			fmt.Fprintf(w, "data: %d\n\n", i)
			f.Flush()
			time.Sleep(50 * time.Millisecond)
		}
	})
	mux.HandleFunc("/stream-slow-first", func(w http.ResponseWriter, r *http.Request) {
		f := w.(http.Flusher)
		w.Write([]byte("first"))
		f.Flush()
		time.Sleep(300 * time.Millisecond)
		w.Write([]byte("rest"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		out := map[string]interface{}{
			"method":  r.Method,
			"path":    r.URL.Path,
			"query":   r.URL.RawQuery,
			"headers": r.Header,
			"body":    string(body),
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})
	return httptest.NewServer(mux), st
}

// ---------- 测试环境 ----------

type testEnv struct {
	t        *testing.T
	rt       *runtime.Runtime
	proxy    *httptest.Server
	upstream *httptest.Server
	state    *upstreamState
}

func newEnv(t *testing.T, cfg *config.Config) *testEnv {
	t.Helper()
	upstream, state := newTestUpstream()
	if cfg == nil {
		cfg = testConfig(upstream.URL)
	} else if len(cfg.Upstreams) > 0 {
		cfg.Upstreams[0].URLs = []string{upstream.URL}
	}
	rt, err := runtime.New("")
	if err != nil {
		t.Fatalf("runtime.New: %v", err)
	}
	if err := rt.Reload(cfg); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	proxy := httptest.NewServer(rt.Handler())
	t.Cleanup(func() {
		proxy.Close()
		rt.Close()
		upstream.Close()
	})
	return &testEnv{t: t, rt: rt, proxy: proxy, upstream: upstream, state: state}
}

func testConfig(upstreamURL string) *config.Config {
	return &config.Config{
		Upstreams: []config.UpstreamConfig{{
			Name: "default",
			URLs: []string{upstreamURL},
			Health: config.HealthConfig{
				Path: "/health", IntervalMS: 3_600_000, TimeoutMS: 1000,
				HealthyThreshold: 1, UnhealthyThreshold: 2,
			},
		}},
		Settings: config.Settings{
			DefaultUpstream:     "default",
			AttemptTimeoutMS:    5000,
			OverallTimeoutMS:    10000,
			MaxRetries:          2,
			RetryBackoffMS:      5,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeoutMS:   90000,
		},
	}
}

func (e *testEnv) reload(cfg *config.Config) error {
	if len(cfg.Upstreams) > 0 {
		cfg.Upstreams[0].URLs = []string{e.upstream.URL}
	}
	return e.rt.Reload(cfg)
}

func doRequest(t *testing.T, method, url string, body io.Reader, headers map[string]string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data), resp.Header
}

// ---------- 用例 ----------

func TestBasicForwardAndRewrite(t *testing.T) {
	cfg := testConfig("")
	cfg.Rules = []config.RuleConfig{
		{
			ID: "r1", Priority: 1, Match: config.MatchConfig{PathPrefix: "/gw/"},
			Actions: config.ActionsConfig{
				PathPrefixReplace: &config.PathReplaceConfig{Match: "/gw/", Replacement: "/api/"},
				QuerySet:          map[string]string{"source": "gw"},
				ReqHeaderSet:      map[string]string{"X-Tenant": "acme"},
				RespHeaderSet:     map[string]string{"X-Proxy": "go"},
			},
		},
	}
	env := newEnv(t, cfg)

	status, body, respHeader := doRequest(t, http.MethodGet, env.proxy.URL+"/gw/users?a=1", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("状态码: %d body=%s", status, body)
	}
	var got map[string]interface{}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got["path"] != "/api/users" {
		t.Fatalf("路径改写失败: %v", got["path"])
	}
	if got["query"] != "a=1&source=gw" {
		t.Fatalf("查询改写失败: %v", got["query"])
	}
	headers := got["headers"].(map[string]interface{})
	if headers["X-Tenant"] == nil || headers["X-Tenant"].([]interface{})[0] != "acme" {
		t.Fatalf("请求头改写失败: %v", headers)
	}
	if respHeader.Get("X-Proxy") != "go" {
		t.Fatalf("响应头改写失败: %v", respHeader)
	}
}

func TestStreamingTTFB(t *testing.T) {
	env := newEnv(t, nil)
	req, _ := http.NewRequest(http.MethodGet, env.proxy.URL+"/stream-slow-first", nil)
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 5)
	n, err := io.ReadFull(resp.Body, buf)
	ttfb := time.Since(start)
	if err != nil || n != 5 || string(buf) != "first" {
		t.Fatalf("首字节读取失败: n=%d err=%v", n, err)
	}
	if ttfb > 200*time.Millisecond {
		t.Fatalf("首字节延迟 %v，疑似整体缓冲", ttfb)
	}
	rest, _ := io.ReadAll(resp.Body)
	if string(rest) != "rest" {
		t.Fatalf("后续内容丢失: %q", rest)
	}
}

func TestSSEStreamEvents(t *testing.T) {
	env := newEnv(t, nil)
	resp, err := http.Get(env.proxy.URL + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	deadline := time.After(3 * time.Second)
	count := 0
	for count < 5 {
		select {
		case <-deadline:
			t.Fatalf("流式事件超时，仅收到 %d 条", count)
		default:
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("读取事件流失败: %v", err)
		}
		if strings.HasPrefix(line, "data:") {
			count++
		}
	}
}

func TestClientCancelReleasesUpstream(t *testing.T) {
	env := newEnv(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, env.proxy.URL+"/slow", nil)
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
	select {
	case <-env.state.slowDone:
	case <-time.After(2 * time.Second):
		t.Fatal("上游未感知到客户端取消（资源泄漏）")
	}
}

func TestUpstreamEarlyClose(t *testing.T) {
	env := newEnv(t, nil)
	status, body, _ := doRequest(t, http.MethodGet, env.proxy.URL+"/early-close", nil, nil)
	if status != http.StatusBadGateway {
		t.Fatalf("上游提前关闭应返回 502, 得到 %d: %s", status, body)
	}
	if !strings.Contains(body, "upstream_connect") {
		t.Fatalf("错误体应包含可区分原因 upstream_connect: %s", body)
	}
}

func TestRetryOnIdempotentRequest(t *testing.T) {
	env := newEnv(t, nil)
	status, _, _ := doRequest(t, http.MethodGet, env.proxy.URL+"/flaky", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("GET 重试后应成功, 状态码 %d", status)
	}
	if hits := env.state.flakyHits.Load(); hits != 3 {
		t.Fatalf("GET 应重试至第 3 次成功, 实际上游命中 %d 次", hits)
	}
}

func TestNoRetryOnNonReplayableRequest(t *testing.T) {
	env := newEnv(t, nil)
	status, _, _ := doRequest(t, http.MethodPost, env.proxy.URL+"/flaky", strings.NewReader("payload"), nil)
	if status != http.StatusBadGateway {
		t.Fatalf("POST 不可重试应返回 502, 得到 %d", status)
	}
	if hits := env.state.flakyHits.Load(); hits != 1 {
		t.Fatalf("非幂等/带请求体请求不得重试, 实际命中 %d 次", hits)
	}
}

func TestTimeout(t *testing.T) {
	cfg := testConfig("")
	cfg.Settings.AttemptTimeoutMS = 200
	env := newEnv(t, cfg)
	status, body, _ := doRequest(t, http.MethodGet, env.proxy.URL+"/delay", nil, nil)
	if status != http.StatusGatewayTimeout {
		t.Fatalf("超时应返回 504, 得到 %d: %s", status, body)
	}
	if !strings.Contains(body, "timeout") {
		t.Fatalf("错误体应包含 timeout: %s", body)
	}
}

func TestQueueReject(t *testing.T) {
	cfg := testConfig("")
	cfg.Upstreams[0].MaxConns = 1
	cfg.Upstreams[0].QueueTimeoutMS = 0
	env := newEnv(t, cfg)

	// 第一个请求占满并发名额。
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	req1, _ := http.NewRequestWithContext(ctx1, http.MethodGet, env.proxy.URL+"/slow", nil)
	go http.DefaultClient.Do(req1)
	select {
	case <-env.state.slowHit:
	case <-time.After(2 * time.Second):
		t.Fatal("上游未收到慢请求")
	}

	status, body, _ := doRequest(t, http.MethodGet, env.proxy.URL+"/", nil, nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("名额满且不排队应返回 503, 得到 %d: %s", status, body)
	}
	if !strings.Contains(body, "queue_full") {
		t.Fatalf("错误体应包含 queue_full: %s", body)
	}
}

func TestSlowRequestDoesNotBlockFast(t *testing.T) {
	cfg := testConfig("")
	cfg.Upstreams[0].MaxConns = 4
	env := newEnv(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, env.proxy.URL+"/slow", nil)
	go http.DefaultClient.Do(req)
	select {
	case <-env.state.slowHit:
	case <-time.After(2 * time.Second):
		t.Fatal("慢请求未到达上游")
	}

	start := time.Now()
	status, _, _ := doRequest(t, http.MethodGet, env.proxy.URL+"/", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("慢连接不应阻塞快请求, 状态码 %d", status)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("快请求被慢请求阻塞")
	}
}

func TestACLStripsAndInjects(t *testing.T) {
	cfg := testConfig("")
	cfg.ACL.StripReqHeaders = []string{"X-Evil"}
	cfg.ACL.InjectReqHeaders = map[string]string{"X-Injected": "yes"}
	env := newEnv(t, cfg)

	status, body, _ := doRequest(t, http.MethodGet, env.proxy.URL+"/", nil, map[string]string{"X-Evil": "spoofed"})
	if status != http.StatusOK {
		t.Fatalf("状态码 %d", status)
	}
	var got map[string]interface{}
	json.Unmarshal([]byte(body), &got)
	headers := got["headers"].(map[string]interface{})
	if headers["X-Evil"] != nil {
		t.Fatalf("不可信请求头应被剥离")
	}
	if headers["X-Injected"] == nil || headers["X-Injected"].([]interface{})[0] != "yes" {
		t.Fatalf("身份头应注入")
	}
	if headers["X-Forwarded-For"] == nil {
		t.Fatalf("应追加 X-Forwarded-For")
	}
}

func TestSignedRequestUntouched(t *testing.T) {
	env := newEnv(t, nil)
	sig := "Bearer signed-token-abc"
	status, body, _ := doRequest(t, http.MethodPost, env.proxy.URL+"/", strings.NewReader("signed-body"), map[string]string{"Authorization": sig})
	if status != http.StatusOK {
		t.Fatalf("状态码 %d", status)
	}
	var got map[string]interface{}
	json.Unmarshal([]byte(body), &got)
	headers := got["headers"].(map[string]interface{})
	if headers["Authorization"] == nil || headers["Authorization"].([]interface{})[0] != sig {
		t.Fatalf("签名头必须原样透传: %v", headers["Authorization"])
	}
	if got["body"] != "signed-body" {
		t.Fatalf("签名请求体必须原样透传: %q", got["body"])
	}
}

func TestRuleCannotRewriteProtectedHeader(t *testing.T) {
	cfg := testConfig("")
	cfg.Rules = []config.RuleConfig{{
		ID: "evil", Priority: 1, Match: config.MatchConfig{PathPrefix: "/"},
		Actions: config.ActionsConfig{ReqHeaderSet: map[string]string{"Authorization": "hacked"}},
	}}
	rt, err := runtime.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if err := rt.Reload(cfg); err == nil {
		t.Fatal("改写受保护请求头的规则必须被拒绝")
	}
}

func TestUpgradePassthrough(t *testing.T) {
	// 原始 TCP 上游：收到 101 请求后回 101 并回声数据。
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		for {
			conn, err := upstream.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				br := bufio.NewReader(conn)
				req, err := http.ReadRequest(br)
				if err != nil {
					return
				}
				if req.URL.Path == "/health" {
					conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"))
					return
				}
				if req.Header.Get("Upgrade") == "" {
					return
				}
				conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: echo\r\nConnection: Upgrade\r\n\r\n"))
				buf := make([]byte, 1024)
				for {
					n, err := conn.Read(buf)
					if n > 0 {
						conn.Write(buf[:n])
					}
					if err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	cfg := testConfig(upstream.Addr().String())
	cfg.Upstreams[0].URLs = []string{"http://" + upstream.Addr().String()}
	rt, err := runtime.New("")
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Reload(cfg); err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(rt.Handler())
	defer proxy.Close()
	defer rt.Close()

	conn, err := net.Dial("tcp", proxy.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /upgrade HTTP/1.1\r\nHost: example.com\r\nUpgrade: echo\r\nConnection: Upgrade\r\n\r\n")
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("应透传 101, 得到 %d", resp.StatusCode)
	}
	conn.Write([]byte("ping"))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(br, buf); err != nil {
		t.Fatalf("双向透传失败: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("回声数据错误: %q", buf)
	}
}
