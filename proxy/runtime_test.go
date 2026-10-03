package proxy_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shikejing/http-rewrite-proxy-go/config"
	"github.com/shikejing/http-rewrite-proxy-go/runtime"
	"github.com/shikejing/http-rewrite-proxy-go/upstream"
)

// runtimeUpstream 记录慢请求到达时的路径，用于验证在途请求视图。
func runtimeUpstream() (*httptest.Server, *[]string, *sync.Mutex) {
	var paths []string
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-time.After(30 * time.Second):
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"path": r.URL.Path})
	})
	return httptest.NewServer(mux), &paths, &mu
}

func rewriteConfig(upstreamURL, from, to string) *config.Config {
	cfg := testConfig(upstreamURL)
	cfg.Rules = []config.RuleConfig{{
		ID: "rw", Priority: 1, Match: config.MatchConfig{PathPrefix: from},
		Actions: config.ActionsConfig{PathPrefixReplace: &config.PathReplaceConfig{Match: from, Replacement: to}},
	}}
	return cfg
}

func TestHotReloadInFlightUnaffected(t *testing.T) {
	upstream, paths, mu := runtimeUpstream()
	defer upstream.Close()

	rt, err := runtime.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if err := rt.Reload(rewriteConfig(upstream.URL, "/old", "/new")); err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(rt.Handler())
	defer proxy.Close()

	// 在途的慢请求使用旧规则。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, proxy.URL+"/old/slow", nil)
	go http.DefaultClient.Do(req)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := append([]string(nil), (*paths)...)
		mu.Unlock()
		if len(got) > 0 {
			if got[0] != "/new/slow" {
				t.Fatalf("在途请求应使用旧规则 /new/slow, 得到 %q", got[0])
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 热更新规则。
	if err := rt.Reload(rewriteConfig(upstream.URL, "/old", "/changed")); err != nil {
		t.Fatal(err)
	}
	cancel()

	// 新请求使用新规则。
	_, body, _ := doRequest(t, http.MethodGet, proxy.URL+"/old/x", nil, nil)
	var got map[string]string
	json.Unmarshal([]byte(body), &got)
	if got["path"] != "/changed/x" {
		t.Fatalf("新请求应使用新规则 /changed/x, 得到 %q", got["path"])
	}
}

func TestRollbackOnInvalidConfig(t *testing.T) {
	upstream, _, _ := runtimeUpstream()
	defer upstream.Close()

	rt, err := runtime.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if err := rt.Reload(rewriteConfig(upstream.URL, "/old", "/new")); err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(rt.Handler())
	defer proxy.Close()

	// 非法配置（改写受保护头）必须被拒绝。
	bad := rewriteConfig(upstream.URL, "/old", "/new")
	bad.Rules[0].Actions = config.ActionsConfig{ReqHeaderSet: map[string]string{"Authorization": "hacked"}}
	if err := rt.Reload(bad); err == nil {
		t.Fatal("非法配置应被拒绝")
	}

	// 旧规则仍然生效。
	_, body, _ := doRequest(t, http.MethodGet, proxy.URL+"/old/x", nil, nil)
	var got map[string]string
	json.Unmarshal([]byte(body), &got)
	if got["path"] != "/new/x" {
		t.Fatalf("回滚后应继续使用旧规则 /new/x, 得到 %q", got["path"])
	}
}

func TestAdminAPI(t *testing.T) {
	upstream, _, _ := runtimeUpstream()
	defer upstream.Close()

	rt, err := runtime.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if err := rt.Reload(rewriteConfig(upstream.URL, "/old", "/new")); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/admin/", rt.AdminHandler())
	mux.Handle("/", rt.Handler())
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// 状态接口。
	resp, err := http.Get(srv.URL + "/admin/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status 应返回 200, 得到 %d", resp.StatusCode)
	}
	var status map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&status)
	if status["upstreams"] == nil {
		t.Fatal("status 应包含上游信息")
	}

	// 通过管理接口替换配置。
	newCfg := rewriteConfig(upstream.URL, "/old", "/admin")
	data, _ := json.Marshal(newCfg)
	resp, err = http.Post(srv.URL+"/admin/config", "application/json", strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("config 应返回 200, 得到 %d", resp.StatusCode)
	}

	_, body, _ := doRequest(t, http.MethodGet, srv.URL+"/old/x", nil, nil)
	var got map[string]string
	json.Unmarshal([]byte(body), &got)
	if got["path"] != "/admin/x" {
		t.Fatalf("管理接口热更新应生效, 得到 %q", got["path"])
	}

	// 非法 JSON 应返回 400。
	resp, _ = http.Post(srv.URL+"/admin/config", "application/json", strings.NewReader("{bad"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("非法 JSON 应返回 400, 得到 %d", resp.StatusCode)
	}
}

func TestHealthRemovalAndRecoveryViaProxy(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	upstream := httptest.NewServer(mux)
	defer upstream.Close()

	cfg := testConfig(upstream.URL)
	cfg.Upstreams[0].Health = config.HealthConfig{
		Path: "/health", IntervalMS: 20, TimeoutMS: 500,
		HealthyThreshold: 1, UnhealthyThreshold: 2,
	}
	rt, err := runtime.New("")
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if err := rt.Reload(cfg); err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(rt.Handler())
	defer proxy.Close()

	// 初始健康。
	status, _, _ := doRequest(t, http.MethodGet, proxy.URL+"/", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("初始应健康, 得到 %d", status)
	}

	// 切换为不健康，轮询状态等待摘除（避免转发成功重置健康检查失败计数）。
	healthy.Store(false)
	waitForInstanceHealthy(t, rt, false)
	status, _, _ = doRequest(t, http.MethodGet, proxy.URL+"/", nil, nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("不健康时应返回 503, 得到 %d", status)
	}

	// 恢复后重新上线。
	healthy.Store(true)
	waitForInstanceHealthy(t, rt, true)
	status, _, _ = doRequest(t, http.MethodGet, proxy.URL+"/", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("恢复后应返回 200, 得到 %d", status)
	}
}

// waitForInstanceHealthy 轮询运行时状态，直到上游实例健康状态符合预期。
func waitForInstanceHealthy(t *testing.T, rt *runtime.Runtime, wantHealthy bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := rt.Status()
		upstreams, ok := st["upstreams"].(map[string][]upstream.InstanceStatus)
		if ok {
			for _, instances := range upstreams {
				for _, inst := range instances {
					if inst.Healthy == wantHealthy {
						return
					}
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等待实例健康状态=%v 超时", wantHealthy)
}
