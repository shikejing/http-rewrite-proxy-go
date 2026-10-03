package upstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shikejing/http-rewrite-proxy-go/config"
)

func newTestPool(t *testing.T, urls []string, intervalMS int) *Pool {
	t.Helper()
	p, err := NewPool("test", urls, 0, 0, config.Settings{}, config.HealthConfig{
		Path:               "/health",
		IntervalMS:         intervalMS,
		TimeoutMS:          500,
		HealthyThreshold:   1,
		UnhealthyThreshold: 2,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	t.Cleanup(p.Stop)
	return p
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("等待条件超时")
}

func TestHealthCheckRemovalAndRecovery(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if !healthy.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := newTestPool(t, []string{srv.URL}, 20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.StartHealthChecks(ctx)

	// 初始健康。
	waitFor(t, time.Second, func() bool { return p.Status()[0].Healthy })

	// 切换为不健康，等待摘除。
	healthy.Store(false)
	waitFor(t, 2*time.Second, func() bool { return !p.Status()[0].Healthy })
	if p.Select() != nil {
		t.Fatalf("全部不健康时 Select 应返回 nil")
	}

	// 恢复，等待重新上线。
	healthy.Store(true)
	waitFor(t, 2*time.Second, func() bool { return p.Status()[0].Healthy })
	if p.Select() == nil {
		t.Fatalf("恢复后应能选中实例")
	}
}

func TestRoundRobinDistribution(t *testing.T) {
	var srvs []*httptest.Server
	var urls []string
	for i := 0; i < 3; i++ {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer s.Close()
		srvs = append(srvs, s)
		urls = append(urls, s.URL)
	}
	p := newTestPool(t, urls, 1000)
	counts := make(map[string]int)
	for i := 0; i < 300; i++ {
		inst := p.Select()
		if inst == nil {
			t.Fatalf("实例应健康")
		}
		counts[inst.rawURL]++
	}
	for _, u := range urls {
		if counts[u] != 100 {
			t.Fatalf("轮询应均匀分布，%s 得到 %d 次", u, counts[u])
		}
	}
}

func TestUnhealthyInstanceSkipped(t *testing.T) {
	var urls []string
	for i := 0; i < 2; i++ {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
		defer s.Close()
		urls = append(urls, s.URL)
	}
	p := newTestPool(t, urls, 1000)
	state := p.state.Load()
	state.instances[0].healthy.Store(false)

	got := make(map[string]int)
	for i := 0; i < 20; i++ {
		inst := p.Select()
		if inst == nil {
			t.Fatal("应有健康实例")
		}
		got[inst.rawURL]++
	}
	if got[urls[0]] != 0 {
		t.Fatalf("不健康实例不应被选中")
	}
	if got[urls[1]] != 20 {
		t.Fatalf("健康实例应被持续选中")
	}
}

func TestUpdatePreservesHealthState(t *testing.T) {
	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer s1.Close()
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer s2.Close()

	p := newTestPool(t, []string{s1.URL}, 1000)
	p.state.Load().instances[0].healthy.Store(false)

	if err := p.Update([]string{s1.URL, s2.URL}, 0, 0); err != nil {
		t.Fatal(err)
	}
	status := p.Status()
	if len(status) != 2 {
		t.Fatalf("更新后应有 2 个实例")
	}
	if status[0].Healthy {
		t.Fatalf("已有实例的健康状态应保留")
	}
	if !status[1].Healthy {
		t.Fatalf("新增实例应默认健康")
	}
}

func TestMarkResultThreshold(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer s.Close()
	p := newTestPool(t, []string{s.URL}, 1000)
	inst := p.state.Load().instances[0]

	p.MarkResult(inst, true)
	if !inst.healthy.Load() {
		t.Fatal("成功不应导致摘除")
	}
	p.MarkResult(inst, false)
	if !inst.healthy.Load() {
		t.Fatal("单次失败不应摘除（阈值为 2）")
	}
	p.MarkResult(inst, false)
	if inst.healthy.Load() {
		t.Fatal("连续 2 次失败应摘除")
	}
	p.MarkResult(inst, false)
	if inst.healthy.Load() {
		t.Fatal("摘除后应保持摘除状态")
	}
	p.MarkResult(inst, true)
	if !inst.healthy.Load() {
		t.Fatal("1 次成功应恢复（健康阈值为 1）")
	}
}
