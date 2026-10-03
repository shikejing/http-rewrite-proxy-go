package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func fastHC() HealthCheckConfig {
	return HealthCheckConfig{
		Interval: 20 * time.Millisecond, Timeout: 200 * time.Millisecond,
		FailThreshold: 2, RecoverThreshold: 1,
	}
}

func TestPoolRoundRobinFair(t *testing.T) {
	p := NewPool(UpstreamConfig{Name: "u", Endpoints: []string{
		"http://127.0.0.1:1", "http://127.0.0.1:2", "http://127.0.0.1:3",
	}}, fastHC())
	seen := map[string]int{}
	for i := 0; i < 9; i++ {
		addr, err := p.Next()
		if err != nil {
			t.Fatal(err)
		}
		seen[addr]++
	}
	// 每个健康实例都被均匀选中，不会被持续绕过
	for addr, n := range seen {
		if n != 3 {
			t.Fatalf("endpoint %s selected %d times, want 3", addr, n)
		}
	}
}

func TestPoolHealthRemoveAndRecover(t *testing.T) {
	var down atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if down.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := NewPool(UpstreamConfig{Name: "u", Endpoints: []string{srv.URL}}, fastHC())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.RunHealthChecks(ctx, srv.Client())

	if p.HealthyCount() != 1 {
		t.Fatal("initially healthy")
	}
	// 摘除：连续失败达到阈值
	down.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	for p.HealthyCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.HealthyCount() != 0 {
		t.Fatal("endpoint should be removed after failures")
	}
	if _, err := p.Next(); err != ErrNoHealthyUpstream {
		t.Fatalf("want ErrNoHealthyUpstream, got %v", err)
	}
	// 恢复：探活成功后重新加入选路
	down.Store(false)
	deadline = time.Now().Add(3 * time.Second)
	for p.HealthyCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.HealthyCount() != 1 {
		t.Fatal("endpoint should recover")
	}
	if _, err := p.Next(); err != nil {
		t.Fatalf("recovered endpoint should be selectable: %v", err)
	}
}

func TestPoolSkipsUnhealthy(t *testing.T) {
	bad := NewPool(UpstreamConfig{Name: "u", Endpoints: []string{
		"http://127.0.0.1:1", "http://127.0.0.1:2",
	}}, fastHC())
	// 手动把第一个实例打入失败状态机
	bad.endpoints[0].record(false, bad.hc)
	bad.endpoints[0].record(false, bad.hc)
	for i := 0; i < 6; i++ {
		addr, err := bad.Next()
		if err != nil {
			t.Fatal(err)
		}
		if addr != "http://127.0.0.1:2" {
			t.Fatalf("unhealthy endpoint was selected: %s", addr)
		}
	}
}
