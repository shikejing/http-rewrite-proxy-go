package proxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRulePriorityOrder(t *testing.T) {
	rs, err := NewRuleSet([]RuleConfig{
		{Name: "b-long", Priority: 10, PathPrefix: "/api/v1", Upstream: "u"},
		{Name: "a-short", Priority: 10, PathPrefix: "/api", Upstream: "u"},
		{Name: "c-high", Priority: 1, PathPrefix: "/api", Upstream: "u"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 优先级数值小者胜
	if got := rs.Match("/api/x").Name(); got != "c-high" {
		t.Fatalf("want c-high, got %s", got)
	}
	// 同优先级时更长前缀胜
	rs2, _ := NewRuleSet([]RuleConfig{
		{Name: "b-long", Priority: 10, PathPrefix: "/api/v1", Upstream: "u"},
		{Name: "a-short", Priority: 10, PathPrefix: "/api", Upstream: "u"},
	})
	if got := rs2.Match("/api/v1/x").Name(); got != "b-long" {
		t.Fatalf("want b-long, got %s", got)
	}
	// 完全并列时按名字典序，结果确定
	rs3, _ := NewRuleSet([]RuleConfig{
		{Name: "zz", Priority: 1, PathPrefix: "/x", Upstream: "u"},
		{Name: "aa", Priority: 1, PathPrefix: "/x", Upstream: "u"},
	})
	if got := rs3.Match("/x/y").Name(); got != "aa" {
		t.Fatalf("want aa, got %s", got)
	}
	if rs.Match("/other") != nil {
		t.Fatal("want no match")
	}
}

func TestRuleApplyRequest(t *testing.T) {
	rs, _ := NewRuleSet([]RuleConfig{{
		Name:        "r",
		PathPrefix:  "/old",
		ReplacePath: "/new",
		Upstream:    "u",
		AddQuery:    map[string]string{"token": "abc"},
		RemoveQuery: []string{"debug"},
		RequestHeaders: HeaderOps{
			Set:    map[string]string{"X-Env": "prod"},
			Remove: []string{"X-Debug"},
		},
	}})
	rule := rs.Match("/old/path")
	if rule == nil {
		t.Fatal("no match")
	}
	req := httptest.NewRequest("GET", "http://proxy/old/path?debug=1&keep=2", nil)
	req.Header.Set("X-Debug", "1")
	rule.ApplyRequest(req)
	if req.URL.Path != "/new/path" {
		t.Fatalf("path = %s", req.URL.Path)
	}
	q := req.URL.Query()
	if q.Get("token") != "abc" || q.Get("debug") != "" || q.Get("keep") != "2" {
		t.Fatalf("query = %s", req.URL.RawQuery)
	}
	if req.Header.Get("X-Env") != "prod" || req.Header.Get("X-Debug") != "" {
		t.Fatalf("headers = %v", req.Header)
	}
}

func TestRuleApplyResponseHeaders(t *testing.T) {
	rs, _ := NewRuleSet([]RuleConfig{{
		Name:       "r",
		PathPrefix: "/",
		Upstream:   "u",
		ResponseHeaders: HeaderOps{
			Set:    map[string]string{"X-Cache": "hit"},
			Remove: []string{"Server"},
		},
	}})
	rule := rs.Match("/anything")
	req := httptest.NewRequest("GET", "http://x/anything", nil)
	_ = req
	h := http.Header{"Server": {"nginx"}, "Content-Type": {"text/plain"}}
	rule.ApplyResponseHeaders(h)
	if h.Get("Server") != "" || h.Get("X-Cache") != "hit" || h.Get("Content-Type") != "text/plain" {
		t.Fatalf("headers = %v", h)
	}
}
