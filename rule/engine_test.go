package rule

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shikejing/http-rewrite-proxy-go/config"
)

func compile(t *testing.T, cfgs []config.RuleConfig) *Engine {
	t.Helper()
	e, err := Compile(cfgs)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return e
}

func TestPriorityOrder(t *testing.T) {
	e := compile(t, []config.RuleConfig{
		{ID: "low", Priority: 1, Match: config.MatchConfig{PathPrefix: "/"}, Actions: config.ActionsConfig{ReqHeaderSet: map[string]string{"X-Order": "low"}}},
		{ID: "high", Priority: 10, Match: config.MatchConfig{PathPrefix: "/"}, Actions: config.ActionsConfig{ReqHeaderSet: map[string]string{"X-Order": "high"}}},
		{ID: "mid", Priority: 5, Match: config.MatchConfig{PathPrefix: "/"}, Actions: config.ActionsConfig{ReqHeaderSet: map[string]string{"X-Order": "mid"}}},
	})
	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	_, _ = e.Apply(r)
	if got := r.Header.Get("X-Order"); got != "low" {
		t.Fatalf("高优先级先执行应被低优先级覆盖（set 语义），期望 low，得到 %q", got)
	}
}

func TestTieBreakByID(t *testing.T) {
	e := compile(t, []config.RuleConfig{
		{ID: "b", Priority: 5, Match: config.MatchConfig{PathPrefix: "/"}, Actions: config.ActionsConfig{Upstream: "up-b"}},
		{ID: "a", Priority: 5, Match: config.MatchConfig{PathPrefix: "/"}, Actions: config.ActionsConfig{Upstream: "up-a"}},
	})
	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	up, _ := e.Apply(r)
	if up != "up-a" {
		t.Fatalf("同优先级按 ID 字典序，a 应先执行并决定上游，得到 %q", up)
	}
}

func TestPathPrefixReplace(t *testing.T) {
	e := compile(t, []config.RuleConfig{
		{ID: "r1", Priority: 1, Match: config.MatchConfig{PathPrefix: "/api/v1/"}, Actions: config.ActionsConfig{
			PathPrefixReplace: &config.PathReplaceConfig{Match: "/api/v1/", Replacement: "/api/v2/"},
		}},
	})
	r := httptest.NewRequest(http.MethodGet, "http://example.com/api/v1/users?x=1", nil)
	e.Apply(r)
	if r.URL.Path != "/api/v2/users" {
		t.Fatalf("路径前缀替换失败: %q", r.URL.Path)
	}
	if r.URL.RawQuery != "x=1" {
		t.Fatalf("查询串应保留: %q", r.URL.RawQuery)
	}
}

func TestPathPrefixStrip(t *testing.T) {
	e := compile(t, []config.RuleConfig{
		{ID: "r1", Priority: 1, Match: config.MatchConfig{PathPrefix: "/gw/"}, Actions: config.ActionsConfig{
			PathPrefixReplace: &config.PathReplaceConfig{Match: "/gw/", Replacement: "/"},
		}},
	})
	r := httptest.NewRequest(http.MethodGet, "http://example.com/gw/a/b", nil)
	e.Apply(r)
	if r.URL.Path != "/a/b" {
		t.Fatalf("前缀剥离失败: %q", r.URL.Path)
	}
}

func TestQueryOps(t *testing.T) {
	e := compile(t, []config.RuleConfig{
		{ID: "q", Priority: 1, Match: config.MatchConfig{PathPrefix: "/"}, Actions: config.ActionsConfig{
			QuerySet: map[string]string{"a": "set"},
			QueryAdd: map[string]string{"b": "add"},
			QueryDel: []string{"c"},
		}},
	})
	r := httptest.NewRequest(http.MethodGet, "http://example.com/p?a=old&c=del&keep=1", nil)
	e.Apply(r)
	q := r.URL.Query()
	if q.Get("a") != "set" {
		t.Fatalf("query set 失败: %q", q.Get("a"))
	}
	if q.Get("keep") != "1" {
		t.Fatalf("未匹配的查询参数应保留")
	}
	if q.Get("c") != "" {
		t.Fatalf("query del 失败")
	}
	if vals := q["b"]; len(vals) != 1 || vals[0] != "add" {
		t.Fatalf("query add 失败: %v", vals)
	}
}

func TestHeaderOpsAndResponseActions(t *testing.T) {
	e := compile(t, []config.RuleConfig{
		{ID: "h", Priority: 1, Match: config.MatchConfig{PathPrefix: "/", Methods: []string{"GET"}}, Actions: config.ActionsConfig{
			ReqHeaderSet:  map[string]string{"X-Req-Set": "s"},
			ReqHeaderAdd:  map[string]string{"X-Req-Add": "a"},
			ReqHeaderDel:  []string{"X-Del"},
			RespHeaderSet: map[string]string{"X-Resp-Set": "rs"},
			RespHeaderAdd: map[string]string{"X-Resp-Add": "ra"},
			RespHeaderDel: []string{"Server"},
		}},
	})
	r := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	r.Header.Set("X-Del", "gone")
	_, respActions := e.Apply(r)
	if r.Header.Get("X-Req-Set") != "s" || r.Header.Get("X-Req-Add") != "a" || r.Header.Get("X-Del") != "" {
		t.Fatalf("请求头改写失败: %v", r.Header)
	}
	h := http.Header{}
	h.Set("Server", "secret")
	ApplyRespActions(h, respActions)
	if h.Get("X-Resp-Set") != "rs" || h.Get("X-Resp-Add") != "ra" || h.Get("Server") != "" {
		t.Fatalf("响应头动作执行失败: %v", h)
	}
}

func TestMatchAllConditions(t *testing.T) {
	e := compile(t, []config.RuleConfig{
		{ID: "m", Priority: 1, Match: config.MatchConfig{
			PathPrefix: "/secure",
			Methods:    []string{"POST"},
			Headers:    map[string]string{"X-Tenant": "acme"},
		}, Actions: config.ActionsConfig{Upstream: "up"}},
	})
	cases := []struct {
		path, method, tenant string
		want                 bool
	}{
		{"/secure/x", "POST", "acme", true},
		{"/secure/x", "GET", "acme", false},
		{"/secure/x", "POST", "other", false},
		{"/other", "POST", "acme", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, "http://example.com"+c.path, nil)
		if c.tenant != "" {
			r.Header.Set("X-Tenant", c.tenant)
		}
		up, _ := e.Apply(r)
		if (up == "up") != c.want {
			t.Fatalf("path=%s method=%s tenant=%s 命中判定错误: got up=%q wantMatch=%v", c.path, c.method, c.tenant, up, c.want)
		}
	}
}
