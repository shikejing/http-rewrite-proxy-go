package rule

import (
	"net/http"
	"sort"
	"strings"

	"github.com/shikejing/http-rewrite-proxy-go/config"
)

// Engine 是已编译的规则集合，按确定顺序对请求求值。
type Engine struct {
	rules []*CompiledRule
}

// CompiledRule 是编译后的规则。
type CompiledRule struct {
	ID       string
	Priority int
	Match    MatchSpec
	Actions  ActionSpec
}

// MatchSpec 是编译后的匹配条件。
type MatchSpec struct {
	PathPrefix string
	Methods    map[string]bool
	Headers    map[string]string
	Host       string
}

// ActionSpec 是编译后的动作。
type ActionSpec struct {
	Upstream        string
	PathPrefix      string
	PathReplacement string
	QuerySet        map[string]string
	QueryAdd        map[string]string
	QueryDel        []string
	ReqHeaderSet    map[string]string
	ReqHeaderAdd    map[string]string
	ReqHeaderDel    []string
	RespHeaderSet   map[string]string
	RespHeaderAdd   map[string]string
	RespHeaderDel   []string
}

// RespAction 是需要在响应上执行的头部动作。
type RespAction struct {
	Op    string // "set" | "add" | "del"
	Key   string
	Value string
}

// Compile 把配置规则编译为引擎。
// 规则按优先级从高到低排序；优先级相同时按 ID 字典序排序，保证生效顺序确定且可解释。
func Compile(cfgs []config.RuleConfig) (*Engine, error) {
	rules := make([]*CompiledRule, 0, len(cfgs))
	for i := range cfgs {
		c := cfgs[i]
		cr := &CompiledRule{
			ID:       c.ID,
			Priority: c.Priority,
			Match: MatchSpec{
				PathPrefix: c.Match.PathPrefix,
				Methods:    make(map[string]bool, len(c.Match.Methods)),
				Headers:    make(map[string]string, len(c.Match.Headers)),
				Host:       c.Match.Host,
			},
			Actions: ActionSpec{
				Upstream:      c.Actions.Upstream,
				QuerySet:      c.Actions.QuerySet,
				QueryAdd:      c.Actions.QueryAdd,
				QueryDel:      c.Actions.QueryDel,
				ReqHeaderSet:  c.Actions.ReqHeaderSet,
				ReqHeaderAdd:  c.Actions.ReqHeaderAdd,
				ReqHeaderDel:  c.Actions.ReqHeaderDel,
				RespHeaderSet: c.Actions.RespHeaderSet,
				RespHeaderAdd: c.Actions.RespHeaderAdd,
				RespHeaderDel: c.Actions.RespHeaderDel,
			},
		}
		for _, m := range c.Match.Methods {
			cr.Match.Methods[m] = true
		}
		for k, v := range c.Match.Headers {
			cr.Match.Headers[k] = v
		}
		if pr := c.Actions.PathPrefixReplace; pr != nil {
			cr.Actions.PathPrefix = pr.Match
			cr.Actions.PathReplacement = pr.Replacement
		}
		rules = append(rules, cr)
	}
	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i].Priority != rules[j].Priority {
			return rules[i].Priority > rules[j].Priority
		}
		return rules[i].ID < rules[j].ID
	})
	return &Engine{rules: rules}, nil
}

// Match 计算请求命中的规则，按优先级从高到低返回。
func (e *Engine) Match(r *http.Request) []*CompiledRule {
	var out []*CompiledRule
	for _, rule := range e.rules {
		if rule.matches(r) {
			out = append(out, rule)
		}
	}
	return out
}

func (rule *CompiledRule) matches(r *http.Request) bool {
	m := rule.Match
	if m.PathPrefix != "" && !strings.HasPrefix(r.URL.Path, m.PathPrefix) {
		return false
	}
	if len(m.Methods) > 0 && !m.Methods[r.Method] {
		return false
	}
	if m.Host != "" && !hostMatches(r.Host, m.Host) {
		return false
	}
	for k, v := range m.Headers {
		if r.Header.Get(k) != v {
			return false
		}
	}
	return true
}

func hostMatches(actual, pattern string) bool {
	if actual == pattern {
		return true
	}
	if h, _, err := splitHostPort(actual); err == nil && h == pattern {
		return true
	}
	return false
}

func splitHostPort(hostport string) (host, port string, err error) {
	// 避免引入 net 包的 SplitHostPort 对无端口串报错的差异，这里简单处理。
	if i := strings.LastIndex(hostport, ":"); i >= 0 && !strings.Contains(hostport[i:], "]") {
		return hostport[:i], hostport[i+1:], nil
	}
	return hostport, "", nil
}

// Apply 对请求依次应用命中规则；返回选中的上游名与响应头动作序列。
// 语义：
//   - 转发目标由第一个指定 upstream 的命中规则决定（高优先级优先）；
//   - 路径/查询/请求头改写按优先级从高到低依次叠加；
//   - 响应头动作按相同顺序收集，在响应返回客户端前依次执行。
func (e *Engine) Apply(r *http.Request) (upstream string, respActions []RespAction) {
	for _, rule := range e.Match(r) {
		a := rule.Actions
		if upstream == "" && a.Upstream != "" {
			upstream = a.Upstream
		}
		if a.PathPrefix != "" && strings.HasPrefix(r.URL.Path, a.PathPrefix) {
			r.URL.Path = a.PathReplacement + r.URL.Path[len(a.PathPrefix):]
			if r.URL.RawPath != "" {
				if strings.HasPrefix(r.URL.RawPath, a.PathPrefix) {
					r.URL.RawPath = a.PathReplacement + r.URL.RawPath[len(a.PathPrefix):]
				} else {
					r.URL.RawPath = ""
				}
			}
		}
		if len(a.QuerySet) > 0 || len(a.QueryAdd) > 0 || len(a.QueryDel) > 0 {
			q := r.URL.Query()
			for _, k := range a.QueryDel {
				q.Del(k)
			}
			for k, v := range a.QuerySet {
				q.Set(k, v)
			}
			for k, v := range a.QueryAdd {
				q.Add(k, v)
			}
			r.URL.RawQuery = q.Encode()
		}
		for _, k := range a.ReqHeaderDel {
			r.Header.Del(k)
		}
		for k, v := range a.ReqHeaderSet {
			r.Header.Set(k, v)
		}
		for k, v := range a.ReqHeaderAdd {
			r.Header.Add(k, v)
		}
		for _, k := range a.RespHeaderDel {
			respActions = append(respActions, RespAction{Op: "del", Key: k})
		}
		for k, v := range a.RespHeaderSet {
			respActions = append(respActions, RespAction{Op: "set", Key: k, Value: v})
		}
		for k, v := range a.RespHeaderAdd {
			respActions = append(respActions, RespAction{Op: "add", Key: k, Value: v})
		}
	}
	return upstream, respActions
}

// ApplyRespActions 在响应头上执行收集到的动作。
func ApplyRespActions(h http.Header, actions []RespAction) {
	for _, a := range actions {
		switch a.Op {
		case "set":
			h.Set(a.Key, a.Value)
		case "add":
			h.Add(a.Key, a.Value)
		case "del":
			h.Del(a.Key)
		}
	}
}
