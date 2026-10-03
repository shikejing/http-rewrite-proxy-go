package proxy

import (
	"net/http"
	"sort"
	"strings"
)

// Rule 是编译后的单条规则。
type Rule struct {
	cfg RuleConfig
}

// Name 返回规则名。
func (r *Rule) Name() string { return r.cfg.Name }

// Upstream 返回规则指向的上游集群名。
func (r *Rule) Upstream() string { return r.cfg.Upstream }

// RuleSet 是按优先级排序后的规则集合，规则视图不可变。
//
// 生效顺序（确定且可解释）：
//  1. Priority 数值小者优先；
//  2. Priority 相同时，PathPrefix 更长（更具体）者优先；
//  3. 仍相同时，按规则名字典序，保证结果确定。
type RuleSet struct {
	rules []*Rule
}

// NewRuleSet 编译并排序规则。
func NewRuleSet(cfgs []RuleConfig) (*RuleSet, error) {
	rules := make([]*Rule, 0, len(cfgs))
	for _, c := range cfgs {
		rules = append(rules, &Rule{cfg: c})
	}
	sort.SliceStable(rules, func(i, j int) bool {
		a, b := rules[i].cfg, rules[j].cfg
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		if len(a.PathPrefix) != len(b.PathPrefix) {
			return len(a.PathPrefix) > len(b.PathPrefix)
		}
		return a.Name < b.Name
	})
	return &RuleSet{rules: rules}, nil
}

// Match 返回命中规则中优先级最高的一条；未命中返回 nil。
func (rs *RuleSet) Match(path string) *Rule {
	for _, r := range rs.rules {
		if strings.HasPrefix(path, r.cfg.PathPrefix) {
			return r
		}
	}
	return nil
}

// ApplyRequest 对出站请求应用路径前缀替换、查询参数增删、请求头改写。
func (r *Rule) ApplyRequest(req *http.Request) {
	c := r.cfg
	if c.ReplacePath != "" {
		req.URL.Path = c.ReplacePath + strings.TrimPrefix(req.URL.Path, c.PathPrefix)
	}
	q := req.URL.Query()
	for _, k := range c.RemoveQuery {
		q.Del(k)
	}
	for k, v := range c.AddQuery {
		q.Set(k, v)
	}
	req.URL.RawQuery = q.Encode()
	applyHeaderOps(req.Header, c.RequestHeaders)
}

// ApplyResponseHeaders 对响应头应用改写。
func (r *Rule) ApplyResponseHeaders(h http.Header) {
	applyHeaderOps(h, r.cfg.ResponseHeaders)
}

func applyHeaderOps(h http.Header, ops HeaderOps) {
	for _, k := range ops.Remove {
		h.Del(k)
	}
	for k, v := range ops.Set {
		h.Set(k, v)
	}
}

// hasRewrite 报告规则是否包含任何改写动作（用于签名请求保护判定）。
func (r *Rule) hasRewrite() bool {
	c := r.cfg
	return c.ReplacePath != "" ||
		len(c.AddQuery) > 0 || len(c.RemoveQuery) > 0 ||
		len(c.RequestHeaders.Set) > 0 || len(c.RequestHeaders.Remove) > 0
}
