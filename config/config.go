package config

import (
	"fmt"
	"net/url"
	"strings"
)

// Config 是转发代理的完整配置视图。
type Config struct {
	Upstreams []UpstreamConfig `json:"upstreams"`
	Rules     []RuleConfig     `json:"rules"`
	Settings  Settings         `json:"settings"`
	ACL       ACLConfig        `json:"acl"`
}

// UpstreamConfig 定义一个上游集群（池）。
type UpstreamConfig struct {
	Name           string       `json:"name"`
	URLs           []string     `json:"urls"`
	Health         HealthConfig `json:"health"`
	MaxConns       int          `json:"max_conns"`
	QueueTimeoutMS int          `json:"queue_timeout_ms"`
}

// HealthConfig 定义健康检查参数。
type HealthConfig struct {
	Path               string `json:"path"`
	IntervalMS         int    `json:"interval_ms"`
	TimeoutMS          int    `json:"timeout_ms"`
	HealthyThreshold   int    `json:"healthy_threshold"`
	UnhealthyThreshold int    `json:"unhealthy_threshold"`
}

// RuleConfig 定义一条改写规则。
type RuleConfig struct {
	ID       string        `json:"id"`
	Priority int           `json:"priority"`
	Match    MatchConfig   `json:"match"`
	Actions  ActionsConfig `json:"actions"`
}

// MatchConfig 定义规则匹配条件（各条件之间为“与”关系）。
type MatchConfig struct {
	PathPrefix string            `json:"path_prefix"`
	Methods    []string          `json:"methods"`
	Headers    map[string]string `json:"headers"`
	Host       string            `json:"host"`
}

// ActionsConfig 定义命中后执行的改写动作。
type ActionsConfig struct {
	Upstream          string             `json:"upstream"`
	PathPrefixReplace *PathReplaceConfig `json:"path_prefix_replace"`
	QuerySet          map[string]string  `json:"query_set"`
	QueryAdd          map[string]string  `json:"query_add"`
	QueryDel          []string           `json:"query_del"`
	ReqHeaderSet      map[string]string  `json:"req_header_set"`
	ReqHeaderAdd      map[string]string  `json:"req_header_add"`
	ReqHeaderDel      []string           `json:"req_header_del"`
	RespHeaderSet     map[string]string  `json:"resp_header_set"`
	RespHeaderAdd     map[string]string  `json:"resp_header_add"`
	RespHeaderDel     []string           `json:"resp_header_del"`
}

// PathReplaceConfig 定义路径前缀替换。
type PathReplaceConfig struct {
	Match       string `json:"match"`
	Replacement string `json:"replacement"`
}

// Settings 定义全局转发参数。
type Settings struct {
	DefaultUpstream     string `json:"default_upstream"`
	AttemptTimeoutMS    int    `json:"attempt_timeout_ms"`
	OverallTimeoutMS    int    `json:"overall_timeout_ms"`
	MaxRetries          int    `json:"max_retries"`
	RetryBackoffMS      int    `json:"retry_backoff_ms"`
	FlushIntervalMS     int    `json:"flush_interval_ms"`
	MaxIdleConns        int    `json:"max_idle_conns"`
	MaxIdleConnsPerHost int    `json:"max_idle_conns_per_host"`
	IdleConnTimeoutMS   int    `json:"idle_conn_timeout_ms"`
}

// ACLConfig 定义身份透传与访问控制。
type ACLConfig struct {
	ProtectedHeaders []string          `json:"protected_headers"`
	StripReqHeaders  []string          `json:"strip_req_headers"`
	InjectReqHeaders map[string]string `json:"inject_req_headers"`
	StripRespHeaders []string          `json:"strip_resp_headers"`
}

// Default 返回内置默认配置。
func Default() *Config {
	return &Config{
		Settings: Settings{
			AttemptTimeoutMS:    30000,
			OverallTimeoutMS:    60000,
			MaxRetries:          2,
			RetryBackoffMS:      10,
			FlushIntervalMS:     0,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeoutMS:   90000,
		},
		ACL: ACLConfig{
			ProtectedHeaders: []string{"Authorization", "Proxy-Authorization"},
		},
	}
}

// Validate 校验配置的自洽性，返回的错误必须可解释。
func (c *Config) Validate() error {
	seenUpstream := make(map[string]bool)
	for i, u := range c.Upstreams {
		if strings.TrimSpace(u.Name) == "" {
			return fmt.Errorf("upstreams[%d]: name 不能为空", i)
		}
		if seenUpstream[u.Name] {
			return fmt.Errorf("upstreams[%d]: 上游名 %q 重复", i, u.Name)
		}
		seenUpstream[u.Name] = true
		if len(u.URLs) == 0 {
			return fmt.Errorf("upstreams[%d] %q: urls 不能为空", i, u.Name)
		}
		for j, raw := range u.URLs {
			pu, err := url.Parse(raw)
			if err != nil {
				return fmt.Errorf("upstreams[%d] %q: urls[%d] 非法: %w", i, u.Name, j, err)
			}
			if pu.Scheme != "http" && pu.Scheme != "https" {
				return fmt.Errorf("upstreams[%d] %q: urls[%d] 协议必须是 http/https: %q", i, u.Name, j, raw)
			}
			if pu.Host == "" {
				return fmt.Errorf("upstreams[%d] %q: urls[%d] 缺少 host: %q", i, u.Name, j, raw)
			}
		}
		if u.MaxConns < 0 {
			return fmt.Errorf("upstreams[%d] %q: max_conns 不能为负", i, u.Name)
		}
		if u.QueueTimeoutMS < 0 {
			return fmt.Errorf("upstreams[%d] %q: queue_timeout_ms 不能为负", i, u.Name)
		}
		if u.Health.IntervalMS < 0 || u.Health.TimeoutMS < 0 {
			return fmt.Errorf("upstreams[%d] %q: 健康检查间隔/超时不能为负", i, u.Name)
		}
		if u.Health.HealthyThreshold < 0 || u.Health.UnhealthyThreshold < 0 {
			return fmt.Errorf("upstreams[%d] %q: 健康阈值不能为负", i, u.Name)
		}
	}
	if c.Settings.DefaultUpstream != "" && !seenUpstream[c.Settings.DefaultUpstream] {
		return fmt.Errorf("settings.default_upstream %q 不存在", c.Settings.DefaultUpstream)
	}
	if len(c.Upstreams) > 0 && c.Settings.DefaultUpstream == "" {
		return fmt.Errorf("存在上游时 settings.default_upstream 不能为空")
	}
	if c.Settings.AttemptTimeoutMS < 0 || c.Settings.OverallTimeoutMS < 0 {
		return fmt.Errorf("settings: 超时不能为负")
	}
	if c.Settings.MaxRetries < 0 {
		return fmt.Errorf("settings: max_retries 不能为负")
	}
	if c.Settings.RetryBackoffMS < 0 || c.Settings.FlushIntervalMS < 0 {
		return fmt.Errorf("settings: retry_backoff_ms/flush_interval_ms 不能为负")
	}
	if c.Settings.MaxIdleConns < 0 || c.Settings.MaxIdleConnsPerHost < 0 || c.Settings.IdleConnTimeoutMS < 0 {
		return fmt.Errorf("settings: 连接池参数不能为负")
	}
	seenRule := make(map[string]bool)
	for i, r := range c.Rules {
		if strings.TrimSpace(r.ID) == "" {
			return fmt.Errorf("rules[%d]: id 不能为空", i)
		}
		if seenRule[r.ID] {
			return fmt.Errorf("rules[%d]: 规则 id %q 重复", i, r.ID)
		}
		seenRule[r.ID] = true
		if strings.TrimSpace(r.Match.PathPrefix) == "" {
			return fmt.Errorf("rules[%d] %q: match.path_prefix 不能为空", i, r.ID)
		}
		for _, m := range r.Match.Methods {
			if strings.ToUpper(m) != m || !validToken(m) {
				return fmt.Errorf("rules[%d] %q: method %q 非法（需大写 HTTP 方法）", i, r.ID, m)
			}
		}
		for k := range r.Match.Headers {
			if !validHeaderName(k) {
				return fmt.Errorf("rules[%d] %q: match.headers 键 %q 非法", i, r.ID, k)
			}
		}
		if r.Actions.Upstream != "" && !seenUpstream[r.Actions.Upstream] {
			return fmt.Errorf("rules[%d] %q: actions.upstream %q 不存在", i, r.ID, r.Actions.Upstream)
		}
		if pr := r.Actions.PathPrefixReplace; pr != nil {
			if strings.TrimSpace(pr.Match) == "" {
				return fmt.Errorf("rules[%d] %q: path_prefix_replace.match 不能为空", i, r.ID)
			}
		}
		for _, m := range []map[string]string{r.Actions.QuerySet, r.Actions.QueryAdd, r.Actions.ReqHeaderSet, r.Actions.ReqHeaderAdd, r.Actions.RespHeaderSet, r.Actions.RespHeaderAdd, c.ACL.InjectReqHeaders} {
			for k := range m {
				if !validKey(k) {
					return fmt.Errorf("rules[%d] %q: 动作键 %q 非法", i, r.ID, k)
				}
			}
		}
		for _, m := range [][]string{r.Actions.QueryDel, r.Actions.ReqHeaderDel, r.Actions.RespHeaderDel, c.ACL.ProtectedHeaders, c.ACL.StripReqHeaders, c.ACL.StripRespHeaders} {
			for _, k := range m {
				if !validKey(k) {
					return fmt.Errorf("rules[%d] %q: 删除键 %q 非法", i, r.ID, k)
				}
			}
		}
	}
	return nil
}

func validKey(k string) bool {
	return strings.TrimSpace(k) != "" && !strings.ContainsAny(k, " \t\r\n:")
}

func validHeaderName(k string) bool {
	return validKey(k)
}

func validToken(t string) bool {
	if t == "" {
		return false
	}
	for _, c := range t {
		if !(c >= 'A' && c <= 'Z') {
			return false
		}
	}
	return true
}
