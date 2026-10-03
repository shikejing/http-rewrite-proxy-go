package proxy

import (
	"fmt"
	"strings"
	"time"
)

// Config 是整个转发层的完整配置视图。
type Config struct {
	Rules       []RuleConfig      `json:"rules"`
	Upstreams   []UpstreamConfig  `json:"upstreams"`
	Retry       RetryConfig       `json:"retry"`
	Timeouts    TimeoutConfig     `json:"timeouts"`
	Auth        AuthConfig        `json:"auth"`
	Limits      LimitConfig       `json:"limits"`
	HealthCheck HealthCheckConfig `json:"health_check"`
}

// RuleConfig 描述一条匹配+改写规则。
type RuleConfig struct {
	Name            string            `json:"name"`
	Priority        int               `json:"priority"` // 数值小者优先
	PathPrefix      string            `json:"path_prefix"`
	ReplacePath     string            `json:"replace_path"`
	Upstream        string            `json:"upstream"`
	AddQuery        map[string]string `json:"add_query"`
	RemoveQuery     []string          `json:"remove_query"`
	RequestHeaders  HeaderOps         `json:"request_headers"`
	ResponseHeaders HeaderOps         `json:"response_headers"`
}

// HeaderOps 描述一组头部改写操作。
type HeaderOps struct {
	Set    map[string]string `json:"set"`
	Remove []string          `json:"remove"`
}

// UpstreamConfig 描述一个上游集群。
type UpstreamConfig struct {
	Name      string   `json:"name"`
	Endpoints []string `json:"endpoints"` // 例如 "http://127.0.0.1:9001"
}

// RetryConfig 描述重试策略。
type RetryConfig struct {
	MaxAttempts int           `json:"max_attempts"`
	Backoff     time.Duration `json:"backoff"`
}

// TimeoutConfig 描述超时策略。
type TimeoutConfig struct {
	PerAttempt time.Duration `json:"per_attempt"` // 单次尝试超时
	Overall    time.Duration `json:"overall"`     // 含重试的总超时
}

// AuthConfig 描述身份透传与访问控制。
type AuthConfig struct {
	StripHeaders  []string          `json:"strip_headers"`  // 转发前剥离的敏感头
	InjectHeaders map[string]string `json:"inject_headers"` // 转发前注入的头
	// SignedHeaders 命中这些头的请求视为带签名请求，禁止改写。
	SignedHeaders []string `json:"signed_headers"`
	// AllowRewriteSigned 为 true 时允许对带签名请求做改写（默认拒绝）。
	AllowRewriteSigned bool `json:"allow_rewrite_signed"`
}

// LimitConfig 描述并发与连接上限。
type LimitConfig struct {
	MaxConcurrent int64 `json:"max_concurrent"` // 全局并发上限，0 表示不限
	QueueLimit    int   `json:"queue_limit"`    // 达到上限时允许排队数，超出直接拒绝
}

// HealthCheckConfig 描述健康检查策略。
type HealthCheckConfig struct {
	Interval         time.Duration `json:"interval"`
	Timeout          time.Duration `json:"timeout"`
	FailThreshold    int           `json:"fail_threshold"`    // 连续失败多少次摘除
	RecoverThreshold int           `json:"recover_threshold"` // 连续成功多少次恢复
}

// setDefaults 填充默认值，返回副本。
func (c *Config) setDefaults() {
	if c.Retry.MaxAttempts <= 0 {
		c.Retry.MaxAttempts = 1
	}
	if c.Timeouts.PerAttempt <= 0 {
		c.Timeouts.PerAttempt = 10 * time.Second
	}
	if c.Timeouts.Overall <= 0 {
		c.Timeouts.Overall = 30 * time.Second
	}
	if c.HealthCheck.Interval <= 0 {
		c.HealthCheck.Interval = 2 * time.Second
	}
	if c.HealthCheck.Timeout <= 0 {
		c.HealthCheck.Timeout = time.Second
	}
	if c.HealthCheck.FailThreshold <= 0 {
		c.HealthCheck.FailThreshold = 2
	}
	if c.HealthCheck.RecoverThreshold <= 0 {
		c.HealthCheck.RecoverThreshold = 1
	}
}

// Validate 校验配置合法性。
func (c *Config) Validate() error {
	upstreams := map[string]bool{}
	for _, u := range c.Upstreams {
		if u.Name == "" {
			return fmt.Errorf("upstream: name 不能为空")
		}
		if upstreams[u.Name] {
			return fmt.Errorf("upstream %q 重复定义", u.Name)
		}
		if len(u.Endpoints) == 0 {
			return fmt.Errorf("upstream %q 至少需要一个 endpoint", u.Name)
		}
		for _, e := range u.Endpoints {
			if !strings.HasPrefix(e, "http://") && !strings.HasPrefix(e, "https://") {
				return fmt.Errorf("upstream %q 的 endpoint %q 必须以 http(s):// 开头", u.Name, e)
			}
		}
		upstreams[u.Name] = true
	}
	names := map[string]bool{}
	for i, r := range c.Rules {
		if r.Name == "" {
			return fmt.Errorf("rule[%d]: name 不能为空", i)
		}
		if names[r.Name] {
			return fmt.Errorf("rule %q 重复定义", r.Name)
		}
		names[r.Name] = true
		if r.PathPrefix == "" || !strings.HasPrefix(r.PathPrefix, "/") {
			return fmt.Errorf("rule %q: path_prefix 必须以 / 开头", r.Name)
		}
		if r.ReplacePath != "" && !strings.HasPrefix(r.ReplacePath, "/") {
			return fmt.Errorf("rule %q: replace_path 必须以 / 开头", r.Name)
		}
		if r.Upstream == "" {
			return fmt.Errorf("rule %q: 必须指定 upstream", r.Name)
		}
		if !upstreams[r.Upstream] {
			return fmt.Errorf("rule %q 引用了不存在的 upstream %q", r.Name, r.Upstream)
		}
	}
	if c.Retry.MaxAttempts < 0 {
		return fmt.Errorf("retry.max_attempts 不能为负")
	}
	if c.Limits.MaxConcurrent < 0 || c.Limits.QueueLimit < 0 {
		return fmt.Errorf("limits 不能为负")
	}
	return nil
}
