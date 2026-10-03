package config

import "testing"

func validConfig() *Config {
	return &Config{
		Upstreams: []UpstreamConfig{{
			Name: "default",
			URLs: []string{"http://127.0.0.1:8080"},
		}},
		Settings: Settings{DefaultUpstream: "default"},
	}
}

func TestValidateOK(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("合法配置不应报错: %v", err)
	}
}

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{"上游名为空", func(c *Config) { c.Upstreams[0].Name = "" }},
		{"上游名重复", func(c *Config) { c.Upstreams = append(c.Upstreams, UpstreamConfig{Name: "default", URLs: []string{"http://x"}}) }},
		{"URL 为空", func(c *Config) { c.Upstreams[0].URLs = nil }},
		{"URL 协议非法", func(c *Config) { c.Upstreams[0].URLs = []string{"ftp://x"} }},
		{"URL 缺 host", func(c *Config) { c.Upstreams[0].URLs = []string{"http://"} }},
		{"max_conns 为负", func(c *Config) { c.Upstreams[0].MaxConns = -1 }},
		{"默认上游不存在", func(c *Config) { c.Settings.DefaultUpstream = "nope" }},
		{"规则 id 为空", func(c *Config) { c.Rules = []RuleConfig{{ID: "", Match: MatchConfig{PathPrefix: "/"}}} }},
		{"规则 id 重复", func(c *Config) {
			c.Rules = []RuleConfig{
				{ID: "a", Match: MatchConfig{PathPrefix: "/"}},
				{ID: "a", Match: MatchConfig{PathPrefix: "/"}},
			}
		}},
		{"匹配前缀为空", func(c *Config) { c.Rules = []RuleConfig{{ID: "a", Match: MatchConfig{PathPrefix: ""}}} }},
		{"方法非法", func(c *Config) { c.Rules = []RuleConfig{{ID: "a", Match: MatchConfig{PathPrefix: "/", Methods: []string{"get"}}}} }},
		{"动作上游不存在", func(c *Config) {
			c.Rules = []RuleConfig{{ID: "a", Match: MatchConfig{PathPrefix: "/"}, Actions: ActionsConfig{Upstream: "nope"}}}
		}},
		{"超时为负", func(c *Config) { c.Settings.AttemptTimeoutMS = -1 }},
		{"重试为负", func(c *Config) { c.Settings.MaxRetries = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mut(c)
			if err := c.Validate(); err == nil {
				t.Fatalf("应返回校验错误")
			}
		})
	}
}

func TestDefaultHasProtectedHeaders(t *testing.T) {
	d := Default()
	if err := d.Validate(); err != nil {
		t.Fatalf("默认配置应合法: %v", err)
	}
	if len(d.ACL.ProtectedHeaders) == 0 {
		t.Fatal("默认应包含受保护头")
	}
}
