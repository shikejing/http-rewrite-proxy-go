package proxy

import (
	"fmt"
	"net"
	"net/http"
	"net/textproto"
	"strings"

	"github.com/shikejing/http-rewrite-proxy-go/config"
)

// ACL 负责身份透传与访问控制：剥离伪造身份头、注入可信头、保护敏感头不被改写。
type ACL struct {
	protected map[string]bool
	stripReq  map[string]bool
	injectReq map[string]string
	stripResp map[string]bool
}

// builtinProtected 是无论配置如何都受保护的敏感头底线。
var builtinProtected = []string{"Authorization", "Proxy-Authorization"}

// NewACL 从配置构建 ACL。
func NewACL(cfg config.ACLConfig) *ACL {
	a := &ACL{
		protected: make(map[string]bool, len(cfg.ProtectedHeaders)+len(builtinProtected)),
		stripReq:  make(map[string]bool, len(cfg.StripReqHeaders)),
		injectReq: make(map[string]string, len(cfg.InjectReqHeaders)),
		stripResp: make(map[string]bool, len(cfg.StripRespHeaders)),
	}
	for _, h := range builtinProtected {
		a.protected[textproto.CanonicalMIMEHeaderKey(h)] = true
	}
	for _, h := range cfg.ProtectedHeaders {
		a.protected[textproto.CanonicalMIMEHeaderKey(h)] = true
	}
	for _, h := range cfg.StripReqHeaders {
		a.stripReq[textproto.CanonicalMIMEHeaderKey(h)] = true
	}
	for k, v := range cfg.InjectReqHeaders {
		a.injectReq[textproto.CanonicalMIMEHeaderKey(k)] = v
	}
	for _, h := range cfg.StripRespHeaders {
		a.stripResp[textproto.CanonicalMIMEHeaderKey(h)] = true
	}
	return a
}

// PrepareRequest 在规则改写前处理请求头：剥离不可信头、注入身份头、追加 X-Forwarded-*。
func (a *ACL) PrepareRequest(r *http.Request) {
	for h := range a.stripReq {
		r.Header.Del(h)
	}
	for h, v := range a.injectReq {
		r.Header.Set(h, v)
	}
	a.applyForwarded(r)
}

// applyForwarded 追加标准转发身份头，保证上游能看到真实客户端信息。
func (a *ACL) applyForwarded(r *http.Request) {
	clientIP := r.RemoteAddr
	if host, _, err := net.SplitHostPort(clientIP); err == nil {
		clientIP = host
	}
	if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
		clientIP = prior + ", " + clientIP
	}
	r.Header.Set("X-Forwarded-For", clientIP)
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	r.Header.Set("X-Forwarded-Proto", scheme)
	r.Header.Set("X-Forwarded-Host", r.Host)
}

// PrepareResponse 在响应返回客户端前剥离敏感响应头。
func (a *ACL) PrepareResponse(h http.Header) {
	for name := range a.stripResp {
		h.Del(name)
	}
}

// ValidateRules 拒绝任何针对受保护头的改写动作（越权改写必须在配置期失败）。
// 匹配动作（只读）不受此限。
func (a *ACL) ValidateRules(rules []config.RuleConfig) error {
	for _, rule := range rules {
		for _, m := range []map[string]string{rule.Actions.ReqHeaderSet, rule.Actions.ReqHeaderAdd} {
			for k := range m {
				if a.protected[textproto.CanonicalMIMEHeaderKey(k)] {
					return fmt.Errorf("rule %q: 禁止改写受保护请求头 %q", rule.ID, k)
				}
			}
		}
		for _, k := range rule.Actions.ReqHeaderDel {
			if a.protected[textproto.CanonicalMIMEHeaderKey(k)] {
				return fmt.Errorf("rule %q: 禁止删除受保护请求头 %q", rule.ID, k)
			}
		}
	}
	return nil
}

// IsProtected 判断请求头是否受保护（用于测试与审计）。
func (a *ACL) IsProtected(header string) bool {
	return a.protected[textproto.CanonicalMIMEHeaderKey(header)]
}

var _ = strings.TrimSpace
