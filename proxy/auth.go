package proxy

import (
	"errors"
	"net/http"
)

// ErrSignedRewriteDenied 表示对带签名请求的改写被拒绝。
var ErrSignedRewriteDenied = errors.New("rewrite denied for signed request")

// isSigned 判断请求是否携带任一签名头。
func isSigned(r *http.Request, signedHeaders []string) bool {
	for _, h := range signedHeaders {
		if r.Header.Get(h) != "" {
			return true
		}
	}
	return false
}

// applyAuth 执行敏感头剥离与注入。
// willRewrite 为 true 表示本请求将被规则改写；
// 对带签名请求的越权改写返回 ErrSignedRewriteDenied。
func applyAuth(r *http.Request, cfg AuthConfig, willRewrite bool) error {
	if willRewrite && isSigned(r, cfg.SignedHeaders) && !cfg.AllowRewriteSigned {
		return ErrSignedRewriteDenied
	}
	for _, h := range cfg.StripHeaders {
		r.Header.Del(h)
	}
	for k, v := range cfg.InjectHeaders {
		r.Header.Set(k, v)
	}
	return nil
}
