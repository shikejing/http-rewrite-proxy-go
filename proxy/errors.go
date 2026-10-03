// Package proxy 实现转发处理器：流式转发、重试、超时、错误分类与访问控制。
package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/url"
)

// Kind 是失败原因分类，用于区分可重试错误与客户端错误。
type Kind int

const (
	KindNoHealthyUpstream Kind = iota
	KindQueueFull
	KindTimeout
	KindUpstreamConnect
	KindUpstreamRead
	KindClientCanceled
	KindBadGateway
)

// Error 是带分类的代理错误。
type Error struct {
	Kind   Kind
	Status int
	Msg    string
	Err    error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Msg, e.Err)
	}
	return e.Msg
}

func (e *Error) Unwrap() error { return e.Err }

// 预定义错误。
var (
	ErrNoHealthyUpstream = &Error{Kind: KindNoHealthyUpstream, Status: 503, Msg: "no healthy upstream"}
	ErrQueueRejected     = &Error{Kind: KindQueueFull, Status: 503, Msg: "upstream queue full"}
	ErrGatewayTimeout    = &Error{Kind: KindTimeout, Status: 504, Msg: "upstream timeout"}
	ErrBadGateway        = &Error{Kind: KindBadGateway, Status: 502, Msg: "bad gateway"}
)

// classify 把底层错误映射为可区分的代理错误。
func classify(err error) *Error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return &Error{Kind: KindClientCanceled, Status: 499, Msg: "client canceled request", Err: err}
	case errors.Is(err, context.DeadlineExceeded):
		return &Error{Kind: KindTimeout, Status: 504, Msg: "upstream timeout", Err: err}
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		return &Error{Kind: KindUpstreamConnect, Status: 502, Msg: "upstream connection error", Err: err}
	}
	return &Error{Kind: KindBadGateway, Status: 502, Msg: "bad gateway", Err: err}
}

// isRetryable 判断错误是否值得重试：仅连接类/超时类错误可重试，客户端取消不可重试。
func isRetryable(e *Error) bool {
	if e == nil {
		return false
	}
	switch e.Kind {
	case KindUpstreamConnect, KindTimeout, KindUpstreamRead:
		return true
	default:
		return false
	}
}
