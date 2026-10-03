package proxy

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/shikejing/http-rewrite-proxy-go/rule"
	"github.com/shikejing/http-rewrite-proxy-go/upstream"
)

// forwardWithRetry 执行转发，按策略重试。
// 超时先后关系：整体截止时间（overall）是外层硬约束，单次尝试超时（attempt）是内层约束；
// 仅可安全重放的请求（无请求体的幂等方法）才会重试，重试不会重复产生副作用。
// 上下文生命周期：尝试超时与整体截止都绑定到响应体关闭，确保流式转发期间不被提前取消。
func (h *Handler) forwardWithRetry(r *http.Request, pool *upstream.Pool, respActions []rule.RespAction) (*http.Response, *Error) {
	view := h.views.View()
	settings := view.Config.Settings

	maxAttempts := settings.MaxRetries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	replayable := isReplayable(r)

	ctx := r.Context()
	var overallCancel context.CancelFunc
	if settings.OverallTimeoutMS > 0 {
		ctx, overallCancel = context.WithTimeout(ctx, time.Duration(settings.OverallTimeoutMS)*time.Millisecond)
	}
	// cancelAll 在确定不再返回响应体时调用；成功路径下由响应体 Close 触发。
	cancelAll := func() {
		if overallCancel != nil {
			overallCancel()
		}
	}

	var lastErr *Error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			if !replayable {
				break
			}
			backoff := replayBackoff(attempt, time.Duration(settings.RetryBackoffMS)*time.Millisecond)
			select {
			case <-ctx.Done():
				cancelAll()
				return nil, classify(ctx.Err())
			case <-time.After(backoff):
			}
		}

		inst := pool.Select()
		if inst == nil {
			cancelAll()
			return nil, ErrNoHealthyUpstream
		}
		inst.AddInflight(1)

		actx := ctx
		var acancel context.CancelFunc
		if settings.AttemptTimeoutMS > 0 {
			actx, acancel = context.WithTimeout(ctx, time.Duration(settings.AttemptTimeoutMS)*time.Millisecond)
		}
		req := r.Clone(actx)
		req.RequestURI = "" // 服务端请求携带的 RequestURI 不能用于客户端请求
		req.URL.Scheme = inst.URL.Scheme
		req.URL.Host = inst.URL.Host
		req.Host = "" // Host 头由 URL 重新推导，X-Forwarded-Host 保留原始 Host

		resp, err := pool.Client().Do(req)
		inst.AddInflight(-1)

		if err == nil && resp.StatusCode < 500 {
			// 成功：上下文绑定到响应体关闭，流式读取期间不取消。
			if acancel != nil || overallCancel != nil {
				resp.Body = &bodyCancelOnClose{ReadCloser: resp.Body, cancel: func() {
					if acancel != nil {
						acancel()
					}
					cancelAll()
				}}
			}
			pool.MarkResult(inst, true)
			return resp, nil
		}

		// 失败：立即清理本次尝试上下文。
		if acancel != nil {
			acancel()
		}
		if err != nil {
			lastErr = classify(err)
		} else {
			lastErr = &Error{
				Kind:   KindUpstreamConnect,
				Status: 502,
				Msg:    fmt.Sprintf("upstream returned status %d", resp.StatusCode),
			}
			resp.Body.Close()
		}

		if lastErr.Kind == KindClientCanceled {
			cancelAll()
			return nil, lastErr
		}
		if !replayable {
			// 不可重试请求只尝试一次，失败即反馈。
			pool.MarkResult(inst, false)
			cancelAll()
			return nil, lastErr
		}
		if attempt == maxAttempts-1 {
			// 重试用尽才反馈失败，避免中途摘除导致返回 503 而非真实原因。
			pool.MarkResult(inst, false)
		}
	}
	cancelAll()
	if lastErr == nil {
		lastErr = ErrBadGateway
	}
	return nil, lastErr
}

// bodyCancelOnClose 在响应体关闭时同时取消关联的上下文，释放上游资源。
// 若底层 body 支持写（如 101 升级后的连接），Write 一并透传。
type bodyCancelOnClose struct {
	io.ReadCloser
	cancel func()
}

func (b *bodyCancelOnClose) Write(p []byte) (int, error) {
	if w, ok := b.ReadCloser.(io.Writer); ok {
		return w.Write(p)
	}
	return 0, io.ErrShortWrite
}

func (b *bodyCancelOnClose) Close() error {
	err := b.ReadCloser.Close()
	if b.cancel != nil {
		b.cancel()
	}
	return err
}

// isReplayable 判断请求是否可安全重放：无请求体且方法属于幂等安全集合。
func isReplayable(r *http.Request) bool {
	if r.Body != nil && r.Body != http.NoBody {
		return false
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	}
	return false
}

// replayBackoff 返回重试退避时间（指数退避，上限 5s）。
func replayBackoff(attempt int, base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	d := base * time.Duration(math.Pow(2, float64(attempt-1)))
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}
