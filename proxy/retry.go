package proxy

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// ErrOverallTimeout 表示总超时（含重试）到期。
var ErrOverallTimeout = errors.New("overall timeout exceeded")

// ErrRetriesExhausted 表示重试次数耗尽，上游仍返回 5xx。
var ErrRetriesExhausted = errors.New("retries exhausted: upstream still failing")

// replayable 判断请求是否可安全重放（可重试）。
// 仅幂等方法（GET/HEAD/PUT/DELETE/OPTIONS）且无不可重放 body 时可重试；
// 带有 GetBody 的请求（body 可重建）也视为可重放。
func replayable(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	case http.MethodPut, http.MethodDelete:
		// 幂等方法，但 body 必须可重建
		return r.Body == nil || r.GetBody != nil
	default:
		// POST/PATCH 等非幂等方法不重试，避免重复副作用
		return false
	}
}

// doWithRetry 在总超时内按重试策略执行 attempt。
// 超时与重试的先后关系：每次尝试受 PerAttempt 约束，
// 所有尝试共享 Overall 总预算；Overall 到期立即停止，不再发起新尝试。
// attempt 返回 (statusCode, err)；err 非 nil 或状态码为 5xx 且请求可重放时重试。
func doWithRetry(ctx context.Context, retry RetryConfig, overall time.Duration,
	canRetry bool, attempt func(ctx context.Context) (int, error)) (int, error) {

	octx, cancel := context.WithTimeout(ctx, overall)
	defer cancel()

	attempts := retry.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	if !canRetry {
		attempts = 1
	}

	var lastErr error
	var lastStatus int
	for i := 0; i < attempts; i++ {
		if i > 0 && retry.Backoff > 0 {
			select {
			case <-octx.Done():
				return lastStatus, ErrOverallTimeout
			case <-time.After(retry.Backoff):
			}
		}
		status, err := attempt(octx)
		lastStatus, lastErr = status, err
		if err == nil && status < 500 {
			return status, nil
		}
		if errors.Is(err, context.Canceled) {
			return status, err // 客户端取消，立即退出
		}
		if octx.Err() != nil {
			return status, ErrOverallTimeout
		}
	}
	if lastErr == nil && lastStatus >= 500 {
		lastErr = ErrRetriesExhausted
	}
	return lastStatus, lastErr
}
