// Package limiter 实现上游并发上限：达到上限后按确定策略排队，超时拒绝。
package limiter

import (
	"context"
	"errors"
	"time"
)

// ErrQueueFull 表示排队等待超过上限时间，请求被拒绝。
var ErrQueueFull = errors.New("limiter: queue timeout, request rejected")

// token 是信号量占位。
type token struct{}

// Limiter 用带缓冲通道实现并发信号量。
type Limiter struct {
	sem          chan token
	queueTimeout time.Duration
}

// New 创建限流器。maxConns<=0 表示不限制；queueTimeout<=0 表示不排队（立即拒绝）。
func New(maxConns int, queueTimeout time.Duration) *Limiter {
	l := &Limiter{queueTimeout: queueTimeout}
	if maxConns > 0 {
		l.sem = make(chan token, maxConns)
	}
	return l
}

// Acquire 获取一个并发名额；返回 release 释放名额。
// 行为确定：先尝试立即获取；名额已满时进入排队，排队超时返回 ErrQueueFull，
// 调用方上下文取消则返回 ctx.Err()。
func (l *Limiter) Acquire(ctx context.Context) (release func(), err error) {
	if l.sem == nil {
		return func() {}, nil
	}
	select {
	case l.sem <- token{}:
		return l.release, nil
	default:
	}
	if l.queueTimeout <= 0 {
		return nil, ErrQueueFull
	}
	timer := time.NewTimer(l.queueTimeout)
	defer timer.Stop()
	select {
	case l.sem <- token{}:
		return l.release, nil
	case <-timer.C:
		return nil, ErrQueueFull
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *Limiter) release() {
	select {
	case <-l.sem:
	default:
	}
}

// Available 返回当前空闲名额（用于观测）。
func (l *Limiter) Available() int {
	if l.sem == nil {
		return -1
	}
	return cap(l.sem) - len(l.sem)
}
