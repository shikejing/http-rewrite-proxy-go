package limiter

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUnlimited(t *testing.T) {
	l := New(0, 0)
	if l.Available() != -1 {
		t.Fatalf("无限制时 Available 应为 -1")
	}
	for i := 0; i < 100; i++ {
		release, err := l.Acquire(context.Background())
		if err != nil {
			t.Fatalf("无限制不应失败: %v", err)
		}
		release()
	}
}

func TestAcquireRelease(t *testing.T) {
	l := New(2, 0)
	r1, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r2, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if l.Available() != 0 {
		t.Fatalf("名额应已满")
	}
	// 不排队：立即拒绝。
	if _, err := l.Acquire(context.Background()); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("名额已满且不排队时应返回 ErrQueueFull, 得到 %v", err)
	}
	r1()
	if l.Available() != 1 {
		t.Fatalf("释放后应有 1 个名额")
	}
	r2()
	if l.Available() != 2 {
		t.Fatalf("全部释放后应有 2 个名额")
	}
}

func TestQueueTimeoutReject(t *testing.T) {
	l := New(1, 50*time.Millisecond)
	release, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	start := time.Now()
	_, err = l.Acquire(context.Background())
	elapsed := time.Since(start)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("排队超时应返回 ErrQueueFull, 得到 %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("不应在排队超时前拒绝: %v", elapsed)
	}
}

func TestQueueAcquireAfterRelease(t *testing.T) {
	l := New(1, 2*time.Second)
	release, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(30 * time.Millisecond)
		release()
	}()
	r2, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatalf("排队后应能取得名额: %v", err)
	}
	r2()
}

func TestContextCanceledWhileQueued(t *testing.T) {
	l := New(1, 2*time.Second)
	release, err := l.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := l.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("排队中取消应返回 ctx.Err(), 得到 %v", err)
	}
}
