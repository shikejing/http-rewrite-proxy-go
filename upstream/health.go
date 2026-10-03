package upstream

import (
	"context"
	"net/http"
	"net/url"
	"time"
)

// StartHealthChecks 启动周期性健康检查。
func (p *Pool) StartHealthChecks(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		interval := time.Duration(p.healthCfg.IntervalMS) * time.Millisecond
		if interval <= 0 {
			interval = 5 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		p.checkOnce(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			case <-ticker.C:
				p.checkOnce(ctx)
			}
		}
	}()
}

// checkOnce 对所有实例执行一次健康检查。
func (p *Pool) checkOnce(ctx context.Context) {
	state := p.state.Load()
	timeout := time.Duration(p.healthCfg.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	path := p.healthCfg.Path
	if path == "" {
		path = "/"
	}
	for _, inst := range state.instances {
		probeCtx, cancel := context.WithTimeout(ctx, timeout)
		ok := inst.probe(probeCtx, p.client, path)
		cancel()
		p.MarkResult(inst, ok)
	}
}

// probe 探测单个实例：GET healthPath，2xx 视为成功。
func (inst *Instance) probe(ctx context.Context, client *http.Client, healthPath string) bool {
	target := inst.URL.ResolveReference(&url.URL{Path: healthPath})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	// 排空连接以便复用。
	buf := make([]byte, 1024)
	for {
		n, rerr := resp.Body.Read(buf)
		_ = n
		if rerr != nil {
			break
		}
	}
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
