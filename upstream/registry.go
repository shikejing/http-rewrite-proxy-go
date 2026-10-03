package upstream

import (
	"fmt"
	"sync"

	"github.com/shikejing/http-rewrite-proxy-go/config"
)

// Registry 管理所有上游池，配置热更新时整体应用。
type Registry struct {
	mu    sync.Mutex
	pools map[string]*Pool
}

// NewRegistry 创建池注册表。
func NewRegistry() *Registry {
	return &Registry{pools: map[string]*Pool{}}
}

// Apply 根据配置创建/更新/移除池。
// 语义：先在锁内构建新池映射（新建池可能失败），成功后整体替换；
// 已存在的池保留其实例与健康状态，被移除的池停止后台任务并关闭连接。
func (r *Registry) Apply(cfgs []config.UpstreamConfig, settings config.Settings) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	next := make(map[string]*Pool, len(cfgs))
	for i := range cfgs {
		c := cfgs[i]
		if existing, ok := r.pools[c.Name]; ok {
			if err := existing.Update(c.URLs, c.MaxConns, c.QueueTimeoutMS); err != nil {
				return fmt.Errorf("更新上游 %q 失败: %w", c.Name, err)
			}
			next[c.Name] = existing
			continue
		}
		pool, err := NewPool(c.Name, c.URLs, c.MaxConns, c.QueueTimeoutMS, settings, c.Health)
		if err != nil {
			return fmt.Errorf("创建上游 %q 失败: %w", c.Name, err)
		}
		pool.StartHealthChecks(nil)
		next[c.Name] = pool
	}
	for name, old := range r.pools {
		if _, kept := next[name]; !kept {
			old.Stop()
		}
	}
	r.pools = next
	return nil
}

// Get 按名取池。
func (r *Registry) Get(name string) *Pool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pools[name]
}

// Status 返回所有池状态。
func (r *Registry) Status() map[string][]InstanceStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][]InstanceStatus, len(r.pools))
	for name, p := range r.pools {
		out[name] = p.Status()
	}
	return out
}

// Stop 停止所有池。
func (r *Registry) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.pools {
		p.Stop()
	}
}
