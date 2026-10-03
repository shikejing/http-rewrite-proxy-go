// Package runtime 把配置存储、规则引擎、上游池注册表原子地组装在一起，
// 配置热更新时保证“校验 -> 编译 -> 应用 -> 切换”要么全部生效，要么全部回滚。
package runtime

import (
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/shikejing/http-rewrite-proxy-go/config"
	"github.com/shikejing/http-rewrite-proxy-go/proxy"
	"github.com/shikejing/http-rewrite-proxy-go/rule"
	"github.com/shikejing/http-rewrite-proxy-go/upstream"
)

// Runtime 是整个转发层的运行时载体。
type Runtime struct {
	store    *config.Store
	registry *upstream.Registry
	view     atomic.Pointer[proxy.View]
	handler  *proxy.Handler
}

// New 创建运行时并加载初始配置。
func New(cfgPath string) (*Runtime, error) {
	rt := &Runtime{
		store:    config.NewStore(cfgPath),
		registry: upstream.NewRegistry(),
	}
	rt.handler = proxy.NewHandler(rt, rt.registry)

	if err := rt.store.Load(); err != nil {
		log.Printf("runtime: 初始配置加载失败（使用默认空配置）: %v", err)
	}
	if err := rt.apply(rt.store.Current()); err != nil {
		log.Printf("runtime: 初始配置应用失败: %v", err)
	}
	return rt, nil
}

// Handler 返回转发处理器。
func (rt *Runtime) Handler() http.Handler {
	return rt.handler
}

// View 返回当前自洽视图（实现 proxy.Views）。
func (rt *Runtime) View() *proxy.View {
	v := rt.view.Load()
	if v == nil {
		return &proxy.View{Config: config.Default(), Engine: mustCompile(nil)}
	}
	return v
}

// apply 按“校验 -> 编译 -> 应用 -> 切换”顺序应用配置；任一环节失败则旧视图不变（回滚）。
func (rt *Runtime) apply(cfg *config.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	acl := proxy.NewACL(cfg.ACL)
	if err := acl.ValidateRules(cfg.Rules); err != nil {
		return err
	}
	engine, err := rule.Compile(cfg.Rules)
	if err != nil {
		return err
	}
	if err := rt.registry.Apply(cfg.Upstreams, cfg.Settings); err != nil {
		return err
	}
	if err := rt.store.Replace(cfg); err != nil {
		return err
	}
	rt.view.Store(&proxy.View{Config: cfg, Engine: engine})
	return nil
}

// Reload 校验并应用新配置；任一环节失败则保留旧视图（回滚）。
func (rt *Runtime) Reload(cfg *config.Config) error {
	return rt.apply(cfg)
}

// ReloadFile 从文件重新加载。
func (rt *Runtime) ReloadFile() error {
	if err := rt.store.Load(); err != nil {
		return err
	}
	return rt.apply(rt.store.Current())
}

// Status 返回运行状态。
func (rt *Runtime) Status() map[string]interface{} {
	cfg := rt.store.Current()
	return map[string]interface{}{
		"upstreams": rt.registry.Status(),
		"settings":  cfg.Settings,
		"rules":     len(cfg.Rules),
	}
}

// Close 停止后台任务。
func (rt *Runtime) Close() {
	rt.registry.Stop()
}

// AdminHandler 返回管理 API：
//
//	GET  /admin/status  运行状态
//	POST /admin/reload  从配置文件重新加载
//	POST /admin/config  用请求体中的 JSON 替换配置
func (rt *Runtime) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, rt.Status())
	})
	mux.HandleFunc("/admin/reload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := rt.ReloadFile(); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "reloaded"})
	})
	mux.HandleFunc("/admin/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var cfg config.Config
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json: " + err.Error()})
			return
		}
		if err := rt.Reload(&cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "reloaded"})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func mustCompile(rules []config.RuleConfig) *rule.Engine {
	e, _ := rule.Compile(rules)
	return e
}

var _ proxy.Views = (*Runtime)(nil)
