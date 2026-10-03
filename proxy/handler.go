package proxy

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/shikejing/http-rewrite-proxy-go/config"
	"github.com/shikejing/http-rewrite-proxy-go/limiter"
	"github.com/shikejing/http-rewrite-proxy-go/rule"
	"github.com/shikejing/http-rewrite-proxy-go/upstream"
)

// View 是某一时刻自洽的转发视图（配置 + 规则引擎）。
type View struct {
	Config *config.Config
	Engine *rule.Engine
}

// Views 提供原子视图获取。
type Views interface {
	View() *View
}

// Handler 是转发处理器，自身无状态，规则视图从 Views 原子获取。
type Handler struct {
	views    Views
	registry *upstream.Registry
}

// NewHandler 创建转发处理器。
func NewHandler(views Views, registry *upstream.Registry) *Handler {
	return &Handler{views: views, registry: registry}
}

// ServeHTTP 实现 http.Handler。
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	view := h.views.View()
	cfg := view.Config
	acl := NewACL(cfg.ACL)

	// 1. 访问控制：剥离不可信请求头、注入身份头、追加转发身份。
	acl.PrepareRequest(r)
	prepareOutgoingRequest(r)

	// 2. 规则改写：路径/查询/请求头按优先级叠加，响应头动作收集待执行。
	upstreamName, respActions := view.Engine.Apply(r)
	if upstreamName == "" {
		upstreamName = cfg.Settings.DefaultUpstream
	}

	// 3. 选择上游池。
	pool := h.registry.Get(upstreamName)
	if pool == nil {
		h.writeProxyError(w, ErrNoHealthyUpstream)
		return
	}

	// 4. 并发上限：立即获取或排队，超时/取消则确定拒绝。
	release, err := pool.Limiter().Acquire(r.Context())
	if err != nil {
		if errors.Is(err, limiter.ErrQueueFull) {
			h.writeProxyError(w, ErrQueueRejected)
		} else {
			h.writeProxyError(w, classify(err))
		}
		return
	}
	defer release()

	// 5. 逐尝试转发（含重试与超时）。
	resp, perr := h.forwardWithRetry(r, pool, respActions)
	if perr != nil {
		h.writeProxyError(w, perr)
		return
	}
	defer resp.Body.Close()

	// 6. 双向流式协议（101）透传。
	if resp.StatusCode == http.StatusSwitchingProtocols {
		h.serveUpgrade(w, r, resp, respActions, acl)
		return
	}

	// 7. 响应头：逐跳头不转发，执行规则响应改写与敏感头剥离。
	out := w.Header()
	copyHopByHopStripped(out, resp.Header, false)
	rule.ApplyRespActions(out, respActions)
	acl.PrepareResponse(out)

	w.WriteHeader(resp.StatusCode)

	// 8. 流式转发响应体，不整体缓冲。
	h.streamBody(w, r, resp, cfg.Settings.FlushIntervalMS)
}

// streamBody 把上游响应体流式拷贝给客户端；客户端断开或上游提前关闭都不泄漏资源。
func (h *Handler) streamBody(w http.ResponseWriter, r *http.Request, resp *http.Response, flushIntervalMS int) {
	flusher, _ := w.(http.Flusher)
	immediateFlush := flushIntervalMS >= 0 || strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream")
	buf := make([]byte, 32*1024)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				// 客户端已断开：关闭上游连接，释放后台任务。
				return
			}
			if immediateFlush && flusher != nil {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				if r.Context().Err() != nil {
					log.Printf("proxy: 客户端中断导致上游读取结束: %v", readErr)
				} else {
					log.Printf("proxy: 上游响应流提前关闭: %v", readErr)
				}
			}
			return
		}
	}
}

// serveUpgrade 处理 101 Switching Protocols：劫持客户端连接，与上游连接双向透传。
func (h *Handler) serveUpgrade(w http.ResponseWriter, r *http.Request, resp *http.Response, respActions []rule.RespAction, acl *ACL) {
	defer resp.Body.Close()
	upstreamConn, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		http.Error(w, "upstream upgrade unsupported", http.StatusBadGateway)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	conn, brw, err := hj.Hijack()
	if err != nil {
		log.Printf("proxy: hijack 失败: %v", err)
		return
	}
	defer conn.Close()

	// 执行响应头改写（保留 Upgrade/Connection）。
	rule.ApplyRespActions(resp.Header, respActions)
	acl.PrepareResponse(resp.Header)

	brw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	resp.Header.Write(brw)
	brw.WriteString("\r\n") // Header.Write 不写终止空行
	brw.Flush()

	// 回放客户端已缓冲但未发送的字节。
	if n := brw.Reader.Buffered(); n > 0 {
		p := make([]byte, n)
		if _, err := brw.Reader.Read(p); err == nil {
			upstreamConn.Write(p)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		io.Copy(upstreamConn, conn)
		upstreamConn.Close()
	}()
	io.Copy(conn, upstreamConn)
	conn.Close()
	<-done
}

// writeProxyError 输出可区分的 JSON 错误；客户端已取消时不再写响应。
func (h *Handler) writeProxyError(w http.ResponseWriter, e *Error) {
	if e == nil || e.Kind == KindClientCanceled {
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(e.Status)
	fmt.Fprintf(w, `{"error":%q,"kind":%q}`+"\n", e.Msg, kindName(e.Kind))
}

func kindName(k Kind) string {
	switch k {
	case KindNoHealthyUpstream:
		return "no_healthy_upstream"
	case KindQueueFull:
		return "queue_full"
	case KindTimeout:
		return "timeout"
	case KindUpstreamConnect:
		return "upstream_connect"
	case KindUpstreamRead:
		return "upstream_read"
	case KindClientCanceled:
		return "client_canceled"
	default:
		return "bad_gateway"
	}
}

// hopByHopHeaders 是逐跳头，不应转发。
var hopByHopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive",
	"Proxy-Authenticate", "Proxy-Authorization",
	"Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

// stripHopByHop 从请求头中移除逐跳头；upgrade 请求保留 Upgrade/Connection。
func stripHopByHop(h http.Header, keepUpgrade bool) {
	// Connection 头中可能列出额外的逐跳头。
	if c := h.Get("Connection"); c != "" {
		for _, f := range strings.Split(c, ",") {
			if f = strings.TrimSpace(f); f != "" {
				if keepUpgrade && (strings.EqualFold(f, "Upgrade") || strings.EqualFold(f, "Connection")) {
					continue
				}
				h.Del(f)
			}
		}
	}
	for _, name := range hopByHopHeaders {
		if keepUpgrade && (name == "Upgrade" || name == "Connection") {
			continue
		}
		h.Del(name)
	}
}

// copyHopByHopStripped 拷贝响应头并剔除逐跳头。
func copyHopByHopStripped(dst, src http.Header, keepUpgrade bool) {
	for k, vs := range src {
		if !keepUpgrade && isHopByHop(k) {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
	if !keepUpgrade {
		// Connection 头列出的额外逐跳头也要剔除。
		if c := src.Get("Connection"); c != "" {
			for _, f := range strings.Split(c, ",") {
				if f = strings.TrimSpace(f); f != "" {
					dst.Del(f)
				}
			}
		}
	}
}

func isHopByHop(name string) bool {
	canonical := http.CanonicalHeaderKey(name)
	for _, h := range hopByHopHeaders {
		if h == canonical {
			return true
		}
	}
	return false
}

// prepareOutgoingRequest 在转发前剔除逐跳头。
func prepareOutgoingRequest(r *http.Request) {
	keepUpgrade := r.Header.Get("Upgrade") != ""
	stripHopByHop(r.Header, keepUpgrade)
	if keepUpgrade {
		// 确保 Connection 头包含 Upgrade。
		conn := r.Header.Get("Connection")
		if !strings.Contains(strings.ToLower(conn), "upgrade") {
			if conn == "" {
				r.Header.Set("Connection", "Upgrade")
			} else {
				r.Header.Set("Connection", conn+", Upgrade")
			}
		}
	}
}

