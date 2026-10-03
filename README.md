# http-rewrite-proxy

可配置的 HTTP 请求转发与改写层：按规则把外部请求路由到多个上游，并在转发链路中改写请求与响应。上游可用本地回环地址模拟，不依赖任何外部服务。

## 构建与运行

```bash
go build ./...
go test ./...          # 运行全部测试（含 -race 可用 go test -race ./...）
./proxy -addr :8080 -config config.json
```

运行期向进程发送 `SIGHUP` 触发配置热更新；加载或校验失败时保留旧配置。

## 规则模型

每条规则（`rules[]`）= 匹配条件 + 改写动作 + 目标上游：

| 字段 | 说明 |
|---|---|
| `name` | 规则名，全局唯一 |
| `priority` | 优先级，**数值小者优先** |
| `path_prefix` | 匹配条件：请求路径前缀 |
| `replace_path` | 改写：用该前缀替换命中的 `path_prefix` |
| `upstream` | 目标上游集群名 |
| `add_query` / `remove_query` | 查询参数增加/删除 |
| `request_headers` / `response_headers` | 头部改写：`set` 覆盖、`remove` 删除 |

### 多规则命中时的生效顺序（确定且可解释）

1. `priority` 数值小者优先；
2. `priority` 相同时，`path_prefix` 更长（更具体）者优先；
3. 仍相同时按规则名字典序，保证结果完全确定。

每条请求只应用**一条**最高优先级规则，不叠加。

## 配置项

```jsonc
{
  "rules": [ /* 见上 */ ],
  "upstreams": [ { "name": "u1", "endpoints": ["http://127.0.0.1:9001"] } ],
  "retry":     { "max_attempts": 2, "backoff": "1ms" },
  "timeouts":  { "per_attempt": "2s", "overall": "5s" },
  "auth": {
    "strip_headers": ["Cookie"],
    "inject_headers": { "X-Internal-Token": "secret" },
    "signed_headers": ["Authorization"],
    "allow_rewrite_signed": false
  },
  "limits":       { "max_concurrent": 1, "queue_limit": 0 },
  "health_check": { "interval": "2s", "timeout": "1s",
                    "fail_threshold": 2, "recover_threshold": 1 }
}
```

### 负载选择与健康检查

- 同一上游集群内按**轮询**在健康实例间选路，健康实例不会被持续绕过。
- 后台探活：连续失败 `fail_threshold` 次摘除，连续成功 `recover_threshold` 次恢复。
- 无健康实例时返回 `503` + `X-Proxy-Fail-Reason: no-healthy-upstream`。

### 流式转发

- 请求体与响应体均流式转发（32KB 缓冲 + 每次写后 flush），大响应不整体进内存。
- 客户端中断会取消上游请求上下文、关闭上游响应体；升级连接（WebSocket 等）双向拷贝，任一侧关闭即同时关闭两侧连接，不泄漏连接与后台 goroutine。

### 重试与超时

- 仅对**可安全重放**的请求重试：`GET/HEAD/OPTIONS` 总是可重试；`PUT/DELETE` 仅在 body 可重建时重试；`POST/PATCH` 等非幂等方法**绝不重试**，不会重复产生副作用。
- 先后关系：每次尝试受 `per_attempt` 约束，所有尝试共享 `overall` 总预算；总预算到期立即停止，不再发起新尝试。
- 上游 5xx 且可重试时按 `max_attempts`（间隔 `backoff`）重试；耗尽后返回 `502` + `retries-exhausted`。

### 身份透传与访问控制

- `strip_headers` 在转发前剥离，`inject_headers` 在转发前注入。
- 携带 `signed_headers` 中任一头部的请求视为**带签名请求**：若命中规则含改写动作且 `allow_rewrite_signed=false`，拒绝转发（`403` + `signed-rewrite-denied`），保证改写不破坏签名。

### 并发与长连接

- `max_concurrent` 限制全局在途请求数（0 = 不限）；达到上限后最多 `queue_limit` 个请求排队等待，超出立即拒绝（`503` + `overload`）。
- 单条慢连接只占用一个配额，不阻塞其他请求的拒绝/准入判定。
- Upgrade 请求（WebSocket 等）整体透传，不受 HTTP 超时影响。

### 配置热更新

- 新配置先完整校验并构建出独立的运行时视图（规则集 + 上游池 + 信号量），再原子切换。
- 在途请求继续持有旧视图快照，不受中断；切换后旧视图的后台健康检查立即停止。
- 校验或构建失败时返回错误并保留旧视图；任一时刻规则视图自洽。

## 失败原因

所有代理侧失败通过 `X-Proxy-Fail-Reason` 响应头区分：

| 值 | 含义 | 状态码 |
|---|---|---|
| `no-matching-rule` | 无规则命中 | 404 |
| `no-healthy-upstream` | 集群无健康实例 | 503 |
| `overload` | 并发超限且排队已满 | 503 |
| `signed-rewrite-denied` | 带签名请求的改写被拒绝 | 403 |
| `upstream-timeout` | 总超时到期 | 504 |
| `upstream-error` | 上游连接/协议错误 | 502 |
| `retries-exhausted` | 重试耗尽上游仍 5xx | 502 |

## 测试覆盖

`proxy/` 下的测试覆盖：规则优先级与改写、上游摘除与恢复、流式转发不缓冲、客户端中断释放上游、重试仅幂等、重试耗尽、签名请求保护、敏感头剥离/注入、热更新不中断在途请求、非法配置回退、并发上限拒绝、Upgrade 透传、热更新并发压测（`-race` 通过）。
