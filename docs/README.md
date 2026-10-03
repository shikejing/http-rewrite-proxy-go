# 可配置请求转发与改写层

一套基于 Go 标准库实现的 HTTP 反向代理，按规则把外部请求路由到多个上游，并在转发链路中改写请求与响应。上游可用本地回环或内存通道模拟，不依赖任何外部服务。

## 架构

```
客户端 ──> 代理(handler) ──> 上游池(pool) ──> 上游实例
              │
              ├── 规则引擎(rule)：匹配 + 改写
              ├── 访问控制(acl)：身份透传 + 敏感头保护
              ├── 限流器(limiter)：并发上限 + 排队/拒绝
              └── 健康检查(health)：摘除/恢复
```

- `config`：配置模型、校验、原子存储（热更新与回滚）。
- `rule`：规则编译与有序求值。
- `upstream`：实例池、轮询负载选择、健康检查、并发限制。
- `limiter`：基于信号量的并发上限与排队超时。
- `proxy`：转发处理器（流式、重试、超时、错误分类、ACL、101 透传）。
- `runtime`：把配置/规则/池组装为原子视图，统一热更新。
- `main`：启动入口与管理 API。

## 规则模型

每条规则由 `id`、`priority`、`match`、`actions` 组成。

### 匹配条件（各条件之间为“与”关系）

| 字段 | 说明 |
| --- | --- |
| `path_prefix` | 路径前缀匹配（必填） |
| `methods` | HTTP 方法白名单，空表示全部 |
| `headers` | 请求头精确匹配（键值都相等） |
| `host` | Host 匹配（不含端口） |

### 动作

| 动作 | 说明 |
| --- | --- |
| `upstream` | 转发到指定上游池 |
| `path_prefix_replace` | 路径前缀替换（`match` → `replacement`） |
| `query_set` | 设置查询参数 |
| `query_add` | 追加查询参数（可同名多值） |
| `query_del` | 删除查询参数 |
| `req_header_set/add/del` | 请求头改写 |
| `resp_header_set/add/del` | 响应头改写 |

### 生效顺序（确定且可解释）

1. 规则按 `priority` 从高到低排序；优先级相同按 `id` 字典序排序。
2. 请求命中的规则按该顺序依次应用动作。
3. **转发目标**由第一个指定 `upstream` 的命中规则决定（高优先级优先）。
4. 路径/查询/请求头改写按优先级从高到低**依次叠加**。
5. 响应头动作按相同顺序收集，在响应返回客户端前依次执行。

## 配置项

### upstreams[]

| 字段 | 说明 | 默认 |
| --- | --- | --- |
| `name` | 上游池名（唯一） | 必填 |
| `urls` | 实例地址列表 | 必填 |
| `health.path` | 健康检查路径 | `/` |
| `health.interval_ms` | 检查间隔 | 5000 |
| `health.timeout_ms` | 单次检查超时 | 2000 |
| `health.healthy_threshold` | 连续成功多少次恢复 | 1 |
| `health.unhealthy_threshold` | 连续失败多少次摘除 | 1 |
| `max_conns` | 并发上限，0 不限 | 0 |
| `queue_timeout_ms` | 排队超时，0 不排队 | 0 |

### settings

| 字段 | 说明 | 默认 |
| --- | --- | --- |
| `default_upstream` | 默认上游池 | 空 |
| `attempt_timeout_ms` | 单次尝试超时（内层） | 30000 |
| `overall_timeout_ms` | 整体截止时间（外层），0 不限 | 60000 |
| `max_retries` | 最大重试次数 | 2 |
| `retry_backoff_ms` | 重试退避基数（指数退避） | 10 |
| `flush_interval_ms` | 响应刷新间隔，-1 不主动刷新（SSE 自动立即刷新） | 0 |
| `max_idle_conns` | 最大空闲连接 | 100 |
| `max_idle_conns_per_host` | 每 host 最大空闲连接 | 32 |
| `idle_conn_timeout_ms` | 空闲连接超时 | 90000 |

### acl

| 字段 | 说明 |
| --- | --- |
| `protected_headers` | 禁止规则改写的请求头（内置 `Authorization`、`Proxy-Authorization`） |
| `strip_req_headers` | 转发前从客户端请求剥离的头 |
| `inject_req_headers` | 注入到转发请求的身份头 |
| `strip_resp_headers` | 返回客户端前剥离的响应头 |

## 行为约定

### 负载选择与健康检查

- 轮询（round-robin）在所有实例间递增，健康实例不会被持续绕过；不健康实例被跳过。
- 健康检查连续失败达到 `unhealthy_threshold` 时摘除，连续成功达到 `healthy_threshold` 时恢复，状态翻转有日志。
- 全部实例不健康时返回 `503 no_healthy_upstream`。

### 并发上限

- 每个上游池独立限流。名额满时进入排队，排队超时返回 `503 queue_full`；客户端在排队中取消则返回 `499 client_canceled`。

### 流式转发

- 请求体与响应体均流式拷贝（32KB 缓冲），不整体缓冲。
- `text/event-stream` 响应立即刷新；其他响应按 `flush_interval_ms` 刷新。
- 客户端中断：上游连接与后台任务立即释放。
- 上游提前关闭：返回 `502 upstream_connect`（响应头尚未发出时）或中断流（已发出响应头时）。

### 重试与超时

- **可重试条件**：无请求体且方法为 `GET/HEAD/OPTIONS`（可安全重放）。
- **不可重试**：有请求体或非幂等方法（如 POST）只尝试一次，不重复产生副作用。
- **超时先后**：`overall_timeout_ms` 是外层硬约束，`attempt_timeout_ms` 是内层单次尝试约束。
- 重试仅在连接类/超时类错误时发生；客户端取消不重试。
- 退避为指数退避（基数 `retry_backoff_ms`，上限 5s）。

### 身份透传与访问控制

- 转发前剥离 `strip_req_headers`，注入 `inject_req_headers`，并追加 `X-Forwarded-For`、`X-Forwarded-Proto`、`X-Forwarded-Host`。
- 规则动作若改写受保护头，配置加载即被拒绝（越权改写失败）。
- 签名头（如 `Authorization`）与请求体原样透传。

### 双向流式透传

- 支持 `101 Switching Protocols`：劫持客户端连接，与上游连接双向拷贝。
- 连接复用与并发上限可配置；单条慢连接不阻塞其他连接（独立连接 + 流式拷贝）。

### 配置热更新

- 管理 API 触发：`POST /admin/reload`（重新读文件）或 `POST /admin/config`（请求体为 JSON）。
- 更新时执行“校验 → 编译规则 → 应用上游 → 原子切换”；任一环节失败则保留旧视图（回滚）。
- 在途请求持有旧视图引用，不受热更新影响；新请求使用新视图。

## 管理 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/admin/status` | 运行状态（上游实例健康、配置摘要） |
| POST | `/admin/reload` | 从配置文件重新加载 |
| POST | `/admin/config` | 用请求体 JSON 替换配置 |

## 错误码

| HTTP 状态 | kind | 含义 |
| --- | --- | --- |
| 503 | `no_healthy_upstream` | 无健康实例 |
| 503 | `queue_full` | 排队超时，请求被拒绝 |
| 504 | `timeout` | 上游超时 |
| 502 | `upstream_connect` | 上游连接错误（含提前关闭） |
| 502 | `bad_gateway` | 其他网关错误 |
| 499 | `client_canceled` | 客户端取消 |

错误响应体为 JSON：`{"error":"...","kind":"..."}`。

## 运行

```bash
go build -o proxy-server .
./proxy-server -addr :8080 -config config.json
```

## 测试

```bash
go test ./...
go test -race ./...
```
