# AgentCusor

电商客服机器人。演示一条完整链路：**WebSocket 逐字流式对话 + 异步副作用队列 + 可观测性**。

重点不在「能聊天」，而在于几个工程约束被真的兑现：逐字输出、端到端反压、
队列只承载副作用、两层幂等、以及每条降级路径都不会把聊天拖垮。

## 架构

```
任意 WebSocket 客户端
        │  ws://localhost:8000/ws?user_id=<id>      发 {"message": "..."}
        ▼
 websocket-server (Go, :8000)          连接池 / 逐字转发 / 反压 / /metrics / /health
        │  gRPC server-streaming (:50051)
        ▼
 agent-grpc (Python + autogen, :50051, 指标 :8001)
        │
        ├─ 意图分类 → 专家 agent → 逐字流式回传（**同步**，反压一路传到 LLM）
        └─ 退款意图 → 旁路入队（**异步**，不阻塞本轮回复）
                    │
                    ▼
              celery-worker ── Redis：建工单 / 去重锁 / 结果键，再 PUBLISH job:events
                    │
                    └──► websocket-server 订阅该频道 → 推 {"type":"job",...} 给对应用户

 prometheus (:9090) 抓三处：websocket-server:8000、agent-grpc:8001，以及自身 :9090
```

### 两个刻意的设计判断

**队列不承载聊天本身。** 把 token 流塞进队列（Go 入队 → worker 调 LLM → 结果传回）看着更"分布式"，
实际会摧毁 Go 侧刻意保留的反压链路——`handler/ws_handler.go` 的 `enqueue` 用**阻塞**发送，
让客户端消费速度经 gRPC 流控一路传回 LLM；中间插一层 broker 这条链路就断了，
还会新增「worker 中途挂掉、半截回复怎么办」这类流式状态问题。
所以队列只承载慢且可异步的**副作用**（建工单 / 发确认邮件 / 通知客服），聊天保持同步流式。

**工单号是确定性推导的。** `job_id = "rf_" + sha1(user_id|normalized_reason|order_id)[:16]`，
同一个 id 同时是 Celery `task_id`、去重锁、结果键和工单号本身。好处是 agent **不必等 worker 跑完**
就能把工单号写进回复；worker 只负责把它落库。

## 起服务

需要 Docker（宿主机不需要装 Go / Python / protoc，构建与依赖解析都在容器里做）。

```bash
docker compose build agent-grpc   # 只需构建一次；celery-worker 复用同一个镜像（同源码同依赖，只是 command 不同）
docker compose up -d
docker compose ps                 # 5 个服务都应是 Up
```

`agent-grpc` 被重建（新容器、新 IP）之后**等几秒再发消息**：网关到它的 gRPC 通道要重新解析/建连，
这段窗口里发出去的那一条会失败——实测见过两种形态：收到一个通用的 `{"type":"error",...}` 帧，
或者干脆一直悬着收不到任何帧。**不用重启 `websocket-server`**，网关会自己重连；等几秒再发即正常。

（这一条如实记账：那句「一直悬着」只在我这边观察过一次，独立复跑时只见到快速 `error` 帧、
没能复现悬住的那半。两种形态都不影响结论——窗口很窄、自愈、别把它当成产品故障去重启网关。）

| 服务 | 端口 | 作用 |
|---|---|---|
| `websocket-server` | 8000 | WebSocket `/ws`、`/metrics`、`/health` |
| `agent-grpc` | 50051 / 8001 | gRPC 对话；8001 是 Prometheus 指标 |
| `celery-worker` | — | 消费退款工单任务；与 `agent-grpc` 同一个镜像 |
| `redis` | 6379 | 工单/去重/结果（db 0）、Celery broker（db 1）、backend（db 2） |
| `prometheus` | 9090 | 抓上面两个 `/metrics` |

LLM 凭据从环境变量注入：`OPENAI_API_KEY`（**shell 环境优先于 `.env`**），
端点用 `OPENAI_BASE_URL`（默认 `https://api.deepseek.com`），模型用 `LLM_MODEL`（默认 `deepseek-flash`）。
`.env` 仅供本机开发，不要提交。

这两个默认值**必须配成一对**：模型名只在它自己的端点上存在。曾经这里兜底到
`api.openai.com` 而模型兜底 `deepseek-flash`，于是不带环境变量起栈时每一次 LLM 调用都 404。
它的危险之处在于**不阻塞启动**——5 个服务照样 Up、`/health` 照样 ok、指标照样有样本，
只有真的发一条消息才会收到 `{"type":"error","message":"系统错误，请稍后重试"}`。
所以起栈后请务必发一条消息确认，别只看 `docker compose ps`。

凭据是从你的 shell 环境注入容器的，**换一个没加载这些变量的 shell 再 `docker compose up -d`，
会把容器重建成「端点/模型对不上」的状态**（compose 只在渲染结果变化时重建，所以这很容易发生）。
排查用 `docker compose exec -T agent-grpc printenv OPENAI_BASE_URL`（`printenv` 只打这一个变量；
别用 `docker compose config`，它会把 `OPENAI_API_KEY` 明文渲染出来）。

## 试一条对话

发 `{"message": "..."}`，收下面几种帧（同一条连接上一轮没结束时再发会被拒：
`{"type":"error","message":"上一条消息还在处理中，请稍候"}`）。

| `type` | 字段 |
|---|---|
| `delta` | `content` —— 逐字增量，客户端拼接即得全文 |
| `done` | `content`（终态全文）、`intent`、`requires_escalation` |
| `error` | `message` |
| `job` | `status`、`job_id`、`ticket_id` —— 异步任务状态，可能在 `done` 之后到达 |

所有帧都带 `requires_escalation`（该字段没有 `omitempty`，所以 `delta` 帧上它恒为 `false`），
取用时按 `ev.requires_escalation` 读即可，只在 `done` 帧上有意义。

最小客户端——浏览器原生就有 WebSocket，贴进 DevTools 控制台即可：

```js
const ws = new WebSocket("ws://localhost:8000/ws?user_id=demo");
let text = "";
ws.onmessage = ({ data }) => {
  const ev = JSON.parse(data);
  switch (ev.type) {
    case "delta": text += ev.content; break;   // 逐字拼接
    case "done":  console.log("全文:", text, "\nintent:", ev.intent, "需升级:", ev.requires_escalation);
                  console.log("拼接是否精确等于终态:", text === ev.content); break;
    case "job":   console.log("异步任务:", ev); break;
    case "error": console.warn("错误:", ev.message); break;
  }
};
ws.onopen = () => ws.send(JSON.stringify({ message: "我要退货，东西是坏的" }));
```

命令行版（`wscat`，本机 `/usr/bin/wscat` 已装，宿主机零安装）：

```bash
wscat -c 'ws://localhost:8000/ws?user_id=demo' -x '{"message":"你们几点下班？"}' -w 15
```

`-x` 是连上之后自动发一条；`-w 15` 是发完**保持连接 15 秒**继续收帧——不写 `-w` 的话命令一发完
连接就关了，只会看到半截 `delta`。`user_id` 是任意字符串，只用来做会话隔离与工单归属，不必叫 `demo`。
逐帧打印的 `requires_escalation` 在 `delta` 帧上恒为 `false`，只看 `done` 帧那个。

发那条退款诉求会看到：逐字回复里带一个工单号、`done.requires_escalation == true`，
随后（可能在 `done` 之后若干毫秒）收到 `{"type":"job","status":"created",...}`。

## 工单落在哪

```bash
docker compose exec -T redis redis-cli keys 'ticket:*'                  # 工单本身（hash）
docker compose exec -T redis redis-cli keys 'job:seen:*'                # 生产端去重锁
docker compose exec -T redis redis-cli keys 'job:result:*'              # 可回查的最终状态（用户掉线也在）
docker compose exec -T redis redis-cli lrange 'tickets:user:demo' 0 -1  # 某用户的工单列表
```

去重做两层，因为 `task_acks_late` + `autoretry` 让「同一个任务跑两次」是正常现象：
生产端 `SET job:seen:<id> NX` 抢不到就不入队；worker 端对外发副作用（发邮件、通知、
推 `job` 帧）都用 `job:step:<id>:<step>` 的 `SET NX` 认领，重投时跳过——否则用户会收到
两封「工单已创建」，或屏幕上两个一模一样的 `job` 帧。`job:result:<id>` 是幂等状态、
重写无副作用，因此不认领，顺带起到刷新 TTL 的作用。

认领之后失败了怎么办，三条支路是**分开选的**：推 `job` 帧的那条会**撤销认领**
（发布抛异常说明这一帧没送出去，重试应当再发；留着认领会让重试被误判成「已做过」而
永久跳过）；发邮件与通知**不撤销**，即 at-most-once——它们的失败也可能是「已经发出去了、
只是回执超时」，撤销就会重复发，而「不重复发」正是这两步存在的理由。

## 指标

```bash
curl -s localhost:9090/api/v1/targets | python3 -m json.tool | grep -E '"job"|"health"'
curl -s localhost:8000/metrics | grep cs_active_websocket_connections
```

## 已验证的行为

- 逐字流式：客户端收到的 delta 拼接**精确等于** `done.content`；长回复同样成立
  （实测数百帧量级、单轮数秒）；思维链已关闭（`extra_body={"thinking":{"type":"disabled"}}`）
  并有 `ThinkStripper` 兜底，客户端不会看到 `<think>`。
- 退款旁路：入队 → 建工单 → 邮件 → 通知 → 回写 `job:result:*` → 推 `job` 帧回同一个 WebSocket；
  `receivers` 计数证明发布确实被网关订阅者收到。
- 去重：同一诉求发两次，工单号相同、只建一张工单、只发一封邮件，且第二次在 broker 层就没有新任务。
  任务被重投（`task_acks_late` 下的正常现象）时同样只发一封邮件、只推一个 `job` 帧。
- 降级：停 `celery-worker` 时聊天照常流式（工单排队、复工后补消费，不丢）；
  停 `redis` 时仍能正常回答，只是不建工单（回复里不带工单号、`requires_escalation=false`）。

## 安全边界（演示用，非生产）

这一节只**记录**边界，不修——本项目是功能演示，不是能上生产的系统。上面所有「已验证的行为」
说的是**功能正确性**，不构成任何安全承诺。

- **`user_id` 没有任何鉴权。** `ServeWS` 直接取 `?user_id=` 当身份（`handler/ws_handler.go:87`），
  只校验非空。于是任意客户端可以用别人的 id 连上，从而：
  - 收到那个人的 `job` 帧（投递按 `user_id` 查连接池，`connection/pool.go` 的 `GetConnection`）；
  - 把自己发的消息写进那个人的对话历史（历史经 prompt 回显，可被套出对方此前的对话）；
  - 顶掉那个人的连接（`AddConnection` 覆盖同名 key）。
  真实实现需要在升级前校验 token/JWT，或由可信签发方对 `user_id` 签名、网关验签。
- **`CheckOrigin` 恒为 `true`**（`handler/ws_handler.go:81`）：放行任意来源，允许跨站 WebSocket 劫持。
  生产必须改成 Origin 白名单。
- **`agent-grpc:50051` 是明文 gRPC 且对外发布**（`docker-compose.yml` 映射 50051）：任何人可绕过网关
  直接调 `ProcessChat`；且失败时会把内部异常原文写进 gRPC status（`agent_service.py` 的
  `context.set_details(str(e))`——Go 网关已把它收敛成通用文案，直连接口没有）。生产应只在内部网络
  暴露、上 TLS/mTLS、异常只回通用文案。
- **没有速率限制，连接池也没有上限**（上限曾是个不生效的假旋钮，已删，见 `config.go` 注释）。
  当前唯一的限流是单连接「上一轮没结束就拒收新消息」。

## 开发命令

宿主机零安装，所有 Go/protobuf 操作都在一次性容器里跑：

```bash
make test            # 在跑起来的 agent-grpc 容器里跑 Python 测试（stdlib unittest）
make proto           # 重新生成 gRPC 代码（容器内 protoc）
make tidy            # 改完 import 后解析依赖并写回 go.mod / go.sum
make check           # 先跑 check-contract，再容器里 go build ./... && go vet ./... && go test ./...
make check-contract  # 单独断言 Go/Python 两侧的队列契约常量一致（见下）
```

`make` 目标是给「宿主零安装」的开发场景用的（所有 Go/protobuf 操作都在一次性容器里跑）。
CI（`.github/workflows/ci.yml`）跑的是同一批检查，但用**原生工具链**（`go build/vet/test`、
`sh scripts/check_contract.sh`、`pip install` + `unittest`），因此不要求 CI 能跑 Docker-in-Docker。

> 如实记账：本仓库的提交历史从 **2026-10-09** 才开始（此前它甚至不是 git 仓库）。README 里
> 「本轮改写」「骨架里的 10s」这类措辞描述的是代码在文件里的痕迹，不是能被 `git log` 佐证的过程。

### 测试

Go 与 Python 两侧各有一批测试，都钉住「错了用户能直接看见」或「不报错但少做一件事」的机制：

| 测试 | 断言什么 | 需要 |
|---|---|---|
| `customer-service-go/**/*_test.go` | 阻塞投递反压（`enqueue` 队列满则阻塞、只有连接结束才放弃；`tryEnqueue` 满则丢且不阻塞）、`NotifyJobEvent` 按 `user_id` 路由到目标用户、连接池覆盖语义、历史裁剪与副本、`agent_client` 对 proto 流契约的消费（含 `ErrStreamWithoutFinal` 与 `onDelta` 报错中断） | `go test`（无需 Redis/网络；gRPC 走 `bufconn`） |
| `tests/test_stream_invariant.py` | `delta` 拼接**精确等于** `done.content`；思维链不进客户端 | 无（离线，假模型客户端） |
| `tests/test_refund_idempotency.py` | 同一任务重投：只建一张工单、只发一封邮件、只推一个 `job` 帧；job 帧带齐 Go 侧要读的字段名 | 跑起来的 Redis |

Python 侧用 stdlib `unittest`（`IsolatedAsyncioTestCase` 跑异步用例），**没有引入 pytest**，
所以测试不增加任何依赖。两个文件都是 `TestCase` 类，将来装了 pytest 也能直接跑。

这两批测试现在由 CI 真跑：`.github/workflows/ci.yml` 里 Go 侧跑
`go build ./... && go vet ./... && go test ./...` 加 `scripts/check_contract.sh`，Python 侧在一个
`redis:7-alpine` service container 旁跑 `unittest discover`。**CI 用原生工具链、`make` 用容器**——
同一批检查的两种入口，不是二选一。

`make test` 走 `docker compose exec`，所以要求栈已经起来（`docker compose up -d`）；
测试文件和其他源码一起在镜像里，**改了测试要先 `docker compose build agent-grpc && docker compose up -d`**。

**这些断言是被"变异检查"验过的**：把 `ThinkStripper` 换成不再剥离、
把 `_claim_step` 换成永远返回 True、把 `_release_step` 换成空操作，
对应的测试都会红——否则一个在「有 bug」和「没 bug」的代码上都通过的测试没有意义。
`test_refund_idempotency.py` 里还留了一条常驻的反证用例
（`test_claims_are_what_suppress_the_duplicates`）：清掉 `job:step:*` 认领后重投**确实**会重复发，
证明认领是有承载力的，而不是一个没人读的装饰。

Go 侧同样用变异检查验过：把 `enqueue` 改成非阻塞、把 `NotifyJobEvent` 的查表键改错、
把 `history.snapshot` 改成不再拷贝、给 `requires_escalation` 加上 `omitempty`，
对应的测试都会红。新加的 `test_published_frame_carries_the_full_contract` 也一样——
把 worker 发布的 `ticket_id` 字段改名，这条立刻失败。

`check-contract` 存在的原因：`job:events` 这个频道名在 Go 和 Python 各写了一份
（`tasks/contract.py` 与 `service/job_subscriber.go`）。改一边、另一边照样编译通过，
然后网关**静默收不到任何任务事件**——症状和「Redis 里没数据」完全一样。这类
「不报错、只失效」的漂移在编译期抓不到，所以用一个只读文本比对把它变成显式失败。
