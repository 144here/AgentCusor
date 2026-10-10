import os

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# 实测可用的后端是 DeepSeek 网关，它只认 deepseek-flash 与 deepseek-v4-pro 两个规范名
# （deepseek-chat / deepseek-v4-flash 之类的别名也返回 200，但一律解析到 deepseek-flash）。
LLM_MODEL = os.environ.get("LLM_MODEL", "deepseek-flash")

# deepseek-flash / deepseek-v4-pro 都是思维链模型，默认先产出 reasoning_content。
# autogen 会把思维链以 "<think>…</think>" 的形式混进同一条流式 chunk 流
# （autogen_ext/models/openai/_openai_client.py 里与正文 chunk 同一个生成器），
# 直接转发会让终端用户看到模型的内部推理，并且破坏 Go 侧依赖的
# 「delta 拼接 == final.response」不变式。本网关支持 thinking={"type":"disabled"}
# 关掉它（实测 chunk 数 178 → 24，延迟与 token 都减半）。
# 这是网关私有字段：万一将来网关开始拒绝未知 body 字段（那会让所有请求 400），
# 把环境变量设成 false 即可停止注入，不必改代码。
LLM_DISABLE_THINKING = os.environ.get("LLM_DISABLE_THINKING", "true").strip().lower() in ("1", "true", "yes", "on")

# OpenAI 兼容端点的地址。留空表示用 SDK 默认（https://api.openai.com/v1）。
# 指向 DeepSeek/自建网关这类兼容端点时必须显式设置，且不能设成空字符串——
# openai SDK 见到空串会当成有效 base_url，请求会直接发到 ""。
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "")

# Prometheus 指标端口（utils/metrics.py: start_metrics_server）。与 gRPC 的 50051 分开。
METRICS_PORT = int(os.environ.get("METRICS_PORT", 8001))

# --- 异步任务队列（Celery over Redis）---
# 队列只承载「副作用」：目前是退款工单（建工单 + 发确认邮件 + 通知客服）。
# 聊天本身仍走同步 gRPC 流式，不进队列——把逐字流塞进任务队列会摧毁
# Go 侧刻意做的反压链路（见 handler/ws_handler.go 的 enqueue 注释）。
# broker 与 backend 分库，避免任务报文和业务键（ticket:* / job:*）互相淹没。
CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/1")
CELERY_RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", f"redis://{REDIS_HOST}:{REDIS_PORT}/2")

# 总开关。关掉后 agent 不再入队，聊天链路完全不受队列影响。
ENABLE_REFUND_QUEUE = os.environ.get("ENABLE_REFUND_QUEUE", "true").strip().lower() in ("1", "true", "yes", "on")

# 入队去重窗口（秒）：同一用户、同一诉求在窗口内只开一张工单。
# 用户连点两次发送、或 Celery 重投同一任务，都不该产生两张工单。
REFUND_DEDUP_TTL = int(os.environ.get("REFUND_DEDUP_TTL", 3600))

# 任务结果在 Redis 里的保留时间（秒），也是异步事件推送后可回查的窗口。
JOB_RESULT_TTL = int(os.environ.get("JOB_RESULT_TTL", 3600))

# 工单本身的保留时间（秒）。
TICKET_TTL = int(os.environ.get("TICKET_TTL", 7 * 86400))