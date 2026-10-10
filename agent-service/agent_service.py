"""电商客服 Agent 服务（gRPC 流式）。

两层多 agent 结构：

    用户消息 ──▶ IntentClassifier（非流式，只出标签）
                     │
                     └─ 标签决定用哪个专家 ──▶ OrderAgent / RefundAgent / GeneralAgent
                                                     （流式，逐字回传）

分工是真实的：分类器的输出确实决定谁来回答，而不是几个 agent 轮流各说一句。
只有被选中那个专家的输出会流向用户；分类结果本身写进 final.intent，永远不出网。

为什么用确定式路由而不是 SelectorGroupChat（模型驱动选人）：
  1. SelectorGroupChat 的选人结果是从模型自由文本里解析出来的，模型改名或加标点
     就会选错，实测中它还会在同一轮里让第二个 agent 接着说话（扮演用户、编造订单号）；
  2. 确定式路由的标签可以直接当作 final.intent 使用，天然可观测、可断言；
  3. 少一次「选人」的 LLM 往返。

加一条异步旁路：识别为退款诉求时，把「建退款工单 + 发确认邮件 + 通知客服」排进
Redis/Celery 队列（tasks/ 包）。队列只承载副作用，**聊天本身仍走同步 gRPC 流式**——
把逐字流塞进任务队列会摧毁 Go 侧刻意做的反压链路。

遗留未实现（不假装它有内容）：final.tool_calls 恒为空——三个专家目前都没有可调用的
工具，用确定性路由 + 队列旁路代替了 tool calling，等接入 tools/ 那轮再填。
requires_escalation 已不再是常量：建了退款工单才为 true。
"""

import asyncio
import logging
import time
from concurrent import futures

import grpc
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.base import TaskResult
from autogen_agentchat.messages import ModelClientStreamingChunkEvent, TextMessage
from autogen_core.models import ModelFamily
from autogen_ext.models.openai import OpenAIChatCompletionClient

from config import (
    ENABLE_REFUND_QUEUE,
    LLM_DISABLE_THINKING,
    LLM_MODEL,
    METRICS_PORT,
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
)
from utils.metrics import CHAT_LATENCY, CHAT_REQUESTS, start_metrics_server

# 导入生成的proto代码
import sys
sys.path.append('./proto')
import agentpb.agent_pb2 as agent_pb2
import agentpb.agent_pb2_grpc as agent_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 退款工单队列是可选的旁路：celery/redis 没装好、或总开关关掉时，
# 聊天链路必须照常工作，只是不再建工单。所以导入失败只降级、不致命。
enqueue_refund_job = None
if ENABLE_REFUND_QUEUE:
    try:
        from tasks.producer import enqueue_refund_job as _enqueue_refund_job

        enqueue_refund_job = _enqueue_refund_job
    except Exception:
        logger.warning(
            "refund queue unavailable; refund tickets will not be created", exc_info=True
        )

# 只取最近若干条历史，避免 prompt 无界增长
MAX_HISTORY_MESSAGES = 20

# 分类是一次额外的 LLM 调用，必须有上限：超时就退回通用客服，
# 而不是把整轮对话一起挂死。
ROUTER_TIMEOUT_SECONDS = 15

INTENT_ORDER_QUERY = "order_query"
INTENT_REFUND_REQUEST = "refund_request"
INTENT_GENERAL = "general"
DEFAULT_INTENT = INTENT_GENERAL

# 面向用户的答复必须干净：不要让专家提起自己的角色分工或路由过程。
_COMMON_RULES = "只输出面向用户的答复，不要提及内部角色分工、分类结果或路由过程。"

# 工单建好后追加给专家的补充信息。工单号必须由模型说出口，否则用户不知道该报什么号。
_TICKET_HINT = (
    "\n补充信息：本次已为用户登记售后工单，工单号是 {ticket_id}。"
    "请在答复中自然地告知用户这个工单号，并说明客服会继续跟进。"
)

# 意图标签 → 专家 agent。这张表的键就是分类器的合法输出集合。
SPECIALISTS = {
    INTENT_ORDER_QUERY: {
        "name": "OrderAgent",
        "system_message": (
            "你是电商客服团队里的订单专员，负责回答订单状态、物流进度、配送时间相关的问题。"
            "请用简洁、礼貌的中文直接回答用户的问题。" + _COMMON_RULES
        ),
    },
    INTENT_REFUND_REQUEST: {
        "name": "RefundAgent",
        "system_message": (
            "你是电商客服团队里的退货专员，负责回答退货、换货、退款进度相关的问题。"
            "请用简洁、礼貌的中文直接回答用户的问题。" + _COMMON_RULES
        ),
    },
    INTENT_GENERAL: {
        "name": "GeneralAgent",
        "system_message": (
            "你是电商客服团队里的通用客服，负责回答商品信息、优惠活动、账号问题等咨询。"
            "请用简洁、礼貌的中文直接回答用户的问题。" + _COMMON_RULES
        ),
    },
}

CLASSIFIER_NAME = "IntentClassifier"
CLASSIFIER_SYSTEM_MESSAGE = (
    "你是一个意图分类器。判断用户当前这条消息属于以下哪一类：\n"
    f"- {INTENT_ORDER_QUERY}：订单状态、物流进度、配送时间\n"
    f"- {INTENT_REFUND_REQUEST}：退货、换货、退款进度\n"
    f"- {INTENT_GENERAL}：以上都不属于的其他咨询\n"
    f"只输出一个标签，必须是 {', '.join(SPECIALISTS)} 中的某一个，"
    "不要输出解释、标点、代码块或其他任何内容。"
)

# 非 OpenAI 官方模型名（deepseek-flash、qwen 等）不在 autogen 的内置表里，
# 不显式声明 model_info 就会在构造 client 时抛 ValueError。按本服务的实际用法
# 声明能力：纯文本对话、不用工具、不要结构化输出。
# family 用 UNKNOWN 只是表示「不套用任何已知家族的解析规则」，它**挡不住**流式
# chunk 里的思维链（那只影响 CreateResult.thought 的归类）——见 ThinkStripper。
COMPAT_MODEL_INFO = {
    "vision": False,
    "function_calling": False,
    "json_output": False,
    "family": ModelFamily.UNKNOWN,
    "structured_output": False,
}


def build_model_client() -> OpenAIChatCompletionClient:
    """构造模型客户端，兼容 OpenAI 官方模型与 OpenAI 协议端点。

    先按官方模型构造（能拿到 autogen 内置表里的准确能力描述，保留工具/视觉等能力）；
    只有在内置表查不到模型名时才补一份保守的 model_info。
    """
    kwargs = {"api_key": OPENAI_API_KEY, "model": LLM_MODEL}
    if OPENAI_BASE_URL:
        kwargs["base_url"] = OPENAI_BASE_URL
    if LLM_DISABLE_THINKING:
        # autogen 允许把 extra_body 透传给 OpenAI SDK（它在 create_kwargs 白名单里），
        # 这里借它关掉思维链。client 级参数，分类器那次调用也一并生效。
        kwargs["extra_body"] = {"thinking": {"type": "disabled"}}

    try:
        return OpenAIChatCompletionClient(**kwargs)
    except ValueError as e:
        # 只吞「模型名不认识的」这一种，其他 ValueError（例如没给 model）照常抛。
        if "model_info is required" not in str(e):
            raise
        logger.info(
            "model %s not in autogen's builtin table, using compat model_info", LLM_MODEL
        )
        return OpenAIChatCompletionClient(**kwargs, model_info=COMPAT_MODEL_INFO)


class ThinkStripper:
    """把 autogen 混在流式 chunk 里的思维链剥掉。

    autogen 对带 reasoning_content 的模型（DeepSeek flash/pro、R1 等）会在流式 chunk
    里插入 "<think>" + 推理 + "</think>"，而终态 CreateResult.content 里没有它们。
    原样转发有两个后果：终端用户会看到模型的内部推理；「delta 拼接 == final.response」
    这个 Go 侧依赖的不变式不再成立（客户端拼出来的文本与 done 里的对不上）。
    关掉思维链（LLM_DISABLE_THINKING）是性能手段，这里是不依赖网关参数的兜底。

    前提：两个标记都是整块给出的——"<think>" 是拼在首个推理 chunk 前面一次 yield 的，
    "</think>" 自己也单独 yield 一次，所以标记不会被切在两个 chunk 之间。需要跨 chunk
    保留的只有「当前是否在思维链里」这一个状态。
    """

    _OPEN = "<think>"
    _CLOSE = "</think>"

    def __init__(self) -> None:
        self._inside = False

    def feed(self, chunk: str) -> str:
        """吃掉一个 chunk，返回其中属于正文的部分（可能为空串）。"""
        if self._inside:
            end = chunk.find(self._CLOSE)
            if end < 0:
                return ""
            self._inside = False
            chunk = chunk[end + len(self._CLOSE):]

        start = chunk.find(self._OPEN)
        if start < 0:
            return chunk
        self._inside = True
        return chunk[:start]


class AgentServicer(agent_pb2_grpc.AgentServiceServicer):
    """gRPC Agent服务实现

    无状态推理：对话历史的真源在 Go 侧，每次请求由 ChatRequest.history 带过来，
    这里不读写 Redis。
    """

    def __init__(self):
        self.model_client = build_model_client()
        logger.info(
            "model client ready: model=%s base_url=%s thinking=%s",
            LLM_MODEL,
            OPENAI_BASE_URL or "(sdk default)",
            "disabled" if LLM_DISABLE_THINKING else "enabled",
        )

    async def ProcessChat(self, request, context):
        """流式处理聊天请求：先若干条 delta，最后一条必定是 final"""
        user_id = request.user_id
        logger.info(
            "ProcessChat user=%s history=%d msg=%s",
            user_id, len(request.history), request.message,
        )

        started = time.monotonic()
        intent = DEFAULT_INTENT  # 失败路径也要能打标签，先给个合法值

        try:
            if not request.message:
                raise ValueError("empty message")

            task = self._build_task(request)

            intent = await self._classify_intent(task)
            specialist = SPECIALISTS[intent]
            logger.info(
                "ProcessChat user=%s intent=%s agent=%s",
                user_id, intent, specialist["name"],
            )

            # 退款诉求走异步旁路：先把工单排进队列，拿到确定性工单号后再让专家回复，
            # 这样工单号能直接写进答复里，而建单/邮件/通知都在后台做。
            ticket_id = ""
            if intent == INTENT_REFUND_REQUEST:
                ticket_id = await self._open_refund_ticket(user_id, request.message)

            system_message = specialist["system_message"]
            if ticket_id:
                system_message += _TICKET_HINT.format(ticket_id=ticket_id)

            agent = AssistantAgent(
                name=specialist["name"],
                model_client=self.model_client,
                system_message=system_message,
                model_client_stream=True,
            )
            deltas = []
            final_text = None
            stripper = ThinkStripper()

            async for event in agent.run_stream(task=task, output_task_messages=False):
                if isinstance(event, ModelClientStreamingChunkEvent):
                    chunk = stripper.feed(event.content or "")
                    if not chunk:
                        continue
                    deltas.append(chunk)
                    yield agent_pb2.ChatEvent(delta=chunk)
                elif isinstance(event, TaskResult):
                    final_text = self._last_agent_text(event, specialist["name"])

            text = final_text if final_text is not None else "".join(deltas)
            logger.info(
                "ProcessChat done user=%s intent=%s chars=%d ticket=%s",
                user_id, intent, len(text), ticket_id or "-",
            )
            CHAT_REQUESTS.labels(intent=intent, outcome="ok").inc()
            CHAT_LATENCY.labels(intent=intent).observe(time.monotonic() - started)
            yield agent_pb2.ChatEvent(
                final=agent_pb2.ChatResponse(
                    response=text,
                    intent=intent,
                    # 建了工单就代表这轮需要后台/人工跟进，不再恒为 false。
                    requires_escalation=bool(ticket_id),
                )
            )

        except Exception as e:
            CHAT_REQUESTS.labels(intent=intent, outcome="error").inc()
            logger.exception("ProcessChat failed for user %s", user_id)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            yield agent_pb2.ChatEvent(
                final=agent_pb2.ChatResponse(response="系统错误，请稍后重试")
            )

    async def _open_refund_ticket(self, user_id: str, message: str) -> str:
        """把退款诉求排进队列，返回工单号；没建成则返回空串。

        工单号是「用户 + 归一化诉求」的确定性哈希（见 tasks/contract.py），所以
        不必等 worker 跑完就能告诉用户——worker 只是把它落库。去重也在这一层完成：
        窗口内重复的诉求会复用同一个工单号，不会再排一次队。
        """
        if enqueue_refund_job is None:
            return ""

        try:
            # 入队走的是同步 Redis/Celery 客户端，扔到线程里，别堵住事件循环——
            # 同一个进程还要同时服务其它用户的流式响应。
            outcome = await asyncio.to_thread(enqueue_refund_job, user_id, message)
        except Exception:
            logger.exception("enqueue refund job raised for user %s", user_id)
            return ""

        job_id = outcome.get("job_id") or ""
        if job_id:
            logger.info(
                "refund ticket ready user=%s job_id=%s queued=%s deduped=%s",
                user_id, job_id, outcome.get("queued"), outcome.get("deduped"),
            )
        elif outcome.get("error"):
            logger.warning("refund ticket not created for user %s: %s", user_id, outcome["error"])
        return job_id

    async def _classify_intent(self, task):
        """让分类器 agent 判一个标签。任何异常/超时/无法识别的输出都退回通用客服。

        分类失败不该让整轮对话失败：通用客服仍然能给出有意义的回答。
        """
        classifier = AssistantAgent(
            name=CLASSIFIER_NAME,
            model_client=self.model_client,
            system_message=CLASSIFIER_SYSTEM_MESSAGE,
        )
        try:
            result = await asyncio.wait_for(
                classifier.run(task=task), timeout=ROUTER_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            logger.warning(
                "intent classifier timed out after %ss, falling back to %s",
                ROUTER_TIMEOUT_SECONDS, DEFAULT_INTENT,
            )
            return DEFAULT_INTENT
        except Exception:
            logger.exception("intent classifier failed, falling back to %s", DEFAULT_INTENT)
            return DEFAULT_INTENT

        return self._normalize_intent(self._last_agent_text(result, CLASSIFIER_NAME))

    @staticmethod
    def _normalize_intent(raw):
        """把分类器的自由文本收敛成一个合法标签。

        模型可能返回 `order_query`、`order_query.`、带代码块的 ```order_query```，
        或者夹着解释的整句。先整体比对，再逐个找标签，都失败就退回默认值——
        宁可走通用客服，也不要把未知字符串塞进 final.intent。
        """
        text = (raw or "").strip().lower()
        if text in SPECIALISTS:
            return text
        for label in SPECIALISTS:
            if label in text:
                return label
        logger.warning(
            "unrecognized intent %r from classifier, falling back to %s", raw, DEFAULT_INTENT
        )
        return DEFAULT_INTENT

    @staticmethod
    def _build_task(request):
        """把 Go 过来的历史转成消息序列，末尾接上当前用户消息"""
        messages = []
        for m in list(request.history)[-MAX_HISTORY_MESSAGES:]:
            if not m.content:
                continue
            role = (m.role or "").strip().lower()
            source = "assistant" if role == "assistant" else "user"
            messages.append(TextMessage(source=source, content=m.content))
        messages.append(TextMessage(source="user", content=request.message))
        return messages

    @staticmethod
    def _last_agent_text(result, agent_name):
        """从 TaskResult 里取指定 agent 产出的最后一条文本。

        必须按 agent 名过滤：TaskResult.messages 里还混着用户消息，
        多 agent 场景下「最后一条」很可能是别的 agent 说的。
        """
        for msg in reversed(list(result.messages)):
            if isinstance(msg, TextMessage) and msg.source == agent_name:
                return msg.content
        return None


async def serve():
    """启动gRPC服务器"""
    try:
        # prometheus.yml 抓的就是这个端口。指标端点起不来不该拖垮业务端口。
        start_metrics_server(METRICS_PORT)
        logger.info("metrics exporter listening on :%d", METRICS_PORT)
    except Exception:
        logger.warning("metrics exporter failed on :%d", METRICS_PORT, exc_info=True)

    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=100))
    agent_pb2_grpc.add_AgentServiceServicer_to_server(AgentServicer(), server)

    server.add_insecure_port('[::]:50051')
    logger.info("Agent gRPC server listening on :50051")

    await server.start()
    await server.wait_for_termination()


if __name__ == '__main__':
    asyncio.run(serve())
