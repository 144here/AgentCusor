"""流式不变式：delta 拼接必须精确等于终态文本（done 帧里的 content）。

为什么这条最该先测：它是本项目唯一一条**错了用户能直接看见**的不变式。
客户端拿 delta 拼出答复，done 帧里另给一份全文；两者一旦不一致，用户屏幕上就是
错乱或半截的回复——而服务端日志正常、指标正常、健康检查正常。其它不变式（工单号、
去重、库号一致）坏掉时用户只是"少收到一条通知"，只有这条会直接毁掉这条回复。

它同时也是 ThinkStripper 存在的理由：模型带思维链时，autogen 会把 "<think>" + 推理
混进流式 chunk（而终态 content 里没有），原样转发就会破坏这条不变式。

测试**完全离线**：不经 gRPC、不经 Redis、不调 LLM。用脚本化的假模型客户端喂一段
chunk 流，驱动真实的 ProcessChat——不复制它内部的拼接逻辑，否则测的就是测试自己。
"""

import os
import sys
import types
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# agent_service.py 里写的是 sys.path.append('./proto')（cwd 相对），
# 所以先把工作目录摆正，这个测试才能从任意目录被调用。
os.chdir(ROOT)
sys.path.insert(0, ROOT)

from autogen_core.models import CreateResult, RequestUsage
from autogen_ext.models.replay import ReplayChatCompletionClient

import agent_service

INTENT = "order_query"
BODY = "您好，您的包裹已发出，预计明天送达。"

# chunk 流刻意按 autogen 对 DeepSeek 的真实切法构造：开标记 "<think>" 拼在首个推理
# chunk 前面一起 yield，闭标记 "</think>" 自己单独 yield 一次（见 ThinkStripper 的注释）。
# 别把开闭标记塞进同一个 chunk——那会落进 ThinkStripper 已知的那个脆弱路径
# （同一 chunk 内遇到开标记后丢弃剩余部分，于是闭标记被吃掉、后续正文全被吞）。
REASONING_CHUNKS = [
    "<think>用户问的是物流进度，",
    "应该由订单专员回答。",
    "</think>",
]
TEXT_CHUNKS = ["您好，", "您的包裹已发出，", "预计明天送达。"]
CHUNKS = REASONING_CHUNKS + TEXT_CHUNKS


def _create_result(content: str) -> CreateResult:
    return CreateResult(
        finish_reason="stop",
        content=content,
        usage=RequestUsage(prompt_tokens=1, completion_tokens=1),
        cached=False,
    )


class ScriptedModelClient(ReplayChatCompletionClient):
    """分类器走 create()、专家走 create_stream()，且**两者的文本必须不同**。

    ReplayChatCompletionClient 只能把同一个字符串既当流式 chunk 又当终态文本，
    而这里恰恰要「流里多出思维链、终态没有」——所以只重写 create_stream，
    create() 仍旧交给父类，用 INTENT 喂给意图分类器。
    """

    def __init__(self, chunks, final_text: str, intent: str = INTENT):
        super().__init__([intent])
        self._scripted_chunks = list(chunks)
        self._final_text = final_text

    async def create_stream(self, messages, **kwargs):  # type: ignore[override]
        for chunk in self._scripted_chunks:
            yield chunk
        yield _create_result(self._final_text)


async def _run_chat(chunks=CHUNKS, final_text=BODY, intent=INTENT):
    """跑一轮真实的 ProcessChat，返回 (deltas, final, events)。"""
    client = ScriptedModelClient(chunks, final_text, intent)
    # 只在构造期替换模型客户端，ProcessChat 本身走的是真实代码路径。
    with mock.patch.object(agent_service, "build_model_client", return_value=client):
        servicer = agent_service.AgentServicer()

    request = types.SimpleNamespace(user_id="u-invariant", message="我的包裹到哪了", history=[])
    events = [e async for e in servicer.ProcessChat(request, mock.MagicMock())]

    deltas = [e.delta for e in events if e.WhichOneof("event") == "delta"]
    final = next(e.final for e in events if e.WhichOneof("event") == "final")
    return deltas, final, events


class StreamInvariantTest(unittest.IsolatedAsyncioTestCase):
    async def test_delta_concat_equals_final(self):
        """核心断言：客户端拼出来的文本，必须逐字等于 done 帧里的全文。"""
        deltas, final, _ = await _run_chat()

        self.assertGreater(len(deltas), 1, "只收到一条 delta，说明没有真的流式回传")
        self.assertEqual("".join(deltas), final.response)

    async def test_thinking_never_reaches_the_client(self):
        """思维链既不进 delta，也不进终态。"""
        deltas, final, _ = await _run_chat()
        joined = "".join(deltas)

        for marker in ("<think>", "</think>"):
            self.assertNotIn(marker, joined, f"标记 {marker} 泄漏到客户端了")
            self.assertNotIn(marker, final.response)
        self.assertNotIn("订单专员", joined, "推理内容本身泄漏到客户端了")
        self.assertNotIn("订单专员", final.response)

    async def test_fixture_actually_exercises_the_stripper(self):
        """证明上面两条不是空跑：不剥离的话，拼接**确实**对不上终态。

        没有这条，把 fixture 里的思维链删掉，「拼接 == 终态」依然会绿——测试就退化成
        一句恒真的话。这条断言让 fixture 的有效性本身也被检查。
        """
        raw = "".join(CHUNKS)

        self.assertIn("<think>", raw)
        self.assertNotEqual(raw, BODY, "原始 chunk 流与终态相同，说明这个 fixture 没有覆盖剥离逻辑")

    async def test_intent_is_reported(self):
        """顺带钉住路由：分类器说 order_query，终态就必须报 order_query。"""
        _, final, _ = await _run_chat()
        self.assertEqual(final.intent, INTENT)

    async def test_long_reply_streams_without_gaps(self):
        """长回复（几百个 chunk）同样不能有缺口——这才是 Go 侧阻塞投递防的场景。"""
        long_body = "".join(f"第{i}句。" for i in range(300))
        deltas, final, _ = await _run_chat(chunks=[long_body[i:i + 4] for i in range(0, len(long_body), 4)],
                                           final_text=long_body)

        self.assertGreater(len(deltas), 100)
        self.assertEqual("".join(deltas), final.response)


if __name__ == "__main__":
    unittest.main()
