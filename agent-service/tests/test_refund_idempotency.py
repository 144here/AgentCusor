"""重投只应产生：一张工单、一封邮件、一个 job 帧。

为什么这条先测：`task_acks_late` + `autoretry` 让「同一个任务被执行两次」是**正常现象**
而不是异常——worker 重启、回执超时、Redis 抖动都会导致重投。所以幂等不是防御性编程，
而是这条链路正确性的一部分：用户看到两封「工单已创建」或屏幕上两个一模一样的 job 帧
（工单号相同，像开了两张单），是**直接可见**的错误。

这一条同时覆盖三层去重，它们各自的机制不同：

  1. 存储层 `create_refund_ticket`：同一 ticket_id 只建一次；
  2. 副作用层 `job:step:<id>:<step>` 的 SET NX 认领：邮件/通知/推送各认领一次；
  3. 结果层 `job:result:<id>`：幂等状态，重写无副作用，因此**刻意不认领**。

「一次投两次」用 `apply(task_id=...)` 模拟，同一个 task_id 就是同一个 Celery 任务——
比手工调用函数更接近真实重投，因为 autoretry 包装与 `self.request` 都会照常生效。
"""

import json
import os
import sys
import time
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

from tasks import celery_tasks as ct
from tasks.contract import JOB_EVENTS_CHANNEL, dedup_key, job_id_for, result_key
from tools.database import get_ticket, list_tickets
from utils.redis_client import redis_client


def _step_key(job_id: str, step: str) -> str:
    """与 celery_tasks 里的拼法一致。步骤名从那边 import，不在这里硬写字符串。"""
    return f"job:step:{job_id}:{step}"


class RefundRedeliveryTest(unittest.TestCase):
    def setUp(self):
        self.r = redis_client()
        # 唯一化 job_id，既避免和库里既有工单互相影响，也让本测试的键能被精确清掉。
        self.user_id = f"test-redeliver-{int(time.time() * 1000)}"
        self.reason = "东西是坏的"
        self.job_id = job_id_for(self.user_id, self.reason)
        self.payload = {
            "job_id": self.job_id,
            "user_id": self.user_id,
            "reason": self.reason,
            "order_id": "",
            "enqueued_at": "2026-01-01T00:00:00Z",
        }

        self.created_flags = []
        self.emails = []
        self.notifies = []
        self.frames = []

        self.pubsub = self.r.pubsub()
        self.pubsub.subscribe(JOB_EVENTS_CHANNEL)
        self._drain()  # 丢掉 subscribe 的确认帧

    def tearDown(self):
        self.pubsub.close()
        keys = [
            f"ticket:{self.job_id}",
            f"tickets:user:{self.user_id}",
            dedup_key(self.job_id),
            result_key(self.job_id),
        ]
        keys += [
            _step_key(self.job_id, step)
            for step in (ct._STEP_EMAIL, ct._STEP_NOTIFY, ct._STEP_PUBLISH)
        ]
        self.r.delete(*keys)

    def _drain(self, timeout: float = 1.0) -> None:
        """把频道上属于本次测试的 job 帧收进 self.frames。"""
        while True:
            message = self.pubsub.get_message(timeout=timeout)
            if message is None:
                return
            if message.get("type") != "message":
                continue
            data = json.loads(message["data"])
            if data.get("job_id") == self.job_id:
                self.frames.append(data)

    def _deliver(self) -> None:
        """把同一个 task_id 投递一次，记录三类副作用各发生了几次。"""
        real_create = ct.create_refund_ticket

        def spy_create(*args, **kwargs):
            ticket = real_create(*args, **kwargs)
            self.created_flags.append(ticket["created"])
            return ticket

        with mock.patch.object(ct, "create_refund_ticket", side_effect=spy_create), \
                mock.patch.object(ct, "send_email", side_effect=lambda **kw: self.emails.append(kw)), \
                mock.patch.object(ct, "notify_support_team",
                                  side_effect=lambda *a, **kw: self.notifies.append((a, kw))):
            ct.create_refund_job.apply(kwargs=self.payload, task_id=self.job_id).get()

        self._drain()

    def test_redelivery_yields_exactly_one_of_each(self):
        self._deliver()
        self._deliver()  # 同一个 task_id 再投一次 = Celery 的重投

        # 第一层：存储。第二次投递不能再建一张工单。
        self.assertEqual(self.created_flags, [True, False],
                         "第二次投递又建了一张工单（或第一次就没建成）")
        tickets = list_tickets(self.user_id)
        self.assertEqual(len(tickets), 1, f"该用户名下工单数应为 1，实际 {len(tickets)}")
        ticket = get_ticket(self.job_id)
        self.assertEqual(ticket["status"], "open")
        self.assertEqual(ticket["reason"], self.reason)
        self.assertEqual(ticket["user_id"], self.user_id)

        # 第二层：副作用。邮件与客服通知各只发一次。
        self.assertEqual(len(self.emails), 1, "重投多发了一封「工单已创建」邮件")
        self.assertEqual(len(self.notifies), 1, "重投多通知了一次客服")

        # 第三层：结果键是幂等的，重写只是刷新 TTL，状态仍然只有一份。
        stored = json.loads(self.r.get(result_key(self.job_id)))
        self.assertEqual(stored["status"], "created")
        self.assertEqual(stored["ticket_id"], self.job_id)

        # 直接可见的那一条：用户只应收到一个 job 帧。
        self.assertEqual(len(self.frames), 1, f"重投推了 {len(self.frames)} 个 job 帧")
        self.assertEqual(self.frames[0]["ticket_id"], self.job_id)
        self.assertEqual(self.frames[0]["status"], "created")

    def test_published_frame_carries_the_full_contract(self):
        """发布出去的 job 帧必须带齐 Go 侧要读的字段名。

        Go 按 json tag 读 `job_id/user_id/type/status/ticket_id`
        （customer-service-go/service/job_subscriber.go 的 JobEvent）。改这边而 Go 没跟上，
        **不报错**，只会让对应字段静默变零值，用户屏幕上是一个丢字段的 job 帧——
        与频道名漂移同族。这里从生产端钉字段名，Go 侧另有一条测试钉消费端
        （service/job_subscriber_test.go）。
        """
        self._deliver()

        self.assertEqual(len(self.frames), 1, "本轮应恰好推一个 job 帧")
        required = {"job_id", "user_id", "ticket_id", "type", "status"}
        missing = required - set(self.frames[0])
        self.assertFalse(missing, f"job 帧缺少 Go 侧要读的字段: {missing}")

    def test_claims_are_what_suppress_the_duplicates(self):
        """反证：认领键一清掉，重投就真的会重复发。

        没有这条，上面那个测试可能只是在跑一条「本来就不会重复」的路径。
        清掉 job:step:* 后邮件与推送都变成两份，才证明认领是**有承载力**的，
        而不是一个谁也没读的装饰（本项目已经修过三处这类装饰了）。
        """
        self._deliver()
        for step in (ct._STEP_EMAIL, ct._STEP_NOTIFY, ct._STEP_PUBLISH):
            self.r.delete(_step_key(self.job_id, step))
        self._deliver()

        self.assertEqual(len(self.emails), 2,
                         "认领被清掉后仍然只发一封，说明抑制不是来自 job:step:* 认领")
        self.assertEqual(len(self.frames), 2)
        # 工单这一层不依赖认领，所以清掉认领也不会多建。
        self.assertEqual(self.created_flags, [True, False])
        self.assertEqual(len(list_tickets(self.user_id)), 1)

    def test_publish_failure_releases_the_claim(self):
        """推送失败必须撤销认领，否则自动重试会被判成「这步做过」而永久跳过。

        这是本项目最怕的那类故障：不报错、只是少做一件事（用户永远收不到工单通知）。
        走的是真实 autoretry（`.apply()` 在 eager 模式下会把重试跑完），跑完仍要没有
        残留的认领键——而且**每一次尝试**都撤销过，不只是最后一次。
        """
        with mock.patch.object(ct, "_publish", side_effect=RuntimeError("redis 挂了")):
            result = ct.create_refund_job.apply(kwargs=self.payload, task_id=self.job_id)

        self.assertTrue(result.failed(), "推送一直失败，任务本应最终失败")
        self.assertFalse(self.r.exists(_step_key(self.job_id, ct._STEP_PUBLISH)),
                         "推送失败后认领没被撤销，重试将永久跳过这一步")

    def test_released_claim_lets_a_later_retry_publish(self):
        """承接上一条：撤销之后，下一次投递必须真的能把帧推出去。

        否则「撤销认领」这个动作就无法和「永久跳过」区分开——只断言键不在了，
        并不能说明用户最终会收到通知。
        """
        with mock.patch.object(ct, "_publish", side_effect=RuntimeError("redis 挂了")):
            ct.create_refund_job.apply(kwargs=self.payload, task_id=self.job_id)

        self._deliver()  # 换回可用的实现再投一次

        self.assertEqual(len(self.frames), 1, "认领撤销后重投仍然没把 job 帧推出去")
        self.assertEqual(self.frames[0]["ticket_id"], self.job_id)

    def test_result_is_written_before_the_frame_is_published(self):
        """顺序不变式：先落可回查的 job:result，再推帧。

        反了的话，客户端收到帧立刻回查结果键，会读到 "queued" 而不是 "created"。
        用「推帧的那一刻结果键必须已经存在」来钉住顺序，而不是去比时间戳。
        """
        seen = {}

        def spy_publish(result):
            seen["status_at_publish"] = json.loads(self.r.get(result_key(self.job_id)))["status"]

        with mock.patch.object(ct, "_publish", side_effect=spy_publish):
            ct.create_refund_job.apply(kwargs=self.payload, task_id=self.job_id).get()

        self.assertEqual(seen.get("status_at_publish"), "created")


if __name__ == "__main__":
    unittest.main()
