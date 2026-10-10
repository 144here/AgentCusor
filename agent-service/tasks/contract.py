"""队列契约：入队内容、结果回写路由、去重键。

三个契约集中在这一个文件里，因为生产端（tasks/producer.py）和消费端
（tasks/celery_tasks.py）必须严格一致地实现——分散在两处就一定会漂移。

1. **入队内容**：Celery 任务 ``REFUND_TASK_NAME``，kwargs 为
   ``{job_id, user_id, reason, order_id, enqueued_at}``。刻意自包含：用 redis-cli
   直接看 broker 里的报文就能读懂，不依赖 Celery 的私有 header。
2. **结果回写路由**：worker 先写 ``job:result:<job_id>``（带 TTL），再 PUBLISH 到
   ``JOB_EVENTS_CHANNEL``；Go 网关订阅该频道并把事件推给对应的 WebSocket 用户。
   写键与 PUBLISH 分开，是因为「最终状态可回查」和「在线用户即时收到」是两种不同的
   需求：用户掉线时 PUBLISH 会丢，而 ``job:result:*`` 还在。
3. **去重键**：``job_id`` 由 (user_id, 归一化后的诉求, order_id) 确定性算出，
   同时用作 Celery task_id 和 Redis 键的组成部分；入队前用 ``SET NX`` 抢
   ``job:seen:<job_id>``。确定性 id 还有个好处：agent 不必等 worker 跑完，
   就能把工单号直接告诉用户（worker 只是把它落库）。
"""

import hashlib
import re

# Celery 任务名。生产端和消费端都从这里取，不各写一份字符串。
REFUND_TASK_NAME = "refund.create_ticket"

# Go 侧订阅的频道名必须与此一致（customer-service-go/service/job_subscriber.go）。
# `make check-contract` 会在构建期断言两边相等——这条漂移不报错，只是网关静默收不到事件。
JOB_EVENTS_CHANNEL = "job:events"

_KEY_PREFIX = "job:"

_WHITESPACE = re.compile(r"\s+")


def normalize_reason(reason: str) -> str:
    """归一化用户措辞，让「东西是坏的」和「东西是坏的 」命中同一张工单。

    只做空白折叠与大小写统一，不做语义归一——判断「坏了」和「有破损」是不是同一件事
    需要模型或规则库，那是另一个问题；这一层保守一点，宁可多开一张工单。
    """
    return _WHITESPACE.sub(" ", reason or "").strip().lower()


def job_id_for(user_id: str, reason: str, order_id: str = "") -> str:
    """确定性工单号：同一用户、同一诉求、同一订单永远得到同一个 id。"""
    raw = f"{user_id}|{normalize_reason(reason)}|{order_id}"
    return "rf_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def dedup_key(job_id: str) -> str:
    """入队去重锁。抢到才入队，抢不到说明窗口内已经排过同一个任务。"""
    return f"{_KEY_PREFIX}seen:{job_id}"


def result_key(job_id: str) -> str:
    """任务结果（最终状态）。用户掉线时事件会丢，这个键不会。"""
    return f"{_KEY_PREFIX}result:{job_id}"
