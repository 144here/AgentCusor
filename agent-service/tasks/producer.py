"""把退款工单排进 Redis 队列（生产端）。

设计要点：**这个函数永远不抛异常**。队列是辅助路径——建工单失败绝不能让用户
看不到回答。任何失败都收敛成返回值里的 ``error``，由调用方决定怎么呈现。

去重在这里而不是在 worker 里做，是为了连「入队」都省掉；worker 侧另有一层
幂等（见 tools/database.py），因为 acks_late + autoretry 意味着同一个任务
被投递两次是正常的。
"""

import json
import logging
import time

from config import JOB_RESULT_TTL, REFUND_DEDUP_TTL
from tasks.celery_app import app
from tasks.contract import REFUND_TASK_NAME, dedup_key, job_id_for, result_key
from utils.redis_client import redis_client

logger = logging.getLogger(__name__)


def enqueue_refund_job(user_id: str, reason: str, order_id: str = "") -> dict:
    """入队一张退款工单。

    返回 ``{"job_id", "queued", "deduped", "error"}``：

    - ``queued=True``：本次真的排进了队列；
    - ``deduped=True``：去重窗口内已有同一诉求，复用已有工单号，未重复入队；
    - 两者都 False 且 ``job_id`` 为空：入队失败，调用方不应向用户承诺工单号。
    """
    if not user_id:
        return {"job_id": "", "queued": False, "deduped": False, "error": "missing user_id"}

    job_id = job_id_for(user_id, reason, order_id)
    r = redis_client()

    try:
        # SET NX：抢到锁的这次请求才有资格入队。
        acquired = r.set(dedup_key(job_id), "1", nx=True, ex=REFUND_DEDUP_TTL)
    except Exception as e:
        logger.warning("refund dedup guard unavailable: %s", e)
        return {"job_id": "", "queued": False, "deduped": False, "error": f"redis: {e}"}

    if not acquired:
        logger.info("refund job deduplicated job_id=%s user=%s", job_id, user_id)
        return {"job_id": job_id, "queued": False, "deduped": True, "error": ""}

    payload = {
        "job_id": job_id,
        "user_id": user_id,
        "reason": reason,
        "order_id": order_id,
        "enqueued_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    try:
        # 先落一条 queued 状态：worker 接手前「已入队」这件事就可观测，
        # 也让结果键在 worker 与生产端之间只有一个写入约定。
        r.set(
            result_key(job_id),
            json.dumps({**payload, "status": "queued"}),
            ex=JOB_RESULT_TTL,
        )
        app.send_task(REFUND_TASK_NAME, kwargs=payload, task_id=job_id)
    except Exception as e:
        # 入队失败必须把去重锁连同状态一起放掉，否则窗口内这个诉求再也排不进来，
        # 用户会拿到一个永远不存在的工单号。
        _release(r, job_id)
        logger.warning("enqueue refund job failed job_id=%s: %s", job_id, e)
        return {"job_id": "", "queued": False, "deduped": False, "error": str(e)}

    logger.info("refund job queued job_id=%s user=%s", job_id, user_id)
    return {"job_id": job_id, "queued": True, "deduped": False, "error": ""}


def _release(r, job_id: str) -> None:
    """回滚去重锁与占位状态；清理本身失败也只能记日志，不能再抛。"""
    try:
        r.delete(dedup_key(job_id), result_key(job_id))
    except Exception as e:
        logger.warning("rollback of job %s failed: %s", job_id, e)
