"""退款工单的存储层。

为什么用 Redis 而不是 SQLite/Postgres：本项目已经依赖 Redis，工单在这里是
「异步任务的可观测产物」，用 Redis 可以同时满足存储、结果回写（job:result:*）
和去重（job:seen:*）三种用途，不必为演示再引入第二个存储和一个 ORM。
真实系统里这一层会是 DB + 事务，接口形状不变。

**幂等性是这一层的主要职责**：ticket_id 由调用方传入（它就是确定性去重键），
同一 id 只建一次。Celery 开了 acks_late + autoretry，同一个任务被投递两次是
正常现象而不是异常——重复执行必须只产生一张工单。
"""

import time

from config import TICKET_TTL
from utils.redis_client import redis_client

_TICKET_PREFIX = "ticket:"
_USER_INDEX_PREFIX = "tickets:user:"
_MAX_TICKETS_PER_USER = 100


def _user_index_key(user_id: str) -> str:
    return f"{_USER_INDEX_PREFIX}{user_id}"


def create_refund_ticket(ticket_id: str, user_id: str, reason: str, order_id: str = "") -> dict:
    """建一张退款工单；已存在则原样返回，不覆盖也不重复计数。

    返回 {"ticket_id", "status", "created", "created_at"}。created=False 表示
    这次是重复投递，调用方应当据此跳过后续的邮件/通知等副作用。
    """
    r = redis_client()
    key = f"{_TICKET_PREFIX}{ticket_id}"

    existing = r.hgetall(key)
    if existing:
        return {
            "ticket_id": ticket_id,
            "status": existing.get("status", ""),
            "created": False,
            "created_at": existing.get("created_at", ""),
        }

    created_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    ticket = {
        "ticket_id": ticket_id,
        "user_id": user_id,
        "order_id": order_id,
        "reason": reason,
        "status": "open",
        "created_at": created_at,
    }

    # 一次管道写完，避免「工单写了但用户索引没写」的中间态。
    pipe = r.pipeline()
    pipe.hset(key, mapping=ticket)
    pipe.expire(key, TICKET_TTL)
    index = _user_index_key(user_id)
    pipe.lpush(index, ticket_id)
    pipe.ltrim(index, 0, _MAX_TICKETS_PER_USER - 1)
    pipe.expire(index, TICKET_TTL)
    pipe.execute()

    return {"ticket_id": ticket_id, "status": "open", "created": True, "created_at": created_at}


def get_ticket(ticket_id: str) -> dict | None:
    ticket = redis_client().hgetall(f"{_TICKET_PREFIX}{ticket_id}")
    return ticket or None


def list_tickets(user_id: str, limit: int = 20) -> list[dict]:
    """按创建时间倒序返回该用户的工单。"""
    ids = redis_client().lrange(_user_index_key(user_id), 0, max(limit - 1, 0))
    out = []
    for ticket_id in ids:
        ticket = get_ticket(ticket_id)
        if ticket:
            out.append(ticket)
    return out
