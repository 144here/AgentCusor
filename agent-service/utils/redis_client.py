"""共享的 Redis 客户端单例。

工单存储（tools/database.py）、入队/去重（tasks/producer.py）和任务回写
（tasks/celery_tasks.py）都需要一个业务库客户端。三处各建一个不仅浪费连接，
还会让 decode_responses 之类的设置随时间分叉。redis.Redis 自带连接池、线程安全，
按进程复用一个实例即可。
"""

import redis

from config import REDIS_DB, REDIS_HOST, REDIS_PORT

_client = None


def redis_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True
        )
    return _client
