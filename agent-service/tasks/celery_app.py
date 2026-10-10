"""Celery 应用与队列契约。

原文件 `from config import CELERY_BROKER_URL, CELERY_RESULT_BACKEND` 引用的两个名字
在 config.py 里根本不存在，所以这个模块此前一 import 就 ImportError（连同引用它的
celery_tasks.py 一起是坏的）。本轮补上配置项并把它真正接起来。
"""

from celery import Celery

from config import CELERY_BROKER_URL, CELERY_RESULT_BACKEND

app = Celery(
    "cs_tasks",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    # 任务成功执行完才 ack：worker 中途挂掉时任务会被重投，
    # 这正是存储层必须幂等的原因（见 tools/database.py）。
    task_acks_late=True,
    worker_prefetch_multiplier=1,  # 减少任务饥饿
    task_time_limit=60,  # 单个任务最大运行时间，按需调整
    # worker 启动时显式加载任务模块；不依赖 autodiscover 的包名猜测。
    imports=("tasks.celery_tasks",),
    # 生产端就在聊天的关键路径上（agent 入队后才回复用户），所以 Redis 不可达时
    # 必须在几秒内失败返回，而不是默默重试几十秒把回复一起拖住。
    broker_connection_timeout=3,
    broker_transport_options={"socket_connect_timeout": 3, "socket_timeout": 3},
)
