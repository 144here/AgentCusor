"""退款工单的异步执行体（消费端）。

原文件 import 了不存在的 `tools.database`、不存在的 `config.CELERY_BROKER_URL`，
外加一串与任何调用方都不相连的示例任务，一 import 就报错。本轮改写成一条真实链路：

    建工单 → 发确认邮件 → 通知客服 → 回写 job:result:<id> → PUBLISH 到 job:events

**幂等性分两层**，因为 `task_acks_late` + `autoretry` 让「同一个任务跑两次」成为
正常现象而不是异常：

- 工单本身：`create_refund_ticket` 对同一 ticket_id 只建一次；
- 外发副作用（邮件、通知、事件推送）：用 `job:step:<id>:<step>` 的 SET NX 认领，
  重投时跳过——否则用户会收到两封「工单已创建」、或两个一样的 job 帧。

三条支路的失败策略**刻意不同**，别照抄其中一条：

- 事件推送（`_publish`）：认领后失败会 `_release_step` 撤销认领。因为抛异常意味着
  这一帧**没送出去**，重试应该再发一次；若把认领留下，重试会被误判成「已做过」而
  永久跳过，用户就永远收不到通知了。
- 邮件与通知：认领后失败**不撤销**，即 at-most-once。它们的失败可能是「其实已经发出去了、
  只是回执超时」，撤销就会重复发——而「不重复发」正是这两步存在的理由。代价是极端情况下
  可能漏一封，这个取舍是有意选的（推送那一路有 `job:result:<id>` 可回查兜底，邮件没有）。
"""

import json

import structlog

from config import JOB_RESULT_TTL
from tasks.celery_app import app
from tasks.contract import JOB_EVENTS_CHANNEL, REFUND_TASK_NAME, result_key
from tools.database import create_refund_ticket
from utils.logger import configure_logging
from utils.metrics import REQUESTS
from utils.redis_client import redis_client
from utils.retry import retry

configure_logging("INFO")
log = structlog.get_logger(__name__)

_STEP_EMAIL = "email"
_STEP_NOTIFY = "support"
# 事件推送同样要认领。它是这一步里唯一「直接到达用户」的副作用：工单与邮件都被
# step 认领抑制了，但 PUBLISH 原先没有，所以重投时客户端会收到两个一模一样的
# job 帧（工单号相同，看起来像开了两张单）。
_STEP_PUBLISH = "published"


@app.task(
    bind=True,
    name=REFUND_TASK_NAME,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def create_refund_job(self, job_id: str, user_id: str, reason: str, order_id: str = "", enqueued_at: str = ""):
    """为一次退款诉求建工单并做完后续动作。"""
    REQUESTS.labels(path="/task/refund", status="started").inc()
    log.info("refund_job_started", job_id=job_id, user_id=user_id, celery_task_id=self.request.id)

    try:
        ticket = create_refund_ticket(job_id, user_id, reason, order_id)
        if not ticket["created"]:
            # 重投（或生产端去重窗口已过、用户又发了一次）：工单已存在，不再新建。
            log.info("refund_ticket_already_exists", job_id=job_id, created_at=ticket["created_at"])

        if _claim_step(job_id, _STEP_EMAIL):
            send_email(
                to=user_id,  # demo 里把 user_id 当收件地址；真实实现查用户资料
                subject=f"退货工单 {job_id} 已创建",
                body=f"我们已收到您的退款申请（{reason}），工单号 {job_id}，客服会尽快跟进。",
            )

        if _claim_step(job_id, _STEP_NOTIFY):
            notify_support_team(job_id, order_id)

        result = {
            "job_id": job_id,
            "user_id": user_id,
            "ticket_id": job_id,
            "type": "refund_ticket",
            "status": "created",
            "reason": reason,
            "created_at": ticket["created_at"],
            "enqueued_at": enqueued_at,
        }
        # 顺序不能反：先落可回查的最终状态，再推事件。否则客户端收到事件后
        # 立刻回查 job:result:* 可能还是 "queued"。
        # 结果键是幂等状态、重写无副作用（顺带刷新 TTL），所以不认领；
        # 真正需要去重的只有 PUBLISH——它会直接变成用户屏幕上的一个 job 帧。
        _write_result(result)
        if _claim_step(job_id, _STEP_PUBLISH):
            try:
                _publish(result)
            except Exception:
                # 认领了却没发出去：撤销认领，否则自动重试会因「这步做过」而跳过，
                # 用户就永远收不到通知了——那比多发一帧更糟。
                _release_step(job_id, _STEP_PUBLISH)
                raise

        REQUESTS.labels(path="/task/refund", status="ok").inc()
        log.info("refund_job_done", job_id=job_id, user_id=user_id)
        return result

    except Exception as e:
        REQUESTS.labels(path="/task/refund", status="error").inc()
        log.exception("refund_job_failed", job_id=job_id, err=str(e))
        raise


def _claim_step(job_id: str, step: str) -> bool:
    """认领一个副作用步骤；返回 False 表示这步已经做过了（重投场景）。"""
    claimed = redis_client().set(f"job:step:{job_id}:{step}", "1", nx=True, ex=JOB_RESULT_TTL)
    return bool(claimed)


def _release_step(job_id: str, step: str) -> None:
    """撤销认领，只在「认领成功但动作失败」时调用。

    不撤销的话，自动重试会看到认领已存在而跳过这一步，于是这个副作用永远不会发生
    ——那正是本项目一直在防的「不报错、只是少做了一件事」。生产端的去重锁也是
    同样的处置（抢到锁但入队失败就回滚锁）。
    """
    redis_client().delete(f"job:step:{job_id}:{step}")


def _write_result(result: dict) -> None:
    redis_client().set(result_key(result["job_id"]), json.dumps(result), ex=JOB_RESULT_TTL)


def _publish(result: dict) -> None:
    """推给 Go 网关（它订阅这个频道并转发给对应的 WebSocket 用户）。"""
    receivers = redis_client().publish(JOB_EVENTS_CHANNEL, json.dumps(result))
    log.info("refund_event_published", job_id=result["job_id"], receivers=receivers)


def send_email(to: str, subject: str, body: str) -> bool:
    """轻量 demo：真实实现换成 SMTP / SendGrid / SES。重试装饰器保证发送稳定性。"""

    @retry(tries=4, delay=0.5, backoff=2)
    def _send() -> bool:
        # 真实实现：smtplib / requests.post(MAILER_URL, json={...})
        log.info("email_sent", to=to, subject=subject, body=body)
        return True

    return _send()


def notify_support_team(ticket_id: str, order_id: str) -> None:
    """通知客服（真实实现是 IM webhook 或内部工单系统的回调）。"""
    log.info("support_notified", ticket_id=ticket_id, order_id=order_id)
