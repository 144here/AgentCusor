package service

import (
	"encoding/json"
	"testing"
)

// 这份载荷逐字对应 agent-service/tasks/celery_tasks.py 里 `_publish` 出去的 result dict。
//
// 字段名是 Go 与 Python 之间的第二处契约（第一处是频道名，由 scripts/check_contract.sh 钉）。
// Python 改名而这里没跟上**不会报错**——只会让 JobEvent 的对应字段静默变成零值，
// 屏幕上就是一个丢了 ticket_id 的 job 帧。与频道名漂移同族，所以这里也钉一份。
const workerPublishedFrame = `{
  "job_id": "rf_0123456789abcdef",
  "user_id": "u-42",
  "ticket_id": "rf_0123456789abcdef",
  "type": "refund_ticket",
  "status": "created",
  "reason": "东西是坏的",
  "created_at": "2026-01-01T00:00:00Z",
  "enqueued_at": "2026-01-01T00:00:00Z"
}`

func TestJobEventUnmarshalsEveryPublishedField(t *testing.T) {
	var ev JobEvent
	if err := json.Unmarshal([]byte(workerPublishedFrame), &ev); err != nil {
		t.Fatalf("无法解析 worker 发布的载荷: %v", err)
	}

	want := JobEvent{
		JobID:    "rf_0123456789abcdef",
		UserID:   "u-42",
		Type:     "refund_ticket",
		Status:   "created",
		TicketID: "rf_0123456789abcdef",
	}
	if ev != want {
		t.Fatalf("JobEvent 解析结果 = %+v, want %+v\n（某个 json tag 与 worker 发布的不一致？）", ev, want)
	}
}
