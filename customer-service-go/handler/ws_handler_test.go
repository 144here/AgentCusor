package handler

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/customer-service/connection"
	"github.com/customer-service/service"
)

// 造一个只带 MessageQueue / Done 的连接，足够驱动 enqueue / tryEnqueue / NotifyJobEvent。
func newTestUserConn(userID string, queueCap int) *connection.UserConnection {
	return &connection.UserConnection{
		UserID:       userID,
		MessageQueue: make(chan interface{}, queueCap),
		Done:         make(chan struct{}),
	}
}

func decode(t *testing.T, payload interface{}) wsEvent {
	t.Helper()
	b, ok := payload.([]byte)
	if !ok {
		t.Fatalf("队列里存的不是 []byte: %T", payload)
	}
	var ev wsEvent
	if err := json.Unmarshal(b, &ev); err != nil {
		t.Fatalf("帧不是合法 JSON: %v", err)
	}
	return ev
}

func TestEnqueueDelivers(t *testing.T) {
	uc := newTestUserConn("u1", 4)

	if err := enqueue(uc, uc.Done, wsEvent{Type: eventDelta, Content: "你好"}); err != nil {
		t.Fatalf("enqueue 返回错误: %v", err)
	}

	select {
	case msg := <-uc.MessageQueue:
		if ev := decode(t, msg); ev.Type != eventDelta || ev.Content != "你好" {
			t.Fatalf("送达的帧不对: %+v", ev)
		}
	default:
		t.Fatal("enqueue 声称成功但队列里没有东西")
	}
}

// delta 走的是阻塞投递：队列满时不能返回、也不能丢，只能等连接结束（done 关闭）才放弃。
// 这条是本项目端到端反压的根基——改成非阻塞（select/default）就会漏掉 delta，
// 客户端拼出的文本出现缺口。
func TestEnqueueBlocksWhileQueueFullAndGivesUpOnlyWhenClientGone(t *testing.T) {
	uc := newTestUserConn("u1", 1)
	uc.MessageQueue <- []byte(`{"type":"delta"}`) // 填满唯一的槽

	returned := make(chan error, 1)
	go func() { returned <- enqueue(uc, uc.Done, wsEvent{Type: eventDelta, Content: "x"}) }()

	// 队列满、连接仍在：投递方必须阻塞，不能立刻返回。
	select {
	case err := <-returned:
		t.Fatalf("队列满时 enqueue 提前返回了: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// 连接结束：投递方必须立刻放弃并报 errClientGone。
	close(uc.Done)
	select {
	case err := <-returned:
		if !errors.Is(err, errClientGone) {
			t.Fatalf("连接结束后 err = %v, want errClientGone", err)
		}
	case <-time.After(time.Second):
		t.Fatal("done 关闭后 enqueue 仍然阻塞，投递方会永久卡住")
	}
}

// 可丢的提示帧走 tryEnqueue：队列满必须立刻丢弃并返回，绝不能阻塞读循环。
func TestTryEnqueueDropsWhenFullWithoutBlocking(t *testing.T) {
	uc := newTestUserConn("u1", 1)
	tryEnqueue(uc, wsEvent{Type: eventError, Message: "first"})

	done := make(chan struct{})
	go func() {
		tryEnqueue(uc, wsEvent{Type: eventError, Message: "second"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("tryEnqueue 在队列满时阻塞了（读循环会因此停摆）")
	}

	if got := len(uc.MessageQueue); got != 1 {
		t.Fatalf("队列长度 = %d, want 1（第二帧应被丢弃而不是入队）", got)
	}
}

// 线协议形状：requires_escalation 没有 omitempty，所以每个帧上都在（delta 帧恒为 false）；
// 空的可选字段则必须被省略。改动其中任意一项都会让客户端解析出错。
func TestDeltaFrameJSONShape(t *testing.T) {
	b, err := json.Marshal(wsEvent{Type: eventDelta, Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}

	if _, ok := raw["requires_escalation"]; !ok {
		t.Fatalf("delta 帧缺少 requires_escalation（该字段刻意没有 omitempty）: %s", b)
	}
	for _, k := range []string{"intent", "message", "status", "job_id", "ticket_id", "timestamp"} {
		if _, ok := raw[k]; ok {
			t.Fatalf("空字段 %q 未被 omitempty 省略: %s", k, b)
		}
	}
	if string(raw["type"]) != `"delta"` || string(raw["content"]) != `"hi"` {
		t.Fatalf("帧内容不对: %s", b)
	}
}

// done 帧必须带全文、意图与升级标记——客户端只有这一帧能拿到它们。
func TestDoneFrameCarriesIntentAndEscalation(t *testing.T) {
	b, _ := json.Marshal(wsEvent{
		Type: eventDone, Content: "全文", Intent: "refund_request", RequiresEscalation: true,
	})
	var ev wsEvent
	if err := json.Unmarshal(b, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Content != "全文" || ev.Intent != "refund_request" || !ev.RequiresEscalation {
		t.Fatalf("done 帧丢字段: %+v", ev)
	}
}

// 同步/异步两条路的合流点：worker 推来的 job 事件，必须经连接池按 user_id 落到目标用户。
// 改成「广播给所有连接」或查错键，这条就红——而这正是冒充/串号类缺陷的现场。
func TestNotifyJobEventRoutesToTargetUserOnly(t *testing.T) {
	pool := connection.NewConnectionPool()
	ucA := pool.AddConnection("userA", nil, make(chan struct{}))
	ucB := pool.AddConnection("userB", nil, make(chan struct{}))

	h := &WSHandler{connPool: pool}
	h.NotifyJobEvent(service.JobEvent{
		JobID: "rf_x", UserID: "userA", Type: "refund_ticket", Status: "created", TicketID: "rf_x",
	})

	select {
	case msg := <-ucA.MessageQueue:
		ev := decode(t, msg)
		if ev.Type != eventJob || ev.JobID != "rf_x" || ev.TicketID != "rf_x" || ev.Status != "created" {
			t.Fatalf("job 帧内容不对: %+v", ev)
		}
	default:
		t.Fatal("job 事件没有送达目标用户")
	}

	if got := len(ucB.MessageQueue); got != 0 {
		t.Fatalf("非目标用户收到了 job 事件, 队列长度 = %d", got)
	}
}

// 用户离线时必须安静丢弃（工单已落在 job:result:<id>，通知是尽力而为的）——不得 panic。
func TestNotifyJobEventOfflineUserIsNoOp(t *testing.T) {
	h := &WSHandler{connPool: connection.NewConnectionPool()}
	h.NotifyJobEvent(service.JobEvent{JobID: "rf_y", UserID: "nobody", Status: "created"})
}
