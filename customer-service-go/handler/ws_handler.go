package handler

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/customer-service/connection"
	"github.com/customer-service/service"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = pongWait * 9 / 10
	maxMessageSize = 64 * 1024

	// chatTimeout 覆盖一整轮流式回复。LLM 首字节可能就要几秒，长回复还会更久，
	// 所以留足余量（骨架里的 10s 对 LLM 太短）。
	chatTimeout = 120 * time.Second

	// inboundBuffer 是每连接的入站缓冲：读循环投递后立刻回去继续收包，
	// 真正的处理由单独 worker 串行消费，保证同一用户轮次顺序。
	inboundBuffer = 8
)

// 服务端 → 客户端的帧类型
const (
	eventDelta = "delta"
	eventDone  = "done"
	eventError = "error"
	// eventJob 是异步任务（退款工单）的状态变更，由 Redis 订阅者推来，
	// 可能在 done 之后若干毫秒到达，也可能永远不到（用户已离线）。
	eventJob = "job"
)

// errClientGone 表示连接已结束，事件不必再发。
var errClientGone = errors.New("client gone")

// wsChatRequest 客户端 → 服务端：{"message": "..."}
type wsChatRequest struct {
	Message string `json:"message"`
}

// wsEvent 服务端 → 客户端。
// delta 帧带 content；done 帧带 content/intent/requires_escalation；error 帧带 message；
// job 帧带 status/job_id/ticket_id（不带 user_id——客户端本来就知道自己是谁）。
type wsEvent struct {
	Type               string `json:"type"`
	Content            string `json:"content,omitempty"`
	Intent             string `json:"intent,omitempty"`
	RequiresEscalation bool   `json:"requires_escalation"`
	Message            string `json:"message,omitempty"`
	Status             string `json:"status,omitempty"`
	JobID              string `json:"job_id,omitempty"`
	TicketID           string `json:"ticket_id,omitempty"`
	Timestamp          string `json:"timestamp,omitempty"`
}

type WSHandler struct {
	connPool    *connection.ConnectionPool
	agentClient *service.AgentClient
	history     *historyStore
	upgrader    websocket.Upgrader
}

func NewWSHandler(connPool *connection.ConnectionPool, agentClient *service.AgentClient) *WSHandler {
	return &WSHandler{
		connPool:    connPool,
		agentClient: agentClient,
		history:     newHistoryStore(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// 注意：放行任意来源，只够开发/联调。生产必须改成 Origin 白名单。
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (h *WSHandler) ServeWS(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "Missing user_id", http.StatusBadRequest)
		return
	}

	ws, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[ws] upgrade error: %v", err)
		return
	}

	// done 必须先建好再交给连接池：异步任务通知随时可能通过连接池找到这个连接，
	// 它得立刻就能拿到生命周期信号。
	done := make(chan struct{})
	uc := h.connPool.AddConnection(userID, ws, done)
	inbound := make(chan string, inboundBuffer)

	// ctx 跟随连接生命周期：一旦断开，worker 里在途的 gRPC 流立刻被取消，
	// 不让 LLM 白跑完一整轮。连接关闭由 writeLoop 负责，见那里的注释。
	ctx, cancel := context.WithCancel(context.Background())

	go h.writeLoop(uc, done)
	go h.worker(ctx, userID, uc, inbound, done)

	log.Printf("[ws] user %s connected (total=%d)", userID, h.connPool.GetConnectionCount())

	defer func() {
		h.connPool.RemoveConnection(userID)
		close(done)
		cancel()
		log.Printf("[ws] user %s disconnected (total=%d)", userID, h.connPool.GetConnectionCount())
	}()

	// 读超时靠 pong 续期，避免半开连接永久占着 goroutine
	ws.SetReadLimit(maxMessageSize)
	_ = ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, payload, err := ws.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[ws] user %s read error: %v", userID, err)
			}
			return
		}

		var req wsChatRequest
		if err := json.Unmarshal(payload, &req); err != nil || req.Message == "" {
			tryEnqueue(uc, wsEvent{
				Type:      eventError,
				Message:   `消息格式不正确，应为 {"message": "..."}`,
				Timestamp: time.Now().Format(time.RFC3339),
			})
			continue
		}

		select {
		case inbound <- req.Message:
		default:
			// 上一轮还在处理。提示帧一律非阻塞投递：读循环绝不能停摆，
			// 否则连 pong 都不再读，连接会被当成超时踢掉。
			tryEnqueue(uc, wsEvent{
				Type:      eventError,
				Message:   "上一条消息还在处理中，请稍候",
				Timestamp: time.Now().Format(time.RFC3339),
			})
		}
	}
}

// NotifyJobEvent 把异步任务（退款工单）的状态推给对应用户。
//
// 用户不在线就丢弃：工单已经落在 Redis 的 job:result:<id> 里，下次对话仍然查得到，
// 这类通知是尽力而为的，不值得为送达而阻塞唯一的 Redis 订阅者。
func (h *WSHandler) NotifyJobEvent(event service.JobEvent) {
	uc := h.connPool.GetConnection(event.UserID)
	if uc == nil {
		log.Printf("[ws] user %s offline, dropping job event %s", event.UserID, event.JobID)
		return
	}

	frame := wsEvent{
		Type:      eventJob,
		Status:    event.Status,
		JobID:     event.JobID,
		TicketID:  event.TicketID,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	if err := enqueue(uc, uc.Done, frame); err != nil {
		if !errors.Is(err, errClientGone) {
			log.Printf("[ws] user %s: push job event %s failed: %v", event.UserID, event.JobID, err)
		}
		return
	}
	log.Printf("[ws] user %s: job event %s (%s) pushed", event.UserID, event.JobID, event.Status)
}

// worker 是每连接唯一的处理协程，让同一用户的多轮请求串行执行——历史顺序才是确定的。
func (h *WSHandler) worker(ctx context.Context, userID string, uc *connection.UserConnection, inbound <-chan string, done <-chan struct{}) {
	for {
		select {
		case msg := <-inbound:
			h.processMessage(ctx, userID, msg, uc, done)
		case <-done:
			return
		}
	}
}

// processMessage 调 agent 流式出结果，并把 delta 逐条推给客户端。
func (h *WSHandler) processMessage(ctx context.Context, userID, message string, uc *connection.UserConnection, done <-chan struct{}) {
	ctx, cancel := context.WithTimeout(ctx, chatTimeout)
	defer cancel()

	// 快照里不含当前这条：当前消息单独走 ChatRequest.message，
	// 若也放进 history 会在 prompt 里出现两次。
	history := h.history.snapshot(userID)

	result, err := h.agentClient.ProcessChatStream(ctx, userID, message, history, func(delta string) error {
		return enqueue(uc, done, wsEvent{Type: eventDelta, Content: delta})
	})
	if err != nil {
		if errors.Is(err, errClientGone) || errors.Is(err, context.Canceled) {
			// 连接已断：正常收尾，不是错误
			return
		}
		log.Printf("[ws] agent error for %s: %v", userID, err)
		tryEnqueue(uc, wsEvent{
			Type:      eventError,
			Message:   "系统错误，请稍后重试",
			Timestamp: time.Now().Format(time.RFC3339),
		})
		return
	}

	// 只在成功时落历史：失败的一轮只有用户半句、没有回答，留着会污染下一轮 prompt。
	h.history.append(userID, "user", message)
	h.history.append(userID, "assistant", result.Text)

	_ = enqueue(uc, done, wsEvent{
		Type:               eventDone,
		Content:            result.Text,
		Intent:             result.Intent,
		RequiresEscalation: result.RequiresEscalation,
		Timestamp:          time.Now().Format(time.RFC3339),
	})
}

// enqueue 阻塞投递，直到事件入队或连接结束。
// delta 绝不能丢：队列只有 100 个槽而长回复有几百个 delta，一丢客户端拼出来的
// 文本就有缺口。阻塞的附带好处是反压会经 gRPC 流控一路传到 LLM，让它放慢。
func enqueue(uc *connection.UserConnection, done <-chan struct{}, ev wsEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	select {
	case uc.MessageQueue <- payload:
		return nil
	case <-done:
		return errClientGone
	}
}

// tryEnqueue 尽力投递，队列满就丢。只用于可丢弃的提示帧，绝不让读循环停摆。
func tryEnqueue(uc *connection.UserConnection, ev wsEvent) {
	payload, err := json.Marshal(ev)
	if err != nil {
		log.Printf("[ws] marshal %s event for %s: %v", ev.Type, uc.UserID, err)
		return
	}
	select {
	case uc.MessageQueue <- payload:
	default:
		log.Printf("[ws] user %s queue full, dropping %s notice", uc.UserID, ev.Type)
	}
}

// writeLoop 是该连接唯一的写入方（gorilla/websocket 不允许并发写），
// 同时由它负责关闭连接：写失败时关掉连接，阻塞在 ReadMessage 的读循环才会醒来退出。
func (h *WSHandler) writeLoop(uc *connection.UserConnection, done <-chan struct{}) {
	conn, ok := uc.WebSocketConn.(*websocket.Conn)
	if !ok {
		log.Printf("[ws] user %s: unexpected connection type %T", uc.UserID, uc.WebSocketConn)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case msg := <-uc.MessageQueue:
			payload, ok := msg.([]byte)
			if !ok {
				log.Printf("[ws] user %s: dropping non-[]byte queue entry %T", uc.UserID, msg)
				continue
			}
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				log.Printf("[ws] write to %s failed: %v", uc.UserID, err)
				return
			}
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}
