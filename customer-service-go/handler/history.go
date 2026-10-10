package handler

import (
	"sync"

	"github.com/customer-service/agentpb"
)

// maxHistoryMessages 是每个用户保留、并随请求带给 agent 的最大消息条数
// （user + assistant 合计），与 Python 侧 MAX_HISTORY_MESSAGES 对应。
const maxHistoryMessages = 20

// historyStore 保存每个用户的对话历史。
//
// 历史刻意放在进程内、独立于 WebSocket 连接：否则客户端一断线重连上下文就没了。
// 已知局限（本轮接受）：不淘汰闲置用户，内存随 userID 数量增长；
// 多实例部署时各实例历史不共享，需要粘性会话或外部存储。
type historyStore struct {
	mu       sync.Mutex
	sessions map[string][]*agentpb.Message
}

func newHistoryStore() *historyStore {
	return &historyStore{sessions: make(map[string][]*agentpb.Message)}
}

// append 追加一条消息，并把该用户的历史裁剪到最近 maxHistoryMessages 条。
func (s *historyStore) append(userID, role, content string) {
	if userID == "" || content == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := append(s.sessions[userID], &agentpb.Message{Role: role, Content: content})
	if len(msgs) > maxHistoryMessages {
		msgs = msgs[len(msgs)-maxHistoryMessages:]
	}
	s.sessions[userID] = msgs
}

// snapshot 返回该用户历史的副本，供本次请求使用。
// 返回副本而不是切片本身：调用方随后会在别的 goroutine 里读它，而 append 会原地
// 改写底层数组。消息本身按 protobuf 约定只读，多 goroutine 并发读是安全的。
func (s *historyStore) snapshot(userID string) []*agentpb.Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := s.sessions[userID]
	if len(msgs) == 0 {
		return nil
	}
	out := make([]*agentpb.Message, len(msgs))
	copy(out, msgs)
	return out
}
