package connection

import "testing"

func TestAddGetRemoveAndCount(t *testing.T) {
	pool := NewConnectionPool()
	uc := pool.AddConnection("u1", "fake-ws", make(chan struct{}))

	if got := pool.GetConnection("u1"); got != uc {
		t.Fatalf("GetConnection 未返回刚加入的连接")
	}
	if got := pool.GetConnectionCount(); got != 1 {
		t.Fatalf("连接数 = %d, want 1", got)
	}

	pool.RemoveConnection("u1")
	if got := pool.GetConnection("u1"); got != nil {
		t.Fatalf("移除后 GetConnection 应为 nil, got %v", got)
	}
	if got := pool.GetConnectionCount(); got != 0 {
		t.Fatalf("移除后连接数 = %d, want 0", got)
	}
}

// 同一 userID 再次连接必须覆盖旧连接、且计数不增长。
// 网关的 job 帧投递按 user_id 查表（ws_handler.go 的 NotifyJobEvent），
// 若这里变成「追加」或「保留两条」，投递就会命中一个已被顶掉的连接。
func TestAddConnectionOverwritesSameUser(t *testing.T) {
	pool := NewConnectionPool()
	first := pool.AddConnection("u1", "ws-1", make(chan struct{}))
	second := pool.AddConnection("u1", "ws-2", make(chan struct{}))

	if first == second {
		t.Fatal("两次 Add 返回了同一个连接，测试前提不成立")
	}
	if pool.GetConnection("u1") != second {
		t.Fatalf("同一 userID 再次 Add 未覆盖旧连接")
	}
	if got := pool.GetConnectionCount(); got != 1 {
		t.Fatalf("同 userID 覆盖后连接数 = %d, want 1", got)
	}
}

// GetConnection 对不存在的 user 返回 nil（NotifyJobEvent 依赖它判「用户离线」）。
func TestGetConnectionMissingReturnsNil(t *testing.T) {
	if got := NewConnectionPool().GetConnection("nobody"); got != nil {
		t.Fatalf("不存在的 user 应返回 nil, got %v", got)
	}
}
