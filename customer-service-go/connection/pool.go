package connection

import (
	"sync"
)

// UserConnection 用户连接信息
type UserConnection struct {
    UserID         string
    WebSocketConn  interface{} // *websocket.Conn
    MessageQueue   chan interface{}
    // Done 在连接结束时关闭。任何向 MessageQueue 的阻塞投递都必须同时监听它，
    // 否则给一个已经没人读的队列发消息会永久卡住投递方（异步任务通知走的就是这条路）。
    Done chan struct{}
}

// ConnectionPool 连接池 - 支持高并发
type ConnectionPool struct {
    mu          sync.RWMutex
    connections map[string]*UserConnection
}

// 这里原先有个 maxSize 字段（由 config.MaxConnPool=1000 传入）但从未被读取，
// 连接数上限是假的。已删除而不是补一个真上限：那需要先定「满了怎么办」的策略
// （拒绝新连接？踢掉最久未活跃的？），本项目没有任何场景需要它——演示规模是
// 个位数连接，而凭空加一条未被需求覆盖的拒绝路径只会多一个没测过的分支。
func NewConnectionPool() *ConnectionPool {
    return &ConnectionPool{
        connections: make(map[string]*UserConnection),
    }
}

// messageQueueCapacity 是每个连接待发队列的槽数。
//
// 它刻意不做成配置项。delta 走的是 handler.enqueue 的**阻塞**投递，所以这个数字
// 不决定「丢不丢」（一个都不会丢），只决定「客户端落后多少才让生产方停下」。
// 原先 config 里那个 MaxQueueSize=10000 既没人读、又和这里的 100 不一致，
// 看着像个能调队列深度的旋钮，实际上改它什么也不会发生。
const messageQueueCapacity = 100

// AddConnection 添加连接（线程安全）。done 由调用方创建并在连接结束时关闭。
func (cp *ConnectionPool) AddConnection(userID string, wsConn interface{}, done chan struct{}) *UserConnection {
    cp.mu.Lock()
    defer cp.mu.Unlock()

    conn := &UserConnection{
        UserID:        userID,
        WebSocketConn: wsConn,
        MessageQueue:  make(chan interface{}, messageQueueCapacity),
        Done:          done,
    }

    cp.connections[userID] = conn
    return conn
}

// GetConnection 获取连接
func (cp *ConnectionPool) GetConnection(userID string) *UserConnection {
    cp.mu.RLock()
    defer cp.mu.RUnlock()
    return cp.connections[userID]
}

// RemoveConnection 移除连接
func (cp *ConnectionPool) RemoveConnection(userID string) {
    cp.mu.Lock()
    defer cp.mu.Unlock()
    delete(cp.connections, userID)
}

// GetConnectionCount 获取连接数
func (cp *ConnectionPool) GetConnectionCount() int {
    cp.mu.RLock()
    defer cp.mu.RUnlock()
    return len(cp.connections)
}