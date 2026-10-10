package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/customer-service/agentpb"
)

// ErrStreamWithoutFinal 表示流在拿到 final 事件之前就结束了。
// 此时文本可能已经产出，但服务端没有给出确认，按错误处理让上层提示重试。
var ErrStreamWithoutFinal = errors.New("agent stream ended without final event")

// StreamResult 是一轮流式对话的汇总，对应 proto 里的 ChatResponse。
// 刻意不含 tool_calls：proto 里保留该字段作为扩展点，但三个专家目前都没有工具，
// 读进来也没人消费——留一个只赋值不读的字段正是本项目在清的那类死物。
type StreamResult struct {
	Text               string
	Intent             string
	RequiresEscalation bool
}

// AgentClient 是 agent-service 的 gRPC 客户端。
type AgentClient struct {
	conn   *grpc.ClientConn
	client agentpb.AgentServiceClient
}

func NewAgentClient(addr string) (*AgentClient, error) {
	// 故意不加 WithBlock：agent-grpc 可能比本服务晚就绪（compose 的 depends_on
	// 不等健康检查），非阻塞拨号让连接在后台自行重建，首次 RPC 自己等。
	// agent-grpc 用的是 add_insecure_port，所以这里用 insecure。
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &AgentClient{conn: conn, client: agentpb.NewAgentServiceClient(conn)}, nil
}

func (c *AgentClient) Close() error {
	return c.conn.Close()
}

// ProcessChatStream 发起一次流式对话：每收到一个 delta 就同步回调 onDelta。
// onDelta 返回的错误会立即中断流并向上传播（用于把写队列的反压传回来）。
func (c *AgentClient) ProcessChatStream(
	ctx context.Context,
	userID, message string,
	history []*agentpb.Message,
	onDelta func(string) error,
) (*StreamResult, error) {
	// 自己派生 cancel：无论从哪条路径返回都保证流被拆掉，不给服务端留孤儿生成器。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := c.client.ProcessChat(ctx, &agentpb.ChatRequest{
		UserId:  userID,
		Message: message,
		History: history,
	})
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}

	result := &StreamResult{}
	gotFinal := false

	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("recv: %w", err)
		}

		switch e := ev.GetEvent().(type) {
		case *agentpb.ChatEvent_Delta:
			if e.Delta == "" {
				continue
			}
			if err := onDelta(e.Delta); err != nil {
				return nil, err
			}
		case *agentpb.ChatEvent_Final:
			gotFinal = true
			if e.Final != nil {
				result.Text = e.Final.GetResponse()
				result.Intent = e.Final.GetIntent()
				result.RequiresEscalation = e.Final.GetRequiresEscalation()
			}
		default:
			log.Printf("[agent] user %s: unknown chat event %T", userID, ev.GetEvent())
		}
	}

	if !gotFinal {
		return nil, ErrStreamWithoutFinal
	}
	return result, nil
}
