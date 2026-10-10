package service

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/customer-service/agentpb"
)

// scriptedAgent 复刻 agent-gRPC 的流式契约：先若干 delta，最后一条 final（若给了的话）。
type scriptedAgent struct {
	agentpb.UnimplementedAgentServiceServer
	deltas []string
	final  *agentpb.ChatResponse // 为 nil 时只发 delta 就结束流
}

func (s *scriptedAgent) ProcessChat(_ *agentpb.ChatRequest, stream agentpb.AgentService_ProcessChatServer) error {
	for _, d := range s.deltas {
		if err := stream.Send(&agentpb.ChatEvent{Event: &agentpb.ChatEvent_Delta{Delta: d}}); err != nil {
			return err
		}
	}
	if s.final != nil {
		return stream.Send(&agentpb.ChatEvent{Event: &agentpb.ChatEvent_Final{Final: s.final}})
	}
	return nil
}

func newTestAgentClient(t *testing.T, srv agentpb.AgentServiceServer) *AgentClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	gs := grpc.NewServer()
	agentpb.RegisterAgentServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &AgentClient{conn: conn, client: agentpb.NewAgentServiceClient(conn)}
}

// Go 侧对 proto 流契约的消费：每个 delta 依序回调，final 解析进 StreamResult。
// delta 拼接 == final.response 是跨语言的不变式（Python 侧有对称的 test_stream_invariant）。
func TestProcessChatStreamConcatenatesDeltasAndParsesFinal(t *testing.T) {
	c := newTestAgentClient(t, &scriptedAgent{
		deltas: []string{"你好", "，世界"},
		final:  &agentpb.ChatResponse{Response: "你好，世界", Intent: "order_query", RequiresEscalation: true},
	})

	var got []string
	res, err := c.ProcessChatStream(context.Background(), "u1", "hi", nil, func(d string) error {
		got = append(got, d)
		return nil
	})
	if err != nil {
		t.Fatalf("ProcessChatStream 报错: %v", err)
	}
	if strings.Join(got, "") != "你好，世界" {
		t.Fatalf("回调收到的拼接 = %q", strings.Join(got, ""))
	}
	if res.Text != "你好，世界" || res.Intent != "order_query" || !res.RequiresEscalation {
		t.Fatalf("StreamResult 解析不对: %+v", res)
	}
}

// 流在拿到 final 之前就结束 → 必须报错，让上层提示重试，而不是把半截文本当成功。
func TestProcessChatStreamEndingWithoutFinalIsAnError(t *testing.T) {
	c := newTestAgentClient(t, &scriptedAgent{deltas: []string{"半截"}})

	_, err := c.ProcessChatStream(context.Background(), "u1", "hi", nil, func(string) error { return nil })
	if !errors.Is(err, ErrStreamWithoutFinal) {
		t.Fatalf("err = %v, want ErrStreamWithoutFinal", err)
	}
}

// onDelta 返回错误必须立刻中断流并上抛——这是把写队列的反压传回 LLM 的通道。
// 若吞掉它，客户端已经断开了服务端还会把整轮跑完。
func TestOnDeltaErrorAbortsTheStream(t *testing.T) {
	c := newTestAgentClient(t, &scriptedAgent{
		deltas: []string{"a", "b", "c"},
		final:  &agentpb.ChatResponse{Response: "abc"},
	})

	sentinel := errors.New("client gone")
	calls := 0
	_, err := c.ProcessChatStream(context.Background(), "u1", "hi", nil, func(string) error {
		calls++
		return sentinel
	})

	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if calls != 1 {
		t.Fatalf("onDelta 被调用 %d 次, want 1（报错后应立即中断）", calls)
	}
}
