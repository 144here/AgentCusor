package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// JobEventsChannel 是 Python worker 推送任务结果的频道（见 agent-service/tasks/contract.py）。
// 两边必须一致：改名时一起改，`make check-contract` 会在构建期断言它们相等——
// 否则改名不报错，只是网关静默收不到任何 job 帧。
const JobEventsChannel = "job:events"

// jobResubscribeDelay 是订阅断开后的重试间隔。
const jobResubscribeDelay = 3 * time.Second

// JobEvent 是一次异步任务的状态变更，字段对应 worker 写进 job:result:<id> 的 JSON。
type JobEvent struct {
	JobID    string `json:"job_id"`
	UserID   string `json:"user_id"`
	Type     string `json:"type"`
	Status   string `json:"status"`
	TicketID string `json:"ticket_id"`
}

// JobSubscriber 订阅任务事件频道并把事件交给回调。
//
// 刻意做成「一个共享订阅者」而不是每个用户一条 Redis 连接：事件量远小于聊天量，
// 而且用户是否在线是由连接池回答的，Redis 这层不需要知道用户。
type JobSubscriber struct {
	client *redis.Client
}

func NewJobSubscriber(addr string, db int) *JobSubscriber {
	// redis.NewClient 不会立刻拨号，Redis 没起来也不会让进程启动失败——
	// 由 Run 的重连循环负责等它可用。
	return &JobSubscriber{client: redis.NewClient(&redis.Options{Addr: addr, DB: db})}
}

func (s *JobSubscriber) Close() error {
	return s.client.Close()
}

// Run 持续订阅任务事件直到 ctx 结束。订阅断开（Redis 重启、网络抖动）会自动重连，
// 否则用户的通知会静默消失——那是比报错更难发现的一类故障。
func (s *JobSubscriber) Run(ctx context.Context, onEvent func(JobEvent)) error {
	for {
		if err := s.consume(ctx, onEvent); err != nil && ctx.Err() == nil {
			log.Printf("[jobs] subscriber error: %v; resubscribing in %s", err, jobResubscribeDelay)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-time.After(jobResubscribeDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *JobSubscriber) consume(ctx context.Context, onEvent func(JobEvent)) error {
	sub := s.client.Subscribe(ctx, JobEventsChannel)
	defer func() { _ = sub.Close() }()

	// 必须先确认订阅真的建立了再开始收消息：Subscribe 只是构造请求，
	// 不等它完成就进入读循环，会漏掉这段时间内发布的事件。
	if _, err := sub.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe %s: %w", JobEventsChannel, err)
	}
	log.Printf("[jobs] subscribed to %s on redis %s", JobEventsChannel, s.client.Options().Addr)

	ch := sub.Channel()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return errors.New("job event channel closed")
			}
			var ev JobEvent
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				// 一条坏消息不该拖垮整个订阅。
				log.Printf("[jobs] bad event payload %q: %v", msg.Payload, err)
				continue
			}
			onEvent(ev)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
