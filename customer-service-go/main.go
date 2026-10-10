package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/customer-service/connection"
	"github.com/customer-service/handler"
	"github.com/customer-service/service"
)

// main 退化为组装层：配置 → agent 客户端 → 连接池 → WebSocket handler → 任务事件订阅者。
// 具体行为（升级、读写循环、流式转发、历史、任务通知）都在各自的包里。
func main() {
	cfg := LoadConfig()

	agentClient, err := service.NewAgentClient(cfg.GRPCAgentAddr)
	if err != nil {
		log.Fatalf("[main] connect agent at %s: %v", cfg.GRPCAgentAddr, err)
	}
	defer agentClient.Close()

	pool := connection.NewConnectionPool()
	wsHandler := handler.NewWSHandler(pool, agentClient)

	// 连接数直接读连接池的实时值，用 GaugeFunc 而不是自己再维护一个计数器：
	// 多一份状态就多一处能和事实不符的地方。
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cs_active_websocket_connections",
		Help: "Number of active websocket connections",
	}, func() float64 { return float64(pool.GetConnectionCount()) }))

	// 异步任务（退款工单）的状态变更由 Python worker 发到 Redis 的 job:events，
	// 这里订阅并转发给对应的用户。单独一个 goroutine：订阅断开会自己重连，
	// 不影响 HTTP 服务。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobSubscriber := service.NewJobSubscriber(cfg.RedisAddr, cfg.RedisDB)
	defer jobSubscriber.Close()
	go func() {
		if err := jobSubscriber.Run(ctx, wsHandler.NotifyJobEvent); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("[main] job subscriber stopped: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler.ServeWS)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"status":      "ok",
			"connections": pool.GetConnectionCount(),
		}); err != nil {
			log.Printf("[health] encode response: %v", err)
		}
	})

	addr := ":" + cfg.HTTPPort
	log.Printf("[main] agent=%s redis=%s listening on %s (ws://<host>%s/ws?user_id=<id>)", cfg.GRPCAgentAddr, cfg.RedisAddr, addr, addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("[main] server stopped: %v", err)
	}
}
