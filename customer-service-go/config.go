package main

import (
	"log"
	"os"
	"strconv"
)

type Config struct {
	HTTPPort      string
	GRPCAgentAddr string
	RedisAddr     string
	RedisDB       int
}

// 说明：这里曾有两个常量 MaxConnPool=1000 与 MaxQueueSize=10000，两者都没有任何读取方
// ——连接数上限从未被强制，队列深度也一直是 connection 包里的硬编码值。
// 已删除而不是接上去：假的可配置项比没有配置项更坏，它会让读代码的人以为
// 改了它就会生效（RedisDB 那次漂移就是同一族故障）。真要限流再连策略一起设计。

func LoadConfig() *Config {
	return &Config{
		HTTPPort:      getEnv("HTTP_PORT", "8000"),
		GRPCAgentAddr: getEnv("GRPC_AGENT_ADDR", "localhost:50051"),
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		// 必须与 Python 侧 config.REDIS_DB 指向同一个库：网关订阅的 job:events 就是从那儿发出来的。
		// 两边不同库**不会报错**，只是静默收不到任何任务事件——这类故障比报错难查得多，
		// 所以这里跟 Python 用同一个环境变量名，由 compose 同时注入，不能各写各的常量。
		RedisDB: getEnvInt("REDIS_DB", 0),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt 读整数环境变量。解析不了就退回默认值并留一行日志：库号写错时静默退回 0
// 会表现为「事件全收不到」，跟 Redis 里没数据一模一样，必须留下痕迹。
func getEnvInt(key string, defaultValue int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("[config] %s=%q is not an integer, falling back to %d", key, raw, defaultValue)
		return defaultValue
	}
	return value
}
