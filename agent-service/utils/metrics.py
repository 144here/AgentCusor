# utils/metrics.py
#
# 注意：WebSocket 连接数（cs_active_websocket_connections）**不在这里**——那是 Go 网关
# 才有的状态，定义在 customer-service-go/main.go。本进程里它只会恒为 0，是误导。
from prometheus_client import Counter, Histogram, start_http_server

REQUESTS = Counter("cs_agent_requests_total", "Total number of incoming requests", ["path", "status"])

# 聊天本身的指标按意图打标签：能直接回答「退款类问题是不是比订单类慢」这类问题，
# 也是验证意图路由是否真的生效的观测手段（prometheus.yml 抓取本进程的 8001 端口）。
CHAT_REQUESTS = Counter(
    "cs_agent_chat_requests_total", "Chat turns by intent and outcome", ["intent", "outcome"]
)
CHAT_LATENCY = Histogram(
    "cs_agent_chat_latency_seconds", "Chat turn latency (first delta to final)", ["intent"]
)


def start_metrics_server(port: int = 8001):
    """在独立端口上启动 Prometheus exporter(通常由 docker-compose 启动）"""
    start_http_server(port)

# 使用示例：
# from utils.metrics import REQUESTS
# REQUESTS.labels(path="/chat", status="200").inc()