# 构建阶段
FROM golang:1.21-alpine AS builder

# 构建容器不走宿主机的 VPN 代理，直连 proxy.golang.org 会中断，改用可直连的镜像源
ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o customer-service .

# 运行阶段
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/customer-service .

EXPOSE 8000

CMD ["./customer-service"]