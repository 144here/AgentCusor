.PHONY: proto check check-contract tidy test

# proto 在一次性容器里生成 gRPC 代码，宿主无需安装 protoc / protoc-gen-go。
# 容器不继承宿主的 VPN 代理，必须显式指定可直连的 GOPROXY，否则 go install / tidy 会 EOF。
# go mod tidy 也在这里跑，因为它要联网补 go.sum（x/net、x/sys、genproto 等本地缺失）。
proto:
	docker run --rm -v "$(CURDIR)":/work -w /work -e GOPROXY=https://goproxy.cn,direct golang:1.21-alpine sh -c '\
	  apk add --no-cache protobuf && \
	  go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.31.0 && \
	  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0 && \
	  protoc -I agent-service/proto \
	    --go_out=customer-service-go --go_opt=module=github.com/customer-service \
	    --go-grpc_out=customer-service-go --go-grpc_opt=module=github.com/customer-service \
	    agent.proto && \
	  cd customer-service-go && go mod tidy'

# check 在容器里编译 + 静态检查 + 跑 Go 测试，宿主不装 Go 工具链。
# 先跑 check-contract：跨语言的契约常量为空漂移是编译期发现不了的。
# 与 CI（.github/workflows/ci.yml 的 contract-and-go job）跑的是同一批命令。
check: check-contract
	docker run --rm -v "$(CURDIR)/customer-service-go":/app -w /app -e GOPROXY=https://goproxy.cn,direct golang:1.21-alpine sh -c 'go build ./... && go vet ./... && go test ./...'

# check-contract 断言 Go 侧与 Python 侧的队列契约常量一致（理由见脚本注释）。
# 挂整个仓库而不是子目录：这个检查天生要同时看到两边的源码。
check-contract:
	docker run --rm -v "$(CURDIR)":/repo -w /repo alpine sh scripts/check_contract.sh

# tidy 在容器里解析依赖并写回 go.mod / go.sum（要联网，宿主的 module cache 里没有这些 zip）。
# 改完 import 后先跑这个，再跑 check。
tidy:
	docker run --rm -v "$(CURDIR)/customer-service-go":/app -w /app -e GOPROXY=https://goproxy.cn,direct golang:1.21-alpine go mod tidy

# test 在跑起来的 agent-grpc 容器里执行 Python 测试。用 stdlib unittest，不引入 pytest，
# 所以测试不增加任何依赖。前置条件：
#   1. `docker compose up -d`（退重投那条要连 Redis，走的是容器内的 REDIS_HOST=redis）；
#   2. 测试文件在镜像里，改了测试要先 `docker compose build agent-grpc && docker compose up -d`。
# -t tests 把顶层目录设为 tests，这样不必给它加 __init__.py（本仓库用 PEP 420 命名空间包）。
test:
	docker compose exec -T agent-grpc python3 -m unittest discover -s tests -t tests -v
