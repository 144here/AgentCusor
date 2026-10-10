#!/bin/sh
# 断言 Go 侧与 Python 侧的队列契约常量一致，不一致就让构建失败。
#
# 为什么需要它：job:events 这个频道名在两边各写了一份——Python 是
# agent-service/tasks/contract.py 的 JOB_EVENTS_CHANNEL，Go 是
# customer-service-go/service/job_subscriber.go 的 JobEventsChannel。
# 改一边、另一边照样编译通过，然后网关静默收不到任何 job 帧，症状和
# 「Redis 里没数据」一模一样。这与 config.go 曾把库号写死那次是同一族故障
# （不报错、只静默失效），所以在构建期把它变成显式失败。
#
# 只用 POSIX 工具，能在 alpine 里跑（宿主零安装）。
set -eu

PY=agent-service/tasks/contract.py
GO=customer-service-go/service/job_subscriber.go

for f in "$PY" "$GO"; do
	[ -f "$f" ] || { echo "contract check FAILED: $f 不存在" >&2; exit 1; }
done

# 抽出所有匹配行（不是只抽第一行）。sed 不认识注释和字符串，所以一行躺在
# 块注释或 docstring 里、文本又恰好一模一样时也会被匹配到——那样读到的就是假值。
# 对策是要求「恰好一处」：多出来的那处会让检查失败，而不是被 head -n 1 悄悄吞掉。
#
# 正则可选地接受 `const` 前缀、任意缩进和任意空白：Go 里合法的
# `const ( ... )` 块形式与 tab 缩进都要能抽到。早期版本写死 `^const X *= *"`
# 有两个反效果：块形式抽不到 → 合法重构被误报为失败；而若同时存在一个
# 顶格的诱饵行，真声明因缩进抽不到、诱饵却抽到了 → 变成**假通过**。
py_lines=$(sed -nE 's/^[[:space:]]*(const[[:space:]]+)?JOB_EVENTS_CHANNEL[[:space:]]*=[[:space:]]*"([^"]*)".*/\2/p' "$PY")
go_lines=$(sed -nE 's/^[[:space:]]*(const[[:space:]]+)?JobEventsChannel[[:space:]]*=[[:space:]]*"([^"]*)".*/\2/p' "$GO")

# grep -c 在计数为 0 时返回非零，set -e 会直接中断，故用 || true 兜住。
count() { printf '%s\n' "$1" | grep -c '[^[:space:]]' || true; }

py_n=$(count "$py_lines")
go_n=$(count "$go_lines")

if [ "$py_n" -ne 1 ] || [ "$go_n" -ne 1 ]; then
	echo "contract check FAILED: 常量声明不是恰好一处（python=$py_n, go=$go_n）" >&2
	echo "  0 处 = 变量或文件被改名，检查已失效，必须一起改" >&2
	echo "  >1 处 = 有注释/docstring 里的同形文本干扰，脚本会读到假值" >&2
	echo "  $PY:" >&2
	printf '    %s\n' $py_lines >&2 2>/dev/null || true
	echo "  $GO:" >&2
	printf '    %s\n' $go_lines >&2 2>/dev/null || true
	exit 1
fi

if [ "$py_lines" != "$go_lines" ]; then
	echo "contract check FAILED: 频道名两边不一致" >&2
	echo "  python: $py_lines" >&2
	echo "  go    : $go_lines" >&2
	exit 1
fi

echo "contract check OK: 频道名两边一致 -> $py_lines"
