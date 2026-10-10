package handler

import (
	"fmt"
	"testing"
)

// 历史必须裁到最近 maxHistoryMessages 条：否则 prompt 无界增长。
func TestAppendTrimsToMaxHistoryMessages(t *testing.T) {
	store := newHistoryStore()
	const extra = 5
	for i := 0; i < maxHistoryMessages+extra; i++ {
		store.append("u1", "user", fmt.Sprintf("m%d", i))
	}

	got := store.snapshot("u1")
	if len(got) != maxHistoryMessages {
		t.Fatalf("历史长度 = %d, want %d", len(got), maxHistoryMessages)
	}
	// 保留的是最近 N 条：最后一条 = 最后写入，首条 = 倒数第 N 条。
	if last := got[len(got)-1].Content; last != fmt.Sprintf("m%d", maxHistoryMessages+extra-1) {
		t.Fatalf("裁剪未保留最近条目: last = %q", last)
	}
	if first := got[0].Content; first != fmt.Sprintf("m%d", extra) {
		t.Fatalf("裁剪后首条 = %q, want m%d", first, extra)
	}
}

// snapshot 必须返回副本：调用方随后会在别的 goroutine 里读它，而 append 会原地改写底层数组。
func TestSnapshotReturnsACopy(t *testing.T) {
	store := newHistoryStore()
	store.append("u1", "user", "a")
	store.append("u1", "assistant", "b")

	snap := store.snapshot("u1")
	snap[0] = nil // 篡改返回值

	again := store.snapshot("u1")
	if again[0] == nil {
		t.Fatal("snapshot 返回的是内部切片，被调用方修改后污染了存储")
	}
	if again[0].Content != "a" || again[0].Role != "user" {
		t.Fatalf("存储被污染: role=%q content=%q", again[0].Role, again[0].Content)
	}
	if len(again) != 2 || again[1].Content != "b" {
		t.Fatalf("历史内容不对: %v", again)
	}
}

func TestAppendIgnoresEmptyInputs(t *testing.T) {
	store := newHistoryStore()
	store.append("", "user", "x")
	store.append("u1", "user", "")

	if got := len(store.snapshot("u1")); got != 0 {
		t.Fatalf("空 content 不应写入历史, len = %d", got)
	}
	if got := len(store.snapshot("")); got != 0 {
		t.Fatalf("空 userID 不应写入历史, len = %d", got)
	}
}

func TestSnapshotUnknownUserIsEmpty(t *testing.T) {
	if got := newHistoryStore().snapshot("nobody"); len(got) != 0 {
		t.Fatalf("未知用户的历史应为空, len = %d", len(got))
	}
}
