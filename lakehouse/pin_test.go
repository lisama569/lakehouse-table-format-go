package lakehouse

import (
	"errors"
	"os"
	"testing"
)

// TestPinResults 固定/解固的各类结果要能明确判定。
func TestPinResults(t *testing.T) {
	tb := newTable(t, pkSchema())
	s2 := commitN(t, tb, 1, "a")

	if err := tb.Pin(s2); err != nil {
		t.Fatalf("pin should succeed: %v", err)
	}
	pins := tb.Pins()
	if len(pins) != 1 || pins[0] != s2 {
		t.Fatalf("pins should be [%d], got %v", s2, pins)
	}

	// 重复固定 => ErrAlreadyPinned
	if err := tb.Pin(s2); !errors.Is(err, ErrAlreadyPinned) {
		t.Fatalf("re-pin should be ErrAlreadyPinned, got %v", err)
	}
	// 固定不存在的快照 => ErrSnapshotNotFound（而不是当成功）
	if err := tb.Pin(404); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("pin missing snapshot should be ErrSnapshotNotFound, got %v", err)
	}
	t.Logf("判定依据: pin 序号=%d 成功, 重复 pin=%v, pin 404=%v", s2, ErrAlreadyPinned, ErrSnapshotNotFound)

	// 解除固定成功
	if err := tb.Unpin(s2); err != nil {
		t.Fatalf("unpin should succeed: %v", err)
	}
	// 解除一个未固定的快照 => ErrNotPinned
	if err := tb.Unpin(s2); !errors.Is(err, ErrNotPinned) {
		t.Fatalf("unpin unpinned snapshot should be ErrNotPinned, got %v", err)
	}
	// 解除不存在的快照 => ErrSnapshotNotFound
	if err := tb.Unpin(404); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("unpin missing snapshot should be ErrSnapshotNotFound, got %v", err)
	}
	if pins := tb.Pins(); len(pins) != 0 {
		t.Fatalf("all pins should be gone, got %v", pins)
	}
	t.Logf("判定依据: unpin 序号=%d 成功, 再 unpin=%v, unpin 404=%v, 固定列表为空",
		s2, ErrNotPinned, ErrSnapshotNotFound)
}

// TestPinnedSnapshotSurvivesCleanup 固定快照掉到保留窗口之外，清理也必须整批拒绝。
func TestPinnedSnapshotSurvivesCleanup(t *testing.T) {
	tb := newTable(t, pkSchema())
	for i := 0; i < 5; i++ {
		commitN(t, tb, int64(i), "x")
	}
	// 快照 1..6，固定老快照 2
	if err := tb.Pin(2); err != nil {
		t.Fatal(err)
	}

	// 尝试清理 <=4（含被固定的 2）=> ErrCleanupPinned，且零删除
	err := tb.Cleanup(4, 2)
	if !errors.Is(err, ErrCleanupPinned) || !errors.Is(err, ErrCleanupRejected) {
		t.Fatalf("expected ErrCleanupPinned+ErrCleanupRejected, got %v", err)
	}
	for _, s := range []int{1, 2, 3, 4, 5, 6} {
		if _, err := tb.LoadSnapshot(s); err != nil {
			t.Fatalf("rejected pinned cleanup must delete nothing; snapshot %d gone: %v", s, err)
		}
	}
	if _, err := tb.Scan(2); err != nil {
		t.Fatalf("pinned snapshot 2 must remain readable: %v", err)
	}
	t.Logf("判定依据: 清理 <=4 命中固定快照=2 => %v, 快照 1..6 与数据全部保留", err)

	// 解除固定后，同一请求可以正常清理
	if err := tb.Unpin(2); err != nil {
		t.Fatal(err)
	}
	if err := tb.Cleanup(4, 2); err != nil {
		t.Fatalf("cleanup after unpin should succeed: %v", err)
	}
	if _, err := tb.LoadSnapshot(2); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("snapshot 2 should be cleaned after unpin, got %v", err)
	}
	if _, err := tb.Scan(0); err != nil {
		t.Fatalf("table must stay readable after cleanup: %v", err)
	}
	t.Logf("判定依据: 解固后清理 <=4 成功, 快照=2 已删, 最新快照完整可读")
}

// TestPinnedDataFilesSurviveCompaction 固定老快照后做合并/写入，老快照文件不被回收且可读。
func TestPinnedDataFilesSurviveCompaction(t *testing.T) {
	tb := newTable(t, pkSchema())
	s2 := commitN(t, tb, 1, "old-a")
	commitN(t, tb, 2, "old-b")
	if err := tb.Pin(s2); err != nil {
		t.Fatal(err)
	}
	if _, err := tb.Compact(); err != nil {
		t.Fatal(err)
	}
	commitN(t, tb, 3, "new-c")

	// 触发一次清理：被固定快照 s2 必须仍然挡住清理
	err := tb.Cleanup(tb.CurrentSeq()-1, 1)
	if err != nil && !errors.Is(err, ErrCleanupPinned) && !errors.Is(err, ErrCleanupNothing) {
		t.Fatalf("unexpected cleanup result: %v", err)
	}
	// 无论清理成不成，固定快照本身与它的数据必须可读
	old, err := tb.Scan(s2)
	if err != nil {
		t.Fatalf("pinned snapshot %d must remain readable after compaction: %v", s2, err)
	}
	if len(old) != 1 || old[0][2] != "old-a" {
		t.Fatalf("pinned snapshot content wrong: %+v", old)
	}
	cur, err := tb.Scan(0)
	if err != nil || len(cur) != 3 {
		t.Fatalf("current state should have 3 rows, got %d (%v)", len(cur), err)
	}
	t.Logf("判定依据: 固定快照=%d 经合并与新写入后仍读出 old-a, 最新快照 %d 行可读", s2, len(cur))
}

// TestCleanupNothingClassification 没有可清理对象时单独归类。
func TestCleanupNothingClassification(t *testing.T) {
	tb := newTable(t, pkSchema())
	commitN(t, tb, 1, "a")
	err := tb.Cleanup(0, 1)
	if !errors.Is(err, ErrCleanupNothing) || !errors.Is(err, ErrCleanupRejected) {
		t.Fatalf("expected ErrCleanupNothing+ErrCleanupRejected, got %v", err)
	}
	if _, statErr := os.Stat(snapshotPath(tb.dir, 1)); statErr != nil {
		t.Fatalf("rejected cleanup must delete nothing: %v", statErr)
	}
	t.Logf("判定依据: beforeSeq=0 无对象 => %v, 快照文件仍在", err)
}
