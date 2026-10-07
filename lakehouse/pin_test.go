package lakehouse

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// commitRows 提交一批 upsert 行并返回快照序号。
func commitRows(t *testing.T, tb *Table, start, n int) int {
	t.Helper()
	tx := tb.Begin()
	for i := start; i < start+n; i++ {
		tx.Upsert(Row{1: i, 2: fmt.Sprintf("v%d", i)})
	}
	seq, err := tx.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	return seq
}

// TestPinUnpinResults 固定/解固的结果可判定：不存在 => 找不到，未固定 => 未固定。
func TestPinUnpinResults(t *testing.T) {
	tb := newTable(t, pkSchema())
	seq := commitRows(t, tb, 0, 2) // 2

	// 固定不存在的快照：必须明确是"找不到"，不能当成功
	if err := tb.PinSnapshot(999); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("pin missing snapshot: expected ErrSnapshotNotFound, got %v", err)
	}
	// 解固未固定的快照：必须明确是"未固定"
	if err := tb.UnpinSnapshot(seq); !errors.Is(err, ErrSnapshotNotPinned) {
		t.Fatalf("unpin unpinned snapshot: expected ErrSnapshotNotPinned, got %v", err)
	}
	// 正常固定（幂等）与解固
	if err := tb.PinSnapshot(seq); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if err := tb.PinSnapshot(seq); err != nil {
		t.Fatalf("re-pin must be idempotent: %v", err)
	}
	if got := tb.PinnedSnapshots(); len(got) != 1 || got[0] != seq {
		t.Fatalf("PinnedSnapshots=%v, want [%d]", got, seq)
	}
	if err := tb.UnpinSnapshot(seq); err != nil {
		t.Fatalf("unpin: %v", err)
	}
	if got := tb.PinnedSnapshots(); len(got) != 0 {
		t.Fatalf("after unpin PinnedSnapshots=%v, want empty", got)
	}
	t.Logf("判定依据: pin(999)=>ErrSnapshotNotFound, unpin(未固定)=>ErrSnapshotNotPinned, pin 幂等, unpin 后清单为空")
}

// TestCleanupRejectsPinnedSnapshot 被固定的快照即使掉到保留窗口之外，
// 清理也必须整体拒绝（ErrCleanupPinned），且一个文件都不删。
func TestCleanupRejectsPinnedSnapshot(t *testing.T) {
	tb := newTable(t, pkSchema())
	for i := 0; i < 4; i++ {
		commitRows(t, tb, i, 1) // 快照 2..5
	}
	pinned := 2
	if err := tb.PinSnapshot(pinned); err != nil {
		t.Fatal(err)
	}
	// 记录清理前的文件全集
	before := listFiles(t, tb)

	// 快照 2 已掉出"最近 2 个"的保留窗口，但被固定 => 整体拒绝
	err := tb.Cleanup(3, 2)
	if !errors.Is(err, ErrCleanupPinned) {
		t.Fatalf("expected ErrCleanupPinned, got %v", err)
	}
	if !errors.Is(err, ErrCleanupRejected) {
		t.Fatalf("ErrCleanupPinned must also classify as ErrCleanupRejected, got %v", err)
	}
	after := listFiles(t, tb)
	if len(before) != len(after) {
		t.Fatalf("rejected cleanup must not delete any file: before=%d after=%d", len(before), len(after))
	}
	// 表完整可读：被固定快照与最新快照都正常
	if rows, err := tb.Scan(pinned); err != nil || len(rows) != 1 {
		t.Fatalf("pinned snapshot seq=%d must stay readable: rows=%v err=%v", pinned, rows, err)
	}
	if rows, err := tb.Scan(0); err != nil || len(rows) != 4 {
		t.Fatalf("latest snapshot must stay readable: rows=%v err=%v", rows, err)
	}

	// 解固后同一清理请求成功，被固定过的快照可被清掉
	if err := tb.UnpinSnapshot(pinned); err != nil {
		t.Fatal(err)
	}
	if err := tb.Cleanup(3, 2); err != nil {
		t.Fatalf("cleanup after unpin: %v", err)
	}
	if _, err := tb.Scan(pinned); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("snapshot %d should be cleaned after unpin, got %v", pinned, err)
	}
	t.Logf("判定依据: 固定 seq=%d 时清理=>ErrCleanupPinned 且文件数 %d 不变, 解固后清理成功且 seq=%d 不可读",
		pinned, len(before), pinned)
}

// TestCleanupRejectionKinds 清理拒绝三分：越权 / 无对象 / 固定保护，互不混淆。
func TestCleanupRejectionKinds(t *testing.T) {
	tb := newTable(t, pkSchema())
	commitRows(t, tb, 0, 1) // 快照 2
	cur := tb.CurrentSeq()

	// 越权：会删掉保留线内的快照
	err := tb.Cleanup(cur, 1)
	if !errors.Is(err, ErrCleanupRejected) || errors.Is(err, ErrCleanupNothing) || errors.Is(err, ErrCleanupPinned) {
		t.Fatalf("out-of-range cleanup must be plain ErrCleanupRejected, got %v", err)
	}
	// 无对象：beforeSeq 之前没有任何快照
	err = tb.Cleanup(0, 1)
	if !errors.Is(err, ErrCleanupNothing) || !errors.Is(err, ErrCleanupRejected) {
		t.Fatalf("stale cleanup must classify as ErrCleanupNothing (+ErrCleanupRejected), got %v", err)
	}
	if errors.Is(err, ErrCleanupPinned) {
		t.Fatalf("nothing-to-do must not classify as pinned, got %v", err)
	}
	// 固定保护
	if err := tb.PinSnapshot(1); err != nil {
		t.Fatal(err)
	}
	commitRows(t, tb, 1, 1) // 快照 3，使 1 掉出保留窗口
	err = tb.Cleanup(1, 1)
	if !errors.Is(err, ErrCleanupPinned) || !errors.Is(err, ErrCleanupRejected) {
		t.Fatalf("pinned cleanup must classify as ErrCleanupPinned (+ErrCleanupRejected), got %v", err)
	}
	if tb.CurrentSeq() != cur+1 {
		t.Fatalf("rejected cleanups must not change table")
	}
	t.Logf("判定依据: 越权=>仅 ErrCleanupRejected, 无对象=>+ErrCleanupNothing, 固定=>+ErrCleanupPinned, 三者可区分")
}

// TestMixedConcurrentOps 合并/清理/回滚/写入混合并发：
// 最终状态可预期——序号连续、每个存活快照可读、数据不丢不重。
func TestMixedConcurrentOps(t *testing.T) {
	tb := newTable(t, pkSchema())
	commitRows(t, tb, 0, 3) // 快照 2
	base := tb.CurrentSeq()
	// 固定基线快照，清理线程因此永远被干净拒绝，不会误删
	if err := tb.PinSnapshot(base); err != nil {
		t.Fatal(err)
	}

	const writers = 4
	var wg sync.WaitGroup
	// 写入：各自重试直到成功一次
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				tx := tb.Begin()
				tx.Upsert(Row{1: 100 + i, 2: "w"})
				_, err := tx.Commit()
				if err == nil {
					return
				}
				if errors.Is(err, ErrConflictConcurrentCommit) {
					continue
				}
				t.Errorf("writer: %v", err)
				return
			}
		}(i)
	}
	// 合并：成功一次即可
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, err := tb.Compact()
			if err == nil {
				return
			}
			if errors.Is(err, ErrConflictConcurrentCommit) {
				continue
			}
			t.Errorf("compact: %v", err)
			return
		}
	}()
	// 回滚：成功一次即可
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, err := tb.Rollback(base)
			if err == nil {
				return
			}
			if errors.Is(err, ErrConflictConcurrentCommit) {
				continue
			}
			t.Errorf("rollback: %v", err)
			return
		}
	}()
	// 清理：反复尝试；只能"成功"或"被固定保护拒绝"，不得出现其他错误
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			err := tb.Cleanup(base, 2)
			if err == nil {
				continue
			}
			if errors.Is(err, ErrCleanupPinned) || errors.Is(err, ErrCleanupNothing) ||
				errors.Is(err, ErrCleanupRejected) {
				continue
			}
			t.Errorf("cleanup: %v", err)
			return
		}
	}()
	wg.Wait()

	// 恰有 writers+2 次新提交（4 写 + 1 合并 + 1 回滚），序号连续
	want := base + writers + 2
	if got := tb.CurrentSeq(); got != want {
		t.Fatalf("expected final seq=%d, got %d", want, got)
	}
	hist, err := tb.History(0)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range hist {
		if e.Seq != i+1 {
			t.Fatalf("history must be gapless: entry %d seq=%d", i, e.Seq)
		}
		if _, err := tb.Scan(e.Seq); err != nil {
			t.Fatalf("snapshot seq=%d unreadable after mixed concurrency: %v", e.Seq, err)
		}
	}
	// 被固定的基线快照必须仍在且可读
	if _, err := tb.Scan(base); err != nil {
		t.Fatalf("pinned base snapshot seq=%d must survive: %v", base, err)
	}
	t.Logf("判定依据: 4 写+1 合并+1 回滚并发后序号连续到 %d, 全部 %d 个快照可读, 固定快照 seq=%d 完好",
		want, len(hist), base)
}

// listFiles 递归统计表目录下的文件（用于验证"被拒绝的清理一个文件都不删"）。
func listFiles(t *testing.T, tb *Table) []string {
	t.Helper()
	var files []string
	root := tb.dir
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}
