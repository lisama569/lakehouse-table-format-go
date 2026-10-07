package lakehouse

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// commitN 提交一次 upsert，返回新序号。
func commitN(t *testing.T, tb *Table, id int64, val string) int {
	t.Helper()
	tx := tb.Begin()
	tx.Upsert(Row{1: id, 2: val})
	seq, err := tx.Commit()
	if err != nil {
		t.Fatalf("commit id=%d: %v", id, err)
	}
	return seq
}

// TestHistoryListsMetadata 历史只由快照元数据构成：序号、父版本、操作类型、行数。
func TestHistoryListsMetadata(t *testing.T) {
	tb := newTable(t, pkSchema())
	s2 := commitN(t, tb, 1, "a")
	s3 := commitN(t, tb, 2, "b")
	cseq, err := tb.Compact()
	if err != nil {
		t.Fatal(err)
	}

	hist, err := tb.History(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 4 { // 初始空快照(1) + 2 次写入 + 1 次合并
		t.Fatalf("expected 4 history entries, got %d: %+v", len(hist), hist)
	}
	want := []struct {
		seq, parent int
		kind        string
		rows        int
	}{
		{1, 0, KindSchema, 0},
		{s2, 1, KindUpsert, 1},
		{s3, s2, KindUpsert, 2},
		{cseq, s3, KindCompact, 2},
	}
	for i, w := range want {
		got := hist[i]
		if got.Seq != w.seq || got.Parent != w.parent || got.Kind != w.kind || got.RowCount != w.rows {
			t.Fatalf("history[%d]=%+v, want seq=%d parent=%d kind=%s rows=%d",
				i, got, w.seq, w.parent, w.kind, w.rows)
		}
	}
	t.Logf("判定依据: 历史 4 条按序号升序, 每条 parent/kind/row_count 与提交一致: %+v", hist)
}

// TestHistoryPagination limit/afterSeq 分页，累积读取等价于全量。
func TestHistoryPagination(t *testing.T) {
	tb := newTable(t, pkSchema())
	const n = 6
	for i := 0; i < n; i++ {
		commitN(t, tb, int64(i), fmt.Sprintf("v%d", i))
	}
	var all []CommitInfo
	after := 0
	for {
		page, err := tb.History(after, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		after = page[len(page)-1].Seq
	}
	if len(all) != n+1 { // 含初始快照
		t.Fatalf("pagination should cover all %d entries, got %d", n+1, len(all))
	}
	t.Logf("判定依据: 每页 3 条翻页累积得到 %d 条, 序号连续无遗漏", len(all))
}

// TestRollbackAppendsAndRestores 回滚是追加提交：历史不动，新快照内容回到目标版本。
func TestRollbackAppendsAndRestores(t *testing.T) {
	tb := newTable(t, pkSchema())
	s2 := commitN(t, tb, 1, "v1")
	s3 := commitN(t, tb, 2, "v2")
	s4 := commitN(t, tb, 3, "v3") // 最新有 3 行

	rseq, err := tb.Rollback(s2) // 回到只有 1 行的状态
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if rseq != s4+1 {
		t.Fatalf("rollback must append seq=%d, got %d", s4+1, rseq)
	}

	// 回滚快照的元数据形态
	rsnap, err := tb.LoadSnapshot(rseq)
	if err != nil {
		t.Fatal(err)
	}
	if rsnap.Kind != KindRollback || rsnap.Parent != s4 || rsnap.RollbackTo != s2 {
		t.Fatalf("rollback snapshot metadata wrong: %+v", rsnap)
	}

	// 内容与目标历史快照一致
	rows, err := tb.Scan(rseq)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rolled-back state should have 1 row, got %d (%v)", len(rows), err)
	}
	target, _ := tb.Scan(s2)
	if len(target) != 1 || target[0][2] != "v1" {
		t.Fatalf("target snapshot read changed: %+v", target)
	}

	// 历史快照 s3/s4 依然存在且可读，回滚没有改写或抹掉它们
	for _, seq := range []int{s2, s3, s4} {
		if _, err := tb.LoadSnapshot(seq); err != nil {
			t.Fatalf("history snapshot %d must survive rollback: %v", seq, err)
		}
	}
	hist, _ := tb.History(0, 0)
	if len(hist) != 5 {
		t.Fatalf("history should have 5 entries after rollback, got %d", len(hist))
	}
	last := hist[len(hist)-1]
	if last.Kind != KindRollback || last.RollbackTo != s2 || last.RowCount != 1 {
		t.Fatalf("history rollback entry wrong: %+v", last)
	}
	t.Logf("判定依据: 回滚发布序号=%d (parent=%d, rollback_to=%d), 内容回到 %d 行, s2/s3/s4 均可读",
		rseq, s4, s2, len(rows))
}

// TestRollbackContinuesWrites 回滚后可以继续在历史状态上写入。
func TestRollbackContinuesWrites(t *testing.T) {
	tb := newTable(t, pkSchema())
	s2 := commitN(t, tb, 1, "v1")
	commitN(t, tb, 2, "v2")
	rseq, err := tb.Rollback(s2)
	if err != nil {
		t.Fatal(err)
	}
	nseq := commitN(t, tb, 9, "new") // 在回滚后的状态上追加
	rows, err := tb.Scan(nseq)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after write on rolled-back state, got %d", len(rows))
	}
	// 被回滚"撤销"的主键 2 不应复活
	for _, r := range rows {
		if n, _ := toInt64(r[1]); n == 2 {
			t.Fatalf("rolled-away key 2 resurrected: %+v", r)
		}
	}
	t.Logf("判定依据: 回滚到序号=%d 后继续写入得到序号=%d, 共 %d 行且主键 2 未复活", rseq, nseq, len(rows))
}

// TestRollbackNotFound 回滚不存在的快照必须归类为找不到。
func TestRollbackNotFound(t *testing.T) {
	tb := newTable(t, pkSchema())
	if _, err := tb.Rollback(404); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}
	t.Logf("判定依据: 回滚目标=404 不存在 => %v", ErrSnapshotNotFound)
}

// TestRollbackConcurrentWrite 回滚与并发写竞争：只允许一个生效，绝不互相覆盖。
func TestRollbackConcurrentWrite(t *testing.T) {
	tb := newTable(t, pkSchema())
	s2 := commitN(t, tb, 1, "a")
	commitN(t, tb, 2, "b")
	base := tb.CurrentSeq() // 3

	// 两个操作都基于 base 开启：一个写入事务、一个回滚事务，再并发提交
	tx := tb.Begin()
	tx.Upsert(Row{1: 7, 2: "concurrent"})
	rt, err := tb.BeginRollback(s2)
	if err != nil {
		t.Fatal(err)
	}

	var werr, rerr error
	var wseq, rseq int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); wseq, werr = tx.Commit() }()
	go func() { defer wg.Done(); rseq, rerr = rt.Commit() }()
	wg.Wait()

	win := 0
	if werr == nil {
		win++
	} else if !errors.Is(werr, ErrConflictConcurrentCommit) {
		t.Fatalf("unexpected write error: %v", werr)
	}
	if rerr == nil {
		win++
	} else if !errors.Is(rerr, ErrConflictConcurrentCommit) {
		t.Fatalf("unexpected rollback error: %v", rerr)
	}
	if win != 1 {
		t.Fatalf("exactly one of rollback/write must win, got %d (w=%v r=%v)", win, werr, rerr)
	}
	if tb.CurrentSeq() != base+1 {
		t.Fatalf("seq must advance exactly once: %d", tb.CurrentSeq())
	}
	rows, _ := tb.Scan(0)
	if rerr == nil {
		// 回滚赢：最新状态只有 1 行，并发写入不可见
		if len(rows) != 1 || rows[0][2] != "a" {
			t.Fatalf("rollback winner state wrong: %+v", rows)
		}
	} else {
		// 写入赢：并发行存在，回滚未生效
		if len(rows) != 3 {
			t.Fatalf("write winner state should have 3 rows, got %d", len(rows))
		}
	}
	t.Logf("判定依据: 回滚(seq=%d,%v) 与写入(seq=%d,%v) 竞争序号=%d, 唯一胜方=%d, 最新行数=%d",
		rseq, rerr, wseq, werr, base+1, win, len(rows))
}

// TestRollbackConflictRetry 回滚因并发写入冲突失败后，可基于新表头重试并成功。
func TestRollbackConflictRetry(t *testing.T) {
	tb := newTable(t, pkSchema())
	s2 := commitN(t, tb, 1, "a")
	rt, err := tb.BeginRollback(s2)
	if err != nil {
		t.Fatal(err)
	}
	// 另一笔写入抢先提交，rt 的基线过期
	commitN(t, tb, 5, "later")
	if _, err := rt.Commit(); !errors.Is(err, ErrConflictConcurrentCommit) {
		t.Fatalf("stale rollback should conflict, got %v", err)
	}
	// 重新开启回滚应成功，且历史快照不被改写
	rseq, err := tb.Rollback(s2)
	if err != nil {
		t.Fatalf("retried rollback should succeed: %v", err)
	}
	rows, _ := tb.Scan(rseq)
	if len(rows) != 1 {
		t.Fatalf("rolled back state should have 1 row, got %d", len(rows))
	}
	t.Logf("判定依据: 过期回滚 => %v; 重新基于表头回滚到序号=%d 成功, 行数=%d",
		ErrConflictConcurrentCommit, s2, len(rows))
}
