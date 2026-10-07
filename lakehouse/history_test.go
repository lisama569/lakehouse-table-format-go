package lakehouse

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// TestHistoryListsCommits 历史清单按序号列出每次提交的类别、父版本与行数。
func TestHistoryListsCommits(t *testing.T) {
	tb := newTable(t, pkSchema())
	for i := 0; i < 3; i++ {
		tx := tb.Begin()
		tx.Upsert(Row{1: i, 2: fmt.Sprintf("v%d", i)})
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	cseq, err := tb.Compact()
	if err != nil {
		t.Fatal(err)
	}

	hist, err := tb.History(0)
	if err != nil {
		t.Fatal(err)
	}
	// 1(schema 初始) + 3(upsert) + 1(compact) = 5 条
	if len(hist) != 5 {
		t.Fatalf("expected 5 history entries, got %d", len(hist))
	}
	for i, e := range hist {
		if e.Seq != i+1 {
			t.Fatalf("entries must be ordered by seq: entry %d has seq=%d", i, e.Seq)
		}
		if e.Parent != e.Seq-1 {
			t.Fatalf("entry seq=%d parent=%d, want %d", e.Seq, e.Parent, e.Seq-1)
		}
		t.Logf("历史: seq=%d kind=%s parent=%d rows=%d", e.Seq, e.Kind, e.Parent, e.Rows)
	}
	if hist[1].Kind != "upsert" || hist[1].Rows != 1 {
		t.Fatalf("upsert entry should carry kind=upsert rows=1, got %+v", hist[1])
	}
	last := hist[len(hist)-1]
	if last.Seq != cseq || last.Kind != "compact" || last.Rows != 3 {
		t.Fatalf("compact entry mismatch: %+v (compact seq=%d)", last, cseq)
	}

	// limit 只取最近 N 条，且仍按序号升序
	limited, err := tb.History(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 || limited[0].Seq != cseq-1 || limited[1].Seq != cseq {
		t.Fatalf("History(2) should return last 2 entries, got %+v", limited)
	}
	t.Logf("判定依据: 5 次提交全部按序号列出, kind/parent/rows 与提交行为一致, limit=2 只回最近 2 条")
}

// TestRollbackRestoresContent 回滚是前追加提交：内容回到历史状态，历史快照不被改写。
func TestRollbackRestoresContent(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx := tb.Begin()
	tx.Upsert(Row{1: 1, 2: "a"})
	s1, _ := tx.Commit() // 2
	tx = tb.Begin()
	tx.Upsert(Row{1: 2, 2: "b"})
	s2, _ := tx.Commit() // 3

	before, err := tb.Scan(s2)
	if err != nil || len(before) != 2 {
		t.Fatalf("pre-rollback scan seq=%d: rows=%v err=%v", s2, before, err)
	}

	rseq, err := tb.Rollback(s1)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if rseq != s2+1 {
		t.Fatalf("rollback must append a new commit seq=%d, got %d", s2+1, rseq)
	}

	// 新快照内容 == 目标历史快照内容
	rows, err := tb.Scan(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][2] != "a" {
		t.Fatalf("after rollback expect only row (1,a), got %+v", rows)
	}

	// 历史快照及其读取结果不被改写
	again, err := tb.Scan(s2)
	if err != nil || len(again) != 2 {
		t.Fatalf("historical snapshot seq=%d must stay intact: rows=%v err=%v", s2, again, err)
	}

	// 回滚本身在历史中可见：kind=rollback，parent 是回滚前的最新快照
	snap, err := tb.LoadSnapshot(rseq)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Kind != "rollback" || snap.Parent != s2 || snap.RollbackTo != s1 {
		t.Fatalf("rollback snapshot mismatch: %+v", snap)
	}
	t.Logf("判定依据: 回滚产生新提交 seq=%d(kind=rollback,parent=%d,target=%d), 最新读回到 1 行, 历史快照 seq=%d 仍读出 2 行",
		rseq, snap.Parent, s1, s2)
}

// TestRollbackMissingTarget 回滚不存在的快照归类为 ErrSnapshotNotFound，且不产生提交。
func TestRollbackMissingTarget(t *testing.T) {
	tb := newTable(t, pkSchema())
	cur := tb.CurrentSeq()
	if _, err := tb.Rollback(999); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}
	if tb.CurrentSeq() != cur {
		t.Fatalf("failed rollback must not advance seq")
	}
	t.Logf("判定依据: 目标 seq=999 不存在 => ErrSnapshotNotFound, 当前序号保持 %d", cur)
}

// TestRollbackCannotCombineWrites 回滚与写入/结构演进互斥。
func TestRollbackCannotCombineWrites(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx := tb.Begin()
	tx.Upsert(Row{1: 1, 2: "a"})
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	cur := tb.CurrentSeq()

	tx = tb.Begin()
	tx.RollbackTo(1)
	tx.Append(Row{1: 9, 2: "x"})
	if _, err := tx.Commit(); err == nil {
		t.Fatalf("rollback+write in one txn must be rejected")
	}
	if tb.CurrentSeq() != cur {
		t.Fatalf("rejected txn must not advance seq")
	}
	t.Logf("判定依据: 同一事务内 rollback+append 被拒绝, 序号保持 %d", cur)
}

// TestRollbackVsConcurrentWrite 回滚与并发写入竞争：恰有一方成功，
// 失败方收到 ErrConflictConcurrentCommit；不会出现回滚盖住写入或两笔都算数。
func TestRollbackVsConcurrentWrite(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx := tb.Begin()
	tx.Upsert(Row{1: 1, 2: "a"})
	s1, _ := tx.Commit() // 2

	// 同一基线上：一个回滚事务 + 一个写事务
	rtx := tb.Begin()
	rtx.RollbackTo(1)
	wtx := tb.Begin()
	wtx.Upsert(Row{1: 2, 2: "b"})

	rSeq, rErr := rtx.Commit()
	wSeq, wErr := wtx.Commit()

	if (rErr == nil) == (wErr == nil) {
		t.Fatalf("exactly one of rollback/write must win: rErr=%v wErr=%v", rErr, wErr)
	}
	loserErr := rErr
	if rErr == nil {
		loserErr = wErr
	}
	if !errors.Is(loserErr, ErrConflictConcurrentCommit) {
		t.Fatalf("loser must get ErrConflictConcurrentCommit, got %v", loserErr)
	}

	rows, err := tb.Scan(0)
	if err != nil {
		t.Fatal(err)
	}
	if rErr == nil {
		// 回滚赢：内容回到快照 1（空表），写入不可见
		if len(rows) != 0 {
			t.Fatalf("rollback won (seq=%d): expected 0 rows, got %+v", rSeq, rows)
		}
		t.Logf("判定依据: 回滚 seq=%d 胜出, 写入失败(%v), 最新内容为空表=快照 1 状态", rSeq, wErr)
	} else {
		// 写入赢：两行都在，回滚未生效
		if len(rows) != 2 {
			t.Fatalf("write won (seq=%d): expected 2 rows, got %+v", wSeq, rows)
		}
		t.Logf("判定依据: 写入 seq=%d 胜出, 回滚失败(%v), 最新内容含全部 2 行", wSeq, rErr)
	}
	if tb.CurrentSeq() != s1+1 {
		t.Fatalf("exactly one commit may land: current seq=%d, want %d", tb.CurrentSeq(), s1+1)
	}
}

// TestRollbackUnderConcurrentLoad 多路回滚与写入并发：每次竞争恰一胜，
// 最终序号连续、每个已提交快照都可读且内容确定。
func TestRollbackUnderConcurrentLoad(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx := tb.Begin()
	tx.Upsert(Row{1: 1, 2: "a"})
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	target := tb.CurrentSeq()

	const actors = 8
	var wg sync.WaitGroup
	for i := 0; i < actors; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				var err error
				if i%2 == 0 {
					_, err = tb.Rollback(target)
				} else {
					tx := tb.Begin()
					tx.Upsert(Row{1: 100 + i, 2: "w"})
					_, err = tx.Commit()
				}
				if err == nil {
					return
				}
				if errors.Is(err, ErrConflictConcurrentCommit) {
					continue // 竞争失败，重试直到成功一次
				}
				t.Errorf("unexpected error: %v", err)
				return
			}
		}(i)
	}
	wg.Wait()

	// 8 个 actor 各成功一次 => 序号从 target 起连续推进 8 步
	if got := tb.CurrentSeq(); got != target+actors {
		t.Fatalf("expected seq=%d after %d successful commits, got %d", target+actors, actors, got)
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
			t.Fatalf("every committed snapshot must be readable: seq=%d err=%v", e.Seq, err)
		}
	}
	t.Logf("判定依据: %d 路并发(回滚/写入各半)各成功 1 次, 序号连续到 %d, 全部 %d 个快照可读",
		actors, target+actors, len(hist))
}
