package lakehouse

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// TestCompactMergesFiles 合并后数据不丢不重，且只引用一个数据文件。
func TestCompactMergesFiles(t *testing.T) {
	tb := newTable(t, pkSchema())
	for i := 0; i < 5; i++ {
		tx := tb.Begin()
		tx.Upsert(Row{1: i, 2: fmt.Sprintf("v%d", i)})
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	// 同键再写一次，验证合并去重
	tx := tb.Begin()
	tx.Upsert(Row{1: 0, 2: "v0-new"})
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	cseq, err := tb.Compact()
	if err != nil {
		t.Fatal(err)
	}
	snap, _ := tb.LoadSnapshot(cseq)
	if len(snap.DataFiles) != 1 {
		t.Fatalf("compacted snapshot should reference 1 file, got %d", len(snap.DataFiles))
	}
	rows, _ := tb.Scan(cseq)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows after compact, got %d", len(rows))
	}
	for _, r := range rows {
		if n, _ := toInt64(r[1]); n == 0 && r[2] != "v0-new" {
			t.Fatalf("latest upsert lost in compact: %+v", r)
		}
	}
	t.Logf("判定依据: 合并快照=%d 引用 1 个数据文件, 5 行无重复, 主键 0 为最新值", cseq)
}

// TestCompactConcurrentReadWrite 合并期间并发读写不丢不重。
func TestCompactConcurrentReadWrite(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx := tb.Begin()
	tx.Upsert(Row{1: 1, 2: "a"})
	base, _ := tx.Commit()

	const writers = 4
	var wg sync.WaitGroup
	errs := make([]error, writers)
	// 并发：一个合并 + 多个顺序重试的写
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			_, err := tb.Compact()
			if err == nil || !errors.Is(err, ErrConflictConcurrentCommit) {
				return
			}
		}
	}()
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				tx := tb.Begin()
				tx.Upsert(Row{1: 10 + i, 2: fmt.Sprintf("w%d", i)})
				_, err := tx.Commit()
				if err == nil {
					return
				}
				if errors.Is(err, ErrConflictConcurrentCommit) {
					continue // 与合并或其他写冲突，重试
				}
				errs[i] = err
				return
			}
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	rows, err := tb.Scan(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1+writers {
		t.Fatalf("expected %d rows, got %d", 1+writers, len(rows))
	}
	old, err := tb.Scan(base)
	if err != nil || len(old) != 1 {
		t.Fatalf("base snapshot %d must stay readable: rows=%v err=%v", base, old, err)
	}
	t.Logf("判定依据: 合并与 %d 个并发写完成后, 最新快照=%d 共 %d 行, 基线快照=%d 仍可读",
		writers, tb.CurrentSeq(), len(rows), base)
}

// TestCleanupRetention 清理必须保留最近 retain 个快照及被引用文件。
func TestCleanupRetention(t *testing.T) {
	tb := newTable(t, pkSchema())
	for i := 0; i < 5; i++ {
		tx := tb.Begin()
		tx.Upsert(Row{1: i, 2: "x"})
		if _, err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	// 快照 1..6，保留最近 2 个 => 可清理 <=4
	if err := tb.Cleanup(4, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := tb.Scan(4); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("snapshot 4 should be gone, got %v", err)
	}
	if _, err := tb.Scan(5); err != nil {
		t.Fatalf("snapshot 5 must be retained: %v", err)
	}
	rows, _ := tb.Scan(0)
	if len(rows) != 5 {
		t.Fatalf("latest snapshot must keep all 5 rows, got %d", len(rows))
	}
	t.Logf("判定依据: 清理 <=4 且保留最近 2 个快照, 快照=4 已删, 快照=5/6 可读, 数据完整")
}

// TestCleanupRejected 越权/过期清理请求被明确拒绝且不删任何东西。
func TestCleanupRejected(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx := tb.Begin()
	tx.Upsert(Row{1: 1, 2: "a"})
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	cur := tb.CurrentSeq() // 2

	// 越权：会删掉被保留的快照
	if err := tb.Cleanup(cur, 1); !errors.Is(err, ErrCleanupRejected) {
		t.Fatalf("expected ErrCleanupRejected, got %v", err)
	}
	// 过期：没有可清理对象
	if err := tb.Cleanup(0, 1); !errors.Is(err, ErrCleanupRejected) {
		t.Fatalf("expected ErrCleanupRejected, got %v", err)
	}
	if tb.CurrentSeq() != cur {
		t.Fatalf("rejected cleanup must not change table")
	}
	if _, err := tb.Scan(cur); err != nil {
		t.Fatalf("current snapshot must survive rejected cleanup: %v", err)
	}
	t.Logf("判定依据: beforeSeq>=%d 越权 / beforeSeq=0 无对象, 均拒绝且快照=%d 完整", cur, cur)
}
