package lakehouse

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestMixedConcurrentOperations 写入/合并/回滚/清理混合并发：
// 状态可预期——唯一提交点推进序号；固定锚点永不消失；每个存活快照的
// 物化行数与其数据实际合并行数一致（不丢不重的内部一致性）。
func TestMixedConcurrentOperations(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tbl")
	tb := newTableAt(t, dir, pkSchema())

	anchor := commitN(t, tb, 1, "anchor-v1")
	if err := tb.Pin(anchor); err != nil {
		t.Fatal(err)
	}
	// 再写几行建立回滚目标
	var targets []int
	for i := 2; i <= 4; i++ {
		targets = append(targets, commitN(t, tb, int64(i), fmt.Sprintf("v%d", i)))
	}

	const writers, compactors, rollbacks, cleaners = 6, 2, 2, 2
	var wg sync.WaitGroup
	run := func(n int, fn func(id int)) {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(id int) { defer wg.Done(); fn(id) }(i)
		}
	}

	// 每个写者用全局不重复的主键，避免触发主键冲突分类（那不是本用例目标）
	var keySeq int
	var keyMu sync.Mutex
	nextKey := func() int64 {
		keyMu.Lock()
		defer keyMu.Unlock()
		keySeq++
		return int64(100 + keySeq)
	}

	run(writers, func(id int) {
		for i := 0; i < 20; i++ {
			tx := tb.Begin()
			tx.Upsert(Row{1: nextKey(), 2: fmt.Sprintf("w%d-%d", id, i)})
			if _, err := tx.Commit(); err != nil {
				if !errors.Is(err, ErrConflictConcurrentCommit) &&
					!errors.Is(err, ErrConflictPrimaryKey) {
					t.Errorf("writer unexpected error: %v", err)
					return
				}
			}
		}
	})
	run(compactors, func(id int) {
		for i := 0; i < 10; i++ {
			if _, err := tb.Compact(); err != nil &&
				!errors.Is(err, ErrConflictConcurrentCommit) {
				t.Errorf("compactor unexpected error: %v", err)
				return
			}
		}
	})
	run(rollbacks, func(id int) {
		for i := 0; i < 10; i++ {
			target := targets[(id+i)%len(targets)]
			if _, err := tb.Rollback(target); err != nil &&
				!errors.Is(err, ErrConflictConcurrentCommit) &&
				!errors.Is(err, ErrSnapshotNotFound) {
				t.Errorf("rollback unexpected error: %v", err)
				return
			}
		}
	})
	run(cleaners, func(id int) {
		for i := 0; i < 10; i++ {
			// 只尝试清理锚点之前的序号（锚点=2，之前只有初始快照 1）；
			// 命中固定或无对象都是合法拒绝，且必须零删除。
			err := tb.Cleanup(anchor-1, 1)
			if err != nil &&
				!errors.Is(err, ErrCleanupPinned) &&
				!errors.Is(err, ErrCleanupNothing) &&
				!errors.Is(err, ErrCleanupOutOfRange) {
				t.Errorf("cleaner unexpected error: %v", err)
				return
			}
		}
	})
	wg.Wait()

	// 不变量 1：固定锚点始终存在、可读、内容不变
	arows, err := tb.Scan(anchor)
	if err != nil {
		t.Fatalf("pinned anchor %d must survive everything: %v", anchor, err)
	}
	if len(arows) != 1 || arows[0][2] != "anchor-v1" {
		t.Fatalf("anchor content changed: %+v", arows)
	}

	// 不变量 2：存活序号严格递增（旧快照可能已被合法清理，不要求从 1 连续）
	seqs := tb.listSnapshotSeqs()
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("snapshot seqs must be strictly increasing, got %v", seqs)
		}
	}
	if len(seqs) == 0 || seqs[0] < 1 {
		t.Fatalf("at least snapshot 1 must have existed, got %v", seqs)
	}

	// 不变量 3：每个存活快照——引用文件齐全；物化行数与实际合并行数一致
	checked := 0
	for _, s := range seqs {
		snap, err := tb.LoadSnapshot(s)
		if err != nil {
			t.Fatalf("load snapshot %d: %v", s, err)
		}
		for _, f := range snap.DataFiles {
			if _, err := os.Stat(filepath.Join(tb.dir, f)); err != nil {
				t.Fatalf("snapshot %d references missing file %s: %v", s, f, err)
			}
		}
		rows, err := tb.scanSnapshot(snap)
		if err != nil {
			t.Fatalf("scan snapshot %d: %v", s, err)
		}
		if len(rows) != snap.RowCount {
			t.Fatalf("snapshot %d row_count=%d but merged rows=%d (lost or duplicated)",
				s, snap.RowCount, len(rows))
		}
		checked++
	}

	// 最新快照可完整读取
	if _, err := tb.Scan(0); err != nil {
		t.Fatalf("latest snapshot unreadable: %v", err)
	}
	t.Logf("判定依据: 混合并发结束后最新序号=%d, 存活快照=%d 个全部行数与元数据一致, 固定锚点=%d 内容不变",
		seqs[len(seqs)-1], checked, anchor)
}

// newTableAt 在指定目录建表（供多句柄场景使用）。
func newTableAt(t *testing.T, dir string, s Schema) *Table {
	t.Helper()
	tb, err := CreateTable(dir, s)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return tb
}
