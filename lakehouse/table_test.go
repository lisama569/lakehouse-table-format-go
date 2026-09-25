package lakehouse

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func pkSchema() Schema {
	return Schema{
		Columns: []Column{
			{ID: 1, Name: "id", Type: TypeInt64},
			{ID: 2, Name: "val", Type: TypeString, Nullable: true},
		},
		PrimaryKey: []int{1},
	}
}

func newTable(t *testing.T, s Schema) *Table {
	t.Helper()
	tb, err := CreateTable(filepath.Join(t.TempDir(), "tbl"), s)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	return tb
}

// TestConcurrentCommitConflict 两个事务基于同一快照提交，后者必须被拒绝。
func TestConcurrentCommitConflict(t *testing.T) {
	tb := newTable(t, pkSchema())

	tx1 := tb.Begin()
	tx1.Upsert(Row{1: 1, 2: "a"})
	tx2 := tb.Begin()
	tx2.Upsert(Row{1: 2, 2: "b"})

	seq1, err := tx1.Commit()
	if err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}
	_, err = tx2.Commit()
	if !errors.Is(err, ErrConflictConcurrentCommit) {
		t.Fatalf("tx2 should fail with ErrConflictConcurrentCommit, got %v", err)
	}
	t.Logf("判定依据: tx1 提交序号=%d 推进了快照, tx2 基线快照=%d 已过期 => %v",
		seq1, tx2.baseSeq, err)

	// 冲突的提交不得部分可见
	rows, err := tb.Scan(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after conflict, got %d", len(rows))
	}
}

// TestPrimaryKeyConflict 并发 upsert 触碰同一主键，必须归类为主键冲突。
func TestPrimaryKeyConflict(t *testing.T) {
	tb := newTable(t, pkSchema())

	tx0 := tb.Begin()
	tx0.Upsert(Row{1: 1, 2: "init"})
	base, err := tx0.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx1 := tb.Begin()
	tx1.Upsert(Row{1: 1, 2: "from-tx1"})
	tx2 := tb.Begin()
	tx2.Upsert(Row{1: 1, 2: "from-tx2"})

	// tx1 先提交；tx2 基线已过期且触碰了 tx1 改过的主键 => 主键冲突
	seq1, err := tx1.Commit()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx2.Commit()
	if !errors.Is(err, ErrConflictPrimaryKey) {
		t.Fatalf("expected ErrConflictPrimaryKey, got %v", err)
	}
	t.Logf("判定依据: 基线快照=%d 之后提交序号=%d 已修改主键 '1', tx2 重复触碰 => %v",
		base, seq1, err)
}

// TestConcurrentCommitRace 多 goroutine 基于同一快照同时提交，只有一个成功。
func TestConcurrentCommitRace(t *testing.T) {
	tb := newTable(t, pkSchema())
	const n = 8
	// 先建立全部事务（同一基线快照），再并发提交
	txs := make([]*Txn, n)
	for i := range txs {
		txs[i] = tb.Begin()
		txs[i].Upsert(Row{1: 100 + i, 2: fmt.Sprintf("w%d", i)})
	}
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = txs[i].Commit()
		}(i)
	}
	wg.Wait()
	wins := 0
	for i := range errs {
		if errs[i] == nil {
			wins++
		} else if !errors.Is(errs[i], ErrConflictConcurrentCommit) {
			t.Fatalf("unexpected error type: %v", errs[i])
		}
	}
	t.Logf("判定依据: %d 个并发提交中 %d 个成功, 其余因基线快照过期被拒", n, wins)
	if wins != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", wins)
	}
	rows, _ := tb.Scan(0)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

// TestSnapshotIsolation 读取必须看到一致快照，不受后续提交影响。
func TestSnapshotIsolation(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx1 := tb.Begin()
	tx1.Upsert(Row{1: 1, 2: "v1"})
	s1, _ := tx1.Commit()

	tx2 := tb.Begin()
	tx2.Upsert(Row{1: 1, 2: "v2"}, Row{1: 2, 2: "x"})
	s2, _ := tx2.Commit()

	old, err := tb.Scan(s1)
	if err != nil {
		t.Fatal(err)
	}
	if len(old) != 1 || old[0][2] != "v1" {
		t.Fatalf("snapshot %d read unstable: %+v", s1, old)
	}
	cur, _ := tb.Scan(s2)
	if len(cur) != 2 {
		t.Fatalf("snapshot %d expected 2 rows, got %d", s2, len(cur))
	}
	for _, r := range cur {
		if r[1] == int64(1) || r[1] == float64(1) {
			if r[2] != "v2" {
				t.Fatalf("upsert did not win: %+v", r)
			}
		}
	}
	t.Logf("判定依据: 快照=%d 看到 v1, 快照=%d 看到 v2 且主键 1 不重复", s1, s2)
}

// TestSnapshotNotFound 读取不存在/已清理的快照须归类。
func TestSnapshotNotFound(t *testing.T) {
	tb := newTable(t, pkSchema())
	_, err := tb.Scan(999)
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}
	t.Logf("判定依据: 快照标识=999 不存在 => %v", err)
}
