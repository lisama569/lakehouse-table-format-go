package lakehouse

import (
	"errors"
	"testing"
)

// evolve 提交一次纯结构演进，返回新快照序号。
func evolve(t *testing.T, tb *Table, s Schema) (int, error) {
	t.Helper()
	tx := tb.Begin()
	tx.EvolveSchema(s)
	return tx.Commit()
}

// TestAddColumnReadAcrossSnapshots 新增可空列：旧快照读不到新列，新快照历史行补 null。
func TestAddColumnReadAcrossSnapshots(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx := tb.Begin()
	tx.Upsert(Row{1: 1, 2: "a"})
	s1, _ := tx.Commit()

	newSchema := Schema{
		Columns: []Column{
			{ID: 1, Name: "id", Type: TypeInt64},
			{ID: 2, Name: "val", Type: TypeString, Nullable: true},
			{ID: 3, Name: "extra", Type: TypeString, Nullable: true},
		},
		PrimaryKey: []int{1},
	}
	s2, err := evolve(t, tb, newSchema)
	if err != nil {
		t.Fatalf("add nullable column should be accepted: %v", err)
	}

	old, _ := tb.Scan(s1)
	if _, has := old[0][3]; has {
		t.Fatalf("snapshot %d must not see new column: %+v", s1, old[0])
	}
	cur, _ := tb.Scan(s2)
	v, has := cur[0][3]
	if !has || v != nil {
		t.Fatalf("snapshot %d should fill null for added column, got %v", s2, v)
	}
	t.Logf("判定依据: 快照=%d 无列 extra, 快照=%d 历史行 extra=null (新增可空列兼容)", s1, s2)
}

// TestRenameColumn 重命名按列 ID 寻址，历史数据在新旧快照下都可读。
func TestRenameColumn(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx := tb.Begin()
	tx.Upsert(Row{1: 1, 2: "a"})
	s1, _ := tx.Commit()

	renamed := Schema{
		Columns: []Column{
			{ID: 1, Name: "id", Type: TypeInt64},
			{ID: 2, Name: "value", Type: TypeString, Nullable: true}, // val -> value
		},
		PrimaryKey: []int{1},
	}
	s2, err := evolve(t, tb, renamed)
	if err != nil {
		t.Fatalf("rename should be accepted: %v", err)
	}
	cur, _ := tb.Scan(s2)
	if cur[0][2] != "a" {
		t.Fatalf("renamed column lost data: %+v", cur[0])
	}
	old, _ := tb.Scan(s1)
	if old[0][2] != "a" {
		t.Fatalf("old snapshot read unstable after rename: %+v", old[0])
	}
	t.Logf("判定依据: 列 id=2 由 val 重命名为 value, 快照=%d 与快照=%d 读取均为 'a'", s1, s2)
}

// TestTypeNarrowing int64->int32：值可表示时允许，读取语义稳定。
func TestTypeNarrowing(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx := tb.Begin()
	tx.Upsert(Row{1: 42, 2: "a"})
	s1, _ := tx.Commit()

	narrowed := Schema{
		Columns: []Column{
			{ID: 1, Name: "id", Type: TypeInt32},
			{ID: 2, Name: "val", Type: TypeString, Nullable: true},
		},
		PrimaryKey: []int{1},
	}
	s2, err := evolve(t, tb, narrowed)
	if err != nil {
		t.Fatalf("narrowing with in-range data should be accepted: %v", err)
	}
	cur, _ := tb.Scan(s2)
	if n, ok := toInt64(cur[0][1]); !ok || n != 42 {
		t.Fatalf("narrowed read wrong: %+v", cur[0])
	}
	t.Logf("判定依据: 列 id 收窄 int64->int32, 存量值 42 可表示, 快照=%d 提交成功, 读取=42", s2)
	_ = s1
}

// TestNarrowingOverflowRejected 收窄时存量值溢出 => 提交期拒绝，而非读取期报错。
func TestNarrowingOverflowRejected(t *testing.T) {
	tb := newTable(t, pkSchema())
	tx := tb.Begin()
	tx.Upsert(Row{1: int64(1) << 40, 2: "big"}) // 超出 int32
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	narrowed := Schema{
		Columns: []Column{
			{ID: 1, Name: "id", Type: TypeInt32},
			{ID: 2, Name: "val", Type: TypeString, Nullable: true},
		},
		PrimaryKey: []int{1},
	}
	_, err := evolve(t, tb, narrowed)
	if !errors.Is(err, ErrSchemaIncompatible) {
		t.Fatalf("expected ErrSchemaIncompatible, got %v", err)
	}
	t.Logf("判定依据: 存量值 2^40 溢出 int32, 提交期拒绝 => %v", err)
}

// TestIncompatibleEvolutions 各类不兼容演进必须在提交时被拒绝。
func TestIncompatibleEvolutions(t *testing.T) {
	cases := []struct {
		name   string
		schema Schema
	}{
		{"删列", Schema{Columns: []Column{{ID: 1, Name: "id", Type: TypeInt64}}, PrimaryKey: []int{1}}},
		{"类型 widening/变更", Schema{Columns: []Column{
			{ID: 1, Name: "id", Type: TypeInt64},
			{ID: 2, Name: "val", Type: TypeInt64, Nullable: true}, // string -> int64
		}, PrimaryKey: []int{1}}},
		{"重命名撞名", Schema{Columns: []Column{
			{ID: 1, Name: "id", Type: TypeInt64},
			{ID: 2, Name: "id", Type: TypeString, Nullable: true},
		}, PrimaryKey: []int{1}}},
		{"新增非空列", Schema{Columns: []Column{
			{ID: 1, Name: "id", Type: TypeInt64},
			{ID: 2, Name: "val", Type: TypeString, Nullable: true},
			{ID: 3, Name: "req", Type: TypeString},
		}, PrimaryKey: []int{1}}},
		{"主键变更", Schema{Columns: []Column{
			{ID: 1, Name: "id", Type: TypeInt64},
			{ID: 2, Name: "val", Type: TypeString, Nullable: true},
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tb := newTable(t, pkSchema())
			_, err := evolve(t, tb, tc.schema)
			if !errors.Is(err, ErrSchemaIncompatible) {
				t.Fatalf("expected ErrSchemaIncompatible, got %v", err)
			}
			t.Logf("判定依据: %s => %v (提交序号未推进: current=%d)", tc.name, err, tb.CurrentSeq())
			if tb.CurrentSeq() != 1 {
				t.Fatalf("rejected commit must not advance snapshot, got %d", tb.CurrentSeq())
			}
		})
	}
}
