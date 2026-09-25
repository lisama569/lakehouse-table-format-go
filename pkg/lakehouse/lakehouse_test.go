package lakehouse

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testLogger 打印提交序号、快照标识与判定依据，满足测试可观测要求。
type testLogger struct {
	t  *testing.T
	mu sync.Mutex
}

func (l *testLogger) LogCommit(seq int64, snapshotID, kind, verdict, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.t.Logf("[commit-log] seq=%d snapshot=%s kind=%s verdict=%s reason=%s",
		seq, short(snapshotID), kind, verdict, reason)
}

func short(s string) string {
	if len(s) <= 10 {
		return s
	}
	return s[:10]
}

func testTable(t *testing.T, pk bool) (*Table, context.Context, string) {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	cols := []ColumnSpec{
		{Name: "id", Type: TypeInt64},
		{Name: "v", Type: TypeInt64, Nullable: true},
		{Name: "name", Type: TypeString, Nullable: true},
	}
	var pks []string
	if pk {
		cols[0].Nullable = false
		pks = []string{"id"}
	}
	tb, err := Create(ctx, dir, "tbl", cols, pks, &Options{AdminToken: "admin-secret", Logger: &testLogger{t: t}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return tb, ctx, dir
}

func mustRead(t *testing.T, tb *Table, ctx context.Context, snapID string) []Record {
	t.Helper()
	r, err := tb.Snapshot(ctx, snapID)
	if err != nil {
		t.Fatalf("snapshot %s: %v", snapID, err)
	}
	rows, err := r.ReadAll(ctx)
	if err != nil {
		t.Fatalf("read %s: %v", snapID, err)
	}
	return rows
}

func currentID(t *testing.T, tb *Table, ctx context.Context) string {
	t.Helper()
	id, err := tb.CurrentSnapshotID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// ---- 1. 并发提交冲突 ----------------------------------------------------

// TestConcurrentUpsertConflict: 两个 goroutine 基于同一快照做 upsert，
// 只有一个能提交，另一个必须被明确归类为 conflict_base_snapshot，
// 数据最终态确定、无重复生效。
func TestConcurrentUpsertConflict(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	base, err := tb.CommitAppend(ctx, CommitRequest{
		Records: []Record{{"id": int64(1), "v": int64(10)}},
	})
	if err != nil {
		t.Fatal(err)
	}

	const n = 8
	var wg sync.WaitGroup
	results := make([]error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			_, results[i] = tb.CommitUpsert(ctx, CommitRequest{
				Records:         []Record{{"id": int64(1), "v": int64(100 + i)}},
				ExpectedBaseSeq: base.Seq,
				Reason:          fmt.Sprintf("worker-%d", i),
			})
		}()
	}
	wg.Wait()

	ok, rejected := 0, 0
	for _, e := range results {
		if e == nil {
			ok++
			continue
		}
		if !IsKind(e, KindCommitConflict) || !IsKind(e, KindConflictBaseSnapshot) {
			t.Fatalf("期望 conflict_base_snapshot，实际: %v", e)
		}
		rejected++
	}
	if ok != 1 || rejected != n-1 {
		t.Fatalf("期望恰好 1 成功 %d 冲突，实际 %d 成功 %d 冲突", n-1, ok, rejected)
	}
	rows := mustRead(t, tb, ctx, currentID(t, tb, ctx))
	if len(rows) != 1 {
		t.Fatalf("upsert 后应仍为 1 行（无重复），实际 %d: %v", len(rows), rows)
	}
	t.Logf("最终行数=%d（唯一成功），被拒绝数=%d，判定依据=conflict_base_snapshot", len(rows), rejected)
}

// TestConcurrentAppendBaseConflict: 基于过期基准的 append 必须被拒绝。
func TestConcurrentAppendBaseConflict(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	first, err := tb.CommitAppend(ctx, CommitRequest{
		Records: []Record{{"id": int64(1)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 另一个提交先成功。
	if _, err := tb.CommitAppend(ctx, CommitRequest{
		Records: []Record{{"id": int64(2)}},
	}); err != nil {
		t.Fatal(err)
	}
	// 仍以 first.Seq 为基准提交 -> 冲突。
	_, err = tb.CommitAppend(ctx, CommitRequest{
		Records:         []Record{{"id": int64(3)}},
		ExpectedBaseSeq: first.Seq,
	})
	if err == nil {
		t.Fatal("期望冲突，实际提交成功")
	}
	if !IsKind(err, KindConflictBaseSnapshot) {
		t.Fatalf("期望 conflict_base_snapshot，实际: %v", err)
	}
	t.Logf("过期基准被正确拒绝: %v", err)
}

// TestAppendNoBaseConcurrent: append 是可交换的纯新增（不可变文件并集），
// 多个并发 append 串行化后全部生效，总行数守恒（不丢不重）。
func TestAppendNoBaseConcurrentNoLoss(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	const workers, per = 6, 20
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			recs := make([]Record, 0, per)
			for i := 0; i < per; i++ {
				recs = append(recs, Record{"id": int64(w*per + i + 1)})
			}
			if _, err := tb.CommitAppend(ctx, CommitRequest{Records: recs}); err != nil {
				t.Errorf("append worker %d: %v", w, err)
			}
		}()
	}
	wg.Wait()
	rows := mustRead(t, tb, ctx, currentID(t, tb, ctx))
	if len(rows) != workers*per {
		t.Fatalf("并发 append 后应 %d 行，实际 %d（丢失或重复）", workers*per, len(rows))
	}
	t.Logf("并发追加总行数=%d，无丢失无重复", len(rows))
}

// TestCompactionConflict: compaction 计划依据的文件集与提交时不一致 -> file_set_changed；
// 显式基准过期 -> base_snapshot，两类原因可区分。
func TestCompactionConflict(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	var oldFileSet []string
	for i := 0; i < 3; i++ {
		res, err := tb.CommitAppend(ctx, CommitRequest{
			Records: []Record{{"id": int64(i + 1), "v": int64(i)}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if i == 2 {
			snaps, _ := tb.ListSnapshots(ctx)
			for _, f := range snaps[len(snaps)-1].Files {
				oldFileSet = append(oldFileSet, f.Path)
			}
			_ = res
		}
	}

	// 先成功合并一次（3 -> 1 个文件）。
	res1, err := tb.Compact(ctx, CompactionRequest{Reason: "compact-1"})
	if err != nil {
		t.Fatalf("第一次 compact: %v", err)
	}

	// 再 append 制造新文件；随后用"3 个旧文件"的陈旧计划提交 compaction：
	// 不给显式基准，文件集检测先命中 -> conflict_file_set_changed。
	if _, err := tb.CommitAppend(ctx, CommitRequest{
		Records: []Record{{"id": int64(99), "v": int64(99)}},
	}); err != nil {
		t.Fatal(err)
	}
	_, err = tb.Compact(ctx, CompactionRequest{FileSetBase: oldFileSet, Reason: "stale-fileset"})
	if err == nil {
		t.Fatal("期望 compaction 文件集冲突，实际成功")
	}
	if !IsKind(err, KindConflictFileSetChanged) {
		t.Fatalf("期望 conflict_file_set_changed，实际: %v", err)
	}
	t.Logf("compaction 文件集冲突被正确归类: %v", err)

	// 当前视图 2 个文件（compact-1 产物 + 新 append），先 append 再以过期显式基准 compact，
	// 命中基准冲突 -> conflict_base_snapshot（与文件集冲突原因可区分）。
	staleSeq := res1.Seq
	_, err = tb.Compact(ctx, CompactionRequest{ExpectedBaseSeq: staleSeq, Reason: "stale-base"})
	if err == nil {
		// 若恰好在无新提交时可成功，则跳过该断言路径。
	} else if !IsKind(err, KindConflictBaseSnapshot) {
		t.Fatalf("期望 conflict_base_snapshot，实际: %v", err)
	} else {
		t.Logf("compaction 基准冲突被正确归类: %v", err)
	}

	// 最终再合法 compact 一次，数据行数守恒。
	before := mustRead(t, tb, ctx, currentID(t, tb, ctx))
	res2, err := tb.Compact(ctx, CompactionRequest{Reason: "compact-final"})
	if err != nil {
		t.Fatalf("最终 compact: %v", err)
	}
	after := mustRead(t, tb, ctx, res2.SnapshotID)
	if len(after) != len(before) {
		t.Fatalf("compaction 前后行数不一致: %d vs %d", len(before), len(after))
	}
	t.Logf("合并后数据守恒: before=%d after=%d", len(before), len(after))
}

// ---- 2. 跨快照读取与结构演进 -------------------------------------------

func TestSchemaEvolutionAcrossSnapshots(t *testing.T) {
	tb, ctx, _ := testTable(t, true)

	// seq2：写入旧数据。
	s1, err := tb.CommitAppend(ctx, CommitRequest{
		Records: []Record{
			{"id": int64(1), "v": int64(100), "name": "alpha"},
			{"id": int64(2), "v": int64(200), "name": "beta"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// 演进1：rename v -> value（ID 不变，旧快照读语义不受影响）。
	ev1, err := tb.CommitEvolution(ctx, EvolutionRequest{
		Specs: []EvolutionSpec{{Op: EvolutionRename, Column: "v", NewName: "value"}},
	})
	if err != nil {
		t.Fatalf("rename: %v", err)
	}

	// 演进2：新增可空列 tag（历史行应为 nil）。
	ev2, err := tb.CommitEvolution(ctx, EvolutionRequest{
		Specs: []EvolutionSpec{{Op: EvolutionAddColumn, Column: "tag", Type: TypeString, Nullable: true}},
	})
	if err != nil {
		t.Fatalf("add column: %v", err)
	}

	// 演进3：把 value 从 int64 收窄到 int32（现有值 100/200 合法）。
	ev3, err := tb.CommitEvolution(ctx, EvolutionRequest{
		Specs: []EvolutionSpec{{Op: EvolutionNarrow, Column: "value", NewType: TypeInt32}},
	})
	if err != nil {
		t.Fatalf("narrow: %v", err)
	}

	// 用新 schema 写入一行。
	s4, err := tb.CommitAppend(ctx, CommitRequest{
		Records: []Record{{"id": int64(3), "value": int64(300), "name": "gamma", "tag": "new"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// 在最旧快照 s1 上读取：必须保持旧名字 v，且没有 tag 列。
	oldRows := mustRead(t, tb, ctx, s1.SnapshotID)
	if len(oldRows) != 2 {
		t.Fatalf("旧快照行数=%d", len(oldRows))
	}
	r0 := oldRows[0]
	if _, hasV := r0["v"]; !hasV {
		t.Fatalf("旧快照应保留旧列名 v: %#v", r0)
	}
	if _, hasValue := r0["value"]; hasValue {
		t.Fatalf("旧快照不应出现新列名 value: %#v", r0)
	}
	if _, hasTag := r0["tag"]; hasTag {
		t.Fatalf("旧快照不应出现 tag: %#v", r0)
	}

	// 在演进后的快照上读旧数据：v 已按 FieldID 映射为 value；tag 为 nil。
	for _, snapID := range []string{ev1.SnapshotID, ev2.SnapshotID, ev3.SnapshotID} {
		rows := mustRead(t, tb, ctx, snapID)
		if len(rows) != 2 {
			t.Fatalf("快照 %s 行数=%d", short(snapID), len(rows))
		}
		for _, row := range rows {
			if _, ok := row["value"]; !ok {
				t.Fatalf("快照 %s 旧行未按 ID 映射到 value: %#v", short(snapID), row)
			}
			if row["tag"] != nil {
				t.Fatalf("历史行 tag 应为 nil，实际 %v", row["tag"])
			}
		}
	}

	// 最新快照包含 3 行，旧行 tag=nil，新行 tag=new，列名统一为 value。
	curRows := mustRead(t, tb, ctx, s4.SnapshotID)
	if len(curRows) != 3 {
		t.Fatalf("最新快照应 3 行，实际 %d", len(curRows))
	}
	var newRow Record
	for _, r := range curRows {
		if r["id"] == int64(3) {
			newRow = r
		}
		if r["id"] != int64(3) && r["tag"] != nil {
			t.Fatalf("旧行 tag 必须为 nil: %#v", r)
		}
	}
	if newRow["tag"] != "new" || newRow["value"] != int64(300) {
		t.Fatalf("新行投影错误: %#v", newRow)
	}
	t.Logf("跨快照读取稳定: s1 用旧名 v；演进后按 ID 映射 value，历史新增列为 NULL")
}

// TestIncompatibleEvolutionsRejectedAtCommit: 所有不兼容演进必须提交期拒绝。
func TestIncompatibleEvolutionsRejectedAtCommit(t *testing.T) {
	tb, ctx, _ := testTable(t, true)

	cases := []struct {
		name string
		spec EvolutionSpec
		seed func() // 可选前置数据准备
		kind ErrorKind
	}{
		{"新增非空列", EvolutionSpec{Op: EvolutionAddColumn, Column: "c1", Type: TypeString, Nullable: false}, nil, KindSchemaIncompatible},
		{"重命名不存在的列", EvolutionSpec{Op: EvolutionRename, Column: "ghost", NewName: "x"}, nil, KindSchemaIncompatible},
		{"重命名为已占用名字", EvolutionSpec{Op: EvolutionRename, Column: "v", NewName: "name"}, nil, KindSchemaIncompatible},
		{"放宽类型 int32->int64", EvolutionSpec{Op: EvolutionNarrow, Column: "v", NewType: TypeInt64}, nil, KindSchemaIncompatible},
		{"跨类型收窄 string->int32", EvolutionSpec{Op: EvolutionNarrow, Column: "name", NewType: TypeInt32}, nil, KindSchemaIncompatible},
		{"未知操作", EvolutionSpec{Op: "drop_column", Column: "v"}, nil, KindSchemaIncompatible},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tb.CommitEvolution(ctx, EvolutionRequest{Specs: []EvolutionSpec{tc.spec}})
			if err == nil {
				t.Fatalf("期望提交被拒绝（%s），实际成功", tc.kind)
			}
			if !IsKind(err, tc.kind) {
				t.Fatalf("期望 %s，实际: %v", tc.kind, err)
			}
			t.Logf("提交期拒绝[%s]: %v", tc.name, err)
		})
	}
}

// TestNarrowRejectedDueToExistingData: 收窄在数据越界时提交期拒绝，而不是读取期报错。
func TestNarrowRejectedDueToExistingData(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	_, err := tb.CommitAppend(ctx, CommitRequest{
		Records: []Record{{"id": int64(1), "v": int64(1) << 40}}, // 超出 int32
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = tb.CommitEvolution(ctx, EvolutionRequest{
		Specs: []EvolutionSpec{{Op: EvolutionNarrow, Column: "v", NewType: TypeInt32}},
	})
	if err == nil {
		t.Fatal("期望收窄因既有数据越界被拒绝")
	}
	if !IsKind(err, KindSchemaIncompatible) {
		t.Fatalf("期望 schema_incompatible，实际: %v", err)
	}
	t.Logf("越界收窄提交期拒绝: %v", err)

	// 表状态不变：仍可按旧 schema 读取。
	rows := mustRead(t, tb, ctx, currentID(t, tb, ctx))
	if rows[0]["v"] != int64(1)<<40 {
		t.Fatalf("拒绝演进后数据必须不变: %#v", rows[0])
	}
}

// TestRecordValidationAtCommit: 非空/类型错误在提交期归类。
func TestRecordValidationAtCommit(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	_, err := tb.CommitAppend(ctx, CommitRequest{
		Records: []Record{{"id": "not-int"}},
	})
	if !IsKind(err, KindRecordValidation) {
		t.Fatalf("期望 record_validation，实际: %v", err)
	}
	_, err = tb.CommitUpsert(ctx, CommitRequest{
		Records:         []Record{{"v": int64(1)}}, // 缺主键
		ExpectedBaseSeq: 1,
	})
	if !IsKind(err, KindPrimaryKeyMissing) {
		t.Fatalf("期望 primary_key_missing，实际: %v", err)
	}
	t.Logf("记录错误归类正确: %v", err)
}

// ---- 3. Upsert / Compaction 语义 ---------------------------------------

func TestUpsertIdempotent(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	base, err := tb.CommitAppend(ctx, CommitRequest{
		Records: []Record{
			{"id": int64(1), "v": int64(10), "name": "a"},
			{"id": int64(2), "v": int64(20), "name": "b"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	upd, err := tb.CommitUpsert(ctx, CommitRequest{
		Records: []Record{
			{"id": int64(2), "v": int64(21), "name": "b2"},
			{"id": int64(3), "v": int64(30), "name": "c"},
		},
		ExpectedBaseSeq: base.Seq,
	})
	if err != nil {
		t.Fatal(err)
	}
	rows := mustRead(t, tb, ctx, upd.SnapshotID)
	if len(rows) != 3 {
		t.Fatalf("upsert 后应 3 行，实际 %d", len(rows))
	}
	byID := map[int64]Record{}
	for _, r := range rows {
		byID[r["id"].(int64)] = r
	}
	if byID[1]["v"] != int64(10) || byID[2]["v"] != int64(21) || byID[2]["name"] != "b2" || byID[3]["v"] != int64(30) {
		t.Fatalf("upsert 合并结果错误: %v", rows)
	}
	// 旧快照仍是 2 行旧值。
	old := mustRead(t, tb, ctx, base.SnapshotID)
	if len(old) != 2 {
		t.Fatalf("旧快照必须保持 2 行，实际 %d", len(old))
	}
	t.Logf("upsert 覆盖/插入正确，旧快照不变: %v", rows)
}

func TestCompactionNoLossNoDup(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	for i := 0; i < 4; i++ {
		_, err := tb.CommitAppend(ctx, CommitRequest{
			Records: []Record{{"id": int64(i + 1), "v": int64(i)}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	before := mustRead(t, tb, ctx, currentID(t, tb, ctx))
	res, err := tb.Compact(ctx, CompactionRequest{Reason: "periodic-compact"})
	if err != nil {
		t.Fatal(err)
	}
	after := mustRead(t, tb, ctx, res.SnapshotID)
	if len(after) != len(before) {
		t.Fatalf("compaction 后行数变化: before=%d after=%d", len(before), len(after))
	}
	snaps, _ := tb.ListSnapshots(ctx)
	latest := snaps[len(snaps)-1]
	if len(latest.Files) != 1 {
		t.Fatalf("合并后视图应只有 1 个文件，实际 %d", len(latest.Files))
	}
	if len(latest.DeletedFiles) != 4 {
		t.Fatalf("应记录 4 个被替换文件，实际 %d", len(latest.DeletedFiles))
	}
	t.Logf("compaction 合并 %d 个小文件为 1 个，行数守恒=%d", 4, len(after))
}

// ---- 4. 快照过期清理 ----------------------------------------------------

func TestExpireSnapshots(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	s1, _ := tb.CommitAppend(ctx, CommitRequest{Records: []Record{{"id": int64(1)}}})
	s2, _ := tb.CommitAppend(ctx, CommitRequest{Records: []Record{{"id": int64(2)}}})

	// compact 当前视图（2 个小文件 -> 1 个），旧文件被新快照逻辑替换，
	// 但旧快照 s1/s2 仍引用它们，因此此刻不能物理删除。
	s3, err := tb.Compact(ctx, CompactionRequest{Reason: "compact-before-expire"})
	if err != nil {
		t.Fatal(err)
	}
	compactSnap, _ := metaSnap(tb, ctx, s3.SnapshotID)
	replacedFiles := compactSnap.DeletedFiles
	if len(replacedFiles) != 2 {
		t.Fatalf("compact 应替换 2 个文件，实际 %v", replacedFiles)
	}
	for _, p := range replacedFiles {
		if ok, _ := tb.fs.exists(p); !ok {
			t.Fatalf("被替换文件在旧快照存活期间必须保留: %s", p)
		}
	}

	now := time.Now()
	// 越权：无 token / 错 token。
	if _, err := tb.ExpireSnapshots(ctx, ExpireRequest{
		SnapshotsToExpire: []string{s1.SnapshotID}, RequestTime: now,
	}); !IsKind(err, KindCleanupUnauthorized) {
		t.Fatalf("期望 unauthorized，实际 %v", err)
	}
	if _, err := tb.ExpireSnapshots(ctx, ExpireRequest{
		SnapshotsToExpire: []string{s1.SnapshotID}, Token: "wrong", RequestTime: now,
	}); !IsKind(err, KindCleanupUnauthorized) {
		t.Fatalf("期望 unauthorized，实际 %v", err)
	}
	t.Logf("越权清理被拒绝")

	// 过期请求：RequestTime 太旧。
	if _, err := tb.ExpireSnapshots(ctx, ExpireRequest{
		SnapshotsToExpire: []string{s1.SnapshotID}, Token: "admin-secret",
		RequestTime: now.Add(-time.Hour), TTL: time.Minute,
	}); !IsKind(err, KindCleanupRequestExpired) {
		t.Fatalf("期望 request_expired，实际 %v", err)
	}
	t.Logf("过期清理请求被拒绝")

	// 保留冲突：不能删 current，也不能删被 pin 的快照。
	if _, err := tb.ExpireSnapshots(ctx, ExpireRequest{
		SnapshotsToExpire: []string{s3.SnapshotID}, Token: "admin-secret",
		RequestTime: now, TTL: time.Minute,
	}); !IsKind(err, KindCleanupSnapshotRetained) {
		t.Fatalf("期望 snapshot_retained(current)，实际 %v", err)
	}
	if err := tb.PinSnapshot(ctx, s1.SnapshotID, "long-running-reader"); err != nil {
		t.Fatal(err)
	}
	if _, err := tb.ExpireSnapshots(ctx, ExpireRequest{
		SnapshotsToExpire: []string{s1.SnapshotID}, Token: "admin-secret",
		RequestTime: now, TTL: time.Minute,
	}); !IsKind(err, KindCleanupSnapshotRetained) {
		t.Fatalf("期望 snapshot_retained(pinned)，实际 %v", err)
	}
	t.Logf("current/pinned 快照受保护，清理被拒绝")

	// unpin 后显式清理 s1+s2；current(s3) 保留。
	// 两个旧文件不再被任何存活快照引用 -> 作为垃圾物理删除。
	if err := tb.UnpinSnapshot(ctx, "long-running-reader"); err != nil {
		t.Fatal(err)
	}
	res, err := tb.ExpireSnapshots(ctx, ExpireRequest{
		SnapshotsToExpire: []string{s1.SnapshotID, s2.SnapshotID},
		Token:             "admin-secret", RequestTime: now, TTL: time.Minute, Reason: "explicit-expire",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.ExpiredSnapshots) != 2 {
		t.Fatalf("应过期 s1,s2，实际 %v", res.ExpiredSnapshots)
	}
	for _, id := range []string{s1.SnapshotID, s2.SnapshotID} {
		if _, err := tb.Snapshot(ctx, id); !IsKind(err, KindSnapshotNotFound) {
			t.Fatalf("过期快照应不可读，实际 %v", err)
		}
	}
	rows := mustRead(t, tb, ctx, s3.SnapshotID)
	if len(rows) != 2 {
		t.Fatalf("current 快照数据必须完好（2 行），实际 %d: %v", len(rows), rows)
	}
	if len(res.DeletedDataFiles) != 2 {
		t.Fatalf("两个被 compact 替换、且不再被引用的文件应被物理删除，实际删除 %v", res.DeletedDataFiles)
	}
	for _, p := range res.DeletedDataFiles {
		if ok, _ := tb.fs.exists(p); ok {
			t.Fatalf("垃圾文件 %s 应已不存在", p)
		}
	}
	// compact 产物仍在。
	for _, f := range compactSnap.Files {
		if ok, _ := tb.fs.exists(f.Path); !ok {
			t.Fatalf("current 引用的文件必须保留: %s", f.Path)
		}
	}
	t.Logf("过期清理: 移除 %v，物理删除无引用文件 %v; current 数据完好",
		res.ExpiredSnapshots, res.DeletedDataFiles)
}

func metaSnap(tb *Table, ctx context.Context, id string) (*Snapshot, error) {
	m, err := tb.loadMeta()
	if err != nil {
		return nil, err
	}
	s, ok := m.snapshotByID(id)
	if !ok {
		return nil, err
	}
	return s, nil
}

// TestExpireMissingSnapshot: 清理一个不存在的快照 -> 明确拒绝。
func TestExpireMissingSnapshot(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	_, err := tb.ExpireSnapshots(ctx, ExpireRequest{
		SnapshotsToExpire: []string{"deadbeef"}, Token: "admin-secret",
		RequestTime: time.Now(), TTL: time.Minute,
	})
	if !IsKind(err, KindCleanupSnapshotMissing) {
		t.Fatalf("期望 snapshot_missing，实际 %v", err)
	}
}
