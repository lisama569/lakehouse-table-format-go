package lakehouse

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// commit.go: 以快照为单位的提交协议（乐观并发控制）。
//
// 协议：
//  1. Begin 记录基线快照序号 baseSeq；
//  2. Commit 时校验当前序号仍等于 baseSeq，否则 ErrConflictConcurrentCommit；
//  3. upsert 事务额外校验：自基线以来各提交触碰的主键集合与本次无交集，
//     否则 ErrConflictPrimaryKey；
//  4. 结构演进在提交时做兼容校验（validateEvolution + 存量数据收窄扫描）；
//  5. 先写数据文件，再以 O_EXCL 创建下一序号快照文件作为提交点——
//     跨进程并发时只有一个提交者能成功，其余收到 ErrConflictConcurrentCommit。

// Txn 一次以快照为单位的写入事务。
type Txn struct {
	table     *Table
	baseSeq   int
	rows      []Row
	upsert    bool
	schema    *Schema // 非空表示同时提交结构演进
	committed bool
}

// Begin 基于当前快照开启事务。
func (t *Table) Begin() *Txn {
	return &Txn{table: t, baseSeq: t.CurrentSeq()}
}

// Append 追加行。
func (tx *Txn) Append(rows ...Row) { tx.rows = append(tx.rows, rows...) }

// Upsert 按主键更新/插入行（表须定义主键）。
func (tx *Txn) Upsert(rows ...Row) {
	tx.upsert = true
	tx.rows = append(tx.rows, rows...)
}

// EvolveSchema 随本次提交进行结构演进（提交时做兼容校验）。
func (tx *Txn) EvolveSchema(s Schema) { tx.schema = &s }

// Commit 提交事务，返回新快照序号；冲突时返回可归类错误。
func (tx *Txn) Commit() (int, error) {
	t := tx.table
	t.mu.Lock()
	defer t.mu.Unlock()
	var seq int
	err := t.withTableLock(func() error {
		var cerr error
		seq, cerr = tx.commitLocked()
		return cerr
	})
	return seq, err
}

func (tx *Txn) commitLocked() (int, error) {
	t := tx.table
	base, err := t.LoadSnapshot(tx.baseSeq)
	if err != nil {
		return 0, err
	}
	// 1. 基线校验：提交期间快照被推进 => 冲突。
	// 若本次 upsert 触碰的主键与基线之后的提交重叠，归因为更具体的
	// ErrConflictPrimaryKey；否则归因为 ErrConflictConcurrentCommit。
	cur := t.CurrentSeq()
	stale := cur != tx.baseSeq

	// 2. upsert 校验：主键存在且与并发提交触碰的键无交集
	var keys []string
	if tx.upsert {
		if len(base.Schema.PrimaryKey) == 0 {
			return 0, fmt.Errorf("%w: upsert on table without primary key", ErrSchemaIncompatible)
		}
		keys, err = primaryKeysOf(base.Schema, tx.rows)
		if err != nil {
			return 0, err
		}
		if err := t.checkKeyConflicts(tx.baseSeq, keys); err != nil {
			return 0, err
		}
	}
	if stale {
		return 0, fmt.Errorf("%w: base seq=%d, current seq=%d",
			ErrConflictConcurrentCommit, tx.baseSeq, cur)
	}

	// 3. 结构演进校验（含存量数据收窄扫描）
	newSchema := base.Schema
	kind := "append"
	if tx.upsert {
		kind = "upsert"
	}
	if tx.schema != nil {
		if err := validateEvolution(base.Schema, *tx.schema); err != nil {
			return 0, err
		}
		if err := t.validateNarrowingData(base, *tx.schema); err != nil {
			return 0, err
		}
		newSchema = *tx.schema
		kind = "schema"
	}

	// 4. 写数据文件（先数据后快照，快照是提交点）
	seq := tx.baseSeq + 1
	var files []string
	if len(tx.rows) > 0 {
		f, err := t.writeDataFile(seq, newSchema, tx.rows)
		if err != nil {
			return 0, err
		}
		files = append(files, f)
	}

	// 5. 排他创建快照文件：跨进程并发时只有一个提交者成功
	snap := &Snapshot{
		Seq:       seq,
		Schema:    newSchema,
		DataFiles: append(append([]string{}, base.DataFiles...), files...),
		Parent:    tx.baseSeq,
		Kind:      kind,
		Keys:      keys,
		RowCount:  t.nextRowCount(base, tx.rows),
	}
	if err := createExclusive(snapshotPath(t.dir, seq), snap); err != nil {
		return 0, fmt.Errorf("%w: snapshot seq=%d already committed by another writer",
			ErrConflictConcurrentCommit, seq)
	}
	tx.committed = true
	return seq, nil
}

// nextRowCount 计算提交后新快照的逻辑行数并物化，列历史时无需再扫数据。
//   - 无主键（仅追加）表：行数直接为旧行数 + 本次写入行数；
//   - 主键表：旧版本已有主键集合 ∪ 本次主键集合，只读取旧数据文件取键，
//     不做整行合并（约数级别的开销，且结构演进提交沿用基线行数）。
func (t *Table) nextRowCount(base *Snapshot, rows []Row) int {
	if len(base.Schema.PrimaryKey) == 0 {
		return base.RowCount + len(rows)
	}
	if len(rows) == 0 {
		return base.RowCount
	}
	keys := make(map[string]bool, base.RowCount)
	pk := base.Schema.PrimaryKey
	for _, rel := range base.DataFiles {
		_ = t.readDataFile(rel, func(r Row) {
			keys[rowKey(pk, r)] = true
		})
	}
	newKeys, err := primaryKeysOf(base.Schema, rows)
	if err != nil {
		// 主键缺失已在 upsert 校验阶段拦截；append 表走到这里时退化为全量计数
		return t.countRowsFallback(base)
	}
	for _, k := range newKeys {
		keys[k] = true
	}
	return len(keys)
}

// countRowsFallback 兜底：完整合并快照数据后计数。
func (t *Table) countRowsFallback(base *Snapshot) int {
	rows, err := t.scanSnapshot(base)
	if err != nil {
		return base.RowCount
	}
	return len(rows)
}

// checkKeyConflicts 检查 keys 是否与基线之后各 upsert 提交触碰的键重叠。
func (t *Table) checkKeyConflicts(baseSeq int, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	mine := map[string]bool{}
	for _, k := range keys {
		mine[k] = true
	}
	for _, seq := range t.listSnapshotSeqs() {
		if seq <= baseSeq {
			continue
		}
		snap, err := t.LoadSnapshot(seq)
		if err != nil {
			continue
		}
		for _, k := range snap.Keys {
			if mine[k] {
				return fmt.Errorf("%w: key %q already modified by commit seq=%d",
					ErrConflictPrimaryKey, k, seq)
			}
		}
	}
	return nil
}

// validateNarrowingData 扫描存量数据，验证收窄列的所有值均可表示。
func (t *Table) validateNarrowingData(base *Snapshot, new Schema) error {
	cols := needsDataValidation(base.Schema, new)
	if len(cols) == 0 {
		return nil
	}
	rows, err := t.scanSnapshot(base)
	if err != nil {
		return err
	}
	for _, row := range rows {
		for _, c := range cols {
			if err := validateNarrowingValue(c, row[c.ID]); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeDataFile 把一批行写成 JSON Lines 数据文件，返回相对表目录的路径。
func (t *Table) writeDataFile(seq int, schema Schema, rows []Row) (string, error) {
	name := fmt.Sprintf("part-%06d-%d.jsonl", seq, len(rows))
	rel := filepath.Join("data", name)
	f, err := os.Create(filepath.Join(t.dir, rel))
	if err != nil {
		return "", err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, r := range rows {
		m := map[string]any{}
		for id, v := range r {
			m[fmt.Sprintf("%d", id)] = v
		}
		if err := enc.Encode(m); err != nil {
			return "", err
		}
	}
	return rel, nil
}

// primaryKeysOf 提取每行的主键字符串。
func primaryKeysOf(schema Schema, rows []Row) ([]string, error) {
	var keys []string
	for _, r := range rows {
		parts := make([]string, len(schema.PrimaryKey))
		for i, id := range schema.PrimaryKey {
			v, ok := r[id]
			if !ok || v == nil {
				return nil, fmt.Errorf("%w: upsert row missing primary key column id=%d",
					ErrSchemaIncompatible, id)
			}
			parts[i] = fmt.Sprintf("%v", v)
		}
		keys = append(keys, strings.Join(parts, "\x00"))
	}
	return keys, nil
}
