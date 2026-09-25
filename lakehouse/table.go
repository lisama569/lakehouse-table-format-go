package lakehouse

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Table 一张本地湖仓表。
type Table struct {
	dir string
	mu  sync.Mutex // 进程内提交串行化；跨进程由快照文件排他创建兜底
}

// CreateTable 在 dir 创建一张新表，并写入序号 1 的初始空快照。
func CreateTable(dir string, schema Schema) (*Table, error) {
	if err := validateSchema(schema); err != nil {
		return nil, err
	}
	for _, sub := range []string{"data", "snapshots"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	t := &Table{dir: dir}
	if t.CurrentSeq() != 0 {
		return nil, fmt.Errorf("lakehouse: table already exists in %s", dir)
	}
	snap := &Snapshot{Seq: 1, Schema: schema, Parent: 0, Kind: "schema"}
	if err := createExclusive(snapshotPath(dir, 1), snap); err != nil {
		return nil, err
	}
	return t, nil
}

// OpenTable 打开已有表。
func OpenTable(dir string) (*Table, error) {
	t := &Table{dir: dir}
	if t.CurrentSeq() == 0 {
		return nil, fmt.Errorf("lakehouse: no table found in %s", dir)
	}
	return t, nil
}

// CurrentSeq 返回当前最新快照序号（无快照返回 0）。
func (t *Table) CurrentSeq() int {
	seqs := t.listSnapshotSeqs()
	if len(seqs) == 0 {
		return 0
	}
	return seqs[len(seqs)-1]
}

func (t *Table) listSnapshotSeqs() []int {
	entries, err := os.ReadDir(filepath.Join(t.dir, "snapshots"))
	if err != nil {
		return nil
	}
	var seqs []int
	for _, e := range entries {
		name := strings.TrimSuffix(e.Name(), ".json")
		if n, err := strconv.Atoi(name); err == nil {
			seqs = append(seqs, n)
		}
	}
	sort.Ints(seqs)
	return seqs
}

// LoadSnapshot 读取指定序号的快照；不存在返回 ErrSnapshotNotFound。
func (t *Table) LoadSnapshot(seq int) (*Snapshot, error) {
	var snap Snapshot
	if err := readJSON(snapshotPath(t.dir, seq), &snap); err != nil {
		return nil, fmt.Errorf("%w: seq=%d", ErrSnapshotNotFound, seq)
	}
	return &snap, nil
}

// validateSchema 校验结构本身合法：列 ID 唯一、列名唯一、主键存在。
func validateSchema(s Schema) error {
	ids := map[int]bool{}
	names := map[string]bool{}
	for _, c := range s.Columns {
		if ids[c.ID] {
			return fmt.Errorf("%w: duplicate column id %d", ErrSchemaIncompatible, c.ID)
		}
		if names[c.Name] {
			return fmt.Errorf("%w: duplicate column name %q", ErrSchemaIncompatible, c.Name)
		}
		ids[c.ID] = true
		names[c.Name] = true
	}
	for _, pk := range s.PrimaryKey {
		if !ids[pk] {
			return fmt.Errorf("%w: primary key column id %d not found", ErrSchemaIncompatible, pk)
		}
	}
	return nil
}
