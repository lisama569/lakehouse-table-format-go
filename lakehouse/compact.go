package lakehouse

import (
	"fmt"
	"os"
	"path/filepath"
)

// compact.go: 小文件合并与旧快照清理。
//
// 合并：基于当前快照全量合并为单个数据文件，以新快照发布；
// 旧快照及其文件保持不动，并发读写不受影响（读旧快照仍是完整一致的）。
// 若合并期间有新提交抢先落盘，合并以 ErrConflictConcurrentCommit 失败，
// 不会覆盖他人提交。
//
// 清理：仅删除序号 <= beforeSeq 且不在最近 retain 个快照之内的快照；
// 数据文件只有在不被任何存活快照引用时才删除。越权请求整体拒绝，
// 不做部分清理。

// Compact 合并当前快照的所有数据文件，返回新快照序号。
func (t *Table) Compact() (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	baseSeq := t.CurrentSeq()
	base, err := t.LoadSnapshot(baseSeq)
	if err != nil {
		return 0, err
	}
	rows, err := t.scanSnapshot(base)
	if err != nil {
		return 0, err
	}
	seq := baseSeq + 1
	var files []string
	if len(rows) > 0 {
		f, err := t.writeDataFile(seq, base.Schema, rows)
		if err != nil {
			return 0, err
		}
		files = append(files, f)
	}
	snap := &Snapshot{
		Seq:       seq,
		Schema:    base.Schema,
		DataFiles: files,
		Parent:    baseSeq,
		Kind:      "compact",
	}
	if err := createExclusive(snapshotPath(t.dir, seq), snap); err != nil {
		return 0, fmt.Errorf("%w: compact lost race at seq=%d",
			ErrConflictConcurrentCommit, seq)
	}
	return seq, nil
}

// Cleanup 清理序号 <= beforeSeq 的旧快照及不再被引用的数据文件。
// retain 表示必须保留的最近快照个数；若请求会删除被保留的快照，
// 返回 ErrCleanupRejected 且不做任何删除。
func (t *Table) Cleanup(beforeSeq int, retain int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	seqs := t.listSnapshotSeqs()
	if len(seqs) == 0 {
		return fmt.Errorf("%w: table has no snapshots", ErrCleanupRejected)
	}
	if retain < 1 {
		retain = 1
	}
	// 保留线：最近 retain 个快照的最小序号
	keepFrom := seqs[len(seqs)-retain]
	if retain > len(seqs) {
		keepFrom = seqs[0]
	}
	if beforeSeq >= keepFrom {
		return fmt.Errorf("%w: beforeSeq=%d would delete retained snapshots (keep seq>=%d)",
			ErrCleanupRejected, beforeSeq, keepFrom)
	}

	// 待删除快照
	var victims []int
	for _, s := range seqs {
		if s <= beforeSeq {
			victims = append(victims, s)
		}
	}
	if len(victims) == 0 {
		return fmt.Errorf("%w: no snapshots at or before seq=%d", ErrCleanupRejected, beforeSeq)
	}

	// 收集存活快照引用的数据文件
	alive := map[string]bool{}
	for _, s := range seqs {
		if s <= beforeSeq {
			continue
		}
		snap, err := t.LoadSnapshot(s)
		if err != nil {
			return err
		}
		for _, f := range snap.DataFiles {
			alive[f] = true
		}
	}
	// 先删快照文件，再删独占数据文件
	for _, s := range victims {
		snap, err := t.LoadSnapshot(s)
		if err != nil {
			continue
		}
		if err := os.Remove(snapshotPath(t.dir, s)); err != nil {
			return err
		}
		for _, f := range snap.DataFiles {
			if !alive[f] {
				os.Remove(filepath.Join(t.dir, f))
			}
		}
	}
	return nil
}
