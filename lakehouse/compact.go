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
	var seq int
	err := t.withTableLock(func() error {
		var cerr error
		seq, cerr = t.compactLocked()
		return cerr
	})
	return seq, err
}

func (t *Table) compactLocked() (int, error) {
	baseSeq := t.CurrentSeq()
	base, err := t.LoadSnapshot(baseSeq)
	if err != nil {
		return 0, err
	}
	rows, err := t.scanSnapshot(base)
	if err != nil {
		return 0, err
	}
	next := baseSeq + 1
	var files []string
	if len(rows) > 0 {
		f, err := t.writeDataFile(next, base.Schema, rows)
		if err != nil {
			return 0, err
		}
		files = append(files, f)
	}
	snap := &Snapshot{
		Seq:       next,
		Schema:    base.Schema,
		DataFiles: files,
		Parent:    baseSeq,
		Kind:      KindCompact,
		RowCount:  len(rows),
	}
	if err := createExclusive(snapshotPath(t.dir, next), snap); err != nil {
		return 0, fmt.Errorf("%w: compact lost race at seq=%d",
			ErrConflictConcurrentCommit, next)
	}
	return next, nil
}

// Cleanup 清理序号 <= beforeSeq 的旧快照及不再被引用的数据文件。
// retain 表示必须保留的最近快照个数。任何拒绝都是"零删除"：不会删掉任何
// 快照或数据文件，表保持完整可读。拒绝原因用 errors.Is 区分：
//   - ErrCleanupOutOfRange：请求越权（会删掉保留窗口内的快照，或 retain<=0 等非法范围）；
//   - ErrCleanupPinned：待删快照中包含被固定的快照（哪怕已在保留窗口之外）；
//   - ErrCleanupNothing：按当前规则没有任何可清理对象。
//
// 三类原因都同时匹配外层 ErrCleanupRejected。
func (t *Table) Cleanup(beforeSeq int, retain int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.withTableLock(func() error { return t.cleanupLocked(beforeSeq, retain) })
}

// cleanupRejected 生成"外层归类 + 细分原因"的可双重 errors.Is 判定的错误。
func cleanupRejected(reason error, format string, args ...any) error {
	return fmt.Errorf("%w (%w): %s", ErrCleanupRejected, reason, fmt.Sprintf(format, args...))
}

func (t *Table) cleanupLocked(beforeSeq int, retain int) error {
	seqs := t.listSnapshotSeqs()
	if len(seqs) == 0 {
		return cleanupRejected(ErrCleanupNothing, "table has no snapshots")
	}
	if retain < 1 {
		return cleanupRejected(ErrCleanupOutOfRange, "retain=%d must be >= 1", retain)
	}
	// 保留线：最近 retain 个快照的最小序号
	keepFrom := seqs[0]
	if retain < len(seqs) {
		keepFrom = seqs[len(seqs)-retain]
	}
	if beforeSeq >= keepFrom {
		return cleanupRejected(ErrCleanupOutOfRange,
			"beforeSeq=%d would delete retained snapshots (keep seq>=%d)", beforeSeq, keepFrom)
	}

	// 待删除快照
	var victims []int
	for _, s := range seqs {
		if s <= beforeSeq {
			victims = append(victims, s)
		}
	}

	// 固定快照保护：只要候选集合里有被固定的快照，整体拒绝，零删除。
	pinned := t.pinnedSetLocked()
	var blocked []int
	for _, s := range victims {
		if pinned[s] {
			blocked = append(blocked, s)
		}
	}
	if len(blocked) > 0 {
		return cleanupRejected(ErrCleanupPinned,
			"snapshots %v are pinned and must be unpinned first", blocked)
	}

	if len(victims) == 0 {
		return cleanupRejected(ErrCleanupNothing, "no snapshots at or before seq=%d", beforeSeq)
	}

	// 收集存活快照（序号 > beforeSeq）引用的数据文件；
	// 被固定的快照不可能落在 victims 里（上面已整批拒绝），但这里仍把所有
	// 固定快照显式计入存活集合，双保险，任何标记残留都不会导致文件被回收。
	alive := map[string]bool{}
	for _, s := range seqs {
		if s <= beforeSeq && !pinned[s] {
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
				if err := os.Remove(filepath.Join(t.dir, f)); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
		}
	}
	return nil
}
