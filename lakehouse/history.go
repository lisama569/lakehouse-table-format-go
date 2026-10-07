package lakehouse

import "fmt"

// history.go: 提交历史与回滚。
//
// 历史列表只读 snapshots/*.json 的元数据（每条 O(1) 个小文件，不触碰 data/），
// 行数在提交时已物化为 Snapshot.RowCount，因此历史再长也不需要扫全量数据。
//
// 回滚（Rollback）把某个历史快照的 schema 与数据文件清单重新发布为一个
// 追加的新快照（kind=rollback, parent=回滚前最新快照, rollback_to=目标）。
// 历史快照与其读出的结果永不被改写；回滚与普通写入竞争同一序号时，
// 后到者收到 ErrConflictConcurrentCommit，不会互相覆盖，也不会两笔都生效。

// History 返回提交历史摘要，按序号升序。
//   - afterSeq：只返回序号 > afterSeq 的条目（0 表示从头）；
//   - limit：>0 时最多返回 limit 条，用于分页，避免一次返回过多元数据。
//
// 仅读取快照元数据文件，不扫描任何数据文件。
func (t *Table) History(afterSeq int, limit int) ([]CommitInfo, error) {
	seqs := t.listSnapshotSeqs()
	var out []CommitInfo
	for _, s := range seqs {
		if s <= afterSeq {
			continue
		}
		snap, err := t.LoadSnapshot(s)
		if err != nil {
			return nil, err
		}
		out = append(out, commitInfo(snap))
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// commitInfo 从快照元数据提取历史摘要（纯内存，无 I/O）。
func commitInfo(s *Snapshot) CommitInfo {
	return CommitInfo{
		Seq:        s.Seq,
		Parent:     s.Parent,
		Kind:       s.Kind,
		RowCount:   s.RowCount,
		RollbackTo: s.RollbackTo,
	}
}

// Rollback 把表回滚到历史快照 targetSeq，发布为一个向后追加的新快照并返回其序号。
// 它是 BeginRollback + Commit 的便捷封装；需要与并发写入明确分出胜负时，
// 应显式使用 BeginRollback 提前记录基线。
//
// 语义：
//   - 回滚是普通提交：新快照 Parent 指向回滚前的最新快照，序号正常 +1，
//     kind=rollback、rollback_to=targetSeq；历史快照及其读取结果永不被改写；
//   - 内容（schema 与数据文件清单）直接复用目标快照，因此回滚后 Scan(新序号)
//     与 Scan(targetSeq) 的行完全一致；
//   - targetSeq 不存在（或已被清理）返回 ErrSnapshotNotFound；
//   - 回滚与写入/合并并发竞争同一序号时，后到者收到 ErrConflictConcurrentCommit：
//     绝不会出现回滚盖掉并发写入、也不会两笔都算数。
func (t *Table) Rollback(targetSeq int) (int, error) {
	rt, err := t.BeginRollback(targetSeq)
	if err != nil {
		return 0, err
	}
	return rt.Commit()
}

// RollbackTxn 一次带回滚目标的乐观事务。
type RollbackTxn struct {
	table     *Table
	baseSeq   int // BeginRollback 时的表头序号
	targetSeq int
	committed bool
}

// BeginRollback 基于当前表头开启回滚事务，并立即校验目标快照存在。
func (t *Table) BeginRollback(targetSeq int) (*RollbackTxn, error) {
	if _, err := t.LoadSnapshot(targetSeq); err != nil {
		return nil, fmt.Errorf("%w: rollback target seq=%d", ErrSnapshotNotFound, targetSeq)
	}
	return &RollbackTxn{table: t, baseSeq: t.CurrentSeq(), targetSeq: targetSeq}, nil
}

// Commit 发布回滚快照。基线表头在此期间被写入/合并/另一次回滚推进，则
// 返回 ErrConflictConcurrentCommit，不会用历史内容盖掉并发提交。
func (rt *RollbackTxn) Commit() (int, error) {
	t := rt.table
	t.mu.Lock()
	defer t.mu.Unlock()
	var seq int
	err := t.withTableLock(func() error {
		var cerr error
		seq, cerr = rt.commitLocked()
		return cerr
	})
	return seq, err
}

func (rt *RollbackTxn) commitLocked() (int, error) {
	t := rt.table
	cur := t.CurrentSeq()
	if cur != rt.baseSeq {
		return 0, fmt.Errorf("%w: rollback base seq=%d, current seq=%d",
			ErrConflictConcurrentCommit, rt.baseSeq, cur)
	}
	// 提交点再次加载目标：它可能在 Begin 之后被清理。
	target, err := t.LoadSnapshot(rt.targetSeq)
	if err != nil {
		return 0, fmt.Errorf("%w: rollback target seq=%d", ErrSnapshotNotFound, rt.targetSeq)
	}
	next := rt.baseSeq + 1
	snap := &Snapshot{
		Seq:        next,
		Schema:     target.Schema,
		DataFiles:  append([]string{}, target.DataFiles...),
		Parent:     rt.baseSeq,
		Kind:       KindRollback,
		RowCount:   target.RowCount,
		RollbackTo: rt.targetSeq,
	}
	if err := createExclusive(snapshotPath(t.dir, next), snap); err != nil {
		return 0, fmt.Errorf("%w: rollback lost race at seq=%d",
			ErrConflictConcurrentCommit, next)
	}
	rt.committed = true
	return next, nil
}
