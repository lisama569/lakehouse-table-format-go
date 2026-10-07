package lakehouse

// history.go: 提交历史清单。
//
// History 只读取 snapshots/ 下的快照元数据文件，绝不扫描数据文件，
// 因此开销随提交次数线性增长、与表内数据量无关；limit>0 时只加载
// 最近 limit 个快照的元数据，历史再长也能扛住。

// History 按序号升序列出表的提交历史。
// limit > 0 时只返回最近 limit 条（仍按序号升序）；limit <= 0 返回全部。
func (t *Table) History(limit int) ([]HistoryEntry, error) {
	seqs := t.listSnapshotSeqs()
	if limit > 0 && len(seqs) > limit {
		seqs = seqs[len(seqs)-limit:]
	}
	entries := make([]HistoryEntry, 0, len(seqs))
	for _, seq := range seqs {
		snap, err := t.LoadSnapshot(seq)
		if err != nil {
			return nil, err
		}
		entries = append(entries, HistoryEntry{
			Seq:        snap.Seq,
			Kind:       snap.Kind,
			Parent:     snap.Parent,
			Rows:       snap.Rows,
			RollbackTo: snap.RollbackTo,
		})
	}
	return entries, nil
}
