package lakehouse

import (
	"fmt"
	"path/filepath"
	"sort"
)

// pin.go: 快照固定（pin）。
//
// 长期对账/重放任务可把某个快照固定住：被固定的快照不会被清理删除，
// 即使它已掉到保留窗口之外。固定清单位于 <dir>/pins.json，进程内由
// Table.mu 串行化读写，写盘采用临时文件 + rename 保证原子可见。

// pinsFile 固定清单。
type pinsFile struct {
	Pinned []int `json:"pinned"`
}

func (t *Table) pinsPath() string { return filepath.Join(t.dir, "pins.json") }

// loadPins 读取固定清单（文件不存在视为空）。
func (t *Table) loadPins() map[int]bool {
	pinned := map[int]bool{}
	var pf pinsFile
	if err := readJSON(t.pinsPath(), &pf); err == nil {
		for _, s := range pf.Pinned {
			pinned[s] = true
		}
	}
	return pinned
}

func (t *Table) storePins(pinned map[int]bool) error {
	seqs := make([]int, 0, len(pinned))
	for s := range pinned {
		seqs = append(seqs, s)
	}
	sort.Ints(seqs)
	return writeJSONAtomic(t.pinsPath(), pinsFile{Pinned: seqs})
}

// PinSnapshot 固定指定快照；快照不存在返回 ErrSnapshotNotFound。
// 重复固定同一快照是幂等成功。
func (t *Table) PinSnapshot(seq int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, err := t.LoadSnapshot(seq); err != nil {
		return fmt.Errorf("%w: cannot pin seq=%d", ErrSnapshotNotFound, seq)
	}
	pinned := t.loadPins()
	pinned[seq] = true
	return t.storePins(pinned)
}

// UnpinSnapshot 解除固定；该快照未被固定时返回 ErrSnapshotNotPinned。
func (t *Table) UnpinSnapshot(seq int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	pinned := t.loadPins()
	if !pinned[seq] {
		return fmt.Errorf("%w: seq=%d", ErrSnapshotNotPinned, seq)
	}
	delete(pinned, seq)
	return t.storePins(pinned)
}

// PinnedSnapshots 按序号升序返回当前被固定的快照。
func (t *Table) PinnedSnapshots() []int {
	t.mu.Lock()
	defer t.mu.Unlock()
	pinned := t.loadPins()
	seqs := make([]int, 0, len(pinned))
	for s := range pinned {
		seqs = append(seqs, s)
	}
	sort.Ints(seqs)
	return seqs
}
