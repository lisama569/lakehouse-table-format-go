package lakehouse

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// pin.go: 快照固定（pin）。
//
// 被固定的快照受双重保护：清理不会删除其快照文件，且它引用的数据文件一律
// 视为存活，合并/清理都不能回收。固定信息存为 <dir>/pins/NNNNNN 标记文件，
// 以 O_EXCL 排他创建；固定/解除固定都在表级锁（mu + flock）下进行，
// 与跨进程的清理并发也不会丢标记或误删。

// PinMarker pin 标记文件内容。
type PinMarker struct {
	Seq      int    `json:"seq"`
	PinnedAt string `json:"pinned_at"`
}

func pinsDir(dir string) string { return filepath.Join(dir, "pins") }

func pinPath(dir string, seq int) string {
	return filepath.Join(pinsDir(dir), fmt.Sprintf("%06d.json", seq))
}

// Pin 显式固定指定快照。快照不存在返回 ErrSnapshotNotFound；
// 已经固定返回 ErrAlreadyPinned（可用 errors.Is 判定为幂等命中）。
func (t *Table) Pin(seq int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.withTableLock(func() error {
		if _, err := t.LoadSnapshot(seq); err != nil {
			return err
		}
		if err := os.MkdirAll(pinsDir(t.dir), 0o755); err != nil {
			return err
		}
		marker := PinMarker{Seq: seq, PinnedAt: time.Now().UTC().Format(time.RFC3339)}
		if err := createExclusive(pinPath(t.dir, seq), marker); err != nil {
			if errors.Is(err, os.ErrExist) {
				return fmt.Errorf("%w: seq=%d", ErrAlreadyPinned, seq)
			}
			return err
		}
		return nil
	})
}

// Unpin 解除固定。快照本身不存在返回 ErrSnapshotNotFound；
// 快照存在但未被固定返回 ErrNotPinned，成功返回 nil。
func (t *Table) Unpin(seq int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.withTableLock(func() error {
		if _, err := t.LoadSnapshot(seq); err != nil {
			return err
		}
		switch err := os.Remove(pinPath(t.dir, seq)); {
		case err == nil:
			return nil
		case errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("%w: seq=%d", ErrNotPinned, seq)
		default:
			return err
		}
	})
}

// Pins 返回当前被固定的快照序号（升序）；固定快照可能已被手动破坏时
// 仅返回快照文件仍然存在的标记。
func (t *Table) Pins() []int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pinnedSeqsLocked()
}

// pinnedSeqsLocked 读取固定序号集合（调用方持锁）。
func (t *Table) pinnedSeqsLocked() []int {
	entries, err := os.ReadDir(pinsDir(t.dir))
	if err != nil {
		return nil
	}
	var seqs []int
	for _, e := range entries {
		n, err := strconv.Atoi(strings.TrimSuffix(e.Name(), ".json"))
		if err != nil {
			continue
		}
		seqs = append(seqs, n)
	}
	sort.Ints(seqs)
	return seqs
}

// pinnedSetLocked 返回固定序号集合，便于 O(1) 查询。
func (t *Table) pinnedSetLocked() map[int]bool {
	set := map[int]bool{}
	for _, s := range t.pinnedSeqsLocked() {
		set[s] = true
	}
	return set
}
