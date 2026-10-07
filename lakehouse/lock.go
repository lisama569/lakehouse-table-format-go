package lakehouse

import (
	"os"
	"path/filepath"
	"syscall"
)

// lock.go: 表级跨进程互斥锁。
//
// 进程内并发由 Table.mu 串行化；跨进程的元数据变更（提交/合并/回滚/固定/清理）
// 再叠加一层对 <dir>/table.lock 的 flock 排他锁，保证"检查-删除""检查-建标记"
// 这类复合操作在跨进程场景下也不会交错。同进程重复加锁在持有 mu 的前提下不会
// 发生（所有变更路径都先拿 mu 再拿 flock）。

// withTableLock 持有表级排他锁执行 fn。
func (t *Table) withTableLock(fn func() error) error {
	f, err := os.OpenFile(filepath.Join(t.dir, "table.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return fn()
}
