package lakehouse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// LocalFS 用本地文件系统模拟存储层。所有元数据落盘均走"临时文件 + fsync + 原子 rename"，
// 因此读取方要么看到完整的旧版本、要么看到完整的新版本，不存在部分可见。
type LocalFS struct {
	root string
}

func NewLocalFS(root string) *LocalFS { return &LocalFS{root: root} }

func (fs *LocalFS) Root() string { return fs.root }

func (fs *LocalFS) abs(rel string) string { return filepath.Join(fs.root, filepath.FromSlash(rel)) }

func (fs *LocalFS) mkdirAll(rel string) error {
	if err := os.MkdirAll(fs.abs(rel), 0o755); err != nil {
		return ioError("mkdir", rel, err)
	}
	return nil
}

func (fs *LocalFS) exists(rel string) (bool, error) {
	_, err := os.Stat(fs.abs(rel))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, ioError("stat", rel, err)
}

func (fs *LocalFS) remove(rel string) error {
	if err := os.Remove(fs.abs(rel)); err != nil && !os.IsNotExist(err) {
		return ioError("remove", rel, err)
	}
	return nil
}

func (fs *LocalFS) openRead(rel string) (io.ReadCloser, error) {
	f, err := os.Open(fs.abs(rel))
	if err != nil {
		return nil, ioError("open", rel, err)
	}
	return f, nil
}

func (fs *LocalFS) readJSON(rel string, v any) error {
	f, err := os.Open(fs.abs(rel))
	if err != nil {
		return ioError("open", rel, err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	if err := dec.Decode(v); err != nil {
		return ioError("decode", rel, err)
	}
	return nil
}

// writeAtomic 在同目录建临时文件，写完 fsync 后 rename，保证原子可见。
func (fs *LocalFS) writeAtomic(rel string, fn func(w io.Writer) error) error {
	target := fs.abs(rel)
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return ioError("mkdir", rel, err)
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return ioError("create_temp", rel, err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if err := fn(tmp); err != nil {
		_ = tmp.Close()
		cleanup()
		return ioError("write", rel, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return ioError("fsync", rel, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return ioError("close", rel, err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		cleanup()
		return ioError("rename", rel, err)
	}
	// fsync 目录，保证 rename 在崩溃语义下持久化。
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

func (fs *LocalFS) atomicWriteJSON(rel string, v any) error {
	return fs.writeAtomic(rel, func(w io.Writer) error {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	})
}

// fileLock 基于 flock 提供跨进程排他；配合 Table 的进程内 channel 锁双重串行化。
type fileLock struct {
	path string
	f    *os.File
}

func acquireLock(lockPath string) (*fileLock, error) {
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o755); err != nil {
		return nil, ioError("mkdir", lockPath, err)
	}
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, ioError("open_lock", lockPath, err)
	}
	return &fileLock{path: lockPath, f: f}, nil
}

// lockNB 非阻塞尝试一次排他锁。
func (l *fileLock) lockNB() (bool, error) {
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		return true, nil
	}
	if err == syscall.EWOULDBLOCK {
		return false, nil
	}
	return false, ioError("flock", l.path, err)
}

func (l *fileLock) release() error {
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	cerr := l.f.Close()
	if err != nil {
		return ioError("unlock", l.path, err)
	}
	return cerr
}

// withTableLock 串行化整表的元数据提交：
// 先取进程内锁（避免同进程 goroutine 自旋），再取跨进程 flock；
// ctx 取消时等待也会立即中止。
func (t *Table) withTableLock(ctx context.Context, fn func() error) error {
	if t.closed {
		return newError(KindClosed, "表句柄已关闭")
	}
	select {
	case t.mu <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-t.mu }()

	fl, err := acquireLock(t.fs.abs(lockFileName))
	if err != nil {
		return err
	}
	// 非阻塞 + 退避轮询，使等待可被 ctx 中断。
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		got, err := fl.lockNB()
		if err != nil {
			_ = fl.release()
			return err
		}
		if got {
			break
		}
		select {
		case <-ctx.Done():
			_ = fl.release()
			return ctx.Err()
		case <-ticker.C:
		}
	}
	defer func() { _ = fl.release() }()

	return fn()
}

func ioError(op, path string, err error) error {
	return newError(KindIO, "%s %s: %s", op, path, err)
}

var _ = fmt.Sprintf
