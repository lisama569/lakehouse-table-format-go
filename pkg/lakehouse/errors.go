package lakehouse

import (
	"errors"
	"fmt"
)

// ErrorKind 对冲突与拒绝原因进行稳定归类，调用方据此区分不同失败原因。
// 冲突类错误统一以 KindCommitConflict 为大类，并用更细的 Kind 说明判定依据。
type ErrorKind string

const (
	KindUnknown ErrorKind = "unknown"

	// KindSchemaIncompatible 不兼容的结构演进（提交阶段拒绝）。
	KindSchemaIncompatible ErrorKind = "schema_incompatible"

	// KindCommitConflict 提交冲突的通用大类。
	KindCommitConflict ErrorKind = "commit_conflict"
	// KindConflictBaseSnapshot 客户端基于的快照已过期（expectedSeq != 当前序号）。
	KindConflictBaseSnapshot ErrorKind = "conflict_base_snapshot"
	// KindConflictConcurrentCommit 无显式基准时，等待表锁期间已有其它提交抢先生效。
	KindConflictConcurrentCommit ErrorKind = "conflict_concurrent_commit"
	// KindConflictFileSetChanged compaction 期间文件集被并发写改变。
	KindConflictFileSetChanged ErrorKind = "conflict_file_set_changed"

	KindSnapshotNotFound  ErrorKind = "snapshot_not_found"
	KindSchemaNotFound    ErrorKind = "schema_not_found"
	KindPrimaryKeyMissing ErrorKind = "primary_key_missing"
	KindRecordValidation  ErrorKind = "record_validation"

	KindCleanupUnauthorized     ErrorKind = "cleanup_unauthorized"
	KindCleanupRequestExpired   ErrorKind = "cleanup_request_expired"
	KindCleanupSnapshotRetained ErrorKind = "cleanup_snapshot_retained"
	KindCleanupSnapshotMissing  ErrorKind = "cleanup_snapshot_missing"
	KindCompactionConflict      ErrorKind = "compaction_conflict"

	KindClosed          ErrorKind = "table_closed"
	KindInvalidArgument ErrorKind = "invalid_argument"
	KindIO              ErrorKind = "io_error"
)

// Error 携带稳定的错误类别与可读判定依据。
type Error struct {
	Kind   ErrorKind
	Reason string
}

func (e *Error) Error() string {
	return fmt.Sprintf("lakehouse: %s: %s", e.Kind, e.Reason)
}

func newError(kind ErrorKind, format string, args ...any) *Error {
	return &Error{Kind: kind, Reason: fmt.Sprintf(format, args...)}
}

// IsKind 判断错误是否属于指定类别。
// 所有细粒度冲突类别同时归属于 KindCommitConflict，便于调用方粗/细两级判定。
func IsKind(err error, kind ErrorKind) bool {
	var e *Error
	if !errors.As(err, &e) {
		return false
	}
	if e.Kind == kind {
		return true
	}
	if kind == KindCommitConflict {
		switch e.Kind {
		case KindConflictBaseSnapshot, KindConflictConcurrentCommit, KindConflictFileSetChanged, KindCompactionConflict:
			return true
		}
	}
	return false
}
