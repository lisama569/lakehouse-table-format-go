package lakehouse

import "errors"

// 错误类别：调用方可用 errors.Is 区分冲突原因。
var (
	// ErrConflictConcurrentCommit 提交期间基线快照已被其他提交推进。
	ErrConflictConcurrentCommit = errors.New("lakehouse: concurrent commit conflict")
	// ErrConflictPrimaryKey upsert 的主键与并发提交修改的主键重叠。
	ErrConflictPrimaryKey = errors.New("lakehouse: primary key conflict")
	// ErrSchemaIncompatible 结构演进不兼容，提交时被拒绝。
	ErrSchemaIncompatible = errors.New("lakehouse: incompatible schema evolution")
	// ErrSnapshotNotFound 请求读取的快照不存在或已被清理。
	ErrSnapshotNotFound = errors.New("lakehouse: snapshot not found")
	// ErrCleanupRejected 清理请求过期或越权（会删除仍被保留/引用的快照）。
	ErrCleanupRejected = errors.New("lakehouse: cleanup rejected")
)
