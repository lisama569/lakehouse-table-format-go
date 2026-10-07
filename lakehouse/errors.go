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
	// ErrCleanupRejected 清理请求被拒绝的总类别：越权、无对象或触碰固定快照。
	// 更具体的原因可用 ErrCleanupNothing / ErrCleanupPinned 进一步区分。
	ErrCleanupRejected = errors.New("lakehouse: cleanup rejected")
	// ErrCleanupNothing 按当前规则没有任何可清理的对象（请求过期）。
	// 返回的错误同时包裹 ErrCleanupRejected。
	ErrCleanupNothing = errors.New("lakehouse: cleanup has nothing to do")
	// ErrCleanupPinned 待清理范围内包含被固定的快照，整体拒绝。
	// 返回的错误同时包裹 ErrCleanupRejected。
	ErrCleanupPinned = errors.New("lakehouse: cleanup blocked by pinned snapshot")
	// ErrSnapshotNotPinned 解除固定时该快照并未被固定。
	ErrSnapshotNotPinned = errors.New("lakehouse: snapshot not pinned")
)
