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
	// ErrCleanupRejected 清理请求被整体拒绝的外层归类（以下细分原因都包着它，
	// 老代码用 errors.Is(err, ErrCleanupRejected) 依然成立）。
	ErrCleanupRejected = errors.New("lakehouse: cleanup rejected")
	// ErrCleanupPinned 要删的快照中包含被固定（pin）的快照。
	ErrCleanupPinned = errors.New("lakehouse: cleanup rejected because target snapshot is pinned")
	// ErrCleanupNothing 按当前规则没有任何可清理对象。
	ErrCleanupNothing = errors.New("lakehouse: cleanup rejected because nothing to clean")
	// ErrCleanupOutOfRange 请求越权/超出允许范围（会删掉保留窗口内的最新快照等）。
	ErrCleanupOutOfRange = errors.New("lakehouse: cleanup rejected as out of allowed range")
	// ErrAlreadyPinned 固定一个已经被固定的快照（调用方可据此判断为幂等命中）。
	ErrAlreadyPinned = errors.New("lakehouse: snapshot already pinned")
	// ErrNotPinned 解除固定时该快照并未被固定。
	ErrNotPinned = errors.New("lakehouse: snapshot not pinned")
)
