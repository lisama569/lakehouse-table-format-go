package lakehouse

// DataType 列类型。
type DataType string

const (
	TypeInt64  DataType = "int64"
	TypeInt32  DataType = "int32"
	TypeString DataType = "string"
	TypeBool   DataType = "bool"
)

// Column 表列。ID 在表生命周期内稳定，重命名只改 Name。
type Column struct {
	ID       int      `json:"id"`
	Name     string   `json:"name"`
	Type     DataType `json:"type"`
	Nullable bool     `json:"nullable"`
}

// Schema 某一快照下的表结构。
type Schema struct {
	Columns    []Column `json:"columns"`
	PrimaryKey []int    `json:"primary_key,omitempty"` // 列 ID，空表示仅追加表
}

// Row 一行数据：列 ID -> 值。
type Row map[int]any

// Snapshot 一次提交产生的不可变快照。
type Snapshot struct {
	Seq       int      `json:"seq"`
	Schema    Schema   `json:"schema"`
	DataFiles []string `json:"data_files"`
	Parent    int      `json:"parent"`         // 父快照序号，首快照为 0
	Kind      string   `json:"kind"`           // append / upsert / compact / schema / rollback
	Keys      []string `json:"keys,omitempty"` // upsert 提交触碰的主键（用于冲突检测）
	// RowCount 该快照的逻辑行数（主键表按合并去重后计），提交时物化进快照，
	// 列历史时直接读元数据即可，无需扫描数据文件。
	RowCount int `json:"row_count"`
	// RollbackTo 仅 Kind==KindRollback 时非 0：本次回滚恢复到的历史快照序号。
	// 回滚本身仍是向后追加的提交，Parent 指向回滚前的最新快照。
	RollbackTo int `json:"rollback_to,omitempty"`
}

// 快照操作类型。
const (
	KindAppend   = "append"
	KindUpsert   = "upsert"
	KindCompact  = "compact"
	KindSchema   = "schema"
	KindRollback = "rollback"
)

// CommitInfo 历史中的一条提交摘要（只来自快照元数据，不含数据扫描）。
type CommitInfo struct {
	Seq        int    // 快照序号
	Parent     int    // 基于哪个历史快照产生（首快照为 0）
	Kind       string // 操作类型
	RowCount   int    // 该版本的逻辑行数（约数，由提交时物化）
	RollbackTo int    // 仅回滚提交非 0：恢复到的历史快照序号
}
