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
	Seq        int      `json:"seq"`
	Schema     Schema   `json:"schema"`
	DataFiles  []string `json:"data_files"`
	Parent     int      `json:"parent"`               // 父快照序号，首快照为 0
	Kind       string   `json:"kind"`                 // append / upsert / compact / schema / rollback
	Keys       []string `json:"keys,omitempty"`       // upsert 提交触碰的主键（用于冲突检测）
	Rows       int      `json:"rows"`                 // 本次提交新增/合并出的行数（历史展示用，约为量级）
	RollbackTo int      `json:"rollback_to,omitempty"` // rollback 提交：内容来源的历史快照序号
}

// HistoryEntry 历史清单中的一次提交记录（仅由快照元数据构成，不读数据文件）。
type HistoryEntry struct {
	Seq        int    // 快照序号
	Kind       string // 操作类别：append / upsert / compact / schema / rollback
	Parent     int    // 基于哪个历史版本产生
	Rows       int    // 本次提交涉及的行数（约值）
	RollbackTo int    // 仅 rollback：回滚到的目标快照序号，否则为 0
}
