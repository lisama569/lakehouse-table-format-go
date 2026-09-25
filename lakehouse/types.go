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
	Kind      string   `json:"kind"`           // append / upsert / compact / schema
	Keys      []string `json:"keys,omitempty"` // upsert 提交触碰的主键（用于冲突检测）
}
