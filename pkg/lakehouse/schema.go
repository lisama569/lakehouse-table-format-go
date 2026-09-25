package lakehouse

import "fmt"

// DataType 表列支持的标量类型。
type DataType string

const (
	TypeInt64   DataType = "int64"
	TypeInt32   DataType = "int32"
	TypeInt16   DataType = "int16"
	TypeInt8    DataType = "int8"
	TypeBoolean DataType = "boolean"
	TypeString  DataType = "string"
)

// FieldID 列的稳定标识；重命名/类型收窄不改 ID，快照读取按 ID 绑定列。
type FieldID int

// Column 描述一列：稳定 ID + 当前名字 + 当前类型 + 是否可空。
type Column struct {
	ID       FieldID  `json:"id"`
	Name     string   `json:"name"`
	Type     DataType `json:"type"`
	Nullable bool     `json:"nullable"`
}

// Schema 某一历史版本的表结构。
type Schema struct {
	ID          int      `json:"id"`
	Columns     []Column `json:"columns"`
	PrimaryKeys []string `json:"primary_keys"` // 以列名引用；rename 时同步改写
}

// EvolutionOp 是结构演进操作类型。
type EvolutionOp string

const (
	EvolutionAddColumn EvolutionOp = "add_column"
	EvolutionRename    EvolutionOp = "rename"
	EvolutionNarrow    EvolutionOp = "narrow_type"
)

// EvolutionSpec 描述一次结构演进操作。
type EvolutionSpec struct {
	Op       EvolutionOp `json:"op"`
	Column   string      `json:"column,omitempty"`   // 目标列当前名字（rename 时为旧名）
	NewName  string      `json:"new_name,omitempty"` // rename 的新名字
	NewType  DataType    `json:"new_type,omitempty"` // narrow_type 的目标类型
	Type     DataType    `json:"type,omitempty"`     // add-column 的列类型
	Nullable bool        `json:"nullable,omitempty"` // add-column 是否可空（新增列必须为 true）
}

// Record 是逻辑行：列名 -> 值。整型统一为 int64，布尔 bool，字符串 string；缺省/NULL 用 nil 表示。
type Record map[string]any

func (s *Schema) columnByName(name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

func (s *Schema) columnByID(id FieldID) (Column, bool) {
	for _, c := range s.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return Column{}, false
}

func (s *Schema) indexByName() map[string]Column {
	m := make(map[string]Column, len(s.Columns))
	for _, c := range s.Columns {
		m[c.Name] = c
	}
	return m
}

func (s *Schema) findIDByName(name string) (FieldID, bool) {
	c, ok := s.columnByName(name)
	if !ok {
		return 0, false
	}
	return c.ID, true
}

func (s *Schema) copyShallow() *Schema {
	cp := &Schema{
		ID:          s.ID,
		Columns:     append([]Column(nil), s.Columns...),
		PrimaryKeys: append([]string(nil), s.PrimaryKeys...),
	}
	return cp
}

func validDataType(t DataType) bool {
	switch t {
	case TypeInt64, TypeInt32, TypeInt16, TypeInt8, TypeBoolean, TypeString:
		return true
	}
	return false
}

// narrowable 判定 old->new 是否为允许的"类型收窄"。
// 只允许有符号整型沿 int64 > int32 > int16 > int8 单向收窄；
// string/boolean 不参与收窄，禁止放宽或跨类型转换。
func narrowable(oldType, newType DataType) bool {
	rank := map[DataType]int{
		TypeInt64: 4, TypeInt32: 3, TypeInt16: 2, TypeInt8: 1,
	}
	o, ok1 := rank[oldType]
	n, ok2 := rank[newType]
	return ok1 && ok2 && n < o
}

// fitsType 判定整数值是否落在目标整型的取值域内。
func fitsType(v int64, t DataType) bool {
	switch t {
	case TypeInt64:
		return true
	case TypeInt32:
		return v >= -(1<<31) && v <= (1<<31)-1
	case TypeInt16:
		return v >= -(1<<15) && v <= (1<<15)-1
	case TypeInt8:
		return v >= -(1<<7) && v <= (1<<7)-1
	}
	return false
}

// validateEvolution 在提交阶段判定演进是否兼容；不兼容返回 KindSchemaIncompatible。
// 返回应用演进后的 schema 副本（Schema.ID 由提交方分配）。
func validateEvolution(base *Schema, spec EvolutionSpec) (*Schema, error) {
	next := base.copyShallow()

	switch spec.Op {
	case EvolutionAddColumn:
		if spec.Column == "" {
			return nil, newError(KindSchemaIncompatible, "add_column: 列名不能为空")
		}
		if !validDataType(spec.Type) {
			return nil, newError(KindSchemaIncompatible, "add_column: 不支持的类型 %q", spec.Type)
		}
		if _, ok := next.columnByName(spec.Column); ok {
			return nil, newError(KindSchemaIncompatible, "add_column: 列 %q 已存在", spec.Column)
		}
		// 新增列必须可空：历史行无法为非空新列提供值。
		if !spec.Nullable {
			return nil, newError(KindSchemaIncompatible,
				"add_column: 新增列 %q 必须为 nullable（历史快照行无默认值）", spec.Column)
		}
		maxID := FieldID(0)
		for _, c := range next.Columns {
			if c.ID > maxID {
				maxID = c.ID
			}
		}
		next.Columns = append(next.Columns, Column{
			ID: maxID + 1, Name: spec.Column, Type: spec.Type, Nullable: true,
		})
		return next, nil

	case EvolutionRename:
		if spec.NewName == "" {
			return nil, newError(KindSchemaIncompatible, "rename: 新列名不能为空")
		}
		c, ok := next.columnByName(spec.Column)
		if !ok {
			return nil, newError(KindSchemaIncompatible, "rename: 列 %q 不存在", spec.Column)
		}
		if _, clash := next.columnByName(spec.NewName); clash {
			return nil, newError(KindSchemaIncompatible, "rename: 列名 %q 已被占用", spec.NewName)
		}
		for i := range next.Columns {
			if next.Columns[i].ID == c.ID {
				next.Columns[i].Name = spec.NewName
			}
		}
		// 主键按列名引用，同步改写，保证历史主键语义不变。
		for i, n := range next.PrimaryKeys {
			if n == spec.Column {
				next.PrimaryKeys[i] = spec.NewName
			}
		}
		return next, nil

	case EvolutionNarrow:
		c, ok := next.columnByName(spec.Column)
		if !ok {
			return nil, newError(KindSchemaIncompatible, "narrow_type: 列 %q 不存在", spec.Column)
		}
		if c.Type == spec.NewType {
			return nil, newError(KindSchemaIncompatible,
				"narrow_type: 列 %q 已经是 %s（空操作）", spec.Column, spec.NewType)
		}
		if !narrowable(c.Type, spec.NewType) {
			return nil, newError(KindSchemaIncompatible,
				"narrow_type: 不允许将列 %q 从 %s 变为 %s（只允许 int64>int32>int16>int8 单向收窄）",
				spec.Column, c.Type, spec.NewType)
		}
		for i := range next.Columns {
			if next.Columns[i].ID == c.ID {
				next.Columns[i].Type = spec.NewType
			}
		}
		return next, nil

	default:
		return nil, newError(KindSchemaIncompatible, "未知演进操作 %q", spec.Op)
	}
}

// validateRecord 按当前 schema 校验一行（类型、可空、主键）。
func validateRecord(s *Schema, r Record, requirePK bool) error {
	idx := s.indexByName()

	for name, v := range r {
		c, ok := idx[name]
		if !ok {
			return newError(KindRecordValidation, "记录包含未知列 %q", name)
		}
		if v == nil {
			if !c.Nullable {
				return newError(KindRecordValidation, "非空列 %q 不允许为 NULL", name)
			}
			continue
		}
		switch c.Type {
		case TypeInt64, TypeInt32, TypeInt16, TypeInt8:
			n, ok := v.(int64)
			if !ok {
				return newError(KindRecordValidation, "列 %q 期望整型(int64)，实际 %T", name, v)
			}
			if !fitsType(n, c.Type) {
				return newError(KindRecordValidation, "值 %d 超出列 %q 类型 %s 的取值域", n, name, c.Type)
			}
		case TypeBoolean:
			if _, ok := v.(bool); !ok {
				return newError(KindRecordValidation, "列 %q 期望 boolean，实际 %T", name, v)
			}
		case TypeString:
			if _, ok := v.(string); !ok {
				return newError(KindRecordValidation, "列 %q 期望 string，实际 %T", name, v)
			}
		default:
			return newError(KindRecordValidation, "列 %q 具有未知类型 %s", name, c.Type)
		}
	}

	if requirePK {
		if len(s.PrimaryKeys) == 0 {
			return newError(KindPrimaryKeyMissing, "表未定义主键，无法执行 upsert")
		}
		for _, pk := range s.PrimaryKeys {
			c, ok := idx[pk]
			if !ok {
				return newError(KindPrimaryKeyMissing, "主键列 %q 不在当前 schema 中", pk)
			}
			v, present := r[pk]
			if !present || v == nil {
				return newError(KindPrimaryKeyMissing, "主键列 %q 缺失或为 NULL", pk)
			}
			if c.Type == TypeString {
				if s, _ := v.(string); s == "" {
					return newError(KindPrimaryKeyMissing, "主键列 %q 不允许为空串", pk)
				}
			}
		}
	}
	return nil
}

// pkKey 把主键值组合成稳定的去重键。
func pkKey(s *Schema, r Record) string {
	key := ""
	for _, pk := range s.PrimaryKeys {
		key += fmt.Sprintf("%v\x00", r[pk])
	}
	return key
}
