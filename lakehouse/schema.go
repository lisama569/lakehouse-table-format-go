package lakehouse

import "fmt"

// schema.go: 结构演进的兼容规则（提交时校验，读取阶段不再报错）。
//
// 允许的演进：
//   - 新增列：必须可空（历史数据读取时补 null）；
//   - 类型收窄：仅 int64 -> int32，且提交时扫描存量数据验证可收窄；
//   - 列重命名：按列 ID 匹配，新名字不得与其他列冲突。
//
// 拒绝的演进（ErrSchemaIncompatible）：
//   - 删除列、修改主键、新增非空列；
//   - 除 int64->int32 外的任何类型变更；
//   - 重命名与现存列名冲突、列 ID 复用为不同列。

// validateEvolution 校验 old -> new 是否兼容。返回 nil 表示允许提交。
func validateEvolution(old, new Schema) error {
	if err := validateSchema(new); err != nil {
		return err
	}
	// 主键不可变更
	if !intSliceEqual(old.PrimaryKey, new.PrimaryKey) {
		return fmt.Errorf("%w: primary key cannot be changed", ErrSchemaIncompatible)
	}
	oldByID := map[int]Column{}
	oldNames := map[string]bool{}
	for _, c := range old.Columns {
		oldByID[c.ID] = c
		oldNames[c.Name] = true
	}
	// 旧列必须全部保留（禁止删列），且类型变更只允许收窄
	for _, oc := range old.Columns {
		nc, ok := findByID(new, oc.ID)
		if !ok {
			return fmt.Errorf("%w: column %q (id=%d) dropped", ErrSchemaIncompatible, oc.Name, oc.ID)
		}
		if nc.Type != oc.Type && !narrowingOK(oc.Type, nc.Type) {
			return fmt.Errorf("%w: column %q type %s -> %s not allowed",
				ErrSchemaIncompatible, oc.Name, oc.Type, nc.Type)
		}
	}
	// 新列：要么是新增（必须可空），要么按 ID 对应旧列（重命名需查撞名）
	for _, nc := range new.Columns {
		oc, existed := oldByID[nc.ID]
		if !existed {
			if !nc.Nullable {
				return fmt.Errorf("%w: new column %q must be nullable", ErrSchemaIncompatible, nc.Name)
			}
			if oldNames[nc.Name] {
				return fmt.Errorf("%w: new column reuses existing name %q", ErrSchemaIncompatible, nc.Name)
			}
			continue
		}
		if nc.Name != oc.Name && nameTakenByOther(new, nc.Name, nc.ID) {
			return fmt.Errorf("%w: rename to %q conflicts with existing column", ErrSchemaIncompatible, nc.Name)
		}
		_ = oc
	}
	return nil
}

// narrowingOK 允许的类型收窄：int64 -> int32。
func narrowingOK(from, to DataType) bool {
	return from == TypeInt64 && to == TypeInt32
}

// needsDataValidation 返回演进中需要扫描存量数据验证的列（收窄列）。
func needsDataValidation(old, new Schema) []Column {
	var cols []Column
	for _, nc := range new.Columns {
		if oc, ok := findByID(old, nc.ID); ok && oc.Type != nc.Type {
			cols = append(cols, nc)
		}
	}
	return cols
}

// validateNarrowingValue 校验单个值在收窄后的类型下是否可表示。
func validateNarrowingValue(col Column, v any) error {
	if v == nil {
		return nil
	}
	if col.Type == TypeInt32 {
		n, ok := toInt64(v)
		if !ok {
			return fmt.Errorf("%w: column %q value %v is not an integer", ErrSchemaIncompatible, col.Name, v)
		}
		if n < -1<<31 || n > 1<<31-1 {
			return fmt.Errorf("%w: column %q value %d overflows int32", ErrSchemaIncompatible, col.Name, n)
		}
	}
	return nil
}

func findByID(s Schema, id int) (Column, bool) {
	for _, c := range s.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return Column{}, false
}

func nameTakenByOther(s Schema, name string, selfID int) bool {
	for _, c := range s.Columns {
		if c.Name == name && c.ID != selfID {
			return true
		}
	}
	return false
}

func intSliceEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64: // JSON 反序列化产物
		if n == float64(int64(n)) {
			return int64(n), true
		}
	}
	return 0, false
}
