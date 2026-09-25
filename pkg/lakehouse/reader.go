package lakehouse

import (
	"context"
)

// SnapshotReader 对某个不可变快照做一致性读取。
// 绑定快照时刻的 schema 版本；物理列按稳定 FieldID 投影到该 schema：
//   - 重命名：对读取透明（同 ID 映射到新名）；
//   - 新增列：历史行该列为 nil；
//   - 类型收窄：提交阶段已保证既有数据落在新值域内，按 int64 原值返回。
type SnapshotReader struct {
	table  *Table
	meta   *TableMetadata
	snap   *Snapshot
	schema *Schema
	files  []DataFile
}

// SnapshotID 返回该读取绑定的快照标识。
func (r *SnapshotReader) SnapshotID() string { return r.snap.ID }

// Seq 返回该读取绑定的提交序号。
func (r *SnapshotReader) Seq() int64 { return r.snap.Seq }

// SchemaID 返回该读取绑定的 schema 版本。
func (r *SnapshotReader) SchemaID() int { return r.schema.ID }

// projectRecord 按稳定 FieldID 将"文件写入时 schema"下的物理行
// 投影到"快照读取 schema"下的逻辑行。
func projectRecord(readSchema, fileSchema *Schema, physical Record) Record {
	out := Record{}
	for _, col := range readSchema.Columns {
		if fileCol, ok := fileSchema.columnByID(col.ID); ok {
			// 文件由含该列 ID 的 schema 写入：用文件中的列名取值。
			out[col.Name] = physical[fileCol.Name]
		} else {
			// 该列在文件写入之后才新增：历史行为 NULL。
			out[col.Name] = nil
		}
	}
	return out
}

// ReadAll 物化该快照下全部记录。
func (r *SnapshotReader) ReadAll(ctx context.Context) ([]Record, error) {
	var out []Record
	err := r.Stream(ctx, func(rec Record) error {
		out = append(out, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Stream 流式读取，逐条回调；ctx 取消时中止。
// 数据文件均不可变，且读取全程不持提交锁：compaction/清理受保留集保护，
// 被读取快照引用的文件不会被删除。
func (r *SnapshotReader) Stream(ctx context.Context, fn func(Record) error) error {
	for _, f := range r.files {
		if err := ctx.Err(); err != nil {
			return err
		}
		fileSchema, ok := r.meta.schemaByID(f.SchemaID)
		if !ok {
			return newError(KindSchemaNotFound, "数据文件 %s 引用的 schema v%d 不存在", f.Path, f.SchemaID)
		}
		rc, err := r.table.fs.openRead(f.Path)
		if err != nil {
			return err
		}
		_, err = readJSONL(rc, func(rec Record) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fn(projectRecord(r.schema, fileSchema, rec))
		})
		_ = rc.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
