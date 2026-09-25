package lakehouse

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// reader.go: 快照读取。
//
// 读取语义：
//   - 任何读取都基于某个已提交快照的完整文件清单，不会看到部分提交；
//   - 行内按列 ID 寻址，因此列重命名不影响历史数据解读；
//   - 后写文件覆盖先写文件中的同主键行（upsert 语义），不重复、不丢失；
//   - 读取结果按快照 schema 投影：新增列补 null，收窄列返回值已验证可表示。

// Scan 读取指定快照（seq<=0 表示最新）的全部行。
func (t *Table) Scan(seq int) ([]Row, error) {
	if seq <= 0 {
		seq = t.CurrentSeq()
	}
	snap, err := t.LoadSnapshot(seq)
	if err != nil {
		return nil, err
	}
	return t.scanSnapshot(snap)
}

// scanSnapshot 按快照清单合并所有数据文件。
func (t *Table) scanSnapshot(snap *Snapshot) ([]Row, error) {
	pk := snap.Schema.PrimaryKey
	merged := map[string]Row{} // 主键 -> 行（后者覆盖前者）
	var order []string         // 保持出现顺序，输出可预期
	var plain []Row            // 无主键表：直接按序追加

	for _, rel := range snap.DataFiles {
		if err := t.readDataFile(rel, func(r Row) {
			if len(pk) == 0 {
				plain = append(plain, r)
				return
			}
			k := rowKey(pk, r)
			if _, ok := merged[k]; !ok {
				order = append(order, k)
			}
			merged[k] = r
		}); err != nil {
			return nil, err
		}
	}

	var out []Row
	out = append(out, plain...)
	for _, k := range order {
		out = append(out, merged[k])
	}
	// 按快照 schema 投影：保证新增列以 null 出现、只暴露当前 schema 的列
	for _, r := range out {
		projectRow(snap.Schema, r)
	}
	return out, nil
}

// readDataFile 逐行读取数据文件，回调每一行（列 ID -> 值）。
func (t *Table) readDataFile(rel string, fn func(Row)) error {
	f, err := os.Open(filepath.Join(t.dir, rel))
	if err != nil {
		return fmt.Errorf("lakehouse: read data file %s: %w", rel, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			return fmt.Errorf("lakehouse: decode %s: %w", rel, err)
		}
		row := Row{}
		for k, v := range m {
			id, err := strconv.Atoi(k)
			if err != nil {
				return fmt.Errorf("lakehouse: bad column id %q in %s", k, rel)
			}
			row[id] = v
		}
		fn(row)
	}
	return sc.Err()
}

// projectRow 按 schema 投影行：仅保留 schema 内列，缺失列补 null。
func projectRow(schema Schema, r Row) {
	for id := range r {
		if _, ok := findByID(schema, id); !ok {
			delete(r, id)
		}
	}
	for _, c := range schema.Columns {
		if _, ok := r[c.ID]; !ok {
			r[c.ID] = nil
		}
	}
}

func rowKey(pk []int, r Row) string {
	k, err := primaryKeysOf(Schema{PrimaryKey: pk}, []Row{r})
	if err != nil || len(k) == 0 {
		// 数据文件由本库写入，正常不会缺主键；兜底避免 panic
		return fmt.Sprintf("__missing__%p", r)
	}
	return k[0]
}
