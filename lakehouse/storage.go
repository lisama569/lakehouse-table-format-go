package lakehouse

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// storage.go: 本地文件系统存储层。
// 目录布局：
//   <dir>/data/part-*.jsonl      数据文件（JSON Lines，按列 ID 存值）
//   <dir>/snapshots/NNNNNN.json  快照清单（排他创建，作为提交点）

func snapshotPath(dir string, seq int) string {
	return filepath.Join(dir, "snapshots", fmt.Sprintf("%06d.json", seq))
}

// writeJSONAtomic 写临时文件后 rename，保证原子可见。
func writeJSONAtomic(path string, v any) error {
	tmp := path + ".tmp"
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// readJSON 读取 JSON 文件。
func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// createExclusive 以 O_EXCL 方式创建文件并写入内容；已存在则返回错误。
// 这是提交协议在文件系统层面的互斥点：两个并发提交只有一个能创建
// 下一个序号的快照文件。
func createExclusive(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	return f.Close()
}
