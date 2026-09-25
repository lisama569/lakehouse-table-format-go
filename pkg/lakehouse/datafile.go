package lakehouse

import (
	"bytes"
	"encoding/json"
	"io"
)

// normalizeJSONValue 把 encoding/json 解出的值归一化为内部逻辑类型：
// json.Number 若为整数则转 int64；map 深度归一化。
func normalizeJSONValue(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		if f, err := x.Float64(); err == nil {
			return f
		}
		return x.String()
	case map[string]any:
		for k, vv := range x {
			x[k] = normalizeJSONValue(vv)
		}
		return x
	default:
		return v
	}
}

// writeJSONL 将记录编码为 JSON Lines（每条一行），返回写入条数。
func writeJSONL(w io.Writer, records []Record) (int64, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	var n int64
	for _, r := range records {
		buf.Reset()
		if err := enc.Encode(r); err != nil {
			return n, newError(KindIO, "编码 JSONL 失败: %s", err)
		}
		if _, err := io.Copy(w, &buf); err != nil {
			return n, newError(KindIO, "写 JSONL 失败: %s", err)
		}
		n++
	}
	return n, nil
}

// readJSONL 流式读取 JSON Lines，逐行回调；回调返回错误则中止并透传。
func readJSONL(r io.Reader, fn func(Record) error) (int64, error) {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	var n int64
	for dec.More() {
		var raw map[string]any
		if err := dec.Decode(&raw); err != nil {
			return n, newError(KindIO, "解码 JSONL 失败: %s", err)
		}
		rec := Record{}
		for k, v := range raw {
			rec[k] = normalizeJSONValue(v)
		}
		if err := fn(rec); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
