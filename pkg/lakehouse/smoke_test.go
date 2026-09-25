package lakehouse

import (
	"context"
	"testing"
)

func TestSmokeAppendRead(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	tb, err := Create(ctx, dir, "t1", []ColumnSpec{
		{Name: "id", Type: TypeInt64},
		{Name: "name", Type: TypeString, Nullable: true},
	}, []string{"id"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := tb.CommitAppend(ctx, CommitRequest{
		Records: []Record{{"id": int64(1), "name": "a"}, {"id": int64(2), "name": "b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	r, err := tb.Snapshot(ctx, res.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.ReadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %#v", len(rows), rows)
	}
	t.Logf("snapshot=%s seq=%d rows=%v", res.SnapshotID, res.Seq, rows)
}
