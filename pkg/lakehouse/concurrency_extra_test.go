package lakehouse

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestConcurrentReadDuringCompaction: 多轮 append/compact 交错进行时，
// 已打开的快照读取必须稳定（文件不可变 + 快照自包含清单），不丢不重。
func TestConcurrentReadDuringCompaction(t *testing.T) {
	tb, ctx, _ := testTable(t, true)
	const rounds = 10
	const perFile = 50

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 持续读取某个已存在快照，直到 stop。
	var readerErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		id := currentID(t, tb, ctx)
		for {
			select {
			case <-stop:
				return
			default:
			}
			r, err := tb.Snapshot(ctx, id)
			if err != nil {
				readerErr = err
				return
			}
			n, err := r.ReadAll(ctx)
			if err != nil {
				readerErr = err
				return
			}
			// 初始空快照恒为 0 行；换成非空锚点更有意义，见下。
			_ = n
		}
	}()

	// 制造多个小文件后，在锚点快照上持续读取。
	for i := 0; i < 3; i++ {
		recs := make([]Record, perFile)
		for j := 0; j < perFile; j++ {
			recs[j] = Record{"id": int64(i*perFile + j + 1)}
		}
		if _, err := tb.CommitAppend(ctx, CommitRequest{Records: recs}); err != nil {
			t.Fatal(err)
		}
	}
	anchorID := currentID(t, tb, ctx)
	anchorRows := int64(3 * perFile)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			r, err := tb.Snapshot(ctx, anchorID)
			if err != nil {
				readerErr = err
				return
			}
			rows, err := r.ReadAll(ctx)
			if err != nil {
				readerErr = err
				return
			}
			if int64(len(rows)) != anchorRows {
				readerErr = &Error{Kind: KindUnknown,
					Reason: "compaction/清理交错期间锚点快照行数变化"}
				return
			}
		}
	}()

	// 交替追加与合并。
	for i := 0; i < rounds; i++ {
		recs := []Record{{"id": int64(10000 + i)}}
		if _, err := tb.CommitAppend(ctx, CommitRequest{Records: recs}); err != nil {
			t.Fatal(err)
		}
		if _, err := tb.Compact(ctx, CompactionRequest{Reason: "interleaved"}); err != nil {
			// 交错下 compact 冲突是允许的明确拒绝，不算错误。
			if !IsKind(err, KindCommitConflict) {
				t.Fatalf("非预期 compact 错误: %v", err)
			}
		}
	}
	close(stop)
	wg.Wait()
	if readerErr != nil {
		t.Fatalf("并发读取期间出现不一致: %v", readerErr)
	}

	// 清理旧快照（保留 current），锚点快照过期后读应明确报 not_found，
	// 但 current 数据始终完好。
	before := mustRead(t, tb, ctx, currentID(t, tb, ctx))
	if _, err := tb.ExpireSnapshots(context.Background(), ExpireRequest{
		Token: "admin-secret", RequestTime: timeNow(),
	}); err != nil {
		t.Fatal(err)
	}
	after := mustRead(t, tb, ctx, currentID(t, tb, ctx))
	if len(after) != len(before) {
		t.Fatalf("清理后 current 行数变化: %d vs %d", len(before), len(after))
	}
	if _, err := tb.Snapshot(ctx, anchorID); !IsKind(err, KindSnapshotNotFound) {
		t.Fatalf("锚点已过期，应返回 snapshot_not_found，实际 %v", err)
	}
	t.Logf("交错 %d 轮 append/compact 期间读取稳定；清理后 current=%d 行", rounds, len(after))
}

func timeNow() time.Time { return time.Now() }
