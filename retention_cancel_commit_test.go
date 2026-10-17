package rollupdb

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// TestAdvanceCanceledAtCommitBoundaryRollsBackAllState checks cancellation
// after promotion, deletion, frontiers and watermark have been staged but
// before SQLite commits the transaction.
func TestAdvanceCanceledAtCommitBoundaryRollsBackAllState(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, true)
	from := fillTwoMinutes(t, db, now, d)
	before := logicalRetentionState(t, db, d)
	*now = from.Add(5 * time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := false
	ctx = context.WithValue(ctx, advanceBeforeCommitKey{}, func() {
		called = true
		cancel()
	})
	result, err := db.Advance(ctx, d.Name)
	if !called {
		t.Fatal("before-commit cancellation hook not reached")
	}
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(result, AdvanceResult{}) {
		t.Fatalf("canceled advance acknowledged: %+v %v", result, err)
	}
	if after := logicalRetentionState(t, db, d); after != before {
		t.Fatal("canceled transaction partially changed buckets, frontiers, groups or watermark")
	}
	retry, err := db.Advance(context.Background(), d.Name)
	if err != nil || retry.SourceBuckets != 120 || retry.ParentBuckets != 2 {
		t.Fatalf("retry after cancellation: %+v %v", retry, err)
	}
	again, err := db.Advance(context.Background(), d.Name)
	if err != nil || again.SourceBuckets != 0 || again.ParentBuckets != 0 {
		t.Fatalf("retry not idempotent: %+v %v", again, err)
	}
}
