package transport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rollupdb/rollupdb"
)

func fixture(t *testing.T) (*rollupdb.DB, rollupdb.Definition, time.Time) {
	t.Helper()
	now := time.Date(2026, 10, 17, 12, 2, 0, 0, time.UTC)
	db, err := rollupdb.Create(context.Background(), filepath.Join(t.TempDir(), "transport.rollupdb"), rollupdb.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	d := rollupdb.Definition{Name: "api", Version: 1, EffectiveAt: now.Truncate(24 * time.Hour), Dimensions: []string{"service"}, Statistics: []string{"count", "sum", "avg"}, MaxGroups: 2, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 120, GraceSeconds: 120}, {WidthSeconds: 60, RetainSeconds: 0, GraceSeconds: 120}}}
	if err = db.CreateStream(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	return db, d, now
}
func record(t *testing.T, at time.Time, value float64) string {
	t.Helper()
	encoded, err := json.Marshal(rollupdb.Observation{Time: at, Value: value, Dimensions: map[string]string{"service": "web"}})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
func status(t *testing.T, db *rollupdb.DB, name string) rollupdb.StreamStatus {
	t.Helper()
	s, err := db.Status(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestNDJSONBatchesRejectionsAndFinalUnterminatedLine(t *testing.T) {
	db, d, now := fixture(t)
	input := record(t, now.Add(-time.Second), 1) + "\r\n" + record(t, now.Add(-121*time.Second), 2) + "\n" + record(t, now.Add(time.Second), 3)
	var receipts []BatchReceipt
	summary, err := ImportNDJSON(context.Background(), db, d.Name, strings.NewReader(input), 2, func(r BatchReceipt) error { receipts = append(receipts, r); return nil })
	if err != nil || summary.Type != "complete" || summary.CommittedRecords != 3 || summary.CommittedBatches != 2 || summary.Accepted != 1 || summary.RejectedLate != 1 || summary.RejectedInvalid != 1 || summary.NextLine != 4 {
		t.Fatalf("summary: %+v %v", summary, err)
	}
	if len(receipts) != 2 || receipts[0].FromLine != 1 || receipts[0].ThroughLine != 2 || receipts[1].FromLine != 3 || receipts[1].ThroughLine != 3 || len(receipts[0].Result.Outcomes) != 2 {
		t.Fatalf("receipts: %+v", receipts)
	}
	if status(t, db, d.Name).Accepted != 1 {
		t.Fatal("transport changed engine outcomes")
	}
}

func TestNDJSONMalformedDiscardsPendingButPreservesPreviousCommit(t *testing.T) {
	for _, bad := range []string{"", "null", `{"time":"2026-10-17T12:02:00Z"}`, record(t, time.Date(2026, 10, 17, 12, 2, 0, 0, time.UTC), 3) + ` {}`, `{"unknown":1}`, string([]byte{'{', '"', 'x', '"', ':', '"', 255, '"', '}'})} {
		t.Run(bad, func(t *testing.T) {
			db, d, now := fixture(t)
			good := record(t, now, 1)
			input := good + "\n" + good + "\n" + good + "\n" + bad + "\n"
			summary, err := ImportNDJSON(context.Background(), db, d.Name, strings.NewReader(input), 2, nil)
			if err == nil || summary.Type != "error" || summary.CommittedRecords != 2 || summary.NextLine != 3 || summary.FailedLine != 4 || summary.DiscardedRecords != 1 || summary.UnacknowledgedRecords != 0 {
				t.Fatalf("partial progress: %+v %v", summary, err)
			}
			if status(t, db, d.Name).Accepted != 2 {
				t.Fatal("pending observations were submitted")
			}
		})
	}
}

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func TestNDJSONReadErrorAndRecordBounds(t *testing.T) {
	db, d, now := fixture(t)
	good := record(t, now, 1)
	r := io.MultiReader(strings.NewReader(good+"\n"), failedReader{})
	summary, err := ImportNDJSON(context.Background(), db, d.Name, r, 1000, nil)
	if !errors.Is(err, io.ErrUnexpectedEOF) || summary.DiscardedRecords != 1 || summary.CommittedRecords != 0 {
		t.Fatalf("reader failure: %+v %v", summary, err)
	}
	boundary := good + strings.Repeat(" ", MaxRecordBytes-len(good))
	summary, err = ImportNDJSON(context.Background(), db, d.Name, strings.NewReader(boundary+"\r\n"), 1, nil)
	if err != nil || summary.Accepted != 1 {
		t.Fatalf("maximum legal record: %+v %v", summary, err)
	}
	summary, err = ImportNDJSON(context.Background(), db, d.Name, strings.NewReader(boundary+" \n"), 1, nil)
	if err == nil || summary.FailedLine != 1 || summary.CommittedRecords != 0 {
		t.Fatalf("oversize: %+v %v", summary, err)
	}
	if status(t, db, d.Name).Accepted != 1 {
		t.Fatal("oversized line mutated state")
	}
}

func TestNDJSONByteCapacityFlushesBeforeCountLimit(t *testing.T) {
	db, d, now := fixture(t)
	encoded, _ := json.Marshal(rollupdb.Observation{Time: now, Value: 1, Dimensions: map[string]string{"service": strings.Repeat("x", 120<<10)}})
	var receipts []BatchReceipt
	summary, err := ImportNDJSON(context.Background(), db, d.Name, strings.NewReader(strings.Repeat(string(encoded)+"\n", 80)), 1000, func(r BatchReceipt) error { receipts = append(receipts, r); return nil })
	if err != nil || summary.CommittedRecords != 80 || summary.RejectedInvalid != 80 || len(receipts) != 2 {
		t.Fatalf("byte cap: %+v receipts=%d %v", summary, len(receipts), err)
	}
	if receipts[0].ThroughLine >= 80 || receipts[1].FromLine != receipts[0].ThroughLine+1 {
		t.Fatal("noncontiguous byte-bounded batches")
	}
}

func TestNDJSONReceiptFailureCancellationAndEngineFailure(t *testing.T) {
	db, d, now := fixture(t)
	good := record(t, now, 1) + "\n"
	sentinel := errors.New("output failed")
	summary, err := ImportNDJSON(context.Background(), db, d.Name, strings.NewReader(strings.Repeat(good, 3)), 1, func(BatchReceipt) error { return sentinel })
	if !errors.Is(err, sentinel) || summary.CommittedRecords != 1 || summary.NextLine != 2 || status(t, db, d.Name).Accepted != 1 {
		t.Fatalf("receipt failure hid commit: %+v %v", summary, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	summary, err = ImportNDJSON(ctx, db, d.Name, strings.NewReader(strings.Repeat(good, 3)), 1, func(BatchReceipt) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) || summary.CommittedRecords != 1 || status(t, db, d.Name).Accepted != 2 {
		t.Fatalf("cancel: %+v %v", summary, err)
	}
	summary, err = ImportNDJSON(context.Background(), db, d.Name, strings.NewReader(strings.Repeat(good, 3)), 1, func(BatchReceipt) error { db.Close(); return nil })
	if !errors.Is(err, rollupdb.ErrClosed) || summary.CommittedRecords != 1 || summary.UnacknowledgedRecords != 1 || summary.NextLine != 2 {
		t.Fatalf("unacknowledged batch: %+v %v", summary, err)
	}
}
