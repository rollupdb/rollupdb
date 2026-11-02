package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rollupdb/rollupdb"
)

func TestHTTPBatchIDAndIdempotencyKey(t *testing.T) {
	db, d, now := fixture(t)
	h := NewHandler(db)
	batch := rollupdb.Batch{Stream: d.Name, Version: 1, BatchID: "b1", Observations: []rollupdb.Observation{{Time: now.Add(-time.Second), Value: 3, Dimensions: map[string]string{"service": "web"}}}}
	var first, again rollupdb.BatchResult
	w := request(t, h, "POST", "/v1/ingest", batch)
	if err := json.Unmarshal(w.Body.Bytes(), &first); err != nil || w.Code != 200 || first.Replayed || first.Accepted != 1 {
		t.Fatalf("first: %d %s", w.Code, w.Body.String())
	}
	w = request(t, h, "POST", "/v1/ingest", batch)
	if err := json.Unmarshal(w.Body.Bytes(), &again); err != nil || w.Code != 200 || !again.Replayed || again.Accepted != 1 {
		t.Fatalf("retry: %d %s", w.Code, w.Body.String())
	}
	changed := batch
	changed.Observations = []rollupdb.Observation{{Time: now.Add(-time.Second), Value: 4, Dimensions: map[string]string{"service": "web"}}}
	w = request(t, h, "POST", "/v1/ingest", changed)
	if w.Code != 409 || !strings.Contains(w.Body.String(), `"batch_conflict"`) {
		t.Fatalf("conflict: %d %s", w.Code, w.Body.String())
	}

	post := func(key string, b rollupdb.Batch) *httptest.ResponseRecorder {
		encoded, _ := json.Marshal(b)
		r := httptest.NewRequest("POST", "http://127.0.0.1/v1/ingest", bytes.NewReader(encoded))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	keyed := batch
	keyed.BatchID = ""
	if w = post("k1", keyed); w.Code != 200 || strings.Contains(w.Body.String(), `"replayed"`) {
		t.Fatalf("header first: %d %s", w.Code, w.Body.String())
	}
	if w = post("k1", keyed); w.Code != 200 || !strings.Contains(w.Body.String(), `"replayed":true`) {
		t.Fatalf("header retry: %d %s", w.Code, w.Body.String())
	}
	// The same key in both places is fine; different ones are refused.
	same := keyed
	same.BatchID = "k1"
	if w = post("k1", same); w.Code != 200 {
		t.Fatalf("header and body agree: %d %s", w.Code, w.Body.String())
	}
	if w = post("k2", batch); w.Code != 400 {
		t.Fatalf("header and body differ: %d %s", w.Code, w.Body.String())
	}
	if w = post("bad key", keyed); w.Code != 400 {
		t.Fatalf("invalid key: %d %s", w.Code, w.Body.String())
	}
	if s := status(t, db, d.Name); s.Accepted != 2 {
		t.Fatalf("accepted %d, want 2", s.Accepted)
	}
}

// Rerunning the same input with the same prefix skips what committed, so an
// import interrupted half way can simply be started again.
func TestNDJSONRerunWithPrefixSkipsCommittedBatches(t *testing.T) {
	db, d, now := fixture(t)
	var lines []string
	for i := 0; i < 5; i++ {
		lines = append(lines, record(t, now.Add(-time.Duration(10-i)*time.Second), float64(i)))
	}
	input := strings.Join(lines, "\n") + "\n"
	// First run gets through four lines (two full batches), as if killed there.
	summary, err := ImportNDJSONWithIDs(context.Background(), db, d.Name, strings.NewReader(strings.Join(lines[:4], "\n")+"\n"), 2, "load", nil)
	if err != nil || summary.CommittedBatches != 2 || summary.ReplayedBatches != 0 {
		t.Fatalf("first: %+v %v", summary, err)
	}
	var receipts []BatchReceipt
	summary, err = ImportNDJSONWithIDs(context.Background(), db, d.Name, strings.NewReader(input), 2, "load", func(r BatchReceipt) error { receipts = append(receipts, r); return nil })
	if err != nil || summary.CommittedBatches != 3 || summary.ReplayedBatches != 2 || summary.NextLine != 6 {
		t.Fatalf("rerun: %+v %v", summary, err)
	}
	if len(receipts) != 3 || !receipts[0].Result.Replayed || !receipts[1].Result.Replayed || receipts[2].Result.Replayed || receipts[2].FromLine != 5 {
		t.Fatalf("receipts: %+v", receipts)
	}
	if s := status(t, db, d.Name); s.Accepted != 5 {
		t.Fatalf("accepted %d, want 5", s.Accepted)
	}
	// With another batch size the boundaries move, so a batch named like an
	// earlier one holds other lines: refused, never counted twice.
	summary, err = ImportNDJSONWithIDs(context.Background(), db, d.Name, strings.NewReader(input), 3, "load", nil)
	if !errors.Is(err, rollupdb.ErrBatchConflict) || summary.CommittedBatches != 0 {
		t.Fatalf("other batch size: %+v %v", summary, err)
	}
	if s := status(t, db, d.Name); s.Accepted != 5 {
		t.Fatalf("accepted %d after a refused rerun", s.Accepted)
	}
	if _, err = ImportNDJSONWithIDs(context.Background(), db, d.Name, strings.NewReader(input), 2, strings.Repeat("p", MaxBatchIDPrefix+1), nil); err == nil {
		t.Fatal("long prefix accepted")
	}
}
