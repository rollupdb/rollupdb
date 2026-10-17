package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rollupdb/rollupdb"
)

func request(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var b []byte
	switch v := body.(type) {
	case string:
		b = []byte(v)
	case nil:
	default:
		var err error
		b, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	r := httptest.NewRequest(method, "http://127.0.0.1"+path, bytes.NewReader(b))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestHTTPFullLifecycleMatchesEmbeddedAndDownloadsSnapshot(t *testing.T) {
	db, d, now := fixture(t)
	h := NewHandler(db)
	for _, path := range []string{"/v1/info", "/v1/streams/api", "/v1/streams/api/stats"} {
		if w := request(t, h, "GET", path, nil); w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
	next := d
	next.Name = "created"
	if w := request(t, h, "POST", "/v1/streams", next); w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	if w := request(t, h, "POST", "/v1/streams", next); w.Code != 409 {
		t.Fatalf("duplicate: %d", w.Code)
	}
	batch := rollupdb.Batch{Stream: d.Name, Version: 1, Observations: []rollupdb.Observation{{Time: now.Add(-time.Second), Value: 3, Dimensions: map[string]string{"service": "web"}}, {Time: now.Add(-121 * time.Second), Value: 9, Dimensions: map[string]string{"service": "web"}}}}
	w := request(t, h, "POST", "/v1/ingest", batch)
	var result rollupdb.BatchResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != 200 || result.Accepted != 1 || result.RejectedLate != 1 {
		t.Fatalf("ingest: %d %s %v", w.Code, w.Body.String(), err)
	}
	q := rollupdb.Query{Stream: d.Name, Version: 1, From: now.Add(-time.Minute), To: now}
	embedded, err := db.Query(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	expected, _ := json.Marshal(embedded)
	w = request(t, h, "POST", "/v1/query", q)
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != string(expected) {
		t.Fatalf("HTTP changed query: %d %s", w.Code, w.Body.String())
	}
	if w = request(t, h, "POST", "/v1/streams/api/advance", struct{}{}); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w = request(t, h, "GET", "/v1/snapshot", nil)
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/octet-stream" || w.Header().Get("Content-Length") == "" {
		t.Fatalf("snapshot: %d %s", w.Code, w.Body.String())
	}
	path := filepath.Join(t.TempDir(), "download.rollupdb")
	if err = os.WriteFile(path, w.Body.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	copied, err := rollupdb.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer copied.Close()
	got, err := copied.Query(context.Background(), q)
	if err != nil || got.Statistics["count"] != int64(1) || got.Statistics["sum"] != float64(3) {
		t.Fatalf("downloaded snapshot: %+v %v", got, err)
	}
}

func TestHTTPRejectsMalformedOversizedAndBrowserRequestsWithoutWrites(t *testing.T) {
	db, d, now := fixture(t)
	h := NewHandler(db)
	cases := []struct {
		method, path, body string
		want               int
	}{
		{"POST", "/v1/ingest", "null", 400}, {"POST", "/v1/ingest", `{"stream":"api","version":1,"observations":[{"time":"2026-10-17T12:02:00Z"}]}`, 400},
		{"POST", "/v1/ingest", `{} {}`, 400}, {"POST", "/v1/ingest", `{"unknown":1}`, 400},
		{"POST", "/v1/ingest", strings.Repeat(" ", MaxIngestBody+1), 413}, {"POST", "/v1/query", strings.Repeat(" ", MaxJSONBody+1), 413},
		{"POST", "/v1/streams", `{}`, 400}, {"POST", "/v1/streams/api/advance", `{"time":"2030-01-01"}`, 400},
		{"GET", "/v1//info", "", 400}, {"GET", "/v1/../v1/info", "", 400}, {"GET", "/v1/unknown", "", 404}, {"DELETE", "/v1/ingest", "", 405}, {"POST", "/v1/query?from=now", "{}", 400},
		{"GET", "/v1/streams/missing", "", 404},
	}
	for _, c := range cases {
		w := request(t, h, c.method, c.path, c.body)
		if w.Code != c.want || !json.Valid(w.Body.Bytes()) || !bytes.Contains(w.Body.Bytes(), []byte(`"error"`)) {
			t.Fatalf("%s %s: %d %s", c.method, c.path, w.Code, w.Body.String())
		}
	}
	body := record(t, now, 1)
	for _, headers := range []map[string]string{{"Origin": "https://evil.example"}, {"Host": "evil.example"}, {"Content-Type": "text/plain"}, {"Content-Encoding": "gzip"}} {
		r := httptest.NewRequest("POST", "http://127.0.0.1/v1/streams/api/advance", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		want := 415
		for k, v := range headers {
			if k == "Host" {
				r.Host = v
			} else {
				r.Header.Set(k, v)
			}
			if k == "Origin" || k == "Host" {
				want = 403
			}
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("headers %v: %d %s", headers, w.Code, w.Body.String())
		}
	}
	s := status(t, db, d.Name)
	if s.Accepted != 0 || s.ObservedThrough != nil {
		t.Fatalf("bad request mutated engine: %+v", s)
	}
	q := rollupdb.Query{Stream: d.Name, Version: 1, From: now.Add(-time.Nanosecond), To: now}
	if w := request(t, h, "POST", "/v1/query", q); w.Code != 422 {
		t.Fatalf("partial boundary: %d %s", w.Code, w.Body.String())
	}
	db.Close()
	if w := request(t, h, "GET", "/v1/info", nil); w.Code != 503 {
		t.Fatalf("closed: %d", w.Code)
	}
}

func TestLoopbackListenerAndGracefulServe(t *testing.T) {
	for _, address := range []string{":0", "0.0.0.0:0", "[::]:0", "192.0.2.1:0", "localhost:0", "invalid"} {
		if l, err := Listen(address); err == nil {
			l.Close()
			t.Fatalf("nonliteral/nonlocal listener %s", address)
		}
	}
	db, _, _ := fixture(t)
	listener, err := Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, db, listener) }()
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}}
	response, err := client.Get("http://" + listener.Addr().String() + "/v1/info")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.Status)
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown blocked")
	}
	if c, e := net.DialTimeout("tcp", listener.Addr().String(), time.Second); e == nil {
		c.Close()
		t.Fatal("listener survived shutdown")
	}
	if _, err = db.Info(context.Background()); err != nil {
		t.Fatal("Serve closed a caller-owned handle")
	}
}

func TestHTTPQuantileDefinitionAndNullRankRefusal(t *testing.T) {
	db, d, now := fixture(t)
	h := NewHandler(db)
	d.Name = "quantiles"
	d.Sketch = &rollupdb.SketchConfig{Algorithm: rollupdb.SketchAlgorithm, EncodingVersion: 1, K: 200, M: 8}
	if w := request(t, h, "POST", "/v1/streams", d); w.Code != 201 {
		t.Fatalf("sketch definition: %d %s", w.Code, w.Body.String())
	}
	batch := rollupdb.Batch{Stream: d.Name, Version: 1}
	for _, v := range []float64{1, 2, 3} {
		batch.Observations = append(batch.Observations, rollupdb.Observation{Time: now.Add(-time.Second), Value: v, Dimensions: map[string]string{"service": "web"}})
	}
	if w := request(t, h, "POST", "/v1/ingest", batch); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	q := rollupdb.Query{Stream: d.Name, Version: 1, From: now.Add(-time.Second), To: now, Quantiles: []float64{0, .5, 1}}
	w := request(t, h, "POST", "/v1/query", q)
	var result rollupdb.QueryResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != 200 || result.QuantileAccuracy.Availability != "available" || *result.Quantiles[1].Value != 2 {
		t.Fatalf("quantile response: %d %s %v", w.Code, w.Body.String(), err)
	}
	encoded, _ := json.Marshal(q)
	nullRank := strings.Replace(string(encoded), `[0,0.5,1]`, `[null]`, 1)
	if w = request(t, h, "POST", "/v1/query", nullRank); w.Code != 400 {
		t.Fatalf("null rank became zero: %d %s", w.Code, w.Body.String())
	}
}
