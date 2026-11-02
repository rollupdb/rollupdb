package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rollupdb/rollupdb"
)

// The fixtures in testdata/remotewrite were written by the official protobuf
// runtime and an independent snappy encoder (see generate.py there).

const fixtureT0 = 1792929720000 // ms, 2026-10-25T12:02:00Z

func readFixture(t *testing.T, name string) ([]byte, map[string]any) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "remotewrite", name+".bin"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(filepath.Join("testdata", "remotewrite", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var expected map[string]any
	if err = json.Unmarshal(encoded, &expected); err != nil {
		t.Fatal(err)
	}
	return body, expected
}

func TestSnappyHandmadeBlocks(t *testing.T) {
	uv := func(n int) []byte { return binary.AppendUvarint(nil, uint64(n)) }
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	long := bytes.Repeat([]byte("x"), 70)
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{"empty", uv(0), ""},
		{"short literal", cat(uv(3), []byte{2 << 2}, []byte("abc")), "abc"},
		{"literal with a 1-byte length", cat(uv(70), []byte{60 << 2, 69}, long), string(long)},
		{"literal with a 2-byte length", cat(uv(70), []byte{61 << 2, 69, 0}, long), string(long)},
		{"copy 1-byte offset, overlapping run", cat(uv(9), []byte{0 << 2}, []byte("a"), []byte{1 | (4 << 2), 1}), "aaaaaaaaa"},
		{"copy 2-byte offset", cat(uv(8), []byte{3 << 2}, []byte("abcd"), []byte{2 | (3 << 2), 4, 0}), "abcdabcd"},
		{"copy 4-byte offset", cat(uv(6), []byte{2 << 2}, []byte("xyz"), []byte{3 | (2 << 2), 3, 0, 0, 0}), "xyzxyz"},
	}
	for _, c := range cases {
		got, err := snappyDecode(c.in, 1<<20)
		if err != nil || string(got) != c.want {
			t.Fatalf("%s: %q %v", c.name, got, err)
		}
	}
	bad := map[string][]byte{
		"no length":           {},
		"too long":            cat(uv(1<<21), []byte{0}),
		"short output":        cat(uv(4), []byte{2 << 2}, []byte("abc")),
		"literal past input":  cat(uv(5), []byte{4 << 2}, []byte("ab")),
		"literal past length": cat(uv(2), []byte{2 << 2}, []byte("abc")),
		"offset zero":         cat(uv(5), []byte{0}, []byte("a"), []byte{1 | (0 << 2), 0}),
		"offset before start": cat(uv(6), []byte{0}, []byte("a"), []byte{2 | (4 << 2), 2, 0}),
		"copy past length":    cat(uv(3), []byte{0}, []byte("a"), []byte{1 | (4 << 2), 1}),
		"cut copy tag":        cat(uv(3), []byte{0}, []byte("a"), []byte{2 | (1 << 2), 1}),
		"huge literal length": cat(uv(3), []byte{63 << 2, 0xff, 0xff, 0xff, 0xff}),
		"huge 4-byte offset":  cat(uv(3), []byte{0}, []byte("a"), []byte{3 | (1 << 2), 0xff, 0xff, 0xff, 0xff}),
	}
	for name, in := range bad {
		if got, err := snappyDecode(in, 1<<20); err == nil {
			t.Fatalf("%s: decoded %q", name, got)
		}
	}
}

func TestDecodeFixtures(t *testing.T) {
	body, _ := readFixture(t, "basic")
	decoded, err := snappyDecode(body, MaxRemoteWriteDecoded)
	if err != nil {
		t.Fatal(err)
	}
	series, err := decodeWriteRequest(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 4 {
		t.Fatalf("%d series", len(series))
	}
	a := series[0]
	if a.labels["__name__"] != "http_requests_total" || a.labels["instance"] != "a:9090" || a.labels["code"] != "200" || len(a.samples) != 3 {
		t.Fatalf("series 0: %+v", a)
	}
	if a.samples[1].value != 2.5 || a.samples[1].timestamp != fixtureT0+1000 || math.Float64bits(a.samples[2].value) != staleNaN {
		t.Fatalf("samples: %+v", a.samples)
	}
	if h := series[3]; h.labels["__name__"] != "latency_seconds" || len(h.samples) != 0 {
		t.Fatalf("histogram series: %+v", h)
	}
	body, _ = readFixture(t, "large")
	decoded, err = snappyDecode(body, MaxRemoteWriteDecoded)
	if err != nil {
		t.Fatal(err)
	}
	series, err = decodeWriteRequest(decoded)
	if err != nil || len(series) != 1 || len(series[0].samples) != 12000 {
		t.Fatalf("large: %d %v", len(series), err)
	}
	for i, s := range series[0].samples {
		if s.value != float64(i) || s.timestamp != fixtureT0+int64(i) {
			t.Fatalf("sample %d: %+v", i, s)
		}
	}
	// A request cut short anywhere never panics, and one cut inside a field
	// (here the trailing metadata) is refused.
	small, _ := snappyDecode(mustRead(t, "basic"), MaxRemoteWriteDecoded)
	for cut := 1; cut < len(small); cut++ {
		_, _ = decodeWriteRequest(small[:cut])
	}
	if _, err := decodeWriteRequest(small[:len(small)-1]); err == nil {
		t.Fatal("request cut inside its last field was accepted")
	}
}

func mustRead(t *testing.T, name string) []byte {
	body, _ := readFixture(t, name)
	return body
}

func remoteWriteFixture(t *testing.T) (*rollupdb.DB, rollupdb.Definition, http.Handler, *RemoteWriteMap) {
	t.Helper()
	now := time.UnixMilli(fixtureT0 + 20000).UTC()
	db, err := rollupdb.Create(context.Background(), filepath.Join(t.TempDir(), "rw.rollupdb"), rollupdb.WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	d := rollupdb.Definition{Name: "requests", Version: 1, EffectiveAt: now.Truncate(24 * time.Hour), Dimensions: []string{"service", "host"}, Statistics: []string{"count", "sum"}, MaxGroups: 10, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 3600, GraceSeconds: 120}, {WidthSeconds: 60, RetainSeconds: 0, GraceSeconds: 120}}}
	if err = db.CreateStream(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "map.json")
	if err = os.WriteFile(path, []byte(`{"metrics":[{"metric":"http_requests_total","stream":"requests","dimensions":{"service":"job","host":"instance"}}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	m, err := LoadRemoteWriteMap(context.Background(), db, path)
	if err != nil {
		t.Fatal(err)
	}
	return db, d, NewHandler(db, WithRemoteWrite(m)), m
}

func postWrite(t *testing.T, h http.Handler, body []byte, header map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("POST", "http://127.0.0.1/api/v1/write", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("Content-Encoding", "snappy")
	r.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	for k, v := range header {
		if v == "" {
			r.Header.Del(k)
		} else {
			r.Header.Set(k, v)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func rwStats(t *testing.T, h http.Handler) RemoteWriteStats {
	t.Helper()
	w := request(t, h, "GET", "/api/v1/write/stats", nil)
	var s RemoteWriteStats
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil || w.Code != 200 {
		t.Fatalf("stats: %d %s", w.Code, w.Body.String())
	}
	return s
}

func TestRemoteWriteStoresMappedSamplesOnce(t *testing.T) {
	db, d, h, _ := remoteWriteFixture(t)
	body, expected := readFixture(t, "basic")
	if w := postWrite(t, h, body, nil); w.Code != 204 {
		t.Fatalf("write: %d %s", w.Code, w.Body.String())
	}
	s := rwStats(t, h)
	if s.Samples != int64(expected["samples"].(float64)) || s.StaleMarkers != 1 || s.Unmapped != 1 || s.Accepted != 3 || s.Submitted != 3 {
		t.Fatalf("stats: %+v", s)
	}
	ctx := context.Background()
	from := time.UnixMilli(fixtureT0).UTC()
	for _, o := range expected["observations"].([]any) {
		obs := o.(map[string]any)
		at := time.UnixMilli(int64(obs["time_ms"].(float64))).UTC().Truncate(time.Second)
		q, err := db.Query(ctx, rollupdb.Query{Stream: d.Name, Version: 1, From: at, To: at.Add(time.Second), Filter: map[string]string{"service": obs["job"].(string), "host": obs["instance"].(string)}})
		if err != nil || q.Statistics["count"] != int64(1) || q.Statistics["sum"] != obs["value"].(float64) {
			t.Fatalf("observation %v: %+v %v", obs, q.Statistics, err)
		}
	}
	// Prometheus retries the same body after a 5xx: nothing is counted twice.
	if w := postWrite(t, h, body, nil); w.Code != 204 {
		t.Fatalf("retry: %d %s", w.Code, w.Body.String())
	}
	if s = rwStats(t, h); s.ReplayedBatches != 1 || s.Accepted != 3 {
		t.Fatalf("stats after retry: %+v", s)
	}
	q, err := db.Query(ctx, rollupdb.Query{Stream: d.Name, Version: 1, From: from, To: from.Add(2 * time.Second)})
	if err != nil || q.Statistics["count"] != int64(3) {
		t.Fatalf("total after retry: %+v %v", q.Statistics, err)
	}
}

func TestRemoteWriteSplitsLargeRequests(t *testing.T) {
	db, d, h, _ := remoteWriteFixture(t)
	body, expected := readFixture(t, "large")
	if w := postWrite(t, h, body, nil); w.Code != 204 {
		t.Fatalf("write: %d %s", w.Code, w.Body.String())
	}
	from := time.UnixMilli(fixtureT0).UTC()
	q, err := db.Query(context.Background(), rollupdb.Query{Stream: d.Name, Version: 1, From: from, To: from.Add(13 * time.Second)})
	if err != nil || q.Statistics["count"] != int64(12000) || q.Statistics["sum"] != expected["sum"].(float64) {
		t.Fatalf("large: %+v %v", q.Statistics, err)
	}
	if w := postWrite(t, h, body, nil); w.Code != 204 {
		t.Fatalf("retry: %d", w.Code)
	}
	if s := rwStats(t, h); s.ReplayedBatches != 2 || s.Accepted != 12000 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestRemoteWriteRefusals(t *testing.T) {
	_, _, h, _ := remoteWriteFixture(t)
	body := mustRead(t, "basic")
	cases := []struct {
		name   string
		body   []byte
		header map[string]string
		code   int
	}{
		{"remote write 2.0", body, map[string]string{"Content-Type": "application/x-protobuf;proto=io.prometheus.write.v2.Request"}, 415},
		{"json", body, map[string]string{"Content-Type": "application/json"}, 415},
		{"no encoding", body, map[string]string{"Content-Encoding": ""}, 415},
		{"gzip", body, map[string]string{"Content-Encoding": "gzip"}, 415},
		{"not snappy", []byte("hello"), nil, 400},
		{"not protobuf", append(binary.AppendUvarint(nil, 3), 2<<2, 0xff, 0xff, 0xff), nil, 400},
		{"browser", body, map[string]string{"Origin": "https://example.com"}, 403},
	}
	for _, c := range cases {
		if w := postWrite(t, h, c.body, c.header); w.Code != c.code {
			t.Fatalf("%s: %d %s", c.name, w.Code, w.Body.String())
		}
	}
	explicit := postWrite(t, h, body, map[string]string{"Content-Type": "application/x-protobuf;proto=prometheus.WriteRequest"})
	if explicit.Code != 204 {
		t.Fatalf("1.0 with proto parameter: %d %s", explicit.Code, explicit.Body.String())
	}
	// Without the option the endpoint is not there.
	db, _, _, _ := remoteWriteFixture(t)
	if w := postWrite(t, NewHandler(db), body, nil); w.Code != 404 {
		t.Fatalf("disabled endpoint: %d", w.Code)
	}
}

func TestRemoteWriteMapChecks(t *testing.T) {
	db, _, _, _ := remoteWriteFixture(t)
	dir := t.TempDir()
	for name, text := range map[string]string{
		"empty":             `{"metrics":[]}`,
		"unknown stream":    `{"metrics":[{"metric":"m","stream":"nope","dimensions":{"service":"a","host":"b"}}]}`,
		"missing dimension": `{"metrics":[{"metric":"m","stream":"requests","dimensions":{"service":"a"}}]}`,
		"extra dimension":   `{"metrics":[{"metric":"m","stream":"requests","dimensions":{"service":"a","host":"b","zone":"c"}}]}`,
		"wrong dimension":   `{"metrics":[{"metric":"m","stream":"requests","dimensions":{"service":"a","zone":"b"}}]}`,
		"duplicate metric":  `{"metrics":[{"metric":"m","stream":"requests","dimensions":{"service":"a","host":"b"}},{"metric":"m","stream":"requests","dimensions":{"service":"a","host":"b"}}]}`,
		"no metric name":    `{"metrics":[{"metric":"","stream":"requests","dimensions":{"service":"a","host":"b"}}]}`,
		"unknown field":     `{"metrics":[{"metric":"m","stream":"requests","dimensions":{"service":"a","host":"b"},"labels":{}}]}`,
	} {
		path := filepath.Join(dir, strings.ReplaceAll(name, " ", "_")+".json")
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadRemoteWriteMap(context.Background(), db, path); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}
