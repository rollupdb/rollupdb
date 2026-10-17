package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rollupdb/rollupdb"
)

func TestCLIFileLifecycle(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.rollupdb")
	dest := filepath.Join(dir, "snapshot.rollupdb")
	var out bytes.Buffer
	for _, args := range [][]string{{"init", source}, {"create-stream", source, "../../examples/api_requests.json"}, {"stream", source, "api_requests"}, {"advance", source, "api_requests"}, {"stats", source, "api_requests"}, {"snapshot", source, dest}, {"inspect", dest}} {
		out.Reset()
		if err := run(context.Background(), args, &out); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	if !bytes.Contains(out.Bytes(), []byte(`"streams":1`)) {
		t.Fatalf("bad inspect: %s", out.String())
	}
}

func TestCLIExactIngestionQueriesAndStrictJSON(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "live.rollupdb")
	var out bytes.Buffer
	if err := run(ctx, []string{"init", path}, &out); err != nil {
		t.Fatal(err)
	}
	d := rollupdb.Definition{Name: "api", Version: 1, EffectiveAt: time.Unix(0, 0).UTC(), Statistics: []string{"count", "sum", "avg"}, MaxGroups: 1, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 0, GraceSeconds: 3600}}}
	write := func(name string, value any) string {
		t.Helper()
		p := filepath.Join(dir, name)
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(p, body, 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	dfile := write("definition.json", d)
	if err := run(ctx, []string{"create-stream", path, dfile}, &out); err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	batch := rollupdb.Batch{Stream: d.Name, Version: 1, Observations: []rollupdb.Observation{{Time: at, Value: 0}, {Time: at, Value: 2}, {Time: at, Value: 4}}}
	bfile := write("batch.json", batch)
	out.Reset()
	if err := run(ctx, []string{"ingest", path, bfile}, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte(`"accepted":3`)) {
		t.Fatalf("ingest: %s", out.String())
	}
	qfile := write("query.json", rollupdb.Query{Stream: d.Name, Version: 1, From: at, To: at.Add(time.Second)})
	out.Reset()
	if err := run(ctx, []string{"query", path, qfile}, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte(`"avg":2`)) || !bytes.Contains(out.Bytes(), []byte(`"count":3`)) {
		t.Fatalf("query: %s", out.String())
	}
	for _, observation := range []string{`{"time":"` + at.Format(time.RFC3339) + `"}`, `{"time":"` + at.Format(time.RFC3339) + `","value":null}`, `{"time":"` + at.Format(time.RFC3339) + `","value":1,"unknown":2}`} {
		body := `{"stream":"api","version":1,"observations":[` + observation + `]}`
		invalid := filepath.Join(dir, "invalid.json")
		if err := os.WriteFile(invalid, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if err := run(ctx, []string{"ingest", path, invalid}, &out); err == nil {
			t.Fatalf("accepted malformed observation %s", body)
		}
	}
	out.Reset()
	if err := run(ctx, []string{"stats", path, d.Name}, &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte(`"accepted":3`)) || !bytes.Contains(out.Bytes(), []byte(`"rejected_invalid":0`)) {
		t.Fatalf("invalid envelopes changed counters: %s", out.String())
	}
}

func TestCLIRejectsInvalidInvocationAndDefinitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db.rollupdb")
	for _, args := range [][]string{{}, {"unknown", path}, {"init", path, "extra"}, {"snapshot", path}} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("bad invocation created database")
	}
	if err := run(context.Background(), []string{"init", path}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"unexpected":true}`, `{} {}`, `{"name":"invalid"}`, strings.Repeat(" ", 1<<20) + `{}`} {
		config := filepath.Join(t.TempDir(), "stream.json")
		if err := os.WriteFile(config, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if err := run(context.Background(), []string{"create-stream", path, config}, &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
}

func TestCLIQuantileDefinitionAndQuery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "quantiles.rollupdb")
	var out bytes.Buffer
	write := func(name string, value any) string {
		t.Helper()
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, name)
		if err = os.WriteFile(p, body, 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	d := rollupdb.Definition{Name: "latency", Version: 1, EffectiveAt: time.Unix(0, 0).UTC(), Statistics: []string{"count", "avg"}, MaxGroups: 1, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 0, GraceSeconds: 3600}}, Sketch: &rollupdb.SketchConfig{Algorithm: rollupdb.SketchAlgorithm, EncodingVersion: 1, K: 200, M: 8}}
	at := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	batch := rollupdb.Batch{Stream: d.Name, Version: 1, Observations: []rollupdb.Observation{{Time: at, Value: 1}, {Time: at, Value: 2}, {Time: at, Value: 3}}}
	q := rollupdb.Query{Stream: d.Name, Version: 1, From: at, To: at.Add(time.Second), Quantiles: []float64{0.5, 0.99}}
	for _, args := range [][]string{{"init", path}, {"create-stream", path, write("definition.json", d)}, {"ingest", path, write("batch.json", batch)}, {"query", path, write("query.json", q)}} {
		out.Reset()
		if err := run(ctx, args, &out); err != nil {
			t.Fatal(err)
		}
	}
	var result rollupdb.QueryResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.QuantileAccuracy == nil || result.QuantileAccuracy.Availability != "available" || len(result.Quantiles) != 2 || *result.Quantiles[0].Value != 2 || *result.Quantiles[1].Value != 3 {
		t.Fatalf("CLI quantiles: %+v", result)
	}
}

func TestCLIStreamingInputAndPartialProgress(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "ndjson.rollupdb")
	db, err := rollupdb.Create(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	d := rollupdb.Definition{Name: "api", Version: 1, EffectiveAt: time.Unix(0, 0).UTC(), Statistics: []string{"count"}, MaxGroups: 1, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 0, GraceSeconds: 3600}}}
	if err = db.CreateStream(ctx, d); err != nil {
		t.Fatal(err)
	}
	db.Close()
	at := time.Now().UTC().Add(-time.Second)
	encoded, err := json.Marshal(rollupdb.Observation{Time: at, Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	input := strings.Repeat(string(encoded)+"\n", 3) + "null\n"
	var out bytes.Buffer
	err = runWithInput(ctx, []string{"ingest-ndjson", path, "api", "-", "--batch-size", "2"}, strings.NewReader(input), &out)
	if err == nil {
		t.Fatal("malformed input succeeded")
	}
	var summary struct {
		Type             string `json:"type"`
		CommittedRecords int    `json:"committed_records"`
		DiscardedRecords int    `json:"discarded_records"`
		NextLine         int    `json:"next_line"`
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected receipt then failure summary: %s", out.String())
	}
	if err = json.Unmarshal([]byte(lines[1]), &summary); err != nil || summary.Type != "error" || summary.CommittedRecords != 2 || summary.DiscardedRecords != 1 || summary.NextLine != 3 {
		t.Fatalf("summary: %+v %v", summary, err)
	}
	reopened, err := rollupdb.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	s, err := reopened.Status(ctx, "api")
	if err != nil || s.Accepted != 2 {
		t.Fatalf("CLI partial commits: %+v %v", s, err)
	}
	if err = run(ctx, []string{"serve", path, "--listen", "0.0.0.0:7070"}, &out); err == nil {
		t.Fatal("CLI exposed remote listener")
	}
}

func TestCLIServeShutdownAndCanceledStdin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "server.rollupdb")
	db, err := rollupdb.Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	d := rollupdb.Definition{Name: "api", Version: 1, EffectiveAt: time.Unix(0, 0).UTC(), Statistics: []string{"count"}, MaxGroups: 1, Tiers: []rollupdb.Tier{{WidthSeconds: 1, RetainSeconds: 0, GraceSeconds: 3600}}}
	if err = db.CreateStream(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output, input := io.Pipe()
	defer output.Close()
	done := make(chan error, 1)
	go func() {
		defer input.Close()
		done <- run(ctx, []string{"serve", path, "--listen", "127.0.0.1:0"}, input)
	}()
	var listening struct {
		Address string `json:"listening"`
	}
	if err = json.NewDecoder(output).Decode(&listening); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{Proxy: nil}}
	response, err := client.Get("http://" + listening.Address + "/v1/info")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CLI shutdown blocked")
	}
	reopened, err := rollupdb.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	// An idle stdin pipe is owned by the command and must unblock on cancellation.
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close()
	var out bytes.Buffer
	go func() { done <- runWithInput(ctx, []string{"ingest-ndjson", path, "api", "-"}, reader, &out) }()
	cancel()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("canceled import succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled stdin remained blocked")
	}
}

type signaledInput struct {
	reader  *io.PipeReader
	started chan struct{}
	once    sync.Once
}

func (s *signaledInput) Read(p []byte) (int, error) {
	s.once.Do(func() { close(s.started) })
	return s.reader.Read(p)
}
func (s *signaledInput) Close() error { return s.reader.Close() }

func TestCLIVersion(t *testing.T) {
	for _, arg := range []string{"version", "--version"} {
		var out bytes.Buffer
		if err := run(context.Background(), []string{arg}, &out); err != nil {
			t.Fatalf("%s: %v", arg, err)
		}
		if got, want := strings.TrimSpace(out.String()), "rollupdb "+rollupdb.Version; got != want {
			t.Fatalf("%s printed %q, want %q", arg, got, want)
		}
	}
}
