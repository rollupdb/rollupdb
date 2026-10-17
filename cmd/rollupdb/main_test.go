package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rollupdb/rollupdb"
)

func TestCLIFileLifecycle(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.rollupdb")
	dest := filepath.Join(dir, "snapshot.rollupdb")
	var out bytes.Buffer
	for _, args := range [][]string{{"init", source}, {"create-stream", source, "../../examples/api_requests.json"}, {"stream", source, "api_requests"}, {"snapshot", source, dest}, {"inspect", dest}} {
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
