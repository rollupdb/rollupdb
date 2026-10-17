package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
