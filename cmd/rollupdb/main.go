package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"unicode/utf8"

	"github.com/rollupdb/rollupdb"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) (err error) {
	const usage = "Usage: rollupdb init FILE | migrate FILE | inspect FILE | snapshot FILE DEST | create-stream FILE DEFINITION.json | stream FILE NAME | stats FILE NAME | ingest FILE BATCH.json | query FILE QUERY.json"
	if len(args) == 1 && (args[0] == "help" || args[0] == "--help" || args[0] == "-h") {
		_, err := fmt.Fprintln(out, "RollupDB prototype\n"+usage+"\nExact aggregation and optional KLL quantiles are available. Automatic rollups are not yet implemented.")
		return err
	}
	if len(args) < 2 {
		return errors.New(usage)
	}
	op, path := args[0], args[1]
	want := 2
	switch op {
	case "init", "inspect", "migrate":
	case "snapshot", "create-stream", "stream", "ingest", "query", "stats":
		want = 3
	default:
		return fmt.Errorf("unknown command %q", op)
	}
	if len(args) != want {
		return fmt.Errorf("%s expects %d arguments", op, want-1)
	}
	if op == "migrate" {
		return rollupdb.Migrate(ctx, path)
	}
	var db *rollupdb.DB
	if op == "init" {
		db, err = rollupdb.Create(ctx, path)
	} else {
		db, err = rollupdb.Open(ctx, path)
	}
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	switch op {
	case "create-stream":
		var d rollupdb.Definition
		if err = decodeFile(args[2], &d, 1<<20); err != nil {
			return err
		}
		return db.CreateStream(ctx, d)
	case "ingest":
		var batch rollupdb.Batch
		if err = decodeFile(args[2], &batch, 64<<20); err != nil {
			return err
		}
		result, err := db.Ingest(ctx, batch)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(result)
	case "query":
		var query rollupdb.Query
		if err = decodeFile(args[2], &query, 1<<20); err != nil {
			return err
		}
		result, err := db.Query(ctx, query)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(result)
	case "stats":
		result, err := db.Status(ctx, args[2])
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(result)
	case "stream":
		d, err := db.Stream(ctx, args[2])
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(d)
	case "snapshot":
		return db.Snapshot(ctx, args[2])
	default:
		i, err := db.Info(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(i)
	}
}

func decodeFile(path string, target any, limit int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	encoded, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return err
	}
	if int64(len(encoded)) > limit {
		return fmt.Errorf("JSON file exceeds %d bytes", limit)
	}
	if !utf8.Valid(encoded) {
		return errors.New("JSON file must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("expected exactly one JSON object")
	}
	return nil
}
