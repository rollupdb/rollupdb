package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/rollupdb/rollupdb"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) (err error) {
	if len(args) == 1 && (args[0] == "help" || args[0] == "--help" || args[0] == "-h") {
		_, err := fmt.Fprintln(out, "RollupDB prototype\nUsage: rollupdb init FILE | inspect FILE | snapshot FILE DEST | create-stream FILE DEFINITION.json | stream FILE NAME\nIngestion and rollups are not yet implemented.")
		return err
	}
	if len(args) < 2 {
		return errors.New("usage: rollupdb init FILE | inspect FILE | snapshot FILE DEST | create-stream FILE DEFINITION.json | stream FILE NAME")
	}
	op, path := args[0], args[1]
	want := 2
	switch op {
	case "init", "inspect":
	case "snapshot", "create-stream", "stream":
		want = 3
	default:
		return fmt.Errorf("unknown command %q", op)
	}
	if len(args) != want {
		return fmt.Errorf("%s expects %d arguments", op, want-1)
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
		f, err := os.Open(args[2])
		if err != nil {
			return err
		}
		defer f.Close()
		encoded, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
		if err != nil {
			return err
		}
		if len(encoded) > 1<<20 {
			return errors.New("stream definition exceeds 1 MiB")
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.DisallowUnknownFields()
		var d rollupdb.Definition
		if err = decoder.Decode(&d); err != nil {
			return err
		}
		var extra any
		if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			return errors.New("expected exactly one stream definition")
		}
		return db.CreateStream(ctx, d)
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
