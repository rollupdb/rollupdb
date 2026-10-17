package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rollupdb/rollupdb"
	"github.com/rollupdb/rollupdb/transport"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) (err error) {
	return runWithInput(ctx, args, os.Stdin, out)
}

func runWithInput(ctx context.Context, args []string, input io.Reader, out io.Writer) (err error) {
	const usage = "Usage: rollupdb version | init FILE | migrate FILE | inspect FILE | snapshot FILE DEST | create-stream FILE DEFINITION.json | update-stream FILE DEFINITION.json | stream FILE NAME | stats FILE NAME | advance FILE [NAME] | ingest FILE BATCH.json | query FILE QUERY.json | ingest-ndjson FILE STREAM INPUT [--batch-size N] | serve FILE [--listen 127.0.0.1:7070] [--advance-every 1m]"
	if len(args) == 1 && (args[0] == "version" || args[0] == "--version") {
		_, err := fmt.Fprintln(out, "rollupdb "+rollupdb.Version)
		return err
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "--help" || args[0] == "-h") {
		_, err := fmt.Fprintln(out, "RollupDB "+rollupdb.Version+"\n"+usage+"\nExact aggregation and optional KLL quantiles are available. Run advance, or serve with --advance-every, to promote and expire retained buckets.")
		return err
	}
	if len(args) < 2 {
		return errors.New(usage)
	}
	op, path := args[0], args[1]
	if op == "ingest-ndjson" {
		return importCommand(ctx, args[1:], input, out)
	}
	if op == "serve" {
		return serveCommand(ctx, args[1:], out)
	}
	want := 2
	switch op {
	case "init", "inspect", "migrate":
	case "advance":
		// advance FILE rolls up every stream; advance FILE NAME just one.
		if len(args) == 2 {
			want = 2
		} else {
			want = 3
		}
	case "snapshot", "create-stream", "update-stream", "stream", "ingest", "query", "stats":
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
	options, err := clockOptions()
	if err != nil {
		return err
	}
	var db *rollupdb.DB
	if op == "init" {
		db, err = rollupdb.Create(ctx, path, options...)
	} else {
		db, err = rollupdb.Open(ctx, path, options...)
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
	case "update-stream":
		var d rollupdb.Definition
		if err = decodeFile(args[2], &d, 1<<20); err != nil {
			return err
		}
		return db.UpdateStream(ctx, d)
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
	case "advance":
		if len(args) == 2 {
			results, err := db.AdvanceAll(ctx)
			if encodeErr := json.NewEncoder(out).Encode(results); encodeErr != nil {
				return errors.Join(err, encodeErr)
			}
			return err
		}
		result, err := db.Advance(ctx, args[2])
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
	return transport.DecodeObject(encoded, target)
}

func importCommand(ctx context.Context, args []string, input io.Reader, out io.Writer) (err error) {
	if len(args) < 3 {
		return errors.New("ingest-ndjson expects FILE STREAM INPUT [--batch-size N]")
	}
	flags := flag.NewFlagSet("ingest-ndjson", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	batchSize := flags.Int("batch-size", transport.DefaultBatchSize, "records per transaction")
	if err = flags.Parse(args[3:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected import arguments")
	}
	if *batchSize < 1 || *batchSize > rollupdb.MaxBatchSize {
		return errors.New("invalid batch size")
	}
	if args[2] != "-" {
		f, e := os.Open(args[2])
		if e != nil {
			return e
		}
		defer f.Close()
		input = f
	}
	options, err := clockOptions()
	if err != nil {
		return err
	}
	db, err := rollupdb.Open(ctx, args[0], options...)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	// The command owns its file/stdin reader. Closing it on cancellation unblocks
	// a pipe read; the reusable transport function does not own arbitrary readers.
	if closer, ok := input.(io.ReadCloser); ok {
		stopped := make(chan struct{})
		defer close(stopped)
		go func() {
			select {
			case <-ctx.Done():
				_ = closer.Close()
			case <-stopped:
			}
		}()
	}
	encoder := json.NewEncoder(out)
	summary, importErr := transport.ImportNDJSON(ctx, db, args[1], input, *batchSize, func(r transport.BatchReceipt) error { return encoder.Encode(r) })
	outputErr := encoder.Encode(summary)
	return errors.Join(importErr, outputErr)
}

func serveCommand(ctx context.Context, args []string, out io.Writer) (err error) {
	if len(args) < 1 {
		return errors.New("serve expects FILE [--listen ADDRESS] [--advance-every DURATION]")
	}
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("listen", "127.0.0.1:7070", "literal loopback IP and port")
	advanceEvery := flags.Duration("advance-every", time.Minute, "how often to roll up every stream; 0 turns it off")
	if err = flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected server arguments")
	}
	if *advanceEvery < 0 {
		return errors.New("--advance-every must not be negative")
	}
	listener, err := transport.Listen(*address)
	if err != nil {
		return err
	}
	defer listener.Close()
	options, err := clockOptions()
	if err != nil {
		return err
	}
	db, err := rollupdb.Open(ctx, args[0], options...)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	log := &lineWriter{w: out}
	if err = log.encode(struct {
		Listening    string `json:"listening"`
		AdvanceEvery string `json:"advance_every"`
	}{listener.Addr().String(), advanceEvery.String()}); err != nil {
		return err
	}
	if *advanceEvery == 0 {
		return transport.Serve(ctx, db, listener)
	}
	// The schedule stops when the server does: Serve returns only after
	// shutdown, and cancelling scheduleCtx then ends RunRetention before db
	// is closed.
	scheduleCtx, stopSchedule := context.WithCancel(ctx)
	scheduled := make(chan struct{})
	go func() {
		defer close(scheduled)
		_ = db.RunRetention(scheduleCtx, *advanceEvery, func(r rollupdb.RetentionReport) {
			if r.Err != nil {
				_ = log.encode(struct {
					Type  string `json:"type"`
					Error string `json:"error"`
				}{"advance_error", r.Err.Error()})
				return
			}
			_ = log.encode(struct {
				Type   string                 `json:"type"`
				Stream string                 `json:"stream"`
				Result rollupdb.AdvanceResult `json:"result"`
			}{"advance", r.Stream, r.Result})
		})
	}()
	err = transport.Serve(ctx, db, listener)
	stopSchedule()
	<-scheduled
	return err
}

// maxClockStepEnv changes how far the host clock may move past a stream's
// stored watermark in one step (default rollupdb.DefaultMaxClockStep). A Go
// duration such as 9000h; 0 removes the limit.
const maxClockStepEnv = "ROLLUPDB_MAX_CLOCK_STEP"

func clockOptions() ([]rollupdb.Option, error) {
	value, ok := os.LookupEnv(maxClockStepEnv)
	if !ok || value == "" {
		return nil, nil
	}
	step, err := time.ParseDuration(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", maxClockStepEnv, err)
	}
	return []rollupdb.Option{rollupdb.WithMaxClockStep(step)}, nil
}

// lineWriter writes one JSON line at a time, so the scheduler's lines and the
// server's don't interleave.
type lineWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lineWriter) encode(v any) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return json.NewEncoder(l.w).Encode(v)
}
