package transport

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/rollupdb/rollupdb"
)

const MaxRecordBytes = 256 << 10
const MaxBufferedBytes = 8 << 20
const DefaultBatchSize = 1000

type BatchReceipt struct {
	Type        string               `json:"type"`
	FromLine    int64                `json:"from_line"`
	ThroughLine int64                `json:"through_line"`
	Result      rollupdb.BatchResult `json:"result"`
}

type ImportSummary struct {
	Type             string `json:"type"`
	CommittedRecords int64  `json:"committed_records"`
	CommittedBatches int64  `json:"committed_batches"`
	rollupdb.Counters
	NextLine              int64  `json:"next_line"`
	FailedLine            int64  `json:"failed_line,omitempty"`
	DiscardedRecords      int    `json:"discarded_records"`
	UnacknowledgedRecords int    `json:"unacknowledged_records"`
	Error                 string `json:"error,omitempty"`
}

// ImportNDJSON commits bounded batches and emits a receipt after each commit.
// Syntax/read errors discard the not-yet-submitted batch, keeping earlier commits.
// An engine error leaves the submitted batch unacknowledged: a commit-level I/O
// failure can be ambiguous. Receipt delivery can also fail after a real commit.
// Readers such as a blocked stdin must be closed by their owner to unblock I/O;
// context cancellation is checked between reads and before every submission.
func ImportNDJSON(ctx context.Context, db *rollupdb.DB, stream string, input io.Reader, batchSize int, receipt func(BatchReceipt) error) (summary ImportSummary, err error) {
	summary = ImportSummary{Type: "complete", NextLine: 1}
	defer func() {
		if err != nil {
			summary.Type = "error"
			summary.Error = err.Error()
		}
	}()
	if batchSize < 1 || batchSize > rollupdb.MaxBatchSize {
		return summary, fmt.Errorf("batch size must be in [1,%d]", rollupdb.MaxBatchSize)
	}
	if _, err = db.Stream(ctx, stream); err != nil {
		return summary, err
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), MaxRecordBytes+2)
	batch := rollupdb.Batch{Stream: stream, Version: 1}
	bufferedBytes := 0
	flush := func() error {
		if len(batch.Observations) == 0 {
			return nil
		}
		if e := ctx.Err(); e != nil {
			summary.DiscardedRecords = len(batch.Observations)
			return e
		}
		result, e := db.Ingest(ctx, batch)
		if e != nil {
			summary.UnacknowledgedRecords = len(batch.Observations)
			return fmt.Errorf("batch starting at line %d unacknowledged: %w", summary.NextLine, e)
		}
		first := summary.NextLine
		summary.CommittedRecords += int64(len(batch.Observations))
		summary.CommittedBatches++
		summary.NextLine = summary.CommittedRecords + 1
		summary.Accepted += result.Accepted
		summary.RejectedInvalid += result.RejectedInvalid
		summary.RejectedLate += result.RejectedLate
		summary.RejectedGroupLimit += result.RejectedGroupLimit
		batch.Observations = nil
		bufferedBytes = 0
		if receipt != nil {
			if e = receipt(BatchReceipt{"batch", first, summary.NextLine - 1, result}); e != nil {
				return fmt.Errorf("receipt delivery after committed line %d: %w", summary.NextLine-1, e)
			}
		}
		return nil
	}
	var line int64
	for {
		if err = ctx.Err(); err != nil {
			summary.DiscardedRecords = len(batch.Observations)
			return summary, err
		}
		if !scanner.Scan() {
			break
		}
		line++
		encoded := scanner.Bytes()
		var o rollupdb.Observation
		if len(encoded) > MaxRecordBytes {
			err = fmt.Errorf("record exceeds %d bytes", MaxRecordBytes)
		} else {
			err = DecodeObject(encoded, &o)
		}
		if err != nil {
			summary.FailedLine = line
			summary.DiscardedRecords = len(batch.Observations)
			return summary, fmt.Errorf("line %d: %w", line, err)
		}
		// Parse the next record before flushing on byte capacity, so malformed input
		// never causes the previously pending batch to be submitted unexpectedly.
		if bufferedBytes+len(encoded) > MaxBufferedBytes {
			if err = flush(); err != nil {
				summary.DiscardedRecords++ // the already-parsed next record was never submitted
				return summary, err
			}
		}
		batch.Observations = append(batch.Observations, o)
		bufferedBytes += len(encoded)
		if len(batch.Observations) == batchSize {
			if err = flush(); err != nil {
				return summary, err
			}
		}
	}
	if err = scanner.Err(); err != nil {
		summary.FailedLine = line + 1
		summary.DiscardedRecords = len(batch.Observations)
		return summary, fmt.Errorf("reading line %d (maximum record %d bytes): %w", line+1, MaxRecordBytes, err)
	}
	if err = ctx.Err(); err != nil {
		summary.DiscardedRecords = len(batch.Observations)
		return summary, err
	}
	err = flush()
	return summary, err
}
