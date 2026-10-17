package rollupdb

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Streams returns the names of every stream in the database, sorted.
func (db *DB) Streams(ctx context.Context) ([]string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	rows, err := db.sql.QueryContext(ctx, "SELECT DISTINCT name FROM stream_definitions ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// AdvanceAll runs Advance for every stream, each in its own transaction, so
// one stream's failure doesn't undo another's rollup. Streams that aren't
// active yet are skipped. The results hold every stream that advanced, even
// when others failed; the error joins the failures, each naming its stream.
func (db *DB) AdvanceAll(ctx context.Context) (map[string]AdvanceResult, error) {
	results := map[string]AdvanceResult{}
	names, err := db.Streams(ctx)
	if err != nil {
		return results, err
	}
	var failures []error
	for _, name := range names {
		if err = ctx.Err(); err != nil {
			return results, errors.Join(append(failures, err)...)
		}
		r, err := db.Advance(ctx, name)
		switch {
		case errors.Is(err, ErrStreamNotActive):
			continue
		case err != nil:
			failures = append(failures, fmt.Errorf("stream %q: %w", name, err))
		default:
			results[name] = r
		}
	}
	return results, errors.Join(failures...)
}

// RetentionReport is one stream's outcome from a scheduled rollup.
type RetentionReport struct {
	Stream string
	Result AdvanceResult
	Err    error
}

// RunRetention runs AdvanceAll at once and then every interval until ctx is
// done, which is when it returns. report, if not nil, gets one call per
// stream that did work (promoted, expired or reclaimed something) or failed.
// A failure doesn't stop the schedule; the next run tries again.
func (db *DB) RunRetention(ctx context.Context, every time.Duration, report func(RetentionReport)) error {
	if every <= 0 {
		return errors.New("rollupdb: retention interval must be positive")
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		results, err := db.AdvanceAll(ctx)
		if errors.Is(err, ErrClosed) {
			return nil
		}
		// Streams that advanced before ctx ended are still reported; the
		// error from being cancelled is not.
		if ctx.Err() != nil {
			err = nil
		}
		if report != nil {
			for name, r := range results {
				if r.SourceBuckets+r.ParentBuckets+r.ExpiredBuckets+r.ReclaimedGroups > 0 {
					report(RetentionReport{Stream: name, Result: r})
				}
			}
			if err != nil {
				report(RetentionReport{Err: err})
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
