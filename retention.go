package rollupdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// TierFrontier marks a completed half-open prefix from the definition's start.
// For nonfinal tiers it is promoted through; for the final tier it is expired
// through. The final forever tier stays at the definition's effective time.
type TierFrontier struct {
	Tier    int       `json:"tier"`
	Through time.Time `json:"through_exclusive"`
}

// advanceBeforeCommitKey is an internal test seam for interrupting an open
// retention transaction. Production callers never supply this context key.
type advanceBeforeCommitKey struct{}

type AdvanceResult struct {
	ObservedThrough time.Time      `json:"observed_through_inclusive"`
	Frontiers       []TierFrontier `json:"frontiers"`
	SourceBuckets   int64          `json:"source_buckets_removed"`
	ParentBuckets   int64          `json:"parent_buckets_created"`
	ExpiredBuckets  int64          `json:"expired_buckets"`
	ReclaimedGroups int64          `json:"reclaimed_groups"`
}

// readFrontiers treats no rows as the pre-advancement state of new or migrated
// streams. A partially present set is corruption, not a reset of progress.
func readFrontiers(ctx context.Context, tx *sql.Tx, d Definition) ([]int64, error) {
	f := make([]int64, len(d.Tiers))
	for i := range f {
		f[i] = d.EffectiveAt.UnixNano()
	}
	rows, err := tx.QueryContext(ctx, `SELECT tier,through_ns FROM retention_frontiers WHERE stream=? AND version=1 ORDER BY tier`, d.Name)
	if err != nil {
		return nil, err
	}
	count := 0
	for rows.Next() {
		var tier int
		var n int64
		if err = rows.Scan(&tier, &n); err != nil {
			break
		}
		if tier != count || tier >= len(f) {
			err = errors.New("rollupdb: invalid retention frontier set")
			break
		}
		width := d.Tiers[tier].WidthSeconds * int64(time.Second)
		if tier+1 < len(f) {
			width = d.Tiers[tier+1].WidthSeconds * int64(time.Second)
		}
		if n < d.EffectiveAt.UnixNano() || n >= latestTime.UnixNano() || n%width != 0 || (tier > 0 && n > f[tier-1]) || (tier == len(f)-1 && d.Tiers[tier].RetainSeconds == 0 && n != d.EffectiveAt.UnixNano()) {
			err = errors.New("rollupdb: invalid retention frontier")
			break
		}
		f[tier] = n
		count++
	}
	if err == nil {
		err = rows.Err()
	}
	closeErr := rows.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	if count != 0 && count != len(f) {
		return nil, errors.New("rollupdb: incomplete retention frontier set")
	}
	return f, nil
}

func publicFrontiers(f []int64) []TierFrontier {
	out := make([]TierFrontier, len(f))
	for i, n := range f {
		out[i] = TierFrontier{i, time.Unix(0, n).UTC()}
	}
	return out
}

// Advance explicitly runs retention for one stream. Promotion, deletion,
// frontiers and the nondecreasing clock commit as one transaction. Work visits
// populated parent intervals and groups, never every second of empty history.
// It extends existing recorder coverage but does not invent a collection start.
func (db *DB) Advance(ctx context.Context, name string) (AdvanceResult, error) {
	if !identifier.MatchString(name) {
		return AdvanceResult{}, ErrInvalidRequest
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return AdvanceResult{}, ErrClosed
	}
	now := db.now().UTC()
	if !supportedTime(now) {
		return AdvanceResult{}, ErrInvalidRequest
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return AdvanceResult{}, err
	}
	defer tx.Rollback()
	d, err := readDefinition(ctx, tx, name)
	if err != nil {
		return AdvanceResult{}, err
	}
	if now.Before(d.EffectiveAt) {
		return AdvanceResult{}, ErrStreamNotActive
	}
	runtime, err := readRuntime(ctx, tx, name)
	if err != nil {
		return AdvanceResult{}, err
	}
	if err = db.checkClockStep(d.Name, now.UnixNano(), runtime.Watermark); err != nil {
		return AdvanceResult{}, err
	}
	watermark := now.UnixNano()
	if runtime.Watermark.Valid {
		watermark = max(watermark, runtime.Watermark.Int64)
	}
	f, err := readFrontiers(ctx, tx, d)
	if err != nil {
		return AdvanceResult{}, err
	}
	r := AdvanceResult{ObservedThrough: time.Unix(0, watermark).UTC()}
	for tier, policy := range d.Tiers {
		if policy.RetainSeconds == 0 {
			continue
		}
		width := policy.WidthSeconds * int64(time.Second)
		if tier+1 < len(f) {
			width = d.Tiers[tier+1].WidthSeconds * int64(time.Second)
		}
		// Subtraction is safe even with the maximum policy near the supported end.
		limit := max(int64(0), watermark-max(policy.RetainSeconds, policy.GraceSeconds)*int64(time.Second)) / width * width
		if tier > 0 {
			limit = min(limit, f[tier-1]/width*width)
		}
		limit = max(f[tier], limit)
		if tier+1 < len(f) {
			for {
				if err = ctx.Err(); err != nil {
					return AdvanceResult{}, err
				}
				var first int64
				err = tx.QueryRowContext(ctx, `SELECT start_ns FROM buckets WHERE stream=? AND version=1 AND tier=? AND start_ns>=? AND start_ns<? ORDER BY start_ns LIMIT 1`, name, tier, f[tier], limit).Scan(&first)
				if errors.Is(err, sql.ErrNoRows) {
					break
				}
				if err != nil {
					return AdvanceResult{}, err
				}
				parent := first / width * width
				if err = promoteParent(ctx, tx, d, tier, parent, width, &r); err != nil {
					return AdvanceResult{}, err
				}
			}
		} else {
			result, e := tx.ExecContext(ctx, `DELETE FROM buckets WHERE stream=? AND version=1 AND tier=? AND start_ns>=? AND start_ns<?`, name, tier, f[tier], limit)
			if e != nil {
				return AdvanceResult{}, e
			}
			r.ExpiredBuckets, err = result.RowsAffected()
			if err != nil {
				return AdvanceResult{}, err
			}
		}
		f[tier] = limit
	}
	for tier, n := range f {
		if _, err = tx.ExecContext(ctx, `INSERT INTO retention_frontiers VALUES(?,1,?,?) ON CONFLICT(stream,version,tier) DO UPDATE SET through_ns=excluded.through_ns`, name, tier, n); err != nil {
			return AdvanceResult{}, err
		}
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM dimension_groups WHERE stream=? AND version=1 AND NOT EXISTS(SELECT 1 FROM buckets b WHERE b.stream=dimension_groups.stream AND b.version=dimension_groups.version AND b.tuple_key=dimension_groups.tuple_key)`, name)
	if err != nil {
		return AdvanceResult{}, err
	}
	r.ReclaimedGroups, err = result.RowsAffected()
	if err != nil {
		return AdvanceResult{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE stream_runtime SET watermark_ns=? WHERE stream=? AND version=1`, watermark, name); err != nil {
		return AdvanceResult{}, err
	}
	r.Frontiers = publicFrontiers(f)
	// Tests can pause the owner after every retention write but before Commit.
	// The callback must not use this DB handle (its mutex is held).
	if hook, ok := ctx.Value(advanceBeforeCommitKey{}).(func()); ok {
		hook()
	}
	if err = tx.Commit(); err != nil {
		return AdvanceResult{}, err
	}
	return r, nil
}

// One group is merged at a time, bounding working memory by one sketch and the
// nested child width. Cursors close before any write. Parent INSERT deliberately
// refuses existing targets: whole-parent ownership never permits accumulation
// into an already published parent.
func promoteParent(ctx context.Context, tx *sql.Tx, d Definition, tier int, parent, width int64, r *AdvanceResult) error {
	after := ""
	for {
		var key string
		err := tx.QueryRowContext(ctx, `SELECT tuple_key FROM buckets WHERE stream=? AND version=1 AND tier=? AND start_ns>=? AND start_ns<? AND tuple_key>? ORDER BY tuple_key LIMIT 1`, d.Name, tier, parent, parent+width, after).Scan(&key)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT n,sum,sum_comp,min,max,mean,m2,sketch FROM buckets WHERE stream=? AND version=1 AND tuple_key=? AND tier=? AND start_ns>=? AND start_ns<? ORDER BY start_ns`, d.Name, key, tier, parent, parent+width)
		if err != nil {
			return err
		}
		var state exactState
		var sketch *quantileSketch
		var count int64
		// A source from before the stream's sketch was added has none, so the
		// parent can't have a complete one either.
		sketchMissing := false
		for rows.Next() {
			var b exactState
			var encoded []byte
			if err = rows.Scan(&b.N, &b.Sum, &b.SumComp, &b.Min, &b.Max, &b.Mean, &b.M2, &encoded); err != nil {
				break
			}
			if b.N <= 0 || !b.valid() {
				err = errors.New("rollupdb: invalid persisted statistical state")
				break
			}
			if d.Sketch != nil && encoded == nil {
				sketchMissing = true
			} else if d.Sketch != nil && !sketchMissing {
				var next *quantileSketch
				next, err = decodeQuantileSketch(encoded, *d.Sketch, b)
				if err != nil {
					break
				}
				if sketch == nil {
					sketch = next
				} else if err = sketch.merge(next); err != nil {
					break
				}
			} else if encoded != nil {
				err = ErrSketchEncoding
				break
			}
			state, err = state.merge(b)
			if err != nil {
				break
			}
			count++
		}
		if err == nil {
			err = rows.Err()
		}
		closeErr := rows.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		var encoded []byte
		if sketch != nil && !sketchMissing {
			encoded, err = sketch.encode()
			if err != nil {
				return err
			}
			if err = validateKLLPayload(encoded[sketchHeaderBytes:], *d.Sketch, state); err != nil {
				return err
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO buckets(stream,version,tuple_key,tier,start_ns,n,sum,sum_comp,min,max,mean,m2,sketch) VALUES(?,1,?,?,?,?,?,?,?,?,?,?,?)`, d.Name, key, tier+1, parent, state.N, state.Sum, state.SumComp, state.Min, state.Max, state.Mean, state.M2, encoded); err != nil {
			return fmt.Errorf("promote parent: %w", err)
		}
		r.SourceBuckets += count
		r.ParentBuckets++
		after = key
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM buckets WHERE stream=? AND version=1 AND tier=? AND start_ns>=? AND start_ns<?`, d.Name, tier, parent, parent+width)
	return err
}
