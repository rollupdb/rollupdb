package rollupdb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

var ErrPartialBucket = errors.New("rollupdb: requested boundaries cut a retained bucket")

type Query struct {
	Stream    string            `json:"stream"`
	Version   int               `json:"version"`
	From      time.Time         `json:"from"`
	To        time.Time         `json:"to"`
	Filter    map[string]string `json:"filter,omitempty"`
	Quantiles []float64         `json:"quantiles,omitempty"`
}

// UnmarshalJSON keeps a JSON null quantile from silently becoming rank zero.
func (q *Query) UnmarshalJSON(encoded []byte) error {
	var raw struct {
		Stream    string            `json:"stream"`
		Version   int               `json:"version"`
		From      time.Time         `json:"from"`
		To        time.Time         `json:"to"`
		Filter    map[string]string `json:"filter,omitempty"`
		Quantiles []json.RawMessage `json:"quantiles,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	next := Query{Stream: raw.Stream, Version: raw.Version, From: raw.From, To: raw.To, Filter: raw.Filter}
	for _, value := range raw.Quantiles {
		var rank *float64
		if err := json.Unmarshal(value, &rank); err != nil {
			return err
		}
		if rank == nil {
			return fmt.Errorf("%w: null quantile", ErrInvalidRequest)
		}
		next.Quantiles = append(next.Quantiles, *rank)
	}
	*q = next
	return nil
}

type Interval struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}
type CoverageGap struct {
	Interval
	Reason string `json:"reason"`
}

type QueryResult struct {
	Requested           Interval           `json:"requested"`
	Covered             *Interval          `json:"covered"`
	Missing             []CoverageGap      `json:"missing"`
	ResolutionSeconds   int64              `json:"resolution_seconds"`
	DefinitionVersion   int                `json:"definition_version"`
	AvailableStatistics []string           `json:"available_statistics"`
	Statistics          map[string]any     `json:"statistics"`
	Complete            bool               `json:"complete"`
	BucketsMerged       int                `json:"buckets_merged"`
	Quantiles           []QuantileEstimate `json:"quantiles,omitempty"`
	QuantileAccuracy    *QuantileAccuracy  `json:"quantile_accuracy,omitempty"`
}

// Query combines the finest retained tier over aligned boundaries and declared
// dimension filters. It reports absent collection coverage separately from zero
// observations. The current tier is never resampled or split proportionally.
func (db *DB) Query(ctx context.Context, q Query) (QueryResult, error) {
	if !identifier.MatchString(q.Stream) || q.Version != 1 || !supportedTime(q.From) || q.To.After(latestTime) || !q.From.Before(q.To) {
		return QueryResult{}, ErrInvalidRequest
	}
	if len(q.Quantiles) > 32 {
		return QueryResult{}, ErrInvalidRequest
	}
	seenRanks := make(map[float64]bool)
	for _, rank := range q.Quantiles {
		if !finite(rank) || rank < 0 || rank > 1 || seenRanks[rank] {
			return QueryResult{}, ErrInvalidRequest
		}
		seenRanks[rank] = true
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return QueryResult{}, ErrClosed
	}
	tx, err := db.sql.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return QueryResult{}, err
	}
	defer tx.Rollback()
	d, err := readDefinition(ctx, tx, q.Stream)
	if err != nil {
		return QueryResult{}, err
	}
	width := d.Tiers[0].WidthSeconds * int64(time.Second)
	if q.From.UnixNano()%width != 0 || q.To.UnixNano()%width != 0 {
		return QueryResult{}, ErrPartialBucket
	}
	filterIndices := make(map[int]string)
	for name, value := range q.Filter {
		index := -1
		for i, dimension := range d.Dimensions {
			if name == dimension {
				index = i
				break
			}
		}
		if index < 0 || len(value) > MaxDimensionValueBytes || !utf8.ValidString(value) {
			return QueryResult{}, fmt.Errorf("%w: unknown dimension or invalid filter %q", ErrInvalidRequest, name)
		}
		filterIndices[index] = value
	}
	runtime, err := readRuntime(ctx, tx, d.Name)
	if err != nil {
		return QueryResult{}, err
	}
	r := QueryResult{Requested: Interval{q.From.UTC(), q.To.UTC()}, Missing: []CoverageGap{}, ResolutionSeconds: d.Tiers[0].WidthSeconds, DefinitionVersion: 1, AvailableStatistics: append([]string(nil), d.Statistics...)}
	if d.Sketch != nil {
		r.AvailableStatistics = append(r.AvailableStatistics, "quantiles")
	}
	start, end := q.From.UnixNano(), q.To.UnixNano()
	coverageFrom, coverageTo := end, start
	if runtime.CoverageStart.Valid && runtime.Watermark.Valid {
		coverageFrom = max(start, runtime.CoverageStart.Int64)
		coverageTo = min(end, runtime.Watermark.Int64+1)
	}
	covered := coverageFrom < coverageTo
	if covered {
		r.Covered = &Interval{time.Unix(0, coverageFrom).UTC(), time.Unix(0, coverageTo).UTC()}
		if start < coverageFrom {
			r.Missing = append(r.Missing, CoverageGap{Interval{q.From.UTC(), time.Unix(0, coverageFrom).UTC()}, "not_collected"})
		}
		if coverageTo < end {
			r.Missing = append(r.Missing, CoverageGap{Interval{time.Unix(0, coverageTo).UTC(), q.To.UTC()}, "not_observed_yet"})
		}
	} else {
		reason := "not_collected"
		if runtime.Watermark.Valid && start > runtime.Watermark.Int64 {
			reason = "not_observed_yet"
		}
		r.Missing = append(r.Missing, CoverageGap{r.Requested, reason})
	}
	r.Complete = covered && coverageFrom == start && coverageTo == end
	state := exactState{}
	var sketch *quantileSketch
	needSketch := len(q.Quantiles) > 0 && d.Sketch != nil
	rows, err := tx.QueryContext(ctx, `SELECT tuple_key,n,sum,sum_comp,min,max,mean,m2,sketch FROM buckets WHERE stream=? AND version=1 AND tier=0 AND start_ns>=? AND start_ns<? ORDER BY start_ns,tuple_key`, d.Name, start, end)
	if err != nil {
		return QueryResult{}, err
	}
	for rows.Next() {
		var key string
		var b exactState
		var encoded []byte
		if err = rows.Scan(&key, &b.N, &b.Sum, &b.SumComp, &b.Min, &b.Max, &b.Mean, &b.M2, &encoded); err != nil {
			break
		}
		var values []string
		if err = json.Unmarshal([]byte(key), &values); err != nil {
			break
		}
		if len(values) != len(d.Dimensions) {
			err = errors.New("rollupdb: corrupt dimension tuple")
			break
		}
		match := true
		for i, value := range filterIndices {
			if values[i] != value {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		if !covered {
			err = errors.New("rollupdb: bucket outside recorded coverage")
			break
		}
		if needSketch {
			var next *quantileSketch
			next, err = decodeQuantileSketch(encoded, *d.Sketch, b)
			if err != nil {
				break
			}
			if sketch == nil {
				sketch = next
			} else {
				if err = sketch.merge(next); err != nil {
					break
				}
			}
		}
		state, err = state.merge(b)
		if err != nil {
			break
		}
		r.BucketsMerged++
	}
	if err == nil {
		err = rows.Err()
	}
	closeErr := rows.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return QueryResult{}, err
	}
	r.Statistics = state.statistics(d.Statistics, covered)
	r.Quantiles, r.QuantileAccuracy, err = quantileAnswers(q.Quantiles, d.Sketch, sketch, state, covered)
	if err != nil {
		return QueryResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return QueryResult{}, err
	}
	return r, nil
}
