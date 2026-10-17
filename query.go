package rollupdb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

// ResolutionInterval describes an authoritative portion of collected, retained
// history, including covered intervals with no bucket rows.
type ResolutionInterval struct {
	Interval
	Tier         int   `json:"tier"`
	WidthSeconds int64 `json:"width_seconds"`
}

type QueryResult struct {
	Resolutions         []ResolutionInterval `json:"resolutions"`
	Requested           Interval             `json:"requested"`
	Covered             *Interval            `json:"covered"`
	Missing             []CoverageGap        `json:"missing"`
	ResolutionSeconds   int64                `json:"resolution_seconds"`
	DefinitionVersion   int                  `json:"definition_version"`
	AvailableStatistics []string             `json:"available_statistics"`
	Statistics          map[string]any       `json:"statistics"`
	Complete            bool                 `json:"complete"`
	BucketsMerged       int                  `json:"buckets_merged"`
	Quantiles           []QuantileEstimate   `json:"quantiles,omitempty"`
	QuantileAccuracy    *QuantileAccuracy    `json:"quantile_accuracy,omitempty"`
}

// Query combines disjoint retained tiers over aligned boundaries and declared
// dimension filters. It reports absent collection coverage separately from zero
// observations. The current tier is never resampled or split proportionally.
func (db *DB) Query(ctx context.Context, q Query) (QueryResult, error) {
	if !identifier.MatchString(q.Stream) || q.Version < 1 || !supportedTime(q.From) || q.To.After(latestTime) || !q.From.Before(q.To) {
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
	if q.Version > d.Version {
		return QueryResult{}, fmt.Errorf("%w: stream %q is at version %d", ErrInvalidRequest, d.Name, d.Version)
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
	r := QueryResult{Requested: Interval{q.From.UTC(), q.To.UTC()}, Missing: []CoverageGap{}, ResolutionSeconds: 0, DefinitionVersion: d.Version, AvailableStatistics: append([]string(nil), d.Statistics...)}
	if d.Sketch != nil {
		r.AvailableStatistics = append(r.AvailableStatistics, "quantiles")
	}
	start, end := q.From.UnixNano(), q.To.UnixNano()
	frontiers, err := readFrontiers(ctx, tx, d)
	if err != nil {
		return QueryResult{}, err
	}
	expiredThrough := frontiers[len(frontiers)-1]
	coverageFrom, coverageTo := end, start
	cuts := []int64{start, end}
	for _, n := range []sql.NullInt64{runtime.CoverageStart, {Int64: expiredThrough, Valid: true}, {Int64: runtime.Watermark.Int64 + 1, Valid: runtime.Watermark.Valid}} {
		if n.Valid && n.Int64 > start && n.Int64 < end {
			cuts = append(cuts, n.Int64)
		}
	}
	sort.Slice(cuts, func(i, j int) bool { return cuts[i] < cuts[j] })
	for i := 1; i < len(cuts); i++ {
		a, b := cuts[i-1], cuts[i]
		if a == b {
			continue
		}
		reason := ""
		switch {
		case !runtime.CoverageStart.Valid || !runtime.Watermark.Valid:
			reason = "not_collected"
			if runtime.Watermark.Valid && a > runtime.Watermark.Int64 {
				reason = "not_observed_yet"
			}
		case a < runtime.CoverageStart.Int64:
			reason = "not_collected"
		case a > runtime.Watermark.Int64:
			reason = "not_observed_yet"
		case a < expiredThrough:
			reason = "expired"
		}
		if reason == "" {
			coverageFrom = min(coverageFrom, a)
			coverageTo = max(coverageTo, b)
			continue
		}
		gap := CoverageGap{Interval{time.Unix(0, a).UTC(), time.Unix(0, b).UTC()}, reason}
		if len(r.Missing) > 0 && r.Missing[len(r.Missing)-1].Reason == reason && r.Missing[len(r.Missing)-1].To.Equal(gap.From) {
			r.Missing[len(r.Missing)-1].To = gap.To
		} else {
			r.Missing = append(r.Missing, gap)
		}
	}
	covered := coverageFrom < coverageTo
	r.Resolutions = []ResolutionInterval{}
	if covered {
		r.Covered = &Interval{time.Unix(0, coverageFrom).UTC(), time.Unix(0, coverageTo).UTC()}
		// Ownership bands are chronological from coarsest to finest. The adjacent
		// frontier is aligned to this tier's width, including across empty history.
		for tier := len(d.Tiers) - 1; tier >= 0; tier-- {
			lo, hi := frontiers[tier], latestTime.UnixNano()
			if tier > 0 {
				hi = frontiers[tier-1]
			}
			a, b := max(lo, coverageFrom), min(hi, coverageTo)
			if a >= b {
				continue
			}
			width := d.Tiers[tier].WidthSeconds * int64(time.Second)
			bucketFrom := a / width * width
			bucketTo := (b-1)/width*width + width
			if start > bucketFrom || end < bucketTo {
				return QueryResult{}, ErrPartialBucket
			}
			r.Resolutions = append(r.Resolutions, ResolutionInterval{Interval{time.Unix(0, a).UTC(), time.Unix(0, b).UTC()}, tier, d.Tiers[tier].WidthSeconds})
			r.ResolutionSeconds = max(r.ResolutionSeconds, d.Tiers[tier].WidthSeconds)
		}
	}
	r.Complete = covered && coverageFrom == start && coverageTo == end
	state := exactState{}
	var sketch *quantileSketch
	needSketch := len(q.Quantiles) > 0 && d.Sketch != nil
	// A bucket from before the stream's sketch was added has none; percentiles
	// over a range that includes one aren't available.
	sketchMissing := false
	for _, band := range r.Resolutions {
		bandWidth := band.WidthSeconds * int64(time.Second)
		rows, err := tx.QueryContext(ctx, `SELECT tuple_key,start_ns,n,sum,sum_comp,min,max,mean,m2,sketch FROM buckets WHERE stream=? AND version=1 AND tier=? AND start_ns>=? AND start_ns<? ORDER BY start_ns,tuple_key`, d.Name, band.Tier, band.From.UnixNano()/bandWidth*bandWidth, band.To.UnixNano())
		if err != nil {
			return QueryResult{}, err
		}
		for rows.Next() {
			var key string
			var startNS int64
			var b exactState
			var encoded []byte
			if err = rows.Scan(&key, &startNS, &b.N, &b.Sum, &b.SumComp, &b.Min, &b.Max, &b.Mean, &b.M2, &encoded); err != nil {
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
			if needSketch && encoded == nil && d.sketchOptional(startNS) {
				sketchMissing = true
			} else if needSketch && !sketchMissing {
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
	}
	r.Statistics = state.statistics(d.Statistics, covered)
	if sketchMissing {
		r.Quantiles, r.QuantileAccuracy, err = quantileAnswers(q.Quantiles, nil, nil, state, covered)
	} else {
		r.Quantiles, r.QuantileAccuracy, err = quantileAnswers(q.Quantiles, d.Sketch, sketch, state, covered)
	}
	if err != nil {
		return QueryResult{}, err
	}
	if !covered && r.QuantileAccuracy != nil && d.Sketch != nil && len(r.Missing) == 1 && r.Missing[0].Reason == "expired" {
		r.QuantileAccuracy.Availability = "expired"
	}
	if err = tx.Commit(); err != nil {
		return QueryResult{}, err
	}
	return r, nil
}
