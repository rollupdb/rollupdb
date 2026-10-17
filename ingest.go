package rollupdb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
	"unicode/utf8"
)

const MaxBatchSize = 10000
const MaxDimensionValueBytes = 1024
const MaxDimensionTupleBytes = 4096

var ErrInvalidRequest = errors.New("rollupdb: invalid request")
var ErrStreamNotActive = errors.New("rollupdb: stream is not active at the ingestion clock")

// Observation contains event time, one scalar value and the exact declared tuple.
type Observation struct {
	Time       time.Time         `json:"time"`
	Value      float64           `json:"value"`
	Dimensions map[string]string `json:"dimensions"`
}

// UnmarshalJSON distinguishes a missing/null scalar from an explicit zero.
// Malformed JSON observations invalidate the request envelope before ingestion.
func (o *Observation) UnmarshalJSON(encoded []byte) error {
	var raw struct {
		Time       *time.Time        `json:"time"`
		Value      *float64          `json:"value"`
		Dimensions map[string]string `json:"dimensions"`
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	if raw.Time == nil || raw.Value == nil {
		return errors.New("observation requires non-null time and value")
	}
	*o = Observation{Time: *raw.Time, Value: *raw.Value, Dimensions: raw.Dimensions}
	return nil
}

type Batch struct {
	Stream       string        `json:"stream"`
	Version      int           `json:"version"`
	Observations []Observation `json:"observations"`
}

// Counters are committed together with accepted aggregate state.
type Counters struct {
	Accepted           int64 `json:"accepted"`
	RejectedInvalid    int64 `json:"rejected_invalid"`
	RejectedLate       int64 `json:"rejected_late"`
	RejectedGroupLimit int64 `json:"rejected_group_limit"`
}

type Outcome struct {
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

type BatchResult struct {
	Counters
	Outcomes        []Outcome `json:"outcomes"`
	ObservedThrough time.Time `json:"observed_through_inclusive"`
}

type runtimeState struct {
	Watermark, CoverageStart sql.NullInt64
	Counters
}

var earliestTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
var latestTime = time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)

func supportedTime(t time.Time) bool { return !t.Before(earliestTime) && t.Before(latestTime) }

func tupleKey(d Definition, dimensions map[string]string) (string, error) {
	if len(dimensions) != len(d.Dimensions) {
		return "", errors.New("dimensions must exactly match the definition")
	}
	values := make([]string, len(d.Dimensions))
	for i, name := range d.Dimensions {
		value, ok := dimensions[name]
		if !ok || len(value) > MaxDimensionValueBytes || !utf8.ValidString(value) {
			return "", errors.New("invalid dimension value")
		}
		values[i] = value
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	if len(encoded) > MaxDimensionTupleBytes {
		return "", errors.New("dimension tuple exceeds 4096 encoded bytes")
	}
	return string(encoded), nil
}

func readRuntime(ctx context.Context, reader rowReader, name string) (runtimeState, error) {
	var r runtimeState
	err := reader.QueryRowContext(ctx, `SELECT watermark_ns,coverage_start_ns,accepted,rejected_invalid,rejected_late,rejected_group_limit FROM stream_runtime WHERE stream=? AND version=1`, name).Scan(&r.Watermark, &r.CoverageStart, &r.Accepted, &r.RejectedInvalid, &r.RejectedLate, &r.RejectedGroupLimit)
	return r, err
}

func addCounters(a, b Counters) (Counters, error) {
	for _, pair := range [][2]int64{{a.Accepted, b.Accepted}, {a.RejectedInvalid, b.RejectedInvalid}, {a.RejectedLate, b.RejectedLate}, {a.RejectedGroupLimit, b.RejectedGroupLimit}} {
		if pair[0] < 0 || pair[1] < 0 || pair[0] > math.MaxInt64-pair[1] {
			return Counters{}, ErrNumericOverflow
		}
	}
	return Counters{a.Accepted + b.Accepted, a.RejectedInvalid + b.RejectedInvalid, a.RejectedLate + b.RejectedLate, a.RejectedGroupLimit + b.RejectedGroupLimit}, nil
}

type cachedGroup struct {
	exists, dirty bool
	lastSeen      int64
}
type bucketKey struct {
	tuple string
	start int64
}

// Ingest atomically persists a batch's accepted observations and rejection counts.
// Every outcome is ordered like its input. A non-nil error means no successful
// acknowledgment; the returned result is zero. Retrying is new input, not deduped.
func (db *DB) Ingest(ctx context.Context, batch Batch) (BatchResult, error) {
	if !identifier.MatchString(batch.Stream) || batch.Version < 1 || len(batch.Observations) == 0 || len(batch.Observations) > MaxBatchSize {
		return BatchResult{}, fmt.Errorf("%w: use a valid stream, a version of 1 or more and 1..%d observations", ErrInvalidRequest, MaxBatchSize)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return BatchResult{}, ErrClosed
	}
	now := db.now().UTC() // exactly one clock sample per batch
	if !supportedTime(now) {
		return BatchResult{}, fmt.Errorf("%w: clock outside supported range", ErrInvalidRequest)
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return BatchResult{}, err
	}
	defer tx.Rollback()
	d, err := readDefinition(ctx, tx, batch.Stream)
	if err != nil {
		return BatchResult{}, err
	}
	if batch.Version > d.Version {
		return BatchResult{}, fmt.Errorf("%w: stream %q is at version %d", ErrInvalidRequest, d.Name, d.Version)
	}
	if now.Before(d.EffectiveAt) {
		return BatchResult{}, ErrStreamNotActive
	}
	runtime, err := readRuntime(ctx, tx, d.Name)
	if err != nil {
		return BatchResult{}, err
	}
	wallNS := now.UnixNano()
	if err = db.checkClockStep(d.Name, wallNS, runtime.Watermark); err != nil {
		return BatchResult{}, err
	}
	watermark := wallNS
	if runtime.Watermark.Valid && runtime.Watermark.Int64 > watermark {
		watermark = runtime.Watermark.Int64
	}
	width := d.Tiers[0].WidthSeconds * int64(time.Second)
	grace := d.Tiers[0].GraceSeconds * int64(time.Second)
	coverageStart := max(int64(0), watermark-grace) / width * width
	coverageStart = max(coverageStart, d.EffectiveAt.UnixNano())
	if runtime.CoverageStart.Valid {
		coverageStart = runtime.CoverageStart.Int64
	}
	var groupCount int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM dimension_groups WHERE stream=? AND version=1", d.Name).Scan(&groupCount); err != nil {
		return BatchResult{}, err
	}
	groups := make(map[string]*cachedGroup)
	states := make(map[bucketKey]exactState)
	sketches := make(map[bucketKey]*quantileSketch)
	dirty := make(map[bucketKey]bool)
	result := BatchResult{Outcomes: make([]Outcome, len(batch.Observations)), ObservedThrough: time.Unix(0, watermark).UTC()}
	for i, observation := range batch.Observations {
		if err = ctx.Err(); err != nil {
			return BatchResult{}, err
		}
		outcome := Outcome{Status: "rejected_invalid", Reason: "timestamp"}
		rejectInvalid := func(reason string) {
			result.Outcomes[i] = Outcome{Status: "rejected_invalid", Reason: reason}
			result.RejectedInvalid++
		}
		if !supportedTime(observation.Time) || observation.Time.Before(d.EffectiveAt) || observation.Time.After(now) {
			rejectInvalid(outcome.Reason)
			continue
		}
		if !finite(observation.Value) {
			rejectInvalid("non_finite")
			continue
		}
		key, keyErr := tupleKey(d, observation.Dimensions)
		if keyErr != nil {
			rejectInvalid("dimensions")
			continue
		}
		eventNS := observation.Time.UnixNano()
		start := eventNS / width * width
		// Subtract instead of adding a long grace period to an event timestamp;
		// a valid 100-year grace can otherwise overflow signed nanoseconds.
		if watermark-(start+width) >= grace {
			result.Outcomes[i] = Outcome{Status: "rejected_late"}
			result.RejectedLate++
			continue
		}
		group, ok := groups[key]
		if !ok {
			group = &cachedGroup{}
			err = tx.QueryRowContext(ctx, "SELECT last_seen_ns FROM dimension_groups WHERE stream=? AND version=1 AND tuple_key=?", d.Name, key).Scan(&group.lastSeen)
			if err == nil {
				group.exists = true
			} else if !errors.Is(err, sql.ErrNoRows) {
				return BatchResult{}, err
			}
			groups[key] = group
		}
		if !group.exists && groupCount >= d.MaxGroups {
			result.Outcomes[i] = Outcome{Status: "rejected_group_limit"}
			result.RejectedGroupLimit++
			continue
		}
		bk := bucketKey{key, start}
		state, ok := states[bk]
		if !ok {
			var encoded []byte
			err = tx.QueryRowContext(ctx, `SELECT n,sum,sum_comp,min,max,mean,m2,sketch FROM buckets WHERE stream=? AND version=1 AND tuple_key=? AND tier=0 AND start_ns=?`, d.Name, key, start).Scan(&state.N, &state.Sum, &state.SumComp, &state.Min, &state.Max, &state.Mean, &state.M2, &encoded)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return BatchResult{}, err
			}
			if !state.valid() {
				return BatchResult{}, errors.New("rollupdb: invalid persisted statistical state")
			}
			if d.Sketch != nil {
				var sketch *quantileSketch
				switch {
				case state.N == 0 && encoded == nil:
					sketch, err = newQuantileSketch(*d.Sketch)
				case encoded == nil && d.sketchOptional(start):
					// The bucket predates the stream's sketch. Its distribution
					// is unknown, so it stays exact-only rather than holding a
					// sketch of only the newer readings.
				case encoded == nil:
					err = fmt.Errorf("%w: bucket has no sketch", ErrSketchEncoding)
				default:
					sketch, err = decodeQuantileSketch(encoded, *d.Sketch, state)
				}
				if err != nil {
					return BatchResult{}, err
				}
				sketches[bk] = sketch
			} else if encoded != nil {
				return BatchResult{}, ErrSketchEncoding
			}
		}
		next, stateErr := state.add(observation.Value)
		if stateErr != nil {
			rejectInvalid("numeric_overflow")
			continue
		}
		if sketch := sketches[bk]; sketch != nil {
			sketch.update(observation.Value)
		}
		if !group.exists {
			group.exists = true
			groupCount++
		}
		group.dirty = true
		group.lastSeen = max(group.lastSeen, eventNS)
		states[bk] = next
		dirty[bk] = true
		result.Outcomes[i] = Outcome{Status: "accepted"}
		result.Accepted++
	}
	for key, group := range groups {
		if !group.dirty {
			continue
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO dimension_groups(stream,version,tuple_key,last_seen_ns) VALUES(?,1,?,?) ON CONFLICT(stream,version,tuple_key) DO UPDATE SET last_seen_ns=excluded.last_seen_ns`, d.Name, key, group.lastSeen); err != nil {
			return BatchResult{}, err
		}
	}
	for key := range dirty {
		s := states[key]
		var encoded []byte
		if sketch := sketches[key]; d.Sketch != nil && sketch != nil {
			encoded, err = sketch.encode()
			if err != nil {
				return BatchResult{}, err
			}
			if err = validateKLLPayload(encoded[sketchHeaderBytes:], *d.Sketch, s); err != nil {
				return BatchResult{}, err
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO buckets(stream,version,tuple_key,tier,start_ns,n,sum,sum_comp,min,max,mean,m2,sketch) VALUES(?,1,?,0,?,?,?,?,?,?,?,?,?) ON CONFLICT(stream,version,tuple_key,tier,start_ns) DO UPDATE SET n=excluded.n,sum=excluded.sum,sum_comp=excluded.sum_comp,min=excluded.min,max=excluded.max,mean=excluded.mean,m2=excluded.m2,sketch=excluded.sketch`, d.Name, key.tuple, key.start, s.N, s.Sum, s.SumComp, s.Min, s.Max, s.Mean, s.M2, encoded); err != nil {
			return BatchResult{}, err
		}
	}
	totals, err := addCounters(runtime.Counters, result.Counters)
	if err != nil {
		return BatchResult{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE stream_runtime SET watermark_ns=?,coverage_start_ns=?,accepted=?,rejected_invalid=?,rejected_late=?,rejected_group_limit=? WHERE stream=? AND version=1`, watermark, coverageStart, totals.Accepted, totals.RejectedInvalid, totals.RejectedLate, totals.RejectedGroupLimit, d.Name); err != nil {
		return BatchResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return BatchResult{}, err
	}
	return result, nil
}

type StreamStatus struct {
	Frontiers []TierFrontier `json:"frontiers"`
	Counters
	Groups          int        `json:"groups"`
	Buckets         int        `json:"buckets"`
	CoverageStart   *time.Time `json:"coverage_start"`
	ObservedThrough *time.Time `json:"observed_through_inclusive"`
}

// Status returns committed operational counters and coverage, not raw events.
func (db *DB) Status(ctx context.Context, name string) (StreamStatus, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return StreamStatus{}, ErrClosed
	}
	tx, err := db.sql.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return StreamStatus{}, err
	}
	defer tx.Rollback()
	d, err := readDefinition(ctx, tx, name)
	if err != nil {
		return StreamStatus{}, err
	}
	r, err := readRuntime(ctx, tx, name)
	if err != nil {
		return StreamStatus{}, err
	}
	f, err := readFrontiers(ctx, tx, d)
	if err != nil {
		return StreamStatus{}, err
	}
	s := StreamStatus{Counters: r.Counters, Frontiers: publicFrontiers(f)}
	if r.CoverageStart.Valid {
		t := time.Unix(0, r.CoverageStart.Int64).UTC()
		s.CoverageStart = &t
	}
	if r.Watermark.Valid {
		t := time.Unix(0, r.Watermark.Int64).UTC()
		s.ObservedThrough = &t
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM dimension_groups WHERE stream=? AND version=1", name).Scan(&s.Groups); err != nil {
		return s, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM buckets WHERE stream=? AND version=1", name).Scan(&s.Buckets); err != nil {
		return s, err
	}
	return s, tx.Commit()
}
