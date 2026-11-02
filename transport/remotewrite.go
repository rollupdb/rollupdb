package transport

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/rollupdb/rollupdb"
)

// MaxRemoteWriteBody caps a compressed remote-write request; MaxRemoteWriteDecoded
// caps it after snappy. Prometheus sends far smaller requests by default.
const MaxRemoteWriteBody = 16 << 20
const MaxRemoteWriteDecoded = 64 << 20

// MaxRemoteWriteSeries and MaxRemoteWriteSamples cap one request after
// decoding; Prometheus sends at most a few thousand samples per request by
// default. Above either cap the request is refused with 400.
const MaxRemoteWriteSeries = 1_000_000
const MaxRemoteWriteSamples = 1_000_000

// maxRemoteWriteInFlight bounds how many requests decode at once, and so the
// memory they can take together; others wait their turn.
const maxRemoteWriteInFlight = 2

// RemoteWriteRoute is where Prometheus remote write 1.0 posts.
const RemoteWriteRoute = "POST /api/v1/write"

// RemoteWriteMap says which Prometheus metrics go to which streams. Each
// stream dimension takes the value of one label; a missing label is "".
type RemoteWriteMap struct {
	Metrics []RemoteWriteMetric `json:"metrics"`
}

type RemoteWriteMetric struct {
	// Metric is the series' __name__.
	Metric string `json:"metric"`
	Stream string `json:"stream"`
	// Dimensions maps each stream dimension to the label that fills it.
	Dimensions map[string]string `json:"dimensions"`
}

// RemoteWriteStats counts what the endpoint has done since the server started.
type RemoteWriteStats struct {
	Requests        int64 `json:"requests"`
	Samples         int64 `json:"samples"`
	Submitted       int64 `json:"submitted"`
	Unmapped        int64 `json:"unmapped"`
	StaleMarkers    int64 `json:"stale_markers"`
	Inactive        int64 `json:"inactive"`
	ReplayedBatches int64 `json:"replayed_batches"`
	rollupdb.Counters
	BadRequests    int64 `json:"bad_requests"`
	FailedRequests int64 `json:"failed_requests"`
}

// LoadRemoteWriteMap reads a map file and checks it against the streams in db.
func LoadRemoteWriteMap(ctx context.Context, db *rollupdb.DB, path string) (*RemoteWriteMap, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m RemoteWriteMap
	if err = DecodeObject(encoded, &m); err != nil {
		return nil, fmt.Errorf("remote-write map: %w", err)
	}
	if err = m.check(ctx, db); err != nil {
		return nil, err
	}
	return &m, nil
}

func (m *RemoteWriteMap) check(ctx context.Context, db *rollupdb.DB) error {
	if len(m.Metrics) == 0 {
		return errors.New("remote-write map: no metrics")
	}
	seen := map[string]bool{}
	for _, metric := range m.Metrics {
		if metric.Metric == "" {
			return errors.New("remote-write map: a metric has no name")
		}
		if seen[metric.Metric] {
			return fmt.Errorf("remote-write map: metric %q listed twice", metric.Metric)
		}
		seen[metric.Metric] = true
		d, err := db.Stream(ctx, metric.Stream)
		if err != nil {
			return fmt.Errorf("remote-write map: metric %q: stream %q: %w", metric.Metric, metric.Stream, err)
		}
		if len(metric.Dimensions) != len(d.Dimensions) {
			return fmt.Errorf("remote-write map: metric %q must map exactly the dimensions of stream %q: %v", metric.Metric, d.Name, d.Dimensions)
		}
		for _, name := range d.Dimensions {
			if metric.Dimensions[name] == "" {
				return fmt.Errorf("remote-write map: metric %q gives no label for dimension %q", metric.Metric, name)
			}
		}
	}
	return nil
}

// staleNaN is the bit pattern Prometheus writes to mark a series stale.
const staleNaN = 0x7ff0000000000002

type remoteWrite struct {
	db      *rollupdb.DB
	metrics map[string]RemoteWriteMetric
	keep    map[string]bool // labels worth decoding: __name__ and every mapped label
	mu      sync.Mutex
	stats   RemoteWriteStats
	slots   chan struct{}
}

func newRemoteWrite(db *rollupdb.DB, m *RemoteWriteMap) *remoteWrite {
	rw := &remoteWrite{db: db, metrics: map[string]RemoteWriteMetric{}, slots: make(chan struct{}, maxRemoteWriteInFlight)}
	rw.keep = map[string]bool{"__name__": true}
	for _, metric := range m.Metrics {
		rw.metrics[metric.Metric] = metric
		for _, label := range metric.Dimensions {
			rw.keep[label] = true
		}
	}
	return rw
}

func (rw *remoteWrite) count(f func(*RemoteWriteStats)) {
	rw.mu.Lock()
	f(&rw.stats)
	rw.mu.Unlock()
}

func (rw *remoteWrite) snapshot() RemoteWriteStats {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.stats
}

// fail answers with a plain-text error, as Prometheus expects; 4xx means "do
// not retry", 5xx means "retry".
func (rw *remoteWrite) fail(w http.ResponseWriter, status int, message string) {
	rw.count(func(s *RemoteWriteStats) {
		if status >= 500 {
			s.FailedRequests++
		} else {
			s.BadRequests++
		}
	})
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, message+"\n")
}

func (rw *remoteWrite) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rw.count(func(s *RemoteWriteStats) { s.Requests++ })
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/x-protobuf" {
		rw.fail(w, 415, "Content-Type must be application/x-protobuf")
		return
	}
	if proto, ok := params["proto"]; ok && proto != "prometheus.WriteRequest" {
		rw.fail(w, 415, "only remote write 1.0 (prometheus.WriteRequest) is supported")
		return
	}
	if r.Header.Get("Content-Encoding") != "snappy" {
		rw.fail(w, 415, "Content-Encoding must be snappy")
		return
	}
	select {
	case rw.slots <- struct{}{}:
		defer func() { <-rw.slots }()
	case <-r.Context().Done():
		rw.fail(w, 503, "busy")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxRemoteWriteBody)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var size *http.MaxBytesError
		if errors.As(err, &size) {
			rw.fail(w, 413, fmt.Sprintf("body exceeds %d bytes", MaxRemoteWriteBody))
		} else {
			rw.fail(w, 400, "could not read the request body")
		}
		return
	}
	decoded, err := snappyDecode(body, MaxRemoteWriteDecoded)
	if err != nil {
		rw.fail(w, 400, err.Error())
		return
	}
	series, err := decodeWriteRequest(decoded, rw.keep)
	if err != nil {
		rw.fail(w, 400, err.Error())
		return
	}
	bodySum := sha256.Sum256(body)
	batches, stats, err := rw.batches(r.Context(), series)
	rw.count(func(s *RemoteWriteStats) {
		s.Samples += stats.Samples
		s.Unmapped += stats.Unmapped
		s.StaleMarkers += stats.StaleMarkers
		s.Inactive += stats.Inactive
	})
	if err != nil {
		rw.fail(w, 503, err.Error())
		return
	}
	chunk := map[string]int{}
	for _, batch := range batches {
		// The ID depends only on the request body, the stream and the batch's
		// place among that stream's batches, so a retry of the same body
		// replays instead of counting twice, even if the map gained other
		// streams in between.
		k := chunk[batch.Stream]
		chunk[batch.Stream]++
		id := sha256.New()
		id.Write(bodySum[:])
		fmt.Fprintf(id, "\x00%s\x00%d", batch.Stream, k)
		batch.BatchID = "rw:" + hex.EncodeToString(id.Sum(nil))
		result, err := rw.db.Ingest(r.Context(), batch)
		if err != nil {
			status, message := remoteWriteStatus(err)
			rw.fail(w, status, message)
			return
		}
		rw.count(func(s *RemoteWriteStats) {
			s.Submitted += int64(len(batch.Observations))
			if result.Replayed {
				s.ReplayedBatches++
				return
			}
			s.Accepted += result.Accepted
			s.RejectedInvalid += result.RejectedInvalid
			s.RejectedLate += result.RejectedLate
			s.RejectedGroupLimit += result.RejectedGroupLimit
		})
	}
	w.WriteHeader(http.StatusNoContent)
}

// remoteWriteStatus decides whether Prometheus should retry. A refused clock
// jump can clear up once the clock is fixed, so it gets 503 and Prometheus
// keeps the samples. Samples from before a stream's effective time are
// skipped in batches, since no retry could ever store them; ErrStreamNotActive
// can then only come from samples dated in the future, which gets 400.
func remoteWriteStatus(err error) (int, string) {
	switch {
	case errors.Is(err, rollupdb.ErrInvalidRequest), errors.Is(err, rollupdb.ErrBatchConflict),
		errors.Is(err, rollupdb.ErrStreamNotFound), errors.Is(err, rollupdb.ErrStreamNotActive),
		errors.Is(err, rollupdb.ErrNumericOverflow):
		_, p := classify(err)
		return 400, p.Code + ": " + p.Message
	default:
		_, p := classify(err)
		return 503, p.Code + ": " + p.Message
	}
}

type batchStats struct{ Samples, Unmapped, StaleMarkers, Inactive int64 }

// batches turns series into per-stream batches in a fixed order: streams by
// name, samples in request order, split every MaxBatchSize observations.
func (rw *remoteWrite) batches(ctx context.Context, series []timeSeries) ([]rollupdb.Batch, batchStats, error) {
	var stats batchStats
	byStream := map[string][]rollupdb.Observation{}
	for _, ts := range series {
		stats.Samples += int64(len(ts.samples))
		metric, ok := rw.metrics[ts.labels["__name__"]]
		if !ok {
			stats.Unmapped += int64(len(ts.samples))
			continue
		}
		dims := make(map[string]string, len(metric.Dimensions))
		for dim, label := range metric.Dimensions {
			dims[dim] = ts.labels[label]
		}
		for _, s := range ts.samples {
			if math.Float64bits(s.value) == staleNaN {
				stats.StaleMarkers++
				continue
			}
			byStream[metric.Stream] = append(byStream[metric.Stream], rollupdb.Observation{Time: time.UnixMilli(s.timestamp).UTC(), Value: s.value, Dimensions: dims})
		}
	}
	names := make([]string, 0, len(byStream))
	for name := range byStream {
		names = append(names, name)
	}
	sort.Strings(names)
	var batches []rollupdb.Batch
	for _, name := range names {
		// Streams can't be deleted and the map was checked at start, so a
		// failure here is a storage or shutdown error: 503, retry.
		d, err := rw.db.Stream(ctx, name)
		if err != nil {
			return nil, stats, err
		}
		// Samples before the stream's effective time can never be stored; skip
		// and count them rather than fail (and stall) the whole request.
		all := byStream[name]
		obs := all[:0:0]
		for _, o := range all {
			if o.Time.Before(d.EffectiveAt) {
				stats.Inactive++
				continue
			}
			obs = append(obs, o)
		}
		for len(obs) > 0 {
			n := min(len(obs), rollupdb.MaxBatchSize)
			batches = append(batches, rollupdb.Batch{Stream: name, Version: d.Version, Observations: obs[:n]})
			obs = obs[n:]
		}
	}
	return batches, stats, nil
}

type sample struct {
	value     float64
	timestamp int64
}

type timeSeries struct {
	labels  map[string]string
	samples []sample
}

var errProto = errors.New("protobuf: malformed WriteRequest")

// protoField reads one field: its number, wire type, and either the varint or
// fixed value or the length-delimited bytes.
func protoField(b []byte) (num int, wire int, value uint64, data []byte, rest []byte, err error) {
	key, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, 0, 0, nil, nil, errProto
	}
	b = b[n:]
	num, wire = int(key>>3), int(key&7)
	if num == 0 {
		return 0, 0, 0, nil, nil, errProto
	}
	switch wire {
	case 0:
		value, n = binary.Uvarint(b)
		if n <= 0 {
			return 0, 0, 0, nil, nil, errProto
		}
		return num, wire, value, nil, b[n:], nil
	case 1:
		if len(b) < 8 {
			return 0, 0, 0, nil, nil, errProto
		}
		return num, wire, binary.LittleEndian.Uint64(b), nil, b[8:], nil
	case 2:
		length, n := binary.Uvarint(b)
		if n <= 0 || length > uint64(len(b)-n) {
			return 0, 0, 0, nil, nil, errProto
		}
		b = b[n:]
		return num, wire, 0, b[:length], b[length:], nil
	case 5:
		if len(b) < 4 {
			return 0, 0, 0, nil, nil, errProto
		}
		return num, wire, uint64(binary.LittleEndian.Uint32(b)), nil, b[4:], nil
	default:
		return 0, 0, 0, nil, nil, errProto
	}
}

// decodeWriteRequest reads the fields of prometheus.WriteRequest that carry
// float samples: timeseries (1) with labels (1) and samples (2). Metadata,
// exemplars and native histograms are skipped.
// keep names the labels to store; other labels are skipped without
// allocating, so a request full of unused labels costs little. nil keeps all.
func decodeWriteRequest(b []byte, keep map[string]bool) ([]timeSeries, error) {
	var out []timeSeries
	samples := 0
	for len(b) > 0 {
		num, wire, _, data, rest, err := protoField(b)
		if err != nil {
			return nil, err
		}
		b = rest
		if num != 1 {
			continue
		}
		if wire != 2 {
			return nil, errProto
		}
		if len(out) == MaxRemoteWriteSeries {
			return nil, fmt.Errorf("request has more than %d series", MaxRemoteWriteSeries)
		}
		ts, err := decodeTimeSeries(data, MaxRemoteWriteSamples-samples, keep)
		if err != nil {
			return nil, err
		}
		samples += len(ts.samples)
		out = append(out, ts)
	}
	return out, nil
}

// decodeTimeSeries reads one series, refusing more than room samples. The
// labels map is made on the first label, so an empty series costs little.
func decodeTimeSeries(b []byte, room int, keep map[string]bool) (timeSeries, error) {
	var ts timeSeries
	for len(b) > 0 {
		num, wire, _, data, rest, err := protoField(b)
		if err != nil {
			return ts, err
		}
		b = rest
		switch num {
		case 1:
			if wire != 2 {
				return ts, errProto
			}
			rawName, rawValue, err := decodeLabel(data)
			if err != nil {
				return ts, err
			}
			if keep != nil && !keep[string(rawName)] {
				continue
			}
			name, value := string(rawName), string(rawValue)
			if _, dup := ts.labels[name]; dup {
				return ts, fmt.Errorf("protobuf: label %q repeated in one series", name)
			}
			if ts.labels == nil {
				ts.labels = map[string]string{}
			}
			ts.labels[name] = value
		case 2:
			if wire != 2 {
				return ts, errProto
			}
			if len(ts.samples) >= room {
				return ts, fmt.Errorf("request has more than %d samples", MaxRemoteWriteSamples)
			}
			s, err := decodeSample(data)
			if err != nil {
				return ts, err
			}
			ts.samples = append(ts.samples, s)
		}
	}
	return ts, nil
}

func decodeLabel(b []byte) (name, value []byte, err error) {
	for len(b) > 0 {
		num, wire, _, data, rest, e := protoField(b)
		if e != nil {
			return nil, nil, e
		}
		b = rest
		if num != 1 && num != 2 {
			continue
		}
		if wire != 2 {
			return nil, nil, errProto
		}
		if num == 1 {
			name = data
		} else {
			value = data
		}
	}
	return name, value, nil
}

func decodeSample(b []byte) (sample, error) {
	var s sample
	for len(b) > 0 {
		num, wire, v, _, rest, err := protoField(b)
		if err != nil {
			return s, err
		}
		b = rest
		switch num {
		case 1:
			if wire != 1 {
				return s, errProto
			}
			s.value = math.Float64frombits(v)
		case 2:
			if wire != 0 {
				return s, errProto
			}
			s.timestamp = int64(v)
		}
	}
	return s, nil
}

// statsHandler answers GET /api/v1/write/stats.
func (rw *remoteWrite) statsHandler(w http.ResponseWriter, _ *http.Request) {
	jsonResponse(w, 200, rw.snapshot())
}
