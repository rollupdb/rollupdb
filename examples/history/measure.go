package main

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rollupdb/rollupdb"
	"github.com/rollupdb/rollupdb/internal/accuracy"
)

// Inputs are nonnegative integers <=100000, and cases are capped at ten million
// records. Both reference integer accumulators stay below int64 overflow. The
// final variance calculation uses exact rational arithmetic, not engine moments.
type reference struct {
	values       []float64
	sum, squares int64
	min, max     float64
}

func (r *reference) add(value float64) {
	if len(r.values) == 0 {
		r.min = value
		r.max = value
	} else {
		r.min = min(r.min, value)
		r.max = max(r.max, value)
	}
	r.values = append(r.values, value)
	n := int64(value)
	r.sum += n
	r.squares += n * n
}
func (r *reference) statistics() map[string]any {
	n := int64(len(r.values))
	if n == 0 {
		return map[string]any{"count": int64(0), "sum": float64(0), "min": nil, "max": nil, "avg": nil, "variance_population": nil, "variance_sample": nil}
	}
	mean, _ := new(big.Rat).SetFrac(big.NewInt(r.sum), big.NewInt(n)).Float64()
	numerator := new(big.Int).Mul(big.NewInt(r.squares), big.NewInt(n))
	numerator.Sub(numerator, new(big.Int).Mul(big.NewInt(r.sum), big.NewInt(r.sum)))
	population, _ := new(big.Rat).SetFrac(numerator, new(big.Int).Mul(big.NewInt(n), big.NewInt(n))).Float64()
	var sample any
	if n > 1 {
		v, _ := new(big.Rat).SetFrac(numerator, new(big.Int).Mul(big.NewInt(n), big.NewInt(n-1))).Float64()
		sample = v
	}
	return map[string]any{"count": n, "sum": float64(r.sum), "min": r.min, "max": r.max, "avg": mean, "variance_population": population, "variance_sample": sample}
}

type comparison struct {
	Expected      map[string]any         `json:"reference_statistics"`
	AbsoluteError map[string]float64     `json:"exact_absolute_error"`
	Quantiles     []accuracy.Measurement `json:"quantile_measurements"`
}

func evaluate(r *reference, q rollupdb.QueryResult) (comparison, error) {
	expected := r.statistics()
	c := comparison{Expected: expected, AbsoluteError: map[string]float64{}}
	for key, a := range expected {
		b := q.Statistics[key]
		if a == nil || b == nil {
			if a != b {
				return c, fmtError("availability differs for %s", key)
			}
			continue
		}
		if key == "count" {
			if a != b {
				return c, fmtError("count differs: %v %v", a, b)
			}
			c.AbsoluteError[key] = 0
			continue
		}
		av, bv := a.(float64), b.(float64)
		delta := math.Abs(av - bv)
		c.AbsoluteError[key] = delta
		tolerance := 0.0
		if key == "variance_population" || key == "variance_sample" {
			tolerance = 1e-8 + 1e-10*math.Abs(av)
		}
		if delta > tolerance {
			return c, fmtError("%s differs: expected %.17g got %.17g tolerance %g", key, av, bv, tolerance)
		}
	}
	if len(r.values) > 0 && len(q.Quantiles) > 0 {
		if q.QuantileAccuracy == nil {
			return c, fmtError("missing quantile accuracy metadata")
		}
		ranks, estimates := make([]float64, len(q.Quantiles)), make([]float64, len(q.Quantiles))
		for i, e := range q.Quantiles {
			if e.Value == nil {
				return c, fmtError("missing collected quantile")
			}
			ranks[i] = e.Rank
			estimates[i] = *e.Value
		}
		var err error
		c.Quantiles, err = accuracy.Evaluate(r.values, ranks, estimates)
		if err != nil {
			return c, err
		}
		// A broad gross-regression check, not a deterministic claim about a 99% fit.
		epsilon := 0.0
		if q.QuantileAccuracy.NormalizedRankError != nil {
			epsilon = *q.QuantileAccuracy.NormalizedRankError
		}
		for _, m := range c.Quantiles {
			if m.RankError > 4*epsilon+1/float64(len(r.values)) {
				return c, fmtError("gross rank regression: %+v", m)
			}
		}
	}
	return c, nil
}

type fileSizes struct {
	Main  int64 `json:"main_bytes"`
	WAL   int64 `json:"wal_bytes"`
	SHM   int64 `json:"shm_bytes"`
	Other int64 `json:"other_database_directory_bytes"`
	Total int64 `json:"total_bytes"`
}

func directorySizes(dir string) (fileSizes, error) {
	var s fileSizes
	entries, err := os.ReadDir(dir)
	if err != nil {
		return s, err
	}
	for _, entry := range entries {
		info, e := entry.Info()
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return s, e
		}
		if info.IsDir() {
			continue
		}
		n := info.Size()
		s.Total += n
		switch entry.Name() {
		case "live.rollupdb":
			s.Main = n
		case "live.rollupdb-wal":
			s.WAL = n
		case "live.rollupdb-shm":
			s.SHM = n
		default:
			s.Other += n
		}
	}
	return s, nil
}

type diskSampler struct {
	mu                    sync.Mutex
	dir                   string
	peakWorking, peakLive int64
	err                   error
	stop, done            chan struct{}
}

func startSampler(dir string) *diskSampler {
	s := &diskSampler{dir: dir, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				s.sample()
			}
		}
	}()
	s.sample()
	return s
}
func (s *diskSampler) sample() {
	sizes, err := directorySizes(s.dir)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.err = err
		return
	}
	s.peakWorking = max(s.peakWorking, sizes.Total)
	s.peakLive = max(s.peakLive, sizes.Main+sizes.WAL+sizes.SHM)
}
func (s *diskSampler) close() (int64, int64, error) {
	close(s.stop)
	<-s.done
	s.sample()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peakLive, s.peakWorking, s.err
}

func sourceReferences(path string, mainFrom, mainTo, dayFrom, dayTo time.Time) (*reference, *reference, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	all, day := &reference{}, &reference{}
	for {
		var o rollupdb.Observation
		if err = decoder.Decode(&o); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, nil, err
		}
		if !o.Time.Before(mainFrom) && o.Time.Before(mainTo) {
			all.add(o.Value)
		}
		if !o.Time.Before(dayFrom) && o.Time.Before(dayTo) && o.Dimensions["service"] == "g0" {
			day.add(o.Value)
		}
	}
	return all, day, nil
}
func cpuModel() string {
	encoded, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "not recorded"
	}
	for _, line := range strings.Split(string(encoded), "\n") {
		if strings.HasPrefix(line, "model name") {
			_, value, _ := strings.Cut(line, ":")
			return strings.TrimSpace(value)
		}
	}
	return "not recorded"
}
func copyFile(source, destination string) (err error) {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(filepath.Clean(destination), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	return errors.Join(copyErr, output.Close())
}
