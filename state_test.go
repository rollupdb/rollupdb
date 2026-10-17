package rollupdb

import (
	"errors"
	"math"
	"math/big"
	"testing"
)

func rationalReference(values []float64) (sum, mean, population, sample float64) {
	s := new(big.Rat)
	for _, v := range values {
		s.Add(s, new(big.Rat).SetFloat64(v))
	}
	sum, _ = s.Float64()
	if len(values) == 0 {
		return
	}
	m := new(big.Rat).Quo(s, big.NewRat(int64(len(values)), 1))
	mean, _ = m.Float64()
	m2 := new(big.Rat)
	for _, v := range values {
		delta := new(big.Rat).Sub(new(big.Rat).SetFloat64(v), m)
		m2.Add(m2, new(big.Rat).Mul(delta, delta))
	}
	population, _ = new(big.Rat).Quo(m2, big.NewRat(int64(len(values)), 1)).Float64()
	if len(values) > 1 {
		sample, _ = new(big.Rat).Quo(m2, big.NewRat(int64(len(values)-1), 1)).Float64()
	}
	return
}

func aggregateValues(t *testing.T, values []float64) exactState {
	t.Helper()
	var s exactState
	for _, v := range values {
		var err error
		s, err = s.add(v)
		if err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func near(t *testing.T, name string, got, want, absolute, relative float64) {
	t.Helper()
	if math.Abs(got-want) > absolute+relative*math.Abs(want) {
		t.Fatalf("%s got %.17g want %.17g", name, got, want)
	}
}

func TestExactStateAgainstRationalReference(t *testing.T) {
	values := make([]float64, 257)
	for i := range values {
		values[i] = 1e12 + float64(i%7)*0.25
	}
	wantSum, wantMean, wantPop, wantSample := rationalReference(values)
	direct := aggregateValues(t, values)
	left := aggregateValues(t, values[:3])
	right := aggregateValues(t, values[3:])
	merged, err := left.merge(right)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []exactState{direct, merged} {
		if s.N != int64(len(values)) {
			t.Fatal("count mismatch")
		}
		got := s.statistics([]string{"sum", "avg", "variance_population", "variance_sample"}, true)
		near(t, "sum", got["sum"].(float64), wantSum, 0, 1e-15)
		near(t, "avg", got["avg"].(float64), wantMean, 0.0002, 0)
		near(t, "population", got["variance_population"].(float64), wantPop, 0.0002, 0.001)
		near(t, "sample", got["variance_sample"].(float64), wantSample, 0.0002, 0.001)
	}
}

func TestWeightedMergeAndCompensatedSum(t *testing.T) {
	a := aggregateValues(t, []float64{10})
	b := aggregateValues(t, []float64{20, 20, 20})
	s, err := a.merge(b)
	if err != nil {
		t.Fatal(err)
	}
	if s.statistics([]string{"avg"}, true)["avg"] != 17.5 {
		t.Fatal("averaged averages without weighting")
	}
	values := []float64{1e16, 1, -1e16}
	s = aggregateValues(t, values)
	stats := s.statistics([]string{"sum", "avg"}, true)
	if stats["sum"] != float64(1) || stats["avg"] != 1.0/3.0 {
		t.Fatalf("lost compensated sum: %+v", stats)
	}
	a = aggregateValues(t, values[:2])
	b = aggregateValues(t, values[2:])
	s, err = a.merge(b)
	if err != nil || s.Sum+s.SumComp != 1 {
		t.Fatalf("lost compensation during merge: %+v %v", s, err)
	}
}

func TestExactStateEmptySingletonAndOverflow(t *testing.T) {
	names := []string{"count", "sum", "min", "max", "avg", "variance_population", "variance_sample"}
	empty := exactState{}.statistics(names, true)
	if empty["count"] != int64(0) || empty["sum"] != float64(0) || empty["avg"] != nil || empty["min"] != nil {
		t.Fatalf("empty: %+v", empty)
	}
	for _, v := range (exactState{}).statistics(names, false) {
		if v != nil {
			t.Fatal("unavailable statistic returned zero")
		}
	}
	s := aggregateValues(t, []float64{42})
	got := s.statistics(names, true)
	if got["variance_population"] != float64(0) || got["variance_sample"] != nil {
		t.Fatal("singleton variance")
	}
	for _, v := range []float64{math.NaN(), math.Inf(1), -math.MaxFloat64} {
		next, err := s.add(v)
		if !errors.Is(err, ErrNumericOverflow) || next != s {
			t.Fatalf("overflow mutated state: %+v %v", next, err)
		}
	}
	s.N = math.MaxInt64
	if _, err := s.add(1); !errors.Is(err, ErrNumericOverflow) {
		t.Fatal("count overflow accepted")
	}
	if _, err := s.merge(aggregateValues(t, []float64{1})); !errors.Is(err, ErrNumericOverflow) {
		t.Fatal("merge count overflow accepted")
	}
}
