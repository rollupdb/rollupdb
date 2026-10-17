package rollupdb

import (
	"errors"
	"math"
)

var ErrNumericOverflow = errors.New("rollupdb: numeric overflow")

// exactState carries mergeable state, not materialized percentile/average answers.
// Sum uses Neumaier compensation; mean and M2 use Welford/Chan updates.
// All arithmetic remains float64. Updates return a new state, never partial mutation.
type exactState struct {
	N            int64
	Sum, SumComp float64
	Min, Max     float64
	Mean, M2     float64
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

func (s exactState) valid() bool {
	return s.N >= 0 && finite(s.Sum) && finite(s.SumComp) && finite(s.Sum+s.SumComp) && finite(s.Min) && finite(s.Max) && finite(s.Mean) && finite(s.M2) && s.M2 >= 0 && (s.N == 0 || (s.Min <= s.Mean && s.Mean <= s.Max))
}

func (s exactState) addSum(x float64) exactState {
	t := s.Sum + x
	if math.Abs(s.Sum) >= math.Abs(x) {
		s.SumComp += (s.Sum - t) + x
	} else {
		s.SumComp += (x - t) + s.Sum
	}
	s.Sum = t
	return s
}

func (s exactState) add(x float64) (exactState, error) {
	if !finite(x) || s.N == math.MaxInt64 {
		return s, ErrNumericOverflow
	}
	if s.N == 0 {
		return exactState{N: 1, Sum: x, Min: x, Max: x, Mean: x}, nil
	}
	next := s.addSum(x)
	next.N++
	delta := x - s.Mean
	next.Mean = s.Mean + delta/float64(next.N)
	next.M2 = s.M2 + delta*(x-next.Mean)
	next.Min = math.Min(s.Min, x)
	next.Max = math.Max(s.Max, x)
	if !next.valid() {
		return s, ErrNumericOverflow
	}
	return next, nil
}

func (s exactState) merge(b exactState) (exactState, error) {
	if !s.valid() || !b.valid() || b.N > math.MaxInt64-s.N {
		return s, ErrNumericOverflow
	}
	if b.N == 0 {
		return s, nil
	}
	if s.N == 0 {
		return b, nil
	}
	next := s.addSum(b.Sum).addSum(b.SumComp)
	next.N += b.N
	delta := b.Mean - s.Mean
	next.Mean = s.Mean + delta*(float64(b.N)/float64(next.N))
	next.M2 = s.M2 + b.M2 + delta*(delta*(float64(s.N)/float64(next.N)))*float64(b.N)
	next.Min = math.Min(s.Min, b.Min)
	next.Max = math.Max(s.Max, b.Max)
	if !next.valid() {
		return s, ErrNumericOverflow
	}
	return next, nil
}

func (s exactState) statistics(names []string, covered bool) map[string]any {
	result := make(map[string]any, len(names))
	for _, name := range names {
		var value any
		if covered {
			switch name {
			case "count":
				value = s.N
			case "sum":
				value = s.Sum + s.SumComp
			case "min":
				if s.N > 0 {
					value = s.Min
				}
			case "max":
				if s.N > 0 {
					value = s.Max
				}
			case "avg":
				if s.N > 0 {
					value = (s.Sum + s.SumComp) / float64(s.N)
				}
			case "variance_population":
				if s.N > 0 {
					value = s.M2 / float64(s.N)
				}
			case "variance_sample":
				if s.N > 1 {
					value = s.M2 / float64(s.N-1)
				}
			}
		}
		result[name] = value
	}
	return result
}
