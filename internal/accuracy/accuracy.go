// Package accuracy supplies external-to-the-engine sorted-reference measurements.
// It has no dependency on RollupDB or the sketch implementation.
package accuracy

import (
	"errors"
	"math"
	"sort"
)

var DatasetNames = []string{"uniform", "skewed", "heavy_tail", "duplicates", "constant"}

func Dataset(name string, n int) ([]float64, error) {
	if n < 1 {
		return nil, errors.New("positive sample count required")
	}
	values := make([]float64, n)
	for i := range values {
		u := (float64((i*7919)%n) + 0.5) / float64(n)
		switch name {
		case "uniform":
			values[i] = u
		case "skewed":
			if i%20 == 0 {
				values[i] = 100 + u*900
			} else {
				values[i] = u
			}
		case "heavy_tail":
			values[i] = 1 / math.Pow(1-u, 2)
		case "duplicates":
			if i%5 == 0 {
				values[i] = float64(i % 7)
			}
		case "constant":
			values[i] = 42
		default:
			return nil, errors.New("unknown dataset")
		}
	}
	return values, nil
}

type Measurement struct {
	Rank               float64  `json:"rank"`
	Estimate           float64  `json:"estimate"`
	Exact              float64  `json:"exact_nearest_rank"`
	RankLow            float64  `json:"rank_low"`
	RankHigh           float64  `json:"rank_high"`
	RankError          float64  `json:"normalized_rank_error_observed"`
	AbsoluteValueError float64  `json:"absolute_value_error"`
	RelativeValueError *float64 `json:"relative_value_error"`
}

// Evaluate uses inclusive nearest-rank quantiles, with the documented upstream
// 1e-7 natural-rank rounding when N<=1e7, then ceil(rank*N)-1 clamped at 0.
// An estimate's true rank is an interval when values repeat. Rank error is the
// distance to [count(x<estimate)/N,count(x<=estimate)/N], not a midpoint error.
func Evaluate(values, ranks, estimates []float64) ([]Measurement, error) {
	if len(values) == 0 || len(ranks) != len(estimates) {
		return nil, errors.New("invalid measurement inputs")
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	result := make([]Measurement, len(ranks))
	for i, rank := range ranks {
		v := estimates[i]
		if math.IsNaN(rank) || rank < 0 || rank > 1 || math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, errors.New("nonfinite or invalid quantile")
		}
		low := float64(sort.Search(len(sorted), func(j int) bool { return sorted[j] >= v })) / float64(len(sorted))
		high := float64(sort.Search(len(sorted), func(j int) bool { return sorted[j] > v })) / float64(len(sorted))
		naturalRank := rank * float64(len(sorted))
		if len(sorted) <= 10000000 {
			naturalRank = math.Round(naturalRank*1e7) / 1e7
		}
		exact := sorted[max(0, int(math.Ceil(naturalRank))-1)]
		absolute := math.Abs(v - exact)
		m := Measurement{Rank: rank, Estimate: v, Exact: exact, RankLow: low, RankHigh: high, RankError: max(0.0, max(low-rank, rank-high)), AbsoluteValueError: absolute}
		if exact != 0 {
			relative := absolute / math.Abs(exact)
			m.RelativeValueError = &relative
		}
		result[i] = m
	}
	return result, nil
}
