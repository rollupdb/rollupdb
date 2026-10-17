package accuracy

import "testing"

func TestDuplicateIntervalsAndNearestRank(t *testing.T) {
	values := []float64{0, 0, 0, 10}
	m, err := Evaluate(values, []float64{0, 0.5, 0.9, 1}, []float64{0, 0, 0, 10})
	if err != nil {
		t.Fatal(err)
	}
	if m[0].RankError != 0 || m[1].RankError != 0 || m[1].Exact != 0 || m[1].RelativeValueError != nil {
		t.Fatalf("duplicate treatment: %+v", m)
	}
	if m[2].RankError < 0.149999 || m[2].RankError > 0.150001 || m[2].Exact != 10 || m[3].RankError != 0 {
		t.Fatalf("rank distance: %+v", m)
	}
}

func TestNearestRankBoundaryRounding(t *testing.T) {
	values := make([]float64, 100)
	for i := range values {
		values[i] = float64(i)
	}
	m, err := Evaluate(values, []float64{0.14}, []float64{13})
	if err != nil || m[0].Exact != 13 || m[0].RankError != 0 {
		t.Fatalf("boundary convention: %+v %v", m, err)
	}
}
