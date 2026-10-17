package rollupdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/rollupdb/rollupdb/internal/accuracy"
)

func defaultSketchConfig() SketchConfig {
	return SketchConfig{Algorithm: SketchAlgorithm, EncodingVersion: 1, K: 200, M: 8}
}

func makeSketch(t testing.TB, c SketchConfig, values []float64) (*quantileSketch, exactState) {
	t.Helper()
	s, err := newQuantileSketch(c)
	if err != nil {
		t.Fatal(err)
	}
	var state exactState
	for _, v := range values {
		state, err = state.add(v)
		if err != nil {
			t.Fatal(err)
		}
		s.update(v)
	}
	return s, state
}

func TestSketchCodecRoundTripAcrossCapacities(t *testing.T) {
	for _, k := range []uint16{64, 200, 4096} {
		for _, n := range []int{1, 2, 63, 200, 201, 1000, 10000} {
			c := defaultSketchConfig()
			c.K = k
			values, _ := accuracy.Dataset("uniform", n)
			s, state := makeSketch(t, c, values)
			encoded, err := s.encode()
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeQuantileSketch(encoded, c, state)
			if err != nil {
				t.Fatalf("k=%d n=%d: %v", k, n, err)
			}
			if decoded.items.GetN() != uint64(n) {
				t.Fatal("lost sketch count")
			}
			again, err := decoded.encode()
			if err != nil || !bytes.Equal(encoded, again) {
				t.Fatalf("serialization changed without update: %v", err)
			}
		}
	}
}

func TestSketchAccuracyDirectAndUnequalPartitionMerges(t *testing.T) {
	c := defaultSketchConfig()
	ranks := []float64{0, 0.5, 0.9, 0.95, 0.99, 1}
	for _, name := range accuracy.DatasetNames {
		t.Run(name, func(t *testing.T) {
			values, _ := accuracy.Dataset(name, 20000)
			direct, state := makeSketch(t, c, values)
			a, _ := makeSketch(t, c, values[:137])
			b, _ := makeSketch(t, c, values[137:])
			if err := a.merge(b); err != nil {
				t.Fatal(err)
			}
			for _, s := range []*quantileSketch{direct, a} {
				answers, meta, err := quantileAnswers(ranks, &c, s, state, true)
				if err != nil {
					t.Fatal(err)
				}
				estimates := make([]float64, len(answers))
				for i, a := range answers {
					if a.Value == nil {
						t.Fatal("missing quantile")
					}
					estimates[i] = *a.Value
				}
				measurements, err := accuracy.Evaluate(values, ranks, estimates)
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range measurements {
					// A conservative regression ceiling, not a probabilistic 99% CI assertion.
					if m.RankError > 4*(*meta.NormalizedRankError)+1/float64(len(values)) {
						t.Fatalf("%s rank %.2f error %.5f", name, m.Rank, m.RankError)
					}
					if name == "constant" && m.AbsoluteValueError != 0 {
						t.Fatal("constant not preserved")
					}
				}
				if *answers[0].Value != state.Min || *answers[len(answers)-1].Value != state.Max {
					t.Fatal("endpoints not exact")
				}
			}
		})
	}
}

func TestSketchRejectsIncompatibleParametersWithoutMutation(t *testing.T) {
	c := defaultSketchConfig()
	a, _ := makeSketch(t, c, []float64{1, 2, 3})
	c.K = 400
	b, _ := makeSketch(t, c, []float64{4, 5, 6})
	before, _ := a.encode()
	if err := a.merge(b); !errors.Is(err, ErrSketchEncoding) {
		t.Fatal("mixed K accepted")
	}
	after, _ := a.encode()
	if !bytes.Equal(before, after) {
		t.Fatal("failed merge mutated target")
	}
	for _, change := range []func(*SketchConfig){func(c *SketchConfig) { c.Algorithm = "future" }, func(c *SketchConfig) { c.EncodingVersion = 2 }, func(c *SketchConfig) { c.K = 63 }, func(c *SketchConfig) { c.K = 4097 }, func(c *SketchConfig) { c.M = 4 }} {
		c = defaultSketchConfig()
		change(&c)
		if err := c.Validate(); !errors.Is(err, ErrSketchEncoding) {
			t.Fatal("unsupported config accepted")
		}
	}
}

func TestSketchRejectsMalformedAndUnknownEncoding(t *testing.T) {
	c := defaultSketchConfig()
	values, _ := accuracy.Dataset("uniform", 1000)
	s, state := makeSketch(t, c, values)
	encoded, _ := s.encode()
	mutations := map[string]func([]byte) []byte{
		"short":         func(b []byte) []byte { return b[:20] },
		"checksum":      func(b []byte) []byte { b[len(b)-1] ^= 1; return b },
		"codec_version": func(b []byte) []byte { b[4] = 2; return b },
		"type":          func(b []byte) []byte { b[5] = 2; return b },
		"settings":      func(b []byte) []byte { b[6] ^= 1; return b },
	}
	for name, change := range mutations {
		t.Run(name, func(t *testing.T) {
			_, err := decodeQuantileSketch(change(append([]byte(nil), encoded...)), c, state)
			if !errors.Is(err, ErrSketchEncoding) {
				t.Fatalf("accepted corruption: %v", err)
			}
		})
	}
	// Recompute the outer checksum to exercise actual structural checks too.
	for name, change := range map[string]func([]byte) []byte{
		"native_version":  func(b []byte) []byte { b[1] = 255; return b },
		"levels_overflow": func(b []byte) []byte { b[18] = 64; return b },
		"levels_zero":     func(b []byte) []byte { b[18] = 0; return b },
		"level_index":     func(b []byte) []byte { binary.LittleEndian.PutUint32(b[20:24], math.MaxUint32); return b },
		"count":           func(b []byte) []byte { binary.LittleEndian.PutUint64(b[8:16], 999); return b },
		"min_k":           func(b []byte) []byte { binary.LittleEndian.PutUint16(b[16:18], 64); return b },
		"truncated":       func(b []byte) []byte { return b[:len(b)-1] },
		"nonfinite": func(b []byte) []byte {
			binary.LittleEndian.PutUint64(b[len(b)-8:], math.Float64bits(math.Inf(1)))
			return b
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := change(append([]byte(nil), encoded[48:]...))
			if _, err := decodeQuantileSketch(wrapSketch(c, p), c, state); !errors.Is(err, ErrSketchEncoding) {
				t.Fatalf("accepted malformed payload: %v", err)
			}
		})
	}
}

func FuzzSketchDecoderRejectsMalformedPayloads(f *testing.F) {
	c := defaultSketchConfig()
	values, _ := accuracy.Dataset("uniform", 1000)
	s, state := makeSketch(f, c, values)
	encoded, _ := s.encode()
	f.Add(encoded[48:])
	f.Add([]byte{})
	f.Add([]byte{2, 255, 15, 4, 200, 0, 8, 0})
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > maxSketchBytes-48 {
			return
		}
		s, err := decodeQuantileSketch(wrapSketch(c, payload), c, state)
		if err != nil {
			return
		}
		if s == nil || s.items.GetN() != uint64(state.N) {
			t.Fatal("invalid decoder success")
		}
		if _, err = s.items.GetQuantile(0.5, true); err != nil {
			t.Fatal(err)
		}
	})
}
