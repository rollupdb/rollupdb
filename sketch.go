package rollupdb

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/kll"
)

const SketchAlgorithm = "kll_float64"
const SketchEncodingVersion = 1
const sketchHeaderBytes = 48
const maxSketchBytes = 4 << 20

var ErrSketchEncoding = errors.New("rollupdb: invalid or incompatible sketch encoding")

// SketchConfig is immutable. This first codec accepts equal-K merges only,
// with the numeric float64 serializer and fixed M=8 from DataSketches v0.2.0.
type SketchConfig struct {
	Algorithm       string `json:"algorithm"`
	EncodingVersion int    `json:"encoding_version"`
	K               uint16 `json:"k"`
	M               uint8  `json:"m"`
}

func (c SketchConfig) Validate() error {
	if c.Algorithm != SketchAlgorithm || c.EncodingVersion != SketchEncodingVersion || c.K < 64 || c.K > 4096 || c.M != 8 {
		return fmt.Errorf("%w: require kll_float64, encoding 1, 64<=k<=4096, m=8", ErrSketchEncoding)
	}
	return nil
}

type quantileSketch struct {
	config SketchConfig
	items  *kll.ItemsSketch[float64]
}

func newQuantileSketch(config SketchConfig) (*quantileSketch, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	s, err := kll.NewKllItemsSketch[float64](config.K, config.M, common.ItemSketchDoubleComparator(false), common.ItemSketchDoubleSerDe{})
	if err != nil {
		return nil, err
	}
	return &quantileSketch{config, s}, nil
}

func (s *quantileSketch) update(value float64) { s.items.Update(value) }

func (s *quantileSketch) merge(other *quantileSketch) error {
	if other == nil || s.config != other.config {
		return ErrSketchEncoding
	}
	if s.items.GetN() > math.MaxInt64-other.items.GetN() {
		return ErrNumericOverflow
	}
	s.items.Merge(other.items)
	return nil
}

func wrapSketch(config SketchConfig, payload []byte) []byte {
	encoded := make([]byte, sketchHeaderBytes+len(payload))
	copy(encoded, "RDKL")
	encoded[4] = byte(config.EncodingVersion)
	encoded[5] = 1
	binary.LittleEndian.PutUint16(encoded[6:8], config.K)
	encoded[8] = config.M
	binary.LittleEndian.PutUint32(encoded[12:16], uint32(len(payload)))
	checksum := sha256.Sum256(payload)
	copy(encoded[16:48], checksum[:])
	copy(encoded[48:], payload)
	return encoded
}

func (s *quantileSketch) encode() ([]byte, error) {
	payload, err := s.items.ToSlice()
	if err != nil {
		return nil, err
	}
	return wrapSketch(s.config, payload), nil
}

// decodeQuantileSketch validates bounded layout, checksum, weighted count,
// extrema and parameters before calling the pinned upstream deserializer.
func decodeQuantileSketch(encoded []byte, config SketchConfig, state exactState) (_ *quantileSketch, err error) {
	if err = config.Validate(); err != nil {
		return nil, err
	}
	if len(encoded) < sketchHeaderBytes+8 || len(encoded) > maxSketchBytes || string(encoded[:4]) != "RDKL" || int(encoded[4]) != config.EncodingVersion || encoded[5] != 1 || binary.LittleEndian.Uint16(encoded[6:8]) != config.K || encoded[8] != config.M || encoded[9] != 0 || encoded[10] != 0 || encoded[11] != 0 || int(binary.LittleEndian.Uint32(encoded[12:16])) != len(encoded)-sketchHeaderBytes {
		return nil, ErrSketchEncoding
	}
	payload := encoded[sketchHeaderBytes:]
	checksum := sha256.Sum256(payload)
	if string(checksum[:]) != string(encoded[16:48]) {
		return nil, fmt.Errorf("%w: checksum", ErrSketchEncoding)
	}
	if err = validateKLLPayload(payload, config, state); err != nil {
		return nil, err
	}
	// Keep an error boundary if a future dependency behavior escapes validation.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: decoder rejected payload", ErrSketchEncoding)
		}
	}()
	items, err := kll.NewKllItemsSketchFromSlice[float64](payload, common.ItemSketchDoubleComparator(false), common.ItemSketchDoubleSerDe{})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSketchEncoding, err)
	}
	if items.GetN() != uint64(state.N) || items.GetK() != config.K {
		return nil, ErrSketchEncoding
	}
	return &quantileSketch{config, items}, nil
}

type QuantileEstimate struct {
	Rank        float64  `json:"rank"`
	Value       *float64 `json:"value"`
	Approximate bool     `json:"approximate"`
}

type QuantileAccuracy struct {
	Availability        string   `json:"availability"`
	Algorithm           string   `json:"algorithm,omitempty"`
	EncodingVersion     int      `json:"encoding_version,omitempty"`
	K                   uint16   `json:"k,omitempty"`
	M                   uint8    `json:"m,omitempty"`
	NormalizedRankError *float64 `json:"normalized_rank_error"`
	ErrorModel          string   `json:"error_model,omitempty"`
	RetainedItems       uint32   `json:"retained_items"`
	EstimationMode      bool     `json:"estimation_mode"`
}

func quantileAnswers(ranks []float64, config *SketchConfig, s *quantileSketch, state exactState, covered bool) ([]QuantileEstimate, *QuantileAccuracy, error) {
	if len(ranks) == 0 {
		return nil, nil, nil
	}
	answers := make([]QuantileEstimate, len(ranks))
	for i, rank := range ranks {
		answers[i].Rank = rank
	}
	accuracy := &QuantileAccuracy{Availability: "not_retained"}
	if config == nil {
		return answers, accuracy, nil
	}
	accuracy.Algorithm = config.Algorithm
	accuracy.EncodingVersion = config.EncodingVersion
	accuracy.K = config.K
	accuracy.M = config.M
	if !covered {
		accuracy.Availability = "not_collected"
		return answers, accuracy, nil
	}
	if state.N == 0 {
		accuracy.Availability = "no_observations"
		return answers, accuracy, nil
	}
	if s == nil || s.items.GetN() != uint64(state.N) {
		return nil, nil, ErrSketchEncoding
	}
	accuracy.Availability = "available"
	accuracy.EstimationMode = s.items.IsEstimationMode()
	accuracy.RetainedItems = s.items.GetNumRetained()
	epsilon := 0.0
	if accuracy.EstimationMode {
		epsilon = s.items.GetNormalizedRankError(false)
		accuracy.ErrorModel = "empirical_99_percent_max_rank_error"
	} else {
		accuracy.ErrorModel = "exact_retained_distribution"
	}
	accuracy.NormalizedRankError = &epsilon
	for i, rank := range ranks {
		value := state.Min
		if rank == 1 {
			value = state.Max
		} else if rank != 0 {
			var err error
			value, err = s.items.GetQuantile(rank, true)
			if err != nil {
				return nil, nil, err
			}
		}
		if !finite(value) {
			return nil, nil, ErrSketchEncoding
		}
		answers[i].Value = &value
		answers[i].Approximate = accuracy.EstimationMode && rank > 0 && rank < 1
	}
	return answers, accuracy, nil
}
