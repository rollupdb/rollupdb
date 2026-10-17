// The compactCapacity function is adapted from Apache DataSketches Go v0.2.0.
// Copyright 2023-2026 The Apache Software Foundation.
// Licensed under the Apache License, Version 2.0; see
// third_party/datasketches/LICENSE and third_party/datasketches/NOTICE.
// Remaining validation code is original RollupDB code; see the root NOTICE.
package rollupdb

import (
	"encoding/binary"
	"math"
)

// compactCapacity mirrors the pinned serializer's integer capacity rounding.
// It validates layout only; sketch updates/compaction/merging use upstream code.
func compactCapacity(k uint16, levels int) uint32 {
	round := func(k uint64, depth int) uint64 {
		power := uint64(1)
		for range depth {
			power *= 3
		}
		return min(k, (((2*k)<<depth)/power+1)/2)
	}
	var total uint64
	for depth := 0; depth < levels; depth++ {
		capacity := uint64(0)
		if depth <= 30 {
			capacity = round(uint64(k), depth)
		} else {
			half := depth / 2
			capacity = round(round(uint64(k), half), depth-half)
		}
		total += max(uint64(8), capacity)
	}
	return uint32(total)
}

func validateKLLPayload(p []byte, c SketchConfig, s exactState) error {
	if len(p) < 8 || s.N <= 0 || !s.valid() || p[2] != 15 || p[7] != 0 || binary.LittleEndian.Uint16(p[4:6]) != c.K || p[6] != 8 {
		return ErrSketchEncoding
	}
	if p[0] == 2 && p[1] == 2 {
		if len(p) != 16 || p[3]&^byte(6) != 0 || p[3]&4 == 0 || s.N != 1 {
			return ErrSketchEncoding
		}
		value := math.Float64frombits(binary.LittleEndian.Uint64(p[8:16]))
		if !finite(value) || value != s.Min || value != s.Max {
			return ErrSketchEncoding
		}
		return nil
	}
	if p[0] != 5 || p[1] != 1 || p[3]&^byte(2) != 0 || len(p) < 20 || p[19] != 0 || s.N < 2 || binary.LittleEndian.Uint64(p[8:16]) != uint64(s.N) || binary.LittleEndian.Uint16(p[16:18]) != c.K {
		return ErrSketchEncoding
	}
	levels := int(p[18])
	if levels < 1 || levels > 63 || len(p) < 20+4*levels+16 {
		return ErrSketchEncoding
	}
	capacity := compactCapacity(c.K, levels)
	indices := make([]uint32, levels+1)
	indices[levels] = capacity
	for i := 0; i < levels; i++ {
		indices[i] = binary.LittleEndian.Uint32(p[20+4*i:])
		if indices[i] > capacity || (i > 0 && indices[i] < indices[i-1]) {
			return ErrSketchEncoding
		}
	}
	// A real compact sketch has a populated highest level. Otherwise a crafted
	// level count can exceed the merge work-array bound implied by N.
	if indices[levels-1] == capacity {
		return ErrSketchEncoding
	}
	base := 20 + 4*levels
	retained := capacity - indices[0]
	if len(p) != base+16+int(retained)*8 {
		return ErrSketchEncoding
	}
	minimum := math.Float64frombits(binary.LittleEndian.Uint64(p[base:]))
	maximum := math.Float64frombits(binary.LittleEndian.Uint64(p[base+8:]))
	if minimum != s.Min || maximum != s.Max {
		return ErrSketchEncoding
	}
	var weighted uint64
	for level := 0; level < levels; level++ {
		count := uint64(indices[level+1] - indices[level])
		if count > uint64(math.MaxInt64)>>level {
			return ErrSketchEncoding
		}
		weight := count << level
		if weighted > math.MaxInt64-weight {
			return ErrSketchEncoding
		}
		weighted += weight
		previous := minimum
		for index := indices[level]; index < indices[level+1]; index++ {
			offset := base + 16 + int(index-indices[0])*8
			value := math.Float64frombits(binary.LittleEndian.Uint64(p[offset:]))
			if !finite(value) || value < minimum || value > maximum || ((level > 0 || p[3]&2 != 0) && value < previous) {
				return ErrSketchEncoding
			}
			previous = value
		}
	}
	if weighted != uint64(s.N) {
		return ErrSketchEncoding
	}
	return nil
}
