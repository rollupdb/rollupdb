package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var errSnappy = errors.New("snappy: corrupt input")

// snappyDecode decodes one snappy block (the format Prometheus remote write
// uses, not the framed stream format). It refuses output longer than limit.
func snappyDecode(src []byte, limit int) ([]byte, error) {
	n, read := binary.Uvarint(src)
	if read <= 0 || n > uint64(limit) {
		if read > 0 {
			return nil, fmt.Errorf("snappy: decoded length %d exceeds %d bytes", n, limit)
		}
		return nil, errSnappy
	}
	src = src[read:]
	dst := make([]byte, 0, int(n))
	for len(src) > 0 {
		tag := src[0]
		var length, offset int
		switch tag & 3 {
		case 0: // literal
			length = int(tag >> 2)
			src = src[1:]
			if length >= 60 {
				extra := length - 59 // 1 to 4 little-endian bytes hold length-1
				if len(src) < extra {
					return nil, errSnappy
				}
				var v uint64
				for i := 0; i < extra; i++ {
					v |= uint64(src[i]) << (8 * i)
				}
				src = src[extra:]
				if v >= uint64(limit) {
					return nil, errSnappy
				}
				length = int(v)
			}
			length++
			if length > len(src) || len(dst)+length > int(n) {
				return nil, errSnappy
			}
			dst = append(dst, src[:length]...)
			src = src[length:]
			continue
		case 1: // copy, 1-byte offset
			if len(src) < 2 {
				return nil, errSnappy
			}
			length = 4 + int(tag>>2)&7
			offset = int(tag&0xe0)<<3 | int(src[1])
			src = src[2:]
		case 2: // copy, 2-byte offset
			if len(src) < 3 {
				return nil, errSnappy
			}
			length = 1 + int(tag>>2)
			offset = int(binary.LittleEndian.Uint16(src[1:3]))
			src = src[3:]
		case 3: // copy, 4-byte offset
			if len(src) < 5 {
				return nil, errSnappy
			}
			length = 1 + int(tag>>2)
			v := binary.LittleEndian.Uint32(src[1:5])
			if v > uint32(limit) {
				return nil, errSnappy
			}
			offset = int(v)
			src = src[5:]
		}
		if offset <= 0 || offset > len(dst) || len(dst)+length > int(n) {
			return nil, errSnappy
		}
		// Copies may overlap their own output (a run), so go byte by byte.
		start := len(dst) - offset
		for i := 0; i < length; i++ {
			dst = append(dst, dst[start+i])
		}
	}
	if len(dst) != int(n) {
		return nil, errSnappy
	}
	return dst, nil
}
