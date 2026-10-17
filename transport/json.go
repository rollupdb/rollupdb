// Package transport adapts bounded local inputs to the embedded engine.
package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

// DecodeObject refuses non-object, non-UTF-8, unknown-field and trailing input.
// As with encoding/json, duplicate object keys use the last supplied value.
func DecodeObject(encoded []byte, target any) error {
	encoded = bytes.TrimSpace(encoded)
	if !utf8.Valid(encoded) {
		return errors.New("JSON must be valid UTF-8")
	}
	if len(encoded) == 0 || encoded[0] != '{' {
		return errors.New("expected a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("expected exactly one JSON object")
	}
	return nil
}
