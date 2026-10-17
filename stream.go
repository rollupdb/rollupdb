package rollupdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Tier specifies a fixed UTC bucket width, retention age from bucket end, and
// grace from bucket end, all in whole seconds. Zero retention means forever and
// is allowed only on the final tier. Grace must not decrease across tiers.
type Tier struct {
	WidthSeconds  int64 `json:"width_seconds"`
	RetainSeconds int64 `json:"retain_seconds"`
	GraceSeconds  int64 `json:"grace_seconds"`
}

// Definition is immutable once persisted. Only creation of version 1 is exposed
// in this prototype; definition changes are future work.
type Definition struct {
	Name        string        `json:"name"`
	Version     int           `json:"version"`
	EffectiveAt time.Time     `json:"effective_at"`
	Dimensions  []string      `json:"dimensions"`
	Statistics  []string      `json:"statistics"`
	MaxGroups   int           `json:"max_groups"`
	Tiers       []Tier        `json:"tiers"`
	Sketch      *SketchConfig `json:"sketch,omitempty"`
}

var identifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)

// Validate checks the fixed-tier and optional immutable sketch contracts.
func (d Definition) Validate() error {
	if !identifier.MatchString(d.Name) {
		return errors.New("invalid stream name: use up to 64 ASCII letters, digits and underscores, starting with a letter or underscore")
	}
	if d.Version != 1 {
		return errors.New("only new stream version 1 is supported")
	}
	if d.Sketch != nil {
		if err := d.Sketch.Validate(); err != nil {
			return err
		}
	}
	if d.MaxGroups < 1 || d.MaxGroups > 1_000_000 {
		return errors.New("max_groups must be between 1 and 1000000")
	}
	if len(d.Dimensions) > 16 {
		return errors.New("at most 16 dimensions are supported")
	}
	seen := make(map[string]bool)
	for _, dimension := range d.Dimensions {
		if !identifier.MatchString(dimension) || seen[dimension] {
			return fmt.Errorf("invalid or duplicate dimension %q", dimension)
		}
		seen[dimension] = true
	}
	if len(d.Statistics) == 0 {
		return errors.New("at least one statistic is required")
	}
	seen = make(map[string]bool)
	for _, stat := range d.Statistics {
		switch stat {
		case "count", "sum", "min", "max", "avg", "variance_population", "variance_sample":
		default:
			return fmt.Errorf("unsupported statistic %q", stat)
		}
		if seen[stat] {
			return fmt.Errorf("duplicate statistic %q", stat)
		}
		seen[stat] = true
	}
	if len(d.Tiers) == 0 || len(d.Tiers) > 4 {
		return errors.New("one to four tiers are required")
	}
	var previous Tier
	for n, tier := range d.Tiers {
		switch tier.WidthSeconds {
		case 1, 60, 3600, 86400:
		default:
			return errors.New("tier widths must be 1, 60, 3600 or 86400 seconds")
		}
		if tier.RetainSeconds < 0 || tier.RetainSeconds > 100*366*86400 || tier.GraceSeconds < 0 || tier.GraceSeconds > 100*366*86400 {
			return errors.New("retention and grace must be between zero and 100 leap years")
		}
		if tier.RetainSeconds == 0 && n != len(d.Tiers)-1 {
			return errors.New("only the last tier may retain forever")
		}
		if tier.RetainSeconds != 0 && (tier.RetainSeconds < tier.WidthSeconds || tier.RetainSeconds%tier.WidthSeconds != 0 || tier.RetainSeconds < tier.GraceSeconds) {
			return errors.New("finite retention must cover width and grace and be a multiple of width")
		}
		if n > 0 && (tier.WidthSeconds <= previous.WidthSeconds || tier.WidthSeconds%previous.WidthSeconds != 0 || tier.GraceSeconds < previous.GraceSeconds || (tier.RetainSeconds != 0 && tier.RetainSeconds < previous.RetainSeconds)) {
			return errors.New("tiers must have increasing nested widths and nondecreasing grace and retention")
		}
		previous = tier
	}
	if d.EffectiveAt.Before(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)) || !d.EffectiveAt.Before(time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)) {
		return errors.New("effective_at must be between 1970 inclusive and 2200 exclusive")
	}
	if d.EffectiveAt.Nanosecond() != 0 || d.EffectiveAt.Unix()%previous.WidthSeconds != 0 {
		return errors.New("effective_at must align to the coarsest tier boundary in UTC")
	}
	return nil
}

// CreateStream persists a validated initial definition atomically.
func (db *DB) CreateStream(ctx context.Context, d Definition) error {
	if err := d.Validate(); err != nil {
		return fmt.Errorf("stream definition: %w", err)
	}
	d.EffectiveAt = d.EffectiveAt.UTC()
	encoded, err := json.Marshal(d)
	if err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM stream_definitions WHERE name=?", d.Name).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return ErrStreamExists
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO stream_definitions(name,version,effective_at,definition_json) VALUES(?,?,?,?)", d.Name, d.Version, d.EffectiveAt.Format(time.RFC3339Nano), string(encoded)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO stream_runtime(stream,version) VALUES(?,?)", d.Name, d.Version); err != nil {
		return err
	}
	return tx.Commit()
}

// Stream returns the initial definition for a named stream.
func (db *DB) Stream(ctx context.Context, name string) (Definition, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return Definition{}, ErrClosed
	}
	return readDefinition(ctx, db.sql, name)
}

type rowReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readDefinition(ctx context.Context, reader rowReader, name string) (Definition, error) {
	var encoded string
	if err := reader.QueryRowContext(ctx, "SELECT definition_json FROM stream_definitions WHERE name=? AND version=1", name).Scan(&encoded); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Definition{}, ErrStreamNotFound
		}
		return Definition{}, err
	}
	var d Definition
	if err := json.Unmarshal([]byte(encoded), &d); err != nil {
		return d, err
	}
	if d.Name != name {
		return Definition{}, errors.New("rollupdb: corrupt definition identity")
	}
	return d, d.Validate()
}
