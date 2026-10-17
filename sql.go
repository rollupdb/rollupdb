package rollupdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"modernc.org/sqlite"
)

// MaxSQLRows caps the rows one SQL call returns; SQLResult.Truncated says
// when there were more.
const MaxSQLRows = 10_000

// ErrSQL reports SQL that was refused or failed to run. Nothing was written.
var ErrSQL = errors.New("rollupdb: sql")

// SQLResult is the answer to a read-only SQL query.
type SQLResult struct {
	Columns   []string `json:"columns"`
	Rows      [][]any  `json:"rows"`
	Truncated bool     `json:"truncated"`
}

// SQL runs one read-only SELECT (or WITH ... SELECT, or VALUES) over the
// retained buckets. Each stream is a view named after it, one row per
// retained bucket per dimension group, with these columns:
//
//	start, end      the bucket's interval in UTC, as RFC 3339 text
//	width_seconds   the bucket's width; tier is its tier number
//	one column per dimension, named after it (prefixed dim_ if the name
//	                clashes with a fixed column)
//	count, sum, min, max, mean   exact state of the bucket
//	m2              the sum of squared differences from the mean, for variance
//	sketch          the bucket's KLL sketch, or NULL
//
// Retained buckets never overlap, so sum(count), sum(sum), min(min) and
// max(max) are correct across any rows; the mean is sum(sum)/sum(count).
// Three aggregates merge the rest correctly:
//
//	rollup_variance(count, mean, m2)         population variance
//	rollup_variance_sample(count, mean, m2)  sample variance
//	rollup_quantile(sketch, rank)            a percentile from the merged
//	                                         sketches; NULL if any row has none
//
// The statement runs with the connection in query_only mode inside a
// transaction that is always rolled back, so it can't change the file.
// The SQL sees what is retained now: it doesn't report coverage gaps the way
// Query does.
func (db *DB) SQL(ctx context.Context, query string) (SQLResult, error) {
	if err := singleReadStatement(query); err != nil {
		return SQLResult{}, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return SQLResult{}, ErrClosed
	}
	// query_only belongs to the connection, not the transaction. Hold the
	// connection directly so the flag can be cleared after the transaction
	// ends however it ends, including when ctx is cancelled mid-query and
	// database/sql rolls the transaction back on its own.
	conn, err := db.sql.Conn(ctx)
	if err != nil {
		return SQLResult{}, err
	}
	defer func() {
		if _, resetErr := conn.ExecContext(context.Background(), "PRAGMA query_only=OFF"); resetErr != nil {
			// A connection that may still be read-only must not be reused.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return SQLResult{}, err
	}
	result, err := runReadOnly(ctx, tx, query)
	_ = tx.Rollback()
	return result, err
}

func runReadOnly(ctx context.Context, tx *sql.Tx, query string) (SQLResult, error) {
	failed := func(err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr // cancelled or timed out: not the statement's fault
		}
		return fmt.Errorf("%w: %v", ErrSQL, err)
	}
	if err := createStreamViews(ctx, tx); err != nil {
		return SQLResult{}, err
	}
	if _, err := tx.ExecContext(ctx, "PRAGMA query_only=ON"); err != nil {
		return SQLResult{}, err
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return SQLResult{}, failed(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return SQLResult{}, failed(err)
	}
	result := SQLResult{Columns: columns, Rows: [][]any{}}
	for rows.Next() {
		if len(result.Rows) == MaxSQLRows {
			result.Truncated = true
			break
		}
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err = rows.Scan(pointers...); err != nil {
			return SQLResult{}, failed(err)
		}
		for i, v := range values {
			if f, ok := v.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
				values[i] = nil // JSON has no NaN or infinity
			}
		}
		result.Rows = append(result.Rows, values)
	}
	if err = rows.Err(); err != nil {
		return SQLResult{}, failed(err)
	}
	return result, nil
}

// singleReadStatement accepts exactly one statement starting with SELECT,
// WITH or VALUES. query_only mode is the real guard against writes; this
// keeps out PRAGMA, ATTACH, VACUUM and multi-statement input, which
// query_only doesn't cover.
func singleReadStatement(query string) error {
	refuse := func(why string) error { return fmt.Errorf("%w: %s", ErrSQL, why) }
	if len(query) > 64<<10 {
		return refuse("statement longer than 64 KiB")
	}
	i, first := 0, ""
	end := -1
	for i < len(query) {
		c := query[i]
		switch {
		case c == '-' && i+1 < len(query) && query[i+1] == '-':
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		case c == '/' && i+1 < len(query) && query[i+1] == '*':
			j := strings.Index(query[i+2:], "*/")
			if j < 0 {
				return refuse("unterminated comment")
			}
			i += j + 4
			continue
		case c == '\'' || c == '"' || c == '`' || c == '[':
			closer := c
			if c == '[' {
				closer = ']'
			}
			j := i + 1
			for ; j < len(query); j++ {
				if query[j] == closer {
					if closer != ']' && j+1 < len(query) && query[j+1] == closer {
						j++ // doubled quote
						continue
					}
					break
				}
			}
			if j >= len(query) {
				return refuse("unterminated quote")
			}
			i = j + 1
			continue
		case c == ';':
			if end < 0 {
				end = i
			}
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
		default:
			if end >= 0 {
				return refuse("only one statement is allowed")
			}
			if first == "" {
				j := i
				for j < len(query) && (query[j] >= 'a' && query[j] <= 'z' || query[j] >= 'A' && query[j] <= 'Z') {
					j++
				}
				first = strings.ToUpper(query[i:j])
				if first == "" {
					return refuse("the statement must start with SELECT, WITH or VALUES")
				}
				i = j
				continue
			}
		}
		i++
	}
	switch first {
	case "SELECT", "WITH", "VALUES":
		return nil
	case "":
		return refuse("empty statement")
	}
	return refuse("only SELECT, WITH and VALUES statements are allowed")
}

var fixedViewColumns = []string{"start", "end", "width_seconds", "tier", "count", "sum", "min", "max", "mean", "m2", "sketch"}

// createStreamViews reads every definition before creating any view, so no
// view can shadow a table the engine reads, then creates one temp view per
// stream; see streamViewNames.
func createStreamViews(ctx context.Context, tx *sql.Tx) error {
	tables := map[string]bool{}
	rows, err := tx.QueryContext(ctx, "SELECT name FROM main.sqlite_schema")
	if err != nil {
		return err
	}
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		tables[strings.ToLower(name)] = true
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	if rows, err = tx.QueryContext(ctx, "SELECT DISTINCT name FROM main.stream_definitions ORDER BY name"); err != nil {
		return err
	}
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		names = append(names, name)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	definitions := make([]Definition, 0, len(names))
	for _, name := range names {
		d, err := readDefinition(ctx, tx, name)
		if err != nil {
			return err
		}
		definitions = append(definitions, d)
	}
	views := streamViewNames(definitions, tables)
	for i, d := range definitions {
		if _, err = tx.ExecContext(ctx, streamViewSQL(d, views[i])); err != nil {
			return fmt.Errorf("stream view %q: %w", d.Name, err)
		}
	}
	return nil
}

// streamViewNames gives each stream a view named after it when that name is
// free. SQLite names are case-insensitive, a view mustn't shadow one of the
// file's tables, and names starting with sqlite_ are reserved; a stream whose
// name can't be used gets stream_<name>, with stream_ added until it's free.
func streamViewNames(definitions []Definition, tables map[string]bool) []string {
	claimed := map[string]bool{}
	free := func(name string) bool {
		lower := strings.ToLower(name)
		return !tables[lower] && !claimed[lower] && !strings.HasPrefix(lower, "sqlite_")
	}
	views := make([]string, len(definitions))
	for i, d := range definitions {
		if free(d.Name) {
			views[i] = d.Name
			claimed[strings.ToLower(d.Name)] = true
		}
	}
	for i, d := range definitions {
		if views[i] != "" {
			continue
		}
		view := "stream_" + d.Name
		for !free(view) {
			view = "stream_" + view
		}
		views[i] = view
		claimed[strings.ToLower(view)] = true
	}
	return views
}

// streamViewSQL builds a stream's view. Names come from validated
// definitions (identifier characters only), and are quoted as well.
func streamViewSQL(d Definition, view string) string {
	var width strings.Builder
	width.WriteString("CASE tier")
	for i, t := range d.Tiers {
		fmt.Fprintf(&width, " WHEN %d THEN %d", i, t.WidthSeconds)
	}
	width.WriteString(" END")
	used := map[string]bool{}
	for _, column := range fixedViewColumns {
		used[column] = true
	}
	var b strings.Builder
	fmt.Fprintf(&b, `CREATE TEMP VIEW "%s" AS SELECT strftime('%%Y-%%m-%%dT%%H:%%M:%%SZ', start_ns/1000000000, 'unixepoch') AS "start", `, view)
	fmt.Fprintf(&b, `strftime('%%Y-%%m-%%dT%%H:%%M:%%SZ', start_ns/1000000000 + %s, 'unixepoch') AS "end", %s AS "width_seconds", tier AS "tier", `, width.String(), width.String())
	for i, dimension := range d.Dimensions {
		column := dimension
		for used[strings.ToLower(column)] {
			column = "dim_" + column
		}
		used[strings.ToLower(column)] = true
		fmt.Fprintf(&b, `json_extract(tuple_key, '$[%d]') AS "%s", `, i, column)
	}
	fmt.Fprintf(&b, `n AS "count", sum + sum_comp AS "sum", min AS "min", max AS "max", mean AS "mean", m2 AS "m2", sketch AS "sketch" FROM main.buckets WHERE stream = '%s' AND version = 1`, d.Name)
	return b.String()
}

func init() {
	for name, sample := range map[string]bool{"rollup_variance": false, "rollup_variance_sample": true} {
		sample := sample
		sqlite.MustRegisterFunction(name, &sqlite.FunctionImpl{
			NArgs:         3,
			Deterministic: true,
			MakeAggregate: func(sqlite.FunctionContext) (sqlite.AggregateFunction, error) {
				return &varianceAggregate{sample: sample}, nil
			},
		})
	}
	sqlite.MustRegisterFunction("rollup_quantile", &sqlite.FunctionImpl{
		NArgs:         2,
		Deterministic: true,
		MakeAggregate: func(sqlite.FunctionContext) (sqlite.AggregateFunction, error) {
			return &quantileAggregate{}, nil
		},
	})
}

func number(v driver.Value) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

// varianceAggregate merges (count, mean, m2) with Chan's formula.
type varianceAggregate struct {
	sample      bool
	n, mean, m2 float64
}

func (a *varianceAggregate) Step(_ *sqlite.FunctionContext, args []driver.Value) error {
	n, ok1 := number(args[0])
	mean, ok2 := number(args[1])
	m2, ok3 := number(args[2])
	if !ok1 || !ok2 || !ok3 {
		if args[0] == nil && args[1] == nil && args[2] == nil {
			return nil
		}
		return errors.New("rollup_variance takes count, mean and m2 columns")
	}
	if n <= 0 {
		return nil
	}
	total := a.n + n
	delta := mean - a.mean
	a.m2 += m2 + delta*delta*a.n*n/total
	a.mean += delta * n / total
	a.n = total
	return nil
}

func (a *varianceAggregate) WindowInverse(*sqlite.FunctionContext, []driver.Value) error {
	return errors.New("rollup_variance can't be used as a window function")
}

func (a *varianceAggregate) WindowValue(*sqlite.FunctionContext) (driver.Value, error) {
	switch {
	case a.sample && a.n > 1:
		return a.m2 / (a.n - 1), nil
	case !a.sample && a.n > 0:
		return a.m2 / a.n, nil
	}
	return nil, nil
}

func (a *varianceAggregate) Final(*sqlite.FunctionContext) {}

// quantileAggregate merges bucket sketches and reads one percentile.
type quantileAggregate struct {
	merged  *quantileSketch
	rank    float64
	rankSet bool
	missing bool
}

func (a *quantileAggregate) Step(_ *sqlite.FunctionContext, args []driver.Value) error {
	rank, ok := number(args[1])
	if !ok || math.IsNaN(rank) || rank < 0 || rank > 1 {
		return errors.New("rollup_quantile needs a rank between 0 and 1, such as 0.99")
	}
	if a.rankSet && rank != a.rank {
		return errors.New("rollup_quantile needs the same rank on every row")
	}
	a.rank, a.rankSet = rank, true
	if a.missing {
		return nil
	}
	encoded, ok := args[0].([]byte)
	if !ok || encoded == nil {
		a.missing = true // a bucket without a sketch: the merged distribution is unknown
		a.merged = nil
		return nil
	}
	s, err := decodeStoredSketch(encoded)
	if err != nil {
		return err
	}
	if a.merged == nil {
		a.merged = s
		return nil
	}
	return a.merged.merge(s)
}

func (a *quantileAggregate) WindowInverse(*sqlite.FunctionContext, []driver.Value) error {
	return errors.New("rollup_quantile can't be used as a window function")
}

func (a *quantileAggregate) WindowValue(*sqlite.FunctionContext) (driver.Value, error) {
	if a.missing || a.merged == nil || a.merged.items.IsEmpty() {
		return nil, nil
	}
	return a.merged.items.GetQuantile(a.rank, true)
}

func (a *quantileAggregate) Final(*sqlite.FunctionContext) {}

// decodeStoredSketch reads a sketch passed to rollup_quantile, taking the
// settings from its envelope. The bytes may come from anywhere (a SQL literal
// as easily as a bucket), so they get the same layout checks as a bucket's
// sketch, with the count and extremes the payload itself declares standing
// in for the bucket's exact state.
func decodeStoredSketch(encoded []byte) (*quantileSketch, error) {
	if len(encoded) < sketchHeaderBytes+8 || len(encoded) > maxSketchBytes || string(encoded[:4]) != "RDKL" {
		return nil, ErrSketchEncoding
	}
	config := SketchConfig{Algorithm: SketchAlgorithm, EncodingVersion: int(encoded[4]), K: binary.LittleEndian.Uint16(encoded[6:8]), M: encoded[8]}
	state, ok := declaredState(encoded[sketchHeaderBytes:])
	if !ok {
		return nil, ErrSketchEncoding
	}
	return decodeQuantileSketch(encoded, config, state)
}

// declaredState reads the count, minimum and maximum a KLL payload claims.
// validateKLLPayload then checks the payload is consistent with them.
func declaredState(p []byte) (exactState, bool) {
	value := func(at int) float64 { return math.Float64frombits(binary.LittleEndian.Uint64(p[at:])) }
	switch {
	case len(p) == 16 && p[0] == 2 && p[1] == 2:
		v := value(8)
		return exactState{N: 1, Min: v, Max: v, Mean: v}, finite(v)
	case len(p) >= 20 && p[0] == 5:
		n := binary.LittleEndian.Uint64(p[8:16])
		levels := int(p[18])
		if n < 2 || n > math.MaxInt64 || levels < 1 || levels > 63 || len(p) < 20+4*levels+16 {
			return exactState{}, false
		}
		base := 20 + 4*levels
		minimum, maximum := value(base), value(base+8)
		if !finite(minimum) || !finite(maximum) || minimum > maximum {
			return exactState{}, false
		}
		return exactState{N: int64(n), Min: minimum, Max: maximum, Mean: minimum}, true
	}
	return exactState{}, false
}
