package rollupdb

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSQLMatchesQueryAcrossTiers(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, true)
	from := fillTwoMinutes(t, db, now, d)
	// Promote the first two minutes, then add fresh one-second buckets, so the
	// view mixes tiers.
	*now = from.Add(5 * time.Minute)
	if _, err := db.Advance(ctx, d.Name); err != nil {
		t.Fatal(err)
	}
	at := now.Add(-time.Second)
	if _, err := db.Ingest(ctx, batchOf(d, observation(at, 1000, "web"), observation(at, 2000, "worker"))); err != nil {
		t.Fatal(err)
	}
	to := at.Add(time.Second)
	for _, service := range []string{"web", "worker"} {
		want, err := db.Query(ctx, Query{Stream: d.Name, Version: 1, From: from, To: to, Filter: map[string]string{"service": service}})
		if err != nil {
			t.Fatal(err)
		}
		got, err := db.SQL(ctx, `SELECT sum(count), sum(sum), min(min), max(max), sum(sum) / sum(count),
			rollup_variance(count, mean, m2), rollup_variance_sample(count, mean, m2), count(DISTINCT tier)
			FROM retained WHERE service = '`+service+`'`)
		if err != nil {
			t.Fatal(err)
		}
		row := got.Rows[0]
		if row[0] != want.Statistics["count"] || row[1] != want.Statistics["sum"] || row[2] != want.Statistics["min"] || row[3] != want.Statistics["max"] || row[7] != int64(2) {
			t.Fatalf("%s: sql %v, query %v", service, row, want.Statistics)
		}
		for i, name := range map[int]string{4: "avg", 5: "variance_population", 6: "variance_sample"} {
			w := want.Statistics[name].(float64)
			if g := row[i].(float64); math.Abs(g-w) > 1e-9*math.Max(1, math.Abs(w)) {
				t.Fatalf("%s %s: sql %v, query %v", service, name, g, w)
			}
		}
	}
	got, err := db.SQL(ctx, "SELECT rollup_quantile(sketch, 0.5), min(min), max(max), min(start), max(\"end\"), group_concat(DISTINCT width_seconds) FROM retained")
	if err != nil {
		t.Fatal(err)
	}
	row := got.Rows[0]
	if q, ok := row[0].(float64); !ok || q < row[1].(float64) || q > row[2].(float64) {
		t.Fatalf("median: %v", row)
	}
	if row[3] != from.Format(time.RFC3339) || row[4] != to.Format(time.RFC3339) {
		t.Fatalf("interval columns: %v", row)
	}
	if !reflect.DeepEqual(got.Columns[:2], []string{"rollup_quantile(sketch, 0.5)", "min(min)"}) {
		t.Fatalf("columns: %v", got.Columns)
	}
}

func TestSQLIsReadOnly(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	fillTwoMinutes(t, db, now, d)
	before, err := db.Status(ctx, d.Name)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"DELETE FROM buckets",
		"SELECT 1; DELETE FROM buckets",
		"WITH x AS (SELECT 1) DELETE FROM buckets",
		"WITH x AS (SELECT 1) INSERT INTO stream_runtime(stream, version) VALUES ('x', 1)",
		"PRAGMA query_only=OFF",
		"ATTACH 'other.db' AS other",
		"VACUUM INTO 'copy.db'",
		"SELECT 1 /* unterminated",
		"SELECT 'unterminated",
		"",
		"   ;",
		"SELECT * FROM missing_table",
		"SELECT rollup_quantile(sketch, 2) FROM retained",
	} {
		if _, err = db.SQL(ctx, statement); !errors.Is(err, ErrSQL) {
			t.Errorf("%q: %v", statement, err)
		}
	}
	after, err := db.Status(ctx, d.Name)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("SQL changed the database: %+v %v", after, err)
	}
	// The connection left query_only mode: writes still work afterwards.
	if _, err = db.Ingest(ctx, batchOf(d, observation(now.Add(-time.Second), 1, "web"))); err != nil {
		t.Fatalf("ingest after SQL: %v", err)
	}
	for _, statement := range []string{"select 1;", "  -- note\nSELECT ';' AS semicolon", "VALUES (1), (2)", `SELECT "count" FROM retained LIMIT 1`} {
		if _, err = db.SQL(ctx, statement); err != nil {
			t.Errorf("%q: %v", statement, err)
		}
	}
}

func TestSQLRowLimitAndQuantileWithoutSketches(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	fillTwoMinutes(t, db, now, d)
	got, err := db.SQL(ctx, "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 20000) SELECT i FROM n")
	if err != nil || len(got.Rows) != MaxSQLRows || !got.Truncated {
		t.Fatalf("row limit: %d rows, truncated %v, %v", len(got.Rows), got.Truncated, err)
	}
	got, err = db.SQL(ctx, "SELECT rollup_quantile(sketch, 0.99) FROM retained")
	if err != nil || got.Rows[0][0] != nil {
		t.Fatalf("quantile without sketches: %v %v", got.Rows, err)
	}
	if got, err = db.SQL(ctx, "SELECT name FROM pragma_table_info('retained') ORDER BY cid"); err != nil {
		t.Fatal(err)
	}
	var columns []string
	for _, row := range got.Rows {
		columns = append(columns, row[0].(string))
	}
	if strings.Join(columns, ",") != "start,end,width_seconds,tier,service,count,sum,min,max,mean,m2,sketch" {
		t.Fatalf("view columns: %v", columns)
	}
}

func TestSingleReadStatement(t *testing.T) {
	for statement, ok := range map[string]bool{
		"SELECT 1":                      true,
		"with x as (select 1) select *": true,
		"SELECT 'a;b'":                  true,
		`SELECT "a""b"`:                 true,
		"SELECT [a;b]":                  true,
		"SELECT 1; ":                    true,
		"SELECT 1;;":                    true,
		"SELECT 1; SELECT 2":            false,
		"INSERT INTO t VALUES (1)":      false,
		"/* x */ DROP TABLE buckets":    false,
		"(SELECT 1)":                    false,
	} {
		if err := singleReadStatement(statement); (err == nil) != ok {
			t.Errorf("%q: %v", statement, err)
		}
	}
}
