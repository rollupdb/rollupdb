package rollupdb

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestStreamsAndAdvanceAll(t *testing.T) {
	ctx := context.Background()
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, true)
	from := fillTwoMinutes(t, db, now, d)
	later := definition("later")
	later.EffectiveAt = now.Add(48 * time.Hour)
	if err := db.CreateStream(ctx, later); err != nil {
		t.Fatal(err)
	}
	names, err := db.Streams(ctx)
	if err != nil || !reflect.DeepEqual(names, []string{"api", "later", "retained"}) {
		t.Fatalf("streams: %v %v", names, err)
	}
	*now = from.Add(5 * time.Minute)
	results, err := db.AdvanceAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := results["later"]; ok {
		t.Fatal("a stream that isn't active yet was advanced")
	}
	if r, ok := results["retained"]; !ok || r.SourceBuckets != 120 || r.ParentBuckets != 4 {
		t.Fatalf("retained: %+v", results)
	}
	if _, ok := results["api"]; !ok {
		t.Fatalf("api missing: %+v", results)
	}
	again, err := db.AdvanceAll(ctx)
	if err != nil || again["retained"].SourceBuckets != 0 {
		t.Fatalf("second run: %+v %v", again, err)
	}
}

func TestRunRetentionReportsWorkAndStops(t *testing.T) {
	db, now, d, _ := retentionDB(t, []Tier{{1, 120, 120}, {60, 0, 120}}, false)
	from := fillTwoMinutes(t, db, now, d)
	*now = from.Add(5 * time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	reports := make(chan RetentionReport, 16)
	done := make(chan error, 1)
	go func() {
		done <- db.RunRetention(ctx, 10*time.Millisecond, func(r RetentionReport) { reports <- r })
	}()
	select {
	case r := <-reports:
		if r.Err != nil || r.Stream != "retained" || r.Result.SourceBuckets != 120 {
			t.Fatalf("report: %+v", r)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no report from the schedule")
	}
	// Later runs find nothing to do and stay quiet.
	time.Sleep(50 * time.Millisecond)
	select {
	case r := <-reports:
		t.Fatalf("idle run reported: %+v", r)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("schedule didn't stop")
	}
	if err := db.RunRetention(context.Background(), 0, nil); err == nil {
		t.Fatal("accepted a zero interval")
	}
}
