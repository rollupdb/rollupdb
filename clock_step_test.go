package rollupdb

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// TestClockJumpIsRefusedAndWritesNothing checks that a clock reading far past
// the stored watermark can't become the watermark: Ingest and Advance refuse
// it, nothing changes, and reopening with a larger step lets a real gap through.
func TestClockJumpIsRefusedAndWritesNothing(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 17, 12, 2, 0, 0, time.UTC)
	clock := WithClock(func() time.Time { return now })
	path := filepath.Join(t.TempDir(), "clock.rollupdb")
	db, err := Create(ctx, path, clock, WithMaxClockStep(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	d := definition("api")
	if err = db.CreateStream(ctx, d); err != nil {
		t.Fatal(err)
	}
	at := now.Add(-time.Second)
	if r, err := db.Ingest(ctx, batchOf(d, observation(at, 1, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("first ingest: %+v %v", r, err)
	}
	before, err := db.Status(ctx, d.Name)
	if err != nil {
		t.Fatal(err)
	}

	// Within the limit is fine.
	now = now.Add(23 * time.Hour)
	if _, err = db.Advance(ctx, d.Name); err != nil {
		t.Fatalf("advance within the step: %v", err)
	}
	within, err := db.Status(ctx, d.Name)
	if err != nil {
		t.Fatal(err)
	}

	// Years ahead is refused by both writers, and nothing moves.
	now = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err = db.Ingest(ctx, batchOf(d, observation(now, 1, "web"))); !errors.Is(err, ErrClockJump) {
		t.Fatalf("ingest after a clock jump: %v", err)
	}
	if _, err = db.Advance(ctx, d.Name); !errors.Is(err, ErrClockJump) {
		t.Fatalf("advance after a clock jump: %v", err)
	}
	after, err := db.Status(ctx, d.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ObservedThrough.Equal(*within.ObservedThrough) || after.Accepted != before.Accepted {
		t.Fatalf("refused clock jump changed state: before %+v, after %+v", within, after)
	}

	// The clock comes back: everything works again from the old watermark.
	now = within.ObservedThrough.Add(time.Hour)
	if _, err = db.Advance(ctx, d.Name); err != nil {
		t.Fatalf("advance after the clock came back: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	// A real gap longer than the step goes through once the limit is lifted.
	now = now.Add(48 * time.Hour)
	reopened, err := Open(ctx, path, clock, WithMaxClockStep(0))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if r, err := reopened.Ingest(ctx, batchOf(d, observation(now.Add(-time.Second), 2, "web"))); err != nil || r.Accepted != 1 {
		t.Fatalf("ingest after lifting the limit: %+v %v", r, err)
	}
}

func TestClockStepDefaults(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	host, err := Create(ctx, filepath.Join(dir, "host.rollupdb"))
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	if host.maxClockStep != DefaultMaxClockStep {
		t.Fatalf("host clock step %s, want %s", host.maxClockStep, DefaultMaxClockStep)
	}
	injected, err := Create(ctx, filepath.Join(dir, "injected.rollupdb"), WithClock(time.Now))
	if err != nil {
		t.Fatal(err)
	}
	defer injected.Close()
	if injected.maxClockStep != 0 {
		t.Fatalf("injected clock step %s, want no limit", injected.maxClockStep)
	}
}
