package rollupdb

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestDefinitionValidation(t *testing.T) {
	base := definition("api")
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	changes := map[string]func(*Definition){
		"identifier":                   func(d *Definition) { d.Name = "bad name" },
		"version":                      func(d *Definition) { d.Version = 2 },
		"group_limit":                  func(d *Definition) { d.MaxGroups = 0 },
		"duplicate_dimension":          func(d *Definition) { d.Dimensions = []string{"service", "service"} },
		"dimension_name":               func(d *Definition) { d.Dimensions = []string{""} },
		"no_statistics":                func(d *Definition) { d.Statistics = nil },
		"unknown_statistic":            func(d *Definition) { d.Statistics = []string{"p99"} },
		"duplicate_statistic":          func(d *Definition) { d.Statistics = []string{"count", "count"} },
		"no_tiers":                     func(d *Definition) { d.Tiers = nil },
		"month":                        func(d *Definition) { d.Tiers[1].WidthSeconds = 2592000 },
		"nonincreasing_width":          func(d *Definition) { d.Tiers[1].WidthSeconds = 1 },
		"forever_before_final":         func(d *Definition) { d.Tiers[0].RetainSeconds = 0 },
		"decreasing_retention":         func(d *Definition) { d.Tiers[1].RetainSeconds = 600 },
		"decreasing_grace":             func(d *Definition) { d.Tiers[1].GraceSeconds = 60 },
		"negative_grace":               func(d *Definition) { d.Tiers[0].GraceSeconds = -1 },
		"retention_shorter_than_grace": func(d *Definition) { d.Tiers[0].RetainSeconds = 60 },
		"unaligned_retention":          func(d *Definition) { d.Tiers[1].RetainSeconds = 3601 },
		"unaligned_effective":          func(d *Definition) { d.EffectiveAt = d.EffectiveAt.Add(time.Second) },
		"fractional_effective":         func(d *Definition) { d.EffectiveAt = d.EffectiveAt.Add(time.Nanosecond) },
		"zero_effective":               func(d *Definition) { d.EffectiveAt = time.Time{} },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			encoded, _ := json.Marshal(base)
			var d Definition
			if err := json.Unmarshal(encoded, &d); err != nil {
				t.Fatal(err)
			}
			change(&d)
			if err := d.Validate(); err == nil {
				t.Fatal("accepted invalid definition")
			}
		})
	}
}

func TestInvalidDefinitionNotPersistedAndOffsetNormalized(t *testing.T) {
	db, _ := newDB(t)
	d := definition("api")
	d.MaxGroups = 0
	if err := db.CreateStream(context.Background(), d); err == nil {
		t.Fatal("accepted invalid definition")
	}
	i, err := db.Info(context.Background())
	if err != nil || i.Streams != 0 {
		t.Fatalf("invalid write persisted: %+v %v", i, err)
	}
	d = definition("api")
	d.EffectiveAt = d.EffectiveAt.In(time.FixedZone("offset", 3*3600))
	if err = db.CreateStream(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	got, err := db.Stream(context.Background(), d.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got.EffectiveAt.Location() != time.UTC {
		t.Fatal("timestamp not normalized")
	}
}
