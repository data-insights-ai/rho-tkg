package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Authored source guards only. Real API/cell qualification is deferred with the
// complete V2 source checkpoint; no fixture here is a publication/RTT result.
func TestGraphColumnsFreshBindingAndLegacyIsolation(t *testing.T) {
	var dataset [32]byte
	for _, writers := range []int{1, 8, 32} {
		for _, layout := range []string{"kv", "columns"} {
			spec := graphSelectorsSpec{1, graphColumnsFamily, 40, 50, writers, layout}
			c := graphColumnsConfig(spec, "01020304050607080910111213141516")
			if !validGraphCostProfile(c) || c.Cost.Version != 6 || c.Limits.Transactions != 3+80*64 || validGraphSelectorsConfig(c) {
				t.Fatal("fresh family leaked into selectors/defaults", c)
			}
			policy := graphSelectorsPolicy(c)
			if dataset == [32]byte{} {
				dataset = policy.Dataset
			}
			if policy.Dataset != dataset || policy.Read.validate() != nil || policy.History.validate() != nil {
				t.Fatal("writer/layout changed dataset or unbounded policy")
			}
			a := &graphSelectorsArtifacts{end: graphSelectorsEnd{Spec: spec}}
			if a.filename("columns.jsonl") != "fdb-columns-columns.jsonl" {
				t.Fatal("fresh file namespace")
			}
		}
	}
	legacy := graphCostConfig("graph-cost-v1", graphCostProfile{1, 40, 50, 1})
	oldSpec := graphSelectorsSpec{1, graphSelectorsFamily, 40, 50, 1, "kv"}
	old := graphSelectorsConfig(oldSpec, "01020304050607080910111213141516")
	if legacy.Limits.Transactions != 83 || !validGraphSelectorsConfig(old) || validGraphColumnsConfig(old) || validGraphColumnsConfig(legacy) {
		t.Fatal("old profile widened")
	}
	oldArtifacts := &graphSelectorsArtifacts{end: graphSelectorsEnd{Spec: oldSpec}}
	if oldArtifacts.filename("events.jsonl") != "fdb-selectors-events.jsonl" {
		t.Fatal("historical filename changed")
	}
	ordinary, _ := wire(graphSelectorsPolicy(old))
	if bytes.Contains(ordinary, []byte("Admission")) {
		t.Fatal("nil admission changed old bytes")
	}
	bad := graphSelectorsSpec{1, graphColumnsFamily, 20, 50, 32, "columns"}
	if validGraphColumnsConfig(graphColumnsConfig(bad, "01020304050607080910111213141516")) {
		t.Fatal("missing genuine first jobs")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := newHarnessContext(ctx, fdb.Database{}, config{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled fresh IO entered SDK", err)
	}
}

func TestGraphColumnsTimingIsOptionalAndBoundToCompleteCall(t *testing.T) {
	if err := (*graphSelectorsArtifacts)(nil).columnTiming("Walk", "old", certificate{}, 0, 1); !errors.Is(err, errInvalid) {
		t.Fatal("nil timing owner accepted", err)
	}
	l := &graphCostColumnLane{}
	if err := l.timed("Walk", "old", certificate{}, 0, time.Time{}); err != nil {
		t.Fatal(err)
	}
	var calls int
	l.timing = func(call, name string, at certificate, rows uint64, nanos int64) error {
		calls++
		if call != "Walk" || name != "old-cache" || at.Round != 7 || rows != 43 || nanos < 0 {
			t.Fatal("timing lost actual call/cut/rows")
		}
		return errMismatch
	}
	if err := l.timed("Walk", "old-cache", certificate{Round: 7}, 43, time.Now()); !errors.Is(err, errMismatch) || calls != 1 {
		t.Fatal("timing error lost", err, calls)
	}
}
