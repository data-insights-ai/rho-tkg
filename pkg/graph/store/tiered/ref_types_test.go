package tiered

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRefTypes_StayOnReferenceShard (Sigma RH-S7): with MintAtValidFrom and
// RelsByMint, a relationship of a Config.RefTypes type between two reference
// nodes is placed on the reference shard whatever its mint day, so its create,
// a new version and its reads open no event shard and make none. A type not
// listed still goes to its day's shard.
func TestRefTypes_StayOnReferenceShard(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	cfg := tierClockCfg(t.TempDir(), clock)
	cfg.RelsByMint = true
	cfg.RefTypes = []string{"DERIVED_IN", "ABOUT"}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	labels, relTypes := registrypkg.NewLabelRegistry(), registrypkg.NewRelTypeRegistry()
	ts.SetLabelRegistry(labels)
	ts.SetNameRegistries(labels, relTypes)
	refTok, _ := labels.GetOrCreate("Case")
	derived, _ := relTypes.GetOrCreate("DERIVED_IN")
	about, _ := relTypes.GetOrCreate("ABOUT")
	other, _ := relTypes.GetOrCreate("OTHER")
	if !ts.IsReferenceType("ABOUT") || ts.IsReferenceType("OTHER") {
		t.Fatal("IsReferenceType: ABOUT must be reference, OTHER not")
	}
	a := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), refTok, nil)
	b := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), refTok, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	before := ts.ShardAccessStats()
	mint := func(at time.Time) types.RelID {
		t.Helper()
		id, ok, err := ts.MintRelIDAt(a.ID(), ms(at), true)
		if err != nil || !ok {
			t.Fatalf("MintRelIDAt(%v): %v, %v", at, ok, err)
		}
		return id
	}
	var ids []types.RelID
	for i, at := range []time.Time{julDay(21, 4), julDay(20, 5), julDay(22, 6)} {
		r := types.NewRelationship(mint(at), []uint16{derived, about}[i%2], a.ID(), b.ID())
		_ = r.SetProperty("fact", "f")
		if i == 2 {
			if err := ts.PutRelationshipGeneratedID(r, generatedcreate.FreshGraphID()); err != nil {
				t.Fatalf("PutRelationshipGeneratedID at %v: %v", at, err)
			}
		} else if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship at %v: %v", at, err)
		}
		ids = append(ids, r.ID())
	}
	if err := ts.PutRelationshipsBatch([]*types.Relationship{types.NewRelationship(mint(julDay(19, 1)), about, b.ID(), a.ID())}); err != nil {
		t.Fatalf("PutRelationshipsBatch: %v", err)
	}
	v2 := types.NewRelationship(ids[0], derived, a.ID(), b.ID())
	_ = v2.SetProperty("fact", "g")
	if err := ts.ReplaceRelationship(v2); err != nil {
		t.Fatalf("ReplaceRelationship: %v", err)
	}
	for _, id := range ids {
		if !ts.RefShardForTest().HasRelID(id.SnowflakeID()) {
			t.Fatalf("reference-typed relationship %d not on the reference shard", id)
		}
		if _, err := ts.GetRelationship(id); err != nil {
			t.Fatalf("GetRelationship(%d): %v", id, err)
		}
	}
	got, err := ts.GetRelationship(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := got.GetProperty("fact"); v != "g" {
		t.Fatalf("version read fact = %v, want g", v)
	}
	after := ts.ShardAccessStats()
	if after.ColdOpens != before.ColdOpens || after.Cold != before.Cold || after.Warm != before.Warm {
		t.Fatalf("reference-typed work touched event shards: cold opens %d->%d, cold %d->%d, warm %d->%d",
			before.ColdOpens, after.ColdOpens, before.Cold, after.Cold, before.Warm, after.Warm)
	}
	if got := tierClockShards(t, ts); len(got) != 1 {
		t.Fatalf("event shards %v, want the hot 2026-07-25 only", got)
	}
	// A type not listed still goes by mint to its day's shard.
	plain := types.NewRelationship(mint(julDay(23, 1)), other, a.ID(), b.ID())
	if err := ts.PutRelationship(plain); err != nil {
		t.Fatal(err)
	}
	if ts.RefShardForTest().HasRelID(plain.ID().SnowflakeID()) {
		t.Fatal("unlisted type placed on the reference shard")
	}
	if _, ok := tierClockShards(t, ts)["2026-07-23"]; !ok {
		t.Fatal("unlisted type made no shard for its day")
	}
}

// TestRefTypes_ConfigRejectsBlank: a blank RefTypes entry is refused at New.
func TestRefTypes_ConfigRejectsBlank(t *testing.T) {
	cfg := mintCfg("", true)
	cfg.RefTypes = []string{"ABOUT", " "}
	if ts, err := New(cfg); err == nil {
		_ = ts.Close()
		t.Fatal("New accepted a blank RefTypes entry")
	}
}
