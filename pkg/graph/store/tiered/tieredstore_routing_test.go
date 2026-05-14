package tiered

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestTieredStore_OntologyRouting_RefNode(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	if ts.OntologyForTest().ClassifyByToken(caseTok) != ClassReference {
		t.Error("Case should be ClassReference")
	}
	if ts.OntologyForTest().ClassifyByToken(signalTok) != ClassEvent {
		t.Error("Signal should be ClassEvent")
	}
}

func TestTieredStore_OntologyRouting_ShardForNode(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	if ts.ShardForNodeForTest(caseTok) != ts.RefShardForTest() {
		t.Error("Case node should go to refShard")
	}
	if ts.ShardForNodeForTest(signalTok) != ts.HotShardForTest().Store() {
		t.Error("Signal node should go to hotShard")
	}
}

func TestTieredStore_OntologyRouting_UnknownDefaultsToEvent(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	unknownTok, _ := reg.GetOrCreate("SomeNewLabel")
	if ts.ShardForNodeForTest(unknownTok) != ts.HotShardForTest().Store() {
		t.Error("unknown label should default to event shard")
	}
}

func TestRelationshipRowExistsRejectsStaleTypeIndex(t *testing.T) {
	relID := snowflake.ID(777)
	stale := newBadgerStoreWithOnlyRelTypeIndex(t, relID, 7)

	if stale.HasRelID(relID) {
		t.Fatal("Badger loadIndexes rebuilt relIDs from a stale type index without an entity row")
	}
	if relationshipRowExists(stale, types.RelID(relID)) {
		t.Fatal("relationshipRowExists treated a stale type index as a live relationship")
	}
}

func TestFindRelInAnyShardStoreSkipsStaleTypeIndex(t *testing.T) {
	relID := snowflake.ID(777)
	stale := newBadgerStoreWithOnlyRelTypeIndex(t, relID, 7)
	live, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore live: %v", err)
	}
	t.Cleanup(func() { _ = live.Close() })

	start := types.NewNode(types.NodeID(10), 1, nil)
	end := types.NewNode(types.NodeID(20), 1, nil)
	if err := live.PutNode(start); err != nil {
		t.Fatalf("PutNode start: %v", err)
	}
	if err := live.PutNode(end); err != nil {
		t.Fatalf("PutNode end: %v", err)
	}
	rel := types.NewRelationship(types.RelID(relID), 7, start.ID(), end.ID())
	if err := live.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship live: %v", err)
	}

	ts := &Store{}
	got := ts.findRelInAnyShardStore(relID, []namedStore{
		{name: "stale", store: stale},
		{name: "live", store: live},
	})
	if got != live {
		t.Fatal("findRelInAnyShardStore returned stale index owner instead of live relationship row owner")
	}
}

func TestTieredRelIDExistsIgnoresStaleTypeIndex(t *testing.T) {
	relID := snowflake.ID(777)
	staleRef := newBadgerStoreWithOnlyRelTypeIndex(t, relID, 7)
	ts := newTestTieredStore(t)
	ts.refShard = staleRef

	exists, err := ts.relIDExists(types.RelID(relID))
	if err != nil {
		t.Fatalf("relIDExists: %v", err)
	}
	if exists {
		t.Fatal("relIDExists treated stale type index as a live relationship")
	}
}

func TestTieredShardForNodeIDIgnoresStaleLabelIndexInRefShard(t *testing.T) {
	nodeID := snowflake.ID(777)
	staleRef := newBadgerStoreWithOnlyLabelIndex(t, nodeID, 7)
	ts := newTestTieredStore(t)
	ts.refShard = staleRef

	shard, checkin, err := ts.shardForNodeIDChecked(types.NodeID(nodeID))
	if err != nil {
		t.Fatalf("shardForNodeIDChecked: %v", err)
	}
	defer checkin()
	if shard == staleRef {
		t.Fatal("shardForNodeIDChecked returned stale label-index owner instead of timestamp owner")
	}
}

func newBadgerStoreWithOnlyLabelIndex(t *testing.T, nodeID snowflake.ID, labelToken uint16) *BadgerStore {
	t.Helper()
	return newBadgerStoreWithStaleIndexKeys(t, storeutil.LabelIndexKey(labelToken, nodeID))
}

func newBadgerStoreWithOnlyRelTypeIndex(t *testing.T, relID snowflake.ID, relType uint16) *BadgerStore {
	t.Helper()
	return newBadgerStoreWithStaleIndexKeys(t, storeutil.RelTypeIndexKey(relType, relID))
}

func newBadgerStoreWithStaleRelTypeAndIncomingIndex(t *testing.T, relID, startID, endID snowflake.ID, relType uint16) *BadgerStore {
	t.Helper()
	return newBadgerStoreWithStaleIndexKeys(t,
		storeutil.RelTypeIndexKey(relType, relID),
		storeutil.InKey(endID, relType, startID, relID),
	)
}

func newBadgerStoreWithStaleIndexKeys(t *testing.T, keys ...[]byte) *BadgerStore {
	t.Helper()
	dir := t.TempDir()
	bs, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("NewBadgerStore stale setup: %v", err)
	}
	if err := bs.DBForTest().Update(func(txn *badgerv4.Txn) error {
		for _, key := range keys {
			if err := txn.Set(key, []byte{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("write stale index keys: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close stale setup: %v", err)
	}

	reopened, err := NewBadgerStore(BadgerStoreConfig{Dir: dir})
	if err != nil {
		t.Fatalf("reopen stale setup: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	return reopened
}

func TestTieredStore_ShardWindowStart_WeeklyISOWeekOne(t *testing.T) {
	cases := []struct {
		name string
		in   time.Time
		want time.Time
	}{
		{
			name: "week one after friday jan one",
			in:   time.Date(2027, time.January, 4, 12, 0, 0, 0, time.UTC),
			want: time.Date(2027, time.January, 4, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "previous iso year spillover",
			in:   time.Date(2027, time.January, 1, 12, 0, 0, 0, time.UTC),
			want: time.Date(2026, time.December, 28, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "week one can start in previous calendar year",
			in:   time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC),
			want: time.Date(2025, time.December, 29, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shardWindowStart(tc.in, 7*24*time.Hour)
			if !got.Equal(tc.want) {
				t.Fatalf("shardWindowStart(%s) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}

func TestTieredStore_ShardWindowStart_SubDayWindows(t *testing.T) {
	in := time.Date(2026, time.May, 10, 12, 34, 56, 789_000_000, time.UTC)
	cases := []struct {
		name     string
		window   time.Duration
		want     time.Time
		wantName string
	}{
		{
			name:     "hour",
			window:   time.Hour,
			want:     time.Date(2026, time.May, 10, 12, 0, 0, 0, time.UTC),
			wantName: "20260510T120000.000Z",
		},
		{
			name:     "minute",
			window:   time.Minute,
			want:     time.Date(2026, time.May, 10, 12, 34, 0, 0, time.UTC),
			wantName: "20260510T123400.000Z",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shardWindowStart(in, tc.window)
			if !got.Equal(tc.want) {
				t.Fatalf("shardWindowStart(%s, %s) = %s, want %s", in, tc.window, got, tc.want)
			}
			if !in.Before(got.Add(tc.window)) {
				t.Fatalf("window [%s, %s) does not contain %s", got, got.Add(tc.window), in)
			}
			if name := shardWindowName(in, tc.window); name != tc.wantName {
				t.Fatalf("shardWindowName(%s, %s) = %q, want %q", in, tc.window, name, tc.wantName)
			}
			nextName := shardWindowName(got.Add(tc.window), tc.window)
			if nextName == tc.wantName {
				t.Fatalf("adjacent %s windows share shard name %q", tc.window, tc.wantName)
			}
		})
	}
}

func TestTieredStore_DepthHot(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Write to warm shard.
	warmN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	// Write to hot shard.
	hotN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotN)

	// DepthHot: only hot shard entities.
	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Errorf("AllNodes(DepthHot) = %d, want 1 (hot only)", len(nodes))
	}
	if nodes[0].ID() != hotN.ID() {
		t.Error("DepthHot should return the hot node")
	}
}

func TestTieredStore_DepthWarm(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotN)

	// DepthWarm: hot + warm.
	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthWarm})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("AllNodes(DepthWarm) = %d, want 2 (hot + warm)", len(nodes))
	}
}

func TestTieredStore_DepthAll(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotN)

	// DepthAll: all tiers.
	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthAll})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("AllNodes(DepthAll) = %d, want 2", len(nodes))
	}
}

func TestTieredStore_DepthZeroIsAll(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotN)

	// Zero Depth should be backward-compatible (all tiers).
	nodes, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("AllNodes(Depth=0) = %d, want 2 (backward-compatible)", len(nodes))
	}
}

func TestTieredStore_InvalidDepthRejected(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	const relType uint16 = 1
	if err := ts.CreateVectorIndex(signalTok, "vec", 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	badOpts := QueryOpts{Depth: ShardDepth(99)}
	checks := []struct {
		name string
		run  func() error
	}{
		{name: "AllNodes", run: func() error {
			_, err := ts.AllNodes(badOpts)
			return err
		}},
		{name: "AllNodeIDs", run: func() error {
			_, err := ts.AllNodeIDs(badOpts)
			return err
		}},
		{name: "AllRelationships", run: func() error {
			_, err := ts.AllRelationships(badOpts)
			return err
		}},
		{name: "AllRelIDs", run: func() error {
			_, err := ts.AllRelIDs(badOpts)
			return err
		}},
		{name: "NodesByLabel", run: func() error {
			_, err := ts.NodesByLabel(signalTok, badOpts)
			return err
		}},
		{name: "ReferenceNodesByLabel", run: func() error {
			_, err := ts.NodesByLabel(caseTok, badOpts)
			return err
		}},
		{name: "RelationshipsByType", run: func() error {
			_, err := ts.RelationshipsByType(relType, badOpts)
			return err
		}},
		{name: "NodesByLabelAndProperty", run: func() error {
			_, err := ts.NodesByLabelAndProperty(signalTok, "status", "open", badOpts)
			return err
		}},
		{name: "ReferenceNodesByLabelAndProperty", run: func() error {
			_, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", badOpts)
			return err
		}},
		{name: "SearchNearestNodes", run: func() error {
			_, err := ts.SearchNearestNodes(signalTok, "vec", []float32{1, 0}, 1, badOpts)
			return err
		}},
		{name: "ForEachNodeHistoryIDByDepth", run: func() error {
			return ts.ForEachNodeHistoryIDByDepth(badOpts.Depth, func(types.NodeID) bool { return true })
		}},
		{name: "ForEachRelHistoryIDByDepth", run: func() error {
			return ts.ForEachRelHistoryIDByDepth(badOpts.Depth, func(types.RelID) bool { return true })
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.run(); !errors.Is(err, ErrInvalidShardDepth) {
				t.Fatalf("%s invalid depth = %v, want ErrInvalidShardDepth", check.name, err)
			}
		})
	}
}

func TestTieredStore_AllIDs_TemporalOpts(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	n1 := types.NewNode(types.NodeID(10), caseTok, nil)
	n1.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200})
	n2 := types.NewNode(types.NodeID(20), caseTok, nil)
	n2.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 0})
	if err := ts.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := ts.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	nodeIDs, err := ts.AllNodeIDs(QueryOpts{ValidAt: 250})
	if err != nil {
		t.Fatalf("AllNodeIDs: %v", err)
	}
	if len(nodeIDs) != 1 || nodeIDs[0] != n2.ID() {
		t.Fatalf("AllNodeIDs ValidAt=250 = %v, want [%d]", nodeIDs, n2.ID())
	}

	r1 := types.NewRelationship(types.RelID(100), 1, n1.ID(), n2.ID())
	r1.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 200})
	r2 := types.NewRelationship(types.RelID(200), 1, n1.ID(), n2.ID())
	r2.SetTemporal(&types.TemporalMetadata{ValidFrom: 300, ValidTo: 0})
	if err := ts.PutRelationship(r1); err != nil {
		t.Fatalf("PutRelationship r1: %v", err)
	}
	if err := ts.PutRelationship(r2); err != nil {
		t.Fatalf("PutRelationship r2: %v", err)
	}

	relIDs, err := ts.AllRelIDs(QueryOpts{ValidAt: 350})
	if err != nil {
		t.Fatalf("AllRelIDs: %v", err)
	}
	if len(relIDs) != 1 || relIDs[0] != r2.ID() {
		t.Fatalf("AllRelIDs ValidAt=350 = %v, want [%d]", relIDs, r2.ID())
	}
}

func TestTieredStore_DepthCounters(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// 1 ref node, 1 warm event, 1 hot event.
	refN := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(refN)
	warmN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotN)

	// NodeCount always returns total (DepthAll).
	count, err := ts.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("NodeCount = %d, want 3", count)
	}

	// AllNodeIDs with DepthHot: ref node (always included) + 1 hot event.
	hotIDs, err := ts.AllNodeIDs(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(hotIDs) != 2 { // ref + hot
		t.Errorf("AllNodeIDs(DepthHot) = %d, want 2 (ref + hot)", len(hotIDs))
	}

	// AllNodeIDs with DepthWarm: ref + warm + hot.
	warmIDs, err := ts.AllNodeIDs(QueryOpts{Depth: DepthWarm})
	if err != nil {
		t.Fatal(err)
	}
	if len(warmIDs) != 3 {
		t.Errorf("AllNodeIDs(DepthWarm) = %d, want 3 (ref + warm + hot)", len(warmIDs))
	}
}

func TestTieredStore_DepthRelationshipsByType(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	rGen := tieredRelGen(t)
	var relType uint16 = 1

	// Create rel in warm shard.
	s1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), relType,
		s1.ID(), s2.ID()))

	forceRotation(t, ts)

	// Create rel in hot shard.
	s3 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s4 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(s3)
	_ = ts.PutNode(s4)
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), relType,
		s3.ID(), s4.ID()))

	// DepthHot: only hot shard rels.
	hotRels, err := ts.RelationshipsByType(relType, QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(hotRels) != 1 {
		t.Errorf("RelationshipsByType(DepthHot) = %d, want 1", len(hotRels))
	}

	// DepthAll: both.
	allRels, err := ts.RelationshipsByType(relType, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(allRels) != 2 {
		t.Errorf("RelationshipsByType(DepthAll) = %d, want 2", len(allRels))
	}
}

func TestTieredStore_DepthAllRelIDs(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	rGen := tieredRelGen(t)

	s1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), 1,
		s1.ID(), s2.ID()))

	forceRotation(t, ts)

	s3 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s4 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(s3)
	_ = ts.PutNode(s4)
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), 1,
		s3.ID(), s4.ID()))

	hotIDs, err := ts.AllRelIDs(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(hotIDs) != 1 {
		t.Errorf("AllRelIDs(DepthHot) = %d, want 1", len(hotIDs))
	}

	allIDs, err := ts.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(allIDs) != 2 {
		t.Errorf("AllRelIDs(DepthAll) = %d, want 2", len(allIDs))
	}
}

func TestTieredStore_ColdShard_TimestampResolution(t *testing.T) {
	// Verify snowflake ID timestamp correctly resolves to cold shard.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n)

	// Remember shard name, rotate, then manually demote to cold.
	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	_ = ts.RotateHotShard()

	demoteToCold(ts, hotName)

	// Resolve shard via shardForNodeID — should find the cold shard.
	shard, err := ts.ShardForNodeIDForTest(n.ID())
	if err != nil {
		t.Fatalf("shardForNodeID: %v", err)
	}
	if !shard.HasNodeID(n.ID().SnowflakeID()) {
		t.Error("shard should have the node")
	}
}

func TestTieredStore_ShardForNodeID_Error(t *testing.T) {
	// Verify shardForNodeID succeeds for a generated but absent event ID.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	gen := tieredNodeGen(t)
	id := gen.Generate()

	// Normal case: no error for non-existent node (falls back to hot shard).
	shard, err := ts.ShardForNodeIDForTest(types.NodeID(id))
	if err != nil {
		t.Fatalf("shardForNodeID should not error: %v", err)
	}
	if shard == nil {
		t.Error("shard should not be nil")
	}
}

func TestTieredStore_ShardForRelID_Error(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	gen := tieredRelGen(t)
	id := gen.Generate()

	shard, err := ts.ShardForRelIDForTest(types.RelID(id))
	if err != nil {
		t.Fatalf("ShardForRelIDForTest should not error: %v", err)
	}
	if shard == nil {
		t.Error("shard should not be nil")
	}
}

func TestTieredStore_RoutingErrorInWrite(t *testing.T) {
	// Verify that write operations propagate routing errors.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	gen := tieredNodeGen(t)
	id := gen.Generate()

	// DeleteNode for non-existent node should hit shardForNodeID then store.
	err := ts.DeleteNode(types.NodeID(id))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestTieredStore_ShardForRelID_FindsInWarmShard(t *testing.T) {
	// Cross-shard relationship in warm shard should be found without probing cold.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("HAS_SIGNAL")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create ref node and event node in hot shard.
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	evtNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatal(err)
	}

	// Create cross-shard relationship (ref→event).
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), relTypeTok, refNode.ID(), evtNode.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Rotate the event shard to warm.
	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	_ = ts.RotateHotShard()

	// Verify the relationship can still be found via ShardForRelIDForTest.
	shard, err := ts.ShardForRelIDForTest(types.RelID(relID))
	if err != nil {
		t.Fatalf("ShardForRelIDForTest: %v", err)
	}
	if !shard.HasRelID(relID) {
		t.Error("expected shard to have the rel")
	}

	// Now demote the old shard to cold and close it.
	demoteToCold(ts, hotName)
	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()
	coldES.LockShardMuForTest()
	if coldES.Store() != nil {
		_ = coldES.Store().Close()
		coldES.SetStoreForTest(nil)
	}
	coldES.UnlockShardMuForTest()

	// Entity lives in ref shard (for ref-node rels). It should still be found.
	// The ref shard fast path should resolve it.
	shard, err = ts.ShardForRelIDForTest(types.RelID(relID))
	if err != nil {
		t.Fatalf("ShardForRelIDForTest after cold: %v", err)
	}
	if !shard.HasRelID(relID) {
		t.Error("expected shard to have the rel after cold demotion")
	}
}
