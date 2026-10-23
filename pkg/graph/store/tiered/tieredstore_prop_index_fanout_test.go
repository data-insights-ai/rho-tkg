package tiered

// WP-31 (sigma row 59, backlog 10(b)): relationship property indexes and
// event-label node property indexes on the tiered store, built per shard by
// the anchored fan-out (reference shard last), definitions on every shard incl.
// the archive and the closed cold shards, answers folded across the shards and
// the history sidecars united (tieredstore_tx_members.go). Store-level pins:
// DDL break cases, every shard carries exactly the anchor's definitions,
// exact-set lookups (a literal expected set, so both index and fallback cannot
// be wrong together), the sidecar union and its declines, mid-DDL faults,
// repair, cold, archive move, retention drop, Elements and nil/NaN. The
// graph-level badger-oracle parity (TxPin + valid window across hot -> warm ->
// cold, numeric widths, no type scan) is in
// internal/core/tiered_prop_index_parity_test.go.

import (
	"errors"
	"math"
	"slices"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const (
	piCase   = uint16(1) // reference
	piSignal = uint16(3) // event
	piType   = uint16(5)
)

// propIndexWorld: rels of type piType with property "w" and Signal nodes with
// property "sev" spread over the reference shard, two event shards (the first
// rotated to warm) and, after archive, the archive shard.
type propIndexWorld struct {
	ts    *Store
	rels  map[string]*types.Relationship
	nodes map[string]*types.Node
	first string // the first event shard (warm after the rotation)
}

func newPropIndexWorld(t *testing.T, ts *Store) *propIndexWorld {
	t.Helper()
	installDefaultTestLabelRegistry(t, ts) // Case=1, User=2, Signal=3
	w := &propIndexWorld{ts: ts, rels: map[string]*types.Relationship{}, nodes: map[string]*types.Node{}}
	ts.mu.RLock()
	w.first = ts.hotShard.name
	ts.mu.RUnlock()
	w.node(t, "c1", piCase, nil)
	w.node(t, "c2", piCase, nil)
	w.node(t, "s1", piSignal, int64(1))
	w.node(t, "s2", piSignal, int64(2))
	w.node(t, "sl", piSignal, []any{int64(1), int64(9)})
	w.rel(t, "ref1", "c1", "c2", int64(1))   // reference shard (archived later in some tests)
	w.rel(t, "ev1", "s1", "s2", int64(1))    // first event shard
	w.rel(t, "ev2", "s1", "s2", int64(2))    // first event shard, other value
	w.rel(t, "evA", "s2", "s1", "a")         // scalar "a"
	w.rel(t, "list", "s2", "s1", []any{"a"}) // list holding "a"
	forceRotation(t, ts)
	w.node(t, "s3", piSignal, int64(1))
	w.rel(t, "hot1", "s3", "s1", int64(1)) // second (hot) event shard
	return w
}

func (w *propIndexWorld) node(t *testing.T, name string, label uint16, sev any) {
	t.Helper()
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), label, nil)
	if sev != nil {
		if err := n.SetProperty("sev", sev); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.ts.PutNode(n); err != nil {
		t.Fatalf("put node %s: %v", name, err)
	}
	w.nodes[name] = n
}

func (w *propIndexWorld) rel(t *testing.T, name, from, to string, v any) {
	t.Helper()
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), piType, w.nodes[from].ID(), w.nodes[to].ID())
	if err := r.SetProperty("w", v); err != nil {
		t.Fatal(err)
	}
	if err := w.ts.PutRelationship(r); err != nil {
		t.Fatalf("put rel %s: %v", name, err)
	}
	w.rels[name] = r
}

func (w *propIndexWorld) relNames(rs []*types.Relationship) []string {
	out := []string{}
	for _, r := range rs {
		for name, x := range w.rels {
			if x.ID() == r.ID() {
				out = append(out, name)
			}
		}
	}
	sort.Strings(out)
	return out
}

func (w *propIndexWorld) nodeNames(ns []*types.Node) []string {
	out := []string{}
	for _, n := range ns {
		for name, x := range w.nodes {
			if x.ID() == n.ID() {
				out = append(out, name)
			}
		}
	}
	sort.Strings(out)
	return out
}

func (w *propIndexWorld) relLookup(t *testing.T, v any) []string {
	t.Helper()
	rs, err := w.ts.RelationshipsByTypeAndProperty(piType, "w", v, storecontract.QueryOpts{})
	if err != nil {
		t.Fatalf("RelationshipsByTypeAndProperty(%v): %v", v, err)
	}
	return w.relNames(rs)
}

func (w *propIndexWorld) nodeLookup(t *testing.T, v any) []string {
	t.Helper()
	ns, err := w.ts.NodesByLabelAndProperty(piSignal, "sev", v, storecontract.QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty(%v): %v", v, err)
	}
	return w.nodeNames(ns)
}

// relMembers / nodeMembers read the history sidecar for value v (every key form).
func (w *propIndexWorld) relMembers(v any) ([]string, error) {
	seen := map[types.RelID]bool{}
	for _, vk := range types.EqualIndexKeys(v) {
		if err := w.ts.ForEachRelPropertyTxMember(piType, "w", vk, func(id types.RelID, _ types.Instant) bool {
			seen[id] = true
			return true
		}); err != nil {
			return nil, err
		}
	}
	out := []string{}
	for name, r := range w.rels {
		if seen[r.ID()] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (w *propIndexWorld) nodeMembers(v any) ([]string, error) {
	seen := map[types.NodeID]bool{}
	for _, vk := range types.EqualIndexKeys(v) {
		if err := w.ts.ForEachNodePropertyTxMember(piSignal, "sev", vk, func(id types.NodeID, _ types.Instant) bool {
			seen[id] = true
			return true
		}); err != nil {
			return nil, err
		}
	}
	out := []string{}
	for name, n := range w.nodes {
		if seen[n.ID()] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

func relIndexOn(s *BadgerStore) (storecontract.PropertyIndexOptions, bool) {
	o, ok, _ := s.RelPropertyIndexOptions(piType, "w")
	return o, ok
}

func nodeIndexOn(s *BadgerStore) (storecontract.PropertyIndexOptions, bool) {
	o, ok, _ := s.PropertyIndexOptions(piSignal, "sev")
	return o, ok
}

func TestTieredStoreRelPropertyIndex_FansOut(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	w := newPropIndexWorld(t, ts)
	want1 := []string{"ev1", "hot1", "ref1"}
	if got := w.relLookup(t, int64(1)); !slices.Equal(got, want1) {
		t.Fatalf("scan answer before create = %v, want %v", got, want1)
	}

	// Break cases before the create.
	if err := ts.CreateRelPropertyIndex(0, "w"); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Errorf("create token 0: %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.CreateRelPropertyIndex(piType, "tkg_valid_from"); !errors.Is(err, types.ErrReservedPrefix) {
		t.Errorf("create reserved key: %v, want ErrReservedPrefix", err)
	}
	if err := ts.DropRelPropertyIndex(piType, "w"); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Errorf("drop before create: %v, want ErrIndexNotFound", err)
	}
	if err := ts.CreateRelPropertyIndexWithOptions(piType, "w", storecontract.PropertyIndexOptions{RangeCounts: true}); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Errorf("create RangeCounts: %v, want ErrCapabilityNotSupported", err)
	}
	if has, err := ts.HasRelPropertyIndex(piType, "w"); err != nil || has {
		t.Errorf("HasRelPropertyIndex before create = %v, %v; want false", has, err)
	}

	if err := ts.CreateRelPropertyIndex(piType, "w"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ts.CreateRelPropertyIndex(piType, "w"); !errors.Is(err, storecontract.ErrIndexExists) {
		t.Errorf("duplicate create: %v, want ErrIndexExists", err)
	}
	if err := ts.CreateRelPropertyIndexWithOptions(piType, "w", storecontract.PropertyIndexOptions{Elements: true}); !errors.Is(err, storecontract.ErrIndexExists) {
		t.Errorf("duplicate create with other options: %v, want ErrIndexExists", err)
	}
	if has, err := ts.HasRelPropertyIndex(piType, "w"); err != nil || !has {
		t.Errorf("HasRelPropertyIndex = %v, %v; want true", has, err)
	}
	if o, ok, err := ts.RelPropertyIndexOptions(piType, "w"); err != nil || !ok || o != (storecontract.PropertyIndexOptions{}) {
		t.Errorf("RelPropertyIndexOptions = %+v, %v, %v; want plain, true", o, ok, err)
	}

	forceRotation(t, ts) // a hot shard opened after the create
	if err := ts.ArchiveNode(w.nodes["c1"].ID()); err != nil {
		t.Fatalf("archive: %v", err)
	}
	shards := allShardStoresForTest(t, ts)
	if _, ok := shards["archive"]; !ok {
		t.Fatal("no archive shard")
	}
	for name, s := range shards {
		if _, ok := relIndexOn(s); !ok {
			t.Errorf("shard %s carries no relationship property index", name)
		}
	}
	if got := w.relLookup(t, int64(1)); !slices.Equal(got, want1) {
		t.Errorf("indexed answer = %v, want %v", got, want1)
	}
	if got := w.relLookup(t, int64(2)); !slices.Equal(got, []string{"ev2"}) {
		t.Errorf("value 2 = %v, want [ev2]", got)
	}
	if got := w.relLookup(t, "a"); !slices.Equal(got, []string{"evA"}) {
		t.Errorf("plain index, value a = %v, want [evA] (a list is not a match)", got)
	}
	for _, v := range []any{nil, math.NaN(), int64(77)} {
		if got := w.relLookup(t, v); len(got) != 0 {
			t.Errorf("value %v = %v, want none", v, got)
		}
	}

	if err := ts.DropRelPropertyIndex(piType, "w"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := ts.DropRelPropertyIndex(piType, "w"); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Errorf("drop twice: %v, want ErrIndexNotFound", err)
	}
	for name, s := range allShardStoresForTest(t, ts) {
		if _, ok := relIndexOn(s); ok {
			t.Errorf("after drop shard %s still carries the index", name)
		}
	}
	// Drop then fallback: same answer by the scan, the sidecar declines.
	if got := w.relLookup(t, int64(1)); !slices.Equal(got, want1) {
		t.Errorf("after drop answer = %v, want %v", got, want1)
	}
	if _, err := w.relMembers(int64(1)); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Errorf("sidecar after drop: %v, want ErrIndexNotFound", err)
	}

	closed := newTestTieredStore(t)
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	ts = closed
	if err := ts.CreateRelPropertyIndex(piType, "w"); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Errorf("create after close: %v, want ErrStoreClosed", err)
	}
	if err := ts.DropRelPropertyIndex(piType, "w"); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Errorf("drop after close: %v, want ErrStoreClosed", err)
	}
	if _, err := ts.HasRelPropertyIndex(piType, "w"); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Errorf("has after close: %v, want ErrStoreClosed", err)
	}
}

func TestTieredStoreNodePropertyIndex_EventLabelFansOut(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	w := newPropIndexWorld(t, ts)
	want1 := []string{"s1", "s3"}
	if got := w.nodeLookup(t, int64(1)); !slices.Equal(got, want1) {
		t.Fatalf("scan answer = %v, want %v", got, want1)
	}
	if err := ts.DropPropertyIndex(piSignal, "sev"); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Errorf("drop before create: %v, want ErrIndexNotFound", err)
	}
	if err := ts.CreatePropertyIndex(piSignal, "sev"); err != nil {
		t.Fatalf("create on an event label: %v", err)
	}
	if err := ts.CreatePropertyIndex(piSignal, "sev"); !errors.Is(err, storecontract.ErrIndexExists) {
		t.Errorf("duplicate create: %v, want ErrIndexExists", err)
	}
	forceRotation(t, ts)
	for name, s := range allShardStoresForTest(t, ts) {
		if _, ok := nodeIndexOn(s); !ok {
			t.Errorf("shard %s carries no node property index", name)
		}
	}
	if has, err := ts.HasPropertyIndex(piSignal, "sev"); err != nil || !has {
		t.Errorf("HasPropertyIndex = %v, %v; want true", has, err)
	}
	if got := w.nodeLookup(t, int64(1)); !slices.Equal(got, want1) {
		t.Errorf("indexed answer = %v, want %v", got, want1)
	}
	if got := w.nodeLookup(t, int64(9)); len(got) != 0 {
		t.Errorf("plain index, element 9 = %v, want none", got)
	}
	// A reference-label index reaches every shard too (one rule for all labels).
	if err := ts.CreatePropertyIndex(piCase, "name"); err != nil {
		t.Fatalf("create on a reference label: %v", err)
	}
	for name, s := range allShardStoresForTest(t, ts) {
		if has, _ := s.HasPropertyIndex(piCase, "name"); !has {
			t.Errorf("shard %s carries no reference-label index", name)
		}
	}
	if err := ts.DropPropertyIndex(piSignal, "sev"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	for name, s := range allShardStoresForTest(t, ts) {
		if _, ok := nodeIndexOn(s); ok {
			t.Errorf("after drop shard %s still carries the index", name)
		}
	}
	if got := w.nodeLookup(t, int64(1)); !slices.Equal(got, want1) {
		t.Errorf("after drop answer = %v, want %v", got, want1)
	}
}

// The history sidecars: the union of every shard's members (a value an update
// removed stays a member), declined when the anchor has no index or any shard
// lacks it or carries other options.
func TestTieredStorePropertyTxMembers_UnionAcrossShards(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	w := newPropIndexWorld(t, ts)
	if _, err := w.relMembers(int64(1)); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Fatalf("rel sidecar without index: %v, want ErrIndexNotFound", err)
	}
	if _, err := w.nodeMembers(int64(1)); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Fatalf("node sidecar without index: %v, want ErrIndexNotFound", err)
	}
	if err := ts.CreateRelPropertyIndex(piType, "w"); err != nil {
		t.Fatal(err)
	}
	if err := ts.CreatePropertyIndex(piSignal, "sev"); err != nil {
		t.Fatal(err)
	}
	// ev1 moves 1 -> 3 (history keeps 1), s1 moves 1 -> 3.
	ev1 := w.rels["ev1"]
	ev1b := ev1.DeepCopy()
	if err := ev1b.SetProperty("w", int64(3)); err != nil {
		t.Fatal(err)
	}
	ev1b.SetVersion(ev1.Version() + 1)
	if err := ts.ReplaceRelWithHistory(ev1b, ev1.Version(), ev1); err != nil {
		t.Fatal(err)
	}
	s1 := w.nodes["s1"]
	s1b := s1.DeepCopy()
	if err := s1b.SetProperty("sev", int64(3)); err != nil {
		t.Fatal(err)
	}
	s1b.SetVersion(s1.Version() + 1)
	if err := ts.ReplaceNodeWithHistory(s1b, s1.Version(), s1); err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(w.nodes["c1"].ID()); err != nil {
		t.Fatal(err)
	}
	if got, err := w.relMembers(int64(1)); err != nil || !slices.Equal(got, []string{"ev1", "hot1", "ref1"}) {
		t.Errorf("rel members of 1 = %v, %v; want [ev1 hot1 ref1]", got, err)
	}
	if got, err := w.relMembers(int64(3)); err != nil || !slices.Equal(got, []string{"ev1"}) {
		t.Errorf("rel members of 3 = %v, %v; want [ev1]", got, err)
	}
	if got, err := w.nodeMembers(int64(1)); err != nil || !slices.Equal(got, []string{"s1", "s3"}) {
		t.Errorf("node members of 1 = %v, %v; want [s1 s3]", got, err)
	}
	if got := w.relLookup(t, int64(1)); !slices.Equal(got, []string{"hot1", "ref1"}) {
		t.Errorf("current answer of 1 = %v, want [hot1 ref1]", got)
	}

	// A shard missing the definition (an interrupted drop): decline.
	ws := eventShardNamed(t, ts, w.first).store
	if err := ws.DropRelPropertyIndex(piType, "w"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.relMembers(int64(1)); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Errorf("rel sidecar with a shard lacking the index: %v, want ErrIndexNotFound", err)
	}
	// ... but the current lookup still answers (that shard scans).
	if got := w.relLookup(t, int64(3)); !slices.Equal(got, []string{"ev1"}) {
		t.Errorf("current answer of 3 with a shard lacking the index = %v, want [ev1]", got)
	}
	// Other options on that shard: decline too.
	if err := ws.CreateRelPropertyIndexWithOptions(piType, "w", storecontract.PropertyIndexOptions{Elements: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.relMembers(int64(1)); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Errorf("rel sidecar with a shard carrying other options: %v, want ErrIndexNotFound", err)
	}
	// A plain anchor over a shard holding a stale Elements index: the list
	// holding "a" must not match a plain lookup of "a".
	if got := w.relLookup(t, "a"); !slices.Equal(got, []string{"evA"}) {
		t.Errorf("plain lookup over a stale Elements shard index = %v, want [evA]", got)
	}
	// Sync repairs the shard to the anchor (what open / lazy open does).
	if err := ts.syncAnchoredIndexes(ws, true); err != nil {
		t.Fatal(err)
	}
	if o, ok := relIndexOn(ws); !ok || o.Elements {
		t.Errorf("after sync the shard index = %+v, %v; want the anchor's plain index", o, ok)
	}
	if got, err := w.relMembers(int64(1)); err != nil || !slices.Equal(got, []string{"ev1", "hot1", "ref1"}) {
		t.Errorf("rel members of 1 after repair = %v, %v; want [ev1 hot1 ref1]", got, err)
	}
}

func TestTieredStorePropertyIndex_Elements(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	w := newPropIndexWorld(t, ts)
	el := storecontract.PropertyIndexOptions{Elements: true}
	if err := ts.CreateRelPropertyIndexWithOptions(piType, "w", el); err != nil {
		t.Fatal(err)
	}
	if err := ts.CreatePropertyIndexWithOptions(piSignal, "sev", el); err != nil {
		t.Fatal(err)
	}
	for name, s := range allShardStoresForTest(t, ts) {
		if o, ok := relIndexOn(s); !ok || !o.Elements {
			t.Errorf("shard %s rel index = %+v, %v; want Elements", name, o, ok)
		}
	}
	if got := w.relLookup(t, "a"); !slices.Equal(got, []string{"evA", "list"}) {
		t.Errorf("Elements lookup a = %v, want [evA list]", got)
	}
	if got := w.nodeLookup(t, int64(9)); !slices.Equal(got, []string{"sl"}) {
		t.Errorf("Elements node lookup 9 = %v, want [sl]", got)
	}
	if got := w.nodeLookup(t, int64(1)); !slices.Equal(got, []string{"s1", "s3", "sl"}) {
		t.Errorf("Elements node lookup 1 = %v, want [s1 s3 sl]", got)
	}
	if got, err := w.relMembers("a"); err != nil || !slices.Equal(got, []string{"evA", "list"}) {
		t.Errorf("Elements rel members of a = %v, %v; want [evA list]", got, err)
	}
	// A shard without its copy answers the same "contains" match by a scan.
	ws := eventShardNamed(t, ts, w.first).store
	if err := ws.DropRelPropertyIndex(piType, "w"); err != nil {
		t.Fatal(err)
	}
	if err := ws.DropPropertyIndex(piSignal, "sev"); err != nil {
		t.Fatal(err)
	}
	if got := w.relLookup(t, "a"); !slices.Equal(got, []string{"evA", "list"}) {
		t.Errorf("Elements lookup a with a shard lacking the index = %v, want [evA list]", got)
	}
	if got := w.nodeLookup(t, int64(9)); !slices.Equal(got, []string{"sl"}) {
		t.Errorf("Elements node lookup 9 with a shard lacking the index = %v, want [sl]", got)
	}
}

// A closed cold shard is reached by the DDL and by the reads, and a lazy
// reopen keeps the definitions.
func TestTieredStorePropertyIndex_ClosedColdShard(t *testing.T) {
	t.Parallel()
	ts := newDiskTestTieredStore(t) // an in-memory shard loses its rows on close
	w := newPropIndexWorld(t, ts)
	demoteToCold(ts, w.first)
	es := eventShardNamed(t, ts, w.first)
	closeShard := func() {
		t.Helper()
		es.shardMu.Lock()
		defer es.shardMu.Unlock()
		if es.store != nil {
			if err := es.store.Close(); err != nil {
				t.Fatal(err)
			}
			es.store = nil
		}
	}
	closeShard()
	if err := ts.CreateRelPropertyIndex(piType, "w"); err != nil {
		t.Fatalf("create with a closed cold shard: %v", err)
	}
	if err := ts.CreatePropertyIndex(piSignal, "sev"); err != nil {
		t.Fatalf("create node index with a closed cold shard: %v", err)
	}
	closeShard()
	// The sidecar union reopens the cold shard, whose definitions survived the
	// close and whose sidecar answers: no decline.
	if got, err := w.relMembers(int64(1)); err != nil || !slices.Equal(got, []string{"ev1", "hot1", "ref1"}) {
		t.Errorf("members with the cold shard closed = %v, %v; want [ev1 hot1 ref1]", got, err)
	}
	if got := w.nodeLookup(t, int64(1)); !slices.Equal(got, []string{"s1", "s3"}) {
		t.Errorf("node lookup = %v, want [s1 s3]", got)
	}
}

// A step that fails mid fan-out leaves every shard as before (create and drop,
// failing at the last event shard and at the reference shard).
func TestTieredStorePropertyIndex_FanOutFaultRollsBack(t *testing.T) {
	t.Parallel()
	errInjected := errors.New("injected shard failure")
	kinds := []struct {
		name   string
		create func(*Store) error
		drop   func(*Store) error
		has    func(*BadgerStore) bool
	}{
		{"rel property",
			func(ts *Store) error { return ts.CreateRelPropertyIndex(piType, "w") },
			func(ts *Store) error { return ts.DropRelPropertyIndex(piType, "w") },
			func(s *BadgerStore) bool { _, ok := relIndexOn(s); return ok }},
		{"node property",
			func(ts *Store) error { return ts.CreatePropertyIndex(piSignal, "sev") },
			func(ts *Store) error { return ts.DropPropertyIndex(piSignal, "sev") },
			func(s *BadgerStore) bool { _, ok := nodeIndexOn(s); return ok }},
	}
	for _, k := range kinds {
		for _, at := range []string{"last event shard", "reference"} {
			for _, create := range []bool{true, false} {
				verb := map[bool]string{true: "create", false: "drop"}[create]
				t.Run(k.name+"/"+verb+"/"+at, func(t *testing.T) {
					ts := newTestTieredStore(t)
					installDefaultTestLabelRegistry(t, ts)
					forceRotation(t, ts)
					forceRotation(t, ts)
					if !create {
						if err := k.create(ts); err != nil {
							t.Fatalf("setup create: %v", err)
						}
					}
					ts.mu.RLock()
					total := 1 + len(ts.eventShards)
					ts.mu.RUnlock()
					ts.shardIdxFault = func(pos int, shard string) error {
						if (at == "reference" && shard == "reference") || (at == "last event shard" && pos == total-2) {
							return errInjected
						}
						return nil
					}
					op := k.create
					if !create {
						op = k.drop
					}
					if err := op(ts); !errors.Is(err, errInjected) {
						t.Fatalf("%s: err = %v, want the injected error", verb, err)
					}
					ts.shardIdxFault = nil
					for name, s := range allShardStoresForTest(t, ts) {
						if got := k.has(s); got != !create {
							t.Errorf("shard %s carries the index = %v after a failed %s, want %v", name, got, verb, !create)
						}
					}
				})
			}
		}
	}
}

// An interrupted create left a definition with OTHER options on a shard: the
// re-run create must replace it, never tolerate it as already done.
func TestTieredStorePropertyIndex_RerunCreateReplacesStaleOptions(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	w := newPropIndexWorld(t, ts)
	hot := ts.hotShard.store
	if err := hot.CreateRelPropertyIndexWithOptions(piType, "w", storecontract.PropertyIndexOptions{Elements: true}); err != nil {
		t.Fatal(err)
	}
	if err := ts.CreateRelPropertyIndex(piType, "w"); err != nil {
		t.Fatalf("create over a stale shard definition: %v", err)
	}
	for name, s := range allShardStoresForTest(t, ts) {
		if o, ok := relIndexOn(s); !ok || o.Elements {
			t.Errorf("shard %s index = %+v, %v; want plain", name, o, ok)
		}
	}
	if got, err := w.relMembers(int64(1)); err != nil || !slices.Equal(got, []string{"ev1", "hot1", "ref1"}) {
		t.Errorf("rel members of 1 = %v, %v; want [ev1 hot1 ref1]", got, err)
	}
}

// Store open (disk) repairs every open shard to the anchor: an orphan
// definition (create interrupted before the anchor), a missing one (drop
// interrupted after some shards), other options.
func TestTieredStorePropertyIndex_InterruptedDDLRepairedAtOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := Config{DataDir: dir, RefLabels: []string{"Case", "User"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.CreateRelPropertyIndex(piType, "w"); err != nil {
		t.Fatal(err)
	}
	if err := ts.CreatePropertyIndexWithOptions(piSignal, "sev", storecontract.PropertyIndexOptions{Elements: true}); err != nil {
		t.Fatal(err)
	}
	hot := ts.hotShard.store
	if err := hot.CreateRelPropertyIndex(piType, "orphan"); err != nil {
		t.Fatal(err)
	}
	if err := hot.DropRelPropertyIndex(piType, "w"); err != nil {
		t.Fatal(err)
	}
	if err := hot.DropPropertyIndex(piSignal, "sev"); err != nil {
		t.Fatal(err)
	}
	if err := hot.CreatePropertyIndex(piSignal, "sev"); err != nil { // other options
		t.Fatal(err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	ts, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	for name, s := range allShardStoresForTest(t, ts) {
		if _, ok := relIndexOn(s); !ok {
			t.Errorf("shard %s lost the anchored rel index", name)
		}
		if has, _ := s.HasRelPropertyIndex(piType, "orphan"); has {
			t.Errorf("shard %s kept the orphan rel index", name)
		}
		if o, ok := nodeIndexOn(s); !ok || !o.Elements {
			t.Errorf("shard %s node index = %+v, %v; want the anchor's Elements index", name, o, ok)
		}
	}
	// Re-running the interrupted create reaches the shards already carrying it.
	if err := ts.CreateRelPropertyIndex(piType, "orphan"); err != nil {
		t.Fatalf("create orphan: %v", err)
	}
}

// An ArchiveNode / RestoreNode between a door's reference and archive reads
// never shows the moved node's rel twice or not at all.
func TestTieredStorePropertyIndex_ArchiveMoveDuringRead(t *testing.T) {
	for _, archived := range []bool{false, true} {
		for _, door := range []string{"lookup", "members"} {
			name := map[bool]string{false: "ArchiveNode", true: "RestoreNode"}[archived] + "/" + door
			t.Run(name, func(t *testing.T) {
				ts := newTestTieredStore(t)
				w := newPropIndexWorld(t, ts)
				if err := ts.CreateRelPropertyIndex(piType, "w"); err != nil {
					t.Fatal(err)
				}
				c1 := w.nodes["c1"].ID()
				if archived {
					if err := ts.ArchiveNode(c1); err != nil {
						t.Fatal(err)
					}
				}
				measure := func() []string {
					if door == "lookup" {
						return w.relLookup(t, int64(1))
					}
					got, err := w.relMembers(int64(1))
					if err != nil {
						t.Fatal(err)
					}
					return got
				}
				want := measure()
				moved := make(chan error, 1)
				var fired atomic.Bool
				ts.fanoutRefReadHook = func() {
					if !fired.CompareAndSwap(false, true) {
						return
					}
					go func() {
						if archived {
							moved <- ts.RestoreNode(c1)
						} else {
							moved <- ts.ArchiveNode(c1)
						}
					}()
					select {
					case err := <-moved:
						moved <- err
					case <-time.After(30 * time.Millisecond):
					}
				}
				got := measure()
				if !fired.Load() {
					t.Fatal("the door never reached its archive read")
				}
				if err := <-moved; err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(got, want) {
					t.Fatalf("during the move: %v, want %v", got, want)
				}
			})
		}
	}
}

// A retention drop removes a whole event shard: its rows leave the answers and
// the sidecar union stays answerable.
func TestTieredStorePropertyIndex_RetentionDrop(t *testing.T) {
	t.Parallel()
	ts := newPersistentTieredStore(t)
	const relType = piType
	gen, rgen := tieredNodeGen(t), tieredRelGen(t)
	mk := func(label uint16, sev any) *types.Node {
		n := types.NewNode(types.NodeID(gen.Generate()), label, nil)
		if sev != nil {
			if err := n.SetProperty("sev", sev); err != nil {
				t.Fatal(err)
			}
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	old1, old2 := mk(piSignal, int64(1)), mk(piSignal, int64(1))
	oldRel := types.NewRelationship(types.RelID(rgen.Generate()), relType, old1.ID(), old2.ID())
	if err := oldRel.SetProperty("w", int64(1)); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelationship(oldRel); err != nil {
		t.Fatal(err)
	}
	if err := ts.CreateRelPropertyIndex(relType, "w"); err != nil {
		t.Fatal(err)
	}
	if err := ts.CreatePropertyIndex(piSignal, "sev"); err != nil {
		t.Fatal(err)
	}
	if err := ts.ForceRotate(); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.PurgeNodesByLabelBefore(piSignal, types.Instant(1<<50), 8); err != nil {
		t.Fatalf("purge: %v", err)
	}
	if rs, err := ts.RelationshipsByTypeAndProperty(relType, "w", int64(1), storecontract.QueryOpts{}); err != nil || len(rs) != 0 {
		t.Errorf("rel lookup after the drop = %d rels, %v; want none", len(rs), err)
	}
	if ns, err := ts.NodesByLabelAndProperty(piSignal, "sev", int64(1), storecontract.QueryOpts{}); err != nil || len(ns) != 0 {
		t.Errorf("node lookup after the drop = %d nodes, %v; want none", len(ns), err)
	}
	for _, vk := range types.EqualIndexKeys(int64(1)) {
		if err := ts.ForEachRelPropertyTxMember(relType, "w", vk, func(types.RelID, types.Instant) bool { return true }); err != nil {
			t.Errorf("rel sidecar after the drop: %v", err)
		}
	}
}
