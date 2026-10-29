package badger

import (
	"fmt"
	"slices"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// entryIDs are the node IDs of key's on-disk entries for value, the write
// buffer's included.
func entryIDs(t *testing.T, bs *Store, key string, value any) []types.NodeID {
	t.Helper()
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	nids, err := bs.propertyIndexDiskKeysCandidatesLocked(key, []string{types.IndexablePropertyValueKey(value)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(nids)
	return nids
}

// TestPropertyIndexValidEnd_RefineGuards (VALIDENDPRUNE): a refined bound
// prunes its entry; a refine is skipped when a row of its node was written
// after the lookup read its sequence, or the store was cleared since, and
// writes only the entries of the node's labels; a history row written alone
// makes the node's entries unknown again, one written with the current row
// leaves its entries to the row's write.
func TestPropertyIndexValidEnd_RefineGuards(t *testing.T) {
	t.Parallel()
	names := registrypkg.NewLabelRegistry()
	d, _ := names.GetOrCreate("D")
	e, _ := names.GetOrCreate("E")
	f, _ := names.GetOrCreate("F")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), LabelNames: names,
		PropertyIndexOnDiskLabels: []string{"D", "E", "F"},
		PropertyIndexValidEnd:     []storecontract.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_"}, {Label: "E", KeyPrefix: "ov_"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	for _, def := range []struct {
		label uint16
		key   string
	}{{d, "ov_h"}, {e, "ov_e"}} {
		if err := bs.CreatePropertyIndex(def.label, def.key); err != nil {
			t.Fatal(err)
		}
	}
	row := func(id int64) *types.Node {
		n := types.NewNode(types.NodeID(id), d, nil)
		if err := n.SetProperty("ov_h", "a"); err != nil {
			t.Fatal(err)
		}
		if err := n.SetProperty("ov_e", "b"); err != nil { // E's key on a D node: no entry
			t.Fatal(err)
		}
		return n
	}
	put := func(id int64) {
		t.Helper()
		if err := bs.PutNode(row(id)); err != nil {
			t.Fatal(err)
		}
	}
	lookup := func(bound types.Instant) ([]*types.Node, []storecontract.ValidEndProbe) {
		t.Helper()
		nodes, probes, err := bs.NodesByLabelAndPropertyKeysValidEnd(d, "ov_h", []string{types.IndexablePropertyValueKey("a")},
			QueryOpts{ValidEndAfter: bound, ValidEndAfterSet: true})
		if err != nil {
			t.Fatal(err)
		}
		if len(probes) != len(nodes) {
			t.Fatalf("%d probes for %d nodes", len(probes), len(nodes))
		}
		return nodes, probes
	}
	ids := func(bound types.Instant) string {
		t.Helper()
		nodes, _ := lookup(bound)
		var out []types.NodeID
		for _, n := range nodes {
			out = append(out, n.ID())
		}
		return fmt.Sprint(out)
	}
	put(1)
	put(2)
	nodes, probes := lookup(500)
	if len(nodes) != 2 || probes[0].Known || probes[1].Known {
		t.Fatalf("new entries: %d nodes, probes %+v, want 2 without a bound", len(nodes), probes)
	}
	for i, n := range nodes {
		bs.RefineNodePropertyValidEnd(n, probes[i], 100)
	}
	if got := ids(500); got != "[]" {
		t.Fatalf("refined to 100, bound 500: %s, want both pruned", got)
	}
	if got := entryIDs(t, bs, "ov_e", "b"); len(got) != 0 {
		t.Errorf("refined D nodes: E's key has entries for %v", got)
	}
	if got := ids(100); got != "[1 2]" {
		t.Fatalf("refined to 100, bound 100: %s, want both (an end at the bound counts)", got)
	}
	nodes, probes = lookup(50)
	if len(nodes) != 2 || !probes[0].Known || probes[0].Bound != 100 {
		t.Fatalf("refined to 100, bound 50: %d nodes, probes %+v, want both with bound 100", len(nodes), probes)
	}

	if err := bs.ReplaceNode(row(1)); err != nil { // after the lookup: its refine is stale
		t.Fatal(err)
	}
	before := bs.PropertyIndexValidEndStats()
	bs.RefineNodePropertyValidEnd(nodes[0], probes[0], 100)
	if st := bs.PropertyIndexValidEndStats(); st.RefineSkipped != before.RefineSkipped+1 || st.Refined != before.Refined {
		t.Errorf("refine after a write: %+v, before %+v: want it skipped", st, before)
	}
	if got := ids(500); got != "[1]" {
		t.Errorf("node 1 rewritten: %s, want it read again, node 2 pruned", got)
	}

	nodes, probes = lookup(500)
	bs.RefineNodePropertyValidEnd(nodes[0], probes[0], 100)
	if got := ids(500); got != "[]" {
		t.Fatalf("node 1 refined again: %s, want both pruned", got)
	}
	hist := row(2)
	hist.SetVersion(1)
	if err := bs.PutNodeVersion(2, 1, hist); err != nil { // a history row alone
		t.Fatal(err)
	}
	if got := ids(500); got != "[2]" {
		t.Errorf("a history row of node 2 written: %s, want node 2 read again", got)
	}

	// A history row written with the current row: the row's write decides
	// the entries (the old value's goes).
	moved := row(2)
	if err := moved.SetProperty("ov_h", "c"); err != nil {
		t.Fatal(err)
	}
	moved.SetVersion(1)
	if err := bs.ReplaceNodeWithHistory(moved, 0, row(2)); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(entryIDs(t, bs, "ov_h", "a"), entryIDs(t, bs, "ov_h", "c")); got != "[1] [2]" {
		t.Errorf("node 2 moved from a to c with its history row: entries %s, want [1] [2]", got)
	}

	// A definition not listed (another key, another label) answers its
	// matches without probes; a dropped one refines nothing.
	for i, def := range []struct {
		label uint16
		key   string
	}{{d, "x"}, {f, "ov_h"}} {
		if err := bs.CreatePropertyIndex(def.label, def.key); err != nil {
			t.Fatal(err)
		}
		n := types.NewNode(types.NodeID(10+i), def.label, nil)
		if err := n.SetProperty(def.key, "a"); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
		if nodes, probes, err := bs.NodesByLabelAndPropertyKeysValidEnd(def.label, def.key, []string{types.IndexablePropertyValueKey("a")},
			QueryOpts{ValidEndAfter: 1, ValidEndAfterSet: true}); err != nil || len(nodes) != 1 || probes != nil {
			t.Errorf("unlisted %d/%s: %d nodes, probes %v, %v, want the node without a probe", def.label, def.key, len(nodes), probes, err)
		}
	}
	nodes, probes = lookup(50)
	if err := bs.DropPropertyIndex(d, "ov_h"); err != nil {
		t.Fatal(err)
	}
	before = bs.PropertyIndexValidEndStats()
	bs.RefineNodePropertyValidEnd(nodes[0], probes[0], 100)
	if st := bs.PropertyIndexValidEndStats(); st != before {
		t.Errorf("refine after the drop: %+v, before %+v: want nothing", st, before)
	}
	if err := bs.CreatePropertyIndex(d, "ov_h"); err != nil {
		t.Fatal(err)
	}

	nodes, probes = lookup(50)
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(d, "ov_h"); err != nil {
		t.Fatal(err)
	}
	put(3)
	before = bs.PropertyIndexValidEndStats()
	bs.RefineNodePropertyValidEnd(nodes[0], probes[0], 100)
	if st := bs.PropertyIndexValidEndStats(); st.RefineSkipped != before.RefineSkipped+1 {
		t.Errorf("refine read before Clear: %+v, before %+v: want it skipped", st, before)
	}
	if got := ids(0); got != "[3]" {
		t.Errorf("after Clear: %s, want only node 3", got)
	}
}

// TestPropertyIndexValidEnd_ProbeFromEarlierOpen (VALIDENDPRUNE): a probe read
// before the store was closed and opened again is refused once the node was
// written on the new open, whatever the sequences counted.
func TestPropertyIndexValidEnd_ProbeFromEarlierOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	names := registrypkg.NewLabelRegistry()
	d, _ := names.GetOrCreate("D")
	keys := registrypkg.NewPropertyKeyRegistry()
	open := func() *Store {
		t.Helper()
		bs, err := New(Config{Dir: dir, PropertyKeyRegistry: keys, LabelNames: names, PropertyIndexOnDiskLabels: []string{"D"},
			PropertyIndexValidEnd: []storecontract.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_"}}})
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}
	row := func(id int64) *types.Node {
		n := types.NewNode(types.NodeID(id), d, nil)
		if err := n.SetProperty("ov_h", "a"); err != nil {
			t.Fatal(err)
		}
		return n
	}
	bs := open()
	if err := bs.CreatePropertyIndex(d, "ov_h"); err != nil {
		t.Fatal(err)
	}
	for id := range int64(8) {
		if err := bs.PutNode(row(id + 1)); err != nil {
			t.Fatal(err)
		}
	}
	nodes, probes, err := bs.NodesByLabelAndPropertyKeysValidEnd(d, "ov_h", []string{types.IndexablePropertyValueKey("a")},
		QueryOpts{ValidEndAfter: 1, ValidEndAfterSet: true})
	if err != nil || len(nodes) != 8 {
		t.Fatalf("%d nodes, %v", len(nodes), err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs = open()
	if err := bs.ReplaceNode(row(1)); err != nil {
		t.Fatal(err)
	}
	bs.RefineNodePropertyValidEnd(nodes[0], probes[0], 0)
	if st := bs.PropertyIndexValidEndStats(); st.RefineSkipped != 1 || st.Refined != 0 {
		t.Errorf("refine read on the earlier open: %+v, want it skipped", st)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	// A read-only open trusts the marked definition but refines nothing.
	bs, err = New(Config{Dir: dir, ReadOnly: true, PropertyKeyRegistry: keys, LabelNames: names, PropertyIndexOnDiskLabels: []string{"D"},
		PropertyIndexValidEnd: []storecontract.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	nodes, probes, err = bs.NodesByLabelAndPropertyKeysValidEnd(d, "ov_h", []string{types.IndexablePropertyValueKey("a")},
		QueryOpts{ValidEndAfter: 1, ValidEndAfterSet: true})
	if err != nil || len(nodes) != 8 || probes == nil || probes[0].Known {
		t.Fatalf("read-only: %d nodes, probes %v, %v: want 8 with unknown bounds", len(nodes), probes, err)
	}
	bs.RefineNodePropertyValidEnd(nodes[0], probes[0], 0)
	if st := bs.PropertyIndexValidEndStats(); st.Refined != 0 || st.RefineSkipped != 0 {
		t.Errorf("read-only refine: %+v, want nothing", st)
	}
}

// TestPropertyIndexValidEnd_NoRegistry (VALIDENDPRUNE): with every property
// index on disk and no label names (none given, none persisted), a listed
// label cannot be named: its definition keeps no bounds.
func TestPropertyIndexValidEnd_NoRegistry(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), PropertyIndexOnDisk: true,
		PropertyIndexValidEnd: []storecontract.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if err := bs.CreatePropertyIndex(1, "ov_h"); err != nil {
		t.Fatal(err)
	}
	n := types.NewNode(1, 1, nil)
	if err := n.SetProperty("ov_h", "a"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	nodes, probes, err := bs.NodesByLabelAndPropertyKeysValidEnd(1, "ov_h", []string{types.IndexablePropertyValueKey("a")},
		QueryOpts{ValidEndAfter: 1, ValidEndAfterSet: true})
	if err != nil || len(nodes) != 1 || probes != nil {
		t.Errorf("%d nodes, probes %v, %v: want the node, no probes", len(nodes), probes, err)
	}
	bs.RefineNodePropertyValidEnd(n, storecontract.ValidEndProbe{}, 0) // no definition trusted
	if st := bs.PropertyIndexValidEndStats(); st != (storecontract.PropertyIndexValidEndStats{}) {
		t.Errorf("refine without a trusted definition: %+v, want nothing", st)
	}
}

// TestPropertyIndexValidEnd_Buffers (VALIDENDPRUNE): a bound is read from the
// newest of the write buffer, the buffer being flushed and the disk; a history
// row of a deleted node writes no entry; a row other than a node's (a
// relationship with the node's ID) leaves its refine alone.
func TestPropertyIndexValidEnd_Buffers(t *testing.T) {
	t.Parallel()
	names := registrypkg.NewLabelRegistry()
	d, _ := names.GetOrCreate("D")
	bs := newFlushParkStore(t, func(cfg *Config) {
		cfg.FlushInterval = time.Hour
		cfg.PropertyKeyRegistry = registrypkg.NewPropertyKeyRegistry()
		cfg.LabelNames = names
		cfg.PropertyIndexOnDiskLabels = []string{"D"}
		cfg.PropertyIndexValidEnd = []storecontract.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_"}}
	})
	if err := bs.CreatePropertyIndex(d, "ov_h"); err != nil {
		t.Fatal(err)
	}
	row := func(id int64) *types.Node {
		n := types.NewNode(types.NodeID(id), d, nil)
		if err := n.SetProperty("ov_h", "a"); err != nil {
			t.Fatal(err)
		}
		return n
	}
	ids := func(bound types.Instant) string {
		t.Helper()
		nodes, _, err := bs.NodesByLabelAndPropertyKeysValidEnd(d, "ov_h", []string{types.IndexablePropertyValueKey("a")},
			QueryOpts{ValidEndAfter: bound, ValidEndAfterSet: true})
		if err != nil {
			t.Fatal(err)
		}
		var out []types.NodeID
		for _, n := range nodes {
			out = append(out, n.ID())
		}
		return fmt.Sprint(out)
	}
	refine := func() {
		t.Helper()
		nodes, probes, err := bs.NodesByLabelAndPropertyKeysValidEnd(d, "ov_h", []string{types.IndexablePropertyValueKey("a")},
			QueryOpts{ValidEndAfter: 500, ValidEndAfterSet: true})
		if err != nil {
			t.Fatal(err)
		}
		for i, n := range nodes {
			bs.RefineNodePropertyValidEnd(n, probes[i], 100)
		}
	}
	flush := func() {
		t.Helper()
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	for id := range int64(2) {
		if err := bs.PutNode(row(id + 1)); err != nil {
			t.Fatal(err)
		}
	}
	refine()
	parkPendingIntoFlushing(t, bs) // the bounds in the buffer being flushed
	if err := bs.ReplaceNode(row(1)); err != nil {
		t.Fatal(err)
	}
	if got := ids(500); got != "[1]" {
		t.Errorf("node 1 written over a bound being flushed: %s, want it read", got)
	}
	commitFlushingToBadger(t, bs)
	flush()
	refine()
	flush() // the bounds on disk
	if err := bs.ReplaceNode(row(1)); err != nil {
		t.Fatal(err)
	}
	if got := ids(500); got != "[1]" {
		t.Errorf("node 1 written over a bound on disk: %s, want it read", got)
	}

	// A relationship's row with node 2's ID: node 2's refine stands.
	nodes, probes, err := bs.NodesByLabelAndPropertyKeysValidEnd(d, "ov_h", []string{types.IndexablePropertyValueKey("a")},
		QueryOpts{ValidEndAfter: 1, ValidEndAfterSet: true})
	if err != nil || len(nodes) != 2 {
		t.Fatalf("%d nodes, %v", len(nodes), err)
	}
	if err := bs.PutRelationship(types.NewRelationship(2, 1, 1, 2)); err != nil {
		t.Fatal(err)
	}
	before := bs.PropertyIndexValidEndStats()
	bs.RefineNodePropertyValidEnd(nodes[1], probes[1], 100)
	if st := bs.PropertyIndexValidEndStats(); st.Refined != before.Refined+1 {
		t.Errorf("refine after a relationship write: %+v, before %+v: want it refined", st, before)
	}

	// Node 3, flushed, then deleted: a history row written after writes no
	// entry for it.
	if err := bs.PutNode(row(3)); err != nil {
		t.Fatal(err)
	}
	flush()
	if err := bs.DeleteNode(3); err != nil {
		t.Fatal(err)
	}
	hist := row(3)
	hist.SetVersion(1)
	if err := bs.PutNodeVersion(3, 1, hist); err != nil {
		t.Fatal(err)
	}
	if got := entryIDs(t, bs, "ov_h", "a"); slices.Contains(got, 3) {
		t.Errorf("deleted node 3's history row: entries %v, want none for node 3", got)
	}
}

// TestPropertyIndexValidEnd_PersistedNames (VALIDENDPRUNE): with every
// property index on disk, an open given no label names names the listed label
// from the registry the store persisted, and keeps trusting its definition.
func TestPropertyIndexValidEnd_PersistedNames(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	names := registrypkg.NewLabelRegistry()
	d, _ := names.GetOrCreate("D")
	keys := registrypkg.NewPropertyKeyRegistry()
	open := func(labels *registrypkg.LabelRegistry) *Store {
		t.Helper()
		bs, err := New(Config{Dir: dir, PropertyKeyRegistry: keys, LabelNames: labels, PropertyIndexOnDisk: true,
			PropertyIndexValidEnd: []storecontract.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_"}}})
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}
	bs := open(names)
	if err := bs.CreatePropertyIndex(d, "ov_h"); err != nil {
		t.Fatal(err)
	}
	n := types.NewNode(1, d, nil)
	if err := n.SetProperty("ov_h", "a"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.SaveLabelRegistry(names); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs = open(nil)
	defer bs.Close()
	_, probes, err := bs.NodesByLabelAndPropertyKeysValidEnd(d, "ov_h", []string{types.IndexablePropertyValueKey("a")},
		QueryOpts{ValidEndAfter: 1, ValidEndAfterSet: true})
	if err != nil || len(probes) != 1 {
		t.Errorf("reopened without label names: probes %v, %v: want the definition trusted", probes, err)
	}
}

// TestPropertyIndexValidEnd_Rejoin (VALIDENDPRUNE): a label that leaves the
// on-disk list and joins again has its key rebuilt; the bounded entries the
// rebuild drops as stale stay dropped (the open does not write them unknown).
func TestPropertyIndexValidEnd_Rejoin(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	names := registrypkg.NewLabelRegistry()
	d, _ := names.GetOrCreate("D")
	keys := registrypkg.NewPropertyKeyRegistry()
	open := func(onDisk bool) *Store {
		t.Helper()
		cfg := Config{Dir: dir, PropertyKeyRegistry: keys, LabelNames: names,
			PropertyIndexValidEnd: []storecontract.PropertyIndexValidEnd{{Label: "D", KeyPrefix: "ov_"}}}
		if onDisk {
			cfg.PropertyIndexOnDiskLabels = []string{"D"}
		}
		bs, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}
	row := func(value string) *types.Node {
		n := types.NewNode(1, d, nil)
		if err := n.SetProperty("ov_h", value); err != nil {
			t.Fatal(err)
		}
		return n
	}
	bs := open(true)
	if err := bs.CreatePropertyIndex(d, "ov_h"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(row("a")); err != nil {
		t.Fatal(err)
	}
	nodes, probes, err := bs.NodesByLabelAndPropertyKeysValidEnd(d, "ov_h", []string{types.IndexablePropertyValueKey("a")},
		QueryOpts{ValidEndAfter: 1, ValidEndAfterSet: true})
	if err != nil || len(nodes) != 1 {
		t.Fatalf("%d nodes, %v", len(nodes), err)
	}
	bs.RefineNodePropertyValidEnd(nodes[0], probes[0], 100)
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs = open(false) // D's index in RAM: the bounded entry of "a" goes stale
	if err := bs.ReplaceNode(row("b")); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs = open(true)
	defer bs.Close()
	if got := fmt.Sprint(entryIDs(t, bs, "ov_h", "a"), entryIDs(t, bs, "ov_h", "b")); got != "[] [1]" {
		t.Errorf("rejoined: entries %s, want [] [1]", got)
	}
}
