package graph_test

// Item C (scan-temporal-opts), testing rules 15-17: the column scans
// (ScanNodeColumns / ScanRelColumns) and the numeric range scans
// (Nodes().ForEachByLabelPropertyRange / Rels().ForEachByTypePropertyRange) are
// doors with the same shape as ByLabel / ByType. Under a temporal QueryOpts they
// must answer EXACTLY what ByLabel(opts) / ByType(opts) answer — the version each
// entity had under the opts, history included — or fail closed. Never the CURRENT
// rows filtered by the opts' valid-time fields with TxAt / TxPin dropped.
//
// Faulty implementations these tests catch (every assertion is a break case):
//   - "current-row push-down": the door forwards opts to the store, which filters
//     LIVE rows by valid time. It drops an updated entity's old version (ValidAt
//     before the update), a deleted entity (no live row), a node whose label was
//     removed later (rule 16: the label held only on an earlier version), and
//     yields the live value instead of the value at t.
//   - "transaction-time pins ignored": storeutil.HasTemporalFilter checks valid
//     time only, so a TxAt / TxPin scan takes the unfiltered current-row shortcut:
//     it over-reports an entity created after the pin and misses one deleted after
//     it.
//   - "no validation": the column door accepts TxPin together with ValidAt
//     instead of returning ErrConflictingTemporalOpts.
//   - "pagination dropped": the exact path ignores Limit.
//   - "range door needs the index under a temporal opt": the ordered sibling serves
//     temporal opts by a full fold without an index; the unordered door must not
//     answer ErrIndexNotFound (or silently current rows) instead.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// scanOptsFixture is the adversarial dataset: per entity one lifecycle.
//
//	A  value 10 valid from 1000, updated to 20 valid from 2000      (before the pin)
//	B  value 30 valid from 1000, DELETED after the pin
//	C  value 40 valid from 1000, CLOSED at 1800 after the pin
//	D  value 50 valid from 1000, label S REMOVED after the pin       (nodes only)
//	E  value 70 valid from 1000, CREATED after the pin
//	F  value 60 valid from 1000, never touched
//
// pin is the transaction time of the last write before the post-pin mutations
// (derived from the entities' own TxFrom, never the wall clock — lesson 60).
type scanOptsFixture struct {
	pin   types.Instant
	far   types.Instant
	nodes map[types.NodeID]string
	rels  map[types.RelID]string
}

const (
	scanLabel   = "S"
	scanRelType = "R"
	scanNodeKey = "score"
	scanRelKey  = "w"
)

func buildScanOptsFixture(t *testing.T, g *graphpkg.Graph) scanOptsFixture {
	t.Helper()
	ctx := context.Background()
	fx := scanOptsFixture{nodes: map[types.NodeID]string{}, rels: map[types.RelID]string{}}

	// Indexes first so the non-temporal fast paths exist where a backend has them.
	// A backend without (rel) property indexes declines; the temporal doors under
	// test must not depend on an index.
	if err := g.Index().CreateProperty(scanLabel, scanNodeKey); err != nil {
		t.Logf("CreateProperty: %v (backend declines; temporal path needs no index)", err)
	}
	if err := g.Index().CreateRelProperty(scanRelType, scanRelKey); err != nil {
		t.Logf("CreateRelProperty: %v (backend declines; temporal path needs no index)", err)
	}

	addNode := func(name string, labels []string, v int64) *types.Node {
		n, err := g.Nodes().Add(ctx, labels, map[string]any{scanNodeKey: v, "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatalf("add node %s: %v", name, err)
		}
		fx.nodes[n.ID()] = name
		return n
	}
	hub, err := g.Nodes().Add(ctx, []string{"H"}, nil)
	if err != nil {
		t.Fatalf("add hub: %v", err)
	}
	tgt, err := g.Nodes().Add(ctx, []string{"H"}, nil)
	if err != nil {
		t.Fatalf("add tgt: %v", err)
	}
	addRel := func(name string, v int64) *types.Relationship {
		r, err := g.Rels().Add(ctx, scanRelType, hub, tgt, map[string]any{scanRelKey: v, "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatalf("add rel %s: %v", name, err)
		}
		fx.rels[r.ID()] = name
		return r
	}

	nA := addNode("A", []string{scanLabel}, 10)
	nB := addNode("B", []string{scanLabel}, 30)
	nC := addNode("C", []string{scanLabel}, 40)
	nD := addNode("D", []string{scanLabel, "Keep"}, 50)
	addNode("F", []string{scanLabel}, 60)
	rA := addRel("A", 10)
	rB := addRel("B", 30)
	rC := addRel("C", 40)
	addRel("F", 60)
	if _, err := g.Nodes().Update(ctx, nA.ID(), map[string]any{scanNodeKey: int64(20), "tkg_valid_from": types.Instant(2000)}); err != nil {
		t.Fatalf("update node A: %v", err)
	}
	last, err := g.Rels().Update(ctx, rA.ID(), map[string]any{scanRelKey: int64(20), "tkg_valid_from": types.Instant(2000)})
	if err != nil {
		t.Fatalf("update rel A: %v", err)
	}
	// The pin is the recorded transaction time of the last pre-pin write: the
	// clock is monotonic, so every write below is stamped strictly later.
	fx.pin = last.Temporal().TxFrom
	if fx.pin == 0 {
		t.Fatal("rel update carries no TxFrom")
	}

	if err := g.Nodes().Delete(ctx, nB.ID()); err != nil {
		t.Fatalf("delete node B: %v", err)
	}
	if err := g.Rels().Delete(ctx, rB.ID()); err != nil {
		t.Fatalf("delete rel B: %v", err)
	}
	if err := g.Nodes().CloseVersion(ctx, nC.ID(), 1800); err != nil {
		t.Fatalf("close node C: %v", err)
	}
	if err := g.Rels().CloseVersion(ctx, rC.ID(), 1800); err != nil {
		t.Fatalf("close rel C: %v", err)
	}
	if err := g.Nodes().RemoveLabel(ctx, nD.ID(), scanLabel); err != nil {
		t.Fatalf("remove label from D: %v", err)
	}
	addNode("E", []string{scanLabel}, 70)
	addRel("E", 70)

	fx.far = types.Instant(time.Now().Add(time.Hour).UnixMilli())
	return fx
}

// scanOptsCase is one temporal QueryOpts with the hand-derived answer (entity
// name -> value) for nodes and for relationships.
type scanOptsCase struct {
	name  string
	opts  graphpkg.QueryOpts
	nodes map[string]int64
	rels  map[string]int64
}

func scanOptsCases(fx scanOptsFixture) []scanOptsCase {
	return []scanOptsCase{
		{
			// Before A's update: A is 10 (the live row starts at 2000), B still
			// exists, D still carries S.
			name:  "ValidAt before the update",
			opts:  graphpkg.QueryOpts{ValidAt: 1500},
			nodes: map[string]int64{"A": 10, "B": 30, "C": 40, "D": 50, "E": 70, "F": 60},
			rels:  map[string]int64{"A": 10, "B": 30, "C": 40, "E": 70, "F": 60},
		},
		{
			// Spans A's update: the most recent overlapping version (20).
			name:  "ValidStart/ValidEnd spanning the update",
			opts:  graphpkg.QueryOpts{ValidStart: 1500, ValidEnd: 2500},
			nodes: map[string]int64{"A": 20, "B": 30, "C": 40, "D": 50, "E": 70, "F": 60},
			rels:  map[string]int64{"A": 20, "B": 30, "C": 40, "E": 70, "F": 60},
		},
		{
			// Rule 16: D's most recent overlapping version lacks S; S held only on
			// the earlier version inside the interval, so D is found with it.
			name:  "interval where the label held only on an earlier version",
			opts:  graphpkg.QueryOpts{ValidStart: 1500, ValidEnd: fx.far},
			nodes: map[string]int64{"A": 20, "B": 30, "C": 40, "D": 50, "E": 70, "F": 60},
			rels:  map[string]int64{"A": 20, "B": 30, "C": 40, "E": 70, "F": 60},
		},
		{
			// Bitemporal: valid at 1500 as recorded by the pin. E is not yet
			// recorded; B's delete and C's close are not yet recorded.
			name:  "TxAt before the delete with ValidAt before the update",
			opts:  graphpkg.QueryOpts{ValidAt: 1500, TxAt: fx.pin},
			nodes: map[string]int64{"A": 10, "B": 30, "C": 40, "D": 50, "F": 60},
			rels:  map[string]int64{"A": 10, "B": 30, "C": 40, "F": 60},
		},
		{
			// TxAt alone (valid at now) as recorded by the pin.
			name:  "TxAt before the delete",
			opts:  graphpkg.QueryOpts{TxAt: fx.pin},
			nodes: map[string]int64{"A": 20, "B": 30, "C": 40, "D": 50, "F": 60},
			rels:  map[string]int64{"A": 20, "B": 30, "C": 40, "F": 60},
		},
		{
			// Belief state at the pin.
			name:  "TxPin before the delete",
			opts:  graphpkg.QueryOpts{TxPin: fx.pin},
			nodes: map[string]int64{"A": 20, "B": 30, "C": 40, "D": 50, "F": 60},
			rels:  map[string]int64{"A": 20, "B": 30, "C": 40, "F": 60},
		},
	}
}

// scanRow is what a door reports for one entity: the value and the version's
// valid range.
type scanRow struct {
	v      int64
	vf, vt int64
}

func (r scanRow) String() string { return fmt.Sprintf("{v=%d vf=%d vt=%d}", r.v, r.vf, r.vt) }

func nodeRowsByLabel(t *testing.T, g *graphpkg.Graph, opts graphpkg.QueryOpts) map[types.NodeID]scanRow {
	t.Helper()
	nodes, err := g.Nodes().ByLabel(scanLabel, opts)
	if err != nil {
		t.Fatalf("ByLabel(%+v): %v", opts, err)
	}
	out := map[types.NodeID]scanRow{}
	for _, n := range nodes {
		v, _ := n.GetProperty(scanNodeKey)
		vf, vt, _ := n.ValidRange()
		out[n.ID()] = scanRow{v: v.(int64), vf: int64(vf), vt: int64(vt)}
	}
	return out
}

func relRowsByType(t *testing.T, g *graphpkg.Graph, opts graphpkg.QueryOpts) map[types.RelID]scanRow {
	t.Helper()
	rels, err := g.Rels().ByType(scanRelType, opts)
	if err != nil {
		t.Fatalf("ByType(%+v): %v", opts, err)
	}
	out := map[types.RelID]scanRow{}
	for _, r := range rels {
		v, _ := r.GetProperty(scanRelKey)
		vf, vt, _ := r.ValidRange()
		out[r.ID()] = scanRow{v: v.(int64), vf: int64(vf), vt: int64(vt)}
	}
	return out
}

// columnInt64 reads row i of column c, failing on an absent or non-int column:
// every fixture row carries the property.
func columnInt64(t *testing.T, cd *graphpkg.ColumnBatch, c, i int) int64 {
	t.Helper()
	if cd.Kinds[c] != graphpkg.ColInt64 || cd.Null[c][i] || len(cd.Ints[c]) <= i {
		t.Fatalf("column %d row %d: kind=%v null=%v len=%d, want a present int64", c, i, cd.Kinds[c], cd.Null[c][i], len(cd.Ints[c]))
	}
	return cd.Ints[c][i]
}

func nodeRowsFromColumns(t *testing.T, g *graphpkg.Graph, opts graphpkg.QueryOpts) (map[types.NodeID]scanRow, bool, error) {
	t.Helper()
	out := map[types.NodeID]scanRow{}
	ok, err := g.ScanNodeColumns(scanLabel, []string{scanNodeKey}, opts, func(b *graphpkg.ColumnBatch) bool {
		for i, id := range b.IDs {
			if _, dup := out[id]; dup {
				t.Errorf("ScanNodeColumns %+v: node %d twice", opts, id)
			}
			out[id] = scanRow{v: columnInt64(t, b, 0, i), vf: b.ValidFrom[i], vt: b.ValidTo[i]}
		}
		return true
	})
	return out, ok, err
}

func relRowsFromColumns(t *testing.T, g *graphpkg.Graph, relType string, opts graphpkg.QueryOpts) (map[types.RelID]scanRow, bool, error) {
	t.Helper()
	out := map[types.RelID]scanRow{}
	ok, err := g.ScanRelColumns(relType, []string{scanRelKey}, opts, func(b *graphpkg.RelColumnBatch) bool {
		if b.RelType != scanRelType {
			t.Errorf("ScanRelColumns %+v: batch type %q, want %q", opts, b.RelType, scanRelType)
		}
		for i, id := range b.IDs {
			if _, dup := out[id]; dup {
				t.Errorf("ScanRelColumns %+v: rel %d twice", opts, id)
			}
			if b.Kinds[0] != graphpkg.ColInt64 || b.Null[0][i] || len(b.Ints[0]) <= i {
				t.Fatalf("rel column row %d: kind=%v null=%v, want a present int64", i, b.Kinds[0], b.Null[0][i])
			}
			out[id] = scanRow{v: b.Ints[0][i], vf: b.ValidFrom[i], vt: b.ValidTo[i]}
		}
		return true
	})
	return out, ok, err
}

// assertNamedValues checks a door's ID -> value answer against the hand-derived
// name -> value map: catches a reference door that is wrong in the same way.
func assertNamedValues[ID comparable](t *testing.T, what string, got map[ID]scanRow, names map[ID]string, want map[string]int64) {
	t.Helper()
	gotNamed := map[string]int64{}
	for id, row := range got {
		name, known := names[id]
		if !known {
			t.Errorf("%s: unknown entity %v", what, id)
			continue
		}
		gotNamed[name] = row.v
	}
	if fmt.Sprint(sortedNamed(gotNamed)) != fmt.Sprint(sortedNamed(want)) {
		t.Errorf("%s:\n got  %v\n want %v", what, sortedNamed(gotNamed), sortedNamed(want))
	}
}

func sortedNamed(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, fmt.Sprintf("%s=%d", k, v))
	}
	sort.Strings(out)
	return out
}

// assertSameRows is the two-door parity check: exact ID set, value and the
// version's valid range.
func assertSameRows[ID comparable](t *testing.T, what string, got, want map[ID]scanRow) {
	t.Helper()
	var diffs []string
	for id, w := range want {
		g, ok := got[id]
		switch {
		case !ok:
			diffs = append(diffs, fmt.Sprintf("missing %v %v", id, w))
		case g != w:
			diffs = append(diffs, fmt.Sprintf("%v: got %v want %v", id, g, w))
		}
	}
	for id, g := range got {
		if _, ok := want[id]; !ok {
			diffs = append(diffs, fmt.Sprintf("extra %v %v", id, g))
		}
	}
	if len(diffs) > 0 {
		sort.Strings(diffs)
		t.Errorf("%s disagrees with the reference door:\n  %s", what, strings.Join(diffs, "\n  "))
	}
}

// pagedOpts derives the paging probes at ValidAt 1500 from the reference's
// sorted IDs: a Limit, an After (skips the first row), and both together.
func pagedOpts(ids []int64) []graphpkg.QueryOpts {
	after := types.EntityID(ids[0])
	return []graphpkg.QueryOpts{
		{ValidAt: 1500, Limit: 2},
		{ValidAt: 1500, After: after},
		{ValidAt: 1500, After: after, Limit: 2},
	}
}

func columnScanNative(b storeBackend) bool { return b.name == "memory" || b.name == "badger" }

// TestScanDoorsAgreeWithByLabelOpts_NodeColumns catches the current-row
// push-down in ScanNodeColumns (core column_scan.go forwarding opts to
// memory NodesByLabel / badger's columnar path, which filter live rows and
// ignore TxAt/TxPin) and the missing opts validation.
func TestScanDoorsAgreeWithByLabelOpts_NodeColumns(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		fx := buildScanOptsFixture(t, g)
		for _, tc := range scanOptsCases(fx) {
			ref := nodeRowsByLabel(t, g, tc.opts)
			assertNamedValues(t, tc.name+"/ByLabel", ref, fx.nodes, tc.nodes)
			got, ok, err := nodeRowsFromColumns(t, g, tc.opts)
			if err != nil {
				t.Errorf("%s: ScanNodeColumns: %v", tc.name, err)
				continue
			}
			if !ok {
				if columnScanNative(b) {
					t.Errorf("%s: ScanNodeColumns ok=false on %s, which has the capability", tc.name, b.name)
				}
				continue
			}
			assertSameRows(t, tc.name+"/ScanNodeColumns", got, ref)
		}

		// Pagination (Limit, After, both) survives the exact path.
		for _, paged := range pagedOpts(sortedIDKeys(nodeRowsByLabel(t, g, graphpkg.QueryOpts{ValidAt: 1500}))) {
			want := nodeRowsByLabel(t, g, paged)
			if len(want) == 0 {
				t.Fatalf("paged reference %+v is empty: the case asserts nothing", paged)
			}
			got, ok, err := nodeRowsFromColumns(t, g, paged)
			switch {
			case err != nil:
				t.Errorf("paged ScanNodeColumns %+v: %v", paged, err)
			case ok:
				assertSameRows(t, fmt.Sprintf("paged ScanNodeColumns %+v", paged), got, want)
			}
		}

		// TxPin with a valid-time filter is a query error, as on ByLabel.
		conflict := graphpkg.QueryOpts{TxPin: fx.pin, ValidAt: 1500}
		if _, err := g.Nodes().ByLabel(scanLabel, conflict); !errors.Is(err, graphpkg.ErrConflictingTemporalOpts) {
			t.Fatalf("ByLabel conflict err = %v, want ErrConflictingTemporalOpts", err)
		}
		if _, ok, err := nodeRowsFromColumns(t, g, conflict); columnScanNative(b) && (!ok || !errors.Is(err, graphpkg.ErrConflictingTemporalOpts)) {
			t.Errorf("ScanNodeColumns conflict: ok=%v err=%v, want ok and ErrConflictingTemporalOpts", ok, err)
		}
	})
}

// TestScanDoorsAgreeWithByLabelOpts_RelColumns is the relationship mirror
// (rule 2): catches the current-row push-down in ScanRelColumns (memory
// RelationshipsByType / segments, badger's columnar path) for a named type AND
// for the every-type scan (relType ""), and the missing opts validation.
func TestScanDoorsAgreeWithByLabelOpts_RelColumns(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		fx := buildScanOptsFixture(t, g)
		for _, tc := range scanOptsCases(fx) {
			ref := relRowsByType(t, g, tc.opts)
			assertNamedValues(t, tc.name+"/ByType", ref, fx.rels, tc.rels)
			for _, relType := range []string{scanRelType, ""} {
				got, ok, err := relRowsFromColumns(t, g, relType, tc.opts)
				if err != nil {
					t.Errorf("%s: ScanRelColumns(%q): %v", tc.name, relType, err)
					continue
				}
				if !ok {
					if columnScanNative(b) {
						t.Errorf("%s: ScanRelColumns(%q) ok=false on %s, which has the capability", tc.name, relType, b.name)
					}
					continue
				}
				assertSameRows(t, fmt.Sprintf("%s/ScanRelColumns(%q)", tc.name, relType), got, ref)
			}
		}

		for _, paged := range pagedOpts(sortedIDKeys(relRowsByType(t, g, graphpkg.QueryOpts{ValidAt: 1500}))) {
			want := relRowsByType(t, g, paged)
			if len(want) == 0 {
				t.Fatalf("paged reference %+v is empty: the case asserts nothing", paged)
			}
			got, ok, err := relRowsFromColumns(t, g, scanRelType, paged)
			switch {
			case err != nil:
				t.Errorf("paged ScanRelColumns %+v: %v", paged, err)
			case ok:
				assertSameRows(t, fmt.Sprintf("paged ScanRelColumns %+v", paged), got, want)
			}
		}

		conflict := graphpkg.QueryOpts{TxPin: fx.pin, ValidAt: 1500}
		for _, relType := range []string{scanRelType, ""} {
			if _, ok, err := relRowsFromColumns(t, g, relType, conflict); columnScanNative(b) && (!ok || !errors.Is(err, graphpkg.ErrConflictingTemporalOpts)) {
				t.Errorf("ScanRelColumns(%q) conflict: ok=%v err=%v, want ok and ErrConflictingTemporalOpts", relType, ok, err)
			}
		}
	})
}

// scanRanges are the numeric windows the range doors are probed with: one that
// holds only A's OLD value (a door yielding the live value 20 misses it) and
// one that holds A's NEW value but not its old one.
var scanRanges = []struct {
	name     string
	min, max float64
}{
	{"[5,15]", 5, 15},
	{"[15,65]", 15, 65},
}

func inScanRange(v int64, min, max float64) bool { return float64(v) >= min && float64(v) <= max }

func filterRows[ID comparable](rows map[ID]scanRow, min, max float64) map[ID]scanRow {
	out := map[ID]scanRow{}
	for id, r := range rows {
		if inScanRange(r.v, min, max) {
			out[id] = r
		}
	}
	return out
}

func filterNamed(m map[string]int64, min, max float64) map[string]int64 {
	out := map[string]int64{}
	for k, v := range m {
		if inScanRange(v, min, max) {
			out[k] = v
		}
	}
	return out
}

// TestScanDoorsAgreeWithByLabelOpts_NodeRange catches the current-row
// push-down in Nodes().ForEachByLabelPropertyRange (badger
// ForEachNodeByLabelPropertyRange filtering the live row through
// storeutil.HasTemporalFilter, which ignores TxAt/TxPin) and the
// index-required decline under a temporal opt (memory, tiered, sharded). The
// reference is ByLabel(opts) with the range applied to the resolved version's
// value — the same value-at-t the ordered sibling sorts on.
func TestScanDoorsAgreeWithByLabelOpts_NodeRange(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		fx := buildScanOptsFixture(t, g)
		for _, tc := range scanOptsCases(fx) {
			ref := nodeRowsByLabel(t, g, tc.opts)
			for _, rg := range scanRanges {
				want := filterRows(ref, rg.min, rg.max)
				what := tc.name + "/ForEachByLabelPropertyRange" + rg.name
				assertNamedValues(t, what+"/reference", want, fx.nodes, filterNamed(tc.nodes, rg.min, rg.max))
				got := map[types.NodeID]scanRow{}
				err := g.Nodes().ForEachByLabelPropertyRange(scanLabel, scanNodeKey, rg.min, rg.max, true, true, tc.opts, func(n *types.Node) bool {
					v, _ := n.GetProperty(scanNodeKey)
					iv, isInt := v.(int64)
					if !isInt || !inScanRange(iv, rg.min, rg.max) {
						return true // the door may over-select; fn re-checks (door contract)
					}
					if _, dup := got[n.ID()]; dup {
						t.Errorf("%s: node %d twice", what, n.ID())
					}
					vf, vt, _ := n.ValidRange()
					got[n.ID()] = scanRow{v: iv, vf: int64(vf), vt: int64(vt)}
					return true
				})
				if err != nil {
					t.Errorf("%s: %v", what, err)
					continue
				}
				assertSameRows(t, what, got, want)
			}
		}
	})
}

// TestScanDoorsAgreeWithByLabelOpts_RelRange is the relationship mirror (rule
// 2): catches the current-row push-down in Rels().ForEachByTypePropertyRange
// (memory and badger ForEachRelByTypePropertyRange) and the index-required
// decline under a temporal opt (tiered, sharded).
func TestScanDoorsAgreeWithByLabelOpts_RelRange(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		fx := buildScanOptsFixture(t, g)
		for _, tc := range scanOptsCases(fx) {
			ref := relRowsByType(t, g, tc.opts)
			for _, rg := range scanRanges {
				want := filterRows(ref, rg.min, rg.max)
				what := tc.name + "/ForEachByTypePropertyRange" + rg.name
				assertNamedValues(t, what+"/reference", want, fx.rels, filterNamed(tc.rels, rg.min, rg.max))
				got := map[types.RelID]scanRow{}
				err := g.Rels().ForEachByTypePropertyRange(scanRelType, scanRelKey, rg.min, rg.max, true, true, tc.opts, func(r *types.Relationship) bool {
					v, _ := r.GetProperty(scanRelKey)
					iv, isInt := v.(int64)
					if !isInt || !inScanRange(iv, rg.min, rg.max) {
						return true
					}
					if _, dup := got[r.ID()]; dup {
						t.Errorf("%s: rel %d twice", what, r.ID())
					}
					vf, vt, _ := r.ValidRange()
					got[r.ID()] = scanRow{v: iv, vf: int64(vf), vt: int64(vt)}
					return true
				})
				if err != nil {
					t.Errorf("%s: %v", what, err)
					continue
				}
				assertSameRows(t, what, got, want)
			}
		}
	})
}

// TestScanDoorsTemporalOpts_RangeLimitAfterEarlyStop pins pagination and early
// stop on the temporal range fold, node and relationship. Faulty implementations
// caught: Limit applied by ID BEFORE the value filter (the page {A, B} holds A,
// out of range, so one row instead of two), After ignored, fn's false ignored.
func TestScanDoorsTemporalOpts_RangeLimitAfterEarlyStop(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		buildScanOptsFixture(t, g)
		base := graphpkg.QueryOpts{ValidAt: 1500}
		const lo, hi = 15, 65

		nodeIDs := sortedIDKeys(filterRows(nodeRowsByLabel(t, g, base), lo, hi))
		relIDs := sortedIDKeys(filterRows(relRowsByType(t, g, base), lo, hi))
		if len(nodeIDs) < 3 || len(relIDs) < 3 {
			t.Fatalf("fixture too small: %d nodes, %d rels in range", len(nodeIDs), len(relIDs))
		}

		nodeRun := func(opts graphpkg.QueryOpts, stopAfter int) []int64 {
			var out []int64
			err := g.Nodes().ForEachByLabelPropertyRange(scanLabel, scanNodeKey, lo, hi, true, true, opts, func(n *types.Node) bool {
				out = append(out, int64(n.ID()))
				return stopAfter == 0 || len(out) < stopAfter
			})
			if err != nil {
				t.Fatalf("node range %+v: %v", opts, err)
			}
			return out
		}
		relRun := func(opts graphpkg.QueryOpts, stopAfter int) []int64 {
			var out []int64
			err := g.Rels().ForEachByTypePropertyRange(scanRelType, scanRelKey, lo, hi, true, true, opts, func(r *types.Relationship) bool {
				out = append(out, int64(r.ID()))
				return stopAfter == 0 || len(out) < stopAfter
			})
			if err != nil {
				t.Fatalf("rel range %+v: %v", opts, err)
			}
			return out
		}
		for _, side := range []struct {
			name string
			ids  []int64
			run  func(graphpkg.QueryOpts, int) []int64
		}{{"node", nodeIDs, nodeRun}, {"rel", relIDs, relRun}} {
			limit := base
			limit.Limit = 2
			if got := side.run(limit, 0); fmt.Sprint(got) != fmt.Sprint(side.ids[:2]) {
				t.Errorf("%s Limit 2: got %v, want %v", side.name, got, side.ids[:2])
			}
			after := base
			after.After = types.EntityID(side.ids[0])
			if got := side.run(after, 0); fmt.Sprint(got) != fmt.Sprint(side.ids[1:]) {
				t.Errorf("%s After first: got %v, want %v", side.name, got, side.ids[1:])
			}
			if got := side.run(base, 1); len(got) != 1 {
				t.Errorf("%s early stop: fn called %d times, want 1", side.name, len(got))
			}
		}
	})
}

func sortedIDKeys[ID ~int64](m map[ID]scanRow) []int64 {
	out := make([]int64, 0, len(m))
	for id := range m {
		out = append(out, int64(id))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestScanDoorsTemporalOpts_RangeNeverDropsAtExclusiveBound pins the
// over-selection contract on the temporal range fold: an int64 past 2^53 whose
// float64 rounds ONTO an exclusive bound is still offered to fn, which re-checks
// exactly. Faulty implementation caught: the fold applying inclMin/inclMax in
// float64 (numericInRange), which drops 2^53+1 for "> 2^53".
func TestScanDoorsTemporalOpts_RangeNeverDropsAtExclusiveBound(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		const big = int64(1)<<53 + 1
		bound := float64(int64(1) << 53)
		n, err := g.Nodes().Add(ctx, []string{scanLabel}, map[string]any{scanNodeKey: big, "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels().Add(ctx, scanRelType, n, n, map[string]any{scanRelKey: big, "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		opts := graphpkg.QueryOpts{ValidAt: 1500}
		var nodes, rels int
		if err := g.Nodes().ForEachByLabelPropertyRange(scanLabel, scanNodeKey, bound, 1e300, false, true, opts, func(got *types.Node) bool {
			v, _ := got.GetProperty(scanNodeKey)
			if got.ID() == n.ID() && v.(int64) > int64(1)<<53 { // fn's exact re-check
				nodes++
			}
			return true
		}); err != nil {
			t.Fatalf("node range: %v", err)
		}
		if err := g.Rels().ForEachByTypePropertyRange(scanRelType, scanRelKey, bound, 1e300, false, true, opts, func(got *types.Relationship) bool {
			v, _ := got.GetProperty(scanRelKey)
			if got.ID() == r.ID() && v.(int64) > int64(1)<<53 {
				rels++
			}
			return true
		}); err != nil {
			t.Fatalf("rel range: %v", err)
		}
		if nodes != 1 || rels != 1 {
			t.Errorf("2^53+1 under exclusive min 2^53: node offered %d times, rel %d times, want 1 each", nodes, rels)
		}
	})
}

// TestScanDoorsTemporalOpts_OrderedRangeNeverDropsAtExclusiveBound is the
// ordered-sibling twin of RangeNeverDropsAtExclusiveBound (lesson 58): the
// temporal folds behind ForEachByLabelPropertyRangeOrdered /
// ForEachByTypePropertyRangeOrdered must over-select like their index path
// (docs/query-planners.md "Over-selecting candidate filter"). Faulty
// implementation caught: the fold applying inclMin/inclMax in float64
// (numericInRange(f, min, max, inclMin, inclMax)), which silently drops 2^53+1
// for "> 2^53".
func TestScanDoorsTemporalOpts_OrderedRangeNeverDropsAtExclusiveBound(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		const big = int64(1)<<53 + 1
		bound := float64(int64(1) << 53)
		n, err := g.Nodes().Add(ctx, []string{scanLabel}, map[string]any{scanNodeKey: big, "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels().Add(ctx, scanRelType, n, n, map[string]any{scanRelKey: big, "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		opts := graphpkg.QueryOpts{ValidAt: 1500}
		for _, desc := range []bool{false, true} {
			var nodes, rels int
			if err := g.Nodes().ForEachByLabelPropertyRangeOrdered(scanLabel, scanNodeKey, bound, 1e300, false, true, desc, opts, func(got *types.Node) bool {
				v, _ := got.GetProperty(scanNodeKey)
				if got.ID() == n.ID() && v.(int64) > int64(1)<<53 { // fn's exact re-check
					nodes++
				}
				return true
			}); err != nil {
				t.Fatalf("node ordered range desc=%v: %v", desc, err)
			}
			if err := g.Rels().ForEachByTypePropertyRangeOrdered(scanRelType, scanRelKey, bound, 1e300, false, true, desc, opts, func(got *types.Relationship) bool {
				v, _ := got.GetProperty(scanRelKey)
				if got.ID() == r.ID() && v.(int64) > int64(1)<<53 {
					rels++
				}
				return true
			}); err != nil {
				t.Fatalf("rel ordered range desc=%v: %v", desc, err)
			}
			if nodes != 1 || rels != 1 {
				t.Errorf("desc=%v: 2^53+1 under exclusive min 2^53: node offered %d times, rel %d times, want 1 each", desc, nodes, rels)
			}
		}
	})
}

// TestScanDoorsTemporalOpts_ColumnScansFailClosedBelowWatermarks pins the scan
// validation on the column doors: a pin below the history-compaction watermark
// is ErrHistoryCompacted and a pin below the retention watermark is
// ErrRetentionExpired, exactly as on ByLabel / ByType. Faulty implementations
// caught: a column door that validates only the TxPin conflict (or nothing) and
// answers from the trimmed history or the purged range.
func TestScanDoorsTemporalOpts_ColumnScansFailClosedBelowWatermarks(t *testing.T) {
	ctx := context.Background()
	scanErrs := func(t *testing.T, g *graphpkg.Graph, opts graphpkg.QueryOpts) (nodeOK bool, relOK []bool, relErr []error, nodeErr error) {
		t.Helper()
		nodeOK, nodeErr = g.ScanNodeColumns(scanLabel, []string{scanNodeKey}, opts, func(*graphpkg.ColumnBatch) bool { return true })
		for _, typ := range []string{scanRelType, ""} {
			ok, err := g.ScanRelColumns(typ, []string{scanRelKey}, opts, func(*graphpkg.RelColumnBatch) bool { return true })
			relOK, relErr = append(relOK, ok), append(relErr, err)
		}
		return
	}
	assertAll := func(t *testing.T, what string, b storeBackend, g *graphpkg.Graph, opts graphpkg.QueryOpts, want error) {
		t.Helper()
		if _, err := g.Nodes().ByLabel(scanLabel, opts); !errors.Is(err, want) {
			t.Fatalf("%s: ByLabel err = %v, want %v (reference door)", what, err, want)
		}
		if _, err := g.Rels().ByType(scanRelType, opts); !errors.Is(err, want) {
			t.Fatalf("%s: ByType err = %v, want %v (reference door)", what, err, want)
		}
		if !columnScanNative(b) {
			return
		}
		nodeOK, relOK, relErr, nodeErr := scanErrs(t, g, opts)
		if !nodeOK || !errors.Is(nodeErr, want) {
			t.Errorf("%s: ScanNodeColumns ok=%v err=%v, want ok and %v", what, nodeOK, nodeErr, want)
		}
		for i := range relOK {
			if !relOK[i] || !errors.Is(relErr[i], want) {
				t.Errorf("%s: ScanRelColumns[%d] ok=%v err=%v, want ok and %v", what, i, relOK[i], relErr[i], want)
			}
		}
	}

	t.Run("compaction", func(t *testing.T) {
		for _, b := range allStoreBackends() {
			if !columnScanNative(b) {
				continue
			}
			t.Run(b.name, func(t *testing.T) {
				g := b.open(t)
				fx := buildScanOptsFixture(t, g)
				// Two more versions each, so a KeepVersions:1 trim removes the
				// versions recorded at fx.pin and below.
				for i := 0; i < 2; i++ {
					for id := range fx.nodes {
						_, _ = g.Nodes().Update(ctx, id, map[string]any{"bump": int64(i)})
					}
					for id := range fx.rels {
						_, _ = g.Rels().Update(ctx, id, map[string]any{"bump": int64(i)})
					}
				}
				if _, err := g.Admin().CompactHistoryNodes(ctx, adminpkg.RetentionPolicy{KeepVersions: 1}); err != nil {
					t.Fatalf("CompactHistoryNodes: %v", err)
				}
				if _, err := g.Admin().CompactHistoryRels(ctx, adminpkg.RetentionPolicy{KeepVersions: 1}); err != nil {
					t.Fatalf("CompactHistoryRels: %v", err)
				}
				assertAll(t, "TxPin below compaction", b, g, graphpkg.QueryOpts{TxPin: fx.pin}, graphpkg.ErrHistoryCompacted)
				assertAll(t, "TxAt below compaction", b, g, graphpkg.QueryOpts{TxAt: fx.pin, ValidAt: 1500}, graphpkg.ErrHistoryCompacted)
			})
		}
	})

	t.Run("retention", func(t *testing.T) {
		for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowRetentionPurge = true }) {
			if !columnScanNative(b) {
				continue
			}
			t.Run(b.name, func(t *testing.T) {
				g := b.open(t)
				buildScanOptsFixture(t, g)
				if _, err := g.Nodes().Add(ctx, []string{"Old"}, nil); err != nil {
					t.Fatal(err)
				}
				before := types.Instant(time.Now().Add(24 * time.Hour).UnixMilli())
				if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Old", Mode: adminpkg.PurgeByAge, Before: before}); err != nil {
					t.Fatalf("PurgeExpiredNodes: %v", err)
				}
				assertAll(t, "ValidAt below retention", b, g, graphpkg.QueryOpts{ValidAt: 1500}, graphpkg.ErrRetentionExpired)
				assertAll(t, "interval below retention", b, g, graphpkg.QueryOpts{ValidStart: 1500, ValidEnd: 2500}, graphpkg.ErrRetentionExpired)
			})
		}
	})
}

// TestScanDoorsTemporalOpts_RangeSkipsAbsentAndNonNumeric pins the value test on
// the temporal range fold: an entity whose version at t lacks the property, or
// holds a string, is not offered — even when its CURRENT version holds an
// in-range number. Faulty implementations caught: reading the live value, a
// fold that offers every label member and leaves the whole predicate to fn.
func TestScanDoorsTemporalOpts_RangeSkipsAbsentAndNonNumeric(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		absent, err := g.Nodes().Add(ctx, []string{scanLabel}, map[string]any{"tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		str, err := g.Nodes().Add(ctx, []string{scanLabel}, map[string]any{scanNodeKey: "7", "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		rAbsent, err := g.Rels().Add(ctx, scanRelType, absent, str, map[string]any{"tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		rStr, err := g.Rels().Add(ctx, scanRelType, absent, str, map[string]any{scanRelKey: "7", "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		// From 2000 on every one of them holds the in-range number 7.
		for _, id := range []types.NodeID{absent.ID(), str.ID()} {
			if _, err := g.Nodes().Update(ctx, id, map[string]any{scanNodeKey: int64(7), "tkg_valid_from": types.Instant(2000)}); err != nil {
				t.Fatal(err)
			}
		}
		for _, id := range []types.RelID{rAbsent.ID(), rStr.ID()} {
			if _, err := g.Rels().Update(ctx, id, map[string]any{scanRelKey: int64(7), "tkg_valid_from": types.Instant(2000)}); err != nil {
				t.Fatal(err)
			}
		}
		for _, tc := range []struct {
			at   types.Instant
			want int
		}{{1500, 0}, {2500, 2}} {
			opts := graphpkg.QueryOpts{ValidAt: tc.at}
			nodes, rels := 0, 0
			if err := g.Nodes().ForEachByLabelPropertyRange(scanLabel, scanNodeKey, 0, 10, true, true, opts, func(*types.Node) bool {
				nodes++
				return true
			}); err != nil {
				t.Fatalf("node range at %d: %v", tc.at, err)
			}
			if err := g.Rels().ForEachByTypePropertyRange(scanRelType, scanRelKey, 0, 10, true, true, opts, func(*types.Relationship) bool {
				rels++
				return true
			}); err != nil {
				t.Fatalf("rel range at %d: %v", tc.at, err)
			}
			if nodes != tc.want || rels != tc.want {
				t.Errorf("at %d: offered %d nodes and %d rels, want %d each", tc.at, nodes, rels, tc.want)
			}
		}
	})
}

// TestScanDoorsTemporalOpts_RelColumnsEveryTypeStops pins the every-type scan
// (relType "") on the temporal path: each type's batches carry their own type
// name and the version value at t, and fn returning false stops the WHOLE scan.
// Faulty implementations caught: a stop that only ends the current type, a
// batch labelled with the wrong type, a type skipped, the live value.
func TestScanDoorsTemporalOpts_RelColumnsEveryTypeStops(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"H"}, nil)
		c, _ := g.Nodes().Add(ctx, []string{"H"}, nil)
		want := map[string]int64{}
		for i, typ := range []string{"R1", "R2", "R3"} {
			r, err := g.Rels().Add(ctx, typ, a, c, map[string]any{scanRelKey: int64(i), "tkg_valid_from": types.Instant(1000)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{scanRelKey: int64(100 + i), "tkg_valid_from": types.Instant(2000)}); err != nil {
				t.Fatal(err)
			}
			want[typ] = int64(i) // the value at 1500, before the update
		}
		opts := graphpkg.QueryOpts{ValidAt: 1500}
		got := map[string]int64{}
		ok, err := g.ScanRelColumns("", []string{scanRelKey}, opts, func(rb *graphpkg.RelColumnBatch) bool {
			for i := range rb.IDs {
				got[rb.RelType] = rb.Ints[0][i]
			}
			return true
		})
		if err != nil {
			t.Fatalf("ScanRelColumns all: %v", err)
		}
		if !ok {
			if columnScanNative(b) {
				t.Fatalf("ok=false on %s", b.name)
			}
			return
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("every-type scan at 1500: got %v, want %v", got, want)
		}
		calls := 0
		if _, err := g.ScanRelColumns("", []string{scanRelKey}, opts, func(*graphpkg.RelColumnBatch) bool {
			calls++
			return false
		}); err != nil {
			t.Fatalf("ScanRelColumns stop: %v", err)
		}
		if calls != 1 {
			t.Errorf("fn returned false on the first batch; called %d times, want 1", calls)
		}
	})
}
