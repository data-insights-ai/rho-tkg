package store_test

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 72: the equality doors page from sorted posting lists. This
// differential runs random node and relationship writes, replaces (property,
// label and valid-time changes), deletes, and index creation after data
// exists, on memory, badger and a two-shard sharded store, and checks every
// keyset page (After, Limit), with and without a valid-time filter, against
// the old gather-sort-cut answer computed from an unindexed label or type scan:
// every current row of the label, filtered by the property match and
// MatchesTemporalFilter, sorted by ID, cut after the cursor to the limit.

type postingsDiffStore interface {
	PutNode(*types.Node) error
	ReplaceNode(*types.Node) error
	DeleteNode(types.NodeID) error
	AddNodeLabelToken(types.NodeID, uint16, *types.Node) error
	RemoveNodeLabelToken(types.NodeID, uint16, *types.Node) error
	PutRelationship(*types.Relationship) error
	ReplaceRelationship(*types.Relationship) error
	DeleteRelationship(types.RelID) error
	CreatePropertyIndex(uint16, string) error
	CreateCompositePropertyIndex(uint16, []string) error
	CreateRelPropertyIndex(uint16, string) error
	NodesByLabel(uint16, storepkg.QueryOpts) ([]*types.Node, error)
	RelationshipsByType(uint16, storepkg.QueryOpts) ([]*types.Relationship, error)
	NodesByLabelAndProperty(uint16, string, any, storepkg.QueryOpts) ([]*types.Node, error)
	NodesByLabelAndProperties(uint16, map[string]any, storepkg.QueryOpts) ([]*types.Node, error)
	RelationshipsByTypeAndProperty(uint16, string, any, storepkg.QueryOpts) ([]*types.Relationship, error)
}

const (
	pdLabel    = uint16(1)
	pdOther    = uint16(2)
	pdEndpoint = uint16(9)
	pdType     = uint16(7)
)

// pdID is a snowflake ID with time n and the given sharded slot.
func pdID(slot uint8, n int64) snowflake.ID { return snowflake.ID(n<<15 | int64(slot)<<10) }

func TestSortedPostingsDifferential(t *testing.T) {
	backends := []struct {
		name string
		open func(t *testing.T) postingsDiffStore
	}{
		{"memory", func(t *testing.T) postingsDiffStore { return memory.New() }},
		{"badger", func(t *testing.T) postingsDiffStore {
			bs, err := badger.New(badger.Config{InMemory: true, FlushInterval: 1<<63 - 1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			return bs
		}},
		{"sharded", func(t *testing.T) postingsDiffStore {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			return st
		}},
	}
	seeds := []int64{1, 2, 3}
	ops := 1500
	if testing.Short() {
		seeds, ops = seeds[:2], 900
	}
	for _, b := range backends {
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("%s/seed=%d", b.name, seed), func(t *testing.T) {
				runPostingsDiff(t, b.open(t), seed, ops)
			})
		}
	}
}

func runPostingsDiff(t *testing.T, st postingsDiffStore, seed int64, ops int) {
	rng := rand.New(rand.NewSource(seed)) // #nosec G404 -- deterministic test input
	const pool = 240
	// IDs in shuffled time order, over both sharded slots, so postings are
	// not filled ascending.
	order := rng.Perm(pool)
	nodeID := func(i int) types.NodeID { return types.NodeID(pdID(uint8(i%2), int64(order[i]+1000))) } // #nosec G115 -- 0 or 1
	relID := func(i int) types.RelID { return types.RelID(pdID(uint8(i%2), int64(order[i]+5000))) }    // #nosec G115 -- 0 or 1
	ends := [2]types.NodeID{types.NodeID(pdID(0, 1)), types.NodeID(pdID(1, 2))}
	for _, e := range ends {
		if err := st.PutNode(types.NewNode(e, pdEndpoint, nil)); err != nil {
			t.Fatal(err)
		}
	}
	temporal := func() *types.TemporalMetadata {
		if rng.Intn(3) == 0 {
			return nil
		}
		from := types.Instant(1 + rng.Intn(100))
		var to types.Instant
		if rng.Intn(2) == 0 {
			to = from + types.Instant(1+rng.Intn(60))
		}
		return &types.TemporalMetadata{ValidFrom: from, ValidTo: to}
	}
	// A node's state: its primary label (fixed: a store replace keeps the
	// labels), whether it carries the other label as an extra (changed through
	// the label doors), its properties and valid time.
	type nodeState struct {
		primary uint16
		extra   bool
		a       int64 // -1 = absent
		b       string
		tm      *types.TemporalMetadata
	}
	state := map[int]*nodeState{}
	build := func(i int, ns *nodeState) *types.Node {
		var extras []uint16
		if ns.extra {
			extras = []uint16{pdLabel + pdOther - ns.primary}
		}
		n := types.NewNode(nodeID(i), ns.primary, extras)
		if ns.a >= 0 {
			_ = n.SetProperty("a", ns.a)
		}
		if ns.b != "" {
			_ = n.SetProperty("b", ns.b)
		}
		n.SetTemporal(ns.tm)
		return n
	}
	randomProps := func(ns *nodeState) {
		ns.a, ns.b = -1, ""
		if rng.Intn(8) != 0 {
			ns.a = int64(rng.Intn(3))
		}
		if rng.Intn(8) != 0 {
			ns.b = []string{"x", "y"}[rng.Intn(2)]
		}
		ns.tm = temporal()
	}
	newRel := func(i int) *types.Relationship {
		r := types.NewRelationship(relID(i), pdType, ends[0], ends[1])
		if rng.Intn(8) != 0 {
			_ = r.SetProperty("k", int64(rng.Intn(3)))
		}
		r.SetTemporal(temporal())
		return r
	}
	liveRel := map[int]bool{}
	var cov pdCoverage
	for op := 0; op < ops; op++ {
		if op == ops/4 { // declared after data exists: the backfill path
			if err := st.CreatePropertyIndex(pdLabel, "a"); err != nil {
				t.Fatal(err)
			}
			if err := st.CreateCompositePropertyIndex(pdLabel, []string{"a", "b"}); err != nil {
				t.Fatal(err)
			}
			if err := st.CreateRelPropertyIndex(pdType, "k"); err != nil {
				t.Fatal(err)
			}
		}
		i := rng.Intn(pool)
		switch r := rng.Intn(10); {
		case r < 5:
			var err error
			if ns := state[i]; ns != nil {
				randomProps(ns)
				err = st.ReplaceNode(build(i, ns))
			} else {
				ns = &nodeState{primary: []uint16{pdLabel, pdOther}[rng.Intn(2)], extra: rng.Intn(3) == 0}
				randomProps(ns)
				state[i] = ns
				err = st.PutNode(build(i, ns))
			}
			if err != nil {
				t.Fatalf("op %d node write: %v", op, err)
			}
		case r < 6:
			// The other label joins or leaves: label 1 gains or loses a member
			// whose properties stay.
			if ns := state[i]; ns != nil {
				tok := pdLabel + pdOther - ns.primary
				ns.extra = !ns.extra
				var err error
				if ns.extra {
					err = st.AddNodeLabelToken(nodeID(i), tok, build(i, ns))
				} else {
					err = st.RemoveNodeLabelToken(nodeID(i), tok, build(i, ns))
				}
				if err != nil {
					t.Fatalf("op %d label change: %v", op, err)
				}
				cov.labelOps++
			}
		case r < 7:
			if state[i] != nil {
				if err := st.DeleteNode(nodeID(i)); err != nil {
					t.Fatalf("op %d delete node: %v", op, err)
				}
				delete(state, i)
				cov.deletes++
			}
		case r < 9:
			var err error
			if liveRel[i] {
				err = st.ReplaceRelationship(newRel(i))
			} else {
				err = st.PutRelationship(newRel(i))
			}
			if err != nil {
				t.Fatalf("op %d rel write: %v", op, err)
			}
			liveRel[i] = true
		default:
			if liveRel[i] {
				if err := st.DeleteRelationship(relID(i)); err != nil {
					t.Fatalf("op %d delete rel: %v", op, err)
				}
				delete(liveRel, i)
			}
		}
		if op%150 == 149 || op == ops-1 {
			checkPostingsDoors(t, st, rng, op, nodeID(rng.Intn(pool)), relID(rng.Intn(pool)), &cov)
		}
	}
	// Generator coverage: the pages held rows, the valid-time filter dropped
	// matches, labels and rows changed and left.
	if cov.ids == 0 || cov.temporalDropped == 0 || cov.labelOps == 0 || cov.deletes == 0 || cov.relIDs == 0 {
		t.Fatalf("generator coverage too thin: %+v", cov)
	}
}

// pdCoverage counts what a differential run exercised.
type pdCoverage struct {
	ids, relIDs, temporalDropped, labelOps, deletes int
}

func checkPostingsDoors(t *testing.T, st postingsDiffStore, rng *rand.Rand, op int, cursorNode types.NodeID, cursorRel types.RelID, cov *pdCoverage) {
	t.Helper()
	nodes, err := st.NodesByLabel(pdLabel, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	rels, err := st.RelationshipsByType(pdType, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	filters := []storepkg.QueryOpts{
		{},
		{ValidAt: types.Instant(1 + rng.Intn(150))},
		{ValidStart: types.Instant(1 + rng.Intn(60)), ValidEnd: types.Instant(61 + rng.Intn(90))},
	}
	matches := func(got, want []snowflake.ID, what string) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Fatalf("op %d %s:\n got  %v\n want %v", op, what, got, want)
		}
	}
	for _, f := range filters {
		for a := int64(0); a < 3; a++ {
			// Single key a, composite (a, b) and the rel key k.
			wantA := pdOracleNodes(nodes, f, map[string]any{"a": a})
			pdWalk(t, wantA, f, types.EntityID(cursorNode), rng, func(o storepkg.QueryOpts) []snowflake.ID {
				out, err := st.NodesByLabelAndProperty(pdLabel, "a", a, o)
				if err != nil {
					t.Fatal(err)
				}
				return pdNodeIDs(out)
			}, matches, fmt.Sprintf("a=%d %+v", a, f))
			for _, bv := range []string{"x", "y"} {
				values := map[string]any{"a": a, "b": bv}
				want := pdOracleNodes(nodes, f, values)
				cov.ids += len(want)
				cov.temporalDropped += len(pdOracleNodes(nodes, storepkg.QueryOpts{}, values)) - len(want)
				pdWalk(t, want, f, types.EntityID(cursorNode), rng, func(o storepkg.QueryOpts) []snowflake.ID {
					out, err := st.NodesByLabelAndProperties(pdLabel, values, o)
					if err != nil {
						t.Fatal(err)
					}
					return pdNodeIDs(out)
				}, matches, fmt.Sprintf("a=%d b=%s %+v", a, bv, f))
			}
			wantK := pdOracleRels(rels, f, a)
			cov.relIDs += len(wantK)
			pdWalk(t, wantK, f, types.EntityID(cursorRel), rng, func(o storepkg.QueryOpts) []snowflake.ID {
				out, err := st.RelationshipsByTypeAndProperty(pdType, "k", a, o)
				if err != nil {
					t.Fatal(err)
				}
				return pdRelIDs(out)
			}, matches, fmt.Sprintf("k=%d %+v", a, f))
		}
	}
}

// pdWalk checks the unpaged answer, a keyset walk in random page sizes, and
// one page from a random cursor (a member or not) against want.
func pdWalk(t *testing.T, want []snowflake.ID, f storepkg.QueryOpts, cursor types.EntityID, rng *rand.Rand, door func(storepkg.QueryOpts) []snowflake.ID, matches func(got, want []snowflake.ID, what string), what string) {
	t.Helper()
	matches(door(f), want, what+" unpaged")
	var walked []snowflake.ID
	o := f
	for steps := 0; ; steps++ {
		o.Limit = 1 + rng.Intn(9)
		page := door(o)
		exp := pdCut(want, o.After, o.Limit)
		matches(page, exp, fmt.Sprintf("%s page after %d limit %d", what, o.After, o.Limit))
		if len(page) == 0 {
			break
		}
		walked = append(walked, page...)
		o.After = types.EntityID(page[len(page)-1])
		if steps > len(want)+1 {
			t.Fatalf("%s: walk does not end", what)
		}
	}
	matches(walked, want, what+" walk")
	o = f
	o.After, o.Limit = cursor, 1+rng.Intn(5)
	matches(door(o), pdCut(want, o.After, o.Limit), fmt.Sprintf("%s page after cursor %d", what, cursor))
}

func pdCut(want []snowflake.ID, after types.EntityID, limit int) []snowflake.ID {
	i, found := slices.BinarySearch(want, after.SnowflakeID())
	if found {
		i++
	}
	out := want[i:]
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out
}

func pdOracleNodes(nodes []*types.Node, f storepkg.QueryOpts, values map[string]any) []snowflake.ID {
	var out []snowflake.ID
	for _, n := range nodes {
		ok := true
		for k, v := range values {
			got, found := n.IndexablePropertyValueKey(k)
			if !found || got != types.IndexablePropertyValueKey(v) {
				ok = false
			}
		}
		if ok && storeutil.MatchesTemporalFilter(n.ID().SnowflakeID(), n.Temporal(), f) {
			out = append(out, n.ID().SnowflakeID())
		}
	}
	slices.Sort(out)
	return out
}

func pdOracleRels(rels []*types.Relationship, f storepkg.QueryOpts, k int64) []snowflake.ID {
	var out []snowflake.ID
	for _, r := range rels {
		got, found := r.IndexablePropertyValueKey("k")
		if found && got == types.IndexablePropertyValueKey(k) && storeutil.MatchesTemporalFilter(r.ID().SnowflakeID(), r.Temporal(), f) {
			out = append(out, r.ID().SnowflakeID())
		}
	}
	slices.Sort(out)
	return out
}

func pdNodeIDs(nodes []*types.Node) []snowflake.ID {
	var out []snowflake.ID
	for _, n := range nodes {
		out = append(out, n.ID().SnowflakeID())
	}
	return out
}

func pdRelIDs(rels []*types.Relationship) []snowflake.ID {
	var out []snowflake.ID
	for _, r := range rels {
		out = append(out, r.ID().SnowflakeID())
	}
	return out
}
