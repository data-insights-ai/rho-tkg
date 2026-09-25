package graph_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ADR-0011 S2 differential oracle, graph level.
//
// One primary graph (memory store, change-log on, no declaration) runs a
// seeded random workload: HOP relationships (the declared bulk type) with
// declared, missing, wrong-kind and nested-kind properties (the v4.37.0 wire),
// an undeclared type, updates with and without history, CloseVersion,
// deletes, cascading node deletes, back-filled transaction times and explicit
// valid-time intervals. Two read-only replicas apply the primary's change-log
// in chunks, so both hold byte-identical rows (same IDs, stamps and hashes):
//
//   - plain:    memory store, no declaration (the oracle);
//   - declared: memory store, HOP declared, a tiny memtable budget so seals
//     happen during apply, plus an explicit seal between chunks — so later
//     chunks update and delete rows that are already sealed.
//
// After every chunk, every read door of the graph API must answer the two
// replicas identically — including order where the door promises it, hashes,
// history, as-of and valid-time answers, stats and the export bytes.

const segOracleHOP = "HOP"

var segOracleSpec = graph.RelSegmentSpec{
	Type: segOracleHOP,
	Columns: []graph.SegmentColumn{
		{Name: "actor", Kind: graph.SegmentString},
		{Name: "family", Kind: graph.SegmentString},
		{Name: "orch", Kind: graph.SegmentString},
		{Name: "t_lo", Kind: graph.SegmentInt64},
		{Name: "weight", Kind: graph.SegmentFloat64},
		{Name: "hot", Kind: graph.SegmentBool},
	},
	IntegrityBlockRows: 16,
}

// segNestedValue returns a random nested-kind property value (every kind the
// allowlist accepts, at depth 1 or 2 inside []any / map[string]any).
func segNestedValue(r *rand.Rand) any {
	leaves := []any{
		int(-5), int8(math.MinInt8), int16(r.Intn(100)), int32(math.MaxInt32), r.Int63(),
		uint(7), uint8(math.MaxUint8), uint16(r.Intn(100)), uint32(math.MaxUint32), uint64(math.MaxUint64),
		float32(1.25), r.Float64(), math.Copysign(0, -1), "s" + fmt.Sprint(r.Intn(9)), true,
		[]string{"a", "b"}, []int{1, 2}, []int64{3}, []float32{1.5}, []float64{2.5}, []bool{true}, []byte{1, 2},
		map[string]string{"k": "v"}, []string(nil), []any(nil), map[string]any(nil),
	}
	v := leaves[r.Intn(len(leaves))]
	switch r.Intn(4) {
	case 0:
		return []any{v}
	case 1:
		return map[string]any{"k": v}
	case 2:
		return []any{map[string]any{"k": v}}
	default:
		return map[string]any{"k": []any{v, int8(1)}}
	}
}

// segHOPProps returns HOP properties: mostly the declared schema, sometimes a
// missing column, a wrong kind (fallback column) or a nested extra property.
func segHOPProps(r *rand.Rand) map[string]any {
	p := map[string]any{
		"actor":  fmt.Sprintf("actor-%02d", r.Intn(12)),
		"family": []string{"f1", "f2"}[r.Intn(2)],
		"orch":   "o1",
		"t_lo":   int64(r.Intn(1 << 20)),
		"weight": float64(r.Intn(1000)) / 8,
		"hot":    r.Intn(2) == 0,
	}
	switch r.Intn(8) {
	case 0:
		delete(p, "orch")
	case 1:
		p["t_lo"] = int32(7) // wrong kind: fallback column
	case 2:
		p["actor"] = int64(r.Intn(5)) // wrong kind for a string column
	case 3:
		p["extra"] = segNestedValue(r)
	case 4:
		p["weight"] = math.NaN()
	}
	if r.Intn(6) == 0 {
		vf := types.Instant(1_700_000_000_000 + int64(r.Intn(1_000_000)))
		p["tkg_valid_from"] = vf
		if r.Intn(2) == 0 {
			p["tkg_valid_to"] = vf + types.Instant(1+r.Intn(500_000))
		}
	}
	return p
}

type segOracle struct {
	t        *testing.T
	ctx      context.Context
	primary  *graph.Graph
	plain    *graph.Graph
	declared *graph.Graph
	nodes    []*types.Node
	liveHOP  []types.RelID
	allRels  []types.RelID
	pins     []types.Instant
	applied  uint64
	// corrections / closedDeletes count the v4.38.0-semantics steps that took
	// effect, so the test can require that they ran.
	corrections, closedDeletes int
}

func newSegOracle(t *testing.T, budget int64) *segOracle {
	t.Helper()
	return newSegOracleDir(t, budget, "")
}

// newSegOracleDir is newSegOracle with the declared replica's sealed
// segments in segDir (ADR-0011 S3; "" = in RAM).
func newSegOracleDir(t *testing.T, budget int64, segDir string) *segOracle {
	t.Helper()
	o := &segOracle{t: t, ctx: context.Background()}
	var err error
	o.primary, err = graph.New(graph.Config{SnowflakeNodeID: 1, Store: memory.New(memory.WithChangeLog()), AllowTxBackfill: true})
	if err != nil {
		t.Fatalf("primary: %v", err)
	}
	t.Cleanup(func() { _ = o.primary.Close() })
	o.plain, err = graph.New(graph.Config{SnowflakeNodeID: 2, Store: memory.New(), ReadOnlyReplica: true, ReplicationSource: o.primary.Replication()})
	if err != nil {
		t.Fatalf("plain replica: %v", err)
	}
	t.Cleanup(func() { _ = o.plain.Close() })
	o.declared, err = graph.New(graph.Config{
		SnowflakeNodeID: 3, Store: memory.New(), ReadOnlyReplica: true, ReplicationSource: o.primary.Replication(),
		RelSegments: []graph.RelSegmentSpec{segOracleSpec}, SegmentMemoryBudget: budget, SegmentDir: segDir,
	})
	if err != nil {
		t.Fatalf("declared replica: %v", err)
	}
	t.Cleanup(func() { _ = o.declared.Close() })
	return o
}

// seed registers HOP first on the primary (so its token matches the
// declared replica's) and creates the node set.
func (o *segOracle) seed(r *rand.Rand, nodes int) {
	for i := 0; i < nodes; i++ {
		n, err := o.primary.Nodes().Add(o.ctx, []string{"Asset"}, map[string]any{"i": int64(i)})
		if err != nil {
			o.t.Fatalf("node: %v", err)
		}
		o.nodes = append(o.nodes, n)
	}
	o.addHOP(r)
}

func (o *segOracle) pick(r *rand.Rand) (*types.Node, *types.Node) {
	for {
		a, b := o.nodes[r.Intn(len(o.nodes))], o.nodes[r.Intn(len(o.nodes))]
		if a.ID() != b.ID() {
			return a, b
		}
	}
}

func (o *segOracle) addHOP(r *rand.Rand) {
	a, b := o.pick(r)
	var rel *types.Relationship
	var err error
	if r.Intn(3) == 0 {
		now, nerr := o.primary.Temporal().PeekTx()
		if nerr != nil {
			o.t.Fatal(nerr)
		}
		rel, err = o.primary.Rels().AddWithTx(o.ctx, segOracleHOP, a, b, segHOPProps(r), now-types.Instant(1+r.Intn(5000)))
	} else {
		rel, err = o.primary.Rels().Add(o.ctx, segOracleHOP, a, b, segHOPProps(r))
	}
	if err != nil {
		o.t.Fatalf("add HOP: %v", err)
	}
	o.liveHOP = append(o.liveHOP, rel.ID())
	o.allRels = append(o.allRels, rel.ID())
}

func (o *segOracle) dropLive(i int) {
	o.liveHOP[i] = o.liveHOP[len(o.liveHOP)-1]
	o.liveHOP = o.liveHOP[:len(o.liveHOP)-1]
}

// step runs one random primary mutation.
func (o *segOracle) step(r *rand.Rand) {
	ctx := o.ctx
	x := r.Intn(100)
	if len(o.liveHOP) == 0 {
		x = 0
	}
	switch {
	case x < 50:
		o.addHOP(r)
	case x < 58:
		a, b := o.pick(r)
		rel, err := o.primary.Rels().Add(ctx, "ORIGIN", a, b, map[string]any{"actor": "x", "n": int64(r.Intn(9))})
		if err != nil {
			o.t.Fatalf("add ORIGIN: %v", err)
		}
		o.allRels = append(o.allRels, rel.ID())
	case x < 72:
		id := o.liveHOP[r.Intn(len(o.liveHOP))]
		upd := map[string]any{"actor": fmt.Sprintf("actor-%02d", r.Intn(12)), "weight": float64(r.Intn(100))}
		if r.Intn(4) == 0 {
			upd["extra"] = segNestedValue(r)
		}
		if _, err := o.primary.Rels().Update(ctx, id, upd); err != nil && !errors.Is(err, graph.ErrAlreadyClosed) {
			o.t.Fatalf("update %d: %v", id, err)
		}
	case x < 76:
		id := o.liveHOP[r.Intn(len(o.liveHOP))]
		if _, err := o.primary.Rels().UpdateInPlace(ctx, id, map[string]any{"family": "f3"}); err != nil && !errors.Is(err, graph.ErrAlreadyClosed) {
			o.t.Fatalf("update in place %d: %v", id, err)
		}
	case x < 84:
		i := r.Intn(len(o.liveHOP))
		if err := o.primary.Rels().Delete(ctx, o.liveHOP[i]); err != nil {
			o.t.Fatalf("delete %d: %v", o.liveHOP[i], err)
		}
		o.dropLive(i)
	case x < 88:
		i := r.Intn(len(o.liveHOP))
		now, err := o.primary.Temporal().PeekTx()
		if err != nil {
			o.t.Fatal(err)
		}
		err = o.primary.Rels().CloseVersion(ctx, o.liveHOP[i], now)
		if err != nil && !errors.Is(err, graph.ErrAlreadyClosed) {
			o.t.Fatalf("close %d: %v", o.liveHOP[i], err)
		}
	case x < 90 && len(o.nodes) > 8:
		k := r.Intn(len(o.nodes))
		victim := o.nodes[k]
		if err := o.primary.Nodes().Delete(ctx, victim.ID()); err != nil {
			o.t.Fatalf("delete node %d: %v", victim.ID(), err)
		}
		o.nodes[k] = o.nodes[len(o.nodes)-1]
		o.nodes = o.nodes[:len(o.nodes)-1]
		kept := o.liveHOP[:0]
		for _, id := range o.liveHOP {
			if _, err := o.primary.Rels().Get(ctx, id); err == nil {
				kept = append(kept, id)
			}
		}
		o.liveHOP = kept
	case x < 92:
		// v4.38.0 semantics on sealed rows: a valid-time correction (appends
		// the patched then-valid pieces) and a close followed by a delete
		// (the delete keeps the recorded close).
		i := r.Intn(len(o.liveHOP))
		id := o.liveHOP[i]
		now, err := o.primary.Temporal().PeekTx()
		if err != nil {
			o.t.Fatal(err)
		}
		if r.Intn(2) == 0 {
			vf := types.Instant(1_700_000_000_000 + int64(r.Intn(900_000)))
			_, err = o.primary.Temporal().SetRelVersionInterval(ctx, id, vf, vf+types.Instant(1000+r.Intn(50_000)), map[string]any{"actor": "corrected"})
			if err != nil && !errors.Is(err, graph.ErrAlreadyClosed) && !errors.Is(err, graph.ErrInvalidTimeRange) {
				o.t.Fatalf("SetRelVersionInterval %d: %v", id, err)
			}
			if err == nil {
				o.corrections++
			}
			break
		}
		if err := o.primary.Rels().CloseVersion(ctx, id, now); err != nil && !errors.Is(err, graph.ErrAlreadyClosed) {
			o.t.Fatalf("close %d: %v", id, err)
		}
		if err := o.primary.Rels().Delete(ctx, id); err != nil {
			o.t.Fatalf("delete after close %d: %v", id, err)
		}
		o.closedDeletes++
		o.dropLive(i)
	case x < 94:
		n, err := o.primary.Nodes().Add(ctx, []string{"Asset"}, nil)
		if err != nil {
			o.t.Fatalf("add node: %v", err)
		}
		o.nodes = append(o.nodes, n)
	default:
		o.addHOP(r)
	}
}

// replicate applies every new primary record to both replicas.
func (o *segOracle) replicate() {
	var recs []store.ChangeRecord
	if err := o.primary.Replication().ForEachChange(o.applied, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		o.t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) == 0 {
		return
	}
	for name, g := range map[string]*graph.Graph{"plain": o.plain, "declared": o.declared} {
		if _, err := g.Replication().ApplyChanges(recs); err != nil {
			o.t.Fatalf("%s ApplyChanges: %v", name, err)
		}
	}
	o.applied = recs[len(recs)-1].LSN
	now, err := o.primary.Temporal().PeekTx()
	if err != nil {
		o.t.Fatal(err)
	}
	o.pins = append(o.pins, now)
}

// --- canonical fingerprints ---

func segRelFP(r *types.Relationship) string {
	if r == nil {
		return "<nil>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "id=%d tok=%d %d->%d v=%d frozen=%t", r.ID(), r.TypeToken().Value(), r.StartNodeID(), r.EndNodeID(), r.Version(), r.IsFrozen())
	if tm := r.Temporal(); tm != nil {
		fmt.Fprintf(&b, " tm=%+v base=%d", *tm, tm.BaseEntityID())
	}
	if ig := r.Integrity(); ig != nil {
		fmt.Fprintf(&b, " ig=%s|%s|%s|%s|%s|%s|%d|%x", ig.Hash, ig.PrevHash, ig.FromNodeHash, ig.ToNodeHash, ig.AuthorID, ig.AuthorizedBy, ig.AuthorizationLevel, ig.Signature)
	}
	for _, p := range r.Properties() {
		fmt.Fprintf(&b, " %s=%T:%x", p.Key, p.Value, types.AppendPropertyValueHashBytes(nil, p.Value))
	}
	return b.String()
}

func segRelsFP(rs []*types.Relationship, err error) string {
	if err != nil {
		return "ERR " + segErrClass(err)
	}
	parts := make([]string, len(rs))
	for i, r := range rs {
		parts[i] = segRelFP(r)
	}
	return fmt.Sprintf("n=%d\n%s", len(rs), strings.Join(parts, "\n"))
}

func segRelMapFP(m map[types.NodeID][]*types.Relationship, err error) string {
	if err != nil {
		return "ERR " + segErrClass(err)
	}
	keys := make([]types.NodeID, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "[%d] %s\n", k, segRelsFP(m[k], nil))
	}
	return b.String()
}

func segErrClass(err error) string {
	for _, s := range []error{store.ErrRelNotFound, store.ErrNodeNotFound, store.ErrVersionNotFound, store.ErrNoVersionValidAt, store.ErrIndexNotFound, store.ErrCapabilityNotSupported} {
		if errors.Is(err, s) {
			return s.Error()
		}
	}
	return err.Error()
}

func segCollect(fn func(func(*types.Relationship) bool) error) string {
	var rs []*types.Relationship
	err := fn(func(r *types.Relationship) bool { rs = append(rs, r); return true })
	return segRelsFP(rs, err)
}

// answers evaluates every read door on g into name -> canonical answer.
func (o *segOracle) answers(g *graph.Graph) map[string]string {
	out := map[string]string{}
	put := func(name string, v string) { out[name] = v }
	rels, nodes := g.Rels(), o.nodes
	pins := append([]types.Instant(nil), o.pins...)
	pins = append(pins, 1_700_000_000_000, 1_700_000_400_000, 1_700_000_900_000)

	n, err := rels.Count()
	put("Count", fmt.Sprint(n, err))
	for _, typ := range []string{segOracleHOP, "ORIGIN"} {
		n, err := rels.CountByType(typ)
		put("CountByType/"+typ, fmt.Sprint(n, err))
		put("ByType/"+typ, segRelsFP(rels.ByType(typ, graph.QueryOpts{})))
		put("ForEachByType/"+typ, segCollect(func(f func(*types.Relationship) bool) error {
			return rels.ForEachByType(typ, graph.QueryOpts{}, f)
		}))
		st, err := g.Stats().RelCountByType(typ)
		put("Stats.RelCountByType/"+typ, fmt.Sprint(st, err))
	}
	var mid types.EntityID
	if len(o.allRels) > 0 {
		mid = types.EntityID(o.allRels[len(o.allRels)/2])
	}
	for _, opts := range []graph.QueryOpts{{Limit: 7}, {After: mid}, {After: mid, Limit: 11}} {
		k := fmt.Sprintf("%+v", opts)
		put("ByType/page"+k, segRelsFP(rels.ByType(segOracleHOP, opts)))
		put("All/page"+k, segRelsFP(rels.All(opts)))
		put("ForEachByType/page"+k, segCollect(func(f func(*types.Relationship) bool) error {
			return rels.ForEachByType(segOracleHOP, opts, f)
		}))
	}
	put("All", segRelsFP(rels.All(graph.QueryOpts{})))
	// ForEach promises no order: compare it sorted by ID.
	var fe []*types.Relationship
	err = rels.ForEach(graph.QueryOpts{}, func(r *types.Relationship) bool { fe = append(fe, r); return true })
	sort.Slice(fe, func(i, j int) bool { return fe[i].ID() < fe[j].ID() })
	put("ForEach(sorted)", segRelsFP(fe, err))
	for _, v := range []any{"actor-03", "actor-07", "f1", "f3", "o1", int64(2)} {
		for _, key := range []string{"actor", "family", "orch"} {
			k := fmt.Sprintf("%s=%T:%v", key, v, v)
			put("ByTypeAndProperty/"+k, segRelsFP(rels.ByTypeAndProperty(segOracleHOP, key, v, graph.QueryOpts{})))
		}
	}
	put("ForEachByTypePropertyRange/weight", segCollect(func(f func(*types.Relationship) bool) error {
		return rels.ForEachByTypePropertyRange(segOracleHOP, "weight", 10, 60, true, false, graph.QueryOpts{}, f)
	}))
	for _, key := range []string{"actor", "weight", "t_lo", "extra"} {
		ps, err := g.Stats().RelPropertyStats(segOracleHOP, key)
		if key == "actor" {
			// actor holds strings AND int64s (the wrong-kind writes). For a
			// mixed-family key the accumulator's documented rule is "the
			// first family observed wins", so Min/Max depend on the order a
			// rescan visits rows — on the plain store that is map order.
			// Count and NDV are order-free and compared; Min/Max are not.
			ps.Min, ps.Max = nil, nil
		}
		put("RelPropertyStats/"+key, fmt.Sprintf("%+v %v", ps, err))
		tc, err := g.Stats().RelPropertyTypeClassCounts(segOracleHOP, key)
		put("RelPropertyTypeClassCounts/"+key, fmt.Sprintf("%+v %v", tc, err))
	}
	c, exact, err := g.Stats().RelRangeCardinality(segOracleHOP, "weight", 5, 70, true, true, graph.QueryOpts{})
	put("RelRangeCardinality", fmt.Sprint(c, exact, err))
	all, err := g.Stats().AllRelTypeCounts()
	put("AllRelTypeCounts", fmt.Sprint(all, err))
	gs, err := g.Stats().RelCount()
	put("Stats.RelCount", fmt.Sprint(gs, err))

	// Point doors over every relationship ever created (deleted ones included).
	var pb strings.Builder
	for _, id := range o.allRels {
		r, err := rels.Get(o.ctx, id)
		fmt.Fprintf(&pb, "Get %d: %s\n", id, segRelsFP([]*types.Relationship{r}, err))
		h, err := rels.History(id)
		fmt.Fprintf(&pb, "History %d: %s\n", id, segRelsFP(h, err))
		ok, err := g.Hash().VerifyRelChain(id)
		fmt.Fprintf(&pb, "VerifyRelChain %d: %t %v\n", id, ok, err)
		for _, t := range pins {
			r, err := g.Temporal().RelAt(id, t)
			fmt.Fprintf(&pb, "RelAt %d@%d: %s\n", id, t, segRelsFP([]*types.Relationship{r}, err))
			r, err = g.Temporal().RelAsOf(id, t)
			fmt.Fprintf(&pb, "RelAsOf %d@%d: %s\n", id, t, segRelsFP([]*types.Relationship{r}, err))
			r, err = g.Temporal().RelAtTx(id, t, t)
			fmt.Fprintf(&pb, "RelAtTx %d@%d: %s\n", id, t, segRelsFP([]*types.Relationship{r}, err))
		}
	}
	put("PointDoors", pb.String())
	put("GetByIDs", segRelsFP(rels.GetByIDs(o.liveHOP)))

	// Adjacency over every node.
	var ab strings.Builder
	ids := make([]types.NodeID, 0, len(nodes))
	for _, nd := range nodes {
		ids = append(ids, nd.ID())
		for _, typ := range []string{"", segOracleHOP, "ORIGIN"} {
			fmt.Fprintf(&ab, "Out %d %s: %s\n", nd.ID(), typ, segRelsFP(rels.Outgoing(nd.ID(), typ)))
			fmt.Fprintf(&ab, "In %d %s: %s\n", nd.ID(), typ, segRelsFP(rels.Incoming(nd.ID(), typ)))
			fmt.Fprintf(&ab, "ForEachOut %d %s: %s\n", nd.ID(), typ, segCollect(func(f func(*types.Relationship) bool) error {
				return rels.ForEachOutgoing(nd.ID(), typ, f)
			}))
			fmt.Fprintf(&ab, "ForEachIn %d %s: %s\n", nd.ID(), typ, segCollect(func(f func(*types.Relationship) bool) error {
				return rels.ForEachIncoming(nd.ID(), typ, f)
			}))
			od, err := rels.OutgoingDegree(nd.ID(), typ)
			fmt.Fprintf(&ab, "OutDeg %d %s: %d %v\n", nd.ID(), typ, od, err)
			id, err := rels.IncomingDegree(nd.ID(), typ)
			fmt.Fprintf(&ab, "InDeg %d %s: %d %v\n", nd.ID(), typ, id, err)
			var eps []string
			err = rels.ForEachAdjacentEndpoint(nd.ID(), typ, false, func(rel types.RelID, other types.NodeID) bool {
				eps = append(eps, fmt.Sprintf("%d>%d", rel, other))
				return true
			})
			fmt.Fprintf(&ab, "Endpoints %d %s: %v %v\n", nd.ID(), typ, eps, err)
		}
		for _, t := range pins {
			fmt.Fprintf(&ab, "OutAt %d@%d: %s\n", nd.ID(), t, segRelsFP(g.Temporal().OutgoingRelsAt(nd.ID(), t)))
			fmt.Fprintf(&ab, "InAt %d@%d: %s\n", nd.ID(), t, segRelsFP(g.Temporal().IncomingRelsAt(nd.ID(), t)))
			fmt.Fprintf(&ab, "AdjRelAt %d@%d: %s\n", nd.ID(), t, segCollect(func(f func(*types.Relationship) bool) error {
				return rels.ForEachAdjacentRelAt(nd.ID(), segOracleHOP, true, graph.QueryOpts{ValidAt: t}, f)
			}))
		}
	}
	put("Adjacency", ab.String())
	put("OutgoingForNodes", segRelMapFP(rels.OutgoingForNodes(ids, segOracleHOP)))
	put("IncomingForNodes", segRelMapFP(rels.IncomingForNodes(ids, "")))

	// Set-valued temporal doors.
	for _, t := range pins {
		k := fmt.Sprint(t)
		put("RelsAt@"+k, segRelsFP(g.Temporal().RelsAt(t)))
		put("RelsByTypeAt@"+k, segRelsFP(g.Temporal().RelsByTypeAt(segOracleHOP, t)))
		put("RelsAsOf@"+k, segRelsFP(g.Temporal().RelsAsOf(t)))
		put("RelsAtTx@"+k, segRelsFP(g.Temporal().RelsAtTx(t, t)))
		put("RelsByTypePropertyAt@"+k, segRelsFP(g.Temporal().RelsByTypePropertyAt(segOracleHOP, "family", "f1", t)))
		put("ByType/ValidAt@"+k, segRelsFP(rels.ByType(segOracleHOP, graph.QueryOpts{ValidAt: t})))
		put("ByType/TxAt@"+k, segRelsFP(rels.ByType(segOracleHOP, graph.QueryOpts{TxAt: t})))
		put("OutgoingForNodesAtTx@"+k, segRelMapFP(rels.OutgoingForNodesAtTx(ids, segOracleHOP, t)))
		if snap, err := g.Temporal().Snapshot(t); err != nil {
			put("Snapshot@"+k, "ERR "+segErrClass(err))
		} else {
			put("Snapshot@"+k, fmt.Sprint(snap.RelCount, "\n", segRelsFP(snap.Relationships, nil)))
		}
	}
	for i := 1; i < len(pins); i++ {
		k := fmt.Sprint(pins[i-1], "-", pins[i])
		put("RelsDuring/"+k, segRelsFP(g.Temporal().RelsDuring(pins[i-1], pins[i])))
		put("RelsDuringTx/"+k, segRelsFP(g.Temporal().RelsDuringTx(pins[i-1], pins[i], pins[i])))
		put("ByType/During/"+k, segRelsFP(rels.ByType(segOracleHOP, graph.QueryOpts{ValidStart: pins[i-1], ValidEnd: pins[i]})))
	}

	// Column scan (ID order) and the export stream.
	var cb strings.Builder
	ok, err := g.ScanRelColumns(segOracleHOP, []string{"weight", "t_lo"}, graph.QueryOpts{}, func(b *store.RelColumnBatch) bool {
		fmt.Fprintf(&cb, "%v %v %v %v %v %v\n", b.IDs, b.StartIDs, b.EndIDs, b.ValidFrom, b.ValidTo, b.ColumnData)
		return true
	})
	put("ScanRelColumns", fmt.Sprint(ok, " ", err, "\n", cb.String()))
	var exp bytes.Buffer
	err = g.IO().Export(&exp)
	// The first record is the export header (it carries the export's own
	// wall-clock stamp); every record after it is graph content.
	body := exp.Bytes()
	if len(body) >= 5 {
		hdr := 5 + int(binary.LittleEndian.Uint32(body[1:5]))
		body = body[min(hdr, len(body)):]
	}
	put("Export(after header)", fmt.Sprintf("%v %x", err, body))
	return out
}

var segOracleProps = []string{"actor", "family", "orch", "t_lo", "weight", "hot"}

func segOracleFact(id types.RelID, start, end types.NodeID, vf, vt, tx int64, v uint32, vals []any) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %d->%d vf=%d vt=%d tx=%d v=%d", id, start, end, vf, vt, tx, v)
	for _, p := range vals {
		if p == nil {
			b.WriteString(" -")
			continue
		}
		fmt.Fprintf(&b, " %T:%x", p, types.AppendPropertyValueHashBytes(nil, p))
	}
	return b.String()
}

// scanFacts is the columnar door's view of HOP in the order it hands rows
// out, rowFacts the row door's (ID order); on the declared replica they must
// agree row for row, in order.
func (o *segOracle) scanFacts(g *graph.Graph) ([]string, bool) {
	var out []string
	ok, err := g.ScanRelSegments(segOracleHOP, segOracleProps, func(b *graph.RelSegmentBatch) bool {
		for k := 0; k < b.Len(); k++ {
			vals := make([]any, len(segOracleProps))
			for c := range segOracleProps {
				col := &b.Cols[c]
				switch {
				case col.Present[k]:
					vals[c] = col.Value(k)
				case col.Other[k]:
					r, err := b.Row(k)
					if err != nil {
						o.t.Fatal(err)
					}
					vals[c], _ = r.GetProperty(segOracleProps[c])
				}
			}
			out = append(out, segOracleFact(b.IDs[k], b.StartIDs[k], b.EndIDs[k], b.ValidFrom[k], b.ValidTo[k], b.TxFrom[k], b.Versions[k], vals))
		}
		return true
	})
	if err != nil {
		o.t.Fatalf("ScanRelSegments: %v", err)
	}
	return out, ok
}

func (o *segOracle) rowFacts(g *graph.Graph) []string {
	rs, err := g.Rels().ByType(segOracleHOP, graph.QueryOpts{})
	if err != nil {
		o.t.Fatal(err)
	}
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		var vf, vt, tx int64
		if tm := r.Temporal(); tm != nil {
			vf, vt, tx = int64(tm.ValidFrom), int64(tm.ValidTo), int64(tm.TxFrom)
		}
		vals := make([]any, len(segOracleProps))
		for c, key := range segOracleProps {
			vals[c], _ = r.GetProperty(key)
		}
		out = append(out, segOracleFact(r.ID(), r.StartNodeID(), r.EndNodeID(), vf, vt, tx, r.Version(), vals))
	}
	return out
}

func (o *segOracle) compare(stage string) {
	o.t.Helper()
	if _, ok := o.scanFacts(o.plain); ok {
		o.t.Fatalf("%s: ScanRelSegments on the undeclared replica must decline", stage)
	}
	scanGot, ok := o.scanFacts(o.declared)
	rowWant := o.rowFacts(o.plain)
	if again, _ := o.scanFacts(o.declared); strings.Join(again, "\n") != strings.Join(scanGot, "\n") {
		o.t.Fatalf("%s: ScanRelSegments handed rows out in a different order on a second call", stage)
	}
	if !ok || strings.Join(scanGot, "\n") != strings.Join(rowWant, "\n") {
		o.t.Fatalf("%s: ScanRelSegments (ok %t, %d rows) differs from the row door (%d rows):\n%s", stage, ok, len(scanGot), len(rowWant),
			segFirstDiff(strings.Join(scanGot, "\n"), strings.Join(rowWant, "\n")))
	}
	want, want2, got := o.answers(o.plain), o.answers(o.plain), o.answers(o.declared)
	names := make([]string, 0, len(want))
	for k := range want {
		names = append(names, k)
	}
	sort.Strings(names)
	bad := 0
	for _, k := range names {
		if want[k] != want2[k] {
			o.t.Fatalf("%s: door %s is nondeterministic on the plain graph", stage, k)
		}
		if want[k] != got[k] {
			bad++
			if bad <= 5 {
				o.t.Errorf("%s: door %s differs\nplain:    %.600s\ndeclared: %.600s", stage, k, segFirstDiff(want[k], got[k]), segFirstDiff(got[k], want[k]))
			}
		}
	}
	if bad > 0 {
		o.t.Fatalf("%s: %d of %d doors differ", stage, bad, len(names))
	}
}

// segFirstDiff returns a from the first line where it differs from b.
func segFirstDiff(a, b string) string {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := range al {
		if i >= len(bl) || al[i] != bl[i] {
			return fmt.Sprintf("line %d: %s", i, al[i])
		}
	}
	return "(prefix of the other)"
}

func (o *segOracle) segStats() store.RelSegmentStats {
	st, err := o.declared.Admin().RelSegmentStats(segOracleHOP)
	if err != nil {
		o.t.Fatalf("RelSegmentStats: %v", err)
	}
	return st
}

func TestRelSegmentsDifferentialOracle(t *testing.T) {
	runSegOracle(t, func(t *testing.T) string { return "" })
}

// runSegOracle is the differential oracle; segDir names the declared
// replica's segment directory ("" = in-RAM segments, S2).
func runSegOracle(t *testing.T, segDir func(*testing.T) string) {
	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			r := rand.New(rand.NewSource(seed))
			o := newSegOracleDir(t, 48<<10, segDir(t)) // ~60 HOP rows per seal
			o.seed(r, 24)
			const chunks, perChunk = 6, 90
			sealedEver := false
			for c := 0; c < chunks; c++ {
				for i := 0; i < perChunk; i++ {
					o.step(r)
				}
				o.replicate()
				if c%2 == 1 {
					if err := o.declared.Admin().SealRelSegments(segOracleHOP); err != nil {
						t.Fatalf("SealRelSegments: %v", err)
					}
				}
				st := o.segStats()
				if st.SealedRows > 0 {
					sealedEver = true
				}
				o.compare(fmt.Sprintf("chunk %d (%+v)", c, st))
			}
			st := o.segStats()
			if !sealedEver || st.Segments == 0 || st.LiveSealedRows == 0 {
				t.Fatalf("the declared replica never sealed: %+v", st)
			}
			if st.LiveSealedRows >= st.SealedRows {
				t.Fatalf("no sealed row was superseded by a later update/delete — the overlay went untested: %+v", st)
			}
			if o.corrections == 0 || o.closedDeletes == 0 {
				t.Fatalf("the v4.38.0 steps never took effect: %d corrections, %d close+delete", o.corrections, o.closedDeletes)
			}
		})
	}
}

// Two-phase temporal (testing rule 15) across a seal: a HOP row is created in
// state X at t0 and sealed; then it is updated and deleted; reads pinned at
// t0 still return X, from the declared replica exactly as from the plain one.
func TestRelSegmentsTwoPhaseAcrossSeal(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	o := newSegOracle(t, 0)
	o.seed(r, 6)
	for i := 0; i < 40; i++ {
		o.addHOP(r)
	}
	o.replicate()
	if err := o.declared.Admin().SealRelSegments(segOracleHOP); err != nil {
		t.Fatal(err)
	}
	st := o.segStats()
	if st.LiveSealedRows != int64(len(o.liveHOP)) || st.UnsealedRows != 0 {
		t.Fatalf("after an explicit seal every HOP row is sealed: %+v, %d live", st, len(o.liveHOP))
	}
	t0 := o.pins[len(o.pins)-1]
	target := o.liveHOP[3]
	before, err := o.declared.Rels().Get(o.ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.primary.Rels().Update(o.ctx, target, map[string]any{"actor": "changed"}); err != nil {
		t.Fatal(err)
	}
	if err := o.primary.Rels().Delete(o.ctx, o.liveHOP[5]); err != nil {
		t.Fatal(err)
	}
	o.replicate()
	got, err := o.declared.Temporal().RelAsOf(target, t0)
	if err != nil {
		t.Fatalf("RelAsOf(t0): %v", err)
	}
	if v, _ := got.GetProperty("actor"); v != mustProp(t, before, "actor") {
		t.Fatalf("as of t0 the sealed state X must come back: actor %v, want %v", v, mustProp(t, before, "actor"))
	}
	cur, err := o.declared.Rels().Get(o.ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := cur.GetProperty("actor"); v != "changed" {
		t.Fatalf("the update after the seal must win now: actor %v", v)
	}
	if _, err := o.declared.Rels().Get(o.ctx, o.liveHOP[5]); !errors.Is(err, store.ErrRelNotFound) {
		t.Fatalf("a sealed row deleted after the seal must be gone: %v", err)
	}
	st = o.segStats()
	if st.LiveSealedRows != int64(len(o.liveHOP))-2 {
		t.Fatalf("the updated and the deleted row left the live sealed set: %+v", st)
	}
	o.compare("two-phase")
}

func mustProp(t *testing.T, r *types.Relationship, key string) any {
	t.Helper()
	v, ok := r.GetProperty(key)
	if !ok {
		t.Fatalf("rel %d has no %q", r.ID(), key)
	}
	return v
}

func TestRelSegmentsConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  graph.Config
		want error
	}{
		{"empty type", graph.Config{RelSegments: []graph.RelSegmentSpec{{Type: " "}}}, graph.ErrRelSegmentDeclaration},
		{"reserved column", graph.Config{RelSegments: []graph.RelSegmentSpec{{Type: "HOP", Columns: []graph.SegmentColumn{{Name: "tkg_x", Kind: graph.SegmentInt}}}}}, graph.ErrRelSegmentDeclaration},
		{"bad kind", graph.Config{RelSegments: []graph.RelSegmentSpec{{Type: "HOP", Columns: []graph.SegmentColumn{{Name: "x", Kind: 99}}}}}, graph.ErrRelSegmentDeclaration},
		{"duplicate column", graph.Config{RelSegments: []graph.RelSegmentSpec{{Type: "HOP", Columns: []graph.SegmentColumn{{Name: "x", Kind: graph.SegmentInt}, {Name: "x", Kind: graph.SegmentBool}}}}}, graph.ErrRelSegmentDeclaration},
		{"duplicate type", graph.Config{RelSegments: []graph.RelSegmentSpec{{Type: "HOP"}, {Type: "HOP"}}}, graph.ErrRelSegmentDeclaration},
		{"block rows", graph.Config{RelSegments: []graph.RelSegmentSpec{{Type: "HOP", IntegrityBlockRows: 48}}}, graph.ErrRelSegmentDeclaration},
		{"negative budget", graph.Config{RelSegments: []graph.RelSegmentSpec{{Type: "HOP"}}, SegmentMemoryBudget: -1}, graph.ErrRelSegmentDeclaration},
		{"badger has no segments (S2)", graph.Config{BadgerInMemory: true, RelSegments: []graph.RelSegmentSpec{{Type: "HOP"}}}, graph.ErrCapabilityNotSupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := graph.New(tc.cfg)
			if err == nil {
				_ = g.Close()
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("New = %v, want %v", err, tc.want)
			}
		})
	}
	g, err := graph.New(graph.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := g.Admin().SealRelSegments("HOP"); !errors.Is(err, graph.ErrRelSegmentNotDeclared) {
		t.Fatalf("SealRelSegments on an unknown type = %v, want ErrRelSegmentNotDeclared", err)
	}
	a, _ := g.Nodes().Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes().Add(context.Background(), []string{"A"}, nil)
	if _, err := g.Rels().Add(context.Background(), "KNOWS", a, b, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Admin().RelSegmentStats("KNOWS"); !errors.Is(err, graph.ErrRelSegmentNotDeclared) {
		t.Fatalf("RelSegmentStats on an undeclared type = %v, want ErrRelSegmentNotDeclared", err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Admin().SealRelSegments("HOP"); !errors.Is(err, graph.ErrGraphClosed) {
		t.Fatalf("SealRelSegments after Close = %v, want ErrGraphClosed", err)
	}
	if _, err := g.Admin().RelSegmentStats("HOP"); !errors.Is(err, graph.ErrGraphClosed) {
		t.Fatalf("RelSegmentStats after Close = %v, want ErrGraphClosed", err)
	}
	if _, err := g.ScanRelSegments("HOP", nil, func(*graph.RelSegmentBatch) bool { return true }); !errors.Is(err, graph.ErrGraphClosed) {
		t.Fatalf("ScanRelSegments after Close = %v, want ErrGraphClosed", err)
	}
	var nilG *graph.Graph
	if ok, err := nilG.ScanRelSegments("HOP", nil, nil); ok || err != nil {
		t.Fatalf("nil graph ScanRelSegments = %t, %v", ok, err)
	}
	// Badger has no segment scan; an unknown type declines.
	bg, err := graph.New(graph.Config{BadgerInMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bg.Close()
	ba, _ := bg.Nodes().Add(context.Background(), []string{"A"}, nil)
	bb, _ := bg.Nodes().Add(context.Background(), []string{"A"}, nil)
	if _, err := bg.Rels().Add(context.Background(), "KNOWS", ba, bb, nil); err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"NOPE", "KNOWS"} {
		if ok, err := bg.ScanRelSegments(typ, nil, func(*graph.RelSegmentBatch) bool { return true }); ok || err != nil {
			t.Fatalf("ScanRelSegments(%s) on badger = %t, %v; want false, nil", typ, ok, err)
		}
	}
}
