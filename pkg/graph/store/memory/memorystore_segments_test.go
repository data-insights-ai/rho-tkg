package memory

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ADR-0011 S2, store level: the memory store with a declared bulk type must
// answer every relationship read door exactly like a twin store without the
// declaration, while sealed rows leave rels / typeIdx / outIdx / inIdx.

const (
	segTestHOP   uint16 = 1
	segTestOther uint16 = 2
)

func segTestDecl(budget int64) storecontract.RelSegmentDeclaration {
	return storecontract.RelSegmentDeclaration{
		TypeName: "HOP", TypeToken: segTestHOP, MemtableBudget: budget, IntegrityBlockRows: 8,
		Columns: []storecontract.SegmentColumn{
			{Name: "actor", Kind: storecontract.SegmentString},
			{Name: "weight", Kind: storecontract.SegmentFloat64},
			{Name: "n", Kind: storecontract.SegmentInt64},
		},
	}
}

// segTwin is a pair of stores fed identical writes: plain (no declaration)
// and declared.
type segTwin struct {
	t        *testing.T
	plain    *Store
	declared *Store
	nodes    []types.NodeID
	rels     []types.RelID // every rel ever put
	nextID   int64
	clock    types.Instant
}

func newSegTwin(t *testing.T, budget int64, nodes int) *segTwin {
	t.Helper()
	tw := &segTwin{t: t, plain: New(), declared: New(), nextID: 1 << 40, clock: 1_700_000_000_000}
	if err := tw.declared.DeclareRelSegment(segTestDecl(budget)); err != nil {
		t.Fatalf("DeclareRelSegment: %v", err)
	}
	for i := 0; i < nodes; i++ {
		tw.addNode()
	}
	return tw
}

func (tw *segTwin) both(name string, fn func(*Store) error) {
	tw.t.Helper()
	e1, e2 := fn(tw.plain), fn(tw.declared)
	if fmt.Sprint(e1) != fmt.Sprint(e2) {
		tw.t.Fatalf("%s: plain err %v, declared err %v", name, e1, e2)
	}
	if e1 != nil {
		tw.t.Fatalf("%s: %v", name, e1)
	}
}

func (tw *segTwin) addNode() types.NodeID {
	tw.nextID++
	id := types.NodeID(tw.nextID)
	n := types.NewNode(id, 1, nil)
	n.SetIntegrity(&types.NodeIntegrity{Hash: strings.Repeat("ab", 32)})
	tw.both("PutNode", func(s *Store) error { return s.PutNode(n) })
	tw.nodes = append(tw.nodes, id)
	return id
}

func (tw *segTwin) tick() types.Instant { tw.clock += 3; return tw.clock }

func segHashed(r *types.Relationship) *types.Relationship {
	name := map[uint16]string{segTestHOP: "HOP", segTestOther: "OTHER"}[r.TypeToken().Value()]
	ig := r.Integrity()
	if ig == nil {
		ig = &types.RelIntegrity{}
	}
	ig.Hash = integrity.ComputeRelHash(r, name)
	ig.FromNodeHash, ig.ToNodeHash = strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	r.SetIntegrity(ig)
	return r
}

func (tw *segTwin) newRel(r *rand.Rand, tok uint16) *types.Relationship {
	tw.nextID++
	a, b := tw.nodes[r.Intn(len(tw.nodes))], tw.nodes[r.Intn(len(tw.nodes))]
	rel := types.NewRelationship(types.RelID(tw.nextID), tok, a, b)
	props := map[string]any{"actor": fmt.Sprintf("a%d", r.Intn(7)), "weight": float64(r.Intn(40)), "n": int64(r.Intn(1000))}
	switch r.Intn(5) {
	case 0:
		props["extra"] = []any{int8(1), map[string]any{"k": uint16(2)}}
	case 1:
		props["n"] = int32(3)
	case 2:
		delete(props, "actor")
	}
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		tw.t.Fatal(err)
	}
	if err := rel.SetProperties(ps); err != nil {
		tw.t.Fatal(err)
	}
	now := tw.tick()
	tm := &types.TemporalMetadata{TxFrom: now}
	if r.Intn(4) == 0 {
		tm.ValidFrom, tm.ValidTo = now-100, now+types.Instant(r.Intn(50))
	}
	rel.SetTemporal(tm)
	return segHashed(rel)
}

func (tw *segTwin) put(r *rand.Rand, tok uint16) types.RelID {
	rel := tw.newRel(r, tok)
	tw.both("PutRelationship", func(s *Store) error { return s.PutRelationship(rel) })
	tw.rels = append(tw.rels, rel.ID())
	return rel.ID()
}

// liveRels returns the plain store's current relationship IDs of tok.
func (tw *segTwin) liveRels(tok uint16) []types.RelID {
	rs, err := tw.plain.RelationshipsByType(tok, QueryOpts{})
	if err != nil {
		tw.t.Fatal(err)
	}
	out := make([]types.RelID, len(rs))
	for i, r := range rs {
		out[i] = r.ID()
	}
	return out
}

// mutate applies one random update/delete through a write door.
func (tw *segTwin) mutate(r *rand.Rand) {
	live := tw.liveRels(segTestHOP)
	if len(live) == 0 {
		return
	}
	id := live[r.Intn(len(live))]
	cur, err := tw.plain.GetRelationship(id)
	if err != nil {
		tw.t.Fatal(err)
	}
	if cur.Temporal() == nil { // no history stamps to write
		tw.both("DeleteRelationship", func(s *Store) error { return s.DeleteRelationship(id) })
		return
	}
	switch r.Intn(5) {
	case 0: // in-place replace
		next := cur.DeepCopy()
		if err := next.SetProperty("actor", "replaced"); err != nil {
			tw.t.Fatal(err)
		}
		segHashed(next)
		tw.both("ReplaceRelationship", func(s *Store) error { return s.ReplaceRelationship(next) })
	case 1: // replace with history
		prev := cur.DeepCopy()
		prev.Temporal().TxTo = tw.tick()
		next := cur.DeepCopy()
		next.SetVersion(cur.Version() + 1)
		next.Temporal().TxFrom = prev.Temporal().TxTo
		if err := next.SetProperty("weight", float64(99)); err != nil {
			tw.t.Fatal(err)
		}
		segHashed(next)
		tw.both("ReplaceRelWithHistory", func(s *Store) error { return s.ReplaceRelWithHistory(next, cur.Version(), prev) })
	case 2: // delete with history
		tomb := cur.DeepCopy()
		now := tw.tick()
		tomb.Temporal().DeletedAt, tomb.Temporal().ValidTo, tomb.Temporal().TxTo = now, now, now
		tw.both("DeleteRelWithHistory", func(s *Store) error { return s.DeleteRelWithHistory(id, cur.Version(), tomb) })
	case 3: // hard delete
		tw.both("DeleteRelationship", func(s *Store) error { return s.DeleteRelationship(id) })
	default: // batch delete of two
		ids := []types.RelID{id, live[r.Intn(len(live))]}
		tw.both("DeleteRelationshipsBatch", func(s *Store) error { return s.DeleteRelationshipsBatch(ids) })
	}
}

func segFP(r *types.Relationship) string {
	if r == nil {
		return "<nil>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d/%d %d->%d v%d f=%t", r.ID(), r.TypeToken().Value(), r.StartNodeID(), r.EndNodeID(), r.Version(), r.IsFrozen())
	if tm := r.Temporal(); tm != nil {
		fmt.Fprintf(&b, " %+v", *tm)
	}
	if ig := r.Integrity(); ig != nil {
		fmt.Fprintf(&b, " %s %s %s %s", ig.Hash, ig.PrevHash, ig.FromNodeHash, ig.ToNodeHash)
	}
	for _, p := range r.Properties() {
		fmt.Fprintf(&b, " %s=%T:%x", p.Key, p.Value, types.AppendPropertyValueHashBytes(nil, p.Value))
	}
	return b.String()
}

func segFPs(rs []*types.Relationship, err error) string {
	if err != nil {
		return "ERR " + err.Error()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "n=%d", len(rs))
	for _, r := range rs {
		b.WriteString("\n" + segFP(r))
	}
	return b.String()
}

func segCollectFn(fn func(func(*types.Relationship) bool) error) string {
	var rs []*types.Relationship
	err := fn(func(r *types.Relationship) bool { rs = append(rs, r); return true })
	return segFPs(rs, err)
}

func segIDs(ids []types.RelID, err error) string {
	if err != nil {
		return "ERR " + err.Error()
	}
	return fmt.Sprint(ids)
}

// storeAnswers evaluates every relationship read door of s.
func (tw *segTwin) storeAnswers(s *Store) map[string]string {
	out := map[string]string{}
	put := func(k, v string) { out[k] = v }
	ids := tw.rels
	var mid types.EntityID
	if len(ids) > 0 {
		mid = types.EntityID(ids[len(ids)/2])
	}
	n, err := s.RelationshipCount()
	put("RelationshipCount", fmt.Sprint(n, err))
	for _, tok := range []uint16{segTestHOP, segTestOther} {
		k := fmt.Sprint(tok)
		c, err := s.RelCountByType(tok)
		put("RelCountByType/"+k, fmt.Sprint(c, err))
		for _, o := range []QueryOpts{{}, {Limit: 5}, {After: mid}, {After: mid, Limit: 3}, {ValidAt: tw.clock - 50}, {ValidStart: tw.clock - 200, ValidEnd: tw.clock - 20}} {
			ko := fmt.Sprintf("%s%+v", k, o)
			put("RelationshipsByType/"+ko, segFPs(s.RelationshipsByType(tok, o)))
			put("ForEachRelByType/"+ko, segCollectFn(func(f func(*types.Relationship) bool) error { return s.ForEachRelByType(tok, o, f) }))
			put("AllRelationships/"+ko, segFPs(s.AllRelationships(o)))
			put("AllRelIDs/"+ko, segIDs(s.AllRelIDs(o)))
			put("RelationshipsByTypeAndProperty/"+ko, segFPs(s.RelationshipsByTypeAndProperty(tok, "actor", "a3", o)))
			put("ForEachRelByTypePropertyRange/"+ko, segCollectFn(func(f func(*types.Relationship) bool) error {
				return s.ForEachRelByTypePropertyRange(tok, "weight", 5, 25, true, true, o, f)
			}))
		}
		put("ForEachRelByTypePropertyRangeOrdered/"+k, segCollectFn(func(f func(*types.Relationship) bool) error {
			return s.ForEachRelByTypePropertyRangeOrdered(tok, "weight", 5, 25, true, false, true, f)
		}))
		put("ForEachRelByTypePropertyPrefix/"+k, segCollectFn(func(f func(*types.Relationship) bool) error {
			return s.ForEachRelByTypePropertyPrefix(tok, "actor", "a", false, f)
		}))
		for _, key := range []string{"actor", "weight", "n", "extra"} {
			ps, err := s.RelPropertyStats(tok, key)
			put("RelPropertyStats/"+k+key, fmt.Sprintf("%+v %v", ps, err))
			tc, err := s.RelPropertyTypeClassCounts(tok, key)
			put("RelPropertyTypeClassCounts/"+k+key, fmt.Sprintf("%+v %v", tc, err))
		}
		c64, exact, err := s.RelRangeCardinality(tok, "weight", 3, 30, true, true)
		put("RelRangeCardinality/"+k, fmt.Sprint(c64, exact, err))
		var cb strings.Builder
		err = s.ScanRelColumns(tok, []string{"weight"}, QueryOpts{}, func(b *storecontract.RelColumnBatch) bool {
			fmt.Fprintf(&cb, "%v %v %v %v %v %v\n", b.IDs, b.StartIDs, b.EndIDs, b.ValidFrom, b.ValidTo, b.ColumnData)
			return true
		})
		put("ScanRelColumns/"+k, fmt.Sprint(err, cb.String()))
		var mem []string
		err = s.ForEachRelTypeTxMember(tok, func(id types.RelID, first types.Instant) bool {
			mem = append(mem, fmt.Sprint(id, "@", first))
			return true
		})
		sort.Strings(mem)
		put("ForEachRelTypeTxMember/"+k, fmt.Sprint(mem, err))
		kept, ok := s.PruneRelTypeTemporalCandidates(tok, ids, QueryOpts{ValidAt: tw.clock - 60})
		put("PruneRelTypeTemporalCandidates/"+k, fmt.Sprint(kept, ok))
	}
	var fe []types.RelID
	err = s.ForEachRelID(func(id types.RelID) bool { fe = append(fe, id); return true })
	sort.Slice(fe, func(i, j int) bool { return fe[i] < fe[j] })
	put("ForEachRelID(sorted)", segIDs(fe, err))
	var del []types.RelID
	err = s.ForEachDeletedRelID(func(id types.RelID) bool { del = append(del, id); return true })
	sort.Slice(del, func(i, j int) bool { return del[i] < del[j] })
	put("ForEachDeletedRelID(sorted)", segIDs(del, err))
	put("AllRelHistoryIDs", fmt.Sprint(s.AllRelHistoryIDs()))
	for _, tx := range []types.Instant{tw.clock - 300, tw.clock - 30, tw.clock} {
		put(fmt.Sprint("RelsAsOf@", tx), segFPs(s.RelsAsOf(tx)))
	}
	var pb strings.Builder
	var live []types.RelID
	for _, id := range ids {
		r, err := s.GetRelationship(id)
		fmt.Fprintf(&pb, "Get %d %s\n", id, segFPs([]*types.Relationship{r}, err))
		if err == nil {
			live = append(live, id)
		}
		h, err := s.GetRelHistory(id)
		fmt.Fprintf(&pb, "History %d %s\n", id, segFPs(h, err))
		for _, tx := range []types.Instant{tw.clock - 300, tw.clock - 30, tw.clock} {
			r, err := s.RelAsOf(id, tx)
			fmt.Fprintf(&pb, "RelAsOf %d@%d %s\n", id, tx, segFPs([]*types.Relationship{r}, err))
		}
		w, ok := s.RelBeliefWatermark(id)
		fmt.Fprintf(&pb, "RelBeliefWatermark %d %d %t\n", id, w, ok)
	}
	put("PointDoors", pb.String())
	put("GetRelationshipsByIDs", segFPs(s.GetRelationshipsByIDs(live)))
	var ab strings.Builder
	for _, nid := range tw.nodes {
		for _, tok := range []uint16{0, segTestHOP, segTestOther} {
			fmt.Fprintf(&ab, "Out %d/%d %s\n", nid, tok, segFPs(s.OutgoingRelationships(nid, tok)))
			fmt.Fprintf(&ab, "In %d/%d %s\n", nid, tok, segFPs(s.IncomingRelationships(nid, tok)))
			fmt.Fprintf(&ab, "FEOut %d/%d %s\n", nid, tok, segCollectFn(func(f func(*types.Relationship) bool) error { return s.ForEachOutgoingRel(nid, tok, f) }))
			fmt.Fprintf(&ab, "FEIn %d/%d %s\n", nid, tok, segCollectFn(func(f func(*types.Relationship) bool) error { return s.ForEachIncomingRel(nid, tok, f) }))
			od, err := s.OutgoingDegree(nid, tok)
			fmt.Fprintf(&ab, "OutDeg %d/%d %d %v\n", nid, tok, od, err)
			id, err := s.IncomingDegree(nid, tok)
			fmt.Fprintf(&ab, "InDeg %d/%d %d %v\n", nid, tok, id, err)
		}
	}
	put("Adjacency", ab.String())
	om, err := s.OutgoingRelationshipsForNodes(tw.nodes, segTestHOP)
	put("OutgoingRelationshipsForNodes", segMapFP(om, err))
	im, err := s.IncomingRelationshipsForNodes(tw.nodes, 0)
	put("IncomingRelationshipsForNodes", segMapFP(im, err))
	return out
}

func segMapFP(m map[types.NodeID][]*types.Relationship, err error) string {
	if err != nil {
		return "ERR " + err.Error()
	}
	keys := make([]types.NodeID, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "[%d] %s\n", k, segFPs(m[k], nil))
	}
	return b.String()
}

func (tw *segTwin) compare(stage string) {
	tw.t.Helper()
	want, got := tw.storeAnswers(tw.plain), tw.storeAnswers(tw.declared)
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	bad := 0
	for _, k := range keys {
		if want[k] != got[k] {
			bad++
			if bad <= 4 {
				tw.t.Errorf("%s: door %s differs\nplain:    %.500s\ndeclared: %.500s", stage, k, segLineDiff(want[k], got[k]), segLineDiff(got[k], want[k]))
			}
		}
	}
	if bad > 0 {
		tw.t.Fatalf("%s: %d of %d doors differ", stage, bad, len(keys))
	}
}

func segLineDiff(a, b string) string {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := range al {
		if i >= len(bl) || al[i] != bl[i] {
			return fmt.Sprintf("line %d: %s", i, al[i])
		}
	}
	return "(prefix)"
}

func (tw *segTwin) stats() storecontract.RelSegmentStats {
	st, err := tw.declared.RelSegmentStats(segTestHOP)
	if err != nil {
		tw.t.Fatal(err)
	}
	return st
}

// sealedLeftRowMaps asserts that no live sealed row is still held in the
// row store's per-row maps.
func (tw *segTwin) sealedLeftRowMaps() {
	tw.t.Helper()
	s := tw.declared
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := s.segTypes[segTestHOP]
	if st == nil || len(st.segs) == 0 {
		tw.t.Fatal("no segments")
	}
	for _, sg := range st.segs {
		err := sg.seg.Scan(func(_ int, r *types.Relationship) bool {
			id := r.ID()
			if _, dead := s.segDead[id]; dead {
				return true
			}
			if _, ok := s.rels[id]; ok {
				tw.t.Errorf("sealed live row %d still in rels", id)
			}
			if _, ok := s.typeIdx[segTestHOP][id]; ok {
				tw.t.Errorf("sealed live row %d still in typeIdx", id)
			}
			if ok := s.outIdx[r.StartNodeID()].has(id); ok {
				tw.t.Errorf("sealed live row %d still in outIdx", id)
			}
			if ok := s.inIdx[r.EndNodeID()].has(id); ok {
				tw.t.Errorf("sealed live row %d still in inIdx", id)
			}
			return true
		})
		if err != nil {
			tw.t.Fatal(err)
		}
	}
}

func TestSegments_StoreDifferentialOracle(t *testing.T) {
	for _, seed := range []int64{11, 12} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			r := rand.New(rand.NewSource(seed))
			tw := newSegTwin(t, 24<<10, 16)
			// Indexes on both twins: the property index and the temporal index
			// are keyed by ID and must stay consistent across seals.
			tw.both("CreateRelPropertyIndex", func(s *Store) error { return s.CreateRelPropertyIndex(segTestHOP, "actor") })
			tw.both("CreateRelPropertyIndex", func(s *Store) error { return s.CreateRelPropertyIndex(segTestHOP, "weight") })
			for round := 0; round < 5; round++ {
				for i := 0; i < 120; i++ {
					switch x := r.Intn(10); {
					case x < 6:
						tw.put(r, segTestHOP)
					case x < 7:
						tw.put(r, segTestOther)
					default:
						tw.mutate(r)
					}
				}
				if round == 1 {
					tw.both("CreateRelTemporalIndex", func(s *Store) error { return s.CreateRelTemporalIndex(segTestHOP) })
				}
				if round%2 == 0 {
					if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
						t.Fatal(err)
					}
				}
				tw.compare(fmt.Sprintf("round %d %+v", round, tw.stats()))
			}
			st := tw.stats()
			if st.Segments < 2 || st.LiveSealedRows == 0 || st.LiveSealedRows == st.SealedRows {
				t.Fatalf("seals and post-seal overlay writes must both have happened: %+v", st)
			}
			tw.sealedLeftRowMaps()
			// A batch put and a node cascade delete touching sealed rows.
			batch := []*types.Relationship{tw.newRel(r, segTestHOP), tw.newRel(r, segTestHOP)}
			tw.both("PutRelationshipsBatch", func(s *Store) error { return s.PutRelationshipsBatch(batch) })
			for _, b := range batch {
				tw.rels = append(tw.rels, b.ID())
			}
			victim := tw.nodes[0]
			tw.both("DeleteNodeCascade", func(s *Store) error { return s.DeleteNodeCascade(victim) })
			tw.nodes = tw.nodes[1:]
			tw.compare("after batch + cascade")
		})
	}
}

func TestSegments_SealOfEmptyTypeIsNoOp(t *testing.T) {
	s := New()
	if err := s.DeclareRelSegment(segTestDecl(0)); err != nil {
		t.Fatal(err)
	}
	if err := s.SealRelSegments(segTestHOP); err != nil {
		t.Fatalf("seal of an empty type: %v", err)
	}
	st, err := s.RelSegmentStats(segTestHOP)
	if err != nil || st != (storecontract.RelSegmentStats{}) {
		t.Fatalf("empty type stats = %+v, %v; want zero", st, err)
	}
	if n, err := s.RelCountByType(segTestHOP); n != 0 || err != nil {
		t.Fatalf("RelCountByType = %d, %v", n, err)
	}
}

func TestSegments_DeclarationAfterRowsExist(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	tw := &segTwin{t: t, plain: New(), declared: New(), nextID: 1 << 40, clock: 1_700_000_000_000}
	for i := 0; i < 10; i++ {
		tw.addNode()
	}
	for i := 0; i < 200; i++ {
		tw.put(r, segTestHOP)
	}
	if err := tw.declared.DeclareRelSegment(segTestDecl(0)); err != nil {
		t.Fatal(err)
	}
	st := tw.stats()
	if st.UnsealedRows != 200 || st.Segments != 0 {
		t.Fatalf("existing rows count as unsealed after the declaration: %+v", st)
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	st = tw.stats()
	if st.LiveSealedRows != 200 || st.UnsealedRows != 0 || st.Segments != 1 {
		t.Fatalf("a seal after a late declaration moves every existing row: %+v", st)
	}
	tw.sealedLeftRowMaps()
	tw.compare("late declaration")
}

func TestSegments_DeclarationRules(t *testing.T) {
	s := New()
	d := segTestDecl(1 << 20)
	if err := s.DeclareRelSegment(d); err != nil {
		t.Fatal(err)
	}
	if err := s.DeclareRelSegment(d); err != nil {
		t.Fatalf("identical re-declaration is a no-op: %v", err)
	}
	d2 := d
	d2.Columns = d.Columns[:1]
	if err := s.DeclareRelSegment(d2); !errors.Is(err, storecontract.ErrRelSegmentDeclaration) {
		t.Fatalf("a different schema for a declared token = %v, want ErrRelSegmentDeclaration", err)
	}
	d3 := d
	d3.TypeToken = 9
	if err := s.DeclareRelSegment(d3); !errors.Is(err, storecontract.ErrRelSegmentDeclaration) {
		t.Fatalf("the same name under another token = %v, want ErrRelSegmentDeclaration", err)
	}
	d4 := segTestDecl(7 << 20)
	d4.TypeName, d4.TypeToken = "OTHER", segTestOther
	if err := s.DeclareRelSegment(d4); !errors.Is(err, storecontract.ErrRelSegmentDeclaration) {
		t.Fatalf("a second, different memtable budget = %v, want ErrRelSegmentDeclaration", err)
	}
	for _, bad := range []storecontract.RelSegmentDeclaration{
		{TypeName: "", TypeToken: 3},
		{TypeName: "X", TypeToken: 0},
		{TypeName: "X", TypeToken: 3, IntegrityBlockRows: 3},
		{TypeName: "X", TypeToken: 3, Columns: []storecontract.SegmentColumn{{Name: "c", Kind: 0}}},
		{TypeName: "X", TypeToken: 3, MemtableBudget: -1},
	} {
		if err := s.DeclareRelSegment(bad); !errors.Is(err, storecontract.ErrRelSegmentDeclaration) {
			t.Fatalf("DeclareRelSegment(%+v) = %v, want ErrRelSegmentDeclaration", bad, err)
		}
	}
	if err := s.SealRelSegments(4); !errors.Is(err, storecontract.ErrRelSegmentNotDeclared) {
		t.Fatalf("SealRelSegments(undeclared) = %v", err)
	}
	if _, err := s.RelSegmentStats(4); !errors.Is(err, storecontract.ErrRelSegmentNotDeclared) {
		t.Fatalf("RelSegmentStats(undeclared) = %v", err)
	}
	var nilStore *Store
	if err := nilStore.DeclareRelSegment(d); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil DeclareRelSegment = %v", err)
	}
	if err := nilStore.SealRelSegments(1); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil SealRelSegments = %v", err)
	}
	if _, err := nilStore.RelSegmentStats(1); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil RelSegmentStats = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.SealRelSegments(segTestHOP); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("SealRelSegments after Close = %v", err)
	}
	if err := s.DeclareRelSegment(d); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("DeclareRelSegment after Close = %v", err)
	}
	if _, err := s.RelSegmentStats(segTestHOP); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("RelSegmentStats after Close = %v", err)
	}
}

// A row the codec cannot seal (no stored hash, or a stored hash that does not
// match its content — e.g. a tampered row a VerifyRelChain test plants) stays
// in the row store with its answers unchanged; every other row is sealed.
func TestSegments_UnsealableRowsStayInMemtable(t *testing.T) {
	r := rand.New(rand.NewSource(8))
	tw := newSegTwin(t, 0, 6)
	for i := 0; i < 50; i++ {
		tw.put(r, segTestHOP)
	}
	noHash := tw.newRel(r, segTestHOP)
	noHash.Integrity().Hash = ""
	tw.both("PutRelationship(no hash)", func(s *Store) error { return s.PutRelationship(noHash) })
	tampered := tw.newRel(r, segTestHOP)
	tampered.Integrity().Hash = strings.Repeat("0", 64)
	tw.both("PutRelationship(tampered)", func(s *Store) error { return s.PutRelationship(tampered) })
	tw.rels = append(tw.rels, noHash.ID(), tampered.ID())
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatalf("seal with unsealable rows present: %v", err)
	}
	st := tw.stats()
	if st.LiveSealedRows != 50 || st.UnsealedRows != 2 {
		t.Fatalf("the 50 good rows seal, the 2 bad ones stay: %+v", st)
	}
	tw.compare("unsealable rows")
}

// Concurrent readers during seals (run with -race): every reader sees the
// full, unchanged baseline on every read while a writer keeps adding HOP rows
// and updating sealed ones, and the store keeps sealing.
func TestSegments_ConcurrentReadsDuringSeal(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	tw := newSegTwin(t, 16<<10, 12)
	var base []types.RelID
	for i := 0; i < 400; i++ {
		base = append(base, tw.put(r, segTestHOP))
	}
	want := map[types.RelID]string{}
	for _, id := range base {
		rel, err := tw.plain.GetRelationship(id)
		if err != nil {
			t.Fatal(err)
		}
		want[id] = segFP(rel)
	}
	s := tw.declared
	stop := make(chan struct{})
	var wg sync.WaitGroup
	errc := make(chan error, 16)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				seen := map[types.RelID]bool{}
				check := func(rel *types.Relationship) {
					if exp, ok := want[rel.ID()]; ok {
						seen[rel.ID()] = true
						got := segFP(rel)
						// Scans hand out frozen rows, point reads unfrozen ones.
						got = strings.Replace(got, " f=true", " f=false", 1)
						if got != exp {
							errc <- fmt.Errorf("reader %d: row %d changed:\n%s\n%s", w, rel.ID(), got, exp)
						}
					}
				}
				switch i % 4 {
				case 0:
					rs, err := s.RelationshipsByType(segTestHOP, QueryOpts{})
					if err != nil {
						errc <- err
						return
					}
					for _, rel := range rs {
						check(rel)
					}
				case 1:
					if err := s.ForEachRelByType(segTestHOP, QueryOpts{}, func(rel *types.Relationship) bool { check(rel); return true }); err != nil {
						errc <- err
						return
					}
				case 2:
					for _, id := range base {
						rel, err := s.GetRelationship(id)
						if err != nil {
							errc <- fmt.Errorf("reader %d: Get %d: %v", w, id, err)
							return
						}
						check(rel)
					}
				default:
					for _, nid := range tw.nodes {
						rs, err := s.OutgoingRelationships(nid, segTestHOP)
						if err != nil {
							errc <- err
							return
						}
						for _, rel := range rs {
							check(rel)
						}
					}
				}
				if len(seen) != len(base) {
					errc <- fmt.Errorf("reader %d (mode %d): saw %d of %d baseline rows", w, i%4, len(seen), len(base))
					return
				}
			}
		}(w)
	}
	// Writer: new rows (so seals keep triggering) and explicit seals.
	wr := rand.New(rand.NewSource(4))
	for i := 0; i < 600; i++ {
		rel := tw.newRel(wr, segTestHOP)
		if err := s.PutRelationship(rel); err != nil {
			t.Fatal(err)
		}
		if i%150 == 0 {
			if err := s.SealRelSegments(segTestHOP); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(stop)
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	waitSealsForTest(t, s)
	if st := tw.stats(); st.Seals < 3 {
		t.Fatalf("the budget and the explicit seals must have sealed several times: %+v", st)
	}
}

// Clear drops every segment (the store is empty again) but keeps the
// declaration: new rows seal again.
func TestSegments_ClearKeepsDeclaration(t *testing.T) {
	r := rand.New(rand.NewSource(6))
	tw := newSegTwin(t, 0, 5)
	for i := 0; i < 30; i++ {
		tw.put(r, segTestHOP)
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	tw.both("Clear", func(s *Store) error { return s.Clear() })
	tw.rels, tw.nodes = nil, nil
	if st := tw.stats(); st != (storecontract.RelSegmentStats{}) {
		t.Fatalf("Clear leaves no segment: %+v", st)
	}
	for i := 0; i < 5; i++ {
		tw.addNode()
	}
	for i := 0; i < 30; i++ {
		tw.put(r, segTestHOP)
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	if st := tw.stats(); st.LiveSealedRows != 30 {
		t.Fatalf("the declaration survives Clear: %+v", st)
	}
	tw.compare("after Clear")
}

// Write doors that probe existence see sealed rows: a create with a sealed
// row's ID is a duplicate, and a node whose only relationships are sealed is
// still connected.
func TestSegments_WriteDoorsSeeSealedRows(t *testing.T) {
	r := rand.New(rand.NewSource(21))
	tw := newSegTwin(t, 0, 4)
	for i := 0; i < 4200; i++ { // above the 4,096-row map-shrink threshold
		tw.put(r, segTestHOP)
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	if st := tw.stats(); st.LiveSealedRows != 4200 || st.UnsealedRows != 0 {
		t.Fatalf("setup: %+v", st)
	}
	tw.sealedLeftRowMaps()
	sealed, err := tw.declared.GetRelationship(tw.rels[7])
	if err != nil {
		t.Fatal(err)
	}
	dup := sealed.DeepCopy()
	for name, fn := range map[string]func() error{
		"PutRelationship":       func() error { return tw.declared.PutRelationship(dup) },
		"PutRelationshipsBatch": func() error { return tw.declared.PutRelationshipsBatch([]*types.Relationship{dup}) },
	} {
		if err := fn(); !errors.Is(err, ErrRelExists) {
			t.Fatalf("%s with a sealed row's ID = %v, want ErrRelExists", name, err)
		}
	}
	for _, nid := range tw.nodes {
		if err := tw.declared.DeleteNode(nid); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("DeleteNode of a node with only sealed relationships = %v, want ErrInvalidStoreMutation", err)
		}
		if err := tw.declared.DeleteNodesBatch([]types.NodeID{nid}); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("DeleteNodesBatch of a node with only sealed relationships = %v, want ErrInvalidStoreMutation", err)
		}
	}
	tw.both("DeleteNodeCascade", func(s *Store) error { return s.DeleteNodeCascade(tw.nodes[0]) })
	tw.nodes = tw.nodes[1:]
	tw.compare("after cascade over sealed rows")
}

// Endpoint bytes must not grow with the number of segments at a fixed
// endpoint set: node identities and hashes are held once per store, and what
// a segment repeats per endpoint (its adjacency directory: a code and two CSR
// offsets) stays small. The same 2,400 rows over 150 endpoints are sealed
// once, and in 8 seals of 300 rows (each touching most endpoints).
func TestSegments_NodeBytesDoNotGrowWithSegmentCount(t *testing.T) {
	const endpoints, rows, perSeal = 150, 2400, 300
	build := func(sealEvery int) (*segTwin, int, int) {
		r := rand.New(rand.NewSource(31))
		tw := newSegTwin(t, 1<<40, endpoints)
		for i := 1; i <= rows; i++ {
			tw.put(r, segTestHOP)
			if sealEvery > 0 && i%sealEvery == 0 {
				if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
			t.Fatal(err)
		}
		per, shared := NodeBytesForTest(tw.declared, segTestHOP)
		return tw, per, shared
	}
	one, per1, shared1 := build(0)
	eight, per8, shared8 := build(perSeal)
	if s1, s8 := one.stats(), eight.stats(); s1.Segments != 1 || s8.Segments != rows/perSeal {
		t.Fatalf("setup: %d and %d segments", s1.Segments, s8.Segments)
	}
	extra := float64(per8-per1) / float64((rows/perSeal-1)*endpoints)
	t.Logf("endpoint bytes: 1 segment %d + %d shared, 8 segments %d + %d shared; %.1f B per endpoint per extra segment",
		per1, shared1, per8, shared8, extra)
	if extra > 8 {
		t.Fatalf("each extra segment repeats %.1f B per endpoint (> 8): endpoint identity/hashes are stored per segment", extra)
	}
	if shared8 > shared1+shared1/10 {
		t.Fatalf("the shared endpoint dictionary grew with the segment count: %d -> %d bytes", shared1, shared8)
	}
	eight.compare("8 segments")
}

// A declared string column's values are held once per store as well: its
// bytes must not grow with the number of segments at a fixed value set
// (per-segment dictionaries repeat the values; small segments of a
// high-cardinality column even fall back to plain strings). 9,600 rows with
// 400 distinct actors are sealed once and in 8 seals of 1,200 rows.
func TestSegments_StringColumnBytesDoNotGrowWithSegmentCount(t *testing.T) {
	const values, rows, perSeal = 400, 9600, 1200
	build := func(sealEvery int) (*segTwin, int, int) {
		r := rand.New(rand.NewSource(33))
		tw := newSegTwin(t, 1<<40, 40)
		for i := 1; i <= rows; i++ {
			rel := tw.newRel(r, segTestHOP)
			if err := rel.SetProperty("actor", fmt.Sprintf("actor-%05d-%s", r.Intn(values), strings.Repeat("x", 12))); err != nil {
				t.Fatal(err)
			}
			segHashed(rel)
			tw.both("PutRelationship", func(s *Store) error { return s.PutRelationship(rel) })
			tw.rels = append(tw.rels, rel.ID())
			if sealEvery > 0 && i%sealEvery == 0 {
				if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
			t.Fatal(err)
		}
		return tw, SealedSectionBytesForTest(tw.declared, segTestHOP)["prop:actor"], sharedStringBytesForTest(tw.declared, segTestHOP, "actor")
	}
	one, col1, shared1 := build(0)
	eight, col8, shared8 := build(perSeal)
	t.Logf("actor bytes: 1 segment %d + %d shared, 8 segments %d + %d shared", col1, shared1, col8, shared8)
	if col8 > col1+col1/4 {
		t.Fatalf("the actor column grew from %d to %d bytes with 8 segments: its values are stored per segment", col1, col8)
	}
	if shared8 > shared1+shared1/10 {
		t.Fatalf("the shared actor dictionary grew with the segment count: %d -> %d", shared1, shared8)
	}
	_ = one
	eight.compare("8 segments, shared strings")
}

// waitSealsForTest waits until no background seal is running.
func waitSealsForTest(t *testing.T, ms *Store) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for sealerBusyForTest(ms) {
		if time.Now().After(deadline) {
			t.Fatal("background seal did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

// A budget seal runs off the writing goroutine: the write that pushes the
// memtable over the budget returns while the seal is still encoding, reads
// during the seal answer like the twin, and a writer that gets a full budget
// ahead of the sealer blocks until it catches up (ADR-0011 §3.5).
func TestSegments_BudgetSealRunsOffTheWriter(t *testing.T) {
	r := rand.New(rand.NewSource(51))
	tw := newSegTwin(t, 12<<10, 8)
	s := tw.declared
	entered, release := make(chan struct{}, 64), make(chan struct{})
	s.sealEncodeHook = func() { entered <- struct{}{}; <-release }
	defer func() { s.sealEncodeHook = nil }()

	// Write until a write crosses the budget (it starts the sealer before it
	// returns); every write must return promptly, and the seal then reaches
	// its encode window on the sealer, not in a writer.
	for i := 0; i < 400 && !sealerBusyForTest(s); i++ {
		done := make(chan struct{})
		go func() { tw.put(r, segTestHOP); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("write %d did not return while a budget seal was encoding: the seal runs in the writer", i)
		}
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("no budget seal started")
	}
	if !tw.stats().Sealing {
		t.Fatal("RelSegmentStats.Sealing is false while a background seal encodes")
	}
	tw.compare("reads while a background seal encodes")

	// Keep writing: once the unsealed rows reach twice the budget, a write
	// blocks until the sealer catches up.
	blocked := false
	for i := 0; i < 400 && !blocked; i++ {
		done := make(chan struct{})
		rel := tw.newRel(r, segTestHOP)
		if err := tw.plain.PutRelationship(rel); err != nil {
			t.Fatal(err)
		}
		tw.rels = append(tw.rels, rel.ID())
		go func() {
			if err := s.PutRelationship(rel); err != nil {
				t.Error(err)
			}
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
			blocked = true
			close(release)
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("the blocked write did not resume after the sealer caught up")
			}
		}
	}
	if !blocked {
		close(release)
		t.Fatal("no write blocked although the sealer never finished: the memtable is unbounded")
	}
	waitSealsForTest(t, s)
	if st := tw.stats(); st.Seals == 0 || st.Sealing {
		t.Fatalf("no seal completed, or still sealing: %+v", st)
	}
	tw.compare("after the background seals")
}

// Close waits for a background seal in flight, and the seal's segment is
// discarded (the store is closed); nothing outlives Close.
func TestSegments_CloseWaitsForTheBackgroundSealer(t *testing.T) {
	r := rand.New(rand.NewSource(52))
	tw := newSegTwin(t, 8<<10, 6)
	s := tw.declared
	entered, release := make(chan struct{}, 64), make(chan struct{})
	s.sealEncodeHook = func() { entered <- struct{}{}; <-release }
	for i := 0; i < 400 && !sealerBusyForTest(s); i++ {
		tw.put(r, segTestHOP)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no background seal started")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned (%v) while a background seal was still encoding", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Close did not return after the seal finished")
	}
	if sealerBusyForTest(s) {
		t.Fatal("a sealer is still running after Close")
	}
}

// segScanRow is what one row of a scan door says about a relationship.
func segScanFact(id types.RelID, start, end types.NodeID, vf, vt, tx int64, v uint32, props []any) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %d->%d vf=%d vt=%d tx=%d v=%d", id, start, end, vf, vt, tx, v)
	for _, p := range props {
		if p == nil {
			b.WriteString(" <absent>")
			continue
		}
		fmt.Fprintf(&b, " %T:%x", p, types.AppendPropertyValueHashBytes(nil, p))
	}
	return b.String()
}

func segRowFact(r *types.Relationship, props []string) string {
	var vf, vt, tx int64
	if tm := r.Temporal(); tm != nil {
		vf, vt, tx = int64(tm.ValidFrom), int64(tm.ValidTo), int64(tm.TxFrom)
	}
	vals := make([]any, len(props))
	for i, k := range props {
		if v, ok := r.GetProperty(k); ok {
			vals[i] = v
		}
	}
	return segScanFact(r.ID(), r.StartNodeID(), r.EndNodeID(), vf, vt, tx, r.Version(), vals)
}

// ScanRelSegments hands out exactly the current rows the row doors return,
// with every field and requested property equal — values from the columns,
// or from Row(k) where the column says Other — over sealed rows, dead sealed
// rows, faulted-in rows and the unsealed memtable.
func TestSegments_ScanRelSegmentsEqualsTheRowPath(t *testing.T) {
	r := rand.New(rand.NewSource(61))
	tw := newSegTwin(t, 1<<40, 12)
	noTemporal := 0
	for round := 0; round < 4; round++ {
		for i := 0; i < 700; i++ {
			switch x := r.Intn(12); {
			case x < 2:
				tw.mutate(r)
			case x < 3: // a row without temporal metadata
				rel := tw.newRel(r, segTestHOP)
				rel.SetTemporal(nil)
				tw.both("PutRelationship", func(s *Store) error { return s.PutRelationship(rel) })
				tw.rels = append(tw.rels, rel.ID())
				noTemporal++
			default:
				tw.put(r, segTestHOP)
			}
		}
		if round < 3 {
			if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
				t.Fatal(err)
			}
		}
	}
	props := []string{"weight", "actor", "n"}
	want := map[string]bool{}
	wantRows := map[types.RelID]string{}
	rows, err := tw.plain.RelationshipsByType(segTestHOP, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		want[segRowFact(row, props)] = true
		wantRows[row.ID()] = segFP(row)
	}
	got := map[string]bool{}
	segments := map[uint64]bool{}
	others := 0
	var lastID types.RelID
	ok, err := tw.declared.ScanRelSegments(segTestHOP, props, func(b *storecontract.RelSegmentBatch) bool {
		if b.Segment != 0 {
			segments[b.Segment] = true
		}
		if b.Sorted || b.Len() == 0 {
			t.Fatalf("batch of segment %d: Sorted %t, %d rows", b.Segment, b.Sorted, b.Len())
		}
		for k := 0; k < b.Len(); k++ {
			if b.IDs[k] <= lastID {
				t.Fatalf("segment %d row %d: ID %d after %d breaks ID order", b.Segment, k, b.IDs[k], lastID)
			}
			lastID = b.IDs[k]
			vals := make([]any, len(props))
			for c := range props {
				col := &b.Cols[c]
				switch {
				case col.Present[k]:
					vals[c] = col.Value(k)
				case col.Other[k]:
					others++
					full, err := b.Row(k)
					if err != nil {
						t.Fatal(err)
					}
					if v, has := full.GetProperty(props[c]); has {
						vals[c] = v
					}
				}
			}
			fact := segScanFact(b.IDs[k], b.StartIDs[k], b.EndIDs[k], b.ValidFrom[k], b.ValidTo[k], b.TxFrom[k], b.Versions[k], vals)
			if full, err := b.Row(k); err != nil || (full.Temporal() != nil) != b.HasTemporal[k] {
				t.Fatalf("row %d: HasTemporal %t disagrees with the row (%v)", k, b.HasTemporal[k], err)
			}
			if got[fact] {
				t.Fatalf("row handed out twice: %s", fact)
			}
			got[fact] = true
			full, err := b.Row(k)
			if err != nil {
				t.Fatal(err)
			}
			if fp := strings.Replace(segFP(full), " f=true", " f=false", 1); fp != strings.Replace(wantRows[b.IDs[k]], " f=true", " f=false", 1) {
				t.Fatalf("Row(%d) of segment %d differs from the row door:\n%s\n%s", k, b.Segment, fp, wantRows[b.IDs[k]])
			}
		}
		return true
	})
	if err != nil || !ok {
		t.Fatalf("ScanRelSegments = %t, %v", ok, err)
	}
	if len(got) != len(want) {
		t.Fatalf("scan handed out %d rows, the row door has %d", len(got), len(want))
	}
	for f := range want {
		if !got[f] {
			t.Fatalf("row missing from the scan or different: %s", f)
		}
	}
	if noTemporal == 0 {
		t.Fatal("no row without temporal metadata was written")
	}
	if len(segments) < 3 || others == 0 {
		t.Fatalf("the scan must span the sealed segments and exercise Other rows: %d segments, %d other values", len(segments), others)
	}
	calls := 0
	if ok, err := tw.declared.ScanRelSegments(segTestHOP, props, func(*storecontract.RelSegmentBatch) bool { calls++; return false }); !ok || err != nil || calls != 1 {
		t.Fatalf("early stop: ok %t err %v calls %d", ok, err, calls)
	}
	for name, try := range map[string]func() (bool, error){
		"undeclared type":   func() (bool, error) { return tw.declared.ScanRelSegments(segTestOther, props, nil) },
		"undeclared column": func() (bool, error) { return tw.declared.ScanRelSegments(segTestHOP, []string{"extra"}, nil) },
		"plain store":       func() (bool, error) { return tw.plain.ScanRelSegments(segTestHOP, props, nil) },
	} {
		if ok, err := try(); ok || err != nil {
			t.Fatalf("%s: ScanRelSegments = %t, %v; want false, nil", name, ok, err)
		}
	}
}

// segScanOrdered collects ScanRelSegments' rows as facts in the order the
// door hands them out; a value the column marks Other is read from Row(k),
// and Row(k) must be the batch's row k.
func segScanOrdered(t *testing.T, s *Store, props []string) []string {
	t.Helper()
	var out []string
	ok, err := s.ScanRelSegments(segTestHOP, props, func(b *storecontract.RelSegmentBatch) bool {
		for k := 0; k < b.Len(); k++ {
			full, err := b.Row(k)
			if err != nil {
				t.Fatal(err)
			}
			if full.ID() != b.IDs[k] {
				t.Fatalf("batch of segment %d: Row(%d) is %d, IDs[%d] is %d", b.Segment, k, full.ID(), k, b.IDs[k])
			}
			vals := make([]any, len(props))
			for c := range props {
				col := &b.Cols[c]
				switch {
				case col.Present[k]:
					vals[c] = col.Value(k)
				case col.Other[k]:
					if v, has := full.GetProperty(props[c]); has {
						vals[c] = v
					}
				}
			}
			out = append(out, segScanFact(b.IDs[k], b.StartIDs[k], b.EndIDs[k], b.ValidFrom[k], b.ValidTo[k], b.TxFrom[k], b.Versions[k], vals))
		}
		return true
	})
	if err != nil || !ok {
		t.Fatalf("ScanRelSegments = %t, %v", ok, err)
	}
	return out
}

// ScanRelSegments hands rows out in the row doors' order — ascending ID,
// the order of RelationshipsByType and ForEachRelByType — on every call:
// over segments whose ID ranges interleave (a row written late with an
// early ID), a disjoint segment, dead sealed rows, faulted-in rows (updated
// after a seal, so their early IDs sit in the memtable) and the unsealed
// tail. A consumer that builds output in scan order (sigma's edge @source)
// is then deterministic and equal to the row feed.
func TestSegments_ScanRelSegmentsIsInIDOrder(t *testing.T) {
	r := rand.New(rand.NewSource(83))
	tw := newSegTwin(t, 1<<40, 12)
	base := tw.nextID + 10
	putAt := func(id int64) {
		tw.nextID = id - 1
		tw.put(r, segTestHOP)
	}
	seal := func() {
		if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
			t.Fatal(err)
		}
	}
	for k := int64(0); k < 900; k++ { // segment 1: even IDs
		putAt(base + 2*k)
	}
	seal()
	for k := int64(0); k < 900; k++ { // segment 2: odd IDs in the same range
		putAt(base + 2*k + 1)
	}
	seal()
	for k := int64(0); k < 700; k++ { // segment 3: above both
		putAt(base + 5000 + k)
	}
	seal()
	for i := 0; i < 150; i++ { // updates and deletes: faulted-in and dead rows
		tw.mutate(r)
	}
	for k := int64(0); k < 300; k++ { // the unsealed tail, interleaved with segment 3
		putAt(base + 5000 + 700 + 3*k)
		if k%3 == 0 {
			putAt(base + 3000 + k) // between segments 2 and 3
		}
	}
	st, err := tw.declared.RelSegmentStats(segTestHOP)
	if err != nil || st.Segments != 3 || st.UnsealedRows < 300 || st.LiveSealedRows == st.SealedRows {
		t.Fatalf("setup: %+v, %v", st, err)
	}
	props := []string{"weight", "actor", "n"}
	rows, err := tw.plain.RelationshipsByType(segTestHOP, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	want := make([]string, len(rows))
	for i, row := range rows {
		want[i] = segRowFact(row, props)
	}
	for call := 0; call < 20; call++ {
		got := segScanOrdered(t, tw.declared, props)
		if len(got) != len(want) {
			t.Fatalf("call %d: %d rows, the row door has %d", call, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("call %d: row %d differs from the row door's row %d (order or value):\n scan: %s\n rows: %s", call, i, i, got[i], want[i])
			}
		}
	}
	// Early stop still stops after the first batch.
	calls := 0
	if ok, err := tw.declared.ScanRelSegments(segTestHOP, props, func(*storecontract.RelSegmentBatch) bool { calls++; return false }); !ok || err != nil || calls != 1 {
		t.Fatalf("early stop: ok %t err %v calls %d", ok, err, calls)
	}
}

// The ID-order scan's memory bound: it holds the decoded rows of the
// segments whose ID ranges overlap the current ID, never the whole type.
// Ten ID-disjoint segments hold one segment at a time; two interleaved
// segments are held together.
func TestSegments_ScanRelSegmentsHoldsOverlappingSegmentsOnly(t *testing.T) {
	r := rand.New(rand.NewSource(89))
	tw := newSegTwin(t, 1<<40, 8)
	seal := func() {
		if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
			t.Fatal(err)
		}
	}
	peak := -1
	segScanDoneForTest = func(p int) { peak = p }
	t.Cleanup(func() { segScanDoneForTest = nil })
	scan := func() int {
		peak = -1
		if ok, err := tw.declared.ScanRelSegments(segTestHOP, []string{"n"}, func(*storecontract.RelSegmentBatch) bool { return true }); !ok || err != nil {
			t.Fatalf("ScanRelSegments = %t, %v", ok, err)
		}
		return peak
	}
	for s := 0; s < 10; s++ {
		for i := 0; i < 500; i++ {
			tw.put(r, segTestHOP)
		}
		seal()
	}
	if got := scan(); got != 500 {
		t.Fatalf("ten ID-disjoint segments of 500 rows: peak %d decoded rows held, want 500", got)
	}
	base := tw.nextID + 10
	for _, odd := range []int64{0, 1} { // two segments interleaved by ID
		for k := int64(0); k < 400; k++ {
			tw.nextID = base + 2*k + odd - 1
			tw.put(r, segTestHOP)
		}
		seal()
	}
	if got := scan(); got != 800 {
		t.Fatalf("two interleaved segments of 400 rows: peak %d decoded rows held, want 800", got)
	}
}

// ScanRelColumns over a declared type is served from the segment columns:
// in ID order and equal to the row path (the twin), with no Relationship
// built for a sealed row whose requested values sit in their columns.
func TestSegments_ScanRelColumnsIsNativeAndEqual(t *testing.T) {
	r := rand.New(rand.NewSource(71))
	tw := newSegTwin(t, 1<<40, 10)
	for i := 0; i < 3000; i++ {
		rel := tw.newRel(r, segTestHOP)
		ps, err := types.NewPropertySlice(map[string]any{"actor": fmt.Sprintf("a%d", r.Intn(9)), "weight": float64(r.Intn(90)), "n": int64(r.Intn(500))})
		if err != nil {
			t.Fatal(err)
		}
		if err := rel.SetProperties(ps); err != nil {
			t.Fatal(err)
		}
		segHashed(rel)
		tw.both("PutRelationship", func(s *Store) error { return s.PutRelationship(rel) })
		tw.rels = append(tw.rels, rel.ID())
		if i == 1500 {
			if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.declared.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ { // an unsealed tail and faulted-in rows
		if r.Intn(2) == 0 {
			tw.mutate(r)
		} else {
			tw.put(r, segTestHOP)
		}
	}
	props := []string{"weight", "n", "actor"}
	for _, opts := range []QueryOpts{{}, {Limit: 50}, {After: types.EntityID(tw.rels[700])}, {After: types.EntityID(tw.rels[700]), Limit: 9}} {
		render := func(s *Store) string {
			var b strings.Builder
			err := s.ScanRelColumns(segTestHOP, props, opts, func(cb *storecontract.RelColumnBatch) bool {
				fmt.Fprintf(&b, "%v %v %v %v %v %v\n", cb.IDs, cb.StartIDs, cb.EndIDs, cb.ValidFrom, cb.ValidTo, cb.ColumnData)
				return true
			})
			return fmt.Sprint(err, "\n", b.String())
		}
		want := render(tw.plain)
		tw.declared.sealedRowBuilds.Store(0)
		got := render(tw.declared)
		if got != want {
			t.Fatalf("opts %+v: ScanRelColumns differs from the row path:\n%.400s\n%.400s", opts, segLineDiff(got, want), segLineDiff(want, got))
		}
		if n := tw.declared.sealedRowBuilds.Load(); n != 0 {
			t.Fatalf("opts %+v: ScanRelColumns built %d relationships from sealed rows: not served from the columns", opts, n)
		}
	}
}
