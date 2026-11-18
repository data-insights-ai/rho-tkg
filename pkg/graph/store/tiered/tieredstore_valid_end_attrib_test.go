package tiered

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R2-2 VEATTRIB over a tiered store with LabelIndexOnDisk (the badger arm's
// tests are in badgerstore_valid_end_attrib_test.go): every shard runs the
// valid-end first call's attribution pass on its own rows.

// vaTiered opens a tiered store with the label index on disk whose reference
// shard holds the labels named in ref, and registers names (tokens in order).
func vaTiered(t *testing.T, ref []string, names ...string) (*Store, []uint16) {
	t.Helper()
	ts, err := New(Config{DataDir: t.TempDir(), RefLabels: ref, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1, LabelIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	toks := make([]uint16, len(names))
	for i, name := range names {
		if toks[i], err = reg.GetOrCreate(name); err != nil {
			t.Fatal(err)
		}
	}
	return ts, toks
}

// vaShards is the reference shard and the hot shard.
func vaShards(t *testing.T, ts *Store) []*BadgerStore {
	t.Helper()
	ts.mu.RLock()
	hot := ts.hotShard.Store()
	ts.mu.RUnlock()
	if ts.RefShardForTest() == nil || hot == nil {
		t.Fatal("reference or hot shard not open")
	}
	return []*BadgerStore{ts.RefShardForTest(), hot}
}

func vaCall(s storecontract.NodeValidEndMeteredCapability, label uint16, budget storecontract.ReadBudget) (*storecontract.ReadMeter, []int64, error) {
	_, m := storecontract.WithReadBudget(context.Background(), budget)
	cs, err := s.NodeValidEndCandidatesMetered([]uint16{label}, types.Instant(1<<62), 0, m.Tally("probe"))
	var ids []int64
	for _, c := range cs {
		ids = append(ids, c.ID)
	}
	slices.Sort(ids)
	return m, ids, err
}

// vaOthers writes labels X1 (reference), X2 (hot), X3 (reference) of each
// nodes and label B (hot) of 3, and returns B's nodes and the tokens.
func vaOthers(t *testing.T, each int, flushed bool) (*Store, []uint16, []int64) {
	t.Helper()
	ts, toks := vaTiered(t, []string{"X1", "X3"}, "X1", "X2", "X3", "B")
	gen := tieredNodeGen(t)
	put := func(tok uint16) int64 {
		t.Helper()
		id := types.NodeID(gen.Generate())
		if err := ts.PutNode(types.NewNode(id, tok, nil)); err != nil {
			t.Fatal(err)
		}
		return int64(id)
	}
	for _, tok := range toks[:3] {
		for i := 0; i < each; i++ {
			put(tok)
		}
	}
	var b []int64
	for i := 0; i < 3; i++ {
		b = append(b, put(toks[3]))
	}
	slices.Sort(b)
	if flushed {
		if err := ts.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	return ts, toks, b
}

// TestValidEndFirstCall_CostIgnoresOtherLabels_Tiered (R2-2 V1): label B's
// first call (3 nodes on the hot shard) costs the same after 1, 2 and 3 other
// tracked labels of 200 (on the reference and the hot shard): its listing (3)
// and one candidate per entity read (3); index entries visited: the hot
// shard's committed listing (3 flushed); rows decoded over both shards: 3
// flushed. Before, each tracked label added a probe per entity on the hot
// shard. Not parallel: the visit counter is the process's.
func TestValidEndFirstCall_CostIgnoresOtherLabels_Tiered(t *testing.T) {
	testtier.Slow(t)
	const each = 200
	for _, flushed := range []bool{true, false} {
		t.Run(map[bool]string{true: "flushed", false: "pending"}[flushed], func(t *testing.T) {
			for k := 1; k <= 3; k++ {
				ts, toks, want := vaOthers(t, each, flushed)
				for _, tok := range toks[:k] {
					if _, got, err := vaCall(ts, tok, storecontract.ReadBudget{}); err != nil || len(got) != each {
						t.Fatalf("label %d = %d candidates, %v; want %d", tok, len(got), err, each)
					}
				}
				shards := vaShards(t, ts)
				decodes := func() (n int64) {
					for _, s := range shards {
						n += s.ValidEndRowDecodesForTest()
					}
					return n
				}
				before := decodes()
				stepsBefore := int64(0)
				for _, s := range shards {
					stepsBefore += s.ValidEndIteratorStepsForTest()
				}
				visited, stop := badger.CountIndexEntriesVisitedForTest()
				m, got, err := vaCall(ts, toks[3], storecontract.ReadBudget{})
				v, d := visited(), decodes()-before
				stop()
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("after %d labels: B = %v, %v; want %v", k, got, err, want)
				}
				var wantV, wantD int64
				if flushed {
					wantV, wantD = 3, 3
				}
				steps := -stepsBefore
				for _, s := range shards {
					steps += s.ValidEndIteratorStepsForTest()
				}
				wantCandidates := uint64(3 + 4*3 + steps)
				if c := m.Work().Candidates; c != wantCandidates || v != wantV || d != wantD {
					t.Errorf("after %d other labels: %d candidates, %d index entries visited, %d rows decoded; want %d, %d, %d, whatever the other labels", k, c, v, d, wantCandidates, wantV, wantD)
				}
			}
		})
	}
}

// TestValidEndFirstCall_RefusedInstallsNothing_Tiered (R2-2 V2): refusal during
// the first enlistment leaves the token untracked. The first call returns the
// typed error, and no shard's index holds more entities or unknown ones than
// before; the unlimited call after it answers and counts as the control.
func TestValidEndFirstCall_RefusedInstallsNothing_Tiered(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, flushed := range []bool{true, false} {
		t.Run(map[bool]string{true: "flushed", false: "pending"}[flushed], func(t *testing.T) {
			t.Parallel()
			open := func() (*Store, uint16, []int64) {
				ts, toks, b := vaOthers(t, 20, flushed)
				for _, tok := range toks[:3] {
					if _, _, err := vaCall(ts, tok, storecontract.ReadBudget{}); err != nil {
						t.Fatal(err)
					}
				}
				return ts, toks[3], b
			}
			ts, tok, want := open()
			control, got, err := vaCall(ts, tok, storecontract.ReadBudget{})
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("control = %v, %v; want %v", got, err, want)
			}
			count := control.Work().Candidates
			ts, tok, want = open()
			stats := func() (entities, unknown int) {
				for _, s := range vaShards(t, ts) {
					_, n := s.ValidEndStats()
					entities, unknown = entities+n.Entities, unknown+n.Unknown
				}
				return entities, unknown
			}
			e0, u0 := stats()
			if _, got, err := vaCall(ts, tok, storecontract.ReadBudget{Candidates: 5}); !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 {
				t.Fatalf("under %d candidates = %v, %v; want ErrReadBudget and no candidates", count-1, got, err)
			}
			if e, u := stats(); e != e0 || u != u0 {
				t.Fatalf("the refused call left %d entities, %d unknown; before it %d, %d", e, u, e0, u0)
			}
			again, got, err := vaCall(ts, tok, storecontract.ReadBudget{})
			if err != nil || !slices.Equal(got, want) || again.Work().Candidates != count {
				t.Fatalf("after the refusal = %v, %v, %d candidates; want %v, %d", got, err, again.Work().Candidates, want, count)
			}
		})
	}
}

// vaStore is what the attribution matrix writes and reads.
type vaStore interface {
	storecontract.NodeValidEndMeteredCapability
	PutNode(n *types.Node) error
	GetNode(id types.NodeID) (*types.Node, error)
	GetNodeHistory(id types.NodeID) ([]*types.Node, error)
	AddNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updated *types.Node, prevVersion uint32, prev *types.Node) error
	RemoveNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updated *types.Node, prevVersion uint32, prev *types.Node) error
	DeleteNodeWithHistory(id types.NodeID, prevNodeVersion uint32, tomb *types.Node, rels []storecontract.RelTombstone) error
}

func vaNode(id types.NodeID, ver uint32, tx, deleted types.Instant, labels ...uint16) *types.Node {
	n := types.NewNode(id, labels[0], labels[1:])
	n.SetVersion(ver)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: tx, DeletedAt: deleted, TxTo: deleted})
	return n
}

// vaScenario is the badger arm's matrix (vaScenario there) over labels a, b, c.
func vaScenario(t *testing.T, s vaStore, ids [9]types.NodeID, a, b, c uint16, flush func()) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	v0 := func(i int, labels ...uint16) *types.Node { return vaNode(ids[i], 0, 10, 0, labels...) }
	must(s.PutNode(v0(1, a, b)))
	must(s.PutNode(v0(2, a, b)))
	must(s.RemoveNodeLabelTokenWithHistory(ids[2], a, vaNode(ids[2], 1, 20, 0, b), 0, v0(2, a, b)))
	must(s.PutNode(v0(3, a, b)))
	must(s.DeleteNodeWithHistory(ids[3], 0, vaNode(ids[3], 0, 10, 30, a, b), nil))
	must(s.PutNode(v0(4, b)))
	must(s.PutNode(v0(5, a, b)))
	must(s.PutNode(v0(6, c)))
	must(s.PutNode(v0(8, b, c)))
	must(s.RemoveNodeLabelTokenWithHistory(ids[8], b, vaNode(ids[8], 1, 20, 0, c), 0, v0(8, b, c)))
	flush()
	must(s.AddNodeLabelTokenWithHistory(ids[4], a, vaNode(ids[4], 1, 40, 0, b, a), 0, v0(4, b)))
	must(s.DeleteNodeWithHistory(ids[5], 0, vaNode(ids[5], 0, 10, 40, a, b), nil))
	must(s.PutNode(vaNode(ids[7], 0, 40, 0, a)))
}

func vaPlainScan(t *testing.T, s vaStore, ids [9]types.NodeID, tok uint16) []int64 {
	t.Helper()
	var out []int64
	for _, id := range ids[1:] {
		rows, err := s.GetNodeHistory(id)
		if err != nil {
			t.Fatalf("history of %d: %v", id, err)
		}
		if n, err := s.GetNode(id); err == nil {
			rows = append(rows, n)
		}
		if slices.ContainsFunc(rows, func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }) {
			out = append(out, int64(id))
		}
	}
	slices.Sort(out)
	return out
}

func vaAttribution(t *testing.T, s vaStore, ids [9]types.NodeID, a, b uint16) (gotA, gotB []int64) {
	t.Helper()
	call := func(tok uint16) []int64 {
		t.Helper()
		_, got, err := vaCall(s, tok, storecontract.ReadBudget{})
		if err != nil {
			t.Fatalf("label %d: %v", tok, err)
		}
		if scan := vaPlainScan(t, s, ids, tok); !slices.Equal(got, scan) {
			t.Fatalf("label %d's candidates %v; the plain scan of its rows %v", tok, got, scan)
		}
		return got
	}
	gotA = call(a)
	gotB = call(b)
	if again := call(a); !slices.Equal(again, gotA) {
		t.Fatalf("label A again %v, first %v", again, gotA)
	}
	return gotA, gotB
}

// TestValidEndAttribution_Matrix_Tiered (R2-2 V3): the badger arm's matrix on
// a tiered store with the label index on disk, its labels on the reference
// shard or on the hot shard, flushed and with the last writes buffered: label
// A's and B's candidates equal the plain scan of every row and the memory
// store's on the same writes.
func TestValidEndAttribution_Matrix_Tiered(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, shard := range []string{"reference", "hot"} {
		for _, flushed := range []bool{true, false} {
			t.Run(shard+"/"+map[bool]string{true: "flushed", false: "pending"}[flushed], func(t *testing.T) {
				t.Parallel()
				var ref []string
				if shard == "reference" {
					ref = []string{"A", "B", "C"}
				}
				ts, toks := vaTiered(t, ref, "A", "B", "C")
				a, b, c := toks[0], toks[1], toks[2]
				var ids [9]types.NodeID
				gen := tieredNodeGen(t)
				for i := range ids {
					ids[i] = types.NodeID(gen.Generate())
				}
				ms := memory.New()
				vaScenario(t, ms, ids, a, b, c, func() {})
				ctlA, ctlB := vaAttribution(t, ms, ids, a, b)
				if len(ctlA) != 6 || len(ctlB) != 6 {
					t.Fatalf("memory control: A %v, B %v; want 6 each", ctlA, ctlB)
				}
				vaScenario(t, ts, ids, a, b, c, func() {
					if err := ts.Flush(); err != nil {
						t.Fatal(err)
					}
				})
				if flushed {
					if err := ts.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				gotA, gotB := vaAttribution(t, ts, ids, a, b)
				if !slices.Equal(gotA, ctlA) || !slices.Equal(gotB, ctlB) {
					t.Fatalf("A %v, B %v; the memory store's %v, %v", gotA, gotB, ctlA, ctlB)
				}
			})
		}
	}
}

// TestValidEndFirstCall_LabelAcrossShardsExceedsTogether admits listing,
// enlistment, pending snapshot, row read, and result copy over both shards.
// Each shard fits alone; together they refuse during the second enlistment
// without decoding or installing that shard. Native iterator steps also count.
func TestValidEndFirstCall_LabelAcrossShardsExceedsTogether(t *testing.T) {
	testtier.Slow(t)
	open := func() (*Store, uint16, []int64) {
		ts, toks := vaTiered(t, []string{"X1"}, "X1", "B")
		x1, b := toks[0], toks[1]
		gen := tieredNodeGen(t)
		var ids []int64
		for i := 0; i < 6; i++ {
			id := types.NodeID(gen.Generate())
			n := types.NewNode(id, b, nil)
			if i < 3 {
				n = types.NewNode(id, x1, []uint16{b})
			}
			if err := ts.PutNode(n); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, int64(id))
		}
		if err := ts.Flush(); err != nil {
			t.Fatal(err)
		}
		slices.Sort(ids)
		return ts, b, ids
	}
	ts, b, want := open()
	shards := vaShards(t, ts)
	for i, s := range shards {
		if got, err := s.NodeIDsByLabel(b); err != nil || len(got) != 3 {
			t.Fatalf("shard %d holds %d nodes of B, %v; want 3", i, len(got), err)
		}
	}
	visited, stop := badger.CountIndexEntriesVisitedForTest()
	m, got, err := vaCall(ts, b, storecontract.ReadBudget{})
	v := visited()
	stop()
	decodes := shards[0].ValidEndRowDecodesForTest() + shards[1].ValidEndRowDecodesForTest()
	steps := shards[0].ValidEndIteratorStepsForTest() + shards[1].ValidEndIteratorStepsForTest()
	wantCandidates := uint64(6 + 4*6 + steps)
	if err != nil || !slices.Equal(got, want) || m.Work().Candidates != wantCandidates || v != 6 || decodes != 6 {
		t.Fatalf("control = %v, %v, %d candidates, %d visited, %d decoded; want %v, %d, 6, 6", got, err, m.Work().Candidates, v, decodes, want, wantCandidates)
	}
	ts, b, want = open()
	if _, got, err := vaCall(ts, b, storecontract.ReadBudget{Candidates: wantCandidates}); err != nil || !slices.Equal(got, want) {
		t.Fatalf("at exact count = %v, %v; want %v", got, err, want)
	}
	ts, b, _ = open()
	hot := vaShards(t, ts)[1]
	_, before := hot.ValidEndStats()
	if _, got, err := vaCall(ts, b, storecontract.ReadBudget{Candidates: wantCandidates/2 + 5}); !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 {
		t.Fatalf("during second shard enlistment = %v, %v; want ErrReadBudget and no candidates", got, err)
	}
	if _, after := hot.ValidEndStats(); hot.ValidEndRowDecodesForTest() != 0 || after.Entities != before.Entities || after.Unknown != before.Unknown {
		t.Fatalf("the hot shard decoded %d rows and holds %+v after the refusal; want none and %+v", hot.ValidEndRowDecodesForTest(), after, before)
	}
}

// TestValidEndFirstCall_FormerMembersAcrossShards also charges rejected former
// entries across shards. The native lookup counter must stop on the second
// shard's second rejected member, before that shard installs candidates.
func TestValidEndFirstCall_FormerMembersAcrossShards(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	open := func() (*Store, uint16, []int64) {
		ts, toks := vaTiered(t, []string{"X1"}, "A", "X1", "B")
		a, x1, b := toks[0], toks[1], toks[2]
		gen := tieredNodeGen(t)
		var want []int64
		put := func(deleted bool, labels ...uint16) {
			t.Helper()
			id := types.NodeID(gen.Generate())
			if err := ts.PutNode(vaNode(id, 0, 10, 0, labels...)); err != nil {
				t.Fatal(err)
			}
			if deleted {
				if err := ts.DeleteNodeWithHistory(id, 0, vaNode(id, 0, 10, 30, labels...), nil); err != nil {
					t.Fatal(err)
				}
			}
			if slices.Contains(labels, b) {
				want = append(want, int64(id))
			}
		}
		for _, ref := range []bool{true, false} {
			with := func(labels ...uint16) []uint16 {
				if ref {
					return append([]uint16{x1}, labels...)
				}
				return labels
			}
			for i := 0; i < 3; i++ {
				put(false, with(b)...)
			}
			put(true, with(b)...)
			put(true, with(a)...)
			put(true, with(a)...)
		}
		if err := ts.Flush(); err != nil {
			t.Fatal(err)
		}
		if _, got, err := vaCall(ts, a, storecontract.ReadBudget{}); err != nil || len(got) != 4 {
			t.Fatalf("A = %v, %v; want its 4 former members", got, err)
		}
		slices.Sort(want)
		return ts, b, want
	}
	lookups := func(shards []*BadgerStore) (n []int64) {
		for _, s := range shards {
			n = append(n, s.ValidEndAttribLookupsForTest())
		}
		return n
	}
	ts, b, want := open()
	shards := vaShards(t, ts)
	l0 := lookups(shards)
	stepsBefore := shards[0].ValidEndIteratorStepsForTest() + shards[1].ValidEndIteratorStepsForTest()
	m, got, err := vaCall(ts, b, storecontract.ReadBudget{})
	l1 := lookups(shards)
	steps := shards[0].ValidEndIteratorStepsForTest() + shards[1].ValidEndIteratorStepsForTest() - stepsBefore
	wantCandidates := uint64(6 + 4*8 + 4 + steps)
	if err != nil || !slices.Equal(got, want) || m.Work().Candidates != wantCandidates {
		t.Fatalf("control = %v, %v, %d candidates; want %v, %d including native copies and forward steps", got, err, m.Work().Candidates, want, wantCandidates)
	}
	for i := range shards {
		if d := l1[i] - l0[i]; d != 3 {
			t.Fatalf("shard %d made %d lookups; want 3 (A's 2, B's 1)", i, d)
		}
	}
	ts, b, _ = open()
	hot := vaShards(t, ts)[1]
	_, before := hot.ValidEndStats()
	h0 := hot.ValidEndAttribLookupsForTest()
	if _, got, err := vaCall(ts, b, storecontract.ReadBudget{Candidates: wantCandidates - 4 - 1}); !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 {
		t.Fatalf("second shard attribution = %v, %v; want ErrReadBudget and no candidates", got, err)
	}
	if _, after := hot.ValidEndStats(); hot.ValidEndAttribLookupsForTest()-h0 != 2 || after.Entities != before.Entities {
		t.Fatalf("the hot shard made %d lookups and holds %+v after the refusal; want 2 and %+v", hot.ValidEndAttribLookupsForTest()-h0, after, before)
	}
}

func TestValidEndMerge_AdmissionBeforeCopy(t *testing.T) {
	ts, toks := vaTiered(t, []string{"Ref"}, "Ref", "B")
	gen := tieredNodeGen(t)
	var want []int64
	for i := range 6 {
		id := types.NodeID(gen.Generate())
		n := types.NewNode(id, toks[1], nil)
		if i < 3 {
			n = types.NewNode(id, toks[0], []uint16{toks[1]})
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
		want = append(want, int64(id))
	}
	slices.Sort(want)
	if _, _, err := vaCall(ts, toks[1], storecontract.ReadBudget{}); err != nil {
		t.Fatal(err)
	}
	copied, stop := CountValidEndMergeCopiesForTest()
	defer stop()
	control, got, err := vaCall(ts, toks[1], storecontract.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || copied() != 6 {
		t.Fatalf("control IDs %v, copies %d, err %v; want %v and 6", got, copied(), err, want)
	}
	before := copied()
	_, got, err = vaCall(ts, toks[1], storecontract.ReadBudget{BufferBytes: control.Work().BufferBytes})
	if err != nil || !slices.Equal(got, want) || copied()-before != 6 {
		t.Fatalf("exact IDs %v, copies %d, err %v", got, copied()-before, err)
	}
	before = copied()
	_, got, err = vaCall(ts, toks[1], storecontract.ReadBudget{BufferBytes: 6 * 56})
	var rb *storecontract.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "buffer bytes" || len(got) != 0 || copied() != before {
		t.Fatalf("limited IDs %v, copies %d, err %v; want refusal before tiered union copy", got, copied()-before, err)
	}
}
