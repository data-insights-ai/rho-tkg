package badger

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestValidEndFirstCall_WalksNoOtherLabel (R2-2 VEATTRIB): with
// LabelIndexOnDisk the valid-end index's first call for a label tells each
// entity it reads which tracked labels it carries. It did so by listing every
// tracked label's keyspace again, uncounted (a label of 3 nodes read after a
// label of 200 walked the 200 keys), then by probing each entity's key of each
// tracked label. It now reads the labels off the current row the pass decodes
// anyway and a former member from the RAM ever-member sidecar: no probe, no
// listing. The native counter is the label index entries the store visits (an
// iterator step or a probe), which the meter does not keep: label 2's first
// call visits its own committed listing only (3, none while the writes are
// buffered), whatever label 1's size; it counts its listing (3) and one
// candidate per entity read (3). Buffered and flushed writes.
// Not parallel: the visit counter is the process's.
func TestValidEndFirstCall_WalksNoOtherLabel(t *testing.T) {
	const b = 3
	for _, a := range []int64{20, 200} {
		for _, flushed := range []bool{true, false} {
			name := map[bool]string{true: "flushed", false: "pending"}[flushed]
			t.Run(fmt.Sprintf("%d/%s", a, name), func(t *testing.T) {
				bs, err := New(Config{Dir: t.TempDir(), LabelIndexOnDisk: true, FlushInterval: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = bs.Close() }()
				for i := int64(1); i <= a; i++ {
					if err := bs.PutNode(veNode(i, 0, 10, 0, 0)); err != nil { // label 1
						t.Fatal(err)
					}
				}
				for i := a + 1; i <= a+b; i++ {
					if err := bs.PutNode(types.NewNode(types.NodeID(i), 2, nil)); err != nil {
						t.Fatal(err)
					}
				}
				if flushed {
					if err := bs.Flush(); err != nil {
						t.Fatal(err)
					}
				}
				if _, got, err := vaCall(bs, 1, storecontract.ReadBudget{}); err != nil || int64(len(got)) != a {
					t.Fatalf("label 1 = %d candidates, %v; want %d", len(got), err, a)
				}

				visited, stop := CountIndexEntriesVisitedForTest()
				defer stop()
				m, got, err := vaCall(bs, 2, storecontract.ReadBudget{})
				want := []int64{a + 1, a + 2, a + 3}
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("label 2 = %v, %v; want %v", got, err, want)
				}
				// The listing walks label 2's committed keys (none while they are
				// buffered: it reads the write buffer); the attribution visits none.
				listing := int64(0)
				if flushed {
					listing = b
				}
				if v := visited(); v != listing {
					t.Fatalf("label 2's first call visited %d label index entries after a label of %d; want %d (its listing, no probe)", v, a, listing)
				}
				if c, want := m.Work().Candidates, uint64(b+b); c != want {
					t.Fatalf("label 2's first call counted %d candidates; want %d (%d listed, %d entities read)", c, want, b, b)
				}
			})
		}
	}
}

// vaCall is one valid-end first call of label under budget (pin 1,000, valid
// from 0: every held entity is listed), the candidate IDs sorted.
func vaCall(s storecontract.NodeValidEndMeteredCapability, label uint16, budget storecontract.ReadBudget) (*storecontract.ReadMeter, []int64, error) {
	_, m := storecontract.WithReadBudget(context.Background(), budget)
	cs, err := s.NodeValidEndCandidatesMetered([]uint16{label}, 1_000, 0, m.Tally("probe"))
	var ids []int64
	for _, c := range cs {
		ids = append(ids, c.ID)
	}
	slices.Sort(ids)
	return m, ids, err
}

// vaMode is one badger label mode of the R2-2 tests: the label index in RAM
// (built at the open), in RAM built lazily per label (a transient read-only
// open over a trusted image: written, closed, reopened), or on disk.
type vaMode struct {
	name       string
	disk, lazy bool
}

var vaModes = []vaMode{{name: "ram"}, {name: "lazy", lazy: true}, {name: "disk", disk: true}}

// vaOpen opens a store of mode m, runs write on it and, flushed, flushes it
// (lazy: always flushed, then closed and reopened read-only and transient).
func vaOpen(t *testing.T, m vaMode, flushed bool, write func(bs *Store)) *Store {
	t.Helper()
	dir := t.TempDir()
	cfg := Config{Dir: dir, LabelIndexOnDisk: m.disk, FlushInterval: time.Hour}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	write(bs)
	if m.lazy {
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
		if bs, err = New(Config{Dir: dir, ReadOnly: true, TransientRead: true}); err != nil {
			t.Fatal(err)
		}
		if !bs.labelLazy {
			t.Fatal("the transient read-only open builds its label maps at the open; the lazy mode is not under test")
		}
	} else if flushed {
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

// vaOtherLabels writes k labels (1..k) of each nodes (IDs from 1) and label 9
// of 3 nodes (1001..1003).
func vaOtherLabels(t *testing.T, bs *Store, k, each int) {
	t.Helper()
	for l := 1; l <= k; l++ {
		for i := 1; i <= each; i++ {
			if err := bs.PutNode(types.NewNode(types.NodeID((l-1)*each+i), uint16(l), nil)); err != nil { // #nosec G115 -- a small label
				t.Fatal(err)
			}
		}
	}
	for i := int64(1001); i <= 1003; i++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(i), 9, nil)); err != nil {
			t.Fatal(err)
		}
	}
}

// vaFigures is what one first call of label 9 costs: the meter's candidates,
// and the native counts the meter does not keep (label index entries visited:
// iterator steps and probes; rows the bulk read decoded).
type vaFigures struct {
	candidates       uint64
	visited, decodes int64
}

// TestValidEndFirstCall_CostIgnoresOtherLabels (R2-2 V1): the first call of
// label 9 (3 nodes) costs the same after 1, 2 and 3 other tracked labels of 200
// nodes each, in every label mode: its own listing (3 candidates, from the
// label index on disk or the lazy walk; none from a map built at the open) and
// one candidate per entity it reads (3); index entries visited: its own
// committed listing only (3 on disk when flushed); rows decoded: one current
// row per entity (3 when flushed; a buffered entity is read from the cache).
// Before, with the label index on disk, each tracked label added a probe per
// entity: +3 candidates and +3 visits per label. The unlimited control answers
// the three nodes. This holds for the listing and the reads; another tracked
// label with former members adds at most its rejected former entries
// (TestValidEndAttribution_WalksFormerSetsNotEntities); the labels here have
// none. Not parallel: the visit counter is the process's.
func TestValidEndFirstCall_CostIgnoresOtherLabels(t *testing.T) {
	const each = 200
	want := []int64{1001, 1002, 1003}
	for _, m := range vaModes {
		for _, flushed := range []bool{true, false} {
			if m.lazy && !flushed {
				continue // a read-only handle holds no buffered write
			}
			t.Run(m.name+"/"+map[bool]string{true: "flushed", false: "pending"}[flushed], func(t *testing.T) {
				var figs []vaFigures
				for k := 1; k <= 3; k++ {
					bs := vaOpen(t, m, flushed, func(bs *Store) { vaOtherLabels(t, bs, k, each) })
					for l := uint16(1); int(l) <= k; l++ {
						if _, got, err := vaCall(bs, l, storecontract.ReadBudget{}); err != nil || len(got) != each {
							t.Fatalf("label %d = %d candidates, %v; want %d", l, len(got), err, each)
						}
					}
					decodes := bs.ValidEndRowDecodesForTest()
					visited, stop := CountIndexEntriesVisitedForTest()
					meter, got, err := vaCall(bs, 9, storecontract.ReadBudget{})
					f := vaFigures{meter.Work().Candidates, visited(), bs.ValidEndRowDecodesForTest() - decodes}
					stop()
					if err != nil || !slices.Equal(got, want) {
						t.Fatalf("after %d labels: label 9 = %v, %v; want %v", k, got, err, want)
					}
					figs = append(figs, f)
				}
				listing := uint64(3) // the keyspace or the lazy walk
				if !m.disk && !m.lazy {
					listing = 0 // a map the open built lists free
				}
				var visits, decodes int64
				if flushed {
					decodes = 3
					if m.disk {
						visits = 3
					}
				}
				for k, f := range figs {
					if f.candidates != listing+3 || f.visited != visits || f.decodes != decodes {
						t.Errorf("after %d other labels: %d candidates, %d index entries visited, %d rows decoded; want %d (%d listed, 3 entities), %d, %d, whatever the other labels", k+1, f.candidates, f.visited, f.decodes, listing+3, listing, visits, decodes)
					}
				}
			})
		}
	}
}

// TestValidEndFirstCall_RefusedInstallsNothing (R2-2 V2): label 9's first
// call after 3 tracked labels, under a Candidates budget one under the
// unlimited control's count, is refused with the typed error before it tracks
// the label: the index holds the entities and the unknown ones it held before
// the call. The unlimited call after it answers the control's three nodes and
// counts the control's candidates (lazy: less the walk of the label map the
// refused call read and was charged for, which the handle keeps). Before, with
// the label index on disk, the refusal came at a probe, after the label's
// members were enlisted: 3 unknown entities stayed behind.
func TestValidEndFirstCall_RefusedInstallsNothing(t *testing.T) {
	t.Parallel()
	const each = 20
	want := []int64{1001, 1002, 1003}
	for _, m := range vaModes {
		for _, flushed := range []bool{true, false} {
			if m.lazy && !flushed {
				continue
			}
			t.Run(m.name+"/"+map[bool]string{true: "flushed", false: "pending"}[flushed], func(t *testing.T) {
				t.Parallel()
				open := func() *Store {
					bs := vaOpen(t, m, flushed, func(bs *Store) { vaOtherLabels(t, bs, 3, each) })
					for l := uint16(1); l <= 3; l++ {
						if _, _, err := vaCall(bs, l, storecontract.ReadBudget{}); err != nil {
							t.Fatal(err)
						}
					}
					return bs
				}
				control, got, err := vaCall(open(), 9, storecontract.ReadBudget{})
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("control = %v, %v; want %v", got, err, want)
				}
				count := control.Work().Candidates
				if count == 0 {
					t.Fatal("the control counted nothing")
				}
				bs := open()
				_, before := bs.ValidEndStats()
				if _, got, err := vaCall(bs, 9, storecontract.ReadBudget{Candidates: count - 1}); !errors.Is(err, storecontract.ErrReadBudget) || len(got) != 0 {
					t.Fatalf("under %d candidates = %v, %v; want ErrReadBudget and no candidates", count-1, got, err)
				}
				if _, after := bs.ValidEndStats(); after.Entities != before.Entities || after.Unknown != before.Unknown || after.Tokens != before.Tokens {
					t.Fatalf("the refused call left the index at %+v; before it %+v", after, before)
				}
				again, got, err := vaCall(bs, 9, storecontract.ReadBudget{})
				if err != nil || !slices.Equal(got, want) {
					t.Fatalf("after the refusal = %v, %v; want %v", got, err, want)
				}
				wantCount := count
				if m.lazy {
					wantCount -= 3 // the label map the refused call walked stays built
				}
				if c := again.Work().Candidates; c != wantCount {
					t.Fatalf("after the refusal the call counted %d candidates; want %d", c, wantCount)
				}
			})
		}
	}
}

// Labels of the attribution matrix: A and B tracked, C never.
const (
	vaA uint16 = 1
	vaB uint16 = 2
	vaC uint16 = 3
)

// vaStore is what the attribution matrix writes and reads: the badger store,
// the memory store (the RAM control) and, in package tiered, the tiered store.
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

// vaScenario writes the attribution matrix over ids[1..8], flush between its
// committed part and its buffered part:
//
//	1 in A and B; 2 left A, in B; 3 in A and B, deleted; 8 left B, in C;
//	6 in C only; then buffered: 4 (in B) gains A, 5 (in A and B) is deleted,
//	7 is created in A.
//
// A's ever-members are 1, 2, 3, 4, 5, 7; B's 1, 2, 3, 4, 5, 8.
func vaScenario(t *testing.T, s vaStore, ids [9]types.NodeID, flush func()) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	v0 := func(i int, labels ...uint16) *types.Node { return vaNode(ids[i], 0, 10, 0, labels...) }
	must(s.PutNode(v0(1, vaA, vaB)))
	must(s.PutNode(v0(2, vaA, vaB)))
	must(s.RemoveNodeLabelTokenWithHistory(ids[2], vaA, vaNode(ids[2], 1, 20, 0, vaB), 0, v0(2, vaA, vaB)))
	must(s.PutNode(v0(3, vaA, vaB)))
	must(s.DeleteNodeWithHistory(ids[3], 0, vaNode(ids[3], 0, 10, 30, vaA, vaB), nil))
	must(s.PutNode(v0(4, vaB)))
	must(s.PutNode(v0(5, vaA, vaB)))
	must(s.PutNode(v0(6, vaC)))
	must(s.PutNode(v0(8, vaB, vaC)))
	must(s.RemoveNodeLabelTokenWithHistory(ids[8], vaB, vaNode(ids[8], 1, 20, 0, vaC), 0, v0(8, vaB, vaC)))
	flush()
	must(s.AddNodeLabelTokenWithHistory(ids[4], vaA, vaNode(ids[4], 1, 40, 0, vaB, vaA), 0, v0(4, vaB)))
	must(s.DeleteNodeWithHistory(ids[5], 0, vaNode(ids[5], 0, 10, 40, vaA, vaB), nil))
	must(s.PutNode(vaNode(ids[7], 0, 40, 0, vaA)))
}

// vaPlainScan is tok's ever-members among ids by reading every row of each:
// its current row and its history rows.
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

// vaAttribution is the matrix's answer on s: A's candidates at A's first
// call, B's at B's first call after A, A's again; each must equal the plain
// scan of s.
func vaAttribution(t *testing.T, s vaStore, ids [9]types.NodeID) (a, b []int64) {
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
	a = call(vaA)
	b = call(vaB)
	if again := call(vaA); !slices.Equal(again, a) {
		t.Fatalf("label A again %v, first %v", again, a)
	}
	return a, b
}

// TestValidEndAttribution_Matrix (R2-2 V3): which tracked labels an entity
// carries now comes from the current row the pass decodes (the wire scanner's
// label capture, or the cached node) and the RAM ever-member sidecar, not from
// the label index. Over every label mode, flushed and with the last writes
// buffered, label A's and B's candidates equal the plain scan of every row and
// the memory store's (the RAM control) on the same writes: a member of both, a
// node that left A, a deleted node, a node whose label add is buffered, one
// whose delete is buffered, a node created in the buffer, one that left B, one
// in an untracked label only.
func TestValidEndAttribution_Matrix(t *testing.T) {
	t.Parallel()
	var ids [9]types.NodeID
	for i := range ids {
		ids[i] = types.NodeID(i)
	}
	wantA, wantB := []int64{1, 2, 3, 4, 5, 7}, []int64{1, 2, 3, 4, 5, 8}
	ms := memory.New()
	vaScenario(t, ms, ids, func() {})
	ctlA, ctlB := vaAttribution(t, ms, ids)
	if !slices.Equal(ctlA, wantA) || !slices.Equal(ctlB, wantB) {
		t.Fatalf("memory control: A %v, B %v; want %v, %v", ctlA, ctlB, wantA, wantB)
	}
	for _, m := range vaModes {
		for _, flushed := range []bool{true, false} {
			if m.lazy && !flushed {
				continue
			}
			t.Run(m.name+"/"+map[bool]string{true: "flushed", false: "pending"}[flushed], func(t *testing.T) {
				t.Parallel()
				bs := vaOpen(t, m, flushed, func(bs *Store) {
					vaScenario(t, bs, ids, func() {
						if err := bs.Flush(); err != nil {
							t.Fatal(err)
						}
					})
				})
				gotA, gotB := vaAttribution(t, bs, ids)
				if !slices.Equal(gotA, ctlA) || !slices.Equal(gotB, ctlB) {
					t.Fatalf("A %v, B %v; the memory store's %v, %v", gotA, gotB, ctlA, ctlB)
				}
			})
		}
	}
}

// TestValidEndAttribution_FlushInTheGap (R2-2 V4, review finding H1): node 1
// (label 3, committed) gains label 2 in the write buffer; label 2's first call
// enlists it from the buffer and reads its rows, and a full flush lands after
// the rows' read and before the tracked tokens are set (validEndAttribHook).
// With the label index on disk the attribution probed node 1's label key after
// that point, in a view opened before the flush's commit and after the flush
// had cleared the buffer: the key was in neither, node 1 was installed without
// label 2 and label 2's candidates missed it. The tokens now come from the rows
// read before the gap, so nothing is read after it. The control (no flush in
// the gap) answers 1 and 3; the hook must have run once and left the buffer
// empty (the native check that the flush did land there).
func TestValidEndAttribution_FlushInTheGap(t *testing.T) {
	t.Parallel()
	for _, m := range []vaMode{{name: "ram"}, {name: "disk", disk: true}} {
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			open := func() *Store {
				return vaOpen(t, m, false, func(bs *Store) {
					must := func(err error) {
						t.Helper()
						if err != nil {
							t.Fatal(err)
						}
					}
					must(bs.PutNode(vaNode(1, 0, 10, 0, 3)))
					must(bs.PutNode(vaNode(3, 0, 10, 0, 2)))
					must(bs.Flush())
					must(bs.AddNodeLabelTokenWithHistory(1, 2, vaNode(1, 1, 20, 0, 3, 2), 0, vaNode(1, 0, 10, 0, 3)))
				})
			}
			want := []int64{1, 3}
			if _, got, err := vaCall(open(), 2, storecontract.ReadBudget{}); err != nil || !slices.Equal(got, want) {
				t.Fatalf("control = %v, %v; want %v", got, err, want)
			}
			bs := open()
			fired := 0
			bs.validEndAttribHook = func() {
				fired++
				if err := bs.Flush(); err != nil {
					t.Error(err)
				}
			}
			_, got, err := vaCall(bs, 2, storecontract.ReadBudget{})
			bs.validEndAttribHook = nil
			buffered := 0
			bs.rangePending(func(string, writeOp) { buffered++ })
			if fired != 1 || buffered != 0 {
				t.Fatalf("the hook ran %d times and left %d buffered ops; want once and none", fired, buffered)
			}
			if err != nil || !slices.Equal(got, want) {
				t.Fatalf("a flush between the rows' read and the attribution: label 2 = %v, %v; want %v", got, err, want)
			}
		})
	}
}
