package tiered

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 95 cut A (tasks/todo.md "R3 sizing"): tiered.Config.AdjacencyIndexOnDisk
// reaches every shard's badger.Config, and every tiered adjacency door answers
// the same with the flag on as off.

// TestTieredConfig_AdjacencyIndexOnDisk_Propagates checks the reference, hot,
// rotation-created, warm, archive and a lazily opened cold shard.
// Break cases: flag off reaches no shard; a cold reopen without the flag opens
// the shard in map mode (the flag is per open, not persisted).
func TestTieredConfig_AdjacencyIndexOnDisk_Propagates(t *testing.T) {
	for _, on := range []bool{true, false} {
		t.Run(fmt.Sprintf("flag=%v", on), func(t *testing.T) {
			dir := t.TempDir()
			cfg := Config{
				DataDir:              dir,
				RefLabels:            []string{"Case"},
				ShardWindow:          7 * 24 * time.Hour,
				FlushInterval:        1<<63 - 1,
				AdjacencyIndexOnDisk: on,
			}
			ts, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			check := func(ts *Store, name string, s *BadgerStore, want bool) {
				t.Helper()
				if s == nil {
					t.Fatalf("%s shard not open", name)
				}
				got, outN, inN, _ := s.AdjacencyIndexOnDiskForTest()
				if got != want {
					t.Errorf("%s shard AdjacencyIndexOnDisk = %v, want %v", name, got, want)
				}
				if want && outN+inN != 0 {
					t.Errorf("%s shard holds %d adjacency map entries in disk mode", name, outN+inN)
				}
			}
			check(ts, "reference", ts.RefShardForTest(), on)
			ts.mu.RLock()
			first := ts.hotShard.name
			check(ts, "hot", ts.hotShard.Store(), on)
			ts.mu.RUnlock()
			forceRotation(t, ts)
			ts.mu.RLock()
			check(ts, "rotation-created hot", ts.hotShard.Store(), on)
			ts.mu.RUnlock()
			check(ts, "warm", eventShardNamed(t, ts, first).store, on)
			if err := ts.EnsureRefArchiveForTest(); err != nil {
				t.Fatalf("archive: %v", err)
			}
			check(ts, "archive", ts.refArchive.Load(), on)
			if err := ts.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			time.Sleep(5 * time.Millisecond)

			for _, reopenOn := range []bool{on, !on} {
				coldCfg := cfg
				coldCfg.ColdAfter = time.Millisecond
				coldCfg.AdjacencyIndexOnDisk = reopenOn
				ts, err = New(coldCfg)
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				cold := eventShardNamed(t, ts, first)
				if cold.currentTier() != TierCold || cold.store != nil {
					t.Fatalf("shard %s tier %s open=%v, want a closed cold shard", first, cold.currentTier(), cold.store != nil)
				}
				s, release, err := ts.ResolveShardStoreForTest(first)
				if err != nil {
					t.Fatalf("lazy open: %v", err)
				}
				check(ts, fmt.Sprintf("lazy cold (reopen flag=%v)", reopenOn), s, reopenOn)
				release()
				if err := ts.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
			}
		})
	}
}

// adjParityFixture is one identical workload applied to a store.
type adjParityFixture struct {
	nodes   []types.NodeID // reference and event nodes
	unknown types.NodeID   // never written
}

func newAdjParityStore(t *testing.T, onDisk bool) (*Store, uint16, uint16) {
	t.Helper()
	ts, err := New(Config{
		InMemory:             true,
		RefLabels:            []string{"Case"},
		ShardWindow:          7 * 24 * time.Hour,
		FlushInterval:        1<<63 - 1,
		AdjacencyIndexOnDisk: onDisk,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	return ts, caseTok, signalTok
}

// renderAdjacency renders every tiered adjacency door for every node and type
// filter as one canonical text.
func renderAdjacency(t *testing.T, ts *Store, fx adjParityFixture) string {
	t.Helper()
	var b strings.Builder
	relsStr := func(rels []*types.Relationship) string {
		out := make([]string, 0, len(rels))
		for _, r := range rels {
			out = append(out, fmt.Sprintf("%d:%d>%d/%d", r.ID(), r.StartNodeID(), r.EndNodeID(), r.TypeToken().Value()))
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}
	all := append(append([]types.NodeID{}, fx.nodes...), fx.unknown)
	for _, typ := range []uint16{0, 1, 2, 9} {
		for _, nid := range all {
			o, oerr := ts.OutgoingRelationships(nid, typ)
			i, ierr := ts.IncomingRelationships(nid, typ)
			od, oderr := ts.OutgoingDegree(nid, typ)
			id, iderr := ts.IncomingDegree(nid, typ)
			fmt.Fprintf(&b, "n%d t%d out[%s]%v in[%s]%v deg %d/%d %v/%v\n",
				nid, typ, relsStr(o), oerr, relsStr(i), ierr, od, id, oderr, iderr)
		}
		om, oerr := ts.OutgoingRelationshipsForNodes(all, typ)
		im, ierr := ts.IncomingRelationshipsForNodes(all, typ)
		for _, nid := range all {
			fmt.Fprintf(&b, "batch n%d t%d out[%s] in[%s]\n", nid, typ, relsStr(om[nid]), relsStr(im[nid]))
		}
		fmt.Fprintf(&b, "batch t%d errs %v %v\n", typ, oerr, ierr)
	}
	for _, nid := range all {
		for _, incoming := range []bool{false, true} {
			var ids []string
			err := ts.ForEachExAdjacentRelID(nid, incoming, func(r types.RelID) bool {
				ids = append(ids, fmt.Sprint(r))
				return true
			})
			sort.Strings(ids)
			fmt.Fprintf(&b, "exadj n%d in=%v [%s] %v\n", nid, incoming, strings.Join(ids, ","), err)
		}
	}
	return b.String()
}

// TestTieredAdjacencyOnDisk_ReadsEqual runs one mixed workload (reference and
// event nodes, cross-shard rels) on a map-mode and a disk-mode store and
// compares every adjacency door after each phase.
// Break cases: typed filter (incl. an unused type), an unflushed rel, a
// deleted rel whose delete is still pending, a self-loop, an unknown node,
// archive + restore of a connected node, repair.
func TestTieredAdjacencyOnDisk_ReadsEqual(t *testing.T) {
	mem, caseTok, signalTok := newAdjParityStore(t, false)
	disk, _, _ := newAdjParityStore(t, true)
	stores := []*Store{mem, disk}
	ngen, rgen := tieredNodeGen(t), tieredRelGen(t)

	var fx adjParityFixture
	node := func(label uint16) types.NodeID {
		id := types.NodeID(ngen.Generate())
		for _, ts := range stores {
			if err := ts.PutNode(types.NewNode(id, label, nil)); err != nil {
				t.Fatalf("PutNode: %v", err)
			}
		}
		fx.nodes = append(fx.nodes, id)
		return id
	}
	rel := func(typ uint16, start, end types.NodeID) types.RelID {
		id := types.RelID(rgen.Generate())
		for _, ts := range stores {
			if err := ts.PutRelationship(types.NewRelationship(id, typ, start, end)); err != nil {
				t.Fatalf("PutRelationship: %v", err)
			}
		}
		return id
	}
	each := func(what string, fn func(ts *Store) error) {
		t.Helper()
		var errs []string
		for _, ts := range stores {
			errs = append(errs, fmt.Sprint(fn(ts)))
		}
		if errs[0] != errs[1] {
			t.Fatalf("%s: map mode %s, disk mode %s", what, errs[0], errs[1])
		}
	}
	compare := func(phase string) {
		t.Helper()
		want, got := renderAdjacency(t, mem, fx), renderAdjacency(t, disk, fx)
		if want != got {
			t.Fatalf("%s: disk mode differs from map mode\nmap:\n%s\ndisk:\n%s", phase, want, got)
		}
	}

	c1, c2 := node(caseTok), node(caseTok)
	e1, e2, e3 := node(signalTok), node(signalTok), node(signalTok)
	fx.unknown = types.NodeID(ngen.Generate())
	rel(1, c1, e1)
	rel(2, e1, e2)
	rel(1, e2, e2) // self-loop
	rel(2, e3, c2)
	rel(1, c1, c2)
	doomed := rel(1, e1, e3)
	compare("unflushed")
	each("flush", func(ts *Store) error { return ts.Flush() })
	compare("flushed")

	rel(2, e2, e3) // unflushed on top of flushed state
	each("delete", func(ts *Store) error { return ts.DeleteRelationship(doomed) })
	compare("pending add + pending delete")
	each("flush", func(ts *Store) error { return ts.Flush() })
	compare("after second flush")

	each("archive", func(ts *Store) error { return ts.ArchiveNode(c2) })
	compare("archived c2")
	each("restore", func(ts *Store) error { return ts.RestoreNode(c2) })
	compare("restored c2")

	var results []RepairResult
	for _, ts := range stores {
		r, err := ts.RunRepair()
		if err != nil {
			t.Fatalf("RunRepair: %v", err)
		}
		results = append(results, *r)
	}
	if results[0] != results[1] {
		t.Fatalf("repair: map mode %+v, disk mode %+v", results[0], results[1])
	}
	compare("after repair")
}
