package badger

import (
	"fmt"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Disk-resident label index probes (enterprise-scale ceiling 1, both-arms parity discipline:
// the disk arm and the in-memory map arm are parallel implementations of
// one contract — every probe compares them on identical op sequences).

func labelIDsVia(t *testing.T, bs *Store, token uint16) []int64 {
	t.Helper()
	nodes, err := bs.NodesByLabel(token, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	ids := make([]int64, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, int64(n.ID()))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TestLabelIndexOnDisk_ParityWithMemoryMode drives both modes through the
// same mutation sequence — puts (multi-label), label-visible updates,
// deletes — comparing NodesByLabel and ForEachNodeByLabel after EVERY
// mutation, both BEFORE flush (pending-overlay visibility: the unflushed
// write buffer must be as visible as the in-memory map was) and after.
func TestLabelIndexOnDisk_ParityWithMemoryMode(t *testing.T) {
	t.Parallel()
	mem, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New mem-mode: %v", err)
	}
	t.Cleanup(func() { mem.Close() })
	disk, err := New(Config{InMemory: true, LabelIndexOnDisk: true})
	if err != nil {
		t.Fatalf("New disk-mode: %v", err)
	}
	t.Cleanup(func() { disk.Close() })
	stores := []*Store{mem, disk}

	const labelA, labelB = uint16(1), uint16(2)
	compare := func(step string) {
		t.Helper()
		for _, tok := range []uint16{labelA, labelB} {
			a, b := labelIDsVia(t, mem, tok), labelIDsVia(t, disk, tok)
			if fmt.Sprint(a) != fmt.Sprint(b) {
				t.Fatalf("%s: label %d mem=%v disk=%v", step, tok, a, b)
			}
			var streamed []int64
			if err := disk.ForEachNodeByLabel(tok, QueryOpts{}, func(n *types.Node) bool {
				streamed = append(streamed, int64(n.ID()))
				return true
			}); err != nil {
				t.Fatalf("%s: ForEachNodeByLabel: %v", step, err)
			}
			sort.Slice(streamed, func(i, j int) bool { return streamed[i] < streamed[j] })
			if fmt.Sprint(streamed) != fmt.Sprint(a) {
				t.Fatalf("%s: label %d streamed=%v want %v", step, tok, streamed, a)
			}
		}
	}

	for i := 1; i <= 12; i++ {
		for _, bs := range stores {
			extra := []uint16(nil)
			if i%3 == 0 {
				extra = []uint16{labelB}
			}
			n := types.NewNode(types.NodeID(i), labelA, extra)
			if err := n.SetProperty("idx", int64(i)); err != nil {
				t.Fatalf("prop: %v", err)
			}
			if err := bs.PutNode(n); err != nil {
				t.Fatalf("put %d: %v", i, err)
			}
		}
		compare(fmt.Sprintf("pre-flush put %d", i)) // pending overlay
	}
	for _, bs := range stores {
		if err := bs.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	compare("post-flush")

	for _, i := range []int{2, 3, 7, 12} {
		for _, bs := range stores {
			if err := bs.DeleteNode(types.NodeID(i)); err != nil {
				t.Fatalf("delete %d: %v", i, err)
			}
		}
		compare(fmt.Sprintf("pre-flush delete %d", i)) // pending delete overlay
	}
	for _, bs := range stores {
		if err := bs.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	compare("post-delete-flush")

	// Counts stay correct without the map.
	for _, tok := range []uint16{labelA, labelB} {
		cm, _ := mem.NodeCountByLabel(tok)
		cd, _ := disk.NodeCountByLabel(tok)
		if cm != cd {
			t.Fatalf("count label %d: mem=%d disk=%d", tok, cm, cd)
		}
	}
}

// TestLabelIndexOnDisk_MapStaysEmpty pins the actual RAM win: after writes
// and scans, the in-memory labelIdx map must hold NOTHING in disk mode —
// if an ungated write path repopulates it, the ceiling silently returns.
func TestLabelIndexOnDisk_MapStaysEmpty(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true, LabelIndexOnDisk: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	for i := 1; i <= 20; i++ {
		n := types.NewNode(types.NodeID(i), 1, []uint16{2})
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := bs.NodesByLabel(1, QueryOpts{}); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if err := bs.DeleteNode(types.NodeID(5)); err != nil {
		t.Fatalf("delete: %v", err)
	}

	bs.idxMu.RLock()
	entries := 0
	for _, set := range bs.labelIdx {
		entries += len(set)
	}
	bs.idxMu.RUnlock()
	if entries != 0 {
		t.Fatalf("disk mode populated the in-memory label map with %d entries", entries)
	}
}

// TestLabelIndexOnDisk_SurvivesReopen pins the persistence contract: a
// disk-backed store reopened with the flag keeps answering label scans from
// the keyspace (loadIndexes builds the map transiently, then drops it),
// and an EXISTING directory written WITHOUT the flag works when reopened
// WITH it (the keyspace has always been written — no migration).
func TestLabelIndexOnDisk_SurvivesReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Write with the flag OFF — legacy directory.
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for i := 1; i <= 10; i++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(i), 1, nil)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen with the flag ON.
	bs2, err := New(Config{Dir: dir, LabelIndexOnDisk: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { bs2.Close() })

	ids := labelIDsVia(t, bs2, 1)
	if len(ids) != 10 {
		t.Fatalf("reopened disk-mode scan: %d nodes, want 10", len(ids))
	}
	if cnt, _ := bs2.NodeCountByLabel(1); cnt != 10 {
		t.Fatalf("reopened count: %d, want 10", cnt)
	}
	bs2.idxMu.RLock()
	mapLen := len(bs2.labelIdx)
	bs2.idxMu.RUnlock()
	if mapLen != 0 {
		t.Fatalf("reopened disk mode kept %d label map entries", mapLen)
	}
}

// TestLabelIndexOnDisk_PropertyIndexCreate pins the index-build interplay:
// CreatePropertyIndex's backfill snapshots label IDs through the disk arm.
func TestLabelIndexOnDisk_PropertyIndexCreate(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true, LabelIndexOnDisk: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })

	for i := 1; i <= 10; i++ {
		n := types.NewNode(types.NodeID(i), 1, nil)
		if err := n.SetProperty("city", fmt.Sprintf("c%d", i%2)); err != nil {
			t.Fatalf("prop: %v", err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := bs.CreatePropertyIndex(1, "city"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}
	nodes, err := bs.NodesByLabelAndProperty(1, "city", "c1", QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(nodes) != 5 {
		t.Fatalf("indexed lookup: %d nodes, want 5", len(nodes))
	}
}
