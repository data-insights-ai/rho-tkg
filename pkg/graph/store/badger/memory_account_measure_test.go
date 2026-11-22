//go:build measure

// Measurement level (RH 4 Nov): live heap moves with load, so this bound runs
// by hand, not in a gate:
// `go test -tags measure -v -run TestMemoryAccountFollowsTheHeap ./pkg/graph/store/badger`.

package badger

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// memAcctChildEnv marks a measuring child of TestMemoryAccountFollowsTheHeap
// and carries its MemTableSize in MiB.
const memAcctChildEnv = "RHO_MEMACCT_CHILD"

// TestMemoryAccountFollowsTheHeap is MEMACCT's heap arm. One store, run as
// ai-soc's tiered shard (label and adjacency indexes on disk, no ordinals),
// in a fresh run of the test binary per MemTableSize; the heap is HeapAlloc
// after a Flush and two collections, the account MemoryAccount.Bytes plus the
// decoded-entity caches (EntityCacheUsage). Phase 1 writes 200,000 nodes of
// one label with a distinct property value each and reads 100,000 of their
// integrity hashes (past the cache's 65,536); phase 2 writes 100,000 rels of
// one type between them.
//   - 64 MiB, phase 1 (no memtable rotates): heap growth within 1.2 of the
//     account's (22 Nov: 1.14 to 1.19).
//   - 64 MiB, phase 2 (the memtable rotates): heap growth at most the
//     account's plus MemTableArenaMax less MemTableArena, the rotated arenas
//     badger may hold.
//   - 512 MiB, phase 2 (no rotation): within 1.2 (1.01 to 1.17); it is the arm that
//     sees the rel-type index.
//
// Not gated, logged: phase 1 at 512 MiB (1.38, todo MEMHEAP).
func TestMemoryAccountFollowsTheHeap(t *testing.T) {
	if os.Getenv(memAcctChildEnv) == "" {
		for _, mib := range []string{"64", "512"} {
			cmd := exec.Command(os.Args[0], "-test.run=^TestMemoryAccountFollowsTheHeap$", "-test.count=1", "-test.v") // #nosec G204 -- the test binary itself
			cmd.Env = append(os.Environ(), memAcctChildEnv+"="+mib)
			out, err := cmd.CombinedOutput()
			t.Logf("MemTableSize %s MiB:\n%s", mib, out)
			if err != nil {
				t.Errorf("measuring child at %s MiB: %v", mib, err)
			}
		}
		return
	}
	rotates := os.Getenv(memAcctChildEnv) == "64"
	memTable := int64(512 << 20)
	if rotates {
		memTable = 64 << 20
	}
	cfg := Config{Dir: t.TempDir(), BlockCacheSize: 1 << 20, IndexCacheSize: 1 << 20, MemTableSize: memTable,
		LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, DisableOrdinals: true}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	account := func() (int64, MemoryAccount, int64) {
		if err := bs.Flush(); err != nil { // the staged writes are no holder of the open store
			t.Fatal(err)
		}
		a := bs.MemoryAccount()
		n, r := bs.EntityCacheUsage()
		var m runtime.MemStats
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&m)
		return a.Bytes() + n.CleanBytes + n.DirtyBytes + r.CleanBytes + r.DirtyBytes, a, int64(m.HeapAlloc) // #nosec G115 -- a heap size
	}
	const n, reads = 200_000, 100_000
	put := func(from, to int) {
		batch := make([]*types.Node, 0, 1000)
		for i := from; i < to; i++ {
			nd := types.NewNode(types.NodeID(existMapBase+int64(i)), 1, nil)
			if err := nd.SetProperty("v", fmt.Sprintf("v%07d", i)); err != nil {
				t.Fatal(err)
			}
			nd.SetIntegrity(&types.NodeIntegrity{Hash: fmt.Sprintf("%064x", i)})
			batch = append(batch, nd)
			if len(batch) == cap(batch) || i == to-1 {
				if err := bs.PutNodesBatch(batch); err != nil {
					t.Fatal(err)
				}
				batch = batch[:0]
			}
		}
	}
	within := func(phase string, da, dh int64) {
		t.Helper()
		if r := float64(dh) / float64(da); r > 1.2 || r < 1/1.2 {
			t.Errorf("%s: live heap grew %d B against the account's %d B (%.2f), want within 1.2", phase, dh, da, r)
		}
	}
	put(0, 1000) // the first write makes the sketch
	a0, _, h0 := account()
	put(1000, 1000+n)
	for i := range reads {
		if _, err := bs.NodeIntegrityHash(types.NodeID(existMapBase + int64(1000+i))); err != nil {
			t.Fatal(err)
		}
	}
	a1, m1, h1 := account()
	t.Logf("phase 1: account and caches grew %d B, live heap %d B: %.2f", a1-a0, h1-h0, float64(h1-h0)/float64(a1-a0))
	if rotates {
		within("phase 1", a1-a0, h1-h0)
	}
	rels := make([]*types.Relationship, 0, 1000)
	for i := range n / 2 {
		rels = append(rels, types.NewRelationship(types.RelID(existMapBase+int64(i)), 1,
			types.NodeID(existMapBase+int64(1000+2*i)), types.NodeID(existMapBase+int64(1001+2*i))))
		if len(rels) == cap(rels) {
			if err := bs.PutRelationshipsBatch(rels); err != nil {
				t.Fatal(err)
			}
			rels = rels[:0]
		}
	}
	a2, m2, h2 := account()
	t.Logf("phase 2: account and caches grew %d B, live heap %d B: %.2f", a2-a1, h2-h1, float64(h2-h1)/float64(a2-a1))
	t.Logf("account after phase 1 %+v, after phase 2 %+v", m1, m2)
	if !rotates {
		within("phase 2", a2-a1, h2-h1)
		return
	}
	if room := m2.MemTableArenaMax - m2.MemTableArena; h2-h1 > a2-a1+room {
		t.Errorf("phase 2 with a rotating memtable: live heap grew %d B, above the account's %d B and the %d B of rotated arenas MemTableArenaMax allows",
			h2-h1, a2-a1, room)
	}
}
