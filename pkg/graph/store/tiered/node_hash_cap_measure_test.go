//go:build measure

// Measurement level: about 40 s of writes, run by hand:
// `go test -tags measure -v -run TestNodeHashCapHitCounts ./pkg/graph/store/tiered`.

package tiered

import (
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodeHashCapHitCounts is the probe the node hash cap is judged on
// (MEMACCT, todo HASHFILL): 300,000 events, each an endpoint once beside a
// repeating reference user, read as a rel write reads them
// (EndpointIntegrityHashes), for 1,000, 50,000 and 200,000 users. It logs each
// shard's hits, misses, entries and bytes (22 Nov: the reference shard hits
// every repeat while the users fit the cap, 299,000 and 250,000 hits; about 8,800
// at 200,000 users; the hot shard 0 hits of 300,000, filled to its cap,
// 6,623,064 B) and fails only if the event shard ever hits.
func TestNodeHashCapHitCounts(t *testing.T) {
	for _, users := range []int{1000, 50_000, 200_000} {
		ts, err := New(volumeCfg(t.TempDir(), t.TempDir(), ""))
		if err != nil {
			t.Fatal(err)
		}
		reg := registrypkg.NewLabelRegistry()
		ts.SetLabelRegistry(reg)
		ct, _ := reg.GetOrCreate("Case")
		st, _ := reg.GetOrCreate("Signal")
		gen := tieredNodeGen(t)
		mk := func(n int, tok uint16) []types.NodeID {
			ids := make([]types.NodeID, 0, n)
			b := make([]*types.Node, 0, 1000)
			for i := range n {
				nd := types.NewNode(types.NodeID(gen.Generate()), tok, nil)
				nd.SetIntegrity(&types.NodeIntegrity{Hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"})
				b = append(b, nd)
				ids = append(ids, nd.ID())
				if len(b) == cap(b) || i == n-1 {
					if err := ts.PutNodesBatch(b); err != nil {
						t.Fatal(err)
					}
					b = b[:0]
				}
			}
			return ids
		}
		us := mk(users, ct)
		const events = 300_000
		for i, e := range mk(events, st) {
			if _, _, err := ts.EndpointIntegrityHashes(us[(i*7919)%users], e); err != nil {
				t.Fatal(err)
			}
		}
		m := ts.MemoryStats()
		t.Logf("users %d events %d: reference hits %d misses %d entries %d bytes %d; hot hits %d misses %d entries %d bytes %d",
			users, events, m.Reference.NodeHashHits, m.Reference.NodeHashMisses, m.Reference.NodeHashEntries, m.Reference.NodeHashes,
			m.Hot.NodeHashHits, m.Hot.NodeHashMisses, m.Hot.NodeHashEntries, m.Hot.NodeHashes)
		if m.Hot.NodeHashHits != 0 {
			t.Errorf("an event shard's hash cache hit %d times; each event is an endpoint once", m.Hot.NodeHashHits)
		}
		_ = ts.Close()
	}
}
