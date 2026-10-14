package graph_test

// Backlog 61 measurement: bytes per edge spent on integrity hashes, on disk
// (badger rows, raw and Snappy-compressed in 4 KiB blocks as badger stores
// them) and resident (memory store heap). Opt-in: RHO_INTEGRITY_BYTES=1.

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/klauspost/compress/snappy"
)

const ibEdges, ibNodes = 20000, 2000

func ibWrite(t *testing.T, g *graphpkg.Graph) {
	t.Helper()
	ctx := context.Background()
	nodes := make([]*types.Node, ibNodes)
	for i := range nodes {
		n, err := g.Nodes().Add(ctx, []string{"Device"}, map[string]any{"key": fmt.Sprintf("dev-%d", i), "estate": "BA"})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
	}
	for i := 0; i < ibEdges; i++ {
		_, err := g.Rels().Add(ctx, "LOGON", nodes[i%ibNodes], nodes[(i*7+1)%ibNodes], map[string]any{
			"record": fmt.Sprintf("rec-%08d", i), "user": fmt.Sprintf("u%d", i%97), "status": "ok", "n": int64(i)})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// ibDisk sums the badger rows by key class: raw bytes and Snappy bytes (4 KiB
// blocks of consecutive values, as an SST block is compressed).
func ibDisk(t *testing.T, db *badgerv4.DB) (relRaw, relSnappy, nodeRaw, allRaw int) {
	t.Helper()
	var block []byte
	flush := func() {
		if len(block) > 0 {
			relSnappy += len(snappy.Encode(nil, block))
			block = block[:0]
		}
	}
	err := db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			k := item.Key()
			allRaw += len(k) + len(v)
			switch k[0] {
			case storeutil.KeyRel:
				relRaw += len(v)
				block = append(block, k...)
				block = append(block, v...)
				if len(block) >= 4096 {
					flush()
				}
			case storeutil.KeyNode:
				nodeRaw += len(v)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	flush()
	return
}

func ibHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func TestIntegrityBytesPerEdge(t *testing.T) {
	if os.Getenv("RHO_INTEGRITY_BYTES") == "" {
		t.Skip("set RHO_INTEGRITY_BYTES=1")
	}
	for _, c := range []struct{ off, binary bool }{{false, false}, {false, true}, {true, false}} {
		off := c.off
		restore := storeutil.SetBinaryHashWriterForTest(c.binary)
		bs, err := badger.New(badger.Config{Dir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		g, err := graphpkg.New(graphpkg.Config{Store: bs, DisableIntegrityHashes: off})
		if err != nil {
			t.Fatal(err)
		}
		ibWrite(t, g)
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		relRaw, relSnappy, nodeRaw, allRaw := ibDisk(t, bs.DBForTest())
		t.Logf("badger off=%-5v binary=%-5v rel row %6.1f B/edge raw, %6.1f B/edge snappy; node row %6.1f B/node; all keys+values %6.1f B/edge",
			off, c.binary, float64(relRaw)/ibEdges, float64(relSnappy)/ibEdges, float64(nodeRaw)/ibNodes, float64(allRaw)/ibEdges)
		_ = g.Close()

		before := ibHeap()
		ms := memory.New()
		g, err = graphpkg.New(graphpkg.Config{Store: ms, DisableIntegrityHashes: off})
		if err != nil {
			t.Fatal(err)
		}
		ibWrite(t, g)
		after := ibHeap()
		t.Logf("memory off=%-5v resident %6.1f B/edge (heap after %d edges + %d nodes, per edge)",
			off, float64(after-before)/ibEdges, ibEdges, ibNodes)
		runtime.KeepAlive(g)
		_ = g.Close()
		restore()
	}
}
