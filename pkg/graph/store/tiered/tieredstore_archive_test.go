package tiered

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredStore_ArchiveRestart: an archived reference node survives a restart
// in the archive shard, not the reference shard. After the reopen the store
// knows it has an archive shard, the reference shard does not hold the node,
// and GetNode serves it by lazily opening the archive, which holds the row.
func TestTieredStore_ArchiveRestart(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	}

	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ts2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts2.Close() }()
	reg2 := registrypkg.NewLabelRegistry()
	ts2.SetLabelRegistry(reg2)
	_, _ = reg2.GetOrCreate("Case")

	if !ts2.HasArchiveShardForTest() {
		t.Fatal("after reopen the store does not know its archive shard")
	}
	if _, err := ts2.RefShardForTest().GetNode(n.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("reference shard GetNode of the archived node: err = %v, want ErrNodeNotFound", err)
	}
	got, err := ts2.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode after restart: %v", err)
	}
	if got.ID() != n.ID() {
		t.Fatalf("GetNode after restart: id %v, want %v", got.ID(), n.ID())
	}
	archive := ts2.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("GetNode served the archived node without opening the archive")
	}
	if _, err := archive.GetNode(n.ID()); err != nil {
		t.Fatalf("archive shard GetNode after restart: %v", err)
	}
}
