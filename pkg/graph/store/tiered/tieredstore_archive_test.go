package tiered

import (
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestTieredStore_ArchiveRestart(t *testing.T) {
	// Verify archive survives restart (disk-backed).
	dir := t.TempDir()

	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n)
	_ = ts.RefShardForTest().Flush()

	_ = ts.ArchiveNode(n.ID())
	if archive := ts.RefArchiveForTest().Load(); archive != nil {
		_ = archive.Flush()
	}

	_ = ts.Close()

	// Reopen.
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts2.Close() }()

	reg2 := registrypkg.NewLabelRegistry()
	ts2.SetLabelRegistry(reg2)
	_, _ = reg2.GetOrCreate("Case")

	// Node should be findable (triggers archive lazy-open via shardForNodeID).
	got, err := ts2.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode after restart: %v", err)
	}
	if got.ID() != n.ID() {
		t.Error("archived node ID mismatch after restart")
	}
}
