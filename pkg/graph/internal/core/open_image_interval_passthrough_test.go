package core

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// TestCoreOpenImageIntervalReachesBadger: graph.Config.OpenImageInterval
// reaches the badger store core.New builds from BadgerDir.
func TestCoreOpenImageIntervalReachesBadger(t *testing.T) {
	t.Parallel()
	g, err := New(Config{BadgerDir: t.TempDir(), OpenImageInterval: time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	bs, ok := g.store.(*badger.Store)
	if !ok {
		t.Fatalf("expected *badger.Store from BadgerDir config, got %T", g.store)
	}
	if got := bs.OpenImageIntervalForTest(); got != time.Hour {
		t.Fatalf("graph.Config OpenImageInterval reached badger as %v, want 1h", got)
	}
}
