package badger_test

import (
	"errors"
	"testing"

	rho "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestDiskIdentityGraphCloseCallbackRefusalLeavesGraphRetryable(t *testing.T) {
	for _, relationship := range []bool{false, true} {
		name := "node"
		if relationship {
			name = "relationship"
		}
		t.Run(name, func(t *testing.T) {
			config := badger.Config{Dir: t.TempDir(), EntityIdentityOnDisk: true, HistoricalMembershipOnDisk: true, PropertyIndexOnDisk: true, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, SyncWrites: true, DisableOrdinals: true, DisablePlannerStats: true, HistoryPresenceProbeOnly: true}
			open := func() (*badger.Store, *rho.Graph) {
				t.Helper()
				bs, err := badger.New(config)
				if err != nil {
					t.Fatal(err)
				}
				g, err := rho.New(rho.Config{Store: bs, DurableCommit: true})
				if err != nil {
					_ = bs.Close()
					t.Fatal(err)
				}
				return bs, g
			}
			bs, g := open()
			t.Cleanup(func() { _ = g.Close(); _ = bs.Close() })
			n, err := g.Nodes().Add(t.Context(), []string{"CloseSubject"}, map[string]any{"tag": "retained"})
			if err != nil {
				t.Fatal(err)
			}
			other, err := g.Nodes().Add(t.Context(), []string{"Endpoint"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			rel, err := g.Rels().Add(t.Context(), "CLOSE_TEST", n, other, nil)
			if err != nil {
				t.Fatal(err)
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			var ownCloseErr error
			callback := func() bool { ownCloseErr = g.Close(); close(entered); <-release; return true }
			go func() {
				if relationship {
					done <- g.Rels().ForEachByType("CLOSE_TEST", rho.QueryOpts{}, func(*types.Relationship) bool { return callback() })
				} else {
					done <- g.Nodes().ForEachByLabel("CloseSubject", rho.QueryOpts{}, func(*types.Node) bool { return callback() })
				}
			}()
			<-entered
			if !errors.Is(ownCloseErr, storecontract.ErrCapabilityNotSupported) {
				t.Fatal("own Graph.Close consumed lifecycle", ownCloseErr)
			}
			if err = g.Close(); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
				t.Fatal("external Graph.Close did not refuse active callback", err)
			}
			// Both refusals must preserve the public graph, not only backend openness.
			if rows, err := g.Nodes().ByLabel("CloseSubject", rho.QueryOpts{}); err != nil || len(rows) != 1 {
				t.Fatal("refusal closed public graph", rows, err)
			}
			close(release)
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			if err = g.Close(); err != nil {
				t.Fatal("retry failed", err)
			}
			if err = g.Close(); err != nil {
				t.Fatal("close replay failed", err)
			}
			// Reopening proves the backend directory lock was released and registry and
			// commit-clock checkpoints survived the retryable close.
			bs, g = open()
			if rows, err := g.Nodes().ByLabel("CloseSubject", rho.QueryOpts{TxPin: pin}); err != nil || len(rows) != 1 || rows[0].ID() != n.ID() {
				t.Fatal("registry/pin lost on retry", rows, err)
			}
			if rows, err := g.Rels().ByType("CLOSE_TEST", rho.QueryOpts{TxPin: pin}); err != nil || len(rows) != 1 || rows[0].ID() != rel.ID() {
				t.Fatal("relationship registry/pin lost", rows, err)
			}
			if now, err := g.Temporal().NowTx(); err != nil || now < pin {
				t.Fatal("commit clock rewound", now, pin, err)
			}
		})
	}
}
