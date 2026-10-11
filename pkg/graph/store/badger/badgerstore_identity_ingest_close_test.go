package badger_test

import (
	"errors"
	"fmt"
	"testing"

	rho "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type identityCloseObservedStore struct {
	*badger.Store
	admitted    chan struct{}
	sealEntered chan struct{}
	sealResume  chan struct{}
}

func (s *identityCloseObservedStore) AdmitClose() (storecontract.CloseAdmissionLease, error) {
	release, err := s.Store.AdmitClose()
	if err == nil {
		close(s.admitted)
	}
	if err == nil && release != nil && s.sealEntered != nil {
		return &identityObservedSealLease{CloseAdmissionLease: release, entered: s.sealEntered, resume: s.sealResume}, nil
	}
	return release, err
}

func TestDiskIdentityGraphCloseDrainsAcceptedUniqueIngest(t *testing.T) {
	config := badger.Config{Dir: t.TempDir(), EntityIdentityOnDisk: true, HistoricalMembershipOnDisk: true, PropertyIndexOnDisk: true, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, SyncWrites: true, DisableOrdinals: true, DisablePlannerStats: true, HistoryPresenceProbeOnly: true}
	bs, err := badger.New(config)
	if err != nil {
		t.Fatal(err)
	}
	observed := &identityCloseObservedStore{Store: bs, admitted: make(chan struct{})}
	g, err := rho.New(rho.Config{Store: observed, DurableCommit: true})
	if err != nil {
		_ = bs.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = bs.Close() })
	if err := g.Constraints().CreateUnique(t.Context(), "Accepted", "key"); err != nil {
		t.Fatal(err)
	}
	s, err := g.Ingest().NewSession(ingest.IngestOptions{DeclareLabels: []string{"Accepted"}})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.AddNode([]string{"Accepted"}, map[string]any{"key": "unique-intent"})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	token, err := s.Submit()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- g.Close() }()
	<-observed.admitted
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := g.Ingest().WaitApplied(token); err != nil {
		t.Fatal("accepted unique intent was discarded during Close", err)
	}
	bs, err = badger.New(config)
	if err != nil {
		t.Fatal(err)
	}
	g, err = rho.New(rho.Config{Store: bs, DurableCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := g.Nodes().ByLabelAndProperty("Accepted", "key", "unique-intent", rho.QueryOpts{}); err != nil || len(rows) != 1 || rows[0].ID() != n.ID() {
		t.Fatal("accepted unique intent absent after reopen", rows, err)
	}
}

func (s *identityCloseObservedStore) NodesByLabelAndProperty(token uint16, key string, value any, opts storecontract.QueryOpts) ([]*types.Node, error) {
	return s.Store.NodesByLabelAndProperty(token, key, value, opts)
}

type identityObservedSealLease struct {
	storecontract.CloseAdmissionLease
	entered chan struct{}
	resume  chan struct{}
}

func (lease *identityObservedSealLease) Seal() error {
	close(lease.entered)
	<-lease.resume
	return lease.CloseAdmissionLease.Seal()
}

func TestDiskIdentityGraphCloseReentryDuringReservedDrain(t *testing.T) {
	config := badger.Config{Dir: t.TempDir(), EntityIdentityOnDisk: true, HistoricalMembershipOnDisk: true, PropertyIndexOnDisk: true, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, SyncWrites: true, DisableOrdinals: true, DisablePlannerStats: true, HistoryPresenceProbeOnly: true}
	bs, err := badger.New(config)
	if err != nil {
		t.Fatal(err)
	}
	observed := &identityCloseObservedStore{Store: bs, admitted: make(chan struct{}), sealEntered: make(chan struct{}), sealResume: make(chan struct{})}
	g, err := rho.New(rho.Config{Store: observed, DurableCommit: true})
	if err != nil {
		_ = bs.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close(); _ = bs.Close() })
	t.Cleanup(func() {
		select {
		case <-observed.sealResume:
		default:
			close(observed.sealResume)
		}
	})
	n, err := g.Nodes().Add(t.Context(), []string{"Reserved"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- g.Close() }()
	<-observed.sealEntered
	var callbackErr error
	if err := g.Nodes().ForEachByLabel("Reserved", rho.QueryOpts{}, func(row *types.Node) bool {
		if err := g.Close(); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
			callbackErr = fmt.Errorf("reserved callback Close: %w", err)
			return false
		}
		if _, err := g.Nodes().Get(t.Context(), row.ID()); err != nil {
			callbackErr = err
			return false
		}
		return true
	}); err != nil || callbackErr != nil {
		t.Fatal("read/callback during accepted-work drain", err, callbackErr)
	}
	if err := g.Close(); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		t.Fatal("external Close waited behind reserved drain", err)
	}
	close(observed.sealResume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	bs, err = badger.New(config)
	if err != nil {
		t.Fatal(err)
	}
	g, err = rho.New(rho.Config{Store: bs, DurableCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Get(t.Context(), n.ID()); err != nil {
		t.Fatal("reserved drain lost row", err)
	}
}
