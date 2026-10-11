package graphstore

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type testApplicationSource struct {
	ApplicationReadSource
	failKey []byte
}

type cancellableApplicationSource struct {
	ApplicationReadSource
	mu      sync.Mutex
	blocked bool
	entered chan struct{}
}

func (s *cancellableApplicationSource) RootBounded(ctx context.Context, maximum int) (raftlog.ApplicationRoot, error) {
	s.mu.Lock()
	blocked := s.blocked
	s.mu.Unlock()
	if blocked {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return raftlog.ApplicationRoot{}, ctx.Err()
	}
	return s.ApplicationReadSource.RootBounded(ctx, maximum)
}

func (s *testApplicationSource) Get(ctx context.Context, key []byte, max int) (raftlog.KV, bool, error) {
	if bytes.Equal(key, s.failKey) {
		return raftlog.KV{}, false, ErrSourceUnavailable
	}
	return s.ApplicationReadSource.Get(ctx, key, max)
}
func TestPartitionTransactionSourcesDirectHistoryAndUnavailable(t *testing.T) {
	native, d, binding, cfg, stores := transactionViewFixture(t)
	sources := []ApplicationReadSource{&testApplicationSource{ApplicationReadSource: native[0]}, &testApplicationSource{ApplicationReadSource: native[1]}}
	v, _, err := OpenPartitionTransactionSources(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	})
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.NewRevision(1, 23)
	if err != nil {
		t.Fatal(err)
	}
	plan, _, err := PlanPartitionTransaction(t.Context(), v, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all}, {Kind: graphstate.CreateNode, Owner: 17, Life: 18, Scope: all}}, revision)
	if err != nil {
		t.Fatal(err)
	}
	current := transactionInstallTemplate(t, native, stores, plan, revision)
	latest, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := latest.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, id := range []graphstate.EntityID{1, 17} {
		old, err := v.Entity(t.Context(), id)
		if err != nil || old.Found {
			t.Fatal("captured source substituted later state", id, old, err)
		}
		now, err := latest.Entity(t.Context(), id)
		if err != nil || !now.Found {
			t.Fatal("new source lost created identity", id, now, err)
		}
	}
	sources[0].(*testApplicationSource).failKey = entityKey(Namespace{d.Graph(), 3}, 1)
	got, err := v.Entity(t.Context(), 1)
	if !errors.Is(err, ErrSourceUnavailable) || got.Found || got.View != (graphstate.ViewID{}) {
		t.Fatal("missing authority became absence", got, err)
	}
	sources[0].(*testApplicationSource).failKey = nil
	if got, err := v.Entity(t.Context(), 1); err != nil || got.Found {
		t.Fatal("source refusal poisoned retained view", got, err)
	}
	if _, err := (&Catalog{data: sources[0]}).initializationView(); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal("data source gained initialization proof", err)
	}
}

func TestPartitionTransactionSourcesDirectAdmission(t *testing.T) {
	native, d, b, cfg, _ := transactionViewFixture(t)
	var nilNative *raftlog.ApplicationView
	var nilProvider *testApplicationSource
	for _, sources := range [][]ApplicationReadSource{nil, {native[0]}, {native[0], nil}, {native[0], nilNative}, {native[0], nilProvider}, {native[0], native[0]}} {
		v, _, err := OpenPartitionTransactionSources(t.Context(), sources, d, b, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
		if v != nil || !errors.Is(err, ErrInvalid) {
			t.Fatal(v, err)
		}
	}
}

func TestApplicationSourceNativeNilSentinelAndZeroWork(t *testing.T) {
	_, d, b, cfg, _ := transactionViewFixture(t)
	n := Namespace{d.Graph(), 3}
	if _, work, err := ReadOwnershipDeclaration(t.Context(), nil, Limits{}, ownershipBudget()); !errors.Is(err, ErrInvalid) || work != (PageWork{}) {
		t.Fatal("native read nil precedence", work, err)
	}
	if _, work, err := StageOwnershipDeclaration(t.Context(), nil, d, Limits{}, ownershipBudget()); !errors.Is(err, ErrInvalid) || work != (PageWork{}) {
		t.Fatal("native stage nil precedence", work, err)
	}
	if _, work, err := OpenPartitionCatalog(t.Context(), nil, n, 2, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, ErrInvalid) || work != (PageWork{}) {
		t.Fatal("native catalog nil precedence", work, err)
	}
	if _, work, err := OpenPartitionCatalogWithDefaultAxis(t.Context(), nil, n, 2, b, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, ErrInvalid) || work != (PageWork{}) {
		t.Fatal("native axis catalog nil precedence", work, err)
	}
	if _, work, err := OpenPartitionGraphReadView(t.Context(), nil, d, b, cfg, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, ErrInvalid) || work != (PageWork{}) {
		t.Fatal("native graph nil precedence", work, err)
	}
	if _, _, work, err := LookupPublishedRange(t.Context(), nil, n, 1, cfg, ownershipBudget()); !errors.Is(err, ErrInvalid) || work != (PageWork{}) {
		t.Fatal("native routing nil precedence", work, err)
	}
}

func TestPartitionTransactionSourceBlockedGuardUsesQueryCancellation(t *testing.T) {
	native, d, b, cfg, _ := transactionViewFixture(t)
	blocked := &cancellableApplicationSource{ApplicationReadSource: native[0], entered: make(chan struct{}, 1)}
	v, _, err := OpenPartitionTransactionSources(t.Context(), []ApplicationReadSource{blocked, native[1]}, d, b, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	}()
	blocked.mu.Lock()
	blocked.blocked = true
	blocked.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	deadline, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	done := make(chan error, 1)
	go func() {
		got, err := v.Entity(ctx, 1)
		if got.Found || got.View != (graphstate.ViewID{}) {
			done <- errors.New("cancelled catalog query returned source data")
			return
		}
		done <- err
	}()
	select {
	case <-blocked.entered:
	case <-deadline.Done():
		t.Fatal("query never reached source guard")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-deadline.Done():
		t.Fatal("blocked source ignored query cancellation")
	}
	blocked.mu.Lock()
	blocked.blocked = false
	blocked.mu.Unlock()
	if got, err := v.Entity(t.Context(), 1); err != nil || got.Found {
		t.Fatal("cancellation poisoned graph source", got, err)
	}
}
