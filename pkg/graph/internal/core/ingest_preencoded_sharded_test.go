package core

import (
	"context"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
)

// nativePreEncodedPut routes the pre-encoded-put fast path for *sharded.Store
// as well as *badger.Store (sharded partitions the pre-encoded wireBodies and
// logBodies per shard with index alignment preserved). These tests are the
// sharded twins of ingest_preencoded_test.go's badger coverage.

// noPreEncodeSharded embeds a real *sharded.Store — promoting every method,
// including PutNodesBatchPreEncoded — but because it is NOT the exact
// *sharded.Store type, nativePreEncodedPut declines it (the wrapper
// boundary), mirroring noPreEncodeBadger.
type noPreEncodeSharded struct {
	*sharded.Store
}

func newShardedTestStore(t *testing.T) *sharded.Store {
	t.Helper()
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	return st
}

func newShardedCore(t *testing.T) *Core {
	t.Helper()
	st := newShardedTestStore(t)
	c, err := New(Config{SnowflakeNodeID: 0, Store: st})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newDisabledShardedCore(t *testing.T) *Core {
	t.Helper()
	st := newShardedTestStore(t)
	c, err := New(Config{SnowflakeNodeID: 0, Store: noPreEncodeSharded{st}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestNativePreEncodedPut_RoutesShardedButNotWrapper directly proves the
// routing gate fix: a bare *sharded.Store gets the fast path, a wrapper
// merely embedding one (promoting its methods, not overriding them) does
// not — the exact same wrapper-boundary contract badger already had.
func TestNativePreEncodedPut_RoutesShardedButNotWrapper(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	c := newShardedCore(t)
	if c.preEncodedPut == nil {
		t.Fatal("native sharded core did not wire preEncodedPut")
	}

	dc := newDisabledShardedCore(t)
	if dc.preEncodedPut != nil {
		t.Fatal("wrapper-embedded sharded core wired preEncodedPut, want nil (wrapper boundary)")
	}
}

// TestIngestPreEncodedEndToEnd_Sharded mirrors TestIngestPreEncodedEndToEnd
// for the sharded backend: both the declared-label (pre-encoded patch) and
// undeclared-label (probe-restamp fallback) sub-paths must produce a correct
// graph now that the fast path actually engages.
func TestIngestPreEncodedEndToEnd_Sharded(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	c := newShardedCore(t)
	if c.preEncodedPut == nil {
		t.Fatalf("native sharded core did not wire preEncodedPut")
	}
	ids := ingestMixedWorkload(t, c)
	if len(ids) != 120 {
		t.Fatalf("want 120 ids, got %d", len(ids))
	}
	for _, id := range ids[:60] {
		n, err := c.Nodes.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("Get declared: %v", err)
		}
		if got := c.Nodes.Labels(n); len(got) != 1 || got[0] != "Declared" {
			t.Fatalf("declared node labels = %v, want [Declared]", got)
		}
		if ok, err := c.Hash.VerifyNodeChain(id); err != nil || !ok {
			t.Fatalf("declared chain ok=%v: %v", ok, err)
		}
	}
	// Undeclared arm — the fallback path; real tokens must resolve back.
	for _, id := range ids[60:] {
		n, err := c.Nodes.Get(context.Background(), id)
		if err != nil {
			t.Fatalf("Get undeclared: %v", err)
		}
		labels := append([]string(nil), c.Nodes.Labels(n)...)
		sort.Strings(labels)
		if len(labels) != 2 || labels[0] != "Extra" || labels[1] != "Undeclared" {
			t.Fatalf("undeclared node labels = %v, want [Extra Undeclared]", labels)
		}
		if v, ok := n.GetProperty("flag"); !ok || v != true {
			t.Fatalf("undeclared node flag prop wrong: %v", v)
		}
		if ok, err := c.Hash.VerifyNodeChain(id); err != nil || !ok {
			t.Fatalf("undeclared chain ok=%v: %v", ok, err)
		}
	}
}

// TestIngestPreEncodedVsDisabledEquivalence_Sharded is the sharded-backend
// divergence check mirroring TestIngestPreEncodedVsDisabledEquivalence: the
// same workload through the capability-enabled native sharded store and the
// capability-DISABLED wrapper must produce semantically identical graphs, and
// only the enabled core may reach a pre-encoded put door.
func TestIngestPreEncodedVsDisabledEquivalence_Sharded(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	enabled := newShardedCore(t)
	disabled := newDisabledShardedCore(t)
	enabledSpy, disabledSpy := installPreEncodedSpy(enabled), installPreEncodedSpy(disabled)

	enabledIDs := ingestMixedWorkload(t, enabled)
	disabledIDs := ingestMixedWorkload(t, disabled)
	if n := enabledSpy.calls.Load(); n == 0 {
		t.Fatal("enabled sharded core: no batch reached a pre-encoded put door, so the fast path never ran")
	}
	if n := disabledSpy.calls.Load(); n != 0 {
		t.Fatalf("disabled sharded core: %d batches reached a pre-encoded put door, want 0", n)
	}

	got := signatureMultiset(t, enabled, enabledIDs)
	want := signatureMultiset(t, disabled, disabledIDs)
	if len(got) != len(want) {
		t.Fatalf("signature multiset size diverge: enabled=%d disabled=%d", len(got), len(want))
	}
	for sig, n := range want {
		if got[sig] != n {
			t.Fatalf("signature %q count diverge: enabled=%d disabled=%d", sig, got[sig], n)
		}
	}
}
