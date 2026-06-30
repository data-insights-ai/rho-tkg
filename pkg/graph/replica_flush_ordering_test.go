package graph_test

import (
	"bytes"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// flakyFlushStore wraps a real backend and injects ONE Flush() failure when
// armed, leaving the just-applied data buffered-but-undurable. The watermark
// store (MetaKV) is forwarded unchanged, so the durable applied-LSN is
// independent of the failed data flush — exactly the seam flushStoreLocked must
// respect (flush the data FIRST, only then advance the watermark).
type flakyFlushStore struct {
	store.MandatoryStore
	failNext atomic.Bool
}

func (s *flakyFlushStore) Flush() error {
	if s.failNext.CompareAndSwap(true, false) {
		return errors.New("injected flush failure")
	}
	if f, ok := s.MandatoryStore.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

func (s *flakyFlushStore) MetaGet(key string) ([]byte, error) {
	return s.MandatoryStore.(store.MetaKVCapability).MetaGet(key)
}

func (s *flakyFlushStore) MetaSet(key string, value []byte) error {
	return s.MandatoryStore.(store.MetaKVCapability).MetaSet(key, value)
}

// The durable applied-LSN watermark must NEVER lead the durable data: on a
// post-apply flush failure, ApplyChange must return the error and leave the
// watermark at its pre-call value, so a restart re-applies the record
// idempotently (no silent gap). The convergence test documents this ordering as
// "guaranteed structurally" and "not expressible at the graph façade" without
// crash injection; the flaky-flush decorator makes the invariant executable.
func TestReplicaApply_FlushFailureDoesNotAdvanceWatermark(t *testing.T) {
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "seed"}) // seed token A into the snapshot

	backend, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("backend New: %v", err)
	}
	flaky := &flakyFlushStore{MandatoryStore: backend}
	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, ReadOnlyReplica: true, Store: flaky})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()

	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	lsn0, _ := primary.Replication().LastCommittedLSN()
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}

	// One real NodePut above the watermark.
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "x"})
	var rec store.ChangeRecord
	if err := primary.Replication().ForEachChange(lsn0, func(r store.ChangeRecord) bool {
		if r.Tag == store.ChangeNodePut {
			rec = r
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if rec.Tag != store.ChangeNodePut {
		t.Fatal("setup: no NodePut record captured")
	}

	// Arm the one-shot flush failure: the apply buffers its write, then the
	// post-apply flushStoreLocked fails — the watermark must NOT advance.
	flaky.failNext.Store(true)
	applyErr := replica.Replication().ApplyChange(rec)
	if applyErr == nil {
		t.Fatal("PROPERTY VIOLATED: ApplyChange returned nil despite an injected flush failure")
	}
	if !strings.Contains(applyErr.Error(), "flush before watermark") {
		t.Fatalf("err = %v, want it to name the flush-before-watermark step", applyErr)
	}
	if got, _ := replica.Replication().AppliedLSN(); got != lsn0 {
		t.Fatalf("PROPERTY VIOLATED: watermark advanced %d -> %d on a failed flush (durable watermark led the data)", lsn0, got)
	}

	// The fault is one-shot: a retry now flushes cleanly, advances the watermark
	// to the record's LSN, and the record is durably applied (idempotent, no gap).
	if _, err := replica.Replication().ApplyChanges([]store.ChangeRecord{rec}); err != nil {
		t.Fatalf("retry ApplyChanges after the fault cleared: %v", err)
	}
	if got, _ := replica.Replication().AppliedLSN(); got != rec.LSN {
		t.Fatalf("watermark after clean retry = %d, want %d", got, rec.LSN)
	}
}
