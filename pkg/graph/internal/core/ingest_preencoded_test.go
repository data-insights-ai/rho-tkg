package core

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// noPreEncodeBadger embeds a real *badger.Store — promoting every method,
// including PutNodesBatchPreEncoded — but because it is NOT the exact
// *badger.Store type, nativePreEncodedPut declines it (the wrapper boundary), so
// the graph's applier takes the encode-at-flush PutNodesBatch path. This is the
// "capability-disabled" arm of the ingest divergence battery: everything else is
// identical to native badger, only the §4.5 pre-encode fast path is off.
type noPreEncodeBadger struct {
	*badger.Store
}

func newBadgerCore(t *testing.T) *Core {
	t.Helper()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	c, err := New(Config{Store: bs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newDisabledBadgerCore(t *testing.T) *Core {
	t.Helper()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	c, err := New(Config{Store: noPreEncodeBadger{bs}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// ingestMixedWorkload drives a mix of DECLARED-label node-creates (whose queued
// tokens equal the applier's real tokens → the pre-encoded patch path is used)
// and UNDECLARED-multi-label creates (probe tokens re-stamped at apply → the
// pre-encoded buffer is invalidated and the applier re-encodes). Returns the
// created node IDs.
func ingestMixedWorkload(t *testing.T, c *Core) []types.NodeID {
	t.Helper()
	sess, err := c.Ingest.NewSession(IngestOptions{Sync: true, DeclareLabels: []string{"Declared"}})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	var ids []types.NodeID
	for i := 0; i < 60; i++ {
		n, err := sess.AddNode([]string{"Declared"}, map[string]any{"seq": int64(i), "name": fmt.Sprintf("n%d", i), "名前": "太郎"})
		if err != nil {
			t.Fatalf("AddNode declared: %v", err)
		}
		ids = append(ids, n.ID())
	}
	for i := 0; i < 60; i++ {
		n, err := sess.AddNode([]string{"Undeclared", "Extra"}, map[string]any{"seq": int64(1000 + i), "flag": true})
		if err != nil {
			t.Fatalf("AddNode undeclared: %v", err)
		}
		ids = append(ids, n.ID())
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := sess.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return ids
}

// nodeSignature is a graph-independent fingerprint (sorted labels + sorted
// non-shadow props) so two graphs with different snowflake IDs can be compared
// for semantic equivalence.
func nodeSignature(t *testing.T, c *Core, id types.NodeID) string {
	t.Helper()
	n, err := c.Nodes.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get %d: %v", id, err)
	}
	labels := append([]string(nil), c.Nodes.Labels(n)...)
	sort.Strings(labels)
	props := n.PropertiesMap()
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sig := fmt.Sprintf("L%v|", labels)
	for _, k := range keys {
		sig += fmt.Sprintf("%s=%v;", k, props[k])
	}
	// Chain integrity must hold on both paths.
	if ok, err := c.Hash.VerifyNodeChain(id); err != nil || !ok {
		t.Fatalf("VerifyNodeChain %d ok=%v: %v", id, ok, err)
	}
	return sig
}

func signatureMultiset(t *testing.T, c *Core, ids []types.NodeID) map[string]int {
	t.Helper()
	m := make(map[string]int)
	for _, id := range ids {
		m[nodeSignature(t, c, id)]++
	}
	return m
}

// TestIngestPreEncodedEndToEnd proves the apply-side pre-encoded consumption
// (native badger) produces a correct graph across BOTH sub-paths: declared labels
// (pre-encoded buffer patched + persisted verbatim) and undeclared labels
// (probe-restamp → conservative re-encode fallback). Every node reads back with
// the right labels/props and passes VerifyNodeChain, and the persisted undeclared
// nodes carry the REAL (resolved) label — proving the fallback fired without
// corrupting the row.
func TestIngestPreEncodedEndToEnd(t *testing.T) {
	t.Parallel()
	c := newBadgerCore(t)
	if c.preEncodedPut == nil {
		t.Fatalf("native badger core did not wire preEncodedPut")
	}
	ids := ingestMixedWorkload(t, c)
	if len(ids) != 120 {
		t.Fatalf("want 120 ids, got %d", len(ids))
	}
	// Declared arm.
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

// TestIngestPreEncodedVsDisabledEquivalence is the end-to-end divergence check
// (constraint B): the SAME workload ingested through the capability-enabled
// native badger and the capability-DISABLED decorator produce semantically
// identical graphs — same multiset of (labels, props) signatures — proving the
// pre-encoded patch path and the encode-at-flush path are equivalent end to end,
// including the probe-restamp fallback.
func TestIngestPreEncodedVsDisabledEquivalence(t *testing.T) {
	t.Parallel()
	on := newBadgerCore(t)
	off := newDisabledBadgerCore(t)
	if on.preEncodedPut == nil {
		t.Fatalf("enabled core missing preEncodedPut")
	}
	if off.preEncodedPut != nil {
		t.Fatalf("disabled core unexpectedly wired preEncodedPut (wrapper boundary breached)")
	}
	onIDs := ingestMixedWorkload(t, on)
	offIDs := ingestMixedWorkload(t, off)

	onSig := signatureMultiset(t, on, onIDs)
	offSig := signatureMultiset(t, off, offIDs)

	if len(onSig) != len(offSig) {
		t.Fatalf("signature-class count diverges: on=%d off=%d", len(onSig), len(offSig))
	}
	for sig, n := range onSig {
		if offSig[sig] != n {
			t.Fatalf("signature %q count diverges: on=%d off=%d", sig, n, offSig[sig])
		}
	}
}
