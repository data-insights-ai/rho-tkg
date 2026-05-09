// Tests in this file pin R5-F3 from the 2026-05-09 maintainability
// review: import must validate that every label/reltype token in the
// stream maps to a registered name in the imported registry. Pre-fix,
// validateNodeWire/validateRelWire only proved that tokens were
// non-zero and fit a uint16 — they didn't check that the registry
// actually issued the token. A corrupt or hostile export carrying
// PrimaryLabel=999 with only 5 registered labels would import without
// error, then every label query against the entity would silently
// resolve to "" and miss.
package core

import (
	"bytes"
	"errors"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
)

// R5-F3: a node record whose PrimaryLabel exceeds the registry's
// issued range must be rejected with ErrCorruptExport.
func TestR5_Import_NodeTokenBeyondRegistry_Rejected(t *testing.T) {
	t.Parallel()
	src := newTestGraph(t)
	defer src.Close()
	if _, err := src.Nodes.Add([]string{"X"}, nil); err != nil {
		t.Fatal(err)
	}

	var stream bytes.Buffer
	if err := src.IO.Export(&stream); err != nil {
		t.Fatalf("Export: %v", err)
	}

	corrupt := patchFirstNodeRecord(t, stream.Bytes(), func(w *storeutil.NodeWire) {
		// 999 is far beyond the one registered label in src; the
		// in-range uint16 wire validation passes, so any rejection
		// must come from the new registry-membership check.
		w.PrimaryLabel = 999
	})

	dst := newTestGraph(t)
	defer dst.Close()
	err := dst.IO.Import(bytes.NewReader(corrupt))
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import with out-of-range node token: got %v, want errors.Is ErrCorruptExport", err)
	}
}

// R5-F3: an extra-label token beyond the registry's issued range must
// also be rejected.
func TestR5_Import_NodeExtraTokenBeyondRegistry_Rejected(t *testing.T) {
	t.Parallel()
	src := newTestGraph(t)
	defer src.Close()
	if _, err := src.Nodes.Add([]string{"X", "Y"}, nil); err != nil {
		t.Fatal(err)
	}

	var stream bytes.Buffer
	if err := src.IO.Export(&stream); err != nil {
		t.Fatalf("Export: %v", err)
	}

	corrupt := patchFirstNodeRecord(t, stream.Bytes(), func(w *storeutil.NodeWire) {
		// keep PrimaryLabel valid; force one extra label out of range.
		if len(w.ExtraLabels) == 0 {
			w.ExtraLabels = []int{12345}
			return
		}
		w.ExtraLabels[0] = 12345
	})

	dst := newTestGraph(t)
	defer dst.Close()
	err := dst.IO.Import(bytes.NewReader(corrupt))
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import with out-of-range extra-label token: got %v, want errors.Is ErrCorruptExport", err)
	}
}

// R5-F3: a relationship record whose RelType exceeds the registry's
// issued range must be rejected with ErrCorruptExport.
func TestR5_Import_RelTokenBeyondRegistry_Rejected(t *testing.T) {
	t.Parallel()
	src := newTestGraph(t)
	defer src.Close()
	a, err := src.Nodes.Add([]string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := src.Nodes.Add([]string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Rels.Add("KNOWS", a, b, nil); err != nil {
		t.Fatal(err)
	}

	var stream bytes.Buffer
	if err := src.IO.Export(&stream); err != nil {
		t.Fatalf("Export: %v", err)
	}

	corrupt := patchFirstRelRecord(t, stream.Bytes(), func(w *storeutil.RelWire) {
		w.RelType = 999
	})

	dst := newTestGraph(t)
	defer dst.Close()
	err = dst.IO.Import(bytes.NewReader(corrupt))
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import with out-of-range rel type token: got %v, want errors.Is ErrCorruptExport", err)
	}
}

// R5-F4: importing a node-history record that conflicts with an
// existing same-id same-version snapshot must be rejected, not silently
// overwritten. Pre-fix, PutNodeVersion blew away the existing version
// in place — leaving the graph with history bytes that never existed
// in either source.
func TestR5_Import_ConflictingDuplicateNodeHistory_Rejected(t *testing.T) {
	t.Parallel()

	// Source: node X with v=1, then update so a v0 history record
	// is materialised with property foo=1.
	src := newTestGraph(t)
	defer src.Close()
	srcNode, err := src.Nodes.Add([]string{"X"}, map[string]any{"foo": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Nodes.Update(srcNode.ID(), map[string]any{"foo": int64(2)}); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	if err := src.IO.Export(&stream); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Destination: same id, but the v0 history record we'll place
	// has property foo=99 — different from the stream's v0 (foo=1).
	dst := newTestGraph(t)
	defer dst.Close()
	if _, err := dst.Nodes.Import(t.Context(), srcNode.ID(), []string{"X"}, map[string]any{"foo": int64(1)}); err != nil {
		t.Fatalf("seed conflicting current node: %v", err)
	}
	if _, err := dst.Nodes.Update(srcNode.ID(), map[string]any{"foo": int64(99)}); err != nil {
		t.Fatalf("seed conflicting v0 history: %v", err)
	}

	err = dst.IO.Import(bytes.NewReader(stream.Bytes()))
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import with conflicting node-history duplicate: got %v, want errors.Is ErrCorruptExport", err)
	}
}

// R5-F4 idempotent counterpart for history: re-importing into the same
// graph must succeed (history v0 is byte-identical to the stream's v0).
func TestR5_Import_IdenticalDuplicateNodeHistory_Allowed(t *testing.T) {
	t.Parallel()
	src := newTestGraph(t)
	defer src.Close()
	srcNode, err := src.Nodes.Add([]string{"X"}, map[string]any{"foo": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Nodes.Update(srcNode.ID(), map[string]any{"foo": int64(2)}); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	if err := src.IO.Export(&stream); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := src.IO.Import(bytes.NewReader(stream.Bytes())); err != nil {
		t.Fatalf("idempotent re-import with history: %v", err)
	}
}

// R5-F4: same protection on the relationship side.
func TestR5_Import_ConflictingDuplicateRelHistory_Rejected(t *testing.T) {
	t.Parallel()

	src := newTestGraph(t)
	defer src.Close()
	a, err := src.Nodes.Add([]string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := src.Nodes.Add([]string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := src.Rels.Add("KNOWS", a, b, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Rels.Update(rel.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatal(err)
	}
	var stream bytes.Buffer
	if err := src.IO.Export(&stream); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Build a destination with the same ids but a diverging v0
	// history. Use ImportRelationship to force the same rel id, then
	// drive an update to materialise a conflicting v0.
	dst := newTestGraph(t)
	defer dst.Close()
	da, err := dst.Nodes.Import(t.Context(), a.ID(), []string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	db, err := dst.Nodes.Import(t.Context(), b.ID(), []string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	dRel, err := dst.Rels.Import(t.Context(), rel.ID(), "KNOWS", da, db, map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("seed conflicting current rel: %v", err)
	}
	if _, err := dst.Rels.Update(dRel.ID(), map[string]any{"w": int64(99)}); err != nil {
		t.Fatalf("seed conflicting rel history: %v", err)
	}

	err = dst.IO.Import(bytes.NewReader(stream.Bytes()))
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import with conflicting rel-history duplicate: got %v, want errors.Is ErrCorruptExport", err)
	}
}

// patchFirstNodeRecord walks the framing, finds the first record with
// tag exportTagNode, unmarshals its body into a NodeWire, applies the
// caller's mutation, re-marshals, and emits the rewritten stream.
// All other records pass through untouched.
func patchFirstNodeRecord(t *testing.T, full []byte, mutate func(*storeutil.NodeWire)) []byte {
	t.Helper()
	return patchFirstRecord(t, full, exportTagNode, func(body []byte) []byte {
		var w storeutil.NodeWire
		if err := msgpack.Unmarshal(body, &w); err != nil {
			t.Fatalf("unmarshal node wire: %v", err)
		}
		mutate(&w)
		out, err := msgpack.Marshal(&w)
		if err != nil {
			t.Fatalf("marshal node wire: %v", err)
		}
		return out
	})
}

func patchFirstRelRecord(t *testing.T, full []byte, mutate func(*storeutil.RelWire)) []byte {
	t.Helper()
	return patchFirstRecord(t, full, exportTagRel, func(body []byte) []byte {
		var w storeutil.RelWire
		if err := msgpack.Unmarshal(body, &w); err != nil {
			t.Fatalf("unmarshal rel wire: %v", err)
		}
		mutate(&w)
		out, err := msgpack.Marshal(&w)
		if err != nil {
			t.Fatalf("marshal rel wire: %v", err)
		}
		return out
	})
}

func patchFirstRecord(t *testing.T, full []byte, target uint8, rewrite func([]byte) []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	r := bytes.NewReader(full)
	patched := false
	for {
		tag, body, err := readExportRecord(r)
		if err != nil {
			break
		}
		if !patched && tag == target {
			body = rewrite(body)
			patched = true
		}
		if werr := writeExportRecord(&out, tag, body); werr != nil {
			t.Fatalf("rewrite stream: %v", werr)
		}
	}
	if !patched {
		t.Fatalf("no record with tag 0x%02x found in stream", target)
	}
	return out.Bytes()
}
