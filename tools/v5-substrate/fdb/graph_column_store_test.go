package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func graphColumnTestPolicy() graphColumnPolicy {
	return graphColumnPolicy{ChunkRows: 2, ChunkBytes: 32 << 10, MaxChunks: 64, MaxGenerations: 4, MaxRetainedBytes: 1 << 20, MaxWorkRows: 32, MaxWorkBytes: 4 << 20}
}

// Guards only; no simulator or fabricated graph state counts as SDK evidence.
func TestGraphColumnCodecAndCompletionRejectFalseAuthority(t *testing.T) {
	p := graphColumnTestPolicy()
	for _, invalid := range []graphColumnPolicy{{}, {ChunkRows: 33}, {ChunkRows: 1, ChunkBytes: 64<<10 + 1}} {
		if !errors.Is(invalid.validate(), errInvalid) {
			t.Fatal("unbounded policy", invalid)
		}
	}
	var absent *graphColumnStore
	if m, err := absent.publish(t.Context(), "x", cut{}, cut{}); !errors.Is(err, errInvalid) || !graphColumnSame(m, graphColumnManifest{}) {
		t.Fatal("nil publication", m, err)
	}
	h := &harness{config: graphCostConfig("columns-guard", graphCostProfile{Version: 1, Facts: 2, Writers: 1})}
	command := graphCommand{Kind: "node-create", Node: h.config.Cost.nodes()[0]}
	x := h.graphTransaction("raw-node", 0, command, snapshot{Values: [2]map[string]value{{}, {}}})
	x.Lifecycle = &lifecycleMetadata{Provenance: 1}
	request, err := wire(x)
	if err != nil {
		t.Fatal(err)
	}
	o := outcome{Commit: true, Round: math.MaxUint64, Sequence: 1, ID: x.ID, Request: x.Request}
	o.Digest = sha256.Sum256(request)
	group := changeGroup{Tx: x, Outcome: o, Writes: []changedValue{{Group: 0, Key: lifeKey(command.Node), After: value{Value: 1, Version: 1, Round: o.Round, TxID: x.ID}}}}
	original, err := wire(group)
	if err != nil {
		t.Fatal(err)
	}
	row, err := h.graphColumnRow(original)
	if err != nil {
		t.Fatal(err)
	}
	g := graphColumnGeneration{Version: 1, ID: "guard", Binding: sha256.Sum256([]byte("binding"))}
	body, err := encodeGraphColumnChunk(h, g, math.MaxUint64, [32]byte{}, []graphColumnRow{row}, p)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphColumnChunk(h, g, math.MaxUint64, [32]byte{}, body, p)
	if err != nil || len(decoded) != 1 || decoded[0].Round != math.MaxUint64 || !graphColumnSameRow(row, decoded[0]) {
		t.Fatal("unsigned coordinate lost", decoded, err)
	}
	for _, bad := range [][]byte{body[:graphColumnHeader-1], append(bytes.Clone(body), 0)} {
		if _, err := decodeGraphColumnChunk(h, g, math.MaxUint64, [32]byte{}, bad, p); !errors.Is(err, errInvalid) {
			t.Fatal("partial/noncanonical chunk", err)
		}
	}
	oversized := row
	oversized.Original = make([]byte, p.ChunkBytes+1)
	if _, err := encodeGraphColumnChunk(h, g, 0, [32]byte{}, []graphColumnRow{oversized}, p); !errors.Is(err, errLimit) {
		t.Fatal("oversized original decoded before admission", err)
	}
	if _, err := h.graphColumnRow(make([]byte, 64<<10+1)); !errors.Is(err, errLimit) {
		t.Fatal("direct original admission", err)
	}
	tiny := p
	tiny.ChunkRows, tiny.ChunkBytes = 32, 1024
	duplicate := make([]graphColumnRow, 32)
	for i := range duplicate {
		duplicate[i] = row
	}
	if _, err := encodeGraphColumnChunk(h, g, 0, [32]byte{}, duplicate, tiny); !errors.Is(err, errLimit) {
		t.Fatal("encoded admission quota", err)
	}
	wrong := bytes.Clone(body)
	binary.BigEndian.PutUint64(wrong[graphColumnHeader+2+9*8:], 1)
	if _, err := decodeGraphColumnChunk(h, g, math.MaxUint64, [32]byte{}, wrong, p); !errors.Is(err, errMismatch) {
		t.Fatal("same count wrong revision", err)
	}
	if _, err := decodeGraphColumnChunk(h, g, 0, [32]byte{}, body, p); !errors.Is(err, errMismatch) {
		t.Fatal("ordinal substituted", err)
	}
	copied := decoded[0].Original
	body[len(body)-1] ^= 1
	if bytes.Equal(copied, body[len(body)-len(copied):]) {
		t.Fatal("decoded original aliases caller bytes")
	}
	fake := graphColumnProgress{Generation: graphColumnGeneration{Cut: certificate{Round: 10, Groups: [2]uint64{1, 0}}}, Rows: 1, Sequences: [2]uint64{1, 0}, History: [2]uint64{1, 0}}
	fake.Metadata = [2]metadata{{Floor: 10, Groups: 1, Versions: 2}, {Floor: 10}}
	if graphColumnComplete(fake) {
		t.Fatal("caller count/hash blessed missing physical history")
	}
}

// Real SDK only. These authored parents remain unexecuted until the common
// implementation checkpoint; no tiny fixture establishes column capacity/GC.
func TestGraphColumnsActualPublicationOldCutReplayAndMissingCache(t *testing.T) {
	h := fixture(t, graphCostConfig(t.Name(), graphCostProfile{Version: 1, Facts: 2, CrossPercent: 50, Writers: 1}))
	initial := mustCut(t, h, []uint8{0, 1}, nil)
	p := graphColumnTestPolicy()
	s, err := newGraphColumnStore(h, p)
	if err != nil {
		t.Fatal(err)
	}
	apply := func(phase string, i uint64, command graphCommand, provenance uint64) {
		t.Helper()
		before, _, err := h.graphCostPointSnapshot(command)
		if err != nil {
			t.Fatal(err)
		}
		coordinator := uint8(0)
		if phase == "setup" {
			coordinator = command.Node.Owner
		}
		x := h.graphTransaction(fmt.Sprintf("columns-%s-%d", phase, i), coordinator, command, before)
		x.Lifecycle = &lifecycleMetadata{Provenance: provenance}
		if phase != "setup" {
			x.Lifecycle.EdgeLife = command.Edge.ID + 1
			x.Lifecycle.Definition = &lifecycleDefinition{ID: uint64(command.Edge.Value), Kind: "i64", I64: command.Edge.Value}
		}
		mustSubmit(t, h, x)
	}
	for i, node := range h.config.Cost.nodes() {
		apply("setup", uint64(i), graphCommand{Kind: "node-create", Node: node}, uint64(i+1))
	}
	for i := uint64(0); i < 2; i++ {
		apply("create", i, graphCommand{Kind: "edge-create", Edge: h.config.Cost.edge(i, 5)}, 4+i)
	}
	old := mustCut(t, h, []uint8{0, 1}, nil)
	oldManifest, err := s.publish(t.Context(), "old", initial, old)
	if err != nil || oldManifest.Rows != 5 || oldManifest.History != [2]uint64{7, 2} {
		t.Fatal("complete old generation", oldManifest, err)
	}
	for i := uint64(0); i < 2; i++ {
		apply("update", i, graphCommand{Kind: "edge-update", Edge: h.config.Cost.edge(i, 6)}, 6+i)
	}
	final := mustCut(t, h, []uint8{0, 1}, nil)
	m, err := s.publish(t.Context(), "final", initial, final)
	if err != nil || m.Rows != 7 || m.History != [2]uint64{12, 3} {
		t.Fatal("complete current generation", m, err)
	}
	if replay, err := s.publish(t.Context(), "old", initial, old); err != nil || !graphColumnSame(replay, oldManifest) {
		t.Fatal("old immutable retry after writes", replay, err)
	}
	if _, err := s.publish(t.Context(), "old", initial, final); !errors.Is(err, errMismatch) {
		t.Fatal("publication identity reused with changed cut", err)
	}
	for _, check := range []struct {
		ID    string
		Cut   cut
		Value int64
	}{{"old", old, 5}, {"final", old, 5}, {"final", final, 6}} {
		for _, owner := range []uint64{7, 9} {
			row, found, err := s.point(t.Context(), check.ID, check.Cut, owner)
			if err != nil || !found || row.Owner != owner || row.Life != owner+1 || row.Value != check.Value || row.Provenance != 4+(owner-7)/2+uint64(check.Value-5)*2 {
				t.Fatal("historical columns substituted current/phantom", row, err)
			}
		}
	}
	if _, found, err := s.point(t.Context(), "final", old, 999); err != nil || found {
		t.Fatal("phantom value", found, err)
	}
	stop := errors.New("reader stopped")
	if eof, err := s.walk(t.Context(), "final", final, nil, func(graphColumnRow) error { return stop }); !errors.Is(err, stop) || !graphColumnSame(eof, graphColumnManifest{}) {
		t.Fatal("callback error published completion", err)
	}
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	for ordinal := uint64(0); ordinal < m.Chunks; ordinal++ {
		raw, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) { return tr.Get(s.chunkKey("final", ordinal)).Get() })
		if err != nil {
			t.Fatal(err)
		}
		if ordinal == 0 {
			if err := s.upload(m.Generation, 0, [32]byte{}, raw.([]byte)); err != nil {
				t.Fatal("identical authenticated chunk replay", err)
			}
		}
		file, err := root.OpenFile(graphColumnCacheName(m.Generation, ordinal), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := file.Write(raw.([]byte))
		if err := errors.Join(writeErr, file.Close()); err != nil {
			t.Fatal(err)
		}
	}
	var seen []uint64
	if _, err := s.walk(t.Context(), "final", final, root, func(row graphColumnRow) error { seen = append(seen, row.Provenance); return nil }); err != nil || len(seen) != 7 || seen[0] != 1 || seen[1] != 3 || seen[2] != 2 {
		t.Fatal("actual enumeration/complete cache", seen, err)
	}
	foreignName := graphColumnCacheName(m.Generation, 0)
	file, err := root.OpenFile(foreignName, os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.Write([]byte("foreign-generation"))
	if err := errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.walk(t.Context(), "final", final, root, func(graphColumnRow) error { return nil }); !errors.Is(err, errMismatch) {
		t.Fatal("foreign cache accepted", err)
	}
	raw, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) { return tr.Get(s.chunkKey("final", 0)).Get() })
	if err != nil {
		t.Fatal(err)
	}
	file, err = root.OpenFile(foreignName, os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr = file.Write(raw.([]byte))
	if err := errors.Join(writeErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	if err := root.Remove(graphColumnCacheName(m.Generation, 1)); err != nil {
		t.Fatal(err)
	}
	if eof, err := s.walk(t.Context(), "final", final, root, func(graphColumnRow) error { return nil }); !errors.Is(err, fs.ErrNotExist) || !graphColumnSame(eof, graphColumnManifest{}) {
		t.Fatal("incomplete cache accepted", err)
	}
	// Remove an authentic history write: source originals still have equal counts,
	// but neither read nor a new publication may substitute their embedded bytes.
	key := fdb.Key(fmt.Sprintf("%shistory/%016x/%x", h.prefix(0), old.record.Round, edgeKey("edge", h.config.Cost.edge(1, 5))))
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) { tr.Clear(key); return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.point(t.Context(), "final", old, 9); !errors.Is(err, errUnavailable) {
		t.Fatal("missing actual history borrowed from column envelope", err)
	}
	if _, err := s.publish(t.Context(), "missing-history", initial, final); !errors.Is(err, errUnavailable) {
		t.Fatal("missing history published manifest", err)
	}
	if _, err := s.manifest("missing-history"); !errors.Is(err, errUnavailable) {
		t.Fatal("partial upload gained manifest", err)
	}
	reserved := graphColumnGeneration{Version: 1, ID: "reserved", Binding: s.digest, Initial: initial.record, Cut: final.record}
	if err := s.begin(reserved); err != nil {
		t.Fatal(err)
	}
	if _, err := s.publish(t.Context(), "quota", initial, final); !errors.Is(err, errLimit) {
		t.Fatal("generation quota ignored partial uploads", err)
	}
	if _, err := s.manifest("quota"); !errors.Is(err, errUnavailable) {
		t.Fatal("quota refusal published manifest", err)
	}
}
