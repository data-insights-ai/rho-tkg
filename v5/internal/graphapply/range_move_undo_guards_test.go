package graphapply

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestRangeMoveUndoCodecAndNativeInverseRefusalsPreserveStoredOriginals(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	// One bounded actual six-voter fixture. Only the source's genuine native
	// view is borrowed; no undo effects or adversarial codec data are installed.
	n, _, _, _, _ := rangeMoveCoordinatorFixture(t, 48)
	h := n.hosts[0][0]
	store := h.machine.store
	index, image, err := store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	view, err := store.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	}()
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	key := genesisConfigurationKey(h.machine.ns)
	existing, found, err := view.Get(t.Context(), key, 4096)
	if err != nil || !found || existing.Deleted || len(existing.Value) < 2 {
		t.Fatal("native inverse-read counterpart has no genuine stored value", found, err)
	}
	absent := append(recordKey(h.machine.ns, 201), 77)
	if _, found, err := view.Get(t.Context(), absent, 4096); err != nil || found {
		t.Fatal("native absence counterpart unexpectedly present", found, err)
	}
	before, err := store.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	newReader := func(t *testing.T, outputBytes int) *reader {
		t.Helper()
		output, err := graphstate.NewOutputBudget(outputBytes)
		if err != nil {
			t.Fatal(err)
		}
		return &reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation, arena: output}
	}
	// Positive counterpart distinguishes logical absence from present content,
	// and verifies proposed new values cannot replace the actual native inverse.
	writes := []raftlog.KV{{Key: key, Value: []byte("proposed replacement")}, {Key: absent, Value: []byte("proposed insertion")}}
	q := newReader(t, 8<<20)
	encoded, err := encodeRangeMoveUndo(q, writes)
	if err != nil || q.rows != len(writes) || len(q.writes) != 0 {
		t.Fatal("native undo counterpart", err)
	}
	previous, err := decodeRangeMoveUndo(encoded)
	if err != nil || len(previous) != 2 || !bytes.Equal(previous[0].Key, key) || !bytes.Equal(previous[0].Value, existing.Value) || previous[0].Deleted || !bytes.Equal(previous[1].Key, absent) || len(previous[1].Value) != 0 || !previous[1].Deleted {
		t.Fatal("native undo did not retain exact present/absent originals", previous, err)
	}
	positive := newReader(t, 8<<20)
	value, present, err := rangeMoveUndoOriginal(positive, key)
	if err != nil || !present || !bytes.Equal(value, existing.Value) {
		t.Fatal("native inverse point counterpart", err)
	}
	if value, present, err := rangeMoveUndoOriginal(positive, absent); err != nil || present || value != nil {
		t.Fatal("native inverse absence counterpart", err)
	}
	t.Run("write-count-admission-before-native-access", func(t *testing.T) {
		q := newReader(t, 8<<20)
		wire, err := encodeRangeMoveUndo(q, make([]raftlog.KV, 129))
		if wire != nil || !errors.Is(err, errLimit) || q.rows != 0 || q.bytes != 0 || q.arena.Used() != 0 || len(q.writes) != 0 {
			t.Fatal("129-write undo count consumed native or output work", err)
		}
	})
	t.Run("encoder-propagates-native-row-refusal-without-partial-wire", func(t *testing.T) {
		q := newReader(t, 8<<20)
		q.rows = q.limits.readRows
		wire, err := encodeRangeMoveUndo(q, writes)
		if wire != nil || !errors.Is(err, errLimit) || q.rows != q.limits.readRows || q.bytes != 0 || len(q.writes) != 0 {
			t.Fatal("encoder swallowed inverse refusal or returned partial undo", err)
		}
	})
	for _, tc := range []struct {
		name       string
		output     int
		prepare    func(*reader)
		want, also error
		rows       int
	}{
		{"rows-exhausted", 8 << 20, func(q *reader) { q.rows = q.limits.readRows }, errLimit, nil, 0},
		{"bytes-below-key-and-work-header", 8 << 20, func(q *reader) { q.bytes = q.limits.readBytes - (len(key) + 63) }, errLimit, nil, 0},
		{"output-below-inverse-metadata", 128 + 4*len(key) - 1, nil, errLimit, graphstate.ErrResourceLimit, 0},
		{"output-leaves-no-complete-key-admission", 128 + 4*len(key) + len(key) + 63, nil, errLimit, nil, 0},
		{"native-value-one-byte-over-remaining-read-budget", 8 << 20, func(q *reader) { q.bytes = q.limits.readBytes - (len(key) + len(existing.Value) + 63) }, errLimit, raftlog.ErrLimit, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := newReader(t, tc.output)
			if tc.prepare != nil {
				tc.prepare(q)
			}
			rows, workBytes := q.rows, q.bytes
			value, present, err := rangeMoveUndoOriginal(q, key)
			if value != nil || present || !errors.Is(err, tc.want) || tc.also != nil && !errors.Is(err, tc.also) || q.rows != rows+tc.rows || q.bytes != workBytes || len(q.writes) != 0 {
				t.Fatal("inverse refusal returned data, lost sentinel or charged unperformed work", tc.name, err, q.rows-rows, q.bytes-workBytes)
			}
		})
	}
	t.Run("closed-native-view-is-not-absence-and-encoder-propagates-it", func(t *testing.T) {
		closed, err := store.ApplicationView(index)
		if err != nil {
			t.Fatal(err)
		}
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		q := newReader(t, 8<<20)
		q.view = closed
		value, present, err := rangeMoveUndoOriginal(q, key)
		if value != nil || present || !errors.Is(err, raftlog.ErrClosed) || q.rows != 1 || q.bytes != 0 || len(q.writes) != 0 {
			t.Fatal("closed native inverse read became logical absence", err)
		}
		q = newReader(t, 8<<20)
		q.view = closed
		wire, err := encodeRangeMoveUndo(q, writes)
		if wire != nil || !errors.Is(err, raftlog.ErrClosed) || q.rows != 1 || q.bytes != 0 || len(q.writes) != 0 {
			t.Fatal("encoder returned partial inverse for a closed owner", err)
		}
	})
	// These are explicitly malformed INERT codec frames, never native undo
	// receipts or successful authority. Resealing reaches the structural decoder
	// instead of making every malformed case merely a checksum error.
	frame := func(t *testing.T, emit func(*boundedWriter)) []byte {
		t.Helper()
		wire, err := boundedEncoding((1<<20)-2048, func(w *boundedWriter) { w.add([]byte{'G', 'M', 'U', 1}); emit(w) })
		if err != nil {
			t.Fatal(err)
		}
		return wire
	}
	badHash := bytes.Clone(encoded)
	badHash[len(badHash)-1] ^= 1
	for _, tc := range []struct {
		name string
		wire []byte
		want error
	}{
		{"bad-undo-wire-hash", badHash, errCorrupt},
		{"count-exceeds-128", frame(t, func(w *boundedWriter) { w.u32(129) }), errLimit},
		{"count-exceeds-delivered-row-admission", frame(t, func(w *boundedWriter) { w.u32(1) }), errCorrupt},
		{"empty-original-key", frame(t, func(w *boundedWriter) { w.u32(1); emitSourceKV(w, raftlog.KV{Value: []byte{1}}) }), errCorrupt},
		{"trailing-row-after-zero-count", frame(t, func(w *boundedWriter) { w.u32(0); emitSourceKV(w, previous[0]) }), errCorrupt},
		{"invalid-tombstone-flag", frame(t, func(w *boundedWriter) { w.u32(1); w.field(key); w.field(nil); w.tag(2) }), errCorrupt},
		{"internally-truncated-second-row", frame(t, func(w *boundedWriter) { w.u32(2); emitSourceKV(w, previous[0]) }), errCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := decodeRangeMoveUndo(tc.wire)
			if rows != nil || !errors.Is(err, tc.want) {
				t.Fatal("malformed undo returned partial rows or lost sentinel", err, tc.want)
			}
		})
	}
	t.Run("every-truncated-undo-prefix-refuses-without-rows", func(t *testing.T) {
		for length := range len(encoded) {
			rows, err := decodeRangeMoveUndo(encoded[:length])
			if rows != nil || !errors.Is(err, errCorrupt) {
				t.Fatal("truncated undo returned partial data", length, err)
			}
		}
	})
	// Decoder buffers may borrow its supplied inert wire, so do not erase it.
	// Private encoding and all failed reads must leave the native store intact.
	actual, found, err := view.Get(t.Context(), key, 4096)
	if err != nil || !found || actual.Deleted || !reflect.DeepEqual(actual, existing) {
		t.Fatal("undo guard changed actual stored original", err)
	}
	afterIndex, afterImage, err := store.Checkpoint()
	if err != nil || afterIndex != index || !bytes.Equal(afterImage, image) {
		t.Fatal("private undo guard changed native checkpoint", err)
	}
	after, err := store.ApplicationUsage()
	if err != nil || after != before {
		t.Fatal("undo codec/read guard changed storage ledgers or leaked native views", before, after, err)
	}
}
