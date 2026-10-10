package raftlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func controlStream(t *testing.T, s *Store) (ApplicationSnapshotManifest, []ApplicationSnapshotChunk) {
	t.Helper()
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	chunks := collectPublished(t, e, ReadBudget{1, 4096})
	if m.Version != 4 || m.Contract.Version != 2 {
		t.Fatal(m)
	}
	for _, c := range chunks {
		if c.Version != 4 {
			t.Fatal(c.Version)
		}
	}
	return m, chunks
}
func TestControlTransferPreservesExactAllFamiliesAndBinding(t *testing.T) {
	donor, _ := newControlStore(t, nil, 2)
	for _, step := range []struct {
		image, value string
		deleted      bool
		puts         []ApplicationControlPut
	}{
		{"r2", "v1", false, []ApplicationControlPut{{Key: []byte{'A', 0}, Value: []byte("reg")}}},
		{"r3", "v2", false, []ApplicationControlPut{{Key: []byte("z"), Value: []byte{}}}},
		{"r4", "", true, nil},
	} {
		index, batch := controlBatch(t, donor, step.image, step.puts, KV{Key: []byte("g"), Value: []byte(step.value), Deleted: step.deleted})
		if err := donor.InstallApplication(index, batch); err != nil {
			t.Fatal(err)
		}
	}
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	m, chunks := controlStream(t, donor)
	if m.ControlBytes != 101 || m.ControlRecords != 2 || m.NamespaceRecords != ([4]uint64{3, 4, 4, 4}) || m.NamespaceBytes != ([4]uint64{148, 190, 183, 183}) {
		t.Fatal(m)
	}
	bytesTotal, rowsTotal, err := snapshotTotals(m)
	if err != nil || bytesTotal != 805 || rowsTotal != 17 {
		t.Fatal(bytesTotal, rowsTotal, err)
	}
	// Literal canonical keys: tags8/18 use complemented insertion;9/10/11 use index.
	expected := map[string]struct {
		index   uint64
		value   string
		deleted bool
	}{
		"\x08g\x00\x00\xff\xff\xff\xff\xff\xff\xff\xfd":         {2, "v1", false},
		"\x08g\x00\x00\xff\xff\xff\xff\xff\xff\xff\xfc":         {3, "v2", false},
		"\x08g\x00\x00\xff\xff\xff\xff\xff\xff\xff\xfb":         {4, "", true},
		"\x09\x00\x00\x00\x00\x00\x00\x00\x01":                  {1, "seed", false},
		"\x09\x00\x00\x00\x00\x00\x00\x00\x02":                  {2, "r2", false},
		"\x09\x00\x00\x00\x00\x00\x00\x00\x03":                  {3, "r3", false},
		"\x09\x00\x00\x00\x00\x00\x00\x00\x04":                  {4, "r4", false},
		"\x0a\x00\x00\x00\x00\x00\x00\x00\x01":                  {1, "", false},
		"\x0a\x00\x00\x00\x00\x00\x00\x00\x02":                  {2, "c", false},
		"\x0a\x00\x00\x00\x00\x00\x00\x00\x03":                  {3, "c", false},
		"\x0a\x00\x00\x00\x00\x00\x00\x00\x04":                  {4, "c", false},
		"\x0b\x00\x00\x00\x00\x00\x00\x00\x01":                  {1, "", false},
		"\x0b\x00\x00\x00\x00\x00\x00\x00\x02":                  {2, "o", false},
		"\x0b\x00\x00\x00\x00\x00\x00\x00\x03":                  {3, "o", false},
		"\x0b\x00\x00\x00\x00\x00\x00\x00\x04":                  {4, "o", false},
		"\x12A\x00\xff\x00\x00\xff\xff\xff\xff\xff\xff\xff\xfd": {2, "reg", false},
		"\x12z\x00\x00\xff\xff\xff\xff\xff\xff\xff\xfc":         {3, "", false},
	}
	check := func(k, raw []byte, bankB bool, seen map[string]bool) {
		key := bytes.Clone(k)
		if bankB {
			if key[0] == 19 {
				key[0] = 18
			} else {
				key[0] -= 4
			}
		}
		want, exists := expected[string(key)]
		if !exists || seen[string(key)] {
			t.Fatal("extra/duplicate literal key", k)
		}
		index := binary.BigEndian.Uint64(key[len(key)-8:])
		if key[0] == 8 || key[0] == 18 {
			index = ^index
		}
		value, deleted, err := inspectAppFrame(k, raw, 4096)
		if err != nil || index != want.index || string(value) != want.value || deleted != want.deleted {
			t.Fatal("literal tuple", k, index, string(value), deleted, want, err)
		}
		seen[string(key)] = true
	}
	checkStream := func(chunks []ApplicationSnapshotChunk) {
		seen := make(map[string]bool)
		for _, chunk := range chunks {
			if err := walkSnapshotChunk(chunk.Data, m.Contract, func(k, raw []byte) error { check(k, raw, false, seen); return nil }); err != nil {
				t.Fatal(err)
			}
		}
		if len(seen) != 17 {
			t.Fatal("missing literal export records", seen)
		}
	}
	checkBank := func(s *Store) {
		if s.activeBank() != 1 || s.ApplicationGeneration() != 2 {
			t.Fatal("wrong active bank/generation")
		}
		seen := make(map[string]bool)
		for _, bounds := range [][2]byte{{12, 16}, {19, 20}} {
			func() {
				it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{bounds[0]}, UpperBound: []byte{bounds[1]}})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := it.Close(); err != nil {
						t.Error(err)
					}
				}()
				for valid := it.First(); valid; valid = it.Next() {
					raw, err := it.ValueAndErr()
					if err != nil {
						t.Fatal(err)
					}
					check(it.Key(), raw, true, seen)
				}
				if err := it.Error(); err != nil {
					t.Fatal(err)
				}
			}()
		}
		if len(seen) != 17 {
			t.Fatal("missing literal bank-B records", seen)
		}
	}
	checkStream(chunks)
	raw, err := EncodeApplicationSnapshotManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeApplicationSnapshotManifestForBinding(raw, m.Contract.MaxImageBytes, donor.ApplicationBinding())
	if err != nil || decoded.ControlBytes != 101 {
		t.Fatal(decoded, err)
	}
	descriptor, err := EncodeApplicationSnapshotDescriptor(m)
	if err != nil || descriptor[2] != 3 || len(descriptor) > 512 {
		t.Fatal(descriptor, err)
	}
	fs := vfs.NewMem()
	receiver, cfg := newControlStore(t, fs, 1)
	old := viewApplication(t, receiver, 1)
	prepared, snap := semanticPrepared(t, receiver, m, chunks)
	claim, err := receiver.ClaimPreparedApplicationSnapshot(prepared, snap)
	if err != nil {
		t.Fatal(err)
	}
	root, err := receiver.PersistApplicationReady(raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Vote: new(uint64(3)), Commit: new(m.Index)}}, claim)
	if err != nil || root.Index != 4 {
		t.Fatal(root, err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if _, found, _, err := old.GetControl(t.Context(), []byte{'A', 0}, ReadBudget{2, 4096}); err != nil || found {
		t.Fatal("old generation substituted", found, err)
	}
	now := viewApplication(t, receiver, 4)
	row, found, _, err := now.GetControl(t.Context(), []byte{'A', 0}, ReadBudget{2, 4096})
	if err != nil || !found || string(row.Value) != "reg" {
		t.Fatal(row, found, err)
	}
	checkBank(receiver)
	if err := receiver.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := now.Close(); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Create = false
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	checkBank(reopened)
	again, stream := controlStream(t, reopened)
	checkStream(stream)
	if again.RecordsHash != m.RecordsHash || again.CutID != m.CutID || len(stream) != len(chunks) {
		t.Fatal("reopen changed stream")
	}
	legacy := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{41}, 1)
	if _, err := legacy.BeginApplicationImport(t.Context(), m); !errors.Is(err, ErrInvalid) {
		t.Fatal("legacy accepted AS4", err)
	}
}

func TestControlManifestSubtotalAndIdentityRefusals(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte("r"), Value: []byte("reg")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	m, _ := controlStream(t, s)
	for _, mutate := range []func(*ApplicationSnapshotManifest){func(m *ApplicationSnapshotManifest) { m.ControlRecords++ }, func(m *ApplicationSnapshotManifest) { m.ControlBytes++ }, func(m *ApplicationSnapshotManifest) { m.Version = 3 }, func(m *ApplicationSnapshotManifest) { m.Contract.Version = 1 }, func(m *ApplicationSnapshotManifest) { m.SemanticContractID = ApplicationSemanticContractID{} }} {
		bad := m
		mutate(&bad)
		if _, err := EncodeApplicationSnapshotManifest(bad); err == nil {
			t.Fatal("forged manifest accepted", bad)
		}
	}
}

func controlWireRecord(key, raw []byte) []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(key)))
	b = binary.BigEndian.AppendUint32(b, uint32(len(raw)))
	b = append(b, key...)
	return append(b, raw...)
}

func TestControlMalformedTransferRefusesAtomicallyAndOriginalRetries(t *testing.T) {
	donor, _ := newControlStore(t, nil, 2)
	installControl(t, donor, "seed", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	m, chunks := controlStream(t, donor)
	last := chunks[len(chunks)-1]
	if !last.Final || len(last.Data) != 61 || last.After[0] != 18 {
		t.Fatal("fixture must end at literal control53 plus header8", last)
	}
	for _, kind := range []string{"wrong-version", "truncated", "omitted", "duplicate", "extra", "tombstone", "future", "cancel", "gap-cursor"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := newControlStore(t, nil, 1)
			i, err := s.BeginApplicationImport(t.Context(), m)
			if err != nil {
				t.Fatal(err)
			}
			defer i.Abort()
			for _, c := range chunks[:len(chunks)-1] {
				if err := i.Append(t.Context(), c); err != nil {
					t.Fatal(err)
				}
			}
			before := readyMetadataFingerprint(t, s)
			status, err := i.Status()
			if err != nil {
				t.Fatal(err)
			}
			bad := last
			bad.Data, bad.After = bytes.Clone(last.Data), bytes.Clone(last.After)
			ctx := t.Context()
			want := ErrCorrupt
			switch kind {
			case "wrong-version":
				bad.Version = 3
				want = ErrInvalid
			case "truncated":
				bad.Data = bad.Data[:len(bad.Data)-1]
			case "omitted":
				bad.Data = nil
			case "duplicate":
				bad.Data = append(bad.Data, last.Data...)
				bad.Visited = 2
				bad.VisitedBytes = uint64(len(bad.Data))
			case "extra":
				key := controlVersionKey(0, []byte("z"), 2)
				bad.Data = append(bad.Data, controlWireRecord(key, appFrame(key, []byte("reg"), false))...)
				bad.After, bad.Visited = key, 2
				bad.VisitedBytes = uint64(len(bad.Data))
			case "tombstone":
				bad.Data = controlWireRecord(bad.After, appFrame(bad.After, nil, true))
			case "future":
				key := controlVersionKey(0, []byte{'A', 0}, 3)
				bad.Data = controlWireRecord(key, appFrame(key, []byte("reg"), false))
				bad.After = key
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "gap-cursor":
				bad.After = bytes.Clone(i.after)
				want = ErrInvalid
			}
			if err := i.Append(ctx, bad); !errors.Is(err, want) {
				t.Fatal("wrong refusal identity", kind, err)
			}
			after, err := i.Status()
			if err != nil || after != status || readyMetadataFingerprint(t, s) != before || s.poison != nil {
				t.Fatal("refusal advanced state or poisoned receiver", after, status, err)
			}
			if err := i.Append(t.Context(), last); err != nil {
				t.Fatal("original retry refused", err)
			}
			if err := i.Verify(t.Context()); err != nil {
				t.Fatal("original retry failed exact verification", err)
			}
		})
	}
}

func TestControlPublishedCursorQuotaCancelAndFutureGapWork(t *testing.T) {
	s, _ := newControlStore(t, nil, 1)
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte{'A', 0}, Value: []byte("reg")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	// This real later installation precedes the Pebble snapshot capture. All
	// four future frames must consume work while the old cut emits only7 rows.
	installControl(t, s, "seed", ApplicationControlPut{Key: []byte("d"), Value: []byte("later")})
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	type cursorLedger struct {
		after, last                                              string
		sequence, chunks, bytes, rows, controlBytes, controlRows uint64
		namespaceBytes, namespaceRows                            [4]uint64
		digest                                                   [32]byte
		ready, final                                             bool
	}
	capture := func() cursorLedger {
		p, v := e.published, e.published.verifier
		return cursorLedger{string(e.after), string(v.last), e.sequence, p.buildChunks, v.bytes, v.rows, v.controlBytes, v.controlRows, v.namespaceBytes, v.namespaceRows, v.digest, p.ready, e.final}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := capture()
	// First root:9 key +40 frame +8 framing=57. Fixed160+2*57+3*9=301.
	if ready, err := e.BuildManifest(t.Context(), ReadBudget{1, 300}); ready || !errors.Is(err, ErrLimit) || capture() != before || s.poison != nil {
		t.Fatal("build one-short advanced", ready, err)
	}
	if ready, err := e.BuildManifest(ctx, ReadBudget{1, 301}); ready || !errors.Is(err, context.Canceled) || capture() != before || s.poison != nil {
		t.Fatal("build cancel advanced", ready, err)
	}
	if ready, err := e.BuildManifest(t.Context(), ReadBudget{1, 301}); ready || err != nil {
		t.Fatal("exact first row", ready, err)
	}
	for pages := 1; ; pages++ {
		if pages >= 16 {
			t.Fatal("bounded fixture manifest did not finish")
		}
		ready, err := e.BuildManifest(t.Context(), ReadBudget{1, 4096})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	m, err := e.Manifest()
	if err != nil || m.Index != 2 || m.ControlBytes != 53 || m.ControlRecords != 1 || m.NamespaceRecords != ([4]uint64{0, 2, 2, 2}) || e.published.buildChunks != 11 {
		t.Fatal("future work or historical set changed", m, e.published.buildChunks, err)
	}
	before = capture()
	if c, err := e.Next(t.Context(), ReadBudget{1, 300}); !errors.Is(err, ErrLimit) || c.Data != nil || c.After != nil || c.Visited != 0 || capture() != before || s.poison != nil {
		t.Fatal("next quota advanced", c, err)
	}
	if c, err := e.Next(ctx, ReadBudget{1, 301}); !errors.Is(err, context.Canceled) || c.Data != nil || c.After != nil || c.Visited != 0 || capture() != before || s.poison != nil {
		t.Fatal("next cancel advanced", c, err)
	}
	var previous []byte
	visited, emitted, gaps, controlGaps := uint64(0), 0, 0, 0
	for pages := range 16 {
		budget := 4096
		if pages == 0 {
			budget = 301
		}
		c, err := e.Next(t.Context(), ReadBudget{1, budget})
		if err != nil || c.Sequence != uint64(pages) || c.Visited != 1 || bytes.Compare(c.After, previous) <= 0 {
			t.Fatal("cursor order/work", c, err)
		}
		previous = bytes.Clone(c.After)
		visited += c.Visited
		if len(c.Data) == 0 {
			gaps++
			if c.After[0] == 18 {
				controlGaps++
			}
			index, err := snapshotCursorIndex(c.After, m.Contract)
			if err != nil || index != 3 {
				t.Fatal("nonfuture gap", c, err)
			}
		} else {
			if err := walkSnapshotChunk(c.Data, m.Contract, func(k, raw []byte) error {
				emitted++
				index, err := snapshotCursorIndex(k, m.Contract)
				if err != nil || index > 2 {
					t.Fatal("future emitted", k, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		if c.Final {
			break
		}
	}
	if visited != 11 || emitted != 7 || gaps != 4 || controlGaps != 1 || !e.final {
		t.Fatal("literal complete work/set", visited, emitted, gaps, controlGaps, e.final)
	}
}
