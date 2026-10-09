package raftlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestPublishedApplicationManifestAS1DefaultsAndAS2Binding(t *testing.T) {
	legacy := fuzzSnapshotManifest()
	zero, err := EncodeApplicationSnapshotManifest(legacy)
	if err != nil {
		t.Fatal(err)
	}
	// Independently lay out the accepted AS1 wire (including fixed CS bytes).
	golden := []byte{'A', 'S', 1, 0}
	golden = append(golden, legacy.Identity.Graph[:]...)
	golden = binary.BigEndian.AppendUint64(golden, legacy.Identity.Partition)
	golden = append(golden, legacy.Identity.Group[:]...)
	for _, n := range legacy.Contract.numbers() {
		golden = binary.BigEndian.AppendUint64(golden, n)
	}
	for _, n := range []uint64{legacy.Index, legacy.Term} {
		golden = binary.BigEndian.AppendUint64(golden, n)
	}
	for _, ns := range [][4]uint64{legacy.NamespaceBytes, legacy.NamespaceRecords} {
		for _, n := range ns {
			golden = binary.BigEndian.AppendUint64(golden, n)
		}
	}
	golden = append(golden, legacy.ImageHash[:]...)
	golden = append(golden, legacy.RecordsHash[:]...)
	golden = binary.BigEndian.AppendUint32(golden, 2)
	golden = binary.BigEndian.AppendUint32(golden, uint32(len(legacy.Image)))
	golden = append(golden, 0x08, 1)
	golden = append(golden, legacy.Image...)
	if !bytes.Equal(zero, golden) {
		t.Fatalf("AS1 bytes changed: wire=%x golden=%x", zero, golden)
	}
	legacy.Version = 1
	one, err := EncodeApplicationSnapshotManifest(legacy)
	if err != nil || !bytes.Equal(zero, one) {
		t.Fatal("explicit AS1 differs", err)
	}
	decoded, err := DecodeApplicationSnapshotManifest(one, legacy.Contract.MaxImageBytes)
	legacy.Version = 0
	if err != nil || !reflect.DeepEqual(decoded, legacy) {
		t.Fatal("AS1 not normalized", decoded, err)
	}
	for _, version := range []uint32{0, 1} {
		x := legacy
		x.Version = version
		x.CutID[0] = 1
		if _, err := EncodeApplicationSnapshotManifest(x); !errors.Is(err, ErrInvalid) {
			t.Fatal("silent AS1 cut loss", err)
		}
	}
	s := publicationStore(t, vfs.NewMem())
	cut, _ := s.PublishedApplicationCut()
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	wire, err := EncodeApplicationSnapshotManifest(m)
	if err != nil || wire[2] != 2 || cap(wire) != len(wire) {
		t.Fatal("AS2 capacity", err, len(wire), cap(wire))
	}
	round, err := DecodeApplicationSnapshotManifest(wire, m.Contract.MaxImageBytes)
	if err != nil || !reflect.DeepEqual(round, m) {
		t.Fatal(round, err)
	}
	id, err := manifestID(round)
	if err != nil || id != e.published.manifestID {
		t.Fatal(id, err)
	}
	if err := validateManifest(m); err != nil {
		t.Fatal(err)
	}
	bad := m
	bad.CutID[0] ^= 1
	if _, err := EncodeApplicationSnapshotManifest(bad); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	bad = m
	bad.Version = 3
	if _, err := EncodeApplicationSnapshotManifest(bad); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for n := 0; n < len(wire); n++ {
		if _, err := DecodeApplicationSnapshotManifest(wire[:n], m.Contract.MaxImageBytes); !errors.Is(err, ErrCorrupt) {
			t.Fatal(n, err)
		}
	}
	changed := bytes.Clone(wire)
	changed[snapshotManifestOverhead] ^= 1
	if _, err := DecodeApplicationSnapshotManifest(changed, m.Contract.MaxImageBytes); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	bad = m
	bad.NamespaceBytes[0] = 1 << 41
	if _, err := manifestID(bad); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestPublishedApplicationImportProgressRefusalsAreAtomic(t *testing.T) {
	src := publicationStore(t, vfs.NewMem())
	generationApply(t, src, "old", KV{Key: []byte("a"), Value: []byte("old")})
	if err := src.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, _ := src.PublishedApplicationCut()
	generationApply(t, src, "future", KV{Key: []byte("z"), Value: []byte("future")})
	e, err := src.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	chunks := collectPublished(t, e, ReadBudget{1, 4096})
	first := chunks[0]
	for _, plain := range []*Store{transferStore(t, "legacy", vfs.NewMem(), 1), generationStore(t, vfs.NewMem())} {
		before := readyMetadataFingerprint(t, plain)
		if _, err := plain.BeginApplicationImport(t.Context(), m); !errors.Is(err, ErrInvalid) {
			t.Fatal("missing persisted cap", err)
		}
		if before != readyMetadataFingerprint(t, plain) {
			t.Fatal("policy refusal wrote")
		}
	}
	dst := publicationStore(t, vfs.NewMem())
	i, err := dst.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Abort()
	cases := []struct {
		name   string
		mutate func(*ApplicationSnapshotChunk)
		want   error
	}{
		{"version", func(c *ApplicationSnapshotChunk) { c.Version = 0 }, ErrInvalid},
		{"cut", func(c *ApplicationSnapshotChunk) { c.CutID[0] ^= 1 }, ErrInvalid},
		{"manifest", func(c *ApplicationSnapshotChunk) { c.ManifestID[0] ^= 1 }, ErrInvalid},
		{"sequence", func(c *ApplicationSnapshotChunk) { c.Sequence++ }, ErrInvalid},
		{"no cursor", func(c *ApplicationSnapshotChunk) { c.After = nil }, ErrInvalid},
		{"wrong cursor", func(c *ApplicationSnapshotChunk) { c.After = []byte{17} }, ErrCorrupt},
		{"oversized cursor", func(c *ApplicationSnapshotChunk) { c.After = make([]byte, 2*m.Contract.MaxKeyBytes+12) }, ErrLimit},
		{"zero visits", func(c *ApplicationSnapshotChunk) { c.Visited = 0 }, ErrInvalid},
		{"too many visits", func(c *ApplicationSnapshotChunk) { c.Visited = uint64(dst.meta.Transfer.Limits.MaxChunkRows + 1) }, ErrLimit},
		{"no work", func(c *ApplicationSnapshotChunk) { c.VisitedBytes = 0 }, ErrInvalid},
		{"too much work", func(c *ApplicationSnapshotChunk) { c.VisitedBytes = uint64(dst.meta.Transfer.Limits.MaxChunkBytes + 1) }, ErrLimit},
		{"empty nonfinal", func(c *ApplicationSnapshotChunk) { c.Data = nil; c.Visited = 0; c.VisitedBytes = 0 }, ErrInvalid},
		{"premature final", func(c *ApplicationSnapshotChunk) { c.Final = true }, ErrCorrupt},
		{"truncation", func(c *ApplicationSnapshotChunk) { c.Data = c.Data[:len(c.Data)-1] }, ErrCorrupt},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := first
			c.Data = copyApplicationBytes(c.Data)
			c.After = copyApplicationBytes(c.After)
			tc.mutate(&c)
			before := readyMetadataFingerprint(t, dst)
			status, _ := i.Status()
			if err := i.Append(t.Context(), c); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
			after, _ := i.Status()
			if before != readyMetadataFingerprint(t, dst) || !reflect.DeepEqual(status, after) || dst.poison != nil {
				t.Fatal("refusal mutated or poisoned")
			}
		})
	}
	// Replay the exact valid stream after every refusal. Force a page boundary
	// before the final marker: zero newly visited rows with unchanged After is legal.
	for j, c := range chunks {
		if j == len(chunks)-1 {
			c.Final = false
		}
		if err := i.Append(t.Context(), c); err != nil {
			t.Fatal(j, err)
		}
	}
	last := chunks[len(chunks)-1]
	terminal := ApplicationSnapshotChunk{Version: 2, CutID: m.CutID, ManifestID: last.ManifestID, Sequence: uint64(len(chunks)), After: copyApplicationBytes(last.After), Final: true}
	before := readyMetadataFingerprint(t, dst)
	bad := terminal
	bad.After = append(copyApplicationBytes(bad.After), 0)
	if err := i.Append(t.Context(), bad); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if before != readyMetadataFingerprint(t, dst) {
		t.Fatal("bad terminal wrote")
	}
	if err := i.Append(t.Context(), terminal); err != nil {
		t.Fatal(err)
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := i.Append(t.Context(), terminal); !errors.Is(err, ErrInvalid) {
		t.Fatal("trailing page", err)
	}
	if dst.meta.Applied != 1 {
		t.Fatal("activated")
	}
}

func TestPublishedApplicationImportAS1NeverDropsAS2Fields(t *testing.T) {
	s := transferStore(t, "legacy", vfs.NewMem(), 1)
	m := fuzzSnapshotManifest()
	i, err := s.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Abort()
	for _, version := range []uint32{0, 1} {
		for _, mutate := range []func(*ApplicationSnapshotChunk){func(c *ApplicationSnapshotChunk) { c.CutID[0] = 1 }, func(c *ApplicationSnapshotChunk) { c.ManifestID[0] = 1 }, func(c *ApplicationSnapshotChunk) { c.After = []byte{8} }, func(c *ApplicationSnapshotChunk) { c.Visited = 1 }, func(c *ApplicationSnapshotChunk) { c.VisitedBytes = 1 }, func(c *ApplicationSnapshotChunk) { c.Version = 2 }} {
			c := ApplicationSnapshotChunk{Version: version, Final: true}
			mutate(&c)
			before := readyMetadataFingerprint(t, s)
			if err := i.Append(t.Context(), c); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
			if before != readyMetadataFingerprint(t, s) {
				t.Fatal("AS1 refusal wrote")
			}
		}
	}
}

func FuzzPublishedApplicationManifest(f *testing.F) {
	m := fuzzSnapshotManifest()
	m.Version = 2
	b, r, err := snapshotTotals(m)
	if err != nil {
		f.Fatal(err)
	}
	m.CutID, err = cutID(ApplicationCutReference{Identity: m.Identity, Contract: m.Contract, Index: m.Index, Term: m.Term, ConfState: m.ConfState, ImageBytes: uint64(len(m.Image)), ImageHash: m.ImageHash, RetainedBytes: b, RetainedRecords: r})
	if err != nil {
		f.Fatal(err)
	}
	wire, err := EncodeApplicationSnapshotManifest(m)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(wire)
	f.Add(wire[:snapshotManifestOverhead])
	f.Add([]byte("AS2"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		decoded, err := DecodeApplicationSnapshotManifest(data, 64<<10)
		if err != nil {
			return
		}
		encoded, err := EncodeApplicationSnapshotManifest(decoded)
		if err != nil {
			t.Fatal(err)
		}
		again, err := DecodeApplicationSnapshotManifest(encoded, 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := manifestID(decoded)
		b, _ := manifestID(again)
		if a != b || sha256.Sum256(decoded.Image) != again.ImageHash {
			t.Fatal("roundtrip changed cut")
		}
	})
}

func TestPublishedApplicationImportFiniteSequenceAndMinimumEmittedWork(t *testing.T) {
	src := publicationStore(t, vfs.NewMem())
	cut, _ := src.PublishedApplicationCut()
	e, err := src.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{3, 4096})
	chunks := collectPublished(t, e, ReadBudget{2, 4096})
	dst := publicationStore(t, vfs.NewMem())
	i, err := dst.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Abort()
	c := chunks[0]
	c.Visited = 1
	before := readyMetadataFingerprint(t, dst)
	if err := i.Append(t.Context(), c); !errors.Is(err, ErrInvalid) {
		t.Fatal("emitted visits not bounded", err)
	}
	c = chunks[0]
	c.VisitedBytes = uint64(len(c.Data) - 1)
	if err := i.Append(t.Context(), c); !errors.Is(err, ErrInvalid) {
		t.Fatal("emitted bytes not bounded", err)
	}
	if before != readyMetadataFingerprint(t, dst) {
		t.Fatal("minimum work refusal wrote")
	}
	limit := dst.meta.Gen.Publication.Limits.MaxTransferChunks
	dst.meta.Gen.Publication.Limits.MaxTransferChunks = 1
	if err := i.Append(t.Context(), chunks[0]); err != nil {
		t.Fatal(err)
	}
	before = readyMetadataFingerprint(t, dst)
	cursor := copyApplicationBytes(i.after)
	if err := i.Append(t.Context(), chunks[1]); !errors.Is(err, ErrLimit) {
		t.Fatal("no finite receiver limit", err)
	}
	if before != readyMetadataFingerprint(t, dst) || !bytes.Equal(cursor, i.after) || dst.poison != nil {
		t.Fatal("counter refusal mutated")
	}
	dst.meta.Gen.Publication.Limits.MaxTransferChunks = limit
	if err := i.Append(t.Context(), chunks[1]); err != nil {
		t.Fatal("counter refusal not retryable", err)
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPublishedApplicationImportStoreCloseReleasesAS2OwnedCursorAndManifest(t *testing.T) {
	src := publicationStore(t, vfs.NewMem())
	cut, _ := src.PublishedApplicationCut()
	e, err := src.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	c, err := e.Next(t.Context(), ReadBudget{1, 4096})
	if err != nil {
		t.Fatal(err)
	}
	dst := publicationStore(t, vfs.NewMem())
	i, err := dst.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Append(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if len(i.after) == 0 || i.manifest.ConfState == nil {
		t.Fatal("fixture not owning AS2 state")
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	if i.after != nil || i.state.last != nil || i.manifest.ConfState != nil || i.manifest.Image != nil || i.ref != nil {
		t.Fatal("Store.Close retained owned AS2 state")
	}
	if err := i.Abort(); err != nil {
		t.Fatal(err)
	}
}
