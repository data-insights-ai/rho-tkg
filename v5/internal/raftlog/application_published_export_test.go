package raftlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestPublishedApplicationExportWaitsForBoundedManifest(t *testing.T) {
	s := publicationStore(t, vfs.NewMem())
	generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	generationApply(t, s, "new", KV{Key: []byte("a"), Value: []byte("new")})
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.Manifest(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := e.Next(t.Context(), ReadBudget{1, 4096}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	done, err := e.BuildManifest(t.Context(), ReadBudget{1, 4096})
	if err != nil || done {
		t.Fatal(done, err)
	}
	for !done {
		done, err = e.BuildManifest(t.Context(), ReadBudget{1, 4096})
		if err != nil {
			t.Fatal(err)
		}
	}
	m, err := e.Manifest()
	if err != nil || m.Version != 2 || m.CutID != cut.ID || m.Index != 2 || string(m.Image) != "old" {
		t.Fatal(m, err)
	}
	if done, err = e.BuildManifest(t.Context(), ReadBudget{1, 4096}); err != nil || !done {
		t.Fatal(done, err)
	}
}

func buildPublished(t *testing.T, e *ApplicationExport, b ReadBudget) ApplicationSnapshotManifest {
	t.Helper()
	for n := 0; n < 1000; n++ {
		done, err := e.BuildManifest(t.Context(), b)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			m, err := e.Manifest()
			if err != nil {
				t.Fatal(err)
			}
			return m
		}
	}
	t.Fatal("manifest never completed")
	return ApplicationSnapshotManifest{}
}
func collectPublished(t *testing.T, e *ApplicationExport, b ReadBudget) []ApplicationSnapshotChunk {
	t.Helper()
	var chunks []ApplicationSnapshotChunk
	for n := 0; n < 1000; n++ {
		c, err := e.Next(t.Context(), b)
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, c)
		if c.Final {
			return chunks
		}
	}
	t.Fatal("stream never completed")
	return nil
}

func TestPublishedApplicationExportReopenedCutSkipsAllFutureNamespaces(t *testing.T) {
	mem := vfs.NewMem()
	s := publicationStore(t, mem)
	generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")}, KV{Key: []byte("b"), Value: []byte("removed")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	generationApply(t, s, "new", KV{Key: []byte("a"), Value: []byte("new")}, KV{Key: []byte("b"), Deleted: true}, KV{Key: []byte("phantom"), Value: []byte("future")})
	p, tc := s.meta.App.Policy, s.meta.Transfer
	g, pc := s.meta.Gen.Limits, s.meta.Gen.Publication.Limits
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{Dir: "db", FS: mem, Application: p, Transfer: tc, Generations: g, PublishedCuts: pc})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	if m.Index != 2 || string(m.Image) != "old" || m.CutID != cut.ID {
		t.Fatal(m)
	}
	chunks := collectPublished(t, e, ReadBudget{1, 4096})
	state := snapshotVerifier{digest: snapshotSeed(m)}
	visited, emitted := uint64(0), 0
	empty := false
	var last []byte
	keys := map[string]string{}
	for seq, c := range chunks {
		if c.Sequence != uint64(seq) || c.Version != 2 || c.Visited > 1 || bytes.Compare(c.After, last) <= 0 || snapshotChunkV2FixedBytes+cap(c.Data)+cap(c.After) > 4096 {
			t.Fatal("progress/capacity", c)
		}
		if len(c.Data) == 0 {
			empty = true
			if c.Visited == 0 {
				t.Fatal("did not account future row")
			}
		}
		visited += c.Visited
		last = c.After
		if err := walkSnapshotChunk(c.Data, m.Contract, func(k, v []byte) error {
			emitted++
			var err error
			state, err = state.accept(m, k, v)
			if k[0] == appDataTag {
				key, index, x := decodeAppKey(k, m.Contract.MaxKeyBytes)
				if x != nil || index > 2 {
					t.Fatal("future leaked", k)
				}
				data, _, x := inspectAppFrame(k, v, m.Contract.MaxValueBytes)
				if x != nil {
					t.Fatal(x)
				}
				keys[string(key)] = string(data)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	if !empty || visited <= uint64(emitted) || !reflect.DeepEqual(keys, map[string]string{"a": "old", "b": "removed"}) {
		t.Fatal("exact historical set", keys, visited, emitted, empty)
	}
	if err := state.complete(m); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Next(t.Context(), ReadBudget{1, 4096}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	receiver := publicationStore(t, vfs.NewMem())
	beforeIndex, beforeImage, err := receiver.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	i, err := receiver.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range chunks {
		if err := i.Append(t.Context(), c); err != nil {
			t.Fatal(err)
		}
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if receiver.meta.Applied != 1 || receiver.meta.Gen.Active != 0 {
		t.Fatal("dormant data activated")
	}
	status, err := i.Status()
	if err != nil || !status.Verified {
		t.Fatal(status, err)
	}
	if err := i.Abort(); err != nil {
		t.Fatal(err)
	}
	index, image, err := receiver.Checkpoint()
	if err != nil || index != beforeIndex || !bytes.Equal(image, beforeImage) {
		t.Fatal(index, string(image), err)
	}
}

func TestPublishedApplicationExportBankReferenceSurvivesReplacementAndReopen(t *testing.T) {
	for _, retired := range []bool{false, true} {
		t.Run(fmt.Sprint(retired), func(t *testing.T) {
			mem := vfs.NewMem()
			s := publicationStore(t, mem)
			generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("original")})
			if err := s.PublishSnapshot(); err != nil {
				t.Fatal(err)
			}
			old, err := s.PublishedApplicationCut()
			if err != nil {
				t.Fatal(err)
			}
			if retired {
				generationFixtureBankB(t, s)
			}
			p, tc, g, pc := s.meta.App.Policy, s.meta.Transfer, s.meta.Gen.Limits, s.meta.Gen.Publication.Limits
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(Config{Dir: "db", FS: mem, Application: p, Transfer: tc, Generations: g, PublishedCuts: pc})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			e, err := s.BeginPublishedApplicationExport(t.Context(), old.ID)
			if err != nil {
				t.Fatal(err)
			}
			generationApply(t, s, "later", KV{Key: []byte("a"), Value: []byte("later")})
			if err := s.PublishSnapshot(); err != nil {
				t.Fatal(err)
			}
			before := readyMetadataFingerprint(t, s)
			if _, err := s.BeginPublishedApplicationExport(t.Context(), old.ID); !errors.Is(err, raft.ErrSnapOutOfDate) {
				t.Fatal(err)
			}
			if readyMetadataFingerprint(t, s) != before {
				t.Fatal("stale request mutated KV")
			}
			m := buildPublished(t, e, ReadBudget{2, 4096})
			if m.CutID != old.ID || string(m.Image) != "old" {
				t.Fatal(m)
			}
			var values []string
			for _, c := range collectPublished(t, e, ReadBudget{2, 4096}) {
				if err := walkSnapshotChunk(c.Data, m.Contract, func(k, v []byte) error {
					if k[0] == appDataTag {
						data, _, err := inspectAppFrame(k, v, m.Contract.MaxValueBytes)
						if err != nil {
							return err
						}
						values = append(values, string(data))
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(values, []string{"original"}) {
				t.Fatal("moving bank leaked", values)
			}
			if retired && s.meta.Gen.Banks[0].State != bankRetired {
				t.Fatal("old handle lost generation")
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
			if retired && s.meta.Gen.Banks[0].State != bankFree {
				t.Fatal("last old handle leaked bank")
			}
			current, err := s.PublishedApplicationCut()
			if err != nil {
				t.Fatal(err)
			}
			b, err := s.BeginPublishedApplicationExport(t.Context(), current.ID)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			bm := buildPublished(t, b, ReadBudget{2, 4096})
			if bm.Index != 3 || string(bm.Image) != "later" {
				t.Fatal(bm)
			}
			collectPublished(t, b, ReadBudget{2, 4096})
		})
	}
}

func TestPublishedApplicationExportAtomicBudgetsAndFinitePasses(t *testing.T) {
	s := publicationStore(t, vfs.NewMem())
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	// First root frame costs 8+9+36+7; header and three key copies also count.
	need := snapshotChunkV2FixedBytes + 2*(8+9+appFrameBytes+7) + 3*9
	initial := *e.published
	for _, b := range []ReadBudget{{0, 4096}, {1, 0}, {1, need - 1}, {4097, 4096}, {1, s.meta.Transfer.Limits.MaxChunkBytes + 1}} {
		want := ErrLimit
		if b.Rows == 0 || b.Bytes == 0 {
			want = ErrInvalid
		}
		if _, err := e.BuildManifest(t.Context(), b); !errors.Is(err, want) {
			t.Fatal(b, err)
		}
		if !reflect.DeepEqual(*e.published, initial) || len(e.after) != 0 {
			t.Fatal("refusal advanced build")
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := e.BuildManifest(canceled, ReadBudget{1, 4096}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if done, err := e.BuildManifest(t.Context(), ReadBudget{1, need}); err != nil || done {
		t.Fatal(done, err)
	}
	buildPublished(t, e, ReadBudget{1, 4096})
	before := readyMetadataFingerprint(t, s)
	sequence := e.sequence
	if _, err := e.Next(t.Context(), ReadBudget{1, need - 1}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if e.sequence != sequence || len(e.after) != 0 || readyMetadataFingerprint(t, s) != before {
		t.Fatal("replay refusal mutated state")
	}
	if _, err := e.Next(canceled, ReadBudget{1, 4096}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	collectPublished(t, e, ReadBudget{1, 4096})
	// A narrow private policy fixture checks independent build/replay counters.
	for _, pass := range []string{"build", "replay"} {
		t.Run(pass, func(t *testing.T) {
			x := publicationStore(t, vfs.NewMem())
			c, _ := x.PublishedApplicationCut()
			h, err := x.BeginPublishedApplicationExport(t.Context(), c.ID)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()
			if pass == "build" {
				x.meta.Gen.Publication.Limits.MaxTransferChunks = 1
				if _, err := h.BuildManifest(t.Context(), ReadBudget{1, 4096}); err != nil {
					t.Fatal(err)
				}
				cursor := bytes.Clone(h.after)
				if _, err := h.BuildManifest(t.Context(), ReadBudget{1, 4096}); !errors.Is(err, ErrLimit) {
					t.Fatal(err)
				}
				if !bytes.Equal(cursor, h.after) {
					t.Fatal("chunk cap advanced build")
				}
			}
			if pass == "replay" {
				buildPublished(t, h, ReadBudget{1, 4096})
				x.meta.Gen.Publication.Limits.MaxTransferChunks = 1
				if _, err := h.Next(t.Context(), ReadBudget{1, 4096}); err != nil {
					t.Fatal(err)
				}
				cursor := bytes.Clone(h.after)
				if _, err := h.Next(t.Context(), ReadBudget{1, 4096}); !errors.Is(err, ErrLimit) {
					t.Fatal(err)
				}
				if !bytes.Equal(cursor, h.after) {
					t.Fatal("chunk cap advanced replay")
				}
			}
		})
	}
}

type publishedCancelContext struct {
	context.Context
	remaining int
}

func (c *publishedCancelContext) Err() error {
	c.remaining--
	if c.remaining <= 0 {
		return context.Canceled
	}
	return nil
}

func TestPublishedApplicationExportOwnershipAndMidPageCancellation(t *testing.T) {
	var nilStore *Store
	var nilExport *ApplicationExport
	if _, err := nilStore.BeginPublishedApplicationExport(t.Context(), [32]byte{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := nilExport.BuildManifest(t.Context(), ReadBudget{1, 4096}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	s := publicationStore(t, vfs.NewMem())
	cut, _ := s.PublishedApplicationCut()
	//nolint:staticcheck // SA1012: exercise the nil-context error contract.
	if _, err := s.BeginPublishedApplicationExport(nil, cut.ID); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	legacy := generationStore(t, vfs.NewMem())
	if _, err := legacy.BeginPublishedApplicationExport(t.Context(), cut.ID); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	le, err := legacy.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer le.Close()
	if done, err := le.BuildManifest(t.Context(), ReadBudget{}); err != nil || !done {
		t.Fatal(done, err)
	}
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	//nolint:staticcheck // SA1012: exercise the nil-context error contract.
	if _, err := e.BuildManifest(nil, ReadBudget{1, 4096}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	state := *e.published
	ctx := &publishedCancelContext{Context: t.Context(), remaining: 4}
	if _, err := e.BuildManifest(ctx, ReadBudget{10, 4096}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*e.published, state) || e.after != nil || s.poison != nil {
		t.Fatal("partial canceled page published")
	}
	m := buildPublished(t, e, ReadBudget{1, 4096})
	m.Image[0] ^= 1
	m.ConfState.Voters[0] = 9
	again, err := e.Manifest()
	if err != nil || string(again.Image) != "initial" || again.ConfState.Voters[0] != 1 {
		t.Fatal("manifest aliases handle", again, err)
	}
	seq := e.sequence
	ctx = &publishedCancelContext{Context: t.Context(), remaining: 4}
	if _, err := e.Next(ctx, ReadBudget{10, 4096}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if seq != e.sequence || e.after != nil || s.poison != nil {
		t.Fatal("partial canceled replay published")
	}
	collectPublished(t, e, ReadBudget{1, 4096})
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.BuildManifest(t.Context(), ReadBudget{1, 4096}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := e.Manifest(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := e.Next(t.Context(), ReadBudget{1, 4096}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	fresh, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := fresh.BuildManifest(t.Context(), ReadBudget{1, 4096}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPublishedApplicationExportPreAdmitsOwnedCapacityBeforeImageCopy(t *testing.T) {
	s := publicationStore(t, vfs.NewMem())
	cut, _ := s.PublishedApplicationCut()
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	pin := e.pinBytes
	tc := s.meta.Transfer
	old := s.meta.Gen.Publication.Limits.MaxTransferChunks
	if pin <= s.meta.App.Bytes+s.meta.LogBytes+s.meta.ImageBytes+s.meta.SnapBytes+metadataBytes(s.meta) {
		t.Fatal("owned header/image/cursors omitted")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	s.meta.Transfer.Limits.MaxPinnedLogicalBytes = pin - 1
	if err := s.db.Set(snapshotKey, []byte("damaged"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	if _, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if before != readyMetadataFingerprint(t, s) || s.pinnedApplicationBytes != 0 || len(s.applicationExports) != 0 || s.poison != nil {
		t.Fatal("pin refusal changed store")
	}
	if err := s.db.Set(snapshotKey, []byte("initial"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	s.meta.Transfer.Limits.MaxPinnedLogicalBytes = pin
	e, err = s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal("exact pin limit", err)
	}
	if _, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID); !errors.Is(err, ErrLimit) {
		t.Fatal("cumulative quota", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	s.meta.Transfer = tc
	s.meta.Gen.Publication.Limits.MaxTransferChunks = old
	// With a dormant bank, its bytes and descriptor belong to the captured DB.
	donor := publicationStore(t, vfs.NewMem())
	generationApply(t, donor, "incoming", KV{Key: []byte("q"), Value: []byte("incoming")})
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	dc, _ := donor.PublishedApplicationCut()
	de, err := donor.BeginPublishedApplicationExport(t.Context(), dc.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer de.Close()
	dm := buildPublished(t, de, ReadBudget{1, 4096})
	imp, err := s.BeginApplicationImport(t.Context(), dm)
	if err != nil {
		t.Fatal(err)
	}
	defer imp.Abort()
	chunks := collectPublished(t, de, ReadBudget{1, 4096})
	if err := imp.Append(t.Context(), chunks[0]); err != nil {
		t.Fatal(err)
	}
	h, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	expected := pin + imp.state.bytes + uint64(len(imp.descriptor())+len(dormantKey))
	if h.pinBytes != expected {
		t.Fatal("inactive capacity miscounted", h.pinBytes, expected)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	s.meta.Transfer.Limits.MaxPinnedLogicalBytes = expected - 1
	before = readyMetadataFingerprint(t, s)
	if _, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if before != readyMetadataFingerprint(t, s) || s.applicationImport != imp {
		t.Fatal("quota altered dormant session")
	}
	s.meta.Transfer = tc
}

func TestPublishedApplicationExportCorruptionCannotBecomeReady(t *testing.T) {
	for _, fault := range []string{"image", "missing image", "frame", "missing envelope", "expected rows", "cut"} {
		t.Run(fault, func(t *testing.T) {
			s := publicationStore(t, vfs.NewMem())
			cut, _ := s.PublishedApplicationCut()
			if fault == "image" {
				if err := s.db.Set(snapshotKey, []byte("damaged"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "missing image" {
				if err := s.db.Delete(snapshotKey, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "frame" {
				k := bankIndexKey(s.activeBank(), appRootTag, 1)
				if err := s.db.Set(k, make([]byte, appFrameBytes), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "missing envelope" {
				if err := s.db.Delete(bankIndexKey(s.activeBank(), appOutcomeTag, 1), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
			if fault == "image" || fault == "missing image" {
				if !errors.Is(err, ErrCorrupt) || s.poison == nil {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal("capture performed a history scan", err)
			}
			defer e.Close()
			if fault == "expected rows" {
				e.published.expectedRows++
			}
			if fault == "cut" {
				e.manifest.CutID[0] ^= 1
			}
			_, err = e.BuildManifest(t.Context(), ReadBudget{10, 4096})
			if !errors.Is(err, ErrCorrupt) || e.published.ready || s.poison == nil {
				t.Fatal(err, e.published)
			}
			if _, err := e.Manifest(); !errors.Is(err, ErrCorrupt) {
				t.Fatal("corrupt handle readable", err)
			}
		})
	}
	for _, key := range [][]byte{nil, {appDataTag}, {appRootTag}, {appRootTag, 0, 0, 0, 0, 0, 0, 0, 0}, appIndexKey(appOutcomeTag, ^uint64(0))} {
		if _, err := snapshotCursorIndex(key, ApplicationContract{MaxKeyBytes: 10}); err == nil {
			t.Fatal("bad cursor", key)
		}
	}
}

func TestPublishedApplicationExportConcurrentReplacementAndClose(t *testing.T) {
	for _, closeStore := range []bool{false, true} {
		t.Run(fmt.Sprint(closeStore), func(t *testing.T) {
			s := publicationStore(t, vfs.NewMem())
			generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
			if err := s.PublishSnapshot(); err != nil {
				t.Fatal(err)
			}
			cut, _ := s.PublishedApplicationCut()
			e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			first := make(chan error, 1)
			resume := make(chan struct{})
			result := make(chan error, 1)
			go func() {
				_, err := e.BuildManifest(t.Context(), ReadBudget{1, 4096})
				first <- err
				<-resume
				for err == nil {
					var done bool
					done, err = e.BuildManifest(t.Context(), ReadBudget{1, 4096})
					if done {
						break
					}
				}
				if err == nil {
					for {
						var c ApplicationSnapshotChunk
						c, err = e.Next(t.Context(), ReadBudget{1, 4096})
						if err != nil || c.Final {
							break
						}
					}
				}
				result <- err
			}()
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			if closeStore {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				generationApply(t, s, "new", KV{Key: []byte("a"), Value: []byte("new")})
				if err := s.PublishSnapshot(); err != nil {
					t.Fatal(err)
				}
			}
			close(resume)
			err = <-result
			if closeStore {
				if !errors.Is(err, ErrClosed) {
					t.Fatal(err)
				}
				if s.pinnedApplicationBytes != 0 || e.published != nil || e.after != nil || e.manifest.ConfState != nil {
					t.Fatal("close leaked owned state")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				m, err := e.Manifest()
				if err != nil || m.CutID != cut.ID || string(m.Image) != "old" {
					t.Fatal(m, err)
				}
			}
		})
	}
}

func TestPublishedApplicationExportEmptyTerminalOwnsCursorBudget(t *testing.T) {
	s := publicationStore(t, vfs.NewMem())
	cut, _ := s.PublishedApplicationCut()
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	buildPublished(t, e, ReadBudget{1, 4096})
	collectPublished(t, e, ReadBudget{1, 4096})
	// Private page-boundary fixture: a transport may have deferred its final
	// marker after the last row. No new physical visit is required at this seam.
	e.final = false
	need := snapshotChunkV2FixedBytes + 4*len(e.after)
	seq, cursor := e.sequence, copyApplicationBytes(e.after)
	if _, err := e.Next(t.Context(), ReadBudget{1, need - 1}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if e.sequence != seq || !bytes.Equal(e.after, cursor) {
		t.Fatal("terminal quota advanced cursor")
	}
	c, err := e.Next(t.Context(), ReadBudget{1, need})
	if err != nil || !c.Final || c.Visited != 0 || c.VisitedBytes != 0 || len(c.Data) != 0 || c.Sequence != seq || !bytes.Equal(c.After, cursor) {
		t.Fatal(c, err)
	}
}

func TestPublishedApplicationExportFixedAllowancesCoverOwnedTypes(t *testing.T) {
	chunk := unsafe.Sizeof(ApplicationSnapshotChunk{})
	handle := unsafe.Sizeof(ApplicationExport{}) + unsafe.Sizeof(publishedExportState{}) + unsafe.Sizeof(pebble.Snapshot{}) + unsafe.Sizeof(generationRef{})
	conf := unsafe.Sizeof(pb.ConfState{})
	t.Logf("owned fixed sizes: chunk=%d handle+state+snapshot+ref=%d conf=%d", chunk, handle, conf)
	if chunk > snapshotChunkV2FixedBytes || handle > publishedExportFixedBytes || conf > 256 || snapshotChunkV2WireHeaderBytes >= snapshotChunkV2FixedBytes {
		t.Fatal("owned representation exceeds separate allowance", chunk, handle, conf)
	}
}
