package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func transferConfig(voter uint64) (ApplicationPolicy, ApplicationTransferConfig) {
	p := DefaultApplicationPolicy(voter)
	return p, ApplicationTransferConfig{
		Identity: ApplicationIdentity{Graph: [16]byte{1}, Partition: 7, Group: [16]byte{3}},
		Contract: ApplicationContractForPolicy(p), Limits: DefaultApplicationTransferLimits(),
	}
}
func transferStore(t *testing.T, dir string, fs vfs.FS, voter uint64) *Store {
	t.Helper()
	p, tc := transferConfig(voter)
	s, err := Open(Config{Dir: dir, FS: fs, Create: true, Application: p, Transfer: tc})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{voter}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	return s
}
func exportChunks(t *testing.T, e *ApplicationExport) []ApplicationSnapshotChunk {
	t.Helper()
	var chunks []ApplicationSnapshotChunk
	for {
		c, err := e.Next(t.Context(), ReadBudget{Rows: 2, Bytes: 512})
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, c)
		if c.Final {
			return chunks
		}
	}
}
func TestSnapshotRetainsHistoryExcludesLaterKeysAndNeverActivates(t *testing.T) {
	fs := vfs.NewCrashableMem()
	src := transferStore(t, "source", fs, 1)
	applyApplication(t, src, "X", KV{Key: []byte("a"), Value: []byte("old")}, KV{Key: []byte("b"), Value: []byte{}})
	applyApplication(t, src, "Y", KV{Key: []byte("a"), Deleted: true})
	if err := src.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	e, err := src.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m, err := e.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	applyApplication(t, src, "later", KV{Key: []byte("z"), Value: []byte("exclude")})
	chunks := exportChunks(t, e)
	dst := transferStore(t, "destination", fs, 3)
	before, _ := encodeMeta(dst.meta)
	imp, err := dst.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range chunks {
		if err := imp.Append(t.Context(), c); err != nil {
			t.Fatal(err)
		}
	}
	if err := imp.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	status, err := imp.Status()
	if err != nil || !status.Verified || status.Records != 12 {
		t.Fatal(status, err)
	}
	after, _ := encodeMeta(dst.meta)
	if !bytes.Equal(before, after) {
		t.Fatal("dormant transfer changed active metadata")
	}
	getApplication(t, viewApplication(t, dst, 1), "a", "", false, false)
	if err := dst.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
	var versions, roots, changes, outcomes int
	for _, c := range chunks {
		err := walkSnapshotChunk(c.Data, m.Contract, func(k, v []byte) error {
			switch k[0] {
			case appDataTag:
				key, index, err := decodeAppKey(k, m.Contract.MaxKeyBytes)
				value, deleted, frameErr := inspectAppFrame(k, v, m.Contract.MaxValueBytes)
				if err != nil || frameErr != nil {
					t.Fatal(err, frameErr)
				}
				switch {
				case string(key) == "a" && index == 2:
					if deleted || string(value) != "old" {
						t.Fatal("old version lost", index, string(value), deleted)
					}
				case string(key) == "a" && index == 3:
					if !deleted || len(value) != 0 {
						t.Fatal("tombstone lost")
					}
				case string(key) == "b" && index == 2:
					if deleted || len(value) != 0 {
						t.Fatal("empty present changed")
					}
				default:
					t.Fatal("unexpected version", string(key), index)
				}
				versions++
				if bytes.Contains(k, []byte("z")) {
					t.Fatal("later key exported")
				}
			case appRootTag:
				roots++
			case appChangeTag:
				changes++
			case appOutcomeTag:
				outcomes++
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if versions != 3 || roots != 3 || changes != 3 || outcomes != 3 {
		t.Fatal(versions, roots, changes, outcomes)
	}
	assertSnapshotIndependentRows(t, dst, m, chunks)
	if err := imp.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := imp.Abort(); err != nil {
		t.Fatal(err)
	}
}
func TestSnapshotRejectsMisboundMalformedAndCancelledTransfers(t *testing.T) {
	fs := vfs.NewMem()
	src := transferStore(t, "source", fs, 1)
	applyApplication(t, src, "X", KV{Key: []byte("a"), Value: []byte("old")})
	e, err := src.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m, _ := e.Manifest()
	chunks := exportChunks(t, e)
	dst := transferStore(t, "destination", fs, 1)
	wrong := m
	wrong.Identity.Group[0]++
	if _, err := dst.BeginApplicationImport(t.Context(), wrong); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ApplicationSnapshotChunk){
		func(c *ApplicationSnapshotChunk) { c.Sequence++ },
		func(c *ApplicationSnapshotChunk) { c.Data = c.Data[:len(c.Data)-1] },
		func(c *ApplicationSnapshotChunk) { c.Data = append(bytes.Clone(c.Data), 1) },
		func(c *ApplicationSnapshotChunk) { c.Data = bytes.Clone(c.Data); c.Data[len(c.Data)-1] ^= 1 },
		func(c *ApplicationSnapshotChunk) { c.Data = []byte{255, 255, 255, 255, 255, 255, 255, 255} },
	} {
		imp, err := dst.BeginApplicationImport(t.Context(), m)
		if err != nil {
			t.Fatal(err)
		}
		bad := chunks[0]
		mutate(&bad)
		if err := imp.Append(t.Context(), bad); !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrLimit) && !errors.Is(err, ErrInvalid) {
			t.Fatal("malformed accepted", err)
		}
		if err := imp.Abort(); err != nil {
			t.Fatal(err)
		}
		if err := dst.ScrubApplication(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := dst.BeginApplicationImport(ctx, m); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	imp, err := dst.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer imp.Abort()
	if err := imp.Append(ctx, chunks[0]); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := imp.Verify(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, c := range chunks {
		if err := imp.Append(t.Context(), c); err != nil {
			t.Fatal(err)
		}
	}
	if err := imp.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotManifestCodecAndPolicyBinding(t *testing.T) {
	fs := vfs.NewCrashableMem()
	s := transferStore(t, "source", fs, 1)
	e, err := s.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m, _ := e.Manifest()
	wire, err := EncodeApplicationSnapshotManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeApplicationSnapshotManifest(wire, m.Contract.MaxImageBytes)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := manifestID(m)
	b, _ := manifestID(decoded)
	if a != b {
		t.Fatal("codec changed binding")
	}
	decoded.Image[0] ^= 1
	if bytes.Equal(decoded.Image, m.Image) {
		t.Fatal("image aliases")
	}
	for j := range len(wire) {
		if _, err := DecodeApplicationSnapshotManifest(wire[:j], m.Contract.MaxImageBytes); err == nil {
			t.Fatal("truncation accepted", j)
		}
	}
	if _, err := DecodeApplicationSnapshotManifest(append(bytes.Clone(wire), 0), m.Contract.MaxImageBytes); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := DecodeApplicationSnapshotManifest(wire, 1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	p, tc := transferConfig(1)
	for _, edit := range []func(*ApplicationTransferConfig){func(c *ApplicationTransferConfig) { c.Identity.Group[0]++ }, func(c *ApplicationTransferConfig) { c.Limits.MaxExports++ }, func(c *ApplicationTransferConfig) { c.Contract.Version++ }} {
		wrong := tc
		edit(&wrong)
		reopened, err := Open(Config{Dir: "source", FS: fs.CrashClone(vfs.CrashCloneCfg{}), Application: p, Transfer: wrong})
		if reopened != nil {
			_ = reopened.Close()
		}
		if !errors.Is(err, ErrInvalid) {
			t.Fatal("wrong binding/policy reopened", err)
		}
	}
	unbound := openApplication(t, vfs.NewCrashableMem(), p)
	if _, err := unbound.BeginApplicationExport(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	scalar, err := Open(Config{Dir: "scalar", FS: vfs.NewMem(), Create: true, Transfer: tc})
	if scalar != nil {
		_ = scalar.Close()
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestSnapshotSessionLimitsRetryDigestAndDormantReopen(t *testing.T) {
	fs := vfs.NewCrashableMem()
	src := transferStore(t, "source", fs, 1)
	applyApplication(t, src, "X", KV{Key: []byte("a"), Value: []byte("old")})
	e, err := src.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m, _ := e.Manifest()
	chunks := exportChunks(t, e)
	dst := transferStore(t, "destination", fs, 3)
	for _, mode := range []string{"partial", "final", "verified"} {
		imp, err := dst.BeginApplicationImport(t.Context(), m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := dst.BeginApplicationImport(t.Context(), m); !errors.Is(err, ErrLimit) {
			t.Fatal("unbounded import sessions", err)
		}
		if err := imp.Append(t.Context(), chunks[0]); err != nil {
			t.Fatal(err)
		}
		if err := imp.Append(t.Context(), chunks[0]); !errors.Is(err, ErrInvalid) {
			t.Fatal("duplicate accepted", err)
		}
		if mode != "partial" {
			for _, c := range chunks[1:] {
				if err := imp.Append(t.Context(), c); err != nil {
					t.Fatal(err)
				}
			}
		}
		if mode == "verified" {
			if err := imp.Verify(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := imp.Verify(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		clone := fs.CrashClone(vfs.CrashCloneCfg{})
		p, tc := transferConfig(3)
		recovered, err := Open(Config{Dir: "destination", FS: clone, Application: p, Transfer: tc})
		if err != nil {
			t.Fatal(err)
		}
		usage, err := recovered.ApplicationTransferUsage()
		if err != nil || usage.Import || usage.StagedBytes != 0 {
			t.Fatal(usage, err)
		}
		it, err := recovered.db.NewIter(nil)
		if err != nil {
			t.Fatal(err)
		}
		for valid := it.SeekGE([]byte{12}); valid; valid = it.Next() {
			if it.Key()[0] < 17 {
				t.Fatal("dormant data survived reopen")
			}
		}
		if err := it.Close(); err != nil {
			t.Fatal(err)
		}
		getApplication(t, viewApplication(t, recovered, 1), "a", "", false, false)
		if err := recovered.ScrubApplication(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := recovered.Close(); err != nil {
			t.Fatal(err)
		}
		if err := imp.Abort(); err != nil {
			t.Fatal(err)
		}
	}
	wrong := m
	wrong.RecordsHash[0] ^= 1
	imp, err := dst.BeginApplicationImport(t.Context(), wrong)
	if err != nil {
		t.Fatal(err)
	}
	for j, c := range chunks {
		err := imp.Append(t.Context(), c)
		if j == len(chunks)-1 {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal("wrong final hash accepted", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if err := imp.Abort(); err != nil {
		t.Fatal(err)
	}
	usage, err := dst.ApplicationTransferUsage()
	if err != nil || usage.Import {
		t.Fatal(usage, err)
	}
	second, err := src.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.BeginApplicationExport(t.Context()); !errors.Is(err, ErrLimit) {
		t.Fatal("unbounded export handles", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Manifest(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := e.Next(t.Context(), ReadBudget{1, 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal("post final accepted", err)
	}
}

func TestSnapshotOrderAndMissingEnvelopeNeverWritePartialChunk(t *testing.T) {
	fs := vfs.NewMem()
	src := transferStore(t, "source", fs, 1)
	applyApplication(t, src, "X", KV{Key: []byte("a"), Value: []byte("x")}, KV{Key: []byte("b"), Value: []byte("y")})
	e, err := src.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m, _ := e.Manifest()
	chunk, err := e.Next(t.Context(), ReadBudget{4096, 4 << 20})
	if err != nil || !chunk.Final {
		t.Fatal(err)
	}
	var frames [][]byte
	if err := walkSnapshotChunk(chunk.Data, m.Contract, func(k, v []byte) error { frames = append(frames, appendSnapshotFrame(nil, k, v)); return nil }); err != nil {
		t.Fatal(err)
	}
	dst := transferStore(t, "destination", fs, 1)
	for _, variant := range []string{"reorder", "duplicate", "omit", "early-final"} {
		bad := chunk
		bad.Data = nil
		for j, f := range frames {
			switch variant {
			case "reorder":
				if j == 0 {
					f = frames[1]
				}
				if j == 1 {
					f = frames[0]
				}
			case "duplicate":
				if j == 1 {
					f = frames[0]
				}
			case "omit":
				if j == len(frames)-1 {
					continue
				}
			case "early-final":
				if j > 0 {
					continue
				}
			}
			bad.Data = append(bad.Data, f...)
		}
		imp, err := dst.BeginApplicationImport(t.Context(), m)
		if err != nil {
			t.Fatal(err)
		}
		if err := imp.Append(t.Context(), bad); !errors.Is(err, ErrCorrupt) {
			t.Fatal(variant, err)
		}
		status, _ := imp.Status()
		if status.Bytes != 0 || status.Records != 0 || status.NextSequence != 0 {
			t.Fatal("failed page partly applied", status)
		}
		if err := imp.Append(t.Context(), chunk); err != nil {
			t.Fatal("valid retry failed", err)
		}
		if err := imp.Verify(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := imp.Abort(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSnapshotBoundedBudgetsNilAndClosedHandles(t *testing.T) {
	fs := vfs.NewMem()
	p, tc := transferConfig(1)
	tc.Limits.MaxStagedBytes = 1
	small, err := Open(Config{Dir: "small", FS: fs, Create: true, Application: p, Transfer: tc})
	if err != nil {
		t.Fatal(err)
	}
	defer small.Close()
	if err := small.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	src := transferStore(t, "source", fs, 1)
	e, err := src.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	m, _ := e.Manifest()
	if _, err := small.BeginApplicationImport(t.Context(), m); !errors.Is(err, ErrLimit) {
		t.Fatal("quota ignored", err)
	}
	for _, b := range []ReadBudget{{0, 1}, {1, 0}, {1, 1}, {4097, 100}, {1, 33 << 20}} {
		if _, err := e.Next(t.Context(), b); !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrLimit) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := e.Next(ctx, ReadBudget{1, 256}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := src.BeginApplicationExport(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var nilStore *Store
	var nilExport *ApplicationExport
	var nilImport *ApplicationImport
	if _, err := nilStore.BeginApplicationExport(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := nilStore.BeginApplicationImport(t.Context(), m); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := nilStore.ApplicationTransferUsage(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := nilExport.Manifest(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := nilExport.Next(t.Context(), ReadBudget{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := nilExport.Close(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := nilImport.Append(t.Context(), ApplicationSnapshotChunk{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := nilImport.Verify(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := nilImport.Status(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := nilImport.Abort(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	//nolint:staticcheck // Deliberate nil-context trust-boundary test.
	if _, err := src.BeginApplicationExport(nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	//nolint:staticcheck // Deliberate nil-context trust-boundary test.
	if _, err := src.BeginApplicationImport(nil, m); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := DecodeApplicationSnapshotManifest(nil, 0); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := EncodeApplicationSnapshotManifest(ApplicationSnapshotManifest{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	imp, err := src.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Manifest(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := imp.Status(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := imp.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := src.ApplicationTransferUsage(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestSnapshotCorruptSourceAndDormantBankStaySeparate(t *testing.T) {
	for _, mode := range []string{"image", "entry", "records", "frame", "missing", "ledger", "large"} {
		t.Run(mode, func(t *testing.T) {
			s := transferStore(t, "source", vfs.NewMem(), 1)
			applyApplication(t, s, "X", KV{Key: []byte("a"), Value: []byte("old")})
			var err error
			switch mode {
			case "image":
				err = s.db.Set(imageKey, []byte("bad"), pebble.Sync)
			case "entry":
				err = s.db.Delete(entryKey(2), pebble.Sync)
			case "records":
				s.meta.App.Records = 1
			case "frame":
				err = s.db.Set(appVersionKey([]byte("a"), 2), []byte("bad"), pebble.Sync)
			case "missing":
				err = s.db.Delete(appVersionKey([]byte("a"), 2), pebble.Sync)
			case "ledger":
				s.meta.App.Bytes++
			case "large":
				err = s.db.Set(appIndexKey(appRootTag, 1), make([]byte, s.meta.App.Policy.MaxValueBytes+appFrameBytes+1), pebble.Sync)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.BeginApplicationExport(t.Context()); !errors.Is(err, ErrCorrupt) {
				t.Fatal("corrupt source exported", err)
			}
			if len(s.applicationExports) != 0 || s.pinnedApplicationBytes != 0 {
				t.Fatal("failed source leaked export")
			}
		})
	}
	for _, mode := range []string{"frame", "missing", "root"} {
		t.Run("dormant-"+mode, func(t *testing.T) {
			s := transferStore(t, "source", vfs.NewMem(), 1)
			e, err := s.BeginApplicationExport(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			m, _ := e.Manifest()
			chunks := exportChunks(t, e)
			imp, err := s.BeginApplicationImport(t.Context(), m)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range chunks {
				if err := imp.Append(t.Context(), c); err != nil {
					t.Fatal(err)
				}
			}
			key := appIndexKey(appRootTag+4, 1)
			switch mode {
			case "frame":
				err = s.db.Set(key, []byte("bad"), pebble.Sync)
			case "missing":
				err = s.db.Delete(key, pebble.Sync)
			case "root":
				err = s.db.Set(key, appFrame(key, []byte("wrong"), false), pebble.Sync)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := imp.Verify(t.Context()); !errors.Is(err, ErrCorrupt) {
				t.Fatal("corrupt dormant accepted", err)
			}
			if err := s.ScrubApplication(t.Context()); err != nil {
				t.Fatal("dormant corruption poisoned active", err)
			}
			if err := imp.Abort(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotLocalBoundsAndOwnedCapacity(t *testing.T) {
	p, tc := transferConfig(1)
	tc.Limits.MaxPinnedLogicalBytes = 1
	s, err := Open(Config{Dir: "pin", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginApplicationExport(t.Context()); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	uninitialized, err := Open(Config{Dir: "uninitialized", FS: vfs.NewMem(), Create: true, Application: p, Transfer: func() ApplicationTransferConfig { _, c := transferConfig(1); return c }()})
	if err != nil {
		t.Fatal(err)
	}
	defer uninitialized.Close()
	if _, err := uninitialized.BeginApplicationExport(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	large := transferStore(t, "large", vfs.NewMem(), 1)
	applyApplication(t, large, "X", KV{Key: []byte("a"), Value: make([]byte, 8192)})
	e, err := large.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	chunk, err := e.Next(t.Context(), ReadBudget{4096, 16 << 10})
	if err != nil || !chunk.Final || cap(chunk.Data) != len(chunk.Data) || cap(chunk.Data) > 16<<10 {
		t.Fatal(len(chunk.Data), cap(chunk.Data), err)
	}
	u, err := large.ApplicationTransferUsage()
	if err != nil || u.Exports != 1 || u.ExportImageBytes != 1 || u.PinnedLogicalBytes == 0 {
		t.Fatal(u, err)
	}
	m, _ := e.Manifest()
	i, err := large.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Abort()
	if err := i.Append(t.Context(), chunk); err != nil {
		t.Fatal(err)
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	u, err = large.ApplicationTransferUsage()
	if err != nil || !u.Import || !u.Verified || u.StagedBytes != u.ActiveBytes || u.StagedRecords != u.ActiveRecords {
		t.Fatal(u, err)
	}
	for _, edit := range []func(*ApplicationTransferConfig){func(c *ApplicationTransferConfig) { c.Identity.Graph = [16]byte{} }, func(c *ApplicationTransferConfig) { c.Limits.MaxChunkRows = 0 }, func(c *ApplicationTransferConfig) { c.Contract.MaxValueBytes = 0 }, func(c *ApplicationTransferConfig) { c.Contract.MaxPageRows = 4097 }} {
		_, c := transferConfig(1)
		edit(&c)
		if err := c.validate(p); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := openApplication(t, vfs.NewMem(), p).ApplicationTransferUsage(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestSnapshotCodecRefusesInvalidMetadataAndHostileLengths(t *testing.T) {
	m := fuzzSnapshotManifest()
	for _, edit := range []func(*ApplicationSnapshotManifest){
		func(m *ApplicationSnapshotManifest) { m.Identity.Partition = 0 }, func(m *ApplicationSnapshotManifest) { m.Contract.Version = 2 }, func(m *ApplicationSnapshotManifest) { m.Contract.MaxKeyBytes = -1 }, func(m *ApplicationSnapshotManifest) { m.Contract.MaxValueBytes = 17 << 20 },
		func(m *ApplicationSnapshotManifest) { m.Index = 0 }, func(m *ApplicationSnapshotManifest) { m.Index = ^uint64(0) - 2 }, func(m *ApplicationSnapshotManifest) { m.Term = 0 }, func(m *ApplicationSnapshotManifest) { m.ConfState = nil }, func(m *ApplicationSnapshotManifest) { m.ConfState = &pb.ConfState{Voters: []uint64{2, 1}} }, func(m *ApplicationSnapshotManifest) { m.ConfState = &pb.ConfState{Voters: []uint64{1, 1}} }, func(m *ApplicationSnapshotManifest) { m.ImageHash[0] ^= 1 }, func(m *ApplicationSnapshotManifest) { m.Image = make([]byte, m.Contract.MaxImageBytes+1) }, func(m *ApplicationSnapshotManifest) { m.NamespaceRecords[2]++ }, func(m *ApplicationSnapshotManifest) { m.NamespaceBytes[3] = 0 }, func(m *ApplicationSnapshotManifest) { m.NamespaceBytes[0] = ^uint64(0) },
	} {
		bad := cloneSnapshotManifest(m)
		edit(&bad)
		if _, err := EncodeApplicationSnapshotManifest(bad); err == nil {
			t.Fatal("invalid metadata encoded")
		}
	}
	_, tc := transferConfig(1)
	encoded := appendTransferConfig(nil, tc)
	for _, offset := range []int{0, 44, 52, 124, 132, 140, 148, 156, 164} {
		bad := bytes.Clone(encoded)
		for j := 0; j < 8 && offset+j < len(bad); j++ {
			bad[offset+j] = 255
		}
		if _, _, err := decodeTransferConfig(bad); err == nil {
			t.Fatal("invalid durable transfer metadata decoded", offset)
		}
	}
	for j := range len(encoded) {
		if _, _, err := decodeTransferConfig(encoded[:j]); !errors.Is(err, ErrCorrupt) {
			t.Fatal(j, err)
		}
	}
	wire, _ := EncodeApplicationSnapshotManifest(m)
	hostile := bytes.Clone(wire)
	for j := range 4 {
		hostile[snapshotManifestOverhead-4+j] = 255
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if _, err := DecodeApplicationSnapshotManifest(hostile, 64<<10); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	if after.TotalAlloc-before.TotalAlloc > 64<<10 {
		t.Fatal("claimed bytes allocated", after.TotalAlloc-before.TotalAlloc)
	}
}

func TestSnapshotReopenCannotBindUnboundDataAndPoisonedAbort(t *testing.T) {
	p, tc := transferConfig(1)
	fs := vfs.NewCrashableMem()
	old := openApplication(t, fs, p)
	applyApplication(t, old, "old", KV{Key: []byte("a"), Value: []byte("preserved")})
	clone := fs.CrashClone(vfs.CrashCloneCfg{})
	if opened, err := Open(Config{Dir: "app", FS: clone, Application: p, Transfer: tc}); opened != nil || !errors.Is(err, ErrInvalid) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatal(err)
	}
	recovered, err := Open(Config{Dir: "app", FS: clone, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	getApplication(t, viewApplication(t, recovered, 2), "a", "preserved", true, false)
	s := transferStore(t, "bound", vfs.NewMem(), 1)
	e, err := s.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m, _ := e.Manifest()
	imp, err := s.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	s.poison = ErrCorrupt
	if err := imp.Abort(); !errors.Is(err, ErrPoisoned) {
		t.Fatal(err)
	}
	if _, err := s.BeginApplicationExport(t.Context()); !errors.Is(err, ErrPoisoned) {
		t.Fatal(err)
	}
	if _, err := s.ApplicationTransferUsage(); !errors.Is(err, ErrPoisoned) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := imp.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotVerifyReleasesLockAndCannotCertifyReplacement(t *testing.T) {
	for _, mode := range []string{"progress", "abort-replace", "close"} {
		t.Run(mode, func(t *testing.T) {
			p, tc := transferConfig(1)
			tc.Limits.MaxChunkRows = 1
			s, err := Open(Config{Dir: "pages", FS: vfs.NewMem(), Create: true, Application: p, Transfer: tc})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.Initialize([]uint64{1}, []byte("initial")); err != nil {
				t.Fatal(err)
			}
			applyApplication(t, s, "X", KV{Key: []byte("a"), Value: []byte("old")})
			e, err := s.BeginApplicationExport(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			m, _ := e.Manifest()
			i, err := s.BeginApplicationImport(t.Context(), m)
			if err != nil {
				t.Fatal(err)
			}
			var chunks []ApplicationSnapshotChunk
			for {
				c, err := e.Next(t.Context(), ReadBudget{1, 512})
				if err != nil {
					t.Fatal(err)
				}
				chunks = append(chunks, c)
				if err := i.Append(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				if c.Final {
					break
				}
			}
			reached, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			i.verifyPageHook = func() { once.Do(func() { close(reached); <-resume }) }
			result := make(chan error, 1)
			go func() { result <- i.Verify(t.Context()) }()
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("verifier stalled")
			}
			if err := i.Verify(t.Context()); !errors.Is(err, ErrLimit) {
				close(resume)
				t.Fatal("unbounded concurrent verifiers", err)
			}
			action := make(chan error, 1)
			go func() {
				switch mode {
				case "progress":
					idx, image, err := s.Checkpoint()
					if err != nil {
						action <- err
						return
					}
					idx++
					err = s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(idx)}, Entries: []*pb.Entry{ent(idx, 2, "Y")}})
					if err == nil {
						err = s.InstallApplication(idx, ApplicationBatch{BaseIndex: idx - 1, BaseImageHash: sha256.Sum256(image), Image: []byte("Y"), Writes: []KV{{Key: []byte("a"), Value: []byte("new")}}})
					}
					action <- err
				case "abort-replace":
					if err := i.Abort(); err != nil {
						action <- err
						return
					}
					replacement, err := s.BeginApplicationImport(t.Context(), m)
					if err == nil {
						for _, c := range chunks {
							if err = replacement.Append(t.Context(), c); err != nil {
								break
							}
						}
						if err == nil && !errors.Is(replacement.Verify(t.Context()), ErrLimit) {
							err = ErrCorrupt
						}
						usage, e := s.ApplicationTransferUsage()
						if e != nil || !usage.Verifier || usage.VerifierImageBytes != len(m.Image) {
							err = errors.Join(e, ErrCorrupt)
						}
						status, e := replacement.Status()
						if e != nil || status.Verified {
							err = errors.Join(e, ErrCorrupt)
						}
					}
					action <- err
				case "close":
					action <- s.Close()
				}
			}()
			select {
			case err := <-action:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				close(resume)
				t.Fatal("active work blocked behind whole-bank verification")
			}
			close(resume)
			select {
			case err := <-result:
				if mode == "progress" {
					if err != nil {
						t.Fatal(err)
					}
					getApplication(t, viewApplication(t, s, 2), "a", "old", true, false)
					getApplication(t, viewApplication(t, s, 3), "a", "new", true, false)
					if err := i.Abort(); err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, ErrClosed) {
					t.Fatal("old verifier certified replacement", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("verifier did not finish")
			}
			if mode == "abort-replace" {
				status, err := s.applicationImport.Status()
				if err != nil || status.Verified || status.Records == 0 {
					t.Fatal(status, err)
				}
				if err := s.applicationImport.Verify(t.Context()); err != nil {
					t.Fatal("old verifier leaked global slot", err)
				}
				if err := s.applicationImport.Abort(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

type snapshotExpectedRecord struct {
	Value   string
	Deleted bool
}

func snapshotSemanticRecord(t *testing.T, k, raw []byte, shift byte) (string, snapshotExpectedRecord) {
	t.Helper()
	value, deleted, err := inspectAppFrame(k, raw, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	tag := k[0] - shift
	var id string
	if tag == appDataTag {
		canonical := bytes.Clone(k)
		canonical[0] = tag
		key, index, err := decodeAppKey(canonical, 1024)
		if err != nil {
			t.Fatal(err)
		}
		id = fmt.Sprintf("%d/%s/%d", tag, key, index)
	} else {
		if tag < appRootTag || tag > appOutcomeTag || len(k) != 9 {
			t.Fatal("unexpected namespace/key", k)
		}
		id = fmt.Sprintf("%d/%d", tag, binary.BigEndian.Uint64(k[1:]))
	}
	return id, snapshotExpectedRecord{string(value), deleted}
}
func assertSnapshotIndependentRows(t *testing.T, dst *Store, m ApplicationSnapshotManifest, chunks []ApplicationSnapshotChunk) {
	t.Helper()
	expected := map[string]snapshotExpectedRecord{
		"8/a/2": {"old", false}, "8/a/3": {"", true}, "8/b/2": {"", false},
		"9/1": {"initial", false}, "9/2": {"X", false}, "9/3": {"Y", false},
		"10/1": {"", false}, "10/2": {"change:X", false}, "10/3": {"change:Y", false},
		"11/1": {"", false}, "11/2": {"outcome:X", false}, "11/3": {"outcome:Y", false},
	}
	source := make(map[string]snapshotExpectedRecord)
	for _, c := range chunks {
		if err := walkSnapshotChunk(c.Data, m.Contract, func(k, v []byte) error {
			id, record := snapshotSemanticRecord(t, k, v, 0)
			if _, exists := source[id]; exists {
				t.Fatal("duplicate semantic record", id)
			}
			source[id] = record
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if !maps.Equal(source, expected) {
		t.Fatal("export changed exact history/envelope set", source)
	}
	dormant := make(map[string]snapshotExpectedRecord)
	it, err := dst.db.NewIter(&pebble.IterOptions{LowerBound: []byte{12}, UpperBound: []byte{16}})
	if err != nil {
		t.Fatal(err)
	}
	for valid := it.First(); valid; valid = it.Next() {
		id, record := snapshotSemanticRecord(t, it.Key(), it.Value(), 4)
		if _, exists := dormant[id]; exists {
			t.Fatal("duplicate dormant semantic record", id)
		}
		dormant[id] = record
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(dormant, expected) {
		t.Fatal("dormant rows changed exact history/envelope set", dormant)
	}
}
