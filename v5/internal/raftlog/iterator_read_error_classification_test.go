package raftlog

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type lazyReadFixture struct {
	s       *Store
	cfg     Config
	toggle  *errorfs.Toggle
	cause   error
	hits    atomic.Int64
	priming atomic.Bool
	primeMu sync.Mutex
	primeOp errorfs.Op
	caller  string
	key     []byte
	v       *ApplicationView
	e       *ApplicationExport
	i       *ApplicationImport
}

func newLazyReadFixture(t *testing.T, door string) *lazyReadFixture {
	t.Helper()
	f := &lazyReadFixture{cause: errors.New("operational deferred blob value read")}
	callers := map[string]string{"get": "(*ApplicationView).Get", "scan": "(*ApplicationView).Scan", "scrub-application": "(*Store).ScrubApplication", "entries": "(*Store).entries", "replace-suffix": "(*Store).rangeBytes", "as1-next": "(*ApplicationExport).Next", "build": "(*ApplicationExport).publishedPage", "as2-next": "(*ApplicationExport).publishedPage", "verify": "(*ApplicationImport).verifyPage"}
	f.caller = callers[door]
	if f.caller == "" {
		t.Fatal(door)
	}
	f.toggle = &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
		if op.Kind != errorfs.OpFileReadAt || !strings.HasSuffix(op.Path, ".blob") {
			return nil
		}
		var pcs [64]uintptr
		n := runtime.Callers(0, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		value, body, caller := false, false, false
		for {
			frame, more := frames.Next()
			fn := frame.Function
			value = value || strings.HasSuffix(fn, "(*Iterator).Value") || strings.HasSuffix(fn, "(*Iterator).ValueAndErr")
			body = body || strings.HasSuffix(fn, "blob.(*FileReader).ReadValueBlock")
			caller = caller || strings.HasSuffix(fn, f.caller)
			if !more {
				break
			}
		}
		// A metadata/footer or positioning error is not deferred-value evidence.
		if value && body && f.priming.Load() {
			f.primeMu.Lock()
			f.primeOp = op
			f.primeMu.Unlock()
			return nil
		}
		if value && body && caller {
			f.hits.Add(1)
			return f.cause
		}
		return nil
	})}
	fs := errorfs.Wrap(vfs.NewMem(), f.toggle)
	original := publicationStore(t, fs)
	writes := []KV{{Key: []byte("a"), Value: []byte("old")}}
	if door == "as1-next" || door == "build" || door == "as2-next" {
		writes = append([]KV{{Key: []byte("0"), Value: []byte("cursor prefix")}}, writes...)
	}
	generationApply(t, original, "old root", writes...)
	if err := original.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	f.key = bankVersionKey(original.activeBank(), []byte("a"), 2)
	targets := [][]byte{f.key}
	if door == "entries" || door == "replace-suffix" {
		if err := original.Persist(raft.Ready{Entries: []*pb.Entry{ent(3, 2, "first"), ent(4, 2, "last")}}); err != nil {
			t.Fatal(err)
		}
		f.key = entryKey(3)
		targets = [][]byte{entryKey(3), entryKey(4)}
	}
	f.cfg = semanticConfig(original, fs)
	f.cfg.Limits = original.limits
	f.cfg.Limits.CacheBytes = 1
	before := readyMetadataFingerprint(t, original)
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	// Private fixture preparation writes a supported higher Pebble format. The
	// adapter has no new format/options API. Equal-byte rewrites force real blob
	// output rather than a trivial compaction move, preserving logical evidence.
	db, err := pebble.Open(f.cfg.Dir, lazyReadOptions(f.cfg))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range targets {
		raw, c, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		owned := bytes.Clone(raw)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if err := db.Set(key, owned, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(t.Context(), []byte{0}, dormantEnd, false); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f.s, err = Open(f.cfg)
	if err != nil {
		t.Fatal("ordinary supported Open", err)
	}
	t.Cleanup(func() {
		f.toggle.Off()
		if err := f.s.Close(); err != nil {
			t.Error(err)
		}
		if f.e != nil {
			if err := f.e.Close(); err != nil {
				t.Error(err)
			}
		}
		if f.v != nil {
			if err := f.v.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if f.s.db.FormatMajorVersion() < pebble.FormatValueSeparation || before != readyMetadataFingerprint(t, f.s) {
		t.Fatal("higher format changed logical evidence")
	}
	switch door {
	case "get", "scan":
		f.v, err = f.s.ApplicationView(2)
		if err != nil {
			t.Fatal(err)
		}
		generationApply(t, f.s, "later root", KV{Key: []byte("a"), Value: []byte("new")}, KV{Key: []byte("phantom"), Value: []byte("future")})
	case "as1-next":
		f.e, err = f.s.BeginApplicationExport(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		prefix, err := f.e.Next(t.Context(), ReadBudget{1, 4096})
		if err != nil || prefix.Final || len(f.e.after) == 0 {
			t.Fatal("successful prefix page", err)
		}
	case "build", "as2-next":
		cut, err := f.s.PublishedApplicationCut()
		if err != nil {
			t.Fatal(err)
		}
		f.e, err = f.s.BeginPublishedApplicationExport(t.Context(), cut.ID)
		if err != nil {
			t.Fatal(err)
		}
		if door == "as2-next" {
			_ = buildPublished(t, f.e, ReadBudget{1, 4096})
			prefix, err := f.e.Next(t.Context(), ReadBudget{1, 4096})
			if err != nil || prefix.Final || len(f.e.after) == 0 {
				t.Fatal("successful replay prefix", err)
			}
		} else {
			ready, err := f.e.BuildManifest(t.Context(), ReadBudget{1, 4096})
			if err != nil || ready || len(f.e.after) == 0 || len(f.e.published.verifier.last) == 0 {
				t.Fatal("successful verifier prefix", err)
			}
		}
	case "verify":
		cut, _ := f.s.PublishedApplicationCut()
		source, err := f.s.BeginPublishedApplicationExport(t.Context(), cut.ID)
		if err != nil {
			t.Fatal(err)
		}
		m := buildPublished(t, source, ReadBudget{1, 4096})
		chunks := collectPublished(t, source, ReadBudget{1, 4096})
		if err := source.Close(); err != nil {
			t.Fatal(err)
		}
		// No export, view, import or claim is live while this fixture backend is
		// prepared for future staged blob writes. Logical/root configuration stays
		// unchanged. An ordinary Open above already proves format compatibility.
		if f.s.views != 0 || len(f.s.applicationExports) != 0 || len(f.s.applicationSnapshotClaims) != 0 || f.s.applicationImport != nil {
			t.Fatal("backend preparation beneath a handle")
		}
		if err := f.s.db.Close(); err != nil {
			t.Fatal(err)
		}
		f.s.db, err = pebble.Open(f.cfg.Dir, lazyReadOptions(f.cfg))
		if err != nil {
			t.Fatal(err)
		}
		f.i, err = f.s.BeginApplicationImport(t.Context(), m)
		if err != nil {
			t.Fatal(err)
		}
		for _, chunk := range chunks {
			if err := f.i.Append(t.Context(), chunk); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.s.db.Flush(); err != nil {
			t.Fatal(err)
		}
		f.key = bankVersionKey(f.i.bank, []byte("a"), 2)
	}
	proveLazyRead(t, f)
	return f
}
func lazyReadOptions(cfg Config) *pebble.Options {
	// Checksum controls damage the exact physical block proved by priming.
	// A background compaction can otherwise copy its healthy bytes to a new
	// blob before Verify creates its fresh iterator. Manual fixture Compact still
	// builds genuine lazy values; ordinary adapter Open compatibility is proved
	// separately before any handle or private staging-backend preparation.
	opts := &pebble.Options{FS: cfg.FS, CacheSize: 1, MemTableSize: cfg.Limits.MemTableBytes, FormatMajorVersion: pebble.FormatValueSeparation, DisableAutomaticCompactions: true}
	opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{Enabled: true, MinimumSize: 1, MaxBlobReferenceDepth: 1}
	}
	return opts
}
func proveLazyRead(t *testing.T, f *lazyReadFixture) {
	t.Helper()
	it, err := f.s.db.NewIter(&pebble.IterOptions{LowerBound: f.key, UpperBound: append(bytes.Clone(f.key), 0)})
	if err != nil {
		t.Fatal(err)
	}
	if !it.First() || !bytes.Equal(it.Key(), f.key) || it.LazyValue().Fetcher == nil {
		t.Fatal("target is not a deferred value", it.Error())
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	// Healthy priming reads the ENTIRE value, not just footer/index. Release its
	// closer; the tested call must still prove fresh blob value-block ReadAt IO.
	f.priming.Store(true)
	f.toggle.On()
	raw, c, err := f.s.db.Get(f.key)
	if err != nil || len(raw) == 0 {
		t.Fatal("healthy whole-value priming", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	f.toggle.Off()
	f.priming.Store(false)
	f.primeMu.Lock()
	op := f.primeOp
	f.primeMu.Unlock()
	if op.Kind != errorfs.OpFileReadAt || !strings.HasSuffix(op.Path, ".blob") {
		t.Fatal("healthy prime did not read actual blob value block")
	}
	if pebble.IsCorruptionError(f.cause) {
		t.Fatal("operational cause carries backend corruption marker")
	}
}
func lazyReadCall(t *testing.T, f *lazyReadFixture, door string) error {
	t.Helper()
	switch door {
	case "get":
		out, found, err := f.v.Get(t.Context(), []byte("a"), 256)
		if !reflect.DeepEqual(out, KV{}) || found {
			t.Error("read failure returned KV", out, found)
		}
		return err
	case "scan":
		page, err := f.v.Scan(t.Context(), nil, nil, nil, ReadBudget{8, 4096})
		if !reflect.DeepEqual(page, ApplicationPage{}) {
			t.Error("read failure returned page", page)
		}
		return err
	case "scrub-application":
		return f.s.ScrubApplication(t.Context())
	case "entries":
		out, err := f.s.Entries(3, 5, math.MaxUint64)
		if out != nil {
			t.Error("read failure returned entries", out)
		}
		return err
	case "replace-suffix":
		return f.s.Persist(raft.Ready{Entries: []*pb.Entry{ent(3, 2, "replacement")}})
	case "as1-next", "as2-next":
		chunk, err := f.e.Next(t.Context(), ReadBudget{8, 4096})
		if !reflect.DeepEqual(chunk, ApplicationSnapshotChunk{}) {
			t.Error("read failure returned chunk", chunk)
		}
		return err
	case "build":
		ready, err := f.e.BuildManifest(t.Context(), ReadBudget{8, 4096})
		if ready {
			t.Error("read failure certified manifest")
		}
		return err
	case "verify":
		return f.i.Verify(t.Context())
	default:
		t.Fatal(door)
		return ErrInvalid
	}
}

// Protobuf ownership clones may have different implementation/cache state.
// Compare their semantic fields, while retaining exact owned image/ID ledgers.
func lazyManifestEqual(a, b ApplicationSnapshotManifest) bool {
	ac, bc := a.ConfState, b.ConfState
	a.ConfState, b.ConfState = nil, nil
	return proto.Equal(ac, bc) && reflect.DeepEqual(a, b)
}

func TestIteratorDeferredValueOperationalClassification(t *testing.T) {
	for _, door := range []string{"get", "scan", "scrub-application", "entries", "replace-suffix", "as1-next", "build", "as2-next", "verify"} {
		t.Run(door, func(t *testing.T) {
			f := newLazyReadFixture(t, door)
			before := readyMetadataFingerprint(t, f.s)
			usage, _ := f.s.ApplicationTransferUsage()
			views, viewBytes := f.s.views, f.s.viewBytes
			var exportState ApplicationExport
			var published publishedExportState
			if f.e != nil {
				exportState = *f.e
				exportState.after = bytes.Clone(f.e.after)
				exportState.manifest = cloneSnapshotManifest(f.e.manifest)
				if f.e.published != nil {
					published = *f.e.published
					published.verifier.last = bytes.Clone(f.e.published.verifier.last)
					exportState.published = &published
				}
			}
			var imported ApplicationImportStatus
			if f.i != nil {
				imported, _ = f.i.Status()
			}
			f.toggle.On()
			err := lazyReadCall(t, f, door)
			f.toggle.Off()
			if f.hits.Load() == 0 || !errors.Is(err, f.cause) {
				t.Fatal("genuine deferred body fault not reached/original cause lost", f.hits.Load(), err)
			}
			if pebble.IsCorruptionError(err) {
				t.Fatal("operational result acquired backend corruption marker", err)
			}
			classified := errors.Is(err, ErrCorrupt)
			if f.s.pinnedApplicationBytes != usage.PinnedLogicalBytes || len(f.s.applicationExports) != usage.Exports || f.s.views != views || f.s.viewBytes != viewBytes || readyMetadataFingerprint(t, f.s) != before {
				t.Fatal("failed value read changed logical bytes/owned ledger")
			}
			if f.e != nil {
				if !bytes.Equal(f.e.after, exportState.after) || f.e.sequence != exportState.sequence || f.e.final != exportState.final || f.e.closed != exportState.closed || f.e.pinBytes != exportState.pinBytes || !lazyManifestEqual(f.e.manifest, exportState.manifest) || f.e.published != nil && !reflect.DeepEqual(*f.e.published, published) {
					t.Fatal("failed value read advanced cursor/digest/sequence")
				}
			}
			if door == "verify" {
				status, err := f.i.Status()
				if err != nil || status != imported || status.Verified || f.s.poison != nil || f.s.applicationVerifier || f.s.applicationVerifierImageBytes != 0 {
					t.Fatal("failed dormant verification sealed/poisoned/leaked", status, err)
				}
			} else {
				_, follow := f.s.LastIndex()
				if !errors.Is(follow, ErrPoisoned) || !errors.Is(follow, f.cause) || !errors.Is(f.s.poison, f.cause) {
					t.Error("existing fail-stop lost operational cause", follow, f.s.poison)
				}
				classified = classified || errors.Is(follow, ErrCorrupt) || errors.Is(f.s.poison, ErrCorrupt)
			}
			t.Logf("door=%s fresh-value-block-hits=%d first=%v poison=%v", door, f.hits.Load(), err, f.s.poison)
			if err := f.s.Close(); err != nil {
				t.Fatal(err)
			}
			if f.e != nil {
				if err := f.e.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if f.v != nil {
				if err := f.v.Close(); err != nil {
					t.Fatal(err)
				}
			}
			reopened, err := Open(f.cfg)
			if err != nil {
				t.Fatal("healthy reopen", err)
			}
			f.s = reopened
			if err := f.s.Scrub(); err != nil {
				t.Fatal(err)
			}
			if err := f.s.ScrubApplication(t.Context()); err != nil {
				t.Fatal(err)
			}
			row, found, err := viewApplication(t, f.s, 2).Get(t.Context(), []byte("a"), 256)
			if err != nil || !found || string(row.Value) != "old" || row.Deleted {
				t.Fatal("old content lost", row, found, err)
			}
			if classified {
				t.Error("deferred operational value read mislabeled corruption")
			}
		})
	}
}

func TestIteratorDeferredValueGenuineCorruption(t *testing.T) {
	for _, door := range []string{"get", "scan", "scrub-application", "entries", "replace-suffix", "as1-next", "build", "as2-next", "verify"} {
		for _, fault := range []string{"stored frame", "blob checksum"} {
			// Immutable exports must retain the captured pre-mutation stream. A
			// later Set cannot corrupt that snapshot; physical checksum damage can.
			if fault == "stored frame" && (door == "as1-next" || door == "build" || door == "as2-next") {
				continue
			}
			t.Run(door+"/"+fault, func(t *testing.T) {
				f := newLazyReadFixture(t, door)
				if fault == "stored frame" {
					if err := f.s.db.Set(f.key, []byte("damaged stored frame"), pebble.Sync); err != nil {
						t.Fatal(err)
					}
				} else {
					f.primeMu.Lock()
					op := f.primeOp
					f.primeMu.Unlock()
					file, err := f.cfg.FS.OpenReadWrite(op.Path, vfs.WriteCategoryUnspecified)
					if err != nil {
						t.Fatal(err)
					}
					b := make([]byte, 1)
					if n, err := file.ReadAt(b, op.Offset); err != nil || n != 1 {
						t.Fatal(n, err)
					}
					b[0] ^= 0x80
					if n, err := file.WriteAt(b, op.Offset); err != nil || n != 1 {
						t.Fatal(n, err)
					}
					if err := file.Sync(); err != nil {
						t.Fatal(err)
					}
					if err := file.Close(); err != nil {
						t.Fatal(err)
					}
				}
				before, err := encodeMeta(f.s.meta)
				if err != nil {
					t.Fatal(err)
				}
				usage, _ := f.s.ApplicationTransferUsage()
				err = lazyReadCall(t, f, door)
				if !errors.Is(err, ErrCorrupt) || errors.Is(err, f.cause) || f.hits.Load() != 0 {
					t.Fatal("real corruption lost diagnosis", err)
				}
				if fault == "blob checksum" && !pebble.IsCorruptionError(err) {
					t.Fatal("actual blob damage lost backend marker", err)
				}
				after, encodeErr := encodeMeta(f.s.meta)
				if encodeErr != nil || !bytes.Equal(before, after) || f.s.pinnedApplicationBytes != usage.PinnedLogicalBytes || len(f.s.applicationExports) != usage.Exports {
					t.Fatal("corruption inspection mutated durable metadata/ownership", encodeErr)
				}
				if door == "verify" {
					status, statusErr := f.i.Status()
					if statusErr != nil || status.Verified || f.s.poison != nil || f.s.applicationVerifier {
						t.Fatal("dormant corruption sealed/poisoned active generation", status, statusErr)
					}
				} else {
					_, follow := f.s.LastIndex()
					if !errors.Is(follow, ErrPoisoned) || !errors.Is(follow, ErrCorrupt) && !pebble.IsCorruptionError(follow) {
						t.Fatal("corrupt source did not fail-stop with retained diagnosis", follow)
					}
				}
				t.Logf("door=%s real-corruption=%s error=%v", door, fault, err)
			})
		}
	}
}
