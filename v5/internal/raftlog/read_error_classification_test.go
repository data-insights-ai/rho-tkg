package raftlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type storedReadFixture struct {
	s         *Store
	cfg       Config
	toggle    *errorfs.Toggle
	hits      atomic.Int64
	cause     error
	key       []byte
	caller    string
	openLine  int
	recording atomic.Bool
	eventsMu  sync.Mutex
	events    []storedReadOperation
}

type storedReadOperation struct {
	errorfs.Op
	openLine int
}

func newStoredReadFixture(t *testing.T, door string) *storedReadFixture {
	t.Helper()
	f := &storedReadFixture{cause: errors.New("operational mandatory evidence SST read")}
	f.toggle = &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
		if op.Kind != errorfs.OpFileReadAt || !strings.HasSuffix(op.Path, ".sst") {
			return nil
		}
		var pcs [64]uintptr
		n := runtime.Callers(0, pcs[:])
		frames := runtime.CallersFrames(pcs[:n])
		point := false
		openLine := 0
		for {
			frame, more := frames.Next()
			if strings.HasSuffix(frame.Function, f.caller) {
				point = true
			}
			if strings.HasSuffix(frame.Function, "/raftlog.Open") {
				openLine = frame.Line
				break
			}
			if !more {
				break
			}
		}
		if !point {
			return nil
		}
		if f.recording.Load() {
			f.eventsMu.Lock()
			f.events = append(f.events, storedReadOperation{op, openLine})
			f.eventsMu.Unlock()
			return nil
		}
		if f.openLine != 0 && f.openLine != openLine {
			return nil
		}
		f.hits.Add(1)
		return f.cause
	})}
	fs := errorfs.Wrap(vfs.NewMem(), f.toggle)
	p, tc := transferConfig(1)
	l := DefaultLimits()
	l.CacheBytes = 1
	f.cfg = Config{Dir: "db", FS: fs, Create: true, Limits: l, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{1000000}}
	s, err := Open(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.s = s
	t.Cleanup(func() {
		f.toggle.Off()
		if f.s != nil {
			if err := f.s.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if err := s.Initialize([]uint64{1}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	generationApply(t, s, "published", KV{Key: []byte("a"), Value: []byte("retained")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	switch door {
	case "root", "change", "outcome":
		generationApply(t, s, "applied")
		tag := appRootTag
		if door == "change" {
			tag = appChangeTag
		}
		if door == "outcome" {
			tag = appOutcomeTag
		}
		f.key = bankIndexKey(s.activeBank(), tag, 3)
		f.caller = "(*Store).appGet"
	case "open-first", "open-last", "scrub-last":
		if err := s.Persist(raft.Ready{Entries: []*pb.Entry{ent(3, 2, "first tail"), ent(4, 2, "last tail")}}); err != nil {
			t.Fatal(err)
		}
		index := uint64(4)
		if door == "open-first" {
			index = 3
		}
		f.key = entryKey(index)
		f.caller = "(*Store).get"
	default:
		t.Fatal(door)
	}
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	// Compact valid bytes into SSTs without disabling production compactions.
	// Target selection below does not depend on physical file identifiers.
	if err := s.db.Compact(t.Context(), []byte{0}, dormantEnd, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	f.cfg.Create = false
	f.s, err = Open(f.cfg)
	if err != nil {
		t.Fatal("healthy fixture reopen", err)
	}
	// Put earlier guard prerequisites into the memtable with byte-identical
	// values and prove they need no SST IO. Targeting then survives file rewrites.
	var prerequisites [][]byte
	switch door {
	case "change", "outcome":
		prerequisites = append(prerequisites, bankIndexKey(f.s.activeBank(), appRootTag, 3))
		if door == "outcome" {
			prerequisites = append(prerequisites, bankIndexKey(f.s.activeBank(), appChangeTag, 3))
		}

	}
	before := readyMetadataFingerprint(t, f.s)
	for _, key := range prerequisites {
		raw, closer, err := f.s.db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		owned := bytes.Clone(raw)
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.s.db.Set(key, owned, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if trace := storedReadTrace(t, f, key); len(trace) != 0 {
			t.Fatalf("prerequisite %x still read SST: %v", key, trace)
		}
	}
	if door == "open-last" {
		// WAL recovery may flush the earlier frame. Learn actual point-read
		// callsites instead of assuming replay keeps it in process memory.
		if err := f.s.Close(); err != nil {
			t.Fatal(err)
		}
		f.eventsMu.Lock()
		f.events = nil
		f.eventsMu.Unlock()
		f.recording.Store(true)
		f.toggle.On()
		f.s, err = Open(f.cfg)
		f.toggle.Off()
		f.recording.Store(false)
		if err != nil {
			t.Fatal("healthy callsite reopen", err)
		}
		f.eventsMu.Lock()
		events := slices.Clone(f.events)
		f.eventsMu.Unlock()
		var lines []int
		for _, op := range events {
			if op.openLine == 0 {
				t.Fatal("point read lacked Open caller")
			}
			if !slices.Contains(lines, op.openLine) {
				lines = append(lines, op.openLine)
			}
		}
		if len(lines) != 2 {
			t.Fatalf("expected distinct first/last real SST callsites: %v events=%v", lines, events)
		}
		f.openLine = lines[1]
		t.Logf("learned first/last Open SST callsites=%v; target=%d", lines, f.openLine)
	}
	if before != readyMetadataFingerprint(t, f.s) {
		t.Fatal("prerequisite rewrite changed logical bytes")
	}
	trace := storedReadTrace(t, f, f.key)
	if len(trace) == 0 {
		t.Fatal("target point read performed no actual SST IO")
	}
	t.Logf("door=%s target-key=%x prerequisite-memory-reads=%d target-SST-reads=%d", door, f.key, len(prerequisites), len(trace))
	return f
}
func storedReadTrace(t *testing.T, f *storedReadFixture, key []byte) []storedReadOperation {
	t.Helper()
	f.eventsMu.Lock()
	f.events = nil
	f.eventsMu.Unlock()
	f.recording.Store(true)
	f.toggle.On()
	var err error
	if key[0] == 1 {
		_, _, _, err = f.s.get(binary.BigEndian.Uint64(key[1:]))
	} else {
		_, _, err = f.s.appGet(key, f.s.meta.App.Policy.MaxImageBytes)
	}
	f.toggle.Off()
	f.recording.Store(false)
	if err != nil {
		t.Fatal("healthy trace", err)
	}
	f.eventsMu.Lock()
	defer f.eventsMu.Unlock()
	return slices.Clone(f.events)
}
func storedReadCall(t *testing.T, f *storedReadFixture, door string) error {
	t.Helper()
	switch door {
	case "root", "change", "outcome":
		return f.s.PublishSnapshot()
	case "open-first", "open-last":
		if err := f.s.Close(); err != nil {
			t.Fatal(err)
		}
		s, err := Open(f.cfg)
		if s != nil {
			f.s = s
			t.Error("failed reopen returned store")
		}
		return err
	case "scrub-last":
		return f.s.Scrub()
	default:
		t.Fatal(door)
		return ErrInvalid
	}
}
func TestStoredReadOperationalSSTClassification(t *testing.T) {
	for _, door := range []string{"root", "change", "outcome", "open-first", "open-last", "scrub-last"} {
		t.Run(door, func(t *testing.T) {
			f := newStoredReadFixture(t, door)
			s := f.s
			before := readyMetadataFingerprint(t, s)
			usage, _ := s.ApplicationTransferUsage()
			views, viewBytes := s.views, s.viewBytes
			f.toggle.On()
			err := storedReadCall(t, f, door)
			f.toggle.Off()
			if f.hits.Load() == 0 {
				t.Fatal("target point-read SST fault did not fire")
			}
			if !errors.Is(err, f.cause) {
				t.Fatal("first error lost operational cause", err)
			}
			t.Logf("door=%s target-key=%x hits=%d first=%v", door, f.key, f.hits.Load(), err)
			classified := errors.Is(err, ErrCorrupt)
			if !strings.HasPrefix(door, "open-") {
				if !errors.Is(s.poison, f.cause) {
					t.Fatal("poison lost read cause", s.poison)
				}
				_, follow := s.LastIndex()
				if !errors.Is(follow, ErrPoisoned) || !errors.Is(follow, f.cause) {
					t.Fatal("follow-up lost operational cause", follow)
				}
				classified = classified || errors.Is(s.poison, ErrCorrupt) || errors.Is(follow, ErrCorrupt)
				t.Logf("door=%s hits=%d first=%v poison=%v follow=%v", door, f.hits.Load(), err, s.poison, follow)
				if usage.PinnedLogicalBytes != s.pinnedApplicationBytes || usage.Exports != len(s.applicationExports) || s.views != views || s.viewBytes != viewBytes || before != readyMetadataFingerprint(t, s) {
					t.Fatal("failed read changed bytes/ownership")
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				t.Logf("door=%s hits=%d first=%v", door, f.hits.Load(), err)
			}
			recovered, err := Open(f.cfg)
			if err != nil {
				t.Fatal("fault-free reopen failed", err)
			}
			f.s = recovered
			if before != readyMetadataFingerprint(t, recovered) {
				t.Fatal("failed read changed recovered bytes")
			}
			if err := recovered.Scrub(); err != nil {
				t.Fatal(err)
			}
			if err := recovered.ScrubApplication(t.Context()); err != nil {
				t.Fatal(err)
			}
			row, found, err := viewApplication(t, recovered, 2).Get(t.Context(), []byte("a"), 256)
			if err != nil || !found || row.Deleted || string(row.Value) != "retained" {
				t.Fatal(row, found, err)
			}
			if classified {
				t.Error("operational point read mislabeled corruption")
			}
		})
	}
}

func TestStoredReadCorruptionControls(t *testing.T) {
	for _, tc := range []struct{ door, fault string }{
		{"root", "missing"}, {"change", "missing"}, {"outcome", "missing"},
		{"root", "frame"}, {"root", "length"}, {"root", "hash"}, {"root", "tombstone"},
		{"open-first", "missing"}, {"open-first", "frame"}, {"open-first", "link"}, {"open-first", "low term"}, {"open-first", "high term"},
		{"open-last", "frame"}, {"open-last", "hash"}, {"open-last", "low term"}, {"open-last", "high term"},
		{"scrub-last", "hash"},
	} {
		t.Run(tc.door+"/"+tc.fault, func(t *testing.T) {
			f := newStoredReadFixture(t, tc.door)
			s := f.s
			var replacement []byte
			m := s.meta
			switch tc.fault {
			case "missing":
				if err := s.db.Delete(f.key, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "frame":
				replacement = []byte("damaged frame")
			case "length":
				replacement = appFrame(f.key, []byte("short"), false)
			case "tombstone":
				replacement = appFrame(f.key, nil, true)
			case "hash":
				switch tc.door {
				case "root":
					replacement = appFrame(f.key, bytes.Repeat([]byte("x"), int(m.ImageBytes)), false)
				case "open-last":
					entry, prev, _, err := s.get(4)
					if err != nil {
						t.Fatal(err)
					}
					entry.Data = []byte("different")
					replacement, _ = encodeEntry(entry, prev)
				case "scrub-last":
					m.LastHash[0] ^= 1
				}
			case "link", "low term", "high term":
				index := binary.BigEndian.Uint64(f.key[1:])
				entry, prev, _, err := s.get(index)
				if err != nil {
					t.Fatal(err)
				}
				switch tc.fault {
				case "link":
					prev[0] ^= 1
				case "low term":
					entry.Term = new(uint64(1))
				case "high term":
					entry.Term = new(uint64(3))
				}
				var hash [32]byte
				replacement, hash = encodeEntry(entry, prev)
				if tc.door == "open-last" {
					m.LastHash = hash
				}
			}
			if replacement != nil {
				if err := s.db.Set(f.key, replacement, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if m.LastHash != s.meta.LastHash {
				raw, err := encodeMeta(m)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.db.Set(metaKey, raw, pebble.Sync); err != nil {
					t.Fatal(err)
				}
				s.meta = m
			}
			before := readyMetadataFingerprint(t, s)
			err := storedReadCall(t, f, tc.door)
			if !errors.Is(err, ErrCorrupt) || errors.Is(err, f.cause) || f.hits.Load() != 0 {
				t.Fatal("corruption lost diagnosis", err)
			}
			if !strings.HasPrefix(tc.door, "open-") {
				if !errors.Is(s.poison, ErrCorrupt) {
					t.Fatal(s.poison)
				}
				_, follow := s.LastIndex()
				if !errors.Is(follow, ErrPoisoned) || !errors.Is(follow, ErrCorrupt) {
					t.Fatal(follow)
				}
				if before != readyMetadataFingerprint(t, s) {
					t.Fatal("corruption inspection wrote durable bytes")
				}
			} else {
				db, err := pebble.Open(f.cfg.Dir, &pebble.Options{FS: f.cfg.FS})
				if err != nil {
					t.Fatal(err)
				}
				if before != readyMetadataFingerprint(t, &Store{db: db}) {
					t.Error("failed recovery wrote durable bytes")
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
