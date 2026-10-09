package raftlog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func openApplication(t *testing.T, fs vfs.FS, p ApplicationPolicy) *Store {
	t.Helper()
	s, err := Open(Config{Dir: "app", FS: fs, Create: true, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil && s.poison == nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{p.LocalVoter}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	return s
}
func applyApplication(t *testing.T, s *Store, image string, writes ...KV) uint64 {
	t.Helper()
	index, previous, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	index++
	persist(t, s, 2, index, ent(index, 2, image))
	if err := s.InstallApplication(index, ApplicationBatch{BaseIndex: index - 1, BaseImageHash: sha256.Sum256(previous), Image: []byte(image), Writes: writes, Changes: []byte("change:" + image), Outcome: []byte("outcome:" + image)}); err != nil {
		t.Fatal(err)
	}
	return index
}
func viewApplication(t *testing.T, s *Store, index uint64) *ApplicationView {
	t.Helper()
	v, err := s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	})
	return v
}
func getApplication(t *testing.T, v *ApplicationView, key string, value string, found, deleted bool) {
	t.Helper()
	row, ok, err := v.Get(t.Context(), []byte(key), 1<<20)
	if err != nil || ok != found || ok && (string(row.Value) != value || row.Deleted != deleted) {
		t.Fatalf("get %q: %+v found=%v err=%v", key, row, ok, err)
	}
}

func TestApplicationHistoryAndReclamation(t *testing.T) {
	fs := vfs.NewCrashableMem()
	p := DefaultApplicationPolicy(1)
	p.ReclaimEntries = 2
	s := openApplication(t, fs, p)
	initial := viewApplication(t, s, 1)
	first := applyApplication(t, s, "first", KV{Key: []byte("a"), Value: []byte("old")}, KV{Key: []byte("a\x00b"), Value: []byte{}}, KV{Key: []byte("z"), Value: []byte("last")})
	old := viewApplication(t, s, first)
	second := applyApplication(t, s, "second", KV{Key: []byte("a"), Value: []byte("new")}, KV{Key: []byte("z"), Deleted: true})
	current := viewApplication(t, s, second)
	getApplication(t, initial, "a", "", false, false)
	getApplication(t, old, "a", "old", true, false)
	getApplication(t, current, "a", "new", true, false)
	getApplication(t, old, "z", "last", true, false)
	getApplication(t, current, "z", "", true, true)
	getApplication(t, current, "a\x00b", "", true, false)
	root, err := old.Root()
	if err != nil || root.Index != first || string(root.Image) != "first" || root.ImageHash != sha256.Sum256(root.Image) {
		t.Fatal(root, err)
	}
	root.Image[0] = 'X'
	root, _ = old.Root()
	if string(root.Image) != "first" {
		t.Fatal("root aliases caller")
	}
	var keys []string
	var next []byte
	for pages := 0; ; pages++ {
		if pages > 4 {
			t.Fatal("cursor stalled")
		}
		page, err := current.Scan(t.Context(), nil, nil, next, ReadBudget{1, 256})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range page.Rows {
			keys = append(keys, string(r.Key))
			if string(r.Key) == "a" && string(r.Value) != "new" {
				t.Fatal("wrong version")
			}
		}
		if page.Complete {
			break
		}
		next = page.Next
	}
	if fmt.Sprint(keys) != fmt.Sprint([]string{"a", "a\x00b"}) {
		t.Fatal("wrong exact set", keys)
	}
	page, err := old.Scan(t.Context(), []byte("a"), []byte("z"), nil, ReadBudget{4, 512})
	if err != nil || !page.Complete || len(page.Rows) != 2 {
		t.Fatal(page, err)
	}
	page.Rows[0].Value[0] = 'X'
	getApplication(t, old, "a", "old", true, false)
	if err := s.ReclaimApplication(); err != nil {
		t.Fatal(err)
	}
	if firstLog, _ := s.FirstIndex(); firstLog != second+1 {
		t.Fatal("not reclaimed", firstLog)
	}
	for _, index := range []uint64{first, second} {
		for _, outcome := range []bool{false, true} {
			b, err := s.ApplicationRecord(t.Context(), index, outcome, 1024)
			if err != nil || len(b) == 0 {
				t.Fatal(b, err)
			}
		}
	}
	crash := fs.CrashClone(vfs.CrashCloneCfg{})
	reopened, err := Open(Config{Dir: "app", FS: crash, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, v := range []struct {
		index uint64
		value string
	}{{first, "old"}, {second, "new"}} {
		view := viewApplication(t, reopened, v.index)
		getApplication(t, view, "a", v.value, true, false)
	}
	if err := reopened.Scrub(); err != nil {
		t.Fatal(err)
	}
	idx, image, err := reopened.Checkpoint()
	if err != nil || idx != second || string(image) != "second" {
		t.Fatal(idx, string(image), err)
	}
}

func TestApplicationVersionRunSeeksAndFutureKeys(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	first := applyApplication(t, s, "first", KV{Key: []byte("b"), Value: []byte("first")})
	for i := range 100 {
		applyApplication(t, s, fmt.Sprint(i), KV{Key: []byte("b"), Value: []byte(fmt.Sprint(i))})
	}
	latest, _, _ := s.Checkpoint()
	applyApplication(t, s, "future", KV{Key: []byte("a"), Value: []byte("future")}, KV{Key: []byte("c"), Value: []byte("future")})
	old := viewApplication(t, s, first)
	getApplication(t, old, "b", "first", true, false)
	getApplication(t, old, "a", "", false, false)
	at := viewApplication(t, s, latest)
	var next []byte
	var rows []KV
	visited := 0
	for pages := 0; ; pages++ {
		if pages > 3 {
			t.Fatal("future cursor stalled")
		}
		page, err := at.Scan(t.Context(), nil, nil, next, ReadBudget{1, 128})
		if err != nil || page.Visited > 1 {
			t.Fatal(page, err)
		}
		visited += page.Visited
		rows = append(rows, page.Rows...)
		if page.Complete {
			break
		}
		next = page.Next
	}
	if visited != 3 || len(rows) != 1 || string(rows[0].Key) != "b" || string(rows[0].Value) != "99" {
		t.Fatal(visited, rows)
	}

}

func TestApplicationSnapshotAndModeGuards(t *testing.T) {
	fs := vfs.NewCrashableMem()
	p := DefaultApplicationPolicy(1)
	s := openApplication(t, fs, p)
	applyApplication(t, s, "kept", KV{Key: []byte("a"), Value: []byte("kept")})
	before, _ := encodeMeta(s.meta)
	snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(100)), Term: new(uint64(3)), ConfState: &pb.ConfState{Voters: []uint64{1}}}, Data: []byte("foreign root")}
	for _, rd := range []raft.Ready{{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(3)), Commit: new(uint64(100))}}, {Entries: []*pb.Entry{{Index: new(uint64(3)), Term: new(uint64(2)), Type: pb.EntryConfChange.Enum()}}}, {HardState: &pb.HardState{Term: new(uint64(3)), Vote: new(uint64(2)), Commit: new(uint64(2))}}} {
		if err := s.Persist(rd); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	after, _ := encodeMeta(s.meta)
	if !bytes.Equal(before, after) {
		t.Fatal("guard mutated metadata")
	}
	if err := s.SaveCheckpoint(2, &pb.ConfState{Voters: []uint64{1}}, []byte("foreign root")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(2, &pb.ConfState{Voters: []uint64{1, 2}}, []byte("kept")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.SaveCheckpoint(2, &pb.ConfState{Voters: []uint64{1}}, []byte("kept")); err != nil {
		t.Fatal(err)
	}
	for _, policy := range []ApplicationPolicy{{}, DefaultApplicationPolicy(2), func() ApplicationPolicy { q := p; q.MaxPageRows--; return q }()} {
		if r, err := Open(Config{Dir: "app", FS: fs.CrashClone(vfs.CrashCloneCfg{}), Application: policy}); !errors.Is(err, ErrInvalid) {
			if r != nil {
				r.Close()
			}
			t.Fatal(err)
		}
	}
	r, err := Open(Config{Dir: "app", FS: fs.CrashClone(vfs.CrashCloneCfg{}), Application: p})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	recovered, _ := encodeMeta(r.meta)
	if !bytes.Equal(before, recovered) {
		t.Fatal("snapshot rejection changed durable metadata")
	}
	getApplication(t, viewApplication(t, r, 2), "a", "kept", true, false)
	fresh, err := Open(Config{Dir: "fresh", FS: vfs.NewMem(), Create: true, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	for _, ids := range [][]uint64{{1, 2}, {2}, {}} {
		if err := fresh.Initialize(ids, nil); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if err := fresh.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationAdmissionInstallAndTailLimits(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	p.MaxTailEntries = 2
	p.MaxTailBytes = uint64(DefaultLimits().MaxEntryBytes + 9 + frameOverhead + 9)
	s := openApplication(t, vfs.NewMem(), p)
	if err := s.AdmitApplication(10); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 2, ent(2, 2, "two"))
	if err := s.AdmitApplication(10); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	before, _ := encodeMeta(s.meta)
	if err := s.Persist(raft.Ready{Entries: []*pb.Entry{ent(3, 2, "three"), ent(4, 2, "four")}}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	after, _ := encodeMeta(s.meta)
	if !bytes.Equal(before, after) {
		t.Fatal("limit mutated metadata")
	}
	for _, b := range []ApplicationBatch{{BaseImageHash: [32]byte{1}}, {BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial")), Writes: []KV{{Key: []byte("same")}, {Key: []byte("same")}}}, {BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial")), Writes: []KV{{Key: nil}}}, {BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial")), Writes: []KV{{Key: []byte("a"), Value: []byte("bad"), Deleted: true}}}} {
		if err := s.InstallApplication(2, b); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	b := ApplicationBatch{BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial")), Image: []byte("two"), Writes: []KV{{Key: []byte("a"), Value: make([]byte, p.MaxValueBytes+1)}}}
	if err := s.InstallApplication(2, b); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	b.Writes = nil
	if err := s.InstallApplication(2, b); err != nil {
		t.Fatal(err)
	}
	if s.meta.App.TailBytes != 0 {
		t.Fatal("tail not discharged")
	}
	if err := s.AdmitApplication(10); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 2, ent(3, 2, "obsolete"), ent(4, 2, "suffix"))
	oldBytes := s.meta.App.TailBytes
	persist(t, s, 3, 3, ent(3, 3, "replacement"))
	if s.meta.Last != 3 || s.meta.App.TailBytes >= oldBytes {
		t.Fatal("replacement tail ledger", s.meta.App.TailBytes, oldBytes)
	}
	if err := s.InstallApplication(3, ApplicationBatch{BaseIndex: 2, BaseImageHash: sha256.Sum256([]byte("two")), Image: []byte("three")}); err != nil {
		t.Fatal(err)
	}
	if s.meta.App.TailBytes != 0 {
		t.Fatal(s.meta.App.TailBytes)
	}
	for _, arg := range []int{-1, DefaultLimits().MaxEntryBytes} {
		err := s.AdmitApplication(arg)
		if !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrLimit) {
			t.Fatal(err)
		}
	}
}

func TestApplicationBoundedViewsAndReadErrors(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	p.MaxViews = 1
	p.MaxViewBytes = p.MaxImageBytes
	s := openApplication(t, vfs.NewMem(), p)
	applyApplication(t, s, "second", KV{Key: []byte("a"), Value: []byte("bytes")}, KV{Key: []byte("b"), Deleted: true})
	v := viewApplication(t, s, 2)
	if _, err := s.ApplicationView(1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := s.ApplicationView(99); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := v.Get(t.Context(), []byte("a"), 2); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	//nolint:staticcheck // SA1012: exercise the nil-context error contract.
	if _, _, err := v.Get(nil, []byte("a"), 256); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := v.Get(ctx, []byte("a"), 256); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, q := range []struct {
		lower, upper, after string
		b                   ReadBudget
		want                error
	}{{"z", "a", "", ReadBudget{1, 128}, ErrInvalid}, {"a", "z", "0", ReadBudget{1, 128}, ErrInvalid}, {"", "", "", ReadBudget{}, ErrInvalid}, {"", "", "", ReadBudget{p.MaxPageRows + 1, 128}, ErrLimit}, {"", "", "", ReadBudget{1, p.MaxPageBytes + 1}, ErrLimit}, {"", "", "", ReadBudget{1, 1}, ErrLimit}} {
		if _, err := v.Scan(t.Context(), []byte(q.lower), []byte(q.upper), []byte(q.after), q.b); !errors.Is(err, q.want) {
			t.Fatal(q, err)
		}
	}
	page, err := v.Scan(t.Context(), []byte("b"), nil, nil, ReadBudget{1, 128})
	if err != nil || !page.Complete || len(page.Rows) != 0 || page.Visited != 1 {
		t.Fatal(page, err)
	}
	if _, err := s.ApplicationRecord(t.Context(), 2, false, 1); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	//nolint:staticcheck // SA1012: exercise the nil-context error contract.
	if _, err := s.ApplicationRecord(nil, 2, false, 128); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.ApplicationRecord(ctx, 2, false, 128); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.ApplicationRecord(t.Context(), 3, false, 128); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Root(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	v2 := viewApplication(t, s, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := v2.Get(t.Context(), []byte("a"), 128); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := v2.Close(); err != nil {
		t.Fatal(err)
	}
	var ns *Store
	var nv *ApplicationView
	if ns.ApplicationLimits().Enabled() || ns.ApplicationBudget() != (ApplicationBudget{}) {
		t.Fatal("nil policy")
	}
	for _, fn := range []func() error{func() error { _, e := ns.ApplicationView(1); return e }, func() error { return ns.InstallApplication(1, ApplicationBatch{}) }, func() error { return ns.AdmitApplication(1) }, ns.ReclaimApplication, func() error { _, e := ns.ApplicationRecord(t.Context(), 1, false, 1); return e }, func() error { _, e := nv.Root(); return e }, nv.Close, func() error { _, _, e := nv.Get(t.Context(), nil, 1); return e }, func() error { _, e := nv.Scan(t.Context(), nil, nil, nil, ReadBudget{}); return e }} {
		if e := fn(); !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
	}
}

func TestApplicationCorruptionAndCodecLimits(t *testing.T) {
	for _, target := range []string{"value", "root", "change", "outcome"} {
		t.Run(target, func(t *testing.T) {
			fs := vfs.NewMem()
			p := DefaultApplicationPolicy(1)
			s := openApplication(t, fs, p)
			applyApplication(t, s, "second", KV{Key: []byte("a"), Value: []byte("value")})
			key := appVersionKey([]byte("a"), 2)
			switch target {
			case "root":
				key = appIndexKey(appRootTag, 2)
			case "change":
				key = appIndexKey(appChangeTag, 2)
			case "outcome":
				key = appIndexKey(appOutcomeTag, 2)
			}
			if err := s.db.Set(key, []byte("damaged"), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if target == "value" {
				v := viewApplication(t, s, 2)
				if _, _, err := v.Get(t.Context(), []byte("a"), 128); !errors.Is(err, ErrCorrupt) {
					t.Fatal(err)
				}
				if _, err := s.LastIndex(); !errors.Is(err, ErrPoisoned) {
					t.Fatal(err)
				}
			} else {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				if r, err := Open(Config{Dir: "app", FS: fs, Application: p}); !errors.Is(err, ErrCorrupt) {
					if r != nil {
						r.Close()
					}
					t.Fatal(err)
				}
			}
		})
	}
	p := DefaultApplicationPolicy(1)
	if err := p.Validate(DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	if err := (ApplicationPolicy{}).Validate(DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*ApplicationPolicy){func(p *ApplicationPolicy) { p.LocalVoter = 0 }, func(p *ApplicationPolicy) { p.MaxKeyBytes = -1 }, func(p *ApplicationPolicy) { p.MaxImageBytes = 1 << 30 }, func(p *ApplicationPolicy) { p.MaxViews = 1 << 30 }, func(p *ApplicationPolicy) { p.RetainedApplicationRecords = 1 }, func(p *ApplicationPolicy) { p.ReclaimEntries = 0 }, func(p *ApplicationPolicy) { p.MaxTailEntries = 1 }, func(p *ApplicationPolicy) { p.MaxTailBytes = 1 }} {
		q := p
		edit(&q)
		if err := q.Validate(DefaultLimits()); !errors.Is(err, ErrInvalid) {
			t.Fatal(q, err)
		}
	}
	a := applicationMetadata{Policy: p}
	wire := appendApplicationMeta(nil, a)
	for i := range len(wire) {
		if _, _, err := decodeApplicationMeta(wire[:i]); !errors.Is(err, ErrCorrupt) {
			t.Fatal(i, err)
		}
	}
	for _, key := range [][]byte{nil, {8, 0, 0}, {8, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, {8, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0}} {
		if _, _, err := decodeAppKey(key, 1024); !errors.Is(err, ErrCorrupt) {
			t.Fatal(key, err)
		}
	}
	for _, value := range [][]byte{nil, []byte("not a frame"), append(appFrame([]byte("a"), nil, false), 1)} {
		if _, _, err := inspectAppFrame([]byte("a"), value, 128); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
}

func TestApplicationConcurrentCloseAndReads(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	applyApplication(t, s, "value", KV{Key: []byte("a"), Value: []byte("value")})
	v := viewApplication(t, s, 2)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 20 {
				_, _, err := v.Get(t.Context(), []byte("a"), 128)
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Error(err)
				}
			}
		})
	}
	wg.Go(func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
}

func TestApplicationOldEmptyRootBoundsFutureWork(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	for i := range 40 {
		applyApplication(t, s, fmt.Sprint(i), KV{Key: []byte(fmt.Sprintf("%03d", i)), Value: []byte("future")})
	}
	old := viewApplication(t, s, 1)
	var next []byte
	visited := 0
	for pages := 0; ; pages++ {
		if pages > 40 {
			t.Fatal("future-only pagination failed")
		}
		page, err := old.Scan(t.Context(), nil, nil, next, ReadBudget{1, 80})
		if err != nil || len(page.Rows) != 0 || page.Visited != 1 || page.Bytes > 80 {
			t.Fatal(page, err)
		}
		visited += page.Visited
		if page.Complete {
			break
		}
		if string(page.Next) != fmt.Sprintf("%03d", pages) {
			t.Fatal("duplicate or skipped future key", page.Next, pages)
		}
		next = page.Next
	}
	if visited != 40 {
		t.Fatal(visited)
	}
}
func TestApplicationMissingRetainedEnvelopesPoison(t *testing.T) {
	for _, tag := range []byte{appRootTag, appChangeTag, appOutcomeTag} {
		t.Run(fmt.Sprint(tag), func(t *testing.T) {
			s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
			applyApplication(t, s, "older", KV{Key: []byte("a"), Value: []byte("old")})
			applyApplication(t, s, "newer", KV{Key: []byte("a"), Value: []byte("new")})
			if err := s.db.Delete(appIndexKey(tag, 2), pebble.Sync); err != nil {
				t.Fatal(err)
			}
			var err error
			if tag == appRootTag {
				_, err = s.ApplicationView(2)
			} else {
				_, err = s.ApplicationRecord(t.Context(), 2, tag == appOutcomeTag, 1024)
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			if _, err := s.ApplicationView(3); !errors.Is(err, ErrPoisoned) {
				t.Fatal(err)
			}
			if _, err := s.ApplicationRecord(t.Context(), 3, false, 1024); !errors.Is(err, ErrPoisoned) {
				t.Fatal(err)
			}
		})
	}
}

func TestApplicationUsageAndScrub(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	if s.ApplicationLimits() != DefaultApplicationPolicy(1) || s.ApplicationBudget().Writes != 4096 {
		t.Fatal("policy accessors")
	}
	applyApplication(t, s, "root", KV{Key: []byte("a"), Value: []byte("value")}, KV{Key: []byte("b"), Deleted: true})
	usage, err := s.ApplicationUsage()
	if err != nil || usage.RetainedRecords != 8 || usage.RetainedBytes != s.meta.App.Bytes || usage.TailBytes != 0 || usage.TailEntries != 0 || usage.CheckpointBytes != 4 || usage.SnapshotBytes != 7 {
		t.Fatal(usage, err)
	}
	v := viewApplication(t, s, 2)
	usage, err = s.ApplicationUsage()
	if err != nil || usage.Views != 1 || usage.ViewBytes != 4 {
		t.Fatal(usage, err)
	}
	if err := s.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.ScrubApplication(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	//nolint:staticcheck // SA1012: exercise the nil-context error contract.
	if err := s.ScrubApplication(nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Delete(appVersionKey([]byte("a"), 2), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.ScrubApplication(t.Context()); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := s.ApplicationUsage(); !errors.Is(err, ErrPoisoned) {
		t.Fatal(err)
	}
	var absent *Store
	if _, err := absent.ApplicationUsage(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := absent.ScrubApplication(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestApplicationInvalidVersionAndMissingScrubRun(t *testing.T) {
	for _, scenario := range []string{"zero version", "missing envelope", "tombstone envelope", "extra envelope"} {
		t.Run(scenario, func(t *testing.T) {
			s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
			applyApplication(t, s, "root", KV{Key: []byte("a"), Value: []byte("value")})
			switch scenario {
			case "zero version":
				k := appVersionKey([]byte("a"), 0)
				if err := s.db.Set(k, appFrame(k, []byte("forged"), false), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "missing envelope":
				if err := s.db.Delete(appIndexKey(appChangeTag, 1), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "tombstone envelope":
				k := appIndexKey(appOutcomeTag, 1)
				if err := s.db.Set(k, appFrame(k, nil, true), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			case "extra envelope":
				k := appIndexKey(appRootTag, 3)
				if err := s.db.Set(k, appFrame(k, nil, false), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.ScrubApplication(t.Context()); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			if _, err := s.ApplicationView(2); !errors.Is(err, ErrPoisoned) {
				t.Fatal(err)
			}
		})
	}
}

func TestApplicationInstallMissingCommittedEntryPoisons(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	persist(t, s, 2, 2, ent(2, 2, "committed"))
	if err := s.db.Delete(entryKey(2), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallApplication(2, ApplicationBatch{BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial"))}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := s.ApplicationView(1); !errors.Is(err, ErrPoisoned) {
		t.Fatal(err)
	}
}
func TestApplicationByteHeadroomReclaims(t *testing.T) {
	l := DefaultLimits()
	l.MaxEntryBytes = 256
	l.MaxReadBytes = 512
	l.MaxReadyBytes = 1024
	l.MaxRetainedBytes = 800
	l.MaxRetainedEntries = 10
	p := DefaultApplicationPolicy(1)
	p.MaxTailBytes = 800
	p.MaxTailEntries = 4
	p.ReclaimEntries = 8
	s, err := Open(Config{Dir: "small", FS: vfs.NewMem(), Create: true, Limits: l, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	for i := uint64(2); i <= 4; i++ {
		persist(t, s, 2, i, ent(i, 2, string(make([]byte, 170))))
		_, image, _ := s.Checkpoint()
		if err := s.InstallApplication(i, ApplicationBatch{BaseIndex: i - 1, BaseImageHash: sha256.Sum256(image)}); err != nil {
			t.Fatal(err)
		}
		if err := s.ReclaimApplication(); err != nil {
			t.Fatal(err)
		}
	}
	if first, _ := s.FirstIndex(); first < 4 {
		t.Fatal("byte headroom did not reclaim", first)
	}
	if err := s.AdmitApplication(170); err != nil {
		t.Fatal("safe admission remained blocked", err)
	}
}
func TestApplicationDisabledAndClosedCapabilities(t *testing.T) {
	s := initialized(t, vfs.NewMem())
	if _, err := s.ApplicationUsage(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.ScrubApplication(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.ApplicationView(1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.InstallApplication(2, ApplicationBatch{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.AdmitApplication(1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.ReclaimApplication(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.ApplicationRecord(t.Context(), 1, false, 128); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func() error{s.ReclaimApplication, func() error { return s.AdmitApplication(1) }, func() error { return s.InstallApplication(2, ApplicationBatch{}) }, func() error { _, e := s.ApplicationView(1); return e }, func() error { _, e := s.ApplicationUsage(); return e }, func() error { _, e := s.ApplicationRecord(t.Context(), 1, false, 1); return e }, func() error { return s.ScrubApplication(t.Context()) }} {
		if err := fn(); !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
	}
}

func TestApplicationEqualImageDoesNotPermitStaleRoot(t *testing.T) {
	s := openApplication(t, vfs.NewMem(), DefaultApplicationPolicy(1))
	persist(t, s, 2, 2, ent(2, 2, "same image"))
	if err := s.InstallApplication(2, ApplicationBatch{BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial")), Image: []byte("initial"), Writes: []KV{{Key: []byte("a"), Value: []byte("new")}}}); err != nil {
		t.Fatal(err)
	}
	persist(t, s, 2, 3, ent(3, 2, "later"))
	stale := ApplicationBatch{BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial")), Image: []byte("stale"), Writes: []KV{{Key: []byte("a"), Value: []byte("stale")}}}
	before, _ := encodeMeta(s.meta)
	if err := s.InstallApplication(3, stale); !errors.Is(err, ErrInvalid) {
		t.Fatal("same image admitted stale root", err)
	}
	after, _ := encodeMeta(s.meta)
	if !bytes.Equal(before, after) {
		t.Fatal("stale batch changed checkpoint/tail/retained ledgers")
	}
	for _, tag := range []byte{appRootTag, appChangeTag, appOutcomeTag} {
		_, closer, err := s.db.Get(appIndexKey(tag, 3))
		if closer != nil {
			_ = closer.Close()
		}
		if !errors.Is(err, pebble.ErrNotFound) {
			t.Fatal("stale batch installed envelope", tag, err)
		}
	}
	getApplication(t, viewApplication(t, s, 2), "a", "new", true, false)
	current := stale
	current.BaseIndex = 2
	current.Image = []byte("current")
	current.Writes[0].Value = []byte("current")
	if err := s.InstallApplication(3, current); err != nil {
		t.Fatal(err)
	}
	getApplication(t, viewApplication(t, s, 3), "a", "current", true, false)
}

func TestApplicationAggregateViewByteBudget(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	p.MaxViewBytes = p.MaxImageBytes
	s := openApplication(t, vfs.NewMem(), p)
	persist(t, s, 2, 2, ent(2, 2, "large root"))
	if err := s.InstallApplication(2, ApplicationBatch{BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial")), Image: make([]byte, p.MaxImageBytes)}); err != nil {
		t.Fatal(err)
	}
	old := viewApplication(t, s, 1)
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplicationView(2); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	after, err := s.ApplicationUsage()
	if err != nil || after.Views != before.Views || after.ViewBytes != before.ViewBytes {
		t.Fatal("failed view changed accounting", after, err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	current := viewApplication(t, s, 2)
	root, err := current.Root()
	if err != nil || len(root.Image) != p.MaxImageBytes {
		t.Fatal(len(root.Image), err)
	}
}

func TestApplicationPolicyExactReadFitAndAggregateInstallIndependence(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	p.MaxKeyBytes = 8
	p.MaxValueBytes = 16
	p.MaxImageBytes = 16
	p.MaxPageBytes = 96
	p.MaxChangeBytes = 96
	p.MaxOutcomeBytes = 96
	if err := p.Validate(DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*ApplicationPolicy){func(q *ApplicationPolicy) { q.MaxPageBytes--; q.MaxChangeBytes--; q.MaxOutcomeBytes-- }, func(q *ApplicationPolicy) { q.MaxChangeBytes++ }, func(q *ApplicationPolicy) { q.MaxOutcomeBytes++ }} {
		q := p
		edit(&q)
		if err := q.Validate(DefaultLimits()); !errors.Is(err, ErrInvalid) {
			t.Fatal(q, err)
		}
	}
	s := openApplication(t, vfs.NewMem(), p)
	persist(t, s, 2, 2, ent(2, 2, "full envelope"))
	b := ApplicationBatch{BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial")), Image: []byte("next"), Writes: []KV{{Key: []byte("12345678"), Value: make([]byte, 16)}}, Changes: make([]byte, 96), Outcome: make([]byte, 96)}
	if err := s.InstallApplication(2, b); err != nil {
		t.Fatal(err)
	}
	v := viewApplication(t, s, 2)
	page, err := v.Scan(t.Context(), nil, nil, nil, ReadBudget{1, 96})
	if err != nil || !page.Complete || len(page.Rows) != 1 {
		t.Fatal(page, err)
	}
	if _, err := v.Scan(t.Context(), nil, nil, nil, ReadBudget{1, 95}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	for _, outcome := range []bool{false, true} {
		data, err := s.ApplicationRecord(t.Context(), 2, outcome, 96)
		if err != nil || len(data) != 96 {
			t.Fatal(len(data), err)
		}
		if _, err := s.ApplicationRecord(t.Context(), 2, outcome, 95); !errors.Is(err, ErrLimit) {
			t.Fatal(err)
		}
	}
	// Independent ceilings are not promises that their simultaneous maxima fit
	// one aggregate installation. That combination is rejected before mutation.
	q := p
	q.MaxInstallBytes = 1294
	if err := q.Validate(DefaultLimits()); err != nil {
		t.Fatal("aggregate policy overconstrained", err)
	}
	if _, _, err := q.measure(b); !errors.Is(err, ErrLimit) {
		t.Fatal("oversized aggregate accepted", err)
	}
}

func TestApplicationRequiresExplicitInitialization(t *testing.T) {
	p := DefaultApplicationPolicy(1)
	s, err := Open(Config{Dir: "uninitialized", FS: vfs.NewMem(), Create: true, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	before, _ := encodeMeta(s.meta)
	if err := s.SaveCheckpoint(0, &pb.ConfState{Voters: []uint64{1}}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(1))}, Entries: []*pb.Entry{ent(1, 1, "bypass initialization")}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.AdmitApplication(1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	after, _ := encodeMeta(s.meta)
	if !bytes.Equal(before, after) {
		t.Fatal("uninitialized mutation changed state")
	}
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
}
