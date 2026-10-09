package raftlog

import (
	"crypto/sha256"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func publicationStore(t *testing.T, mem vfs.FS) *Store {
	t.Helper()
	p, tc := transferConfig(1)
	p.ReclaimEntries = 1
	s, err := Open(Config{Dir: "db", FS: mem, Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 1_000_000}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestApplicationPublicationRetainsExactBaseWhileAppliedMoves(t *testing.T) {
	mem := vfs.NewCrashableMem()
	s := publicationStore(t, mem)
	generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	if cut.Index != 2 || cut.ImageHash != sha256.Sum256([]byte("old")) || cut.RetainedRecords != 7 {
		t.Fatal("wrong published cut", cut)
	}
	generationApply(t, s, "new", KV{Key: []byte("a"), Value: []byte("new")})
	after, err := s.PublishedApplicationCut()
	if err != nil || after.ID != cut.ID || after.Index != 2 {
		t.Fatal("cut followed moving checkpoint", after, err)
	}
	r, err := Open(Config{Dir: "db", FS: mem.CrashClone(vfs.CrashCloneCfg{}), Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits, PublishedCuts: s.meta.Gen.Publication.Limits})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	recovered, err := r.PublishedApplicationCut()
	if err != nil || recovered.ID != cut.ID {
		t.Fatal("published cut not durable", recovered, err)
	}
	index, image, err := r.Checkpoint()
	if err != nil || index != 3 || string(image) != "new" {
		t.Fatal("publication rolled checkpoint back", index, string(image), err)
	}
	if cut.Identity != s.ApplicationIdentity() || cut.Contract != s.meta.Transfer.Contract {
		t.Fatal("published namespace binding lost")
	}
	cut.ConfState.Voters[0] = 99
	unchanged, err := s.PublishedApplicationCut()
	if err != nil || unchanged.ConfState.Voters[0] != 1 {
		t.Fatal("cut leaked membership aliases", err)
	}
}
func TestApplicationPublicationCheapReclaimDoesNotScanRetainedHistory(t *testing.T) {
	for _, method := range []string{"publish", "reclaim"} {
		t.Run(method, func(t *testing.T) {
			s := publicationStore(t, vfs.NewMem())
			generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
			generationApply(t, s, "new", KV{Key: []byte("a"), Value: []byte("new")})
			key := bankVersionKey(s.activeBank(), []byte("a"), 2)
			if err := s.db.Set(key, []byte("old-history poison"), nil); err != nil {
				t.Fatal(err)
			}
			var err error
			if method == "publish" {
				err = s.PublishSnapshot()
			} else {
				err = s.ReclaimApplication()
			}
			if err != nil {
				t.Fatal("ordinary publication scanned all retained rows", err)
			}
			if s.poison != nil || s.meta.Base != 3 {
				t.Fatal("cheap publication visited history or failed to reclaim")
			}
			if _, err := s.BeginApplicationExport(t.Context()); !errors.Is(err, ErrCorrupt) {
				t.Fatal("actual transfer did not detect old history corruption", err)
			}
		})
	}
}

func TestApplicationPublicationCapturePreservesNewerCheckpointAndSerializesReclaim(t *testing.T) {
	s := publicationStore(t, vfs.NewMem())
	generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
	reached, resume := make(chan struct{}), make(chan struct{})
	s.publicationCaptureHook = func() { close(reached); <-resume }
	result := make(chan error, 1)
	go func() { result <- s.PublishSnapshot() }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("capture stalled")
	}
	generationApply(t, s, "new", KV{Key: []byte("a"), Value: []byte("new")})
	close(resume)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil || cut.Index != 2 || cut.ImageHash != sha256.Sum256([]byte("old")) {
		t.Fatal("captured cut moved", cut, err)
	}
	index, image, err := s.Checkpoint()
	if err != nil || index != 3 || string(image) != "new" {
		t.Fatal("moving checkpoint overwritten", index, string(image), err)
	}
	reached, resume = make(chan struct{}), make(chan struct{})
	s.publicationCaptureHook = func() { close(reached); <-resume }
	go func() { result <- s.PublishSnapshot() }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("second capture stalled")
	}
	reclaim := make(chan error, 1)
	go func() { reclaim <- s.ReclaimApplication() }()
	select {
	case err := <-reclaim:
		t.Fatal("reclaim bypassed serialized publication", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(resume)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := <-reclaim; err != nil {
		t.Fatal("healthy reclaim stopped after external publication", err)
	}
	s.publicationCaptureHook = nil
	latest, err := s.PublishedApplicationCut()
	if err != nil || latest.Index != 3 {
		t.Fatal("reclaim used stale base", latest, err)
	}
	if err := s.ReclaimApplication(); err != nil {
		t.Fatal("already reclaimed progress became error", err)
	}
}

func TestApplicationPublicationDurableReferencePinsRetiredBankAcrossReopen(t *testing.T) {
	mem := vfs.NewCrashableMem()
	s := publicationStore(t, mem)
	generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	generationFixtureBankB(t, s) // private format fixture, not snapshot activation.
	if r := s.generationRefs[0]; r != nil && r.refs != 0 {
		t.Fatal("test still relies on transient old-bank references")
	}
	if s.meta.Gen.Banks[0].State != bankRetired {
		t.Fatal("durable publication failed to retain old bank")
	}
	before := readyMetadataFingerprint(t, s)
	if err := s.clearGenerationBank(0, 1); !errors.Is(err, ErrLimit) {
		t.Fatal("published bank reclaimed without replacement", err)
	}
	if readyMetadataFingerprint(t, s) != before {
		t.Fatal("durable-pin refusal changed keys")
	}
	c := Config{Dir: "db", FS: mem.CrashClone(vfs.CrashCloneCfg{}), Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits, PublishedCuts: s.meta.Gen.Publication.Limits}
	r, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	recovered, err := r.PublishedApplicationCut()
	if err != nil || recovered.ID != cut.ID || r.meta.Gen.Banks[0].State != bankRetired {
		t.Fatal("zero-handle reopen deleted published bank", recovered, err)
	}
	image, found, err := r.appGet(bankIndexKey(0, appRootTag, 2), r.meta.App.Policy.MaxImageBytes)
	if err != nil || !found || string(image) != "old" {
		t.Fatal("published old image lost", string(image), found, err)
	}
	generationApply(t, r, "new", KV{Key: []byte("a"), Value: []byte("new")})
	if err := r.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	latest, err := r.PublishedApplicationCut()
	if err != nil || latest.Index != 3 || latest.ID == cut.ID {
		t.Fatal("replacement publication missing", latest, err)
	}
	if r.meta.Gen.Publication.Bank != 1 || r.meta.Gen.Banks[0].State != bankFree {
		t.Fatal("new durable reference did not release old bank", r.meta.Gen)
	}
}
func TestApplicationPublicationHostileDescriptorArithmeticRejects(t *testing.T) {
	s := publicationStore(t, vfs.NewMem())
	c, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []uint64{math.MaxUint64, math.MaxUint64 / 3, math.MaxUint64/(3*(9+appFrameBytes)) + 1, 1 << 40} {
		hostile := c
		hostile.Index = index
		hostile.RetainedRecords = math.MaxUint64
		hostile.RetainedBytes = math.MaxUint64
		if _, err := cutID(hostile); !errors.Is(err, ErrInvalid) {
			t.Fatal("hostile descriptor arithmetic admitted", index, err)
		}
	}
	wrong := c
	wrong.RetainedBytes = 3*(9+appFrameBytes) - 1
	if _, err := cutID(wrong); !errors.Is(err, ErrInvalid) {
		t.Fatal("minimum-byte boundary ignored", err)
	}
}

func TestApplicationPublicationCorruptionAndStaleCaptureRemainDistinct(t *testing.T) {
	for _, kind := range []string{"root", "entry", "image", "recapture-entry", "chain", "ledger", "generation", "closed"} {
		t.Run(kind, func(t *testing.T) {
			s := publicationStore(t, vfs.NewMem())
			generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
			generationApply(t, s, "third", KV{Key: []byte("a"), Value: []byte("third")})
			var before [32]byte
			switch kind {
			case "root":
				if err := s.db.Set(bankIndexKey(s.activeBank(), appRootTag, 3), []byte("broken root"), nil); err != nil {
					t.Fatal(err)
				}
			case "entry":
				if err := s.db.Delete(entryKey(3), nil); err != nil {
					t.Fatal(err)
				}
			case "image":
				if err := s.db.Set(imageKey, []byte("broken image"), nil); err != nil {
					t.Fatal(err)
				}
			case "recapture-entry":
				s.publicationCaptureHook = func() {
					if err := s.db.Delete(entryKey(3), nil); err != nil {
						t.Fatal(err)
					}
					before = readyMetadataFingerprint(t, s)
				}
			case "chain":
				s.publicationCaptureHook = func() {
					if err := s.db.Delete(entryKey(2), nil); err != nil {
						t.Fatal(err)
					}
					before = readyMetadataFingerprint(t, s)
				}
			case "ledger":
				s.publicationCaptureHook = func() { s.mu.Lock(); s.meta.LogBytes = 1; s.mu.Unlock(); before = readyMetadataFingerprint(t, s) }
			case "generation":
				s.publicationCaptureHook = func() {
					s.mu.Lock()
					s.meta.Gen.Banks[0].Generation = 2
					s.meta.Gen.HighWater = 2
					s.mu.Unlock()
					before = readyMetadataFingerprint(t, s)
				}
			case "closed":
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			err := s.PublishSnapshot()
			if kind == "closed" {
				if !errors.Is(err, ErrClosed) {
					t.Fatal(err)
				}
				return
			}
			if kind == "generation" {
				if !errors.Is(err, ErrInvalid) || s.poison != nil {
					t.Fatal("stale capture classified as corruption", err)
				}
				if readyMetadataFingerprint(t, s) != before {
					t.Fatal("stale capture wrote publication")
				}
				return
			}
			if !errors.Is(err, ErrCorrupt) || s.poison == nil {
				t.Fatal("genuine corruption not fail-closed", kind, err)
			}
			if before != [32]byte{} && readyMetadataFingerprint(t, s) != before {
				t.Fatal("corrupt capture committed partial publication")
			}
		})
	}
}
func TestApplicationPublicationCloseWaitsForOwnedCaptureAndReopensComplete(t *testing.T) {
	mem := vfs.NewCrashableMem()
	s := publicationStore(t, mem)
	generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
	reached, resume := make(chan struct{}), make(chan struct{})
	s.publicationCaptureHook = func() { close(reached); <-resume }
	published, closed := make(chan error, 1), make(chan error, 1)
	go func() { published <- s.PublishSnapshot() }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("capture stalled")
	}
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		t.Fatal("close bypassed capture lifetime", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(resume)
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	r, err := Open(Config{Dir: "db", FS: mem.CrashClone(vfs.CrashCloneCfg{}), Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits, PublishedCuts: s.meta.Gen.Publication.Limits})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cut, err := r.PublishedApplicationCut()
	if err != nil || cut.Index != 2 || cut.ImageHash != sha256.Sum256([]byte("old")) {
		t.Fatal("close lost synced cut", cut, err)
	}
}
