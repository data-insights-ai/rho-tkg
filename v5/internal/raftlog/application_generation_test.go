package raftlog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func generationStore(t *testing.T, fs vfs.FS) *Store {
	t.Helper()
	p, tc := transferConfig(1)
	s, err := Open(Config{Dir: "db", FS: fs, Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}})
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
func TestApplicationGenerationMissingAndWrongBaseCannotReuseEqualImage(t *testing.T) {
	s := generationStore(t, vfs.NewCrashableMem())
	persist(t, s, 2, 2, ent(2, 2, "same image"))
	v := viewApplication(t, s, 1)
	root, err := v.Root()
	if err != nil {
		t.Fatal(err)
	}
	if root.Generation != 1 {
		t.Fatal("initial generation", root.Generation)
	}
	before := readyMetadataFingerprint(t, s)
	for _, generation := range []uint64{0, 2} {
		b := ApplicationBatch{BaseGeneration: generation, BaseIndex: root.Index, BaseImageHash: root.ImageHash, Image: root.Image}
		if err := s.InstallApplication(2, b); !errors.Is(err, ErrInvalid) {
			t.Errorf("generation %d admitted equal-image stale batch: %v", generation, err)
		}
		if readyMetadataFingerprint(t, s) != before {
			t.Fatal("generation refusal changed durable keys")
		}
	}
	b := ApplicationBatch{BaseGeneration: root.Generation, BaseIndex: root.Index, BaseImageHash: root.ImageHash, Image: root.Image}
	if err := s.InstallApplication(2, b); err != nil {
		t.Fatal("matching generation refused", err)
	}
	getApplication(t, v, "phantom", "", false, false)
}

func generationApply(t *testing.T, s *Store, image string, writes ...KV) uint64 {
	t.Helper()
	index, previous, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	index++
	persist(t, s, 2, index, ent(index, 2, image))
	if err := s.InstallApplication(index, ApplicationBatch{BaseGeneration: s.activeGeneration(), BaseIndex: index - 1, BaseImageHash: sha256.Sum256(previous), Image: []byte(image), Writes: writes, Changes: []byte("change:" + image), Outcome: []byte("outcome:" + image)}); err != nil {
		t.Fatal(err)
	}
	return index
}

// This fixture writes a valid alternate-bank format directly. It is NOT an
// activation API or evidence of atomic Raft snapshot activation.
func generationFixtureBankB(t *testing.T, s *Store) {
	t.Helper()
	donor := generationStore(t, vfs.NewMem())
	generationApply(t, donor, "old", KV{Key: []byte("a"), Value: []byte("other")}, KV{Key: []byte("empty"), Value: []byte{}})
	e, err := donor.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	manifest, err := e.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	i, err := s.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range exportChunks(t, e) {
		if err := i.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.meta
	m.Gen.Active = i.bank
	m.Gen.Banks[0].State = bankRetired
	target := &m.Gen.Banks[i.bank]
	target.State = bankActive
	target.ReservedBytes, target.ReservedRecords = 0, 0
	m.App.Bytes, m.App.Records, m.App.Through = i.state.bytes, i.state.rows, manifest.Index
	batch := s.db.NewBatch()
	if err := batch.Delete(dormantKey, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.commit(m, batch); err != nil {
		t.Fatal(err)
	}
	s.applicationImport = nil
	i.closed = true
	i.manifest.Image = nil
	if err := s.releaseGeneration(i.ref); err != nil {
		t.Fatal(err)
	}
}
func TestApplicationGenerationBankBReadInstallAndCapturedViews(t *testing.T) {
	fs := vfs.NewCrashableMem()
	s := generationStore(t, fs)
	generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")}, KV{Key: []byte("empty"), Value: []byte{}})
	old := viewApplication(t, s, 2)
	oldRoot, err := old.Root()
	if err != nil {
		t.Fatal(err)
	}
	original, err := s.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	generationFixtureBankB(t, s)
	getApplication(t, viewApplication(t, s, 2), "a", "other", true, false)
	persist(t, s, 2, 3, ent(3, 2, "new"))
	stale := ApplicationBatch{BaseGeneration: oldRoot.Generation, BaseIndex: oldRoot.Index, BaseImageHash: oldRoot.ImageHash, Image: []byte("new"), Writes: []KV{{Key: []byte("a"), Value: []byte("new")}, {Key: []byte("empty"), Deleted: true}}, Changes: []byte("change:new"), Outcome: []byte("outcome:new")}
	beforeStale := readyMetadataFingerprint(t, s)
	if err := s.InstallApplication(3, stale); !errors.Is(err, ErrInvalid) {
		t.Fatal("old physical generation reused identical index/image", err)
	}
	if readyMetadataFingerprint(t, s) != beforeStale {
		t.Fatal("ABA stale batch changed rows")
	}
	stale.BaseGeneration = s.ApplicationGeneration()
	if err := s.InstallApplication(3, stale); err != nil {
		t.Fatal("matching bankB batch refused", err)
	}
	getApplication(t, old, "a", "old", true, false)
	current := viewApplication(t, s, 3)
	getApplication(t, current, "a", "new", true, false)
	getApplication(t, current, "empty", "", true, true)
	root, err := current.Root()
	if err != nil || root.Generation != 2 || root.Index != 3 || string(root.Image) != "new" {
		t.Fatal(root, err)
	}
	page, err := current.Scan(t.Context(), nil, nil, nil, ReadBudget{8, 1024})
	if err != nil || !page.Complete || len(page.Rows) != 1 || string(page.Rows[0].Key) != "a" || string(page.Rows[0].Value) != "new" {
		t.Fatal(page, err)
	}
	for _, outcome := range []bool{false, true} {
		raw, err := s.ApplicationRecord(t.Context(), 3, outcome, 1024)
		prefix := "change:"
		if outcome {
			prefix = "outcome:"
		}
		if err != nil || string(raw) != prefix+"new" {
			t.Fatal(string(raw), err)
		}
	}
	if err := s.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
	e, err := s.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	manifest, err := e.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	if _, err := s.BeginApplicationImport(t.Context(), manifest); !errors.Is(err, ErrLimit) {
		t.Fatal("retired bank reused with old view", err)
	}
	if readyMetadataFingerprint(t, s) != before {
		t.Fatal("busy-bank refusal changed keys")
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginApplicationImport(t.Context(), manifest); !errors.Is(err, ErrLimit) {
		t.Fatal("export-owned retired bank reused", err)
	}
	var originalValue string
	for _, chunk := range exportChunks(t, original) {
		if err := walkSnapshotChunk(chunk.Data, manifest.Contract, func(k, v []byte) error {
			if k[0] == appDataTag {
				key, index, err := decodeAppKey(k, manifest.Contract.MaxKeyBytes)
				if err != nil {
					return err
				}
				if string(key) == "a" && index == 2 {
					value, _, err := inspectAppFrame(k, v, manifest.Contract.MaxValueBytes)
					if err != nil {
						return err
					}
					originalValue = string(value)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if originalValue != "old" {
		t.Fatal("captured export followed active bank", originalValue)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	i, err := s.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	if i.bank != 0 || i.generation != 3 {
		t.Fatal("wrong reusable bank/generation", i.bank, i.generation)
	}
	for _, c := range exportChunks(t, e) {
		if err := i.Append(t.Context(), c); err != nil {
			t.Fatal(err)
		}
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := i.Abort(); err != nil {
		t.Fatal(err)
	}
	getApplication(t, current, "a", "new", true, false)
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal("bankB snapshot root refused", err)
	}
	freshExport, err := s.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer freshExport.Close()
	freshManifest, err := freshExport.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	dormant, err := s.BeginApplicationImport(t.Context(), freshManifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range exportChunks(t, freshExport) {
		if err := dormant.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := dormant.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer dormant.Abort()
	c := Config{Dir: "db", FS: fs.CrashClone(vfs.CrashCloneCfg{}), Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits}
	r, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	getApplication(t, viewApplication(t, r, 3), "a", "new", true, false)
	getApplication(t, viewApplication(t, r, 2), "a", "other", true, false)
	if r.meta.Gen.Active != 1 || r.meta.Gen.Banks[0].State != bankFree || r.meta.Gen.Banks[0].Generation != dormant.generation {
		t.Fatal("reopen cleared wrong bank/generation", r.meta.Gen)
	}
	if err := r.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func generationBoundedStore(t *testing.T, limits ApplicationGenerationLimits) *Store {
	t.Helper()
	p, tc := transferConfig(1)
	p.MaxInstallWrites = 1
	p.MaxInstallBytes = 1320
	tc.Contract = ApplicationContractForPolicy(p)
	s, err := Open(Config{Dir: "db", FS: vfs.NewCrashableMem(), Create: true, Application: p, Transfer: tc, Generations: limits})
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
func TestApplicationGenerationReservationsProtectPendingAndStagingWork(t *testing.T) {
	const initialBytes = 3*(9+appFrameBytes) + 7
	for _, records := range []bool{false, true} {
		t.Run(fmt.Sprintf("record-budget=%v", records), func(t *testing.T) {
			limits := ApplicationGenerationLimits{MaxBytes: 1 << 20, MaxRecords: 1_000_000}
			if records {
				limits.MaxRecords = 3 + 4 + 3
			} else {
				limits.MaxBytes = initialBytes + 1320 + initialBytes
			}
			s := generationBoundedStore(t, limits)
			e, err := s.BeginApplicationExport(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			manifest, err := e.Manifest()
			if err != nil {
				t.Fatal(err)
			}
			persist(t, s, 2, 2, ent(2, 2, "pending"))
			i, err := s.BeginApplicationImport(t.Context(), manifest)
			if err != nil {
				t.Fatal("exact-fit pending+staging refused", err)
			}
			before := readyMetadataFingerprint(t, s)
			if err := s.Persist(raft.Ready{Entries: []*pb.Entry{ent(3, 2, "steals quota")}}); !errors.Is(err, ErrLimit) {
				t.Fatal("new pending entry stole staged capacity", err)
			}
			if readyMetadataFingerprint(t, s) != before {
				t.Fatal("reservation refusal changed keys")
			}
			for _, chunk := range exportChunks(t, e) {
				if err := i.Append(t.Context(), chunk); err != nil {
					t.Fatal("reserved complete import cannot append", err)
				}
			}
			if err := i.Verify(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := s.InstallApplication(2, ApplicationBatch{BaseGeneration: 1, BaseIndex: 1, BaseImageHash: sha256.Sum256([]byte("initial")), Image: []byte("pending")}); err != nil {
				t.Fatal("import stole already-promised installation", err)
			}
			if err := i.Abort(); err != nil {
				t.Fatal(err)
			}
			if err := s.ScrubApplication(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, delta := range []int{-1, 0} {
		t.Run(fmt.Sprintf("pending-import-boundary=%d", delta), func(t *testing.T) {
			s := generationBoundedStore(t, ApplicationGenerationLimits{MaxBytes: uint64(initialBytes + 1320 + initialBytes + delta), MaxRecords: 1_000_000})
			e, err := s.BeginApplicationExport(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer e.Close()
			manifest, err := e.Manifest()
			if err != nil {
				t.Fatal(err)
			}
			persist(t, s, 2, 2, ent(2, 2, "pending"))
			before := readyMetadataFingerprint(t, s)
			i, err := s.BeginApplicationImport(t.Context(), manifest)
			if delta < 0 {
				if !errors.Is(err, ErrLimit) {
					t.Fatal("import consumed pending reservation", err)
				}
				if readyMetadataFingerprint(t, s) != before {
					t.Fatal("failed reservation advanced generation or keys")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := i.Abort(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	s := generationBoundedStore(t, ApplicationGenerationLimits{MaxBytes: initialBytes + 2*1320, MaxRecords: 1_000_000})
	e, err := s.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	manifest, err := e.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	i, err := s.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	if err := s.AdmitApplication(0); !errors.Is(err, ErrLimit) {
		t.Fatal("proposal and no-op headroom ignored staged reservation", err)
	}
	if readyMetadataFingerprint(t, s) != before {
		t.Fatal("proposal refusal wrote keys")
	}
	if err := i.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := s.AdmitApplication(0); err != nil {
		t.Fatal("abort did not release unused full-manifest reservation", err)
	}
}
func TestApplicationGenerationVerifierPinsBankUntilItFinishes(t *testing.T) {
	s := generationStore(t, vfs.NewMem())
	generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
	e, err := s.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	manifest, err := e.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	chunks := exportChunks(t, e)
	i, err := s.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range chunks {
		if err := i.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
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
	if err := i.Abort(); err != nil {
		close(resume)
		t.Fatal(err)
	}
	before := readyMetadataFingerprint(t, s)
	if _, err := s.BeginApplicationImport(t.Context(), manifest); !errors.Is(err, ErrLimit) {
		close(resume)
		t.Fatal("verifier-owned bank reused", err)
	}
	if readyMetadataFingerprint(t, s) != before {
		close(resume)
		t.Fatal("busy verifier changed keys")
	}
	if s.meta.Gen.Banks[1].State != bankRetired || s.meta.Gen.Banks[1].Bytes == 0 {
		close(resume)
		t.Fatal("abort deleted verifier-owned bank")
	}
	close(resume)
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("verifier did not release bank")
	}
	replacement, err := s.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.generation != i.generation+1 {
		t.Fatal("reused generation", replacement.generation, i.generation)
	}
	for _, chunk := range chunks {
		if err := replacement.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
	}
	before = readyMetadataFingerprint(t, s)
	if err := i.Abort(); err != nil {
		t.Fatal(err)
	}
	if readyMetadataFingerprint(t, s) != before {
		t.Fatal("late old Abort deleted replacement")
	}
	if err := replacement.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := replacement.Abort(); err != nil {
		t.Fatal(err)
	}
	getApplication(t, viewApplication(t, s, 2), "a", "old", true, false)
}

func TestApplicationGenerationExportDuringImportChargesExactlyOnce(t *testing.T) {
	for _, generations := range []bool{false, true} {
		t.Run(fmt.Sprintf("generation-mode=%v", generations), func(t *testing.T) {
			p, tc := transferConfig(1)
			config := Config{Dir: "db", Create: true, Application: p, Transfer: tc}
			if generations {
				config.Generations = ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}
			}
			initialize := func(c Config) *Store {
				t.Helper()
				s, err := Open(c)
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
			stage := func(s *Store) (*ApplicationExport, *ApplicationImport, uint64) {
				t.Helper()
				first, err := s.BeginApplicationExport(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := first.Close(); err != nil {
						t.Error(err)
					}
				})
				manifest, err := first.Manifest()
				if err != nil {
					t.Fatal(err)
				}
				i, err := s.BeginApplicationImport(t.Context(), manifest)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := i.Abort(); err != nil {
						t.Error(err)
					}
				})
				for _, chunk := range exportChunks(t, first) {
					if err := i.Append(t.Context(), chunk); err != nil {
						t.Fatal(err)
					}
				}
				raw, err := encodeMeta(s.meta)
				if err != nil {
					t.Fatal(err)
				}
				expected := s.meta.App.Bytes + s.meta.LogBytes + s.meta.ImageBytes + s.meta.SnapBytes + uint64(len(raw)+3) + i.state.bytes + uint64(len(i.descriptor())+len(dormantKey))
				return first, i, expected
			}
			config.FS = vfs.NewMem()
			probe := initialize(config)
			first, _, expected := stage(probe)
			total := first.pinBytes + expected
			for _, delta := range []int{-1, 0} {
				t.Run(fmt.Sprintf("pinned-boundary=%d", delta), func(t *testing.T) {
					c := config
					c.FS = vfs.NewMem()
					c.Transfer.Limits.MaxPinnedLogicalBytes = total
					if delta < 0 {
						c.Transfer.Limits.MaxPinnedLogicalBytes--
					}
					s := initialize(c)
					first, i, want := stage(s)
					before := readyMetadataFingerprint(t, s)
					beforeCursor, err := i.Status()
					if err != nil {
						t.Fatal(err)
					}
					pinned := s.pinnedApplicationBytes
					exports := len(s.applicationExports)
					second, err := s.BeginApplicationExport(t.Context())
					if delta < 0 {
						if second != nil || !errors.Is(err, ErrLimit) {
							if second != nil {
								_ = second.Close()
							}
							t.Fatal("one-short pinned ledger admitted", err)
						}
						afterCursor, err := i.Status()
						if err != nil || afterCursor != beforeCursor {
							t.Fatal("export refusal changed import cursor", afterCursor, err)
						}
						if readyMetadataFingerprint(t, s) != before || s.pinnedApplicationBytes != pinned || len(s.applicationExports) != exports || s.poison != nil {
							t.Fatal("export refusal changed store/pin ledger")
						}
					} else {
						if err != nil {
							t.Fatal("exact pinned ledger refused", err)
						}
						if second.pinBytes != want || s.pinnedApplicationBytes != first.pinBytes+want || s.pinnedApplicationBytes != c.Transfer.Limits.MaxPinnedLogicalBytes {
							t.Fatal("staged bank omitted or counted twice", second.pinBytes, want, s.pinnedApplicationBytes)
						}
						if err := second.Close(); err != nil {
							t.Fatal(err)
						}
						if s.pinnedApplicationBytes != first.pinBytes {
							t.Fatal("export close released wrong generation ledger")
						}
					}
					if err := i.Verify(t.Context()); err != nil {
						t.Fatal("active import changed by export", err)
					}
					if err := s.ScrubApplication(t.Context()); err != nil {
						t.Fatal("active data changed by export", err)
					}
				})
			}
		})
	}
}
