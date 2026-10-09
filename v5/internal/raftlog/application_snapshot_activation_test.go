package raftlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func receiverStore(t *testing.T, fs vfs.FS, voter uint64) *Store {
	t.Helper()
	return receiverStoreAt(t, "db", fs, voter)
}
func receiverStoreAt(t *testing.T, dir string, fs vfs.FS, voter uint64) *Store {
	t.Helper()
	p, tc := transferConfig(voter)
	s, err := Open(Config{Dir: dir, FS: fs, Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{1000000}, Replication: ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1, 2, 3}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	return s
}
func preparedFixture(t *testing.T, s *Store) (*ApplicationImport, *PreparedApplicationSnapshot, *pb.Snapshot) {
	t.Helper()
	donor := receiverStore(t, vfs.NewMem(), 2)
	generationApply(t, donor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, _ := donor.PublishedApplicationCut()
	e, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	i, err := s.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range collectPublished(t, e, ReadBudget{1, 4096}) {
		if err := i.Append(t.Context(), c); err != nil {
			t.Fatal(err)
		}
	}
	if err := i.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	p, err := i.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeApplicationSnapshotDescriptor(m)
	if err != nil {
		t.Fatal(err)
	}
	snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(m.Index), Term: new(m.Term), ConfState: m.ConfState}, Data: data}
	return i, p, snap
}
func TestApplicationSnapshotActivationCoCommitsReadyAndOwnedRoot(t *testing.T) {
	fs := vfs.NewCrashableMem()
	s := receiverStore(t, fs, 1)
	view := viewApplication(t, s, 1)
	i, p, snap := preparedFixture(t, s)
	defer p.Close()
	claim, err := p.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	before := readyMetadataFingerprint(t, s)
	if _, err := s.PersistApplicationReady(raft.Ready{}, claim); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if before != readyMetadataFingerprint(t, s) {
		t.Fatal("empty ready changed KV")
	}
	rd := raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Vote: new(uint64(2)), Commit: new(uint64(2))}}
	root, err := s.PersistApplicationReady(rd, claim)
	if err != nil {
		t.Fatal(err)
	}
	if root.Generation != 2 || root.Index != 2 || string(root.Image) != "incoming" || s.meta.Gen.Active != 1 || s.meta.Hard.GetVote() != 2 || s.meta.App.Policy.LocalVoter != 1 || s.applicationImport != nil || !i.closed {
		t.Fatal(root, s.meta)
	}
	root.Image[0] ^= 1
	index, image, err := s.Checkpoint()
	if err != nil || index != 2 || string(image) != "incoming" {
		t.Fatal(index, string(image), err)
	}
	if row, found, err := view.Get(t.Context(), []byte("a"), 256); err != nil || found {
		t.Fatal("old view leaked new data", row, found, err)
	}
	assertActivationKV(t, s, 2, true)
	old, err := view.Root()
	if err != nil || old.Index != 1 || string(old.Image) != "initial" {
		t.Fatal(old, err)
	}
	if !bytes.Equal(s.meta.Rep.LastActivatedManifestID[:], claim.manifestID[:]) {
		t.Fatal("audit reference not bound")
	}
}

func TestApplicationSnapshotActivationNilZeroAndClaimOwnership(t *testing.T) {
	for _, i := range []*ApplicationImport{nil, {}} {
		if _, err := i.Prepare(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, p := range []*PreparedApplicationSnapshot{nil, {}} {
		if _, err := p.Claim(&pb.Snapshot{}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if err := p.Close(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, c := range []*ApplicationSnapshotClaim{nil, {}, {p: &PreparedApplicationSnapshot{}}} {
		if err := c.Close(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	s := receiverStore(t, vfs.NewMem(), 1)
	i, p, snap := preparedFixture(t, s)
	before := readyMetadataFingerprint(t, s)
	refs := s.generationRefs[i.bank].refs
	for _, bad := range []*pb.Snapshot{nil, {}, proto.Clone(snap).(*pb.Snapshot)} {
		if bad != nil && bad.GetMetadata() != nil {
			bad.Metadata.ConfState.AutoLeave = new(false)
		}
		if _, err := p.Claim(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if p.claim != nil || s.generationRefs[i.bank].refs != refs || len(s.applicationSnapshotClaims) != 0 || before != readyMetadataFingerprint(t, s) {
			t.Fatal("claim refusal leaked/mutated")
		}
	}
	c, err := p.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	snap.Data[0] ^= 1
	if _, err := p.Claim(c.expected); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := p.Close(); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := i.Abort(); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if before != readyMetadataFingerprint(t, s) {
		t.Fatal("revocation changed claimed bank")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if len(s.applicationSnapshotClaims) != 0 || s.generationRefs[i.bank].refs != refs {
		t.Fatal("claim close leaked")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if s.applicationImport != nil || s.meta.Gen.Banks[1].State != bankFree || s.pinnedApplicationBytes != 0 || p.descriptor != nil {
		t.Fatal("prepared close leaked")
	}
}

func TestApplicationSnapshotActivationClaimRefusalsAndReadyIdentity(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	_, p, snap := preparedFixture(t, s)
	defer p.Close()
	c, err := p.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	other := receiverStore(t, vfs.NewMem(), 1)
	cases := []struct {
		name  string
		rd    raft.Ready
		claim *ApplicationSnapshotClaim
		store *Store
		want  error
	}{
		{"nil", raft.Ready{Snapshot: snap}, nil, s, ErrInvalid},
		{"zero", raft.Ready{Snapshot: snap}, &ApplicationSnapshotClaim{}, s, ErrInvalid},
		{"other store", raft.Ready{Snapshot: snap}, c, other, ErrInvalid},
		{"empty", raft.Ready{}, c, s, ErrInvalid},
		{"no hard commit", raft.Ready{Snapshot: snap}, c, s, ErrInvalid},
		{"sender vote", raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Vote: new(uint64(9)), Commit: new(uint64(2))}}, c, s, ErrInvalid},
		{"entry quota", raft.Ready{Snapshot: snap, Entries: make([]*pb.Entry, 4097)}, c, s, ErrLimit},
	}
	changed := proto.Clone(snap).(*pb.Snapshot)
	changed.Data[0] ^= 1
	cases = append(cases, struct {
		name  string
		rd    raft.Ready
		claim *ApplicationSnapshotClaim
		store *Store
		want  error
	}{"descriptor", raft.Ready{Snapshot: changed}, c, s, ErrInvalid})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := readyMetadataFingerprint(t, tc.store)
			u, _ := tc.store.ApplicationTransferUsage()
			if root, err := tc.store.PersistApplicationReady(tc.rd, tc.claim); !errors.Is(err, tc.want) || root.Image != nil {
				t.Fatal(root, err)
			}
			after, _ := tc.store.ApplicationTransferUsage()
			if before != readyMetadataFingerprint(t, tc.store) || u != after || tc.store.poison != nil {
				t.Fatal("refusal changed state/ownership")
			}
		})
	}
	// A returning refusal retains its explicitly owned claim until Close.
	if p.claim != c || c.closed || len(s.applicationSnapshotClaims) != 1 {
		t.Fatal("refusal lost claim")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationSnapshotActivationOwnershipLedgerAndStoreClose(t *testing.T) {
	for _, activate := range []bool{false, true} {
		t.Run(fmt.Sprint(activate), func(t *testing.T) {
			s := receiverStore(t, vfs.NewMem(), 1)
			i, p, snap := preparedFixture(t, s)
			u, _ := s.ApplicationTransferUsage()
			if u.Prepared != 1 || u.PreparedBytes != preparedSnapshotFixedBytes || u.PinnedLogicalBytes != preparedSnapshotFixedBytes {
				t.Fatal(u)
			}
			c, err := p.Claim(snap)
			if err != nil {
				t.Fatal(err)
			}
			if activate {
				if _, err := s.PersistApplicationReady(raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Commit: new(uint64(2))}}, c); err != nil {
					t.Fatal(err)
				}
			}
			u, _ = s.ApplicationTransferUsage()
			if u.Claims != 1 || u.ClaimImageBytes != uint64(len("incoming")) || u.ClaimBytes != applicationSnapshotClaimFixedBytes+u.ClaimImageBytes {
				t.Fatal(u)
			}
			if activate && (u.Import || u.ImportImageBytes != 0 || u.Prepared != 0 || u.PreparedBytes != 0 || u.PinnedLogicalBytes != u.ClaimBytes) {
				t.Fatal("consumed image fell out of ledger", u)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if s.pinnedApplicationBytes != 0 || len(s.applicationSnapshotClaims) != 0 || c.ref != nil || c.image != nil || c.expected != nil || p.descriptor != nil || !p.closed || !i.closed {
				t.Fatal("store close leaked ownership")
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	fixedPrepared := unsafe.Sizeof(PreparedApplicationSnapshot{}) + applicationSnapshotDescriptorLimit
	fixedClaim := unsafe.Sizeof(ApplicationSnapshotClaim{}) + unsafe.Sizeof(PreparedApplicationSnapshot{}) + unsafe.Sizeof(pb.Snapshot{}) + unsafe.Sizeof(pb.SnapshotMetadata{}) + unsafe.Sizeof(pb.ConfState{}) + unsafe.Sizeof(generationRef{}) + 2*applicationSnapshotDescriptorLimit + 3*16
	if fixedPrepared > uintptr(preparedSnapshotFixedBytes) || fixedClaim > uintptr(applicationSnapshotClaimFixedBytes) {
		t.Fatal("fixed reservation too small", fixedPrepared, fixedClaim)
	}
}

func TestApplicationSnapshotActivationPinBoundariesAndAuditLifetime(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	i, p, snap := preparedFixture(t, s)
	tc := s.meta.Transfer
	pinned := s.pinnedApplicationBytes
	need := applicationSnapshotClaimFixedBytes + uint64(cap(i.manifest.Image))
	refs := s.generationRefs[i.bank].refs
	s.meta.Transfer.Limits.MaxPinnedLogicalBytes = pinned + need - 1
	before := readyMetadataFingerprint(t, s)
	if _, err := p.Claim(snap); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if p.claim != nil || len(s.applicationSnapshotClaims) != 0 || s.pinnedApplicationBytes != pinned || s.generationRefs[i.bank].refs != refs || before != readyMetadataFingerprint(t, s) {
		t.Fatal("one-short claim reserved")
	}
	s.meta.Transfer.Limits.MaxPinnedLogicalBytes = pinned + need
	c, err := p.Claim(snap)
	if err != nil {
		t.Fatal("exact claim allowance", err)
	}
	s.meta.Transfer = tc
	audit, err := s.ApplicationActivationAudit()
	if err != nil || audit != (ApplicationActivationAudit{}) {
		t.Fatal(audit, err)
	}
	if _, err := s.PersistApplicationReady(raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Commit: new(uint64(2))}}, c); err != nil {
		t.Fatal(err)
	}
	audit, err = s.ApplicationActivationAudit()
	if err != nil || audit.Index != 2 || audit.Term != 2 || audit.ManifestID != c.manifestID || audit.CutID != s.meta.Gen.Publication.ID {
		t.Fatal(audit, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	generationApplyTerm(t, s, 6, "later")
	if after, err := s.ApplicationActivationAudit(); err != nil || after != audit {
		t.Fatal("Applied rebound audit", after, err)
	}
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	if after, err := s.ApplicationActivationAudit(); err != nil || after != (ApplicationActivationAudit{}) {
		t.Fatal("local publication inherited manifest", after, err)
	}
	var nilStore *Store
	if _, err := nilStore.ApplicationActivationAudit(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	legacy := publicationStore(t, vfs.NewMem())
	if _, err := legacy.ApplicationActivationAudit(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplicationActivationAudit(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func generationApplyTerm(t *testing.T, s *Store, term uint64, image string) {
	t.Helper()
	index, previous, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	index++
	persist(t, s, term, index, ent(index, term, image))
	if err := s.InstallApplication(index, ApplicationBatch{BaseGeneration: s.activeGeneration(), BaseIndex: index - 1, BaseImageHash: sha256.Sum256(previous), Image: []byte(image)}); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationSnapshotActivationReopenRetainsOldHandlesAndBankCuts(t *testing.T) {
	fs := vfs.NewCrashableMem()
	s := receiverStore(t, fs, 1)
	oldView := viewApplication(t, s, 1)
	oldCut, _ := s.PublishedApplicationCut()
	oldExport, err := s.BeginPublishedApplicationExport(t.Context(), oldCut.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, p, snap := preparedFixture(t, s)
	validManifest := cloneSnapshotManifest(p.i.manifest)
	c, err := p.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PersistApplicationReady(raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Vote: new(uint64(3)), Commit: new(uint64(2))}, Entries: []*pb.Entry{ent(3, 5, "tail")}}, c); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if s.meta.Gen.Banks[0].State != bankRetired {
		t.Fatal("old references lost bank")
	}
	before := readyMetadataFingerprint(t, s)
	usage, _ := s.ApplicationTransferUsage()
	if _, err := s.BeginApplicationImport(t.Context(), validManifest); !errors.Is(err, ErrLimit) {
		t.Fatal("occupied bank did not backpressure", err)
	}
	afterUsage, _ := s.ApplicationTransferUsage()
	if before != readyMetadataFingerprint(t, s) || usage != afterUsage {
		t.Fatal("occupied-bank refusal mutated")
	}
	old := buildPublished(t, oldExport, ReadBudget{1, 4096})
	if old.Index != 1 || string(old.Image) != "initial" {
		t.Fatal(old)
	}
	collectPublished(t, oldExport, ReadBudget{1, 4096})
	if err := oldExport.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := oldView.Root()
	if err != nil || root.Generation != 1 || root.Index != 1 || string(root.Image) != "initial" {
		t.Fatal(root, err)
	}
	if err := oldView.Close(); err != nil {
		t.Fatal(err)
	}
	if s.meta.Gen.Banks[0].State != bankFree {
		t.Fatal("old retired bank not released")
	}
	cfg := Config{Dir: "db", FS: fs.CrashClone(vfs.CrashCloneCfg{}), Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits, PublishedCuts: s.meta.Gen.Publication.Limits, Replication: s.meta.Rep.Config}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.activeGeneration() != 2 || reopened.activeBank() != 1 || reopened.meta.Rep.LastActivatedManifestID == [32]byte{} || reopened.meta.Hard.GetVote() != 3 {
		t.Fatal(reopened.meta)
	}
	index, image, err := reopened.Checkpoint()
	if err != nil || index != 2 || string(image) != "incoming" {
		t.Fatal(index, string(image), err)
	}
	assertActivationKV(t, reopened, 2, true)
	if err := reopened.Scrub(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.ScrubApplication(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Snapshot(); !errors.Is(err, raft.ErrSnapshotTemporarilyUnavailable) {
		t.Fatal("unleased scalar snapshot allowed", err)
	}
	cut, _ := reopened.PublishedApplicationCut()
	e, err := reopened.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	if m.Index != 2 || string(m.Image) != "incoming" {
		t.Fatal(m)
	}
}

func awaitActivationError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("activation worker did not finish")
		return ErrInvalid
	}
}
func TestApplicationSnapshotActivationConcurrentClaimRevocationAndClose(t *testing.T) {
	for _, closeStore := range []bool{false, true} {
		t.Run(fmt.Sprint(closeStore), func(t *testing.T) {
			s := receiverStore(t, vfs.NewMem(), 1)
			i, p, snap := preparedFixture(t, s)
			c, err := p.Claim(snap)
			if err != nil {
				t.Fatal(err)
			}
			revoke := make(chan error, 1)
			go func() { revoke <- p.Close() }()
			if err := awaitActivationError(t, revoke); !errors.Is(err, ErrLimit) {
				t.Fatal(err)
			}
			go func() { revoke <- i.Abort() }()
			if err := awaitActivationError(t, revoke); !errors.Is(err, ErrLimit) {
				t.Fatal(err)
			}
			reached := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			s.activationCommitHook = func() { close(reached); <-release }
			activated := make(chan error, 1)
			finished := make(chan error, 1)
			activationDone := make(chan struct{})
			lifecycleDone := make(chan struct{})
			lifecycleStarted := false
			t.Cleanup(func() {
				unblock()
				select {
				case <-activationDone:
				case <-time.After(5 * time.Second):
					t.Error("activation cleanup join timed out")
				}
				if lifecycleStarted {
					select {
					case <-lifecycleDone:
					case <-time.After(5 * time.Second):
						t.Error("lifecycle cleanup join timed out")
					}
				}
			})
			go func() {
				defer close(activationDone)
				_, err := s.PersistApplicationReady(raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Commit: new(uint64(2))}}, c)
				activated <- err
			}()
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("activation hook not reached")
			}
			lifecycleStarted = true
			go func() {
				defer close(lifecycleDone)
				if closeStore {
					finished <- s.Close()
				} else {
					finished <- p.Close()
				}
			}()
			unblock()
			activationErr := awaitActivationError(t, activated)
			if activationErr != nil {
				t.Fatal(activationErr)
			}
			closeErr := awaitActivationError(t, finished)
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			if closeStore {
				if !c.closed || c.ref != nil || len(s.applicationSnapshotClaims) != 0 || s.pinnedApplicationBytes != 0 {
					t.Fatal("shutdown left consumed claim")
				}
			} else {
				if c.closed || !c.consumed || c.ref == nil || p.claim != c {
					t.Fatal("prepared caller revoked in-flight owner")
				}
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func assertActivationKV(t *testing.T, s *Store, index uint64, incoming bool) {
	t.Helper()
	v := viewApplication(t, s, index)
	row, found, err := v.Get(t.Context(), []byte("a"), 256)
	if err != nil || found != incoming || incoming && (row.Deleted || string(row.Value) != "incoming") {
		t.Fatal("wrong generation selected", row, found, err)
	}
	if row, found, err := v.Get(t.Context(), []byte("phantom"), 256); err != nil || found {
		t.Fatal("phantom row", row, found, err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationSnapshotActivationPrepareStatesAndDescriptor(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	i, p, snap := preparedFixture(t, s)
	again, err := i.Prepare()
	if err != nil || again != p {
		t.Fatal("prepare not idempotent", err)
	}
	descriptor := copyApplicationBytes(p.descriptor)
	if len(descriptor) > applicationSnapshotDescriptorLimit || cap(descriptor) != len(descriptor) {
		t.Fatal("descriptor capacity")
	}
	// Verified manifest/header mutations refuse before claiming or copying.
	big := proto.Clone(snap).(*pb.Snapshot)
	big.Data = make([]byte, applicationSnapshotDescriptorLimit+1)
	if _, err := p.Claim(big); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Claim(snap); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := i.Prepare(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	legacy := publicationStore(t, vfs.NewMem())
	donor := publicationStore(t, vfs.NewMem())
	cut, _ := donor.PublishedApplicationCut()
	e, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	plain, err := legacy.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Abort()
	if _, err := plain.Prepare(); !errors.Is(err, ErrInvalid) {
		t.Fatal("legacy prepared activation", err)
	}
	original := m
	m.Version = 0
	m.CutID = [32]byte{}
	if _, err := EncodeApplicationSnapshotDescriptor(m); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	m = original
	m.ImageHash[0] ^= 1
	if _, err := EncodeApplicationSnapshotDescriptor(m); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	fresh := receiverStore(t, vfs.NewMem(), 1)
	donor3 := receiverStore(t, vfs.NewMem(), 2)
	cc, _ := donor3.PublishedApplicationCut()
	ee, err := donor3.BeginPublishedApplicationExport(t.Context(), cc.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer ee.Close()
	mm := buildPublished(t, ee, ReadBudget{1, 4096})
	session, err := fresh.BeginApplicationImport(t.Context(), mm)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Abort()
	if _, err := session.Prepare(); !errors.Is(err, ErrInvalid) {
		t.Fatal("nonfinal prepared", err)
	}
	for _, chunk := range collectPublished(t, ee, ReadBudget{1, 4096}) {
		if err := session.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := session.Prepare(); !errors.Is(err, ErrInvalid) {
		t.Fatal("unverified prepared", err)
	}
	if err := session.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	tc := fresh.meta.Transfer
	fresh.meta.Transfer.Limits.MaxPinnedLogicalBytes = preparedSnapshotFixedBytes - 1
	before := readyMetadataFingerprint(t, fresh)
	if _, err := session.Prepare(); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if before != readyMetadataFingerprint(t, fresh) || session.prepared != nil || fresh.pinnedApplicationBytes != 0 {
		t.Fatal("prepare quota mutated")
	}
	fresh.meta.Transfer.Limits.MaxPinnedLogicalBytes = preparedSnapshotFixedBytes
	prepared, err := session.Prepare()
	if err != nil {
		t.Fatal("exact prepared allowance", err)
	}
	fresh.meta.Transfer = tc
	// Stale physical-generation binding is clean refusal, not corruption.
	prepared.baseGeneration++
	if _, err := prepared.Claim(&pb.Snapshot{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	prepared.baseGeneration--
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationSnapshotActivationReadyTailAndBudgetRefusals(t *testing.T) {
	valid := func(snap *pb.Snapshot) raft.Ready {
		return raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Vote: new(uint64(2)), Commit: new(uint64(2))}, Entries: []*pb.Entry{ent(3, 5, "tail")}}
	}
	reference := receiverStore(t, vfs.NewMem(), 1)
	_, rp, rs := preparedFixture(t, reference)
	rc, err := rp.Claim(rs)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.PersistApplicationReady(valid(rs), rc); err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
	boundary := int(metadataBytes(reference.meta)) + readyEnvelopeBytes + len(rs.Data) + len("tail") + frameOverhead + 9 + 32
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			s := receiverStoreBounded(t)
			_, p, snap := preparedFixture(t, s)
			c, err := p.Claim(snap)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			defer p.Close()
			s.limits.MaxReadyBytes = boundary + delta
			if err := s.limits.Validate(); err != nil {
				t.Fatal("boundary fixture invalid", err)
			}
			before := readyMetadataFingerprint(t, s)
			root, err := s.PersistApplicationReady(valid(snap), c)
			if delta < 0 {
				if !errors.Is(err, ErrLimit) || root.Image != nil || before != readyMetadataFingerprint(t, s) || s.poison != nil {
					t.Fatal("exact boundary refusal", root, err)
				}
			} else {
				if err != nil || root.Index != 2 {
					t.Fatal(root, err)
				}
				if err := s.Scrub(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	for _, kind := range []string{"nil entry", "wrong index", "wrong term", "config entry", "committed config", "too large", "stale commit", "term rollback"} {
		t.Run(kind, func(t *testing.T) {
			s := receiverStore(t, vfs.NewMem(), 1)
			_, p, snap := preparedFixture(t, s)
			c, err := p.Claim(snap)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			defer c.Close()
			rd := valid(snap)
			want := ErrInvalid
			switch kind {
			case "nil entry":
				rd.Entries[0] = nil
			case "wrong index":
				rd.Entries[0].Index = new(uint64(4))
			case "wrong term":
				rd.Entries[0].Term = new(uint64(6))
			case "config entry":
				rd.Entries[0].Type = pb.EntryConfChange.Enum()
			case "committed config":
				rd.CommittedEntries = []*pb.Entry{{Type: pb.EntryConfChangeV2.Enum()}}
			case "too large":
				rd.Entries[0].Data = make([]byte, s.limits.MaxEntryBytes)
				want = ErrLimit
			case "stale commit":
				rd.Commit = new(uint64(1))
			case "term rollback":
				rd.Term = new(uint64(0))
			}
			before := readyMetadataFingerprint(t, s)
			if _, err := s.PersistApplicationReady(rd, c); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if before != readyMetadataFingerprint(t, s) || s.poison != nil {
				t.Fatal("tail refusal changed store")
			}
		})
	}
}

func receiverStoreBounded(t *testing.T) *Store {
	t.Helper()
	p, tc := transferConfig(1)
	l := DefaultLimits()
	l.MaxEntryBytes = 256
	l.MaxReadBytes = 512
	l.MaxReadyBytes = 4096
	s, err := Open(Config{Dir: "db", FS: vfs.NewMem(), Create: true, Limits: l, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{1000000}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1, 2, 3}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestApplicationSnapshotActivationRejectsStaleReadyAndChecksCommittedTerm(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	_, p, snap := preparedFixture(t, s)
	c, err := p.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	defer c.Close()
	// Existing consensus progress can make a prepared cut stale without bank ABA.
	generationApplyTerm(t, s, 2, "local")
	before := readyMetadataFingerprint(t, s)
	if _, err := s.PersistApplicationReady(raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Commit: new(uint64(2))}}, c); !errors.Is(err, ErrInvalid) {
		t.Fatal("stale snapshot accepted", err)
	}
	if before != readyMetadataFingerprint(t, s) {
		t.Fatal("stale activation wrote")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	// A newer cut exercises the constant committed-term lookup. An older app
	// envelope is poisoned deliberately: activation must not scan history.
	donor := receiverStore(t, vfs.NewMem(), 2)
	generationApply(t, donor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	generationApply(t, donor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, _ := donor.PublishedApplicationCut()
	e, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	imp, err := s.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range collectPublished(t, e, ReadBudget{1, 4096}) {
		if err := imp.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := imp.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imp.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeApplicationSnapshotDescriptor(m)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(m.Index), Term: new(m.Term), ConfState: m.ConfState}, Data: data}
	claim, err := prepared.Claim(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	if err := s.db.Set(bankIndexKey(0, appRootTag, 1), make([]byte, appFrameBytes), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	root, err := s.PersistApplicationReady(raft.Ready{Snapshot: snapshot, HardState: &pb.HardState{Term: new(uint64(5)), Commit: new(uint64(3))}}, claim)
	if err != nil || root.Index != 3 {
		t.Fatal("activation scanned old application history", root, err)
	}
	assertActivationKV(t, s, 3, true)
	if _, err := s.PersistApplicationReady(raft.Ready{Snapshot: snapshot}, claim); !errors.Is(err, ErrClosed) {
		t.Fatal("consumed claim reused", err)
	}
}
