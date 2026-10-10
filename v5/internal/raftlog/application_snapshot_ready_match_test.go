package raftlog

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func readyMatchPrepared(t *testing.T, s *Store, id ApplicationSemanticContractID) (*PreparedApplicationSnapshot, *ApplicationSnapshotClaim, *pb.Snapshot, ApplicationSnapshotManifest) {
	t.Helper()
	donor := semanticStore(t, vfs.NewMem(), id, 2)
	generationApply(t, donor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := donor.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := donor.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	m := buildPublished(t, export, ReadBudget{1, 4096})
	i, err := s.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range collectPublished(t, export, ReadBudget{1, 4096}) {
		if err := i.Append(t.Context(), chunk); err != nil {
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
	c, err := p.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return p, c, snap, m
}

func readyMatchRaw(t *testing.T, s *Store, snap *pb.Snapshot) (*raft.RawNode, raft.Ready) {
	t.Helper()
	index, _, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := raft.NewRawNode(&raft.Config{ID: 1, Storage: s, Applied: index, ElectionTick: 10, HeartbeatTick: 1, MaxSizePerMsg: 1 << 20, MaxCommittedSizePerReady: 1 << 20, MaxInflightMsgs: 16, AsyncStorageWrites: false})
	if err != nil {
		t.Fatal(err)
	}
	if raw.HasReady() {
		t.Fatal("fresh follower unexpectedly has Ready")
	}
	// The owned Raft input may be normalized by the pinned dependency. The
	// claim was captured before Step, and the returned Ready is never edited.
	if err := raw.Step(&pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(5)), Snapshot: proto.Clone(snap).(*pb.Snapshot)}); err != nil {
		t.Fatal(err)
	}
	if !raw.HasReady() {
		t.Fatal("snapshot input produced no Ready")
	}
	return raw, raw.Ready()
}

func readyMatchWire(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type readyMatchOwnership struct {
	usage                                 ApplicationTransferUsage
	fingerprint                           [32]byte
	refs                                  [2]generationRef
	refOwners                             [2]*generationRef
	claims                                map[*ApplicationSnapshotClaim]struct{}
	importOwner                           *ApplicationImport
	claimOwner                            *ApplicationSnapshotClaim
	preparedBytes, claimBytes             uint64
	preparedClosed, closed, consumed      bool
	descriptor, expected, image, metadata []byte
}

func (a readyMatchOwnership) equal(b readyMatchOwnership) bool {
	return a.usage == b.usage && a.fingerprint == b.fingerprint && a.refs == b.refs && a.refOwners == b.refOwners && maps.Equal(a.claims, b.claims) && a.importOwner == b.importOwner && a.claimOwner == b.claimOwner && a.preparedBytes == b.preparedBytes && a.claimBytes == b.claimBytes && a.preparedClosed == b.preparedClosed && a.closed == b.closed && a.consumed == b.consumed && bytes.Equal(a.descriptor, b.descriptor) && bytes.Equal(a.expected, b.expected) && bytes.Equal(a.image, b.image) && bytes.Equal(a.metadata, b.metadata)
}

func readyMatchOwner(t *testing.T, s *Store, p *PreparedApplicationSnapshot, c *ApplicationSnapshotClaim) readyMatchOwnership {
	t.Helper()
	u, err := s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	o := readyMatchOwnership{usage: u, fingerprint: readyMetadataFingerprint(t, s), refOwners: s.generationRefs, claims: maps.Clone(s.applicationSnapshotClaims), importOwner: s.applicationImport, claimOwner: p.claim, preparedBytes: p.reservation, claimBytes: c.reservation, preparedClosed: p.closed, closed: c.closed, consumed: c.consumed, descriptor: bytes.Clone(p.descriptor), expected: readyMatchWire(t, c.expected), image: bytes.Clone(c.image), metadata: metadata}
	for j, ref := range s.generationRefs {
		if ref != nil {
			o.refs[j] = *ref
		}
	}
	return o
}

func TestApplicationSnapshotActualReadyFalsePresenceActivatesAS2AndAS3(t *testing.T) {
	for _, id := range []ApplicationSemanticContractID{{}, {7}} {
		t.Run(fmt.Sprintf("bound=%t", id != (ApplicationSemanticContractID{})), func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			s := semanticStore(t, fs, id, 1)
			old := viewApplication(t, s, 1)
			defer old.Close()
			p, c, input, m := readyMatchPrepared(t, s, id)
			descriptor := bytes.Clone(p.descriptor)
			expected := readyMatchWire(t, c.expected)
			if input.Metadata.ConfState.AutoLeave != nil || m.ConfState.AutoLeave != nil || m.Version != semanticManifestVersion(id) {
				t.Fatal("fixture is not original donor evidence", m.Version)
			}
			_, rd := readyMatchRaw(t, s, input)
			if raft.IsEmptySnap(rd.Snapshot) || rd.Snapshot.Metadata.ConfState.AutoLeave == nil || rd.Snapshot.Metadata.ConfState.GetAutoLeave() || proto.Equal(rd.Snapshot, c.expected) {
				t.Fatal("real normalization difference not witnessed")
			}
			actual := readyMatchWire(t, rd.Snapshot)
			root, err := s.PersistApplicationReady(rd, c)
			if err != nil || root.Generation != 2 || root.Index != m.Index || root.ImageHash != m.ImageHash || !bytes.Equal(root.Image, m.Image) {
				t.Fatal(root, err)
			}
			if !bytes.Equal(actual, readyMatchWire(t, rd.Snapshot)) || !bytes.Equal(expected, readyMatchWire(t, c.expected)) || !bytes.Equal(descriptor, c.expected.Data) || c.expected.Metadata.ConfState.AutoLeave != nil || m.ConfState.AutoLeave != nil {
				t.Fatal("Ready matcher changed caller or donor evidence")
			}
			cut, err := s.PublishedApplicationCut()
			if err != nil || cut.ID != m.CutID || cut.SemanticContractID != id {
				t.Fatal("cut identity changed", cut, err)
			}
			audit, err := s.ApplicationActivationAudit()
			if err != nil || audit.CutID != m.CutID || audit.ManifestID != publishedManifestID(m) || audit.Index != m.Index || audit.Term != m.Term {
				t.Fatal(audit, err)
			}
			assertActivationKV(t, s, m.Index, true)
			if row, found, err := old.Get(t.Context(), []byte("a"), 256); err != nil || found {
				t.Fatal("activation leaked into retained old root", row, found, err)
			}
			if r, err := old.Root(); err != nil || r.Index != 1 || string(r.Image) != "initial" {
				t.Fatal("old root changed", r, err)
			}
			cfg := semanticConfig(s, fs.CrashClone(vfs.CrashCloneCfg{}))
			reopened, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			index, image, err := reopened.Checkpoint()
			if err != nil || index != root.Index || !bytes.Equal(image, root.Image) || reopened.activeGeneration() != root.Generation || reopened.meta.Conf.AutoLeave != nil {
				t.Fatal("reopen changed exact donor root/membership", index, image, err)
			}
			after, err := reopened.PublishedApplicationCut()
			if err != nil || !bytes.Equal(readyMatchWire(t, after.ConfState), readyMatchWire(t, cut.ConfState)) {
				t.Fatal("reopen changed cut membership", after, err)
			}
			wantCut := cut
			after.ConfState, wantCut.ConfState = nil, nil
			if after != wantCut {
				t.Fatal("reopen changed cut fields", after, wantCut)
			}
			if after, err := reopened.ApplicationActivationAudit(); err != nil || after != audit {
				t.Fatal("reopen changed audit", after, err)
			}
			assertActivationKV(t, reopened, m.Index, true)
		})
	}
}

func TestApplicationSnapshotActualReadyMismatchPreservesClaimAndStorage(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	p, c, snap, _ := readyMatchPrepared(t, s, ApplicationSemanticContractID{})
	_, rd := readyMatchRaw(t, s, snap)
	cases := []struct {
		name   string
		change func(*pb.Snapshot)
	}{
		{"data", func(x *pb.Snapshot) { x.Data[0] ^= 1 }},
		{"oversized-data", func(x *pb.Snapshot) { x.Data = make([]byte, applicationSnapshotDescriptorLimit+1) }},
		{"snapshot-unknown", func(x *pb.Snapshot) { x.ProtoReflect().SetUnknown([]byte{0xf8, 7, 1}) }},
		{"metadata-nil", func(x *pb.Snapshot) { x.Metadata = nil }},
		{"metadata-unknown", func(x *pb.Snapshot) { x.Metadata.ProtoReflect().SetUnknown([]byte{0xf8, 7, 1}) }},
		{"index", func(x *pb.Snapshot) { x.Metadata.Index = new(x.GetMetadata().GetIndex() + 1) }},
		{"index-nil", func(x *pb.Snapshot) { x.Metadata.Index = nil }},
		{"term", func(x *pb.Snapshot) { x.Metadata.Term = new(x.GetMetadata().GetTerm() + 1) }},
		{"term-nil", func(x *pb.Snapshot) { x.Metadata.Term = nil }},
		{"membership-nil", func(x *pb.Snapshot) { x.Metadata.ConfState = nil }},
		{"membership-unknown", func(x *pb.Snapshot) { x.Metadata.ConfState.ProtoReflect().SetUnknown([]byte{0xf8, 7, 1}) }},
		{"auto-leave-true", func(x *pb.Snapshot) { x.Metadata.ConfState.AutoLeave = new(true) }},
		{"voter-changed", func(x *pb.Snapshot) { x.Metadata.ConfState.Voters[2] = 4 }},
		{"voter-order", func(x *pb.Snapshot) { x.Metadata.ConfState.Voters = []uint64{3, 2, 1} }},
		{"voter-added", func(x *pb.Snapshot) { x.Metadata.ConfState.Voters = append(x.Metadata.ConfState.Voters, 4) }},
		{"learner", func(x *pb.Snapshot) { x.Metadata.ConfState.Learners = []uint64{4} }},
		{"outgoing", func(x *pb.Snapshot) { x.Metadata.ConfState.VotersOutgoing = []uint64{1, 2, 3} }},
		{"next", func(x *pb.Snapshot) { x.Metadata.ConfState.LearnersNext = []uint64{4} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := rd
			bad.Snapshot = proto.Clone(rd.Snapshot).(*pb.Snapshot)
			tc.change(bad.Snapshot)
			caller := readyMatchWire(t, bad.Snapshot)
			before := readyMatchOwner(t, s, p, c)
			if root, err := s.PersistApplicationReady(bad, c); !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(root, ApplicationRoot{}) {
				t.Fatal(root, err)
			}
			validRefusal := false
			if allocations := testing.AllocsPerRun(100, func() {
				root, err := s.PersistApplicationReady(bad, c)
				validRefusal = errors.Is(err, ErrInvalid) && root.Image == nil
			}); allocations != 0 || !validRefusal {
				t.Fatal("malformed Ready allocated before refusal", allocations)
			}
			if !before.equal(readyMatchOwner(t, s, p, c)) || !bytes.Equal(caller, readyMatchWire(t, bad.Snapshot)) || s.poison != nil {
				t.Fatal("refusal changed ownership, storage or caller")
			}
		})
	}
	if _, err := s.PersistApplicationReady(rd, c); err != nil {
		t.Fatal("unchanged actual Ready after refusals", err)
	}
}

func TestApplicationSnapshotReadyMatcherBoundsWithoutAllocationOrMutation(t *testing.T) {
	expected := &pb.Snapshot{Data: []byte("descriptor"), Metadata: &pb.SnapshotMetadata{Index: new(uint64(2)), Term: new(uint64(2)), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}
	actual := proto.Clone(expected).(*pb.Snapshot)
	actual.Metadata.ConfState.AutoLeave = new(false)
	for _, pair := range [][2]*pb.Snapshot{{actual, expected}, {expected, actual}} {
		beforeA, beforeB := readyMatchWire(t, pair[0]), readyMatchWire(t, pair[1])
		if !applicationSnapshotReadyMatches(pair[0], pair[1]) || testing.AllocsPerRun(100, func() { applicationSnapshotReadyMatches(pair[0], pair[1]) }) != 0 || !bytes.Equal(beforeA, readyMatchWire(t, pair[0])) || !bytes.Equal(beforeB, readyMatchWire(t, pair[1])) {
			t.Fatal("false presence is not allocation-free and immutable")
		}
	}
	oversized := proto.Clone(actual).(*pb.Snapshot)
	oversized.Data = make([]byte, applicationSnapshotDescriptorLimit+1)
	giant := proto.Clone(actual).(*pb.Snapshot)
	giant.Metadata.ConfState.Voters = make([]uint64, 1<<16)
	invalid := proto.Clone(actual).(*pb.Snapshot)
	invalid.Metadata.ConfState.AutoLeave = new(true)
	for _, bad := range []*pb.Snapshot{nil, {}, {Data: []byte("descriptor")}, oversized, giant, invalid} {
		for _, pair := range [][2]*pb.Snapshot{{bad, expected}, {expected, bad}} {
			if applicationSnapshotReadyMatches(pair[0], pair[1]) || testing.AllocsPerRun(100, func() { applicationSnapshotReadyMatches(pair[0], pair[1]) }) != 0 {
				t.Fatal("malformed/oversized matcher accepted or allocated")
			}
		}
	}
}

func TestApplicationSnapshotActualIgnoredAndFastForwardReadyCannotActivate(t *testing.T) {
	for _, mode := range []string{"ignored", "fast-forward"} {
		t.Run(mode, func(t *testing.T) {
			s := receiverStore(t, vfs.NewMem(), 1)
			if mode == "ignored" {
				generationApply(t, s, "local")
			} else {
				persist(t, s, 2, 1, ent(2, 2, "matching"))
			}
			p, c, snap, _ := readyMatchPrepared(t, s, ApplicationSemanticContractID{})
			_, rd := readyMatchRaw(t, s, snap)
			if !raft.IsEmptySnap(rd.Snapshot) {
				t.Fatal("ignored/fast-forward produced installation Ready", rd)
			}
			for _, refusal := range []raft.Ready{{}, rd} {
				before := readyMatchOwner(t, s, p, c)
				if root, err := s.PersistApplicationReady(refusal, c); !errors.Is(err, ErrInvalid) || !reflect.DeepEqual(root, ApplicationRoot{}) {
					t.Fatal(root, err)
				}
				if !before.equal(readyMatchOwner(t, s, p, c)) || s.poison != nil {
					t.Fatal("non-activating Ready changed state/claim")
				}
			}
		})
	}
}
