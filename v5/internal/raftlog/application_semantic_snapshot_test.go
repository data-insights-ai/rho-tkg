package raftlog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func semanticManifest(t *testing.T, s *Store) (ApplicationSnapshotManifest, []ApplicationSnapshotChunk) {
	t.Helper()
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	chunks := collectPublished(t, e, ReadBudget{1, 4096})
	if m.Version != 3 || m.SemanticContractID == (ApplicationSemanticContractID{}) {
		t.Fatal(m)
	}
	for _, c := range chunks {
		if c.Version != 3 {
			t.Fatal("bound stream dropped version", c.Version)
		}
	}
	return m, chunks
}
func TestApplicationSemanticSnapshotBoundActivationAndReopen(t *testing.T) {
	id := ApplicationSemanticContractID{7}
	donor := semanticStore(t, vfs.NewMem(), id, 2)
	generationApply(t, donor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	m, chunks := semanticManifest(t, donor)
	wire, err := EncodeApplicationSnapshotManifest(m)
	if err != nil || wire[2] != 3 || len(wire) != cap(wire) {
		t.Fatal(err)
	}
	decoded, err := DecodeApplicationSnapshotManifest(wire, m.Contract.MaxImageBytes)
	if err != nil || !reflect.DeepEqual(decoded, m) {
		t.Fatal(decoded, err)
	}
	data, err := EncodeApplicationSnapshotDescriptor(m)
	if err != nil || data[2] != 2 || !bytes.Equal(data[4:36], id[:]) || len(data) > applicationSnapshotDescriptorLimit {
		t.Fatal(data, err)
	}
	fs := vfs.NewMem()
	s := semanticStore(t, fs, id, 1)
	i, err := s.BeginApplicationImport(t.Context(), m)
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
	p, err := i.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(m.Index), Term: new(m.Term), ConfState: m.ConfState}, Data: data}
	claim, err := p.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	root, err := s.PersistApplicationReady(raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Vote: new(uint64(3)), Commit: new(m.Index)}}, claim)
	if err != nil || root.Index != 2 || string(root.Image) != "incoming" {
		t.Fatal(root, err)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil || cut.SemanticContractID != id || cut.ID != m.CutID {
		t.Fatal("activation zeroed semantic binding", cut, err)
	}
	assertActivationKV(t, s, 2, true)
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Dir: "db", FS: fs, Application: s.meta.App.Policy, Transfer: s.meta.Transfer, Generations: s.meta.Gen.Limits, PublishedCuts: s.meta.Gen.Publication.Limits, Replication: s.meta.Rep.Config, SemanticContractID: id}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	assertActivationKV(t, r, 2, true)
	recovered, stream := semanticManifest(t, r)
	if recovered.CutID != m.CutID || recovered.SemanticContractID != id || recovered.RecordsHash != m.RecordsHash || len(stream) != len(chunks) {
		t.Fatal("reopen moved bound cut", recovered)
	}
	if _, err := r.BeginApplicationExport(t.Context()); !errors.Is(err, ErrInvalid) {
		t.Fatal("bound moving export bypass", err)
	}
}
func TestApplicationSemanticSnapshotRefusesCrossAndUnboundBeforeWrites(t *testing.T) {
	a := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{1}, 2)
	generationApply(t, a, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	if err := a.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	m, chunks := semanticManifest(t, a)
	unbound := receiverStore(t, vfs.NewMem(), 1)
	other := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{2}, 1)
	for _, s := range []*Store{unbound, other} {
		before := readyMetadataFingerprint(t, s)
		usage, _ := s.ApplicationTransferUsage()
		if _, err := s.BeginApplicationImport(t.Context(), m); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		after, _ := s.ApplicationTransferUsage()
		if before != readyMetadataFingerprint(t, s) || usage != after || s.applicationImport != nil {
			t.Fatal("cross semantic mutated")
		}
	}
	plainDonor := receiverStore(t, vfs.NewMem(), 2)
	generationApply(t, plainDonor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	if err := plainDonor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	pc, _ := plainDonor.PublishedApplicationCut()
	e, err := plainDonor.BeginPublishedApplicationExport(t.Context(), pc.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	plain := buildPublished(t, e, ReadBudget{1, 4096})
	receiver := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{1}, 1)
	before := readyMetadataFingerprint(t, receiver)
	if _, err := receiver.BeginApplicationImport(t.Context(), plain); !errors.Is(err, ErrInvalid) {
		t.Fatal("AS2 bypassed semantic store", err)
	}
	if before != readyMetadataFingerprint(t, receiver) {
		t.Fatal("unbound refusal wrote")
	}
	i, err := receiver.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Abort()
	first := chunks[0]
	first.Version = 2
	before = readyMetadataFingerprint(t, receiver)
	if err := i.Append(t.Context(), first); !errors.Is(err, ErrInvalid) {
		t.Fatal("AS2 chunk bypassed", err)
	}
	if before != readyMetadataFingerprint(t, receiver) {
		t.Fatal("chunk refusal wrote")
	}
	for _, c := range chunks {
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
	data[4] ^= 1
	if _, err := p.Claim(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(m.Index), Term: new(m.Term), ConfState: m.ConfState}, Data: data}); !errors.Is(err, ErrInvalid) {
		t.Fatal("descriptor semantic mismatch accepted", err)
	}
	if p.claim != nil {
		t.Fatal("mismatched claim acquired ownership")
	}
}

func TestApplicationSemanticManifestBindingDecoderBeforeAllocation(t *testing.T) {
	s := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{9}, 1)
	m, _ := semanticManifest(t, s)
	wire, err := EncodeApplicationSnapshotManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	binding := s.ApplicationBinding()
	decoded, err := DecodeApplicationSnapshotManifestForBinding(wire, m.Contract.MaxImageBytes, binding)
	if err != nil || !reflect.DeepEqual(decoded, m) {
		t.Fatal(decoded, err)
	}
	wire[len(wire)-1] ^= 1
	if !bytes.Equal(decoded.Image, m.Image) {
		t.Fatal("returned image aliases wire")
	}
	wire[len(wire)-1] ^= 1
	decoded.ConfState.Voters[0] = 99
	if m.ConfState.Voters[0] == 99 {
		t.Fatal("returned conf aliases manifest")
	}
	cases := []struct {
		name    string
		data    []byte
		limit   int
		binding ApplicationBinding
		want    error
	}{
		{"nil", nil, m.Contract.MaxImageBytes, binding, ErrCorrupt},
		{"zero expected", wire, m.Contract.MaxImageBytes, ApplicationBinding{}, ErrInvalid},
		{"zero limit", wire, 0, binding, ErrInvalid},
		{"large limit", wire, 64<<20 + 1, binding, ErrInvalid},
	}
	for _, change := range []func(*ApplicationBinding){func(b *ApplicationBinding) { b.Identity.Graph[0]++ }, func(b *ApplicationBinding) { b.Identity.Partition++ }, func(b *ApplicationBinding) { b.Identity.Group[0]++ }, func(b *ApplicationBinding) { b.SemanticContractID[0]++ }} {
		other := binding
		change(&other)
		cases = append(cases, struct {
			name    string
			data    []byte
			limit   int
			binding ApplicationBinding
			want    error
		}{"cross binding", wire, m.Contract.MaxImageBytes, other, ErrInvalid})
	}
	for n := 0; n < snapshotManifestOverhead+64; n++ {
		cases = append(cases, struct {
			name    string
			data    []byte
			limit   int
			binding ApplicationBinding
			want    error
		}{fmt.Sprint("truncated", n), wire[:n], m.Contract.MaxImageBytes, binding, ErrCorrupt})
	}
	for _, version := range []byte{0, 1, 2, 4} {
		b := bytes.Clone(wire)
		b[2] = version
		want := ErrCorrupt
		if version == 1 || version == 2 {
			want = ErrInvalid
		}
		cases = append(cases, struct {
			name    string
			data    []byte
			limit   int
			binding ApplicationBinding
			want    error
		}{fmt.Sprint("version", version), b, m.Contract.MaxImageBytes, binding, want})
	}
	for _, at := range []int{snapshotManifestOverhead - 8, snapshotManifestOverhead - 4} {
		b := bytes.Clone(wire)
		binary.BigEndian.PutUint32(b[at:], math.MaxUint32)
		cases = append(cases, struct {
			name    string
			data    []byte
			limit   int
			binding ApplicationBinding
			want    error
		}{fmt.Sprint("length limit", at), b, m.Contract.MaxImageBytes, binding, ErrLimit})
	}
	bad := bytes.Clone(wire)
	binary.BigEndian.PutUint32(bad[snapshotManifestOverhead-4:], 0)
	cases = append(cases, struct {
		name    string
		data    []byte
		limit   int
		binding ApplicationBinding
		want    error
	}{"length mismatch", bad, m.Contract.MaxImageBytes, binding, ErrCorrupt})
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := DecodeApplicationSnapshotManifestForBinding(c.data, c.limit, c.binding)
			if !errors.Is(err, c.want) || !reflect.DeepEqual(out, ApplicationSnapshotManifest{}) {
				t.Fatal(out, err)
			}
		})
	}
	oversized := make([]byte, maxSnapshotManifestBytes+65)
	copy(oversized, wire)
	if _, err := DecodeApplicationSnapshotManifestForBinding(oversized, m.Contract.MaxImageBytes, binding); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := DecodeApplicationSnapshotManifest(oversized, m.Contract.MaxImageBytes); !errors.Is(err, ErrCorrupt) {
		t.Fatal("generic AS3 over ceiling", err)
	}
	corrupt := bytes.Clone(wire)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := DecodeApplicationSnapshotManifestForBinding(corrupt, m.Contract.MaxImageBytes, binding); !errors.Is(err, ErrCorrupt) {
		t.Fatal("bound decoder missed corrupt image", err)
	}
	// The delivered buffer is caller-owned. Refusing its scope/length never allocates
	// a decoded protobuf or image, regardless of the amount of delivered data.
	cross := binding
	cross.SemanticContractID[0]++
	large := make([]byte, 4<<20)
	copy(large, wire)
	for _, f := range []func(){func() { _, _ = DecodeApplicationSnapshotManifestForBinding(large, 64<<20, cross) }, func() { _, _ = DecodeApplicationSnapshotManifestForBinding(large, 64<<20, binding) }} {
		if allocs := testing.AllocsPerRun(50, f); allocs != 0 {
			t.Fatal("header refusal allocated", allocs)
		}
	}
	zero := bytes.Clone(wire)
	clear(zero[snapshotManifestOverhead+32 : snapshotManifestOverhead+64])
	if _, err := DecodeApplicationSnapshotManifest(zero, m.Contract.MaxImageBytes); !errors.Is(err, ErrInvalid) {
		t.Fatal("generic zero semantic", err)
	}
	if allocs := testing.AllocsPerRun(50, func() { _, _ = DecodeApplicationSnapshotManifest(zero, m.Contract.MaxImageBytes) }); allocs != 0 {
		t.Fatal("zero semantic decoded body", allocs)
	}
}

func semanticPrepared(t *testing.T, s *Store, m ApplicationSnapshotManifest, chunks []ApplicationSnapshotChunk) (*PreparedApplicationSnapshot, *pb.Snapshot) {
	t.Helper()
	i, err := s.BeginApplicationImport(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := i.Abort(); err != nil {
			t.Error(err)
		}
	})
	for _, c := range chunks {
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
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	data, err := EncodeApplicationSnapshotDescriptor(m)
	if err != nil {
		t.Fatal(err)
	}
	return p, &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(m.Index), Term: new(m.Term), ConfState: m.ConfState}, Data: data}
}
func TestApplicationSemanticActivationReadyBudgetBoundary(t *testing.T) {
	id := ApplicationSemanticContractID{1}
	donor := semanticStore(t, vfs.NewMem(), id, 2)
	generationApply(t, donor, "incoming", KV{Key: []byte("a"), Value: []byte("incoming")})
	if err := donor.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	m, chunks := semanticManifest(t, donor)
	valid := func(snap *pb.Snapshot) raft.Ready {
		return raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Vote: new(uint64(3)), Commit: new(m.Index)}, Entries: []*pb.Entry{ent(3, 5, "tail")}}
	}
	reference := semanticStore(t, vfs.NewMem(), id, 1)
	p, snap := semanticPrepared(t, reference, m, chunks)
	c, err := p.Claim(snap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reference.PersistApplicationReady(valid(snap), c); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Serialize actual resulting AR2 metadata independently; descriptor adds 32.
	wire, err := encodeMeta(reference.meta)
	if err != nil {
		t.Fatal(err)
	}
	boundary := len(wire) + readyEnvelopeBytes + len(snap.Data) + len("tail") + frameOverhead + 9 + 32
	l := DefaultLimits()
	l.MaxEntryBytes = 256
	l.MaxReadBytes = 512
	l.MaxReadyBytes = 4096
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			s := semanticStoreLimits(t, vfs.NewMem(), id, 1, l)
			p, snap := semanticPrepared(t, s, m, chunks)
			c, err := p.Claim(snap)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			s.limits.MaxReadyBytes = boundary + delta
			if err := s.limits.Validate(); err != nil {
				t.Fatal(err)
			}
			before := readyMetadataFingerprint(t, s)
			usage, _ := s.ApplicationTransferUsage()
			root, err := s.PersistApplicationReady(valid(snap), c)
			if delta < 0 {
				after, _ := s.ApplicationTransferUsage()
				if !errors.Is(err, ErrLimit) || root.Image != nil || before != readyMetadataFingerprint(t, s) || usage != after || s.poison != nil {
					t.Fatal("one-short activation mutated", root, err)
				}
				return
			}
			if err != nil || root.Index != 2 {
				t.Fatal(root, err)
			}
			assertActivationKV(t, s, 2, true)
		})
	}
}
func TestApplicationSemanticExportPinBudgetBoundary(t *testing.T) {
	s := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{1}, 1)
	cut, _ := s.PublishedApplicationCut()
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	need := e.pinBytes
	if unsafe.Sizeof(ApplicationExport{})+unsafe.Sizeof(publishedExportState{})+unsafe.Sizeof(pb.ConfState{}) > publishedExportFixedBytes {
		t.Fatal("bound export fixed representation omitted")
	}
	raw, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	expected := s.meta.App.Bytes + s.meta.LogBytes + s.meta.ImageBytes + s.meta.SnapBytes + uint64(len(raw)) + 3 + s.meta.SnapBytes + uint64(snapshotConfCapacity(s.meta.Snap.GetMetadata().GetConfState())) + publishedExportFixedBytes + 4*uint64(2*s.meta.Transfer.Contract.MaxKeyBytes+11)
	if need != expected {
		t.Fatal("bound metadata/cursors/images accounting", need, expected)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			s.meta.Transfer.Limits.MaxPinnedLogicalBytes = need + uint64(max(delta, 0))
			if delta < 0 {
				s.meta.Transfer.Limits.MaxPinnedLogicalBytes = need - 1
			}
			before := readyMetadataFingerprint(t, s)
			usage, _ := s.ApplicationTransferUsage()
			e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
			if delta < 0 {
				after, _ := s.ApplicationTransferUsage()
				if !errors.Is(err, ErrLimit) || e != nil || before != readyMetadataFingerprint(t, s) || usage != after || s.poison != nil {
					t.Fatal("one short export mutated", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := e.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestApplicationSemanticLegacyWireAndHashCompatibility(t *testing.T) {
	// Fixed vectors measured on accepted 1796754 with the same independently
	// constructed stores. These lock complete wire envelopes and hash domains.
	s := receiverStore(t, vfs.NewMem(), 1)
	wire, err := encodeMeta(s.meta)
	if err != nil {
		t.Fatal(err)
	}
	cut, err := s.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	m := buildPublished(t, e, ReadBudget{1, 4096})
	b, err := EncodeApplicationSnapshotManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	d, err := EncodeApplicationSnapshotDescriptor(m)
	if err != nil {
		t.Fatal(err)
	}
	mid, err := manifestID(m)
	if err != nil {
		t.Fatal(err)
	}
	plain := transferStore(t, "db", vfs.NewMem(), 1)
	x, err := plain.BeginApplicationExport(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	old, err := x.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	w, err := EncodeApplicationSnapshotManifest(old)
	if err != nil {
		t.Fatal(err)
	}
	oldid, err := manifestID(old)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		name string
		got  [32]byte
		want string
	}{
		{"AR1-meta", sha256.Sum256(wire), "a74f57633df732b5f5d16bc051445254b94b45677b673041f8fafd8a1e20d757"},
		{"AS2", sha256.Sum256(b), "466bd8ba31417bd68de7dd91b4c5b94dd371305f6cadacc954dbae220bbed81c"},
		{"AD1", sha256.Sum256(d), "f4a62d48bd7109afc511bc2de68b5e86b68f24ebd108759f7ce1b8c1dbb893cf"},
		{"cut", cut.ID, "eb08a6f4ea8818a8f3d64e0ecf8b924a13bd0f7f77d788bd9939b7ae4b02ba15"},
		{"seed2", snapshotSeed(m), "5e798d8d475a5fc45da27f1d0e0f43c3a4721e714fb565a351e10e43a0cb82ca"},
		{"id2", mid, "950581198ca66d22edb786c4e9d7e47ca6575e3dc03fb782a30cdf9e622b2aa1"},
		{"AS1", sha256.Sum256(w), "7f67e792d79b641710865da8277148a0ee7171ef48c581daafcf3505c7431921"},
		{"seed1", snapshotSeed(old), "ace9bb88fe573d375f663784ac14f5a4e4acec89d2ff6f50510d06aaaea4e293"},
		{"id1", oldid, "5202c64634d4c7af616400c3a2f6ab332258c6bbd33c35ac4fea33d0ee0dbd1a"},
	} {
		if fmt.Sprintf("%x", v.got) != v.want {
			t.Fatal(v.name, v.got, v.want)
		}
	}
	old.Version = 1
	explicit, err := EncodeApplicationSnapshotManifest(old)
	if err != nil || !bytes.Equal(explicit, w) {
		t.Fatal("explicit AS1 changed bytes", err)
	}
	normalized, err := DecodeApplicationSnapshotManifest(explicit, old.Contract.MaxImageBytes)
	if err != nil || normalized.Version != 0 {
		t.Fatal("AS1 normalization", err)
	}
	bound := s.ApplicationBinding()
	bound.Identity = s.ApplicationIdentity()
	bound.SemanticContractID = ApplicationSemanticContractID{1}
	for _, legacy := range [][]byte{b, w} {
		if _, err := DecodeApplicationSnapshotManifestForBinding(legacy, m.Contract.MaxImageBytes, bound); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, manifest := range []ApplicationSnapshotManifest{m, old} {
		manifest.SemanticContractID = bound.SemanticContractID
		if _, err := EncodeApplicationSnapshotManifest(manifest); !errors.Is(err, ErrInvalid) {
			t.Fatal("legacy dropped semantic field", err)
		}
	}
}

func TestApplicationSemanticPublishedCutRetainsEarlierMeaning(t *testing.T) {
	s := semanticStore(t, vfs.NewMem(), ApplicationSemanticContractID{1}, 1)
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
	generationApply(t, s, "new", KV{Key: []byte("a"), Value: []byte("new")}, KV{Key: []byte("later"), Value: []byte("must not appear")})
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	m := buildPublished(t, e, ReadBudget{1, 4096})
	chunks := collectPublished(t, e, ReadBudget{1, 4096})
	if m.CutID != cut.ID || m.Index != 2 || string(m.Image) != "old" || m.SemanticContractID != s.ApplicationBinding().SemanticContractID {
		t.Fatal("bound export moved to current cut", m)
	}
	found := false
	for _, c := range chunks {
		if c.Version != 3 {
			t.Fatal(c.Version)
		}
		if err := walkSnapshotChunk(c.Data, m.Contract, func(k, v []byte) error {
			if k[0] != appDataTag {
				return nil
			}
			key, index, err := decodeAppKey(k, m.Contract.MaxKeyBytes)
			if err != nil {
				return err
			}
			data, deleted, err := inspectAppFrame(k, v, m.Contract.MaxValueBytes)
			if err != nil {
				return err
			}
			if string(key) != "a" || index != 2 || deleted || string(data) != "old" {
				t.Fatal("future/phantom data in earlier bound cut", string(key), index, string(data), deleted)
			}
			found = true
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if !found {
		t.Fatal("old value omitted")
	}
	other := m
	other.SemanticContractID[0]++
	totals, records, err := snapshotTotals(other)
	if err != nil {
		t.Fatal(err)
	}
	other.CutID, err = cutID(ApplicationCutReference{SemanticContractID: other.SemanticContractID, Identity: other.Identity, Contract: other.Contract, Index: other.Index, Term: other.Term, ConfState: other.ConfState, ImageBytes: uint64(len(other.Image)), ImageHash: other.ImageHash, RetainedBytes: totals, RetainedRecords: records})
	if err != nil {
		t.Fatal(err)
	}
	if other.CutID == m.CutID || snapshotSeed(other) == snapshotSeed(m) || publishedManifestID(other) == publishedManifestID(m) {
		t.Fatal("semantic agreement omitted from hash domain")
	}
	zero := m
	zero.SemanticContractID = ApplicationSemanticContractID{}
	if _, err := EncodeApplicationSnapshotDescriptor(zero); !errors.Is(err, ErrInvalid) {
		t.Fatal("AD2 dropped semantic field", err)
	}
}
