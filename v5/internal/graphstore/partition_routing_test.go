package graphstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func TestPartitionRoutingRejectsMissingBucketsAndPreservesStableAxisIDKey(t *testing.T) {
	d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 9, []PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{3}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var owners []BucketOwnership
	for _, family := range []RoutingBucketKind{IdentityRoutingBucket, UniqueRoutingBucket, AxisRoutingBucket} {
		for bucket := range 2 {
			owners = append(owners, BucketOwnership{Kind: family, Bucket: uint32(bucket), Partition: []uint64{3, 8}[bucket], Epoch: 1})
		}
	}
	routing, err := NewPartitionRouting(d, 1, 1, owners, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewPartitionRouting(d, 1, 1, owners[:len(owners)-1], Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing empty bucket admitted", err)
	}
	if routing.Graph() != d.Graph() || routing.DeclarationDigest() != d.Digest() || routing.Version() != 1 {
		t.Fatal("routing lost source binding")
	}
	// Axis authority uses the stable ID bytes only. Different descriptor hashes
	// cannot change that ID's authority bucket.
	axisID := make([]byte, 16)
	axisID[0] = 31
	first, err := routing.Bucket(AxisRoutingBucket, axisID)
	if err != nil {
		t.Fatal(err)
	}
	same, err := routing.Bucket(AxisRoutingBucket, append([]byte(nil), axisID...))
	if err != nil || same != first {
		t.Fatal("axis ID routing is unstable", same, first, err)
	}
	body, err := EncodePartitionRouting(routing, d, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePartitionRouting(body, d, DefaultLimits())
	if err != nil || decoded.Digest() != routing.Digest() {
		t.Fatal("routing codec", err)
	}
}

func TestPublishedRangeRejectsGapAndSeparateFencedCurrentOwner(t *testing.T) {
	publication := RangePublication{Graph: graphstate.GraphID{1}, RangeID: 17, First: 17, Last: 32, InitialPartition: 8, InitialEpoch: 3, Configuration: [32]byte{1}, SourcePartition: 3, SourceIndex: 7}
	current := RangeOwnership{Graph: publication.Graph, RangeID: publication.RangeID, Partition: 8, Epoch: 3, Configuration: publication.Configuration}
	if err := publication.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{17, 20, 32} {
		if !publication.Contains(id) {
			t.Fatal("lost inclusive range", id)
		}
	}
	for _, id := range []uint64{0, 1, 16, 33} {
		if publication.Contains(id) {
			t.Fatal("range gap became absence/ownership", id)
		}
	}
	if err := current.Check(publication, 8, 3); err != nil {
		t.Fatal(err)
	}
	current.Fenced = true
	if err := current.Check(publication, 8, 3); !errors.Is(err, ErrStaleOwner) {
		t.Fatal("grant provenance reactivated fenced owner", err)
	}
}

func TestPublishedRangeMVCCSeekSkipsEmptyPagesAndNeverVisitsAdjacentNamespace(t *testing.T) {
	d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 9, []PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{3}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	n := Namespace{Graph: d.Graph(), Partition: 3}
	s, _, _ := ownershipStore(t, vfs.NewMem(), n, d, [16]byte{3})
	_, image, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	cfg := [32]byte{1}
	publication := RangePublication{Graph: n.Graph, RangeID: 17, First: 17, Last: 32, InitialPartition: 8, InitialEpoch: 3, Configuration: cfg, SourcePartition: 3, SourceIndex: 1}
	owner := RangeOwnership{Graph: n.Graph, RangeID: 17, Partition: 8, Epoch: 3, Configuration: cfg}
	pub, err := EncodeRangePublication(publication)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := EncodeRangeOwnership(owner)
	if err != nil {
		t.Fatal(err)
	}
	at := ownershipInstallRaw(t, s, image, []raftlog.KV{{Key: RangePublicationKey(n, 17), Value: pub}, {Key: RangeOwnershipKey(n, 17), Value: fence}, {Key: RangeEndKey(n, 32), Value: pub}})
	old := ownershipView(t, s, at)
	// A later logical key is invisible at old and tombstoned at current. Both
	// cause an empty, incomplete Rows=1 page before the valid end32 locator.
	at = ownershipInstallRaw(t, s, image, []raftlog.KV{{Key: RangeEndKey(n, 24), Deleted: true}})
	current := ownershipView(t, s, at)
	for _, view := range []*raftlog.ApplicationView{old, current} {
		p, o, work, err := LookupPublishedRange(t.Context(), view, n, 22, cfg, OwnershipBudget{SourceRows: 8, SourceBytes: 4096, OutputBytes: 4096})
		if err != nil || p != publication || o != owner || work.Records != 4 {
			t.Fatal("empty page lost valid routing", p, o, work, err)
		}
	}
	// A valid but foreign namespace key must not be decoded or counted when the
	// local range family has no End>=ID. Same tag is not same namespace.
	adjacent := Namespace{Graph: n.Graph, Partition: 4}
	at = ownershipInstallRaw(t, s, image, []raftlog.KV{{Key: RangeEndKey(adjacent, 64), Value: pub}})
	last := ownershipView(t, s, at)
	p, o, work, err := LookupPublishedRange(t.Context(), last, n, 64, cfg, OwnershipBudget{SourceRows: 8, SourceBytes: 4096, OutputBytes: 4096})
	if !errors.Is(err, ErrRoutingUnknown) || p != (RangePublication{}) || o != (RangeOwnership{}) || work.Records != 0 {
		t.Fatal("exhausted local family visited another namespace", p, o, work, err)
	}
	if _, _, _, err := LookupPublishedRange(t.Context(), last, n, 16, cfg, OwnershipBudget{SourceRows: 8, SourceBytes: 4096, OutputBytes: 4096}); !errors.Is(err, ErrRoutingUnknown) {
		t.Fatal("lower-bound gap admitted", err)
	}
	for _, budget := range []OwnershipBudget{{SourceRows: 1, SourceBytes: 4096, OutputBytes: 4096}, {SourceRows: 8, SourceBytes: 512, OutputBytes: 4096}, {SourceRows: 8, SourceBytes: 4096, OutputBytes: 1023}} {
		p, o, work, err := LookupPublishedRange(t.Context(), current, n, 22, cfg, budget)
		if !errors.Is(err, ErrResourceLimit) || p != (RangePublication{}) || o != (RangeOwnership{}) {
			t.Fatal("limit returned partial authority", p, o, work, err)
		}
	}
	if _, err := old.Root(); err != nil {
		t.Fatal("lookup consumed borrow", err)
	}
}

func TestPartitionRoutingPolicyPermitsMoreThanSixteenBucketsAndOwnsInput(t *testing.T) {
	d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 9, []PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{3}}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var owners []BucketOwnership
	for _, family := range []RoutingBucketKind{IdentityRoutingBucket, UniqueRoutingBucket, AxisRoutingBucket} {
		for bucket := range 32 {
			owners = append(owners, BucketOwnership{Kind: family, Bucket: uint32(bucket), Partition: 3, Epoch: 1})
		}
	}
	routing, err := NewPartitionRouting(d, 1, 5, owners, Limits{})
	if err != nil {
		t.Fatal("fixture size became topology ceiling", err)
	}
	before := routing.Digest()
	owners[0].Epoch = 99
	wire, err := EncodePartitionRouting(routing, d, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePartitionRouting(wire, d, Limits{})
	if err != nil || decoded.Digest() != before {
		t.Fatal("caller mutation rebound routing", err)
	}
	if _, err := NewPartitionRouting(d, 1, 32, nil, Limits{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("policy must refuse before giant allocation", err)
	}
}

func TestPartitionRoutingAndBindingDirectPolicyAndCorruptFramingRefusals(t *testing.T) {
	d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 9, []PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{3}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	owners := []BucketOwnership{{Kind: IdentityRoutingBucket, Partition: 3, Epoch: 1}, {Kind: UniqueRoutingBucket, Partition: 3, Epoch: 1}, {Kind: AxisRoutingBucket, Partition: 3, Epoch: 1}}
	r, err := NewPartitionRouting(d, 1, 0, owners, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []Limits{{MaxReadRows: -1}, {MaxReadBytes: 1024}} {
		_, err := NewPartitionRouting(d, 1, 0, owners, policy)
		expected := ErrInvalid
		if !errors.Is(err, expected) {
			t.Fatal(policy, err)
		}
	}
	for _, field := range []string{"kind", "epoch", "bucket", "order", "partition"} {
		bad := append([]BucketOwnership(nil), owners...)
		expected := ErrInvalid
		switch field {
		case "kind":
			bad[0].Kind = 0
		case "epoch":
			bad[0].Epoch = 0
		case "bucket":
			bad[0].Bucket = 1
		case "order":
			bad[0], bad[1] = bad[1], bad[0]
		case "partition":
			bad[0].Partition = 99
			expected = ErrNamespace
		}
		if _, err := NewPartitionRouting(d, 1, 0, bad, Limits{}); !errors.Is(err, expected) {
			t.Fatal(field, err)
		}
	}
	for _, bits := range []uint8{33} {
		if _, err := NewPartitionRouting(d, 1, bits, owners, Limits{}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := NewPartitionRouting(d, 0, 0, owners, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := (PartitionRouting{}).Bucket(IdentityRoutingBucket, []byte{1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := r.Bucket(AxisRoutingBucket, []byte{1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := r.Bucket(IdentityRoutingBucket, nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := (PartitionRouting{}).EncodedBytes(Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := r.EncodedBytes(Limits{MaxReadRows: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := EncodePartitionRouting(r, OwnershipDeclaration{}, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	wire, err := EncodePartitionRouting(r, d, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodePartitionRouting(wire, d, Limits{MaxReadRows: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 4, 20, 28, 60, 68, 69, 73, len(wire) - 1} {
		bad := exactCopy(wire)
		if offset == 60 {
			clear(bad[60:68])
		} else {
			bad[offset] ^= 1
		}
		if offset != len(wire)-1 {
			hash := sha256.Sum256(bad[:len(bad)-32])
			copy(bad[len(bad)-32:], hash[:])
		}
		if _, err := DecodePartitionRouting(bad, d, Limits{}); !errors.Is(err, ErrCorrupt) {
			t.Fatal(offset, err)
		}
	}
	if _, err := DecodePartitionRouting(nil, d, Limits{}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	binding, err := types.NewDefaultAxisBinding(types.GraphID(d.Graph()), temporal.AxisID{31}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	configuration := [32]byte{1}
	bound, err := EncodePartitionRoutingBinding(r, d, binding, configuration, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EncodePartitionRoutingBinding(r, d, binding, [32]byte{}, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := EncodePartitionRoutingBinding(PartitionRouting{}, d, binding, configuration, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := DecodePartitionRoutingBinding(bound, d, binding, [32]byte{}, Limits{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := DecodePartitionRoutingBinding(nil, d, binding, configuration, Limits{}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	for _, offset := range []int{0, 4, 36, 52, 84, 88, len(bound) - 1} {
		bad := exactCopy(bound)
		bad[offset] ^= 1
		if offset != len(bound)-1 {
			bad = sealRecord(bad[:len(bad)-32])
		}
		if _, err := DecodePartitionRoutingBinding(bad, d, binding, configuration, Limits{}); !errors.Is(err, ErrCorrupt) {
			t.Fatal(offset, err)
		}
	}
	if !bytes.Equal(wire, bound[88:len(bound)-32]) || int(binary.BigEndian.Uint32(bound[84:88])) != len(wire) {
		t.Fatal("canonical routing envelope changed")
	}
}

func TestFinalReviewRangeOwnershipInitialEpochFloorAndTransferControls(t *testing.T) {
	p := RangePublication{Graph: graphstate.GraphID{1}, RangeID: 1, First: 1, Last: 16, InitialPartition: 3, InitialEpoch: 2, Configuration: [32]byte{1}, SourcePartition: 3, SourceIndex: 1}
	for _, epoch := range []uint64{1, 2, 3} {
		o := RangeOwnership{Graph: p.Graph, RangeID: p.RangeID, Partition: 8, Epoch: epoch, Configuration: p.Configuration}
		err := o.Check(p, 8, epoch)
		if epoch < 2 {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal("regressed initial lineage", err)
			}
		} else if err != nil {
			t.Fatal("legitimate changed owner/advanced epoch", err)
		}
		o.Fenced = true
		err = o.Check(p, 8, epoch)
		if epoch < 2 {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal("fence masked corruption", err)
			}
		} else if !errors.Is(err, ErrStaleOwner) {
			t.Fatal(err)
		}
	}
}
