package raftlog

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestClaimPreparedApplicationSnapshotChecksStoreBeforeOwnership(t *testing.T) {
	s := receiverStore(t, vfs.NewMem(), 1)
	other := receiverStore(t, vfs.NewMem(), 2)
	_, p, snapshot := preparedFixture(t, s)
	defer p.Close()
	before := readyMetadataFingerprint(t, s)
	usage, err := s.ApplicationTransferUsage()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		store    *Store
		p        *PreparedApplicationSnapshot
		expected *pb.Snapshot
	}{{nil, p, snapshot}, {s, nil, snapshot}, {s, new(PreparedApplicationSnapshot), snapshot}, {s, p, nil}, {other, p, snapshot}} {
		if claim, err := tc.store.ClaimPreparedApplicationSnapshot(tc.p, tc.expected); claim != nil || !errors.Is(err, ErrInvalid) {
			t.Fatal(claim, err)
		}
	}
	oversized := proto.Clone(snapshot).(*pb.Snapshot)
	oversized.Data = make([]byte, applicationSnapshotDescriptorLimit+1)
	if _, err := other.ClaimPreparedApplicationSnapshot(p, oversized); !errors.Is(err, ErrInvalid) || errors.Is(err, ErrLimit) {
		t.Fatal("wrong store reached claim allocation/admission", err)
	}
	after, err := s.ApplicationTransferUsage()
	if err != nil || usage != after || before != readyMetadataFingerprint(t, s) {
		t.Fatal("refusal mutated ownership/data", usage, after, err)
	}
	claim, err := s.ClaimPreparedApplicationSnapshot(p, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimPreparedApplicationSnapshot(p, snapshot); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimPreparedApplicationSnapshot(p, snapshot); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
