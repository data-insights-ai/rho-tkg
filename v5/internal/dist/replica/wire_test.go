package replica

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func field(num protowire.Number, typ protowire.Type) []byte {
	b := protowire.AppendTag(nil, num, typ)
	if typ == protowire.VarintType {
		return protowire.AppendVarint(b, 1)
	}
	return protowire.AppendBytes(b, nil)
}
func TestWireSchemasRejectWrongTypesUnknownsAndExpansion(t *testing.T) {
	l := raftlog.DefaultLimits()
	l.MaxEntryBytes = 256
	l.MaxReadBytes = 512
	l.MaxSnapshotBytes = 128
	valid := []proto.Message{
		&pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Snapshot: &pb.Snapshot{Data: []byte("image"), Metadata: &pb.SnapshotMetadata{Index: new(uint64(6)), Term: new(uint64(2)), ConfState: &pb.ConfState{Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 2, 3}, LearnersNext: []uint64{3}, AutoLeave: new(true)}}}, Context: []byte("read")},
		&pb.Entry{Type: pb.EntryNormal.Enum(), Index: new(uint64(2)), Term: new(uint64(1)), Data: []byte("small")},
		&pb.Snapshot{Data: []byte("state"), Metadata: &pb.SnapshotMetadata{Index: new(uint64(2)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1}}}},
		&pb.SnapshotMetadata{Index: new(uint64(2)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1}}},
		&pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}, AutoLeave: new(false)},
	}
	for kind, msg := range valid {
		b, err := proto.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		if err := preflightWire(b, kind, l); err != nil {
			t.Fatalf("valid schema %d: %v", kind, err)
		}
	}
	wrong := []struct {
		kind int
		num  protowire.Number
		typ  protowire.Type
	}{
		{0, 1, protowire.BytesType}, {0, 7, protowire.VarintType}, {0, 9, protowire.VarintType}, {0, 12, protowire.VarintType}, {0, 14, protowire.BytesType},
		{1, 1, protowire.BytesType}, {1, 4, protowire.VarintType}, {1, 5, protowire.VarintType},
		{2, 1, protowire.VarintType}, {2, 3, protowire.BytesType},
		{3, 1, protowire.VarintType}, {3, 2, protowire.BytesType}, {3, 4, protowire.VarintType},
		{4, 5, protowire.BytesType}, {4, 6, protowire.VarintType}, {5, 1, protowire.VarintType},
	}
	for _, c := range wrong {
		if err := preflightWire(field(c.num, c.typ), c.kind, l); !errors.Is(err, ErrInvalid) {
			t.Fatal(c, err)
		}
	}
	b := protowire.AppendTag(nil, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte{1, 2, 3})
	if err := preflightWire(b, 4, l); err != nil {
		t.Fatal("packed membership", err)
	}
	b = protowire.AppendTag(nil, 5, protowire.VarintType)
	b = protowire.AppendVarint(b, 2)
	if err := preflightWire(b, 4, l); err != nil {
		t.Fatal(err)
	}
	b = protowire.AppendTag(nil, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, 3)
	if err := preflightWire(b, 1, l); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, c := range []struct {
		kind int
		num  protowire.Number
		size int
	}{{0, 12, 1025}, {1, 4, 183}, {2, 1, 129}} {
		b := protowire.AppendTag(nil, c.num, protowire.BytesType)
		b = protowire.AppendBytes(b, make([]byte, c.size))
		if err := preflightWire(b, c.kind, l); !errors.Is(err, ErrLimit) {
			t.Fatal(c, err)
		}
	}
	for _, kind := range []int{0, 1, 2, 3, 4} {
		if err := preflightWire([]byte{0x80}, kind, l); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if err := preflightWire([]byte{0x08, 0x80}, kind, l); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if err := preflightWire([]byte{0x0b}, kind, l); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}

func FuzzConsensusWirePreflight(f *testing.F) {
	for _, b := range [][]byte{{}, {0x08, 1}, {0x3a, 0}, {0x4a, 2, 0x0a, 0}, {0x72, 0}} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 4096 {
			return
		}
		l := raftlog.DefaultLimits()
		if err := preflightWire(b, 0, l); err == nil {
			m := &pb.Message{}
			if err := proto.Unmarshal(b, m); err != nil {
				t.Fatal("preflight accepted malformed wire", err)
			}
			if len(m.Entries) > l.MaxReadEntries {
				t.Fatal("accepted expanded entries")
			}
		}
	})
}
