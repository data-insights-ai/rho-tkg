package replica

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func configUnknownCases() []struct {
	name   string
	change pb.ConfChangeI
	kind   pb.EntryType
} {
	unknown := []byte{0xa0, 0x06, 1}
	v1 := &pb.ConfChange{Type: pb.ConfChangeUpdateNode.Enum(), NodeId: new(uint64(1))}
	v1.ProtoReflect().SetUnknown(unknown)
	v2 := &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeUpdateNode.Enum(), NodeId: new(uint64(1))}}}
	v2.ProtoReflect().SetUnknown(unknown)
	child := &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeUpdateNode.Enum(), NodeId: new(uint64(1))}}}
	child.Changes[0].ProtoReflect().SetUnknown(unknown)
	return []struct {
		name   string
		change pb.ConfChangeI
		kind   pb.EntryType
	}{{"V1 root", v1, pb.EntryConfChange}, {"V2 root", v2, pb.EntryConfChangeV2}, {"V2 child", child, pb.EntryConfChangeV2}}
}

func TestConfigurationUnknownFieldsRejectedBeforeProposalAndReplay(t *testing.T) {
	for _, c := range configUnknownCases() {
		t.Run(c.name, func(t *testing.T) {
			d, _ := driver(t, 1, []uint64{1}, vfs.NewMem())
			if out, err := d.ProposeConfChange(c.change); !errors.Is(err, ErrInvalid) || len(out.Packets) != 0 {
				t.Fatal("proposal schema accepted", out, err)
			}
			if last, err := d.store.LastIndex(); err != nil || last != 1 || d.stopped != nil {
				t.Fatal("invalid proposal changed durable state", last, err)
			}
			// Simulate a verified log from an unsupported newer configuration schema.
			// Entry.Data is opaque to storage; the replica must validate it before apply.
			var data []byte
			var err error
			switch change := c.change.(type) {
			case *pb.ConfChange:
				data, err = proto.Marshal(change)
			case *pb.ConfChangeV2:
				data, err = proto.Marshal(change)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := d.store.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(2))}, Entries: []*pb.Entry{{Index: new(uint64(2)), Term: new(uint64(2)), Type: c.kind.Enum(), Data: data}}}); err != nil {
				t.Fatal(err)
			}
			r, err := Open(Config{ID: 1, Store: d.store, Machine: &scalar{}})
			if err != nil {
				t.Fatal(err)
			}
			out, err := r.Tick()
			if !errors.Is(err, raftlog.ErrCorrupt) || !errors.Is(err, ErrInvalid) || len(out.Packets) != 0 || r.Applied() != 1 || len(r.conf.Voters) != 1 || r.conf.Voters[0] != 1 {
				t.Fatal("replay schema not fail-closed", out, r.conf, err)
			}
			if _, err := r.Tick(); !errors.Is(err, ErrStopped) || !errors.Is(err, raftlog.ErrCorrupt) || !errors.Is(err, ErrInvalid) {
				t.Fatal("stopped layer lost schema sentinels", err)
			}
		})
	}
}
