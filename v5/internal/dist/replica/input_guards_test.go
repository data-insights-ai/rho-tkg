package replica

import (
	"errors"
	"math"
	"runtime"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestInputGuardsRejectBeforeMutation(t *testing.T) {
	cases := map[string]*pb.Message{
		"vote_without_transport_term":        {Type: pb.MsgVote.Enum(), Index: new(uint64(2)), LogTerm: new(uint64(2))},
		"prevote_without_transport_term":     {Type: pb.MsgPreVote.Enum(), Index: new(uint64(2)), LogTerm: new(uint64(2))},
		"response_without_transport_term":    {Type: pb.MsgAppResp.Enum(), Index: new(uint64(2))},
		"empty_proposal":                     {Type: pb.MsgProp.Enum()},
		"malformed_config_proposal":          {Type: pb.MsgProp.Enum(), Entries: []*pb.Entry{{Type: pb.EntryConfChange.Enum(), Data: []byte{0x80}}}},
		"malformed_v2_proposal":              {Type: pb.MsgProp.Enum(), Entries: []*pb.Entry{{Type: pb.EntryConfChangeV2.Enum(), Data: []byte{0x80}}}},
		"append_gap":                         {Type: pb.MsgApp.Enum(), Term: new(uint64(9)), Index: new(uint64(2)), LogTerm: new(uint64(2)), Entries: []*pb.Entry{{Index: new(uint64(4)), Term: new(uint64(9))}}},
		"append_future_term":                 {Type: pb.MsgApp.Enum(), Term: new(uint64(9)), Index: new(uint64(2)), LogTerm: new(uint64(2)), Entries: []*pb.Entry{{Index: new(uint64(3)), Term: new(uint64(10))}}},
		"append_overflowing_index":           {Type: pb.MsgApp.Enum(), Term: new(uint64(9)), Index: new(uint64(math.MaxUint64)), LogTerm: new(uint64(1))},
		"heartbeat_beyond_last":              {Type: pb.MsgHeartbeat.Enum(), Term: new(uint64(9)), Commit: new(uint64(99))},
		"heartbeat_without_term":             {Type: pb.MsgHeartbeat.Enum()},
		"proposal_with_transport_term":       {Type: pb.MsgProp.Enum(), Term: new(uint64(9)), Entries: []*pb.Entry{{Data: command(1)}}},
		"append_first_gap":                   {Type: pb.MsgApp.Enum(), Term: new(uint64(9)), Index: new(uint64(2)), LogTerm: new(uint64(2)), Entries: []*pb.Entry{{Index: new(uint64(3)), Term: new(uint64(9))}, {Index: new(uint64(5)), Term: new(uint64(9))}}},
		"append_term_regression":             {Type: pb.MsgApp.Enum(), Term: new(uint64(9)), Index: new(uint64(2)), LogTerm: new(uint64(2)), Entries: []*pb.Entry{{Index: new(uint64(3)), Term: new(uint64(8))}, {Index: new(uint64(4)), Term: new(uint64(7))}}},
		"append_term_below_predecessor":      {Type: pb.MsgApp.Enum(), Term: new(uint64(9)), Index: new(uint64(2)), LogTerm: new(uint64(2)), Entries: []*pb.Entry{{Index: new(uint64(3)), Term: new(uint64(1))}}},
		"append_zero_entry_term":             {Type: pb.MsgApp.Enum(), Term: new(uint64(9)), Index: new(uint64(2)), LogTerm: new(uint64(2)), Entries: []*pb.Entry{{Index: new(uint64(3))}}},
		"append_zero_message_term":           {Type: pb.MsgApp.Enum(), Index: new(uint64(2)), LogTerm: new(uint64(2))},
		"append_zero_predecessor_term":       {Type: pb.MsgApp.Enum(), Term: new(uint64(9)), Index: new(uint64(2))},
		"append_invalid_zero_predecessor":    {Type: pb.MsgApp.Enum(), Term: new(uint64(9)), LogTerm: new(uint64(1))},
		"append_unrepresentable_final_entry": {Type: pb.MsgApp.Enum(), Term: new(uint64(9)), Index: new(uint64(math.MaxUint64 - 1)), LogTerm: new(uint64(1)), Entries: []*pb.Entry{{Index: new(uint64(math.MaxUint64)), Term: new(uint64(9))}}},
		"snapshot_future_term":               {Type: pb.MsgSnap.Enum(), Term: new(uint64(9)), Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(10)), ConfState: &pb.ConfState{Voters: []uint64{1}}}}},
		"snapshot_below_committed_term":      {Type: pb.MsgSnap.Enum(), Term: new(uint64(9)), Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1}}}}},
		"snapshot_without_configuration":     {Type: pb.MsgSnap.Enum(), Term: new(uint64(9)), Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(9))}}},
		"snapshot_zero_voter":                {Type: pb.MsgSnap.Enum(), Term: new(uint64(9)), Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(9)), ConfState: &pb.ConfState{Voters: []uint64{0, 1}}}}},
		"snapshot_no_voters":                 {Type: pb.MsgSnap.Enum(), Term: new(uint64(9)), Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(9)), ConfState: &pb.ConfState{Learners: []uint64{1}}}}},
		"snapshot_without_metadata":          {Type: pb.MsgSnap.Enum(), Term: new(uint64(9)), Snapshot: &pb.Snapshot{}},
		"snapshot_zero_term":                 {Type: pb.MsgSnap.Enum(), Term: new(uint64(9)), Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(10)), ConfState: &pb.ConfState{Voters: []uint64{1, 2}}}}},
		"snapshot_overflowing_index":         {Type: pb.MsgSnap.Enum(), Term: new(uint64(9)), Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(math.MaxUint64)), Term: new(uint64(9)), ConfState: &pb.ConfState{Voters: []uint64{1, 2}}}}},
		"snapshot_duplicate_voter":           {Type: pb.MsgSnap.Enum(), Term: new(uint64(9)), Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(9)), ConfState: &pb.ConfState{Voters: []uint64{1, 1, 2}}}}},
		"snapshot_invalid_joint":             {Type: pb.MsgSnap.Enum(), Term: new(uint64(9)), Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(9)), ConfState: &pb.ConfState{Voters: []uint64{1, 2}, LearnersNext: []uint64{1}}}}},
	}
	for name, m := range cases {
		t.Run(name, func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			d, state := driver(t, 1, []uint64{1}, fs)
			packets(t, d.Campaign)
			before, _, err := d.store.InitialState()
			if err != nil {
				t.Fatal(err)
			}
			applied := d.Applied()
			status := d.raw.BasicStatus()
			last, err := d.store.LastIndex()
			if err != nil {
				t.Fatal(err)
			}
			m.From, m.To = new(uint64(2)), new(uint64(1))
			b, err := proto.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			var out Output
			var stepErr error
			var panicValue any
			func() {
				defer func() { panicValue = recover() }()
				out, stepErr = d.Step(Packet{From: 2, To: 1, Payload: b, Snapshot: m.GetType() == pb.MsgSnap})
			}()
			if panicValue != nil {
				t.Fatalf("malformed packet panicked: %v", panicValue)
			}
			err = stepErr
			if !errors.Is(err, ErrInvalid) || len(out.Packets) != 0 || len(out.Reads) != 0 {
				t.Fatalf("invalid frame returned output: %+v %v", out, err)
			}
			after, _, err := d.store.InitialState()
			if err != nil {
				t.Fatal("invalid frame poisoned store", err)
			}
			afterStatus := d.raw.BasicStatus()
			afterLast, lastErr := d.store.LastIndex()
			if !proto.Equal(before, after) || applied != d.Applied() || state.value != 0 || lastErr != nil || afterLast != last || status.RaftState != afterStatus.RaftState || status.Lead != afterStatus.Lead || !proto.Equal(status.HardState, afterStatus.HardState) {
				t.Fatal("malformed frame changed state")
			}
			if _, err := d.Tick(); err != nil {
				t.Fatal("preflight rejection stopped replica", err)
			}
			// Reopen only synced bytes after rejection; compare durable membership,
			// hard state, complete tail and replayed application progress.
			s, err := raftlog.Open(raftlog.Config{Dir: "1", FS: fs.CrashClone(vfs.CrashCloneCfg{})})
			if err != nil {
				t.Fatal(err)
			}
			restored := &scalar{value: 999}
			r, err := Open(Config{ID: 1, Store: s, Machine: restored})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			rh, rc, err := s.InitialState()
			if err != nil || !proto.Equal(before, rh) || rc.Equivalent(d.conf) != nil {
				t.Fatal("rejection changed durable state", rh, rc, err)
			}
			packets(t, r.Tick)
			if r.Applied() != applied || restored.value != state.value {
				t.Fatal("rejection changed recovered application", r.Applied(), restored.value)
			}

		})
	}
}

func stepMessage(t *testing.T, d *Driver, m *pb.Message) (Output, error) {
	t.Helper()
	m.To = new(d.config.ID)
	if m.GetFrom() == 0 {
		m.From = new(uint64(2))
	}
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return d.Step(Packet{From: m.GetFrom(), To: m.GetTo(), Payload: b, Snapshot: m.GetType() == pb.MsgSnap})
}

func TestInputGuardsForwardingAndValidConfiguration(t *testing.T) {
	nodes := map[uint64]*Driver{}
	states := map[uint64]*scalar{}
	for id := uint64(1); id <= 3; id++ {
		nodes[id], states[id] = driver(t, id, []uint64{1, 2, 3}, vfs.NewMem())
	}
	deliver(t, nodes, packets(t, nodes[1].Campaign))
	forwarded := packets(t, func() (Output, error) { return nodes[2].Propose(command(7)) })
	if len(forwarded) != 1 || forwarded[0].To != 1 {
		t.Fatal("follower did not forward", forwarded)
	}
	m := &pb.Message{}
	if err := proto.Unmarshal(forwarded[0].Payload, m); err != nil || m.GetType() != pb.MsgProp || m.GetTerm() != 0 {
		t.Fatal(m, err)
	}
	deliver(t, nodes, forwarded)
	for _, d := range nodes {
		deliver(t, nodes, packets(t, d.Tick))
	}
	for id, state := range states {
		if state.value != 7 {
			t.Fatal(id, state.value)
		}
	}
	cc := &pb.ConfChange{Id: new(uint64(77)), Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: new(uint64(4)), Context: []byte("context")}
	deliver(t, nodes, packets(t, func() (Output, error) { return nodes[2].ProposeConfChange(cc) }))
	if !contains(nodes[1].conf.Learners, 4) {
		t.Fatal(nodes[1].conf)
	}
}

func TestInputGuardsValidAppendReplayAndNormalApplicationFailure(t *testing.T) {
	d, state := driver(t, 1, []uint64{1, 2}, vfs.NewMem())
	m := &pb.Message{Type: pb.MsgApp.Enum(), Term: new(uint64(2)), Index: new(uint64(1)), LogTerm: new(uint64(1)), Commit: new(uint64(2)), Entries: []*pb.Entry{{Index: new(uint64(2)), Term: new(uint64(2)), Data: command(3)}}}
	for range 2 {
		out, err := stepMessage(t, d, m)
		if err != nil || state.value != 3 || d.Applied() != 2 {
			t.Fatal(out, state.value, err)
		}
	}
	// The opaque application payload is not a consensus syntax violation.
	// A real reducer error remains stopped, rather than becoming retryable input.
	m.Index, m.LogTerm, m.Commit = new(uint64(2)), new(uint64(2)), new(uint64(3))
	m.Entries = []*pb.Entry{{Index: new(uint64(3)), Term: new(uint64(2)), Data: []byte("bad")}}
	if out, err := stepMessage(t, d, m); !errors.Is(err, ErrInvalid) || len(out.Packets) != 0 {
		t.Fatal(out, err)
	}
	if _, err := d.Tick(); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
}

func TestInputGuardsConfigurationSchemasAndAdmission(t *testing.T) {
	d, _ := driver(t, 1, []uint64{1}, vfs.NewMem())
	packets(t, d.Campaign)
	cases := []struct {
		name   string
		kind   pb.EntryType
		change proto.Message
	}{
		{"remove all voters", pb.EntryConfChange, &pb.ConfChange{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(uint64(1))}},
		{"zero node", pb.EntryConfChange, &pb.ConfChange{NodeId: new(uint64(0))}},
		{"local target", pb.EntryConfChange, &pb.ConfChange{NodeId: new(uint64(math.MaxUint64))}},
		{"invalid v1 type", pb.EntryConfChange, &pb.ConfChange{Type: pb.ConfChangeType(99).Enum(), NodeId: new(uint64(2))}},
		{"invalid transition", pb.EntryConfChangeV2, &pb.ConfChangeV2{Transition: pb.ConfChangeTransition(99).Enum(), Changes: []*pb.ConfChangeSingle{{NodeId: new(uint64(2))}}}},
	}
	for _, unknown := range configUnknownCases() {
		cases = append(cases, struct {
			name   string
			kind   pb.EntryType
			change proto.Message
		}{unknown.name, unknown.kind, unknown.change.(proto.Message)})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data, err := proto.Marshal(c.change)
			if err != nil {
				t.Fatal(err)
			}
			before := d.raw.BasicStatus()
			out, err := stepMessage(t, d, &pb.Message{Type: pb.MsgProp.Enum(), Entries: []*pb.Entry{{Type: c.kind.Enum(), Data: data}}})
			after := d.raw.BasicStatus()
			if !errors.Is(err, ErrInvalid) || len(out.Packets) != 0 || !proto.Equal(before.HardState, after.HardState) || d.stopped != nil {
				t.Fatal(out, err)
			}
		})
	}
	var payload []byte
	for range 257 {
		payload = protowire.AppendTag(payload, 2, protowire.BytesType)
		payload = protowire.AppendBytes(payload, nil)
	}
	m := &pb.Message{Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: []*pb.Entry{{Type: pb.EntryConfChangeV2.Enum(), Data: payload}}}
	wire, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	out, err := d.Step(Packet{From: 2, To: 1, Payload: wire})
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrLimit) || len(out.Packets) != 0 || after.TotalAlloc-before.TotalAlloc > 32<<10 {
		t.Fatal(out, err, after.TotalAlloc-before.TotalAlloc)
	}
	t.Logf("257 configuration children rejected before expansion: %dB total allocations", after.TotalAlloc-before.TotalAlloc)
}

func TestInputGuardsProtobufBooleanAndConfigurationWireCompatibility(t *testing.T) {
	b := protowire.AppendTag(nil, 10, protowire.VarintType)
	b = protowire.AppendVarint(b, 2)
	if err := preflightWire(b, 0, raftlog.DefaultLimits()); err != nil {
		t.Fatal(err)
	}
	m := &pb.Message{}
	if err := proto.Unmarshal(b, m); err != nil || !m.GetReject() {
		t.Fatal(m, err)
	}
	// This is valid protobuf nonzero=true, not malformed consensus input.
	for _, c := range []struct {
		kind    int
		message proto.Message
	}{
		{6, &pb.ConfChange{Id: new(uint64(1)), Type: pb.ConfChangeUpdateNode.Enum(), NodeId: new(uint64(1)), Context: []byte("context")}},
		{7, &pb.ConfChangeV2{Transition: pb.ConfChangeTransitionJointExplicit.Enum(), Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeUpdateNode.Enum(), NodeId: new(uint64(1))}}, Context: []byte("context")}},
		{8, &pb.ConfChangeSingle{Type: pb.ConfChangeUpdateNode.Enum(), NodeId: new(uint64(1))}},
	} {
		b, err := proto.Marshal(c.message)
		if err != nil {
			t.Fatal(err)
		}
		if err := preflightWire(b, c.kind, raftlog.DefaultLimits()); err != nil {
			t.Fatal(c.kind, err)
		}
	}
	for _, c := range []struct {
		kind  int
		field protowire.Number
		typ   protowire.Type
	}{
		{6, 1, protowire.BytesType}, {6, 4, protowire.VarintType}, {6, 5, protowire.VarintType},
		{7, 1, protowire.BytesType}, {7, 2, protowire.VarintType}, {7, 3, protowire.VarintType}, {7, 4, protowire.VarintType},
		{8, 1, protowire.BytesType}, {8, 3, protowire.VarintType},
	} {
		if err := preflightWire(field(c.field, c.typ), c.kind, raftlog.DefaultLimits()); !errors.Is(err, ErrInvalid) {
			t.Fatal(c, err)
		}
	}
}

func TestInputGuardsStoragePreconditionFailureStillStops(t *testing.T) {
	for _, kind := range []pb.MessageType{pb.MsgSnap, pb.MsgHeartbeat} {
		t.Run(kind.String(), func(t *testing.T) {
			d, _ := driver(t, 1, []uint64{1}, vfs.NewMem())
			packets(t, d.Campaign)
			if err := d.store.Close(); err != nil {
				t.Fatal(err)
			}
			m := &pb.Message{Type: kind.Enum(), Term: new(uint64(9)), Commit: new(uint64(2))}
			if kind == pb.MsgSnap {
				m.Snapshot = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(9)), ConfState: &pb.ConfState{Voters: []uint64{1}}}}
			}
			out, err := stepMessage(t, d, m)
			if !errors.Is(err, raftlog.ErrClosed) || len(out.Packets) != 0 {
				t.Fatal(out, err)
			}
			if _, err := d.Tick(); !errors.Is(err, ErrStopped) || !errors.Is(err, raftlog.ErrClosed) {
				t.Fatal(err)
			}
		})
	}
}

func TestInputGuardsConfigurationEnvelopeDoesNotUseFollowerMembership(t *testing.T) {
	data, err := proto.Marshal(&pb.ConfChangeV2{})
	if err != nil {
		t.Fatal(err)
	}
	m := &pb.Message{Type: pb.MsgProp.Enum(), Entries: []*pb.Entry{{Type: pb.EntryConfChangeV2.Enum(), Data: data}}}
	// A lagging follower can forward a leave-joint proposal to the leader,
	// which may have applied a newer membership than this follower has.
	cs := &pb.ConfState{Voters: []uint64{1}}
	if err := validateIncoming(m, cs, 1, false, raftlog.DefaultLimits()); err != nil {
		t.Fatal("blocked valid forwarding against follower config", err)
	}
	if err := validateIncoming(m, cs, 1, true, raftlog.DefaultLimits()); !errors.Is(err, ErrInvalid) {
		t.Fatal("leader did not use changer", err)
	}
	if _, err := decodeIncomingChange(&pb.Entry{Type: pb.EntryType(99).Enum()}, raftlog.DefaultLimits()); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestInputGuardsLaggingFollowerForwardsLeaveJoint(t *testing.T) {
	for _, network := range []bool{false, true} {
		t.Run(map[bool]string{false: "local", true: "network"}[network], func(t *testing.T) {
			nodes := map[uint64]*Driver{}
			for id := uint64(1); id <= 3; id++ {
				nodes[id], _ = driver(t, id, []uint64{1, 2, 3}, vfs.NewMem())
			}
			deliver(t, nodes, packets(t, nodes[1].Campaign))
			joint := &pb.ConfChangeV2{Transition: pb.ConfChangeTransitionJointExplicit.Enum(), Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: new(uint64(4))}}}
			deliver(t, map[uint64]*Driver{1: nodes[1], 3: nodes[3]}, packets(t, func() (Output, error) { return nodes[1].ProposeConfChange(joint) }))
			if len(nodes[1].conf.VotersOutgoing) != 3 || len(nodes[2].conf.VotersOutgoing) != 0 || nodes[2].Applied() >= nodes[1].Applied() {
				t.Fatal("fixture did not leave follower behind", nodes[1].conf, nodes[2].conf)
			}
			var out Output
			var err error
			if network {
				out, err = stepMessage(t, nodes[2], &pb.Message{Type: pb.MsgProp.Enum(), From: new(uint64(3)), Entries: []*pb.Entry{{Type: pb.EntryConfChangeV2.Enum()}}})
			} else {
				out, err = nodes[2].ProposeConfChange(&pb.ConfChangeV2{})
			}
			if err != nil || len(out.Packets) != 1 || out.Packets[0].To != 1 {
				t.Fatal("lagging follower blocked leave-joint forwarding", out, err)
			}
			m := &pb.Message{}
			if err := proto.Unmarshal(out.Packets[0].Payload, m); err != nil || m.GetTerm() != 0 || m.GetType() != pb.MsgProp {
				t.Fatal("invalid forwarded envelope", m, err)
			}
			deliver(t, nodes, out.Packets)
			for range 3 {
				deliver(t, nodes, packets(t, nodes[1].Tick))
			}
			want := &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}, AutoLeave: new(false)}
			for id, d := range nodes {
				if err := want.Equivalent(d.conf); err != nil || d.Applied() != nodes[1].Applied() {
					t.Fatal("lagging config replay failed", id, d.conf, err)
				}
			}
			// The same proposal after leaving joint must be refused at the leader.
			if _, err := stepMessage(t, nodes[1], &pb.Message{Type: pb.MsgProp.Enum(), Entries: []*pb.Entry{{Type: pb.EntryConfChangeV2.Enum()}}}); !errors.Is(err, ErrInvalid) {
				t.Fatal("leader accepted leave-joint in a simple membership", err)
			}
		})
	}
}

func TestInputGuardsSnapshotNonzeroBooleanReplayAndReopen(t *testing.T) {
	fs := vfs.NewCrashableMem()
	d, state := driver(t, 1, []uint64{1, 2, 3}, fs)
	// Write the boolean as varint 2, retaining protobuf's nonzero=true rule.
	cs := &pb.ConfState{Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 2, 3}, LearnersNext: []uint64{3}}
	conf, err := proto.Marshal(cs)
	if err != nil {
		t.Fatal(err)
	}
	conf = protowire.AppendVarint(protowire.AppendTag(conf, 5, protowire.VarintType), 2)
	md := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), conf)
	md = protowire.AppendVarint(protowire.AppendTag(md, 2, protowire.VarintType), 10)
	md = protowire.AppendVarint(protowire.AppendTag(md, 3, protowire.VarintType), 2)
	snap := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), command(73))
	snap = protowire.AppendBytes(protowire.AppendTag(snap, 2, protowire.BytesType), md)
	wire, err := proto.Marshal(&pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(2))})
	if err != nil {
		t.Fatal(err)
	}
	wire = protowire.AppendBytes(protowire.AppendTag(wire, 9, protowire.BytesType), snap)
	out, err := d.Step(Packet{From: 2, To: 1, Snapshot: true, Payload: wire})
	if err != nil || state.value != 73 || d.Applied() != 10 || !d.conf.GetAutoLeave() || len(d.conf.LearnersNext) != 1 {
		t.Fatal("valid joint snapshot rejected or replaced", out, d.conf, state.value, err)
	}
	// A duplicate snapshot is obsolete and must not reapply its image.
	if out, err := d.Step(Packet{From: 2, To: 1, Snapshot: true, Payload: wire}); err != nil || out.Applied != 10 || state.value != 73 {
		t.Fatal("snapshot duplicate changed application", out, state.value, err)
	}
	// The follower applies leave-joint from the log, after the snapshot's membership.
	if _, err := stepMessage(t, d, &pb.Message{Type: pb.MsgApp.Enum(), Term: new(uint64(2)), Index: new(uint64(10)), LogTerm: new(uint64(2)), Commit: new(uint64(11)), Entries: []*pb.Entry{{Type: pb.EntryConfChangeV2.Enum(), Index: new(uint64(11)), Term: new(uint64(2))}}}); err != nil {
		t.Fatal(err)
	}
	crash := fs.CrashClone(vfs.CrashCloneCfg{})
	s, err := raftlog.Open(raftlog.Config{Dir: "1", FS: crash})
	if err != nil {
		t.Fatal(err)
	}
	restored := &scalar{value: 999}
	r, err := Open(Config{ID: 1, Store: s, Machine: restored})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	packets(t, r.Tick)
	want := &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}, AutoLeave: new(false)}
	if restored.value != 73 || r.Applied() != 11 || want.Equivalent(r.conf) != nil {
		t.Fatal("snapshot and tail membership not recovered together", restored.value, r.Applied(), r.conf)
	}
}

func TestInputGuardsConfigurationDecodeAndUnorderedEntryFields(t *testing.T) {
	l := raftlog.DefaultLimits()
	for _, kind := range []pb.EntryType{pb.EntryConfChange, pb.EntryConfChangeV2} {
		if _, err := decodeIncomingChange(&pb.Entry{Type: kind.Enum(), Data: []byte{0x80}}, l); !errors.Is(err, ErrInvalid) {
			t.Fatal("decode accepted truncated configuration", kind, err)
		}
	}
	// Protobuf field order is arbitrary: Entry.Data may precede Entry.Type.
	var children []byte
	for range 257 {
		children = protowire.AppendBytes(protowire.AppendTag(children, 2, protowire.BytesType), nil)
	}
	entry := protowire.AppendBytes(protowire.AppendTag(nil, 4, protowire.BytesType), children)
	entry = protowire.AppendVarint(protowire.AppendTag(entry, 1, protowire.VarintType), uint64(pb.EntryConfChangeV2))
	if err := preflightWire(entry, 1, l); !errors.Is(err, ErrLimit) {
		t.Fatal("field order bypassed configuration child bound", err)
	}
	if err := validateSnapshotConf(&pb.ConfState{Voters: make([]uint64, 257)}, 2); !errors.Is(err, ErrLimit) {
		t.Fatal("snapshot helper bypassed membership bound", err)
	}
	for _, kind := range []int{6, 7, 8} {
		for _, b := range [][]byte{{0x80}, {0x08, 0x80}, {0x0b}, {0x08, 0, 0x08, 0}} {
			if err := preflightWire(b, kind, l); !errors.Is(err, ErrInvalid) {
				t.Fatal(kind, b, err)
			}
		}
	}
}

func TestInputGuardsFiniteTermRejectsWithoutMutation(t *testing.T) {
	for _, kind := range []pb.MessageType{pb.MsgVote, pb.MsgHeartbeat, pb.MsgPreVote, pb.MsgPreVoteResp, pb.MsgTimeoutNow} {
		t.Run(kind.String(), func(t *testing.T) {
			d, _ := driver(t, 1, []uint64{1, 2, 3}, vfs.NewMem())
			before := d.raw.BasicStatus()
			beforeDurable, _, err := d.store.InitialState()
			if err != nil {
				t.Fatal(err)
			}
			for _, term := range []uint64{math.MaxUint64, replicaTermCeiling} {
				if term == replicaTermCeiling && kind != pb.MsgTimeoutNow {
					continue
				}
				want := ErrInvalid
				if term == replicaTermCeiling {
					want = ErrLimit
				}
				out, err := stepMessage(t, d, &pb.Message{Type: kind.Enum(), Term: new(term), Index: new(uint64(1)), LogTerm: new(uint64(1)), Commit: new(uint64(1))})
				h, _, storeErr := d.store.InitialState()
				after := d.raw.BasicStatus()
				if !errors.Is(err, want) || len(out.Packets) != 0 || len(out.Reads) != 0 || storeErr != nil || !proto.Equal(beforeDurable, h) || !proto.Equal(before.HardState, after.HardState) || before.RaftState != after.RaftState || before.Lead != after.Lead || d.stopped != nil {
					t.Fatal("term rejection mutated state", term, out, after, h, err, storeErr)
				}
			}
			if _, err := d.Campaign(); err != nil {
				t.Fatal("rejection blocked ordinary election", err)
			}
		})
	}
}

func TestInputGuardsFinalTermAndExhaustedReopen(t *testing.T) {
	for _, kind := range []pb.MessageType{pb.MsgVote, pb.MsgHeartbeat} {
		t.Run(kind.String(), func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			d, _ := driver(t, 1, []uint64{1, 2, 3}, fs)
			if _, err := stepMessage(t, d, &pb.Message{Type: kind.Enum(), Term: new(replicaTermCeiling), Index: new(uint64(1)), LogTerm: new(uint64(1)), Commit: new(uint64(1))}); err != nil {
				t.Fatal("final supported term rejected", err)
			}
			before := d.raw.BasicStatus()
			for _, event := range []func() (Output, error){d.Campaign, d.Tick, func() (Output, error) {
				return stepMessage(t, d, &pb.Message{Type: pb.MsgTimeoutNow.Enum(), Term: new(replicaTermCeiling)})
			}} {
				out, err := event()
				h, _, storeErr := d.store.InitialState()
				if !errors.Is(err, ErrLimit) || len(out.Packets) != 0 || storeErr != nil || h.GetTerm() != replicaTermCeiling || !proto.Equal(before.HardState, h) || !proto.Equal(before.HardState, d.raw.BasicStatus().HardState) {
					t.Fatal("exhausted event advanced state", out, h, err, storeErr)
				}
			}
			assertExhaustedTermReopen(t, fs, replicaTermCeiling)
		})
	}
	fs := vfs.NewCrashableMem()
	d, _ := driver(t, 1, []uint64{1}, fs)
	if err := d.store.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(math.MaxUint64)), Commit: new(uint64(1))}}); err != nil {
		t.Fatal(err)
	}
	assertExhaustedTermReopen(t, fs, math.MaxUint64)
}

func assertExhaustedTermReopen(t *testing.T, fs *vfs.MemFS, term uint64) {
	t.Helper()
	s, err := raftlog.Open(raftlog.Config{Dir: "1", FS: fs.CrashClone(vfs.CrashCloneCfg{})})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := Open(Config{ID: 1, Store: s, Machine: &scalar{failRestore: ErrInvalid}}); !errors.Is(err, ErrLimit) || errors.Is(err, ErrInvalid) {
		t.Fatal("exhausted reopen not rejected before Restore", err)
	}
	h, _, err := s.InitialState()
	if err != nil || h.GetTerm() != term {
		t.Fatal("exhausted reopen changed durable term", h, err)
	}
}

func TestInputGuardsLastSupportedElectionAndTimeoutNow(t *testing.T) {
	for _, event := range []string{"campaign", "timeout_now", "tick"} {
		t.Run(event, func(t *testing.T) {
			d, _ := driver(t, 1, []uint64{1}, vfs.NewMem())
			if _, err := stepMessage(t, d, &pb.Message{Type: pb.MsgHeartbeat.Enum(), Term: new(replicaTermCeiling - 1), Commit: new(uint64(1))}); err != nil {
				t.Fatal(err)
			}
			var out Output
			var err error
			switch event {
			case "timeout_now":
				out, err = stepMessage(t, d, &pb.Message{Type: pb.MsgTimeoutNow.Enum(), Term: new(replicaTermCeiling - 1)})
			case "campaign":
				out, err = d.Campaign()
			case "tick":
				for range 30 {
					out, err = d.Tick()
					if err != nil || out.Applied == 2 {
						break
					}
				}
			}
			h, _, storeErr := d.store.InitialState()
			if err != nil || storeErr != nil || h.GetTerm() != replicaTermCeiling || d.raw.BasicStatus().RaftState != raft.StateLeader || out.Applied != 2 {
				t.Fatal("last supported election did not finish", out, h, err, storeErr)
			}
			if _, err := d.Tick(); !errors.Is(err, ErrLimit) {
				t.Fatal("final-term tick did not refuse next advancement", err)
			}
		})
	}
}
