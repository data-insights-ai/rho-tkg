package replica

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type scalar struct {
	value                                  int64
	failApply, failRestore, failCheckpoint error
	oversized                              bool
}

func (m *scalar) Restore(_ uint64, image []byte) error {
	if m.failRestore != nil {
		return m.failRestore
	}
	m.value = 0
	if len(image) == 8 {
		m.value = int64(binary.BigEndian.Uint64(image))
	} else if len(image) != 0 {
		return ErrInvalid
	}
	return nil
}
func (m *scalar) Apply(e Entry) error {
	if m.failApply != nil {
		return m.failApply
	}
	if len(e.Data) == 0 {
		return nil
	}
	if len(e.Data) != 8 {
		return ErrInvalid
	}
	m.value += int64(binary.BigEndian.Uint64(e.Data))
	return nil
}
func (m *scalar) Checkpoint(max int) ([]byte, error) {
	if m.failCheckpoint != nil {
		return nil, m.failCheckpoint
	}
	if m.oversized {
		return make([]byte, max+1), nil
	}
	return binary.BigEndian.AppendUint64(nil, uint64(m.value)), nil
}
func command(n uint64) []byte { return binary.BigEndian.AppendUint64(nil, n) }
func driver(t *testing.T, id uint64, peers []uint64, fs vfs.FS) (*Driver, *scalar) {
	t.Helper()
	s, err := raftlog.Open(raftlog.Config{Dir: fmt.Sprint(id), FS: fs, Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Initialize(peers, nil); err != nil {
		t.Fatal(err)
	}
	m := &scalar{}
	d, err := Open(Config{ID: id, Store: s, Machine: m})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d, m
}
func packets(t *testing.T, fn func() (Output, error)) []Packet {
	t.Helper()
	o, err := fn()
	if err != nil {
		t.Fatal(err)
	}
	return o.Packets
}
func deliver(t *testing.T, nodes map[uint64]*Driver, queue []Packet) {
	t.Helper()
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 10000 {
			t.Fatal("unbounded transport")
		}
		p := queue[0]
		queue = queue[1:]
		if d := nodes[p.To]; d != nil {
			o, err := d.Step(p)
			if err != nil {
				t.Fatalf("step %d -> %d: %v", p.From, p.To, err)
			}
			queue = append(queue, o.Packets...)
		}
	}
}

func TestTwoIndependentThreeReplicaGroupsAndReopen(t *testing.T) {
	// This establishes only adapter replication/recovery, not cross-group tx/cuts.
	for group := range 2 {
		t.Run(fmt.Sprint(group), func(t *testing.T) {
			nodes := map[uint64]*Driver{}
			machines := map[uint64]*scalar{}
			files := map[uint64]*vfs.MemFS{}
			for id := uint64(1); id <= 3; id++ {
				fs := vfs.NewCrashableMem()
				d, m := driver(t, id, []uint64{1, 2, 3}, fs)
				nodes[id], machines[id], files[id] = d, m, fs
			}
			deliver(t, nodes, packets(t, nodes[1].Campaign))
			deliver(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(7)) }))
			deliver(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(11)) }))
			for _, d := range nodes {
				deliver(t, nodes, packets(t, d.Tick))
			}
			for id, m := range machines {
				if m.value != 18 {
					t.Fatalf("%d got %d", id, m.value)
				}
			}
			// Commit a proposal while replica 3 is partitioned: majority survives,
			// isolated replica has no new value. Reopen from its synced old image.
			crash := files[3].CrashClone(vfs.CrashCloneCfg{})
			delete(nodes, 3)
			deliver(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(23)) }))
			if machines[1].value != 41 || machines[3].value != 18 {
				t.Fatal("minority exposed committed value")
			}
			store, err := raftlog.Open(raftlog.Config{Dir: "3", FS: crash})
			if err != nil {
				t.Fatal(err)
			}
			m := &scalar{}
			r, err := Open(Config{ID: 3, Store: store, Machine: m})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			nodes[3] = r
			for range 5 {
				deliver(t, nodes, packets(t, nodes[1].Tick))
			}
			if m.value != 41 {
				t.Fatalf("replay/catchup got %d", m.value)
			}
			if err := r.SaveCheckpoint(); err != nil {
				t.Fatal(err)
			}
			if err := store.PublishSnapshot(); err != nil {
				t.Fatal(err)
			}
			if err := store.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMembershipV1JointV2AndReplay(t *testing.T) {
	nodes := map[uint64]*Driver{}
	files := map[uint64]*vfs.MemFS{}
	for id := uint64(1); id <= 3; id++ {
		fs := vfs.NewCrashableMem()
		files[id] = fs
		d, _ := driver(t, id, []uint64{1, 2, 3}, fs)
		nodes[id] = d
	}
	deliver(t, nodes, packets(t, nodes[1].Campaign))
	v1 := &pb.ConfChange{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: new(uint64(4))}
	deliver(t, nodes, packets(t, func() (Output, error) { return nodes[1].ProposeConfChange(v1) }))
	if !contains(nodes[1].conf.Learners, 4) {
		t.Fatal("V1 not applied")
	}
	joint := &pb.ConfChangeV2{Transition: pb.ConfChangeTransitionJointExplicit.Enum(), Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeRemoveNode.Enum(), NodeId: new(uint64(3))}, {Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: new(uint64(3))}}}
	deliver(t, nodes, packets(t, func() (Output, error) { return nodes[1].ProposeConfChange(joint) }))
	if len(nodes[1].conf.VotersOutgoing) != 3 || !contains(nodes[1].conf.LearnersNext, 3) {
		t.Fatal(nodes[1].conf)
	}
	if err := nodes[1].SaveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	// InitialState must be the checkpoint's joint state, not a future membership.
	_, cs, err := nodes[1].store.InitialState()
	if err != nil || cs.Equivalent(nodes[1].conf) != nil {
		t.Fatal(cs, err)
	}
	deliver(t, nodes, packets(t, func() (Output, error) { return nodes[1].ProposeConfChange(&pb.ConfChangeV2{}) }))
	if len(nodes[1].conf.VotersOutgoing) != 0 || !contains(nodes[1].conf.Learners, 3) {
		t.Fatal(nodes[1].conf)
	}
	// No checkpoint after leave-joint: replay must start from joint checkpoint.
	crash := files[1].CrashClone(vfs.CrashCloneCfg{})
	s, err := raftlog.Open(raftlog.Config{Dir: "1", FS: crash})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, image, err := s.Checkpoint()
	if err != nil || checkpoint >= nodes[1].Applied() || len(image) != 8 {
		t.Fatal("did not reopen older joint checkpoint", checkpoint, err)
	}
	_, initial, err := s.InitialState()
	if err != nil || len(initial.VotersOutgoing) != 3 {
		t.Fatal("not joint restart membership", initial, err)
	}
	state := &scalar{value: 999}
	r, err := Open(Config{ID: 1, Store: s, Machine: state})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Tick(); err != nil {
		t.Fatal(err)
	}
	if r.conf.Equivalent(nodes[1].conf) != nil || r.Applied() != nodes[1].Applied() || state.value != 0 {
		t.Fatal("joint checkpoint replay mismatch", r.conf, r.Applied(), state.value)
	}
}

func contains(ids []uint64, id uint64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

func TestPersistBeforePacket(t *testing.T) {
	base := vfs.NewCrashableMem()
	var mu sync.Mutex
	var trace []errorfs.Op
	fs := errorfs.Wrap(base, errorfs.InjectorFunc(func(op errorfs.Op) error {
		mu.Lock()
		defer mu.Unlock()
		trace = append(trace, op)
		return nil
	}))
	d, _ := driver(t, 1, []uint64{1, 2, 3}, fs)
	mu.Lock()
	trace = nil
	mu.Unlock()
	out, err := d.Campaign()
	if err != nil {
		t.Fatal(err)
	}
	// Prevote has no persistent term change. Deliver a genuine higher-term vote
	// request to force a persisted vote before its returned vote response.
	m := &pb.Message{Type: pb.MsgVote.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(5)), Index: new(uint64(1)), LogTerm: new(uint64(1))}
	wire, _ := proto.Marshal(m)
	out, err = d.Step(Packet{From: 2, To: 1, Payload: wire})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Packets) == 0 {
		t.Fatal("no vote response")
	}
	mu.Lock()
	synced := false
	for _, op := range trace {
		if op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData {
			synced = true
		}
	}
	mu.Unlock()
	if !synced {
		t.Fatal("packet without sync")
	}
	crash := base.CrashClone(vfs.CrashCloneCfg{})
	r, err := raftlog.Open(raftlog.Config{Dir: "1", FS: crash})
	if err != nil {
		t.Fatal(err)
	}
	h, _, _ := r.InitialState()
	if h.GetTerm() != 5 || h.GetVote() != 2 {
		t.Fatal(h)
	}
	r.Close()

}

func TestReplayAfterApplyAndCheckpointCrashes(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		t.Run(fmt.Sprint(checkpoint), func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			d, m := driver(t, 1, []uint64{1}, fs)
			packets(t, d.Campaign)
			packets(t, func() (Output, error) { return d.Propose(command(9)) })
			if m.value != 9 {
				t.Fatal(m.value)
			}
			if checkpoint {
				if err := d.SaveCheckpoint(); err != nil {
					t.Fatal(err)
				}
			}
			crash := fs.CrashClone(vfs.CrashCloneCfg{})
			s, err := raftlog.Open(raftlog.Config{Dir: "1", FS: crash})
			if err != nil {
				t.Fatal(err)
			}
			state := &scalar{value: 999}
			r, err := Open(Config{ID: 1, Store: s, Machine: state})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			packets(t, r.Tick)
			if state.value != 9 {
				t.Fatalf("replay duplicated/substituted current state: %d", state.value)
			}
		})
	}
}

func TestReadIndexQuotaCancellationAndPendingApplication(t *testing.T) {
	d, _ := driver(t, 1, []uint64{1}, vfs.NewMem())
	packets(t, d.Campaign)
	o, err := d.ReadIndex([]byte("now"))
	if err != nil || len(o.Reads) != 1 || o.Reads[0].Index > d.Applied() {
		t.Fatal(o, err)
	}
	// Isolate a three-voter leader and require bounded unknown read outcomes.
	nodes := map[uint64]*Driver{}
	for id := uint64(1); id <= 3; id++ {
		r, _ := driver(t, id, []uint64{1, 2, 3}, vfs.NewMem())
		nodes[id] = r
	}
	deliver(t, nodes, packets(t, nodes[1].Campaign))
	leader := nodes[1]
	for i := range 32 {
		o, err := leader.ReadIndex([]byte(fmt.Sprint(i)))
		if err != nil || len(o.Reads) != 0 {
			t.Fatal(i, o, err)
		}
	}
	if _, err := leader.ReadIndex([]byte("overflow")); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := leader.ReadIndex([]byte("0")); err != nil {
		t.Fatal("retry consumed quota", err)
	}
	if err := leader.CancelReadIndex([]byte("0")); err != nil {
		t.Fatal(err)
	}
	if _, err := leader.ReadIndex([]byte("overflow")); !errors.Is(err, ErrLimit) {
		t.Fatal("cancel released hidden RawNode queue", err)
	}
	var seen []ReadResult
	healed, err := leader.ReadIndex([]byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	seen = append(seen, healed.Reads...)
	queue := healed.Packets
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 10000 {
			t.Fatal("transport loop")
		}
		p := queue[0]
		queue = queue[1:]
		o, err := nodes[p.To].Step(p)
		if err != nil {
			t.Fatal(err)
		}
		if p.To == 1 {
			seen = append(seen, o.Reads...)
		}
		queue = append(queue, o.Packets...)
	}
	for range 3 {
		out, err := leader.Tick()
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, out.Reads...)
		queue = out.Packets
		for len(queue) > 0 {
			p := queue[0]
			queue = queue[1:]
			o, err := nodes[p.To].Step(p)
			if err != nil {
				t.Fatal(err)
			}
			if p.To == 1 {
				seen = append(seen, o.Reads...)
			}
			queue = append(queue, o.Packets...)
		}
	}
	exact := map[string]bool{}
	for _, r := range seen {
		key := string(r.Context)
		if key == "0" || exact[key] {
			t.Fatal("cancelled or duplicate read delivered", key)
		}
		exact[key] = true
	}
	if len(exact) != 31 || len(leader.reads) != 0 {
		t.Fatal("uncancelled reads did not resolve exactly once", len(exact), len(leader.reads))
	}
	// A quorum loss followed by leader transition releases bounded abandoned
	// requests without manufacturing results. Cancelled requests stay suppressed.
	for i := range 4 {
		if _, err := leader.ReadIndex([]byte(fmt.Sprint("later", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := leader.CancelReadIndex([]byte("later0")); err != nil {
		t.Fatal(err)
	}
	for range 30 {
		out, err := leader.Tick()
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Reads) != 0 {
			t.Fatal("minority returned read", out)
		}
	}
	if len(leader.reads) != 0 {
		t.Fatal("leader transition retained hidden admission")
	}
	if _, err := leader.ReadIndex([]byte("new")); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if err := leader.CancelReadIndex([]byte("unknown")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestWirePreflightHostileCounts(t *testing.T) {
	d, _ := driver(t, 1, []uint64{1}, vfs.NewMem())
	l := raftlog.DefaultLimits()
	b := []byte{}
	for range l.MaxReadEntries + 1 {
		b = protowire.AppendTag(b, 7, protowire.BytesType)
		b = protowire.AppendBytes(b, nil)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := d.Step(Packet{From: 2, To: 1, Payload: b})
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrLimit) || after.TotalAlloc-before.TotalAlloc > 32<<10 {
		t.Fatal(err, after.TotalAlloc-before.TotalAlloc)
	}
	for _, b := range [][]byte{{0xff}, {0x0b}, {0x72, 0}, {0x08, 0xff, 0xff, 0x01}, {0x08, 1, 0x08, 1}} {
		if _, err := d.Step(Packet{From: 2, To: 1, Payload: b}); !errors.Is(err, ErrInvalid) {
			t.Fatal(b, err)
		}
	}
	cs := []byte{}
	for range 257 {
		cs = protowire.AppendTag(cs, 1, protowire.VarintType)
		cs = protowire.AppendVarint(cs, 1)
	}
	if err := preflightWire(cs, 4, l); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if err := preflightWire([]byte{0x0a, 1, 0x80}, 4, l); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestDriverErrorsAndFeedback(t *testing.T) {
	if _, err := Open(Config{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	d, _ := driver(t, 1, []uint64{1}, vfs.NewMem())
	var nilMachine *scalar
	if _, err := Open(Config{ID: 1, Store: d.store, Machine: nilMachine}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := d.Propose(make([]byte, d.config.Limits.MaxEntryBytes)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := d.ReadIndex(make([]byte, 1025)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err := d.ProposeConfChange((*pb.ConfChange)(nil)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := d.ProposeConfChange((*pb.ConfChangeV2)(nil)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := d.ProposeConfChange(&pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{NodeId: new(uint64(0))}}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, fn := range []func() (Output, error){func() (Output, error) { return d.ReportUnreachable(2) }, func() (Output, error) { return d.ReportSnapshot(2, false) }, func() (Output, error) { return d.ReportSnapshot(2, true) }, func() (Output, error) { return d.TransferLeader(1) }} {
		if _, err := fn(); err != nil {
			t.Fatal(err)
		}
	}
	packets(t, d.Campaign)
	m := d.machine.(*scalar)
	m.failApply = ErrInvalid
	if _, err := d.Propose(command(2)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := d.Campaign(); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if err := d.SaveCheckpoint(); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	for _, fn := range []func() (Output, error){func() (Output, error) { return d.Propose(command(1)) }, func() (Output, error) { return d.Step(Packet{}) }, func() (Output, error) { return d.ReadIndex(nil) }, func() (Output, error) { return d.ProposeConfChange(nil) }, func() (Output, error) { return d.ReportUnreachable(2) }, func() (Output, error) { return d.ReportSnapshot(2, false) }, func() (Output, error) { return d.TransferLeader(1) }} {
		if _, err := fn(); !errors.Is(err, ErrStopped) {
			t.Fatal(err)
		}
	}
	if err := d.CancelReadIndex(nil); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	for _, mode := range []string{"restore", "checkpoint", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			r, m := driver(t, 1, []uint64{1}, vfs.NewMem())
			switch mode {
			case "restore":
				m.failRestore = ErrInvalid
				if _, err := Open(Config{ID: 1, Store: r.store, Machine: m}); !errors.Is(err, ErrInvalid) {
					t.Fatal(err)
				}
			case "checkpoint":
				m.failCheckpoint = ErrInvalid
				if err := r.SaveCheckpoint(); !errors.Is(err, ErrInvalid) {
					t.Fatal(err)
				}
			case "oversized":
				m.oversized = true
				if err := r.SaveCheckpoint(); !errors.Is(err, ErrLimit) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRealProcessKillAndReopen(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL test")
	}
	if mode := os.Getenv("RHO_RAFT_CRASH_MODE"); mode != "" {
		dir := os.Getenv("RHO_RAFT_CRASH_DIR")
		s, err := raftlog.Open(raftlog.Config{Dir: dir, Create: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1}, nil); err != nil {
			t.Fatal(err)
		}
		d, err := Open(Config{ID: 1, Store: s, Machine: &scalar{}})
		if err != nil {
			t.Fatal(err)
		}
		packets(t, d.Campaign)
		packets(t, func() (Output, error) { return d.Propose(command(31)) })
		if mode == "checkpoint" || mode == "snapshot" {
			if err := d.SaveCheckpoint(); err != nil {
				t.Fatal(err)
			}
		}
		if mode == "snapshot" {
			if err := s.PublishSnapshot(); err != nil {
				t.Fatal(err)
			}
		}
		if err := killSelfForCrashTest(); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, mode := range []string{"apply", "checkpoint", "snapshot"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRealProcessKillAndReopen$")
			cmd.Env = append(os.Environ(), "RHO_RAFT_CRASH_MODE="+mode, "RHO_RAFT_CRASH_DIR="+dir)
			output, err := cmd.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok || ctx.Err() != nil {
				t.Fatalf("kill seam did not complete: %v context=%v %s", err, ctx.Err(), output)
			}
			status, isStatus := exit.Sys().(syscall.WaitStatus)
			if !isStatus || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("expected SIGKILL at %s seam: %v %s", mode, err, output)
			}
			s, err := raftlog.Open(raftlog.Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			m := &scalar{value: 999}
			d, err := Open(Config{ID: 1, Store: s, Machine: m})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			packets(t, d.Tick)
			if m.value != 31 {
				t.Fatalf("durable apply/replay %d", m.value)
			}
			if err := s.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestConcurrentSerializedDriver(t *testing.T) {
	d, _ := driver(t, 1, []uint64{1}, vfs.NewMem())
	packets(t, d.Campaign)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10 {
				if _, err := d.Tick(); err != nil {
					t.Error(err)
				}
				_ = d.Applied()
			}
		})
	}
	wg.Wait()
}

func TestPacketOwnershipAndInvalidIdentity(t *testing.T) {
	d, _ := driver(t, 1, []uint64{1, 2, 3}, vfs.NewMem())
	o, err := d.Campaign()
	if err != nil || len(o.Packets) == 0 {
		t.Fatal(o, err)
	}
	p := o.Packets[0]
	p.Payload = bytes.Clone(p.Payload)
	p.Payload[0] ^= 1
	if _, err := d.Step(p); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	m := &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1))}
	b, _ := proto.Marshal(m)
	if _, err := d.Step(Packet{From: 3, To: 1, Payload: b}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestSnapshotInstallThroughDriver(t *testing.T) {
	nodes := map[uint64]*Driver{}
	states := map[uint64]*scalar{}
	for id := uint64(1); id <= 3; id++ {
		d, m := driver(t, id, []uint64{1, 2, 3}, vfs.NewMem())
		nodes[id], states[id] = d, m
	}
	deliver(t, nodes, packets(t, nodes[1].Campaign))
	old := nodes[3]
	delete(nodes, 3)
	for i := range 5 {
		deliver(t, nodes, packets(t, func() (Output, error) { return nodes[1].Propose(command(uint64(i + 1))) }))
	}
	if err := nodes[1].SaveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	if err := nodes[1].store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	nodes[3] = old
	for range 6 {
		deliver(t, nodes, packets(t, nodes[1].Tick))
	}
	if states[3].value != 15 {
		t.Fatalf("snapshot failed or replay doubled: %d", states[3].value)
	}
	if idx, _, err := old.store.Checkpoint(); err != nil || idx <= 1 {
		t.Fatal(idx, err)
	}
	if err := old.store.Scrub(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaderOnlyReadAdmissionAndLimitAgreement(t *testing.T) {
	d, _ := driver(t, 1, []uint64{1, 2, 3}, vfs.NewMem())
	if _, err := d.ReadIndex([]byte("follower")); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if len(d.reads) != 0 {
		t.Fatal("unavailable request enqueued")
	}
	c := d.config
	c.Limits.MaxReadEntries++
	if _, err := Open(c); !errors.Is(err, ErrInvalid) {
		t.Fatal("mismatched store/driver limits accepted", err)
	}
	for _, kind := range []pb.MessageType{pb.MsgReadIndex, pb.MsgReadIndexResp} {
		m := &pb.Message{Type: kind.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1)), Entries: []*pb.Entry{{Data: []byte("remote")}}}
		b, _ := proto.Marshal(m)
		for range 100 {
			if _, err := d.Step(Packet{From: 2, To: 1, Payload: b}); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		}
	}
	if len(d.reads) != 0 {
		t.Fatal("remote read queue grew")
	}
	if cReadBytes(-1) != 0 {
		t.Fatal("negative read limit")
	}
}

func TestPendingReadStateAcrossRealReadyPages(t *testing.T) {
	// Controlled RawNode fixture: queue one read during election while replaying
	// a committed tail. This exercises the Ready/apply seam even though public
	// candidate admission is deliberately stricter (leader-only, current-term).
	l := raftlog.DefaultLimits()
	l.MaxEntryBytes = 256
	l.MaxReadBytes = 512
	l.MaxReadEntries = 2
	l.MaxReadyBytes = 16384
	s, err := raftlog.Open(raftlog.Config{Dir: "paged", FS: vfs.NewMem(), Create: true, Limits: l})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	var es []*pb.Entry
	for i := uint64(2); i <= 65; i++ {
		es = append(es, &pb.Entry{Index: new(i), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: command(1)})
	}
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(65))}, Entries: es}); err != nil {
		t.Fatal(err)
	}
	m := &scalar{}
	d, err := Open(Config{ID: 1, Store: s, Machine: m})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.mu.Lock()
	if err := d.raw.Campaign(); err != nil {
		d.mu.Unlock()
		t.Fatal(err)
	}
	// Persist/apply one election Ready page, then let its stable self-vote
	// establish leadership while the rest of the committed tail is unapplied.
	for steps := 0; d.raw.BasicStatus().RaftState != raft.StateLeader; steps++ {
		if steps > 3 {
			d.mu.Unlock()
			t.Fatal("election fixture stalled")
		}
		firstReady := d.raw.Ready()
		if err := s.Persist(firstReady); err != nil {
			d.mu.Unlock()
			t.Fatal(err)
		}
		if len(firstReady.CommittedEntries) != 2 {
			d.mu.Unlock()
			t.Fatal("fixture did not page", len(firstReady.CommittedEntries))
		}
		for _, e := range firstReady.CommittedEntries {
			if err := m.Apply(Entry{Index: e.GetIndex(), Term: e.GetTerm(), Data: e.GetData()}); err != nil {
				d.mu.Unlock()
				t.Fatal(err)
			}
			d.applied = e.GetIndex()
		}
		d.raw.Advance(firstReady)
	}
	// Election's initial SoftState is already accounted for in this fixture;
	// normal public events account for it during their drain before admission.
	d.lastLeader = 1
	d.reads["paged-token"] = &pendingRead{user: []byte("paged")}
	d.readBytes = 5
	d.raw.ReadIndex([]byte("paged-token"))
	out, err := d.drain()
	d.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Reads) != 1 || out.Reads[0].Index != 65 || out.Applied != 66 || m.value != 64 {
		t.Fatalf("paged application barrier: %+v scalar=%d", out, m.value)
	}
}
