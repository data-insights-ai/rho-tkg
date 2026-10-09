package replica

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type applicationFixture struct {
	value                  uint64
	image                  []byte
	index                  uint64
	failStage, failRestore error
	killStage, killRestore bool
	installFault           *errorfs.Toggle
}

var errFixture = errors.New("application fixture injected failure")

func (m *applicationFixture) Restore(index uint64, image []byte) error {
	if m.failRestore != nil {
		return m.failRestore
	}
	if len(image) != 0 && len(image) != 8 {
		return ErrInvalid
	}
	m.value = 0
	if len(image) > 0 {
		m.value = binary.BigEndian.Uint64(image)
	}
	m.image = bytes.Clone(image)
	m.index = index
	if m.killRestore && m.value > 0 {
		if err := killSelfForCrashTest(); err != nil {
			return err
		}
	}
	return nil
}
func (m *applicationFixture) Stage(e Entry, b raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if m.failStage != nil {
		return raftlog.ApplicationBatch{}, m.failStage
	}
	if len(e.Data) != 0 && len(e.Data) != 8 {
		return raftlog.ApplicationBatch{}, ErrInvalid
	}
	result := raftlog.ApplicationBatch{BaseIndex: m.index, BaseImageHash: sha256.Sum256(m.image), Image: command(m.value)}
	if len(e.Data) != 0 {
		n := binary.BigEndian.Uint64(e.Data)
		result.Changes = append(command(m.value), command(m.value+n)...)
		result.Outcome = []byte("accepted")
		result.Image = command(m.value + n)
		w := raftlog.KV{Key: []byte("a"), Value: command(m.value + n)}
		if n == 0 {
			w.Value = nil
			w.Deleted = true
		}
		result.Writes = []raftlog.KV{w}
	}
	if len(result.Image)+len(result.Changes)+len(result.Outcome) > b.Bytes || len(result.Writes) > b.Writes {
		return raftlog.ApplicationBatch{}, ErrLimit
	}
	if m.installFault != nil && len(e.Data) > 0 {
		m.installFault.On()
	}
	if m.killStage && len(e.Data) > 0 {
		if err := killSelfForCrashTest(); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	return result, nil
}
func applicationDriver(t *testing.T, fs vfs.FS, p raftlog.ApplicationPolicy) (*Driver, *applicationFixture) {
	t.Helper()
	s, err := raftlog.Open(raftlog.Config{Dir: "app", FS: fs, Create: true, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Initialize([]uint64{p.LocalVoter}, nil); err != nil {
		t.Fatal(err)
	}
	m := &applicationFixture{}
	d, err := Open(Config{ID: p.LocalVoter, Store: s, ApplicationMachine: m})
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
func fixtureRead(t *testing.T, s *raftlog.Store, index, value uint64, deleted bool) {
	t.Helper()
	v, err := s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	row, found, err := v.Get(t.Context(), []byte("a"), 1024)
	if err != nil || !found || row.Deleted != deleted || !deleted && (len(row.Value) != 8 || binary.BigEndian.Uint64(row.Value) != value) {
		t.Fatal(row, found, err)
	}
}
func TestApplicationDriverAtomicHistoryAndRestart(t *testing.T) {
	fs := vfs.NewCrashableMem()
	p := raftlog.DefaultApplicationPolicy(1)
	p.ReclaimEntries = 2
	d, m := applicationDriver(t, fs, p)
	packets(t, d.Campaign)
	packets(t, func() (Output, error) { return d.Propose(command(7)) })
	old := d.Applied()
	packets(t, func() (Output, error) { return d.Propose(command(11)) })
	now := d.Applied()
	if m.value != 18 {
		t.Fatal(m.value)
	}
	fixtureRead(t, d.store, old, 7, false)
	fixtureRead(t, d.store, now, 18, false)
	group, err := d.store.ApplicationRecord(t.Context(), now, false, 128)
	if err != nil || !bytes.Equal(group, append(command(7), command(18)...)) {
		t.Fatal(group, err)
	}
	outcome, err := d.store.ApplicationRecord(t.Context(), now, true, 128)
	if err != nil || string(outcome) != "accepted" {
		t.Fatal(outcome, err)
	}
	packets(t, func() (Output, error) { return d.Propose(command(0)) })
	fixtureRead(t, d.store, d.Applied(), 0, true)
	fixtureRead(t, d.store, old, 7, false)
	if err := d.SaveCheckpoint(); err != nil {
		t.Fatal(err)
	}
	reply, err := d.ReadIndex([]byte("after"))
	if err != nil || len(reply.Reads) != 1 || reply.Reads[0].Index > d.Applied() {
		t.Fatal(reply, err)
	}
	crash := fs.CrashClone(vfs.CrashCloneCfg{})
	s, err := raftlog.Open(raftlog.Config{Dir: "app", FS: crash, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	recovered := &applicationFixture{value: 999}
	r, err := Open(Config{ID: 1, Store: s, ApplicationMachine: recovered})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	packets(t, r.Tick)
	if recovered.value != 18 || recovered.index != r.Applied() {
		t.Fatal(recovered.value, recovered.index, r.Applied())
	}
	fixtureRead(t, s, old, 7, false)
	fixtureRead(t, s, now, 18, false)
	first, err := s.FirstIndex()
	if err != nil || first < old {
		t.Fatal("automatic reclamation missing", first, err)
	}
	if err := s.Scrub(); err != nil {
		t.Fatal(err)
	}
}

func TestApplicationDriverRejectsBeforePersistence(t *testing.T) {
	fs := vfs.NewCrashableMem()
	p := raftlog.DefaultApplicationPolicy(1)
	d, m := applicationDriver(t, fs, p)
	packets(t, d.Campaign)
	packets(t, func() (Output, error) { return d.Propose(command(7)) })
	beforeIndex, beforeImage, _ := d.store.Checkpoint()
	beforeHard, beforeConf, _ := d.store.InitialState()
	beforeFirst, _ := d.store.FirstIndex()
	beforeLast, _ := d.store.LastIndex()
	snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(100)), Term: new(uint64(100)), ConfState: &pb.ConfState{Voters: []uint64{1}}}, Data: command(999)}
	msg := &pb.Message{From: new(uint64(2)), To: new(uint64(1)), Type: pb.MsgSnap.Enum(), Term: new(uint64(100)), Snapshot: snap}
	wire, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Step(Packet{From: 2, To: 1, Payload: wire, Snapshot: true}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, fn := range []func() (Output, error){func() (Output, error) {
		return d.ProposeConfChange(&pb.ConfChange{Type: pb.ConfChangeAddNode.Enum(), NodeId: new(uint64(2))})
	}, func() (Output, error) { return d.TransferLeader(2) }, func() (Output, error) { return d.ReportSnapshot(2, true) }, func() (Output, error) { return d.ReportUnreachable(2) }} {
		if _, err := fn(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, rd := range []raft.Ready{{Snapshot: snap}, {Entries: []*pb.Entry{{Type: pb.EntryConfChange.Enum()}}}, {CommittedEntries: []*pb.Entry{{Type: pb.EntryConfChangeV2.Enum()}}}, {Messages: []*pb.Message{msg}}} {
		if err := d.checkApplicationReady(rd); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	// Bypass the network entry point deliberately. A real incoming snapshot Ready
	// must be refused before the store sees new hard state or replaces its image.
	d.mu.Lock()
	err = d.raw.Step(msg)
	if err == nil {
		_, err = d.drain()
	}
	d.mu.Unlock()
	if !errors.Is(err, ErrInvalid) {
		t.Fatal("raw snapshot guard", err)
	}
	index, image, _ := d.store.Checkpoint()
	hard, conf, _ := d.store.InitialState()
	first, _ := d.store.FirstIndex()
	last, _ := d.store.LastIndex()
	if index != beforeIndex || !bytes.Equal(image, beforeImage) || !proto.Equal(hard, beforeHard) || !proto.Equal(conf, beforeConf) || first != beforeFirst || last != beforeLast || m.value != 7 {
		t.Fatal("snapshot guard changed durable state", index, hard, first, last)
	}
	s, err := raftlog.Open(raftlog.Config{Dir: "app", FS: fs.CrashClone(vfs.CrashCloneCfg{}), Application: p})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	index, image, _ = s.Checkpoint()
	hard, conf, _ = s.InitialState()
	if index != beforeIndex || !bytes.Equal(image, beforeImage) || !proto.Equal(hard, beforeHard) || !proto.Equal(conf, beforeConf) {
		t.Fatal("reopened imported snapshot")
	}
	fixtureRead(t, s, beforeIndex, 7, false)
}

func TestApplicationDriverModeAndNilMachines(t *testing.T) {
	d, _ := applicationDriver(t, vfs.NewMem(), raftlog.DefaultApplicationPolicy(1))
	var nilApp *applicationFixture
	for _, c := range []Config{{ID: 1, Store: d.store}, {ID: 1, Store: d.store, ApplicationMachine: nilApp}, {ID: 2, Store: d.store, ApplicationMachine: &applicationFixture{}}, {ID: 1, Store: d.store, Machine: &scalar{}}, {ID: 1, Store: d.store, Machine: &scalar{}, ApplicationMachine: &applicationFixture{}}} {
		if _, err := Open(c); !errors.Is(err, ErrInvalid) {
			t.Fatal(c.ID, err)
		}
	}
	scalarDriver, _ := driver(t, 1, []uint64{1}, vfs.NewMem())
	if _, err := Open(Config{ID: 1, Store: scalarDriver.store, ApplicationMachine: &applicationFixture{}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestApplicationDriverCommittedFailuresRecover(t *testing.T) {
	for _, mode := range []string{"stage", "restore"} {
		t.Run(mode, func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			p := raftlog.DefaultApplicationPolicy(1)
			d, m := applicationDriver(t, fs, p)
			packets(t, d.Campaign)
			if mode == "stage" {
				m.failStage = errFixture
			} else {
				m.failRestore = errFixture
			}
			if _, err := d.Propose(command(13)); !errors.Is(err, errFixture) {
				t.Fatal(err)
			}
			if _, err := d.ReadIndex(nil); !errors.Is(err, ErrStopped) {
				t.Fatal(err)
			}
			if err := d.SaveCheckpoint(); !errors.Is(err, ErrStopped) {
				t.Fatal(err)
			}
			s, err := raftlog.Open(raftlog.Config{Dir: "app", FS: fs.CrashClone(vfs.CrashCloneCfg{}), Application: p})
			if err != nil {
				t.Fatal(err)
			}
			state := &applicationFixture{}
			r, err := Open(Config{ID: 1, Store: s, ApplicationMachine: state})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			packets(t, r.Tick)
			if state.value != 13 {
				t.Fatal("lost or duplicate application", state.value)
			}
			fixtureRead(t, s, r.Applied(), 13, false)
		})
	}
}

func TestApplicationDriverRealProcessKill(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL")
	}
	p := raftlog.DefaultApplicationPolicy(1)
	if mode := os.Getenv("RHO_APP_CRASH_MODE"); mode != "" {
		dir := os.Getenv("RHO_APP_CRASH_DIR")
		s, err := raftlog.Open(raftlog.Config{Dir: dir, Create: true, Application: p})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1}, nil); err != nil {
			t.Fatal(err)
		}
		m := &applicationFixture{}
		d, err := Open(Config{ID: 1, Store: s, ApplicationMachine: m})
		if err != nil {
			t.Fatal(err)
		}
		packets(t, d.Campaign)
		m.killStage = mode == "stage"
		m.killRestore = mode == "restore"
		packets(t, func() (Output, error) { return d.Propose(command(23)) })
		if mode == "reclaim" {
			if err := s.PublishSnapshot(); err != nil {
				t.Fatal(err)
			}
			if err := killSelfForCrashTest(); err != nil {
				t.Fatal(err)
			}
		}
		t.Fatal("kill did not execute")
	}
	for _, mode := range []string{"stage", "restore", "reclaim"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestApplicationDriverRealProcessKill$")
			cmd.Env = append(os.Environ(), "RHO_APP_CRASH_MODE="+mode, "RHO_APP_CRASH_DIR="+dir)
			output, err := cmd.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok || ctx.Err() != nil {
				t.Fatalf("kill seam did not complete: %v context=%v %s", err, ctx.Err(), output)
			}
			status, isStatus := exit.Sys().(syscall.WaitStatus)
			if !isStatus || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("expected SIGKILL at %s seam: %v %s", mode, err, output)
			}
			s, err := raftlog.Open(raftlog.Config{Dir: dir, Application: p})
			if err != nil {
				t.Fatal(err)
			}
			m := &applicationFixture{}
			d, err := Open(Config{ID: 1, Store: s, ApplicationMachine: m})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			packets(t, d.Tick)
			if m.value != 23 {
				t.Fatal("durable root/replay", m.value)
			}
			fixtureRead(t, s, d.Applied(), 23, false)
			if err := s.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestApplicationInstallFatalIOStopsBeforeOutput(t *testing.T) {
	p := raftlog.DefaultApplicationPolicy(1)
	if fault := os.Getenv("RHO_APP_INSTALL_FAULT"); fault != "" {
		dir := os.Getenv("RHO_APP_CRASH_DIR")
		toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
			if fault == "write" && op.Kind == errorfs.OpFileWrite || fault == "sync" && (op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData) {
				return errorfs.ErrInjected
			}
			return nil
		})}
		s, err := raftlog.Open(raftlog.Config{Dir: dir, FS: errorfs.Wrap(vfs.Default, toggle), Create: true, Application: p})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1}, nil); err != nil {
			t.Fatal(err)
		}
		m := &applicationFixture{}
		d, err := Open(Config{ID: 1, Store: s, ApplicationMachine: m})
		if err != nil {
			t.Fatal(err)
		}
		packets(t, d.Campaign)
		packets(t, func() (Output, error) { return d.Propose(command(17)) })
		// Stage enables the fault only AFTER Persist made this command durable,
		// so the failing sync/write belongs to atomic application installation.
		m.installFault = toggle
		out, err := d.Propose(command(31))
		_, _ = os.Stdout.WriteString("APPLICATION_INSTALL_RETURNED_OUTPUT\n")
		t.Fatalf("fatal install returned: %+v %v", out, err)
	}
	for _, fault := range []string{"write", "sync"} {
		t.Run(fault, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestApplicationInstallFatalIOStopsBeforeOutput$")
			cmd.Env = append(os.Environ(), "RHO_APP_INSTALL_FAULT="+fault, "RHO_APP_CRASH_DIR="+dir)
			output, err := cmd.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok || exit.ExitCode() != 1 || !bytes.Contains(output, []byte("fatal commit error")) || bytes.Contains(output, []byte("APPLICATION_INSTALL_RETURNED_OUTPUT")) {
				t.Fatalf("install fail-stop: %v %s", err, output)
			}
			s, err := raftlog.Open(raftlog.Config{Dir: dir, Application: p})
			if err != nil {
				t.Fatal(err)
			}
			m := &applicationFixture{}
			d, err := Open(Config{ID: 1, Store: s, ApplicationMachine: m})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			packets(t, d.Tick)
			// The command was synced before the injected installation failure, so
			// either surviving complete checkpoint or redo must produce 17+31.
			if m.value != 48 {
				t.Fatal("lost committed command or duplicate redo", m.value)
			}
			fixtureRead(t, s, d.Applied(), 48, false)
			if err := s.ScrubApplication(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDriverDualInterfaceTypedNilNormalization(t *testing.T) {
	for _, appMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "scalar", true: "application"}[appMode], func(t *testing.T) {
			fs := vfs.NewCrashableMem()
			p := raftlog.ApplicationPolicy{}
			if appMode {
				p = raftlog.DefaultApplicationPolicy(1)
			}
			s, err := raftlog.Open(raftlog.Config{Dir: "typednil", FS: fs, Create: true, Application: p})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Initialize([]uint64{1}, nil); err != nil {
				t.Fatal(err)
			}
			config := func(store *raftlog.Store) (Config, *scalar, *applicationFixture) {
				var nilScalar *scalar
				var nilApp *applicationFixture
				c := Config{ID: 1, Store: store, Machine: nilScalar, ApplicationMachine: nilApp}
				scalarState := &scalar{}
				appState := &applicationFixture{}
				if appMode {
					c.ApplicationMachine = appState
				} else {
					c.Machine = scalarState
				}
				return c, scalarState, appState
			}
			c, scalarState, appState := config(s)
			d, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if appMode && d.machine != nil || !appMode && d.applicationMachine != nil {
				t.Fatal("unused typed-nil provider retained")
			}
			packets(t, d.Campaign)
			packets(t, func() (Output, error) { return d.Propose(command(9)) })
			if err := d.SaveCheckpoint(); err != nil {
				t.Fatal(err)
			}
			if appMode && appState.value != 9 || !appMode && scalarState.value != 9 {
				t.Fatal("wrong machine path")
			}
			recoveredStore, err := raftlog.Open(raftlog.Config{Dir: "typednil", FS: fs.CrashClone(vfs.CrashCloneCfg{}), Application: p})
			if err != nil {
				t.Fatal(err)
			}
			c, scalarState, appState = config(recoveredStore)
			r, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			packets(t, r.Tick)
			if appMode && appState.value != 9 || !appMode && scalarState.value != 9 {
				t.Fatal("wrong recovered machine path")
			}
		})
	}
}
