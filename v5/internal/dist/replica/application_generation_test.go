package replica

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type generationFixture struct {
	applicationFixture
	invalidGeneration string
}

func (m *generationFixture) Stage(e Entry, b raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	result, err := m.applicationFixture.Stage(e, b)
	if err != nil {
		return result, err
	}
	switch m.invalidGeneration {
	case "missing":
		result.BaseGeneration = 0
	case "wrong":
		result.BaseGeneration = e.Generation + 1
	}
	return result, nil
}
func TestApplicationGenerationDriverKeepsSingletonAndRejectsUnboundStages(t *testing.T) {
	for _, mode := range []string{"correct", "missing", "wrong"} {
		t.Run(mode, func(t *testing.T) {
			mem := vfs.NewCrashableMem()
			p := raftlog.DefaultApplicationPolicy(1)
			tc := raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte{1}, Partition: 1, Group: [16]byte{1}}, Contract: raftlog.ApplicationContractForPolicy(p), Limits: raftlog.DefaultApplicationTransferLimits()}
			config := raftlog.Config{Dir: "gen-driver", FS: mem, Create: true, Application: p, Transfer: tc, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}}
			s, err := raftlog.Open(config)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Initialize([]uint64{1}, nil); err != nil {
				t.Fatal(err)
			}
			machine := &generationFixture{invalidGeneration: mode}
			d, err := Open(Config{ID: 1, Store: s, ApplicationMachine: machine})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			beforeIndex, beforeImage, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			out, err := d.Campaign()
			if mode == "correct" {
				if err != nil {
					t.Fatal(err)
				}
				packets(t, func() (Output, error) { return d.Propose(command(7)) })
				fixtureRead(t, s, d.Applied(), 7, false)
				if _, err := d.Step(Packet{}); !errors.Is(err, ErrInvalid) {
					t.Fatal("singleton network fence relaxed", err)
				}
				if _, err := d.ReportSnapshot(2, true); !errors.Is(err, ErrInvalid) {
					t.Fatal("singleton feedback fence relaxed", err)
				}
				return
			}
			if !errors.Is(err, raftlog.ErrInvalid) {
				t.Fatal("unbound Stage admitted", err)
			}
			if len(out.Packets) != 0 || len(out.Reads) != 0 || out.Applied != 0 {
				t.Fatal("failed committed Stage returned public output", out)
			}
			afterIndex, afterImage, err := s.Checkpoint()
			if err != nil || afterIndex != beforeIndex || !bytes.Equal(afterImage, beforeImage) {
				t.Fatal("unbound Stage changed checkpoint", afterIndex, err)
			}
			if _, err := d.Tick(); !errors.Is(err, ErrStopped) || !errors.Is(err, raftlog.ErrInvalid) {
				t.Fatal("committed failure did not stop driver", err)
			}
			hard, _, err := s.InitialState()
			if err != nil || hard.GetCommit() <= beforeIndex {
				t.Fatal("failure incorrectly treated committed entry as aborted", hard, err)
			}
			config.Create = false
			config.FS = mem.CrashClone(vfs.CrashCloneCfg{})
			recovered, err := raftlog.Open(config)
			if err != nil {
				t.Fatal(err)
			}
			state := &generationFixture{}
			r, err := Open(Config{ID: 1, Store: recovered, ApplicationMachine: state})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			packets(t, r.Tick)
			if state.index != hard.GetCommit() || r.Applied() != hard.GetCommit() {
				t.Fatal("committed no-op not redone with generation binding", state.index, r.Applied())
			}
		})
	}
}
