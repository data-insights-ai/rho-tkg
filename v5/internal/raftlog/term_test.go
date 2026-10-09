package raftlog

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestTermVerifiedLookupParityAndFailures(t *testing.T) {
	s := initialized(t, vfs.NewMem())
	persist(t, s, 3, 3, ent(2, 2, "small"), ent(3, 3, string(bytes.Repeat([]byte("large"), 1<<15))))
	entries, err := s.Entries(2, 4, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		term, err := s.Term(e.GetIndex())
		if err != nil || term != e.GetTerm() {
			t.Fatal("row/term parity", term, e, err)
		}
	}
	if term, err := s.Term(1); err != nil || term != 1 {
		t.Fatal("boundary", term, err)
	}
	if _, err := s.Term(0); !errors.Is(err, raft.ErrCompacted) {
		t.Fatal(err)
	}
	if _, err := s.Term(4); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatal(err)
	}
	for _, mode := range []string{"missing", "payload"} {
		t.Run(mode, func(t *testing.T) {
			r := initialized(t, vfs.NewMem())
			persist(t, r, 2, 2, ent(2, 2, "payload"))
			if mode == "missing" {
				if err := r.db.Delete(entryKey(2), pebble.Sync); err != nil {
					t.Fatal(err)
				}
			} else {
				value, closer, err := r.db.Get(entryKey(2))
				if err != nil {
					t.Fatal(err)
				}
				value = bytes.Clone(value)
				if err := closer.Close(); err != nil {
					t.Fatal(err)
				}
				value[len(value)-1] ^= 1
				if err := r.db.Set(entryKey(2), value, pebble.Sync); err != nil {
					t.Fatal(err)
				}
			}
			if term, err := r.Term(2); term != 0 || !errors.Is(err, ErrCorrupt) {
				t.Fatal(term, err)
			}
			if _, err := r.Term(1); !errors.Is(err, ErrPoisoned) || !errors.Is(err, ErrCorrupt) {
				t.Fatal("poison layer lost sentinel", err)
			}
		})
	}
	l := DefaultLimits()
	l.CacheBytes = 1
	toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
		if op.Kind == errorfs.OpFileReadAt {
			return errorfs.ErrInjected
		}
		return nil
	})}
	r := openTest(t, errorfs.Wrap(vfs.NewMem(), toggle), l)
	if err := r.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	persist(t, r, 2, 2, ent(2, 2, "disk"))
	if err := r.db.Flush(); err != nil {
		t.Fatal(err)
	}
	toggle.On()
	if term, err := r.Term(2); term != 0 || !errors.Is(err, errorfs.ErrInjected) || errors.Is(err, ErrCorrupt) {
		t.Fatal("I/O classification", term, err)
	}
	if _, err := r.Term(2); !errors.Is(err, ErrPoisoned) || !errors.Is(err, errorfs.ErrInjected) {
		t.Fatal("I/O poison sentinel", err)
	}
	toggle.Off()
}

// Warm memtable lookup isolates adapter allocations from cold block loading.
// Hash validation still visits payload bytes: this promises no payload copy,
// not constant-time verification or zero cold-storage allocation.
func BenchmarkTermPayloadAllocation(b *testing.B) {
	l := DefaultLimits()
	l.MemTableBytes = 4 << 20
	s, err := Open(Config{Dir: "bench", FS: vfs.NewMem(), Create: true, Limits: l})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Error(err)
		}
	})
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		b.Fatal(err)
	}
	for _, c := range []struct {
		index uint64
		size  int
	}{{2, 8}, {3, 256 << 10}} {
		e := ent(c.index, 2, string(make([]byte, c.size)))
		if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(c.index)}, Entries: []*pb.Entry{e}}); err != nil {
			b.Fatal(err)
		}
	}
	for _, c := range []struct {
		name  string
		index uint64
		size  int
	}{{"8B", 2, 8}, {"256KiB", 3, 256 << 10}} {
		b.Run(c.name, func(b *testing.B) {
			if _, err := s.Term(c.index); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(c.size))
			for b.Loop() {
				term, err := s.Term(c.index)
				if err != nil || term != 2 {
					b.Fatal(term, err)
				}
			}
		})
	}
}
