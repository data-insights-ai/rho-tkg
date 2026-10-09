package raftlog

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	pb "go.etcd.io/raft/v3/raftpb"
)

type ledgerFS struct {
	vfs.FS
	written, syncs atomic.Uint64
}
type ledgerFile struct {
	vfs.File
	owner *ledgerFS
}

func (l *ledgerFS) Create(path string, c vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := l.FS.Create(path, c)
	if err != nil {
		return nil, err
	}
	return &ledgerFile{File: f, owner: l}, nil
}
func (l *ledgerFS) OpenDir(path string) (vfs.File, error) {
	f, err := l.FS.OpenDir(path)
	if err != nil {
		return nil, err
	}
	return &ledgerFile{File: f, owner: l}, nil
}
func (l *ledgerFS) OpenReadWrite(path string, c vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := l.FS.OpenReadWrite(path, c, opts...)
	if err != nil {
		return nil, err
	}
	return &ledgerFile{File: f, owner: l}, nil
}
func (l *ledgerFS) ReuseForWrite(old, new string, c vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := l.FS.ReuseForWrite(old, new, c)
	if err != nil {
		return nil, err
	}
	return &ledgerFile{File: f, owner: l}, nil
}
func (f *ledgerFile) Write(b []byte) (int, error) {
	n, err := f.File.Write(b)
	f.owner.written.Add(uint64(n))
	return n, err
}
func (f *ledgerFile) Sync() error     { f.owner.syncs.Add(1); return f.File.Sync() }
func (f *ledgerFile) SyncData() error { f.owner.syncs.Add(1); return f.File.SyncData() }

func TestPhysicalLogByteLedger(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	counter := &ledgerFS{FS: vfs.Default}
	s, err := Open(Config{Dir: dir, FS: counter, Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(err)
	}
	for base := uint64(2); base <= 1001; base += 100 {
		var es []*pb.Entry
		for i := base; i < base+100; i++ {
			es = append(es, ent(i, 2, "12345678"))
		}
		persist(t, s, 2, base+99, es...)
	}
	if s.meta.LogBytes != 91_000 || s.meta.LogCount != 1000 {
		t.Fatal(s.meta.LogBytes, s.meta.LogCount)
	}
	value, closer, err := s.db.Get(metaKey)
	if err != nil {
		t.Fatal(err)
	}
	metadataBytes := len(value)
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var diskBytes int64
	var files int
	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		diskBytes += info.Size()
		files++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("single-replica diagnostic: entries=1000 payload=8000B logical key+frame=91000B (83B overhead/entry, no resident per-entry map); mutable metadata=%dB; filesystem=%dB across %d files; writes=%dB sync calls=%d; configured cache=%dB memtable queued<=%dB; no graph/fact, RSS, vendor or throughput acceptance claim", metadataBytes, diskBytes, files, counter.written.Load(), counter.syncs.Load(), s.limits.CacheBytes, 2*s.limits.MemTableBytes)
}
