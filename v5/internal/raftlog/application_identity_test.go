package raftlog

import (
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestApplicationIdentityReportsOwnedDurableConfiguration(t *testing.T) {
	var nilStore *Store
	if got := nilStore.ApplicationIdentity(); got != (ApplicationIdentity{}) {
		t.Fatal(got)
	}
	for _, app := range []ApplicationPolicy{{}, DefaultApplicationPolicy(1)} {
		s, err := Open(Config{Dir: "unbound", FS: vfs.NewMem(), Create: true, Application: app})
		if err != nil {
			t.Fatal(err)
		}
		if got := s.ApplicationIdentity(); got != (ApplicationIdentity{}) {
			t.Fatal(got)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if got := s.ApplicationIdentity(); got != (ApplicationIdentity{}) {
			t.Fatal(got)
		}
	}
	fs := vfs.NewMem()
	p, transfer := transferConfig(1)
	s, err := Open(Config{Dir: "bound", FS: fs, Create: true, Application: p, Transfer: transfer})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.ApplicationIdentity(); got != transfer.Identity {
		t.Fatal(got)
	}
	got := s.ApplicationIdentity()
	got.Graph[0]++
	got.Group[0]++
	got.Partition++
	if got == transfer.Identity || s.ApplicationIdentity() != transfer.Identity {
		t.Fatal("identity retained aliases")
	}
	if err := s.Initialize([]uint64{1}, []byte("initial")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := s.ApplicationIdentity(); got != transfer.Identity {
		t.Fatal("closed configuration changed", got)
	}
	reopened, err := Open(Config{Dir: "bound", FS: fs, Application: p, Transfer: transfer})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.ApplicationIdentity(); got != transfer.Identity {
		t.Fatal("recovered configuration changed", got)
	}
}

func TestApplicationIdentitySerializesWithPublicationAndClose(t *testing.T) {
	s := transferStore(t, "identity-race", vfs.NewMem(), 1)
	applyApplication(t, s, "identity-next")
	want := s.ApplicationIdentity()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				if got := s.ApplicationIdentity(); got != want {
					t.Error(got)
				}
			}
		})
	}
	// Checkpoint publication copies the metadata while readers report the
	// immutable identity. Close also owns the same metadata mutex.
	if err := s.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
