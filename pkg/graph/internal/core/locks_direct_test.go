package core

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/memory"
)

func TestCoreLockHelpersDirectBranches(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	runCalled := false
	_, err = g.runUnderRLock(func() {
		runCalled = true
	})
	if err != nil {
		t.Fatalf("runUnderRLock open: %v", err)
	}
	if !runCalled {
		t.Fatal("runUnderRLock did not call callback while graph was open")
	}
	readErr := errors.New("read callback failed")
	if err := g.readUnderRLock(func() error { return readErr }); !errors.Is(err, readErr) {
		t.Fatalf("readUnderRLock callback error = %v, want %v", err, readErr)
	}

	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	runCalled = false
	_, err = g.runUnderRLock(func() {
		runCalled = true
	})
	if !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("runUnderRLock closed = %v, want ErrGraphClosed", err)
	}
	if runCalled {
		t.Fatal("runUnderRLock called callback after graph close")
	}
	readCalled := false
	if err := g.readUnderRLock(func() error {
		readCalled = true
		return nil
	}); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("readUnderRLock closed = %v, want ErrGraphClosed", err)
	}
	if readCalled {
		t.Fatal("readUnderRLock called callback after graph close")
	}
}
