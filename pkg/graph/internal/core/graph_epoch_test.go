package core

import (
	"errors"
	"strings"
	"testing"
)

// BACKLOG 14g: graphEpochLocked's corrupt-lineage and zero-avoidance
// branches had no direct test.

// TestGraphEpochLocked_CorruptLineageIDRejected seeds graphEpochMeta with a
// value that is neither empty (unminted) nor 8 bytes (a valid minted epoch)
// and asserts graphEpochLocked fails closed rather than silently
// misinterpreting the bytes.
func TestGraphEpochLocked_CorruptLineageIDRejected(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if g.metaKV == nil {
		t.Fatal("test store has no MetaKV — test setup broken")
	}
	if err := g.metaKV.MetaSet(graphEpochMeta, []byte{1, 2, 3}); err != nil {
		t.Fatalf("seed corrupt lineage id: %v", err)
	}

	g.mu.Lock()
	_, err := g.graphEpochLocked()
	g.mu.Unlock()
	if err == nil {
		t.Fatal("graphEpochLocked with a 3-byte corrupt lineage id = nil error, want a failure")
	}
	if !strings.Contains(err.Error(), "corrupt lineage id") {
		t.Fatalf("graphEpochLocked error = %v, want it to mention 'corrupt lineage id'", err)
	}
}

// TestGraphEpochLocked_NeverMintsZero forces the random source to return an
// all-zero buffer and asserts graphEpochLocked mints a NON-zero epoch
// anyway (0 is the reserved zero-cursor sentinel — see the doc comment).
func TestGraphEpochLocked_NeverMintsZero(t *testing.T) {
	// Not t.Parallel(): epochRandRead is a package-level global this test
	// reassigns; Go's default sequential execution (no other test runs
	// concurrently with a non-parallel test) is what keeps that safe.
	g := newTestGraph(t)
	if g.metaKV == nil {
		t.Fatal("test store has no MetaKV — test setup broken")
	}

	orig := epochRandRead
	epochRandRead = func(b []byte) (int, error) {
		for i := range b {
			b[i] = 0
		}
		return len(b), nil
	}
	defer func() { epochRandRead = orig }()

	g.mu.Lock()
	epoch, err := g.graphEpochLocked()
	g.mu.Unlock()
	if err != nil {
		t.Fatalf("graphEpochLocked with an all-zero random source: %v", err)
	}
	if epoch == 0 {
		t.Fatal("graphEpochLocked minted the reserved zero-cursor sentinel (0) from an all-zero random buffer")
	}
}

// TestGraphEpochLocked_RandReadFailurePropagates covers the rand.Read error
// branch alongside the two above, for full coverage of graphEpochLocked's
// minting path.
func TestGraphEpochLocked_RandReadFailurePropagates(t *testing.T) {
	// Not t.Parallel(): see TestGraphEpochLocked_NeverMintsZero.
	g := newTestGraph(t)
	if g.metaKV == nil {
		t.Fatal("test store has no MetaKV — test setup broken")
	}

	boom := errors.New("rand source boom")
	orig := epochRandRead
	epochRandRead = func(b []byte) (int, error) { return 0, boom }
	defer func() { epochRandRead = orig }()

	g.mu.Lock()
	_, err := g.graphEpochLocked()
	g.mu.Unlock()
	if !errors.Is(err, boom) {
		t.Fatalf("graphEpochLocked with a failing random source = %v, want errors.Is boom", err)
	}
}
