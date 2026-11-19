package badger

import (
	"slices"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BuildFormerLabelTokens answers which labels a store holds former members
// of. A tiered store asks it when it seals a closed shard off every caller's
// path, with no cut's allowance behind it, so what it holds must not grow
// with the shard: it walks the history rows into a set of label tokens and
// keeps no member. These tests count, at the sidecar's record, the members a
// call keeps (none), with the full sidecar's answer over the same rows as the
// exact control and the decode count as proof that the walk ran. They use
// process-wide counters and so do not run in parallel.

const ftA, ftB, ftC, ftD, ftE, ftF uint16 = 21, 22, 23, 200, 4097, 65535

// writeFormerTokenRows writes nodes of four kinds, in turn:
//
//	0: current {A}, history {B}       a former member of B
//	1: current {A}, history {A}       none: the current row carries A
//	2: current {A,C}, history {C,D}   a former member of D only
//	3: no current row, history {E}    a former member of E
//
// and returns the former-label set and the number of former members.
func writeFormerTokenRows(t *testing.T, bs *Store, nodes int) (want []uint16, members int) {
	t.Helper()
	for i := 0; i < nodes; i++ {
		id := int64(1000 + i)
		var cur, hist []uint16
		switch i % 4 {
		case 0:
			cur, hist = []uint16{ftA}, []uint16{ftB}
		case 1:
			cur, hist = []uint16{ftA}, []uint16{ftA}
		case 2:
			cur, hist = []uint16{ftA, ftC}, []uint16{ftC, ftD}
		case 3:
			hist = []uint16{ftE}
		}
		if i%4 != 1 {
			members++
		}
		if cur != nil {
			n := bgNode(id, cur, 200)
			n.SetVersion(1)
			if err := bs.PutNode(n); err != nil {
				t.Fatalf("PutNode %d: %v", id, err)
			}
		}
		v0 := bgNode(id, hist, 100)
		v0.SetVersion(0)
		if err := bs.PutNodeVersion(types.NodeID(id), 0, v0); err != nil {
			t.Fatalf("PutNodeVersion %d: %v", id, err)
		}
	}
	return []uint16{ftB, ftD, ftE}, members
}

// sidecarFormerTokens is the control: the full sidecar's former-label set.
func sidecarFormerTokens(t *testing.T, bs *Store) []uint16 {
	t.Helper()
	if err := bs.ensureLabelTxMembersBuilt(); err != nil {
		t.Fatalf("build the sidecar: %v", err)
	}
	tokens, ok := bs.FormerLabelTokens()
	if !ok {
		t.Fatal("FormerLabelTokens after the sidecar's build: not known")
	}
	slices.Sort(tokens)
	return tokens
}

func TestBuildFormerLabelTokens_KeepsNoMember(t *testing.T) {
	const nodes = 400
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	want, members := writeFormerTokenRows(t, bs, nodes)
	if err := bs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	recorded, stopRecorded := CountLabelTxMembersRecordedForTest()
	defer stopRecorded()
	decoded, stopDecoded := CountMembershipDecodesForTest()
	defer stopDecoded()

	// The control: the sidecar over the same rows keeps every former member
	// and names the same labels.
	ctl, err := New(Config{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("open the control: %v", err)
	}
	if got := sidecarFormerTokens(t, ctl); !slices.Equal(got, want) {
		t.Fatalf("control: the sidecar's former labels %v, want %v", got, want)
	}
	if got := recorded(); got != int64(members) || members == 0 {
		t.Fatalf("control: the sidecar took in %d former members, want %d (not none)", got, members)
	}
	if err := ctl.Close(); err != nil {
		t.Fatalf("close the control: %v", err)
	}

	for _, transient := range []bool{false, true} {
		ro, err := New(Config{Dir: dir, ReadOnly: true, TransientRead: transient})
		if err != nil {
			t.Fatalf("open read-only (transient %v): %v", transient, err)
		}
		recorded0, decoded0 := recorded(), decoded()
		tokens, err := ro.BuildFormerLabelTokens()
		if err != nil {
			t.Fatalf("BuildFormerLabelTokens (transient %v): %v", transient, err)
		}
		if !slices.Equal(tokens, want) {
			t.Errorf("transient %v: former labels %v, want %v (ascending)", transient, tokens, want)
		}
		if got := decoded() - decoded0; got < nodes {
			t.Errorf("transient %v: the walk decoded %d rows, want at least the %d history rows", transient, got, nodes)
		}
		if got := recorded() - recorded0; got != 0 {
			t.Errorf("transient %v: the call kept %d former members in RAM, want 0 (the control keeps %d): its heap grows with the shard", transient, got, members)
		}
		if ro.labelTxMembers != nil || ro.labelTxMembersBuilt.Load() {
			t.Errorf("transient %v: the call left a sidecar on the handle (built %v)", transient, ro.labelTxMembersBuilt.Load())
		}
		if _, ok := ro.FormerLabelTokens(); ok {
			t.Errorf("transient %v: FormerLabelTokens answers after the walk; it reads the sidecar only", transient)
		}

		// A pinned scan after the walk builds its sidecar as before, and the
		// next call answers from it without another walk.
		recorded0 = recorded()
		if got := len(labelMembers(t, ro, ftB)); got != nodes/4 {
			t.Errorf("transient %v: a pinned scan after the walk finds %d former members of B, want %d", transient, got, nodes/4)
		}
		if got := recorded() - recorded0; got != int64(members) {
			t.Errorf("transient %v: the pinned scan's sidecar took in %d former members, want %d", transient, got, members)
		}
		decoded0 = decoded()
		tokens, err = ro.BuildFormerLabelTokens()
		slices.Sort(tokens)
		if err != nil || !slices.Equal(tokens, want) {
			t.Errorf("transient %v: with a sidecar: former labels %v, %v; want %v", transient, tokens, err, want)
		}
		if got := decoded() - decoded0; got != 0 {
			t.Errorf("transient %v: with a sidecar the call decoded %d rows, want 0", transient, got)
		}
		if err := ro.Close(); err != nil {
			t.Fatalf("close (transient %v): %v", transient, err)
		}
	}
}

// The walk reads the write buffer too: history rows and current rows not yet
// flushed count as the flushed ones do.
func TestBuildFormerLabelTokens_ReadsBufferedRows(t *testing.T) {
	const nodes = 40
	bs, err := New(Config{Dir: t.TempDir(), FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = bs.Close() }()
	writeFormerTokenRows(t, bs, nodes)
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Buffered: a history row under a label of its own, and a current row
	// that stops carrying C over a flushed history row that carries it.
	late := bgNode(5000, []uint16{ftF}, 300)
	late.SetVersion(0)
	if err := bs.PutNodeVersion(types.NodeID(5000), 0, late); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	moved := bgNode(1002, []uint16{ftA}, 400)
	moved.SetVersion(2)
	if err := bs.RemoveNodeLabelToken(types.NodeID(1002), ftC, moved); err != nil {
		t.Fatalf("RemoveNodeLabelToken: %v", err)
	}
	want := []uint16{ftB, ftC, ftD, ftE, ftF}

	recorded, stop := CountLabelTxMembersRecordedForTest()
	defer stop()
	tokens, err := bs.BuildFormerLabelTokens()
	if err != nil {
		t.Fatalf("BuildFormerLabelTokens: %v", err)
	}
	if !slices.Equal(tokens, want) {
		t.Errorf("former labels %v, want %v", tokens, want)
	}
	if got := recorded(); got != 0 || bs.labelTxMembers != nil {
		t.Errorf("the call kept %d former members and a sidecar (%v), want none", got, bs.labelTxMembers != nil)
	}
	if got := sidecarFormerTokens(t, bs); !slices.Equal(got, want) || recorded() == 0 {
		t.Errorf("control: the sidecar over the same rows names %v (%d members), want %v and some", got, recorded(), want)
	}
}

// A label carried only by a delta history row is found: the walk decodes a
// delta's labels as the sidecar's build does.
func TestBuildFormerLabelTokens_ReadsDeltaRows(t *testing.T) {
	bs := deltaTestStore(t, true)
	const special = uint16(30)
	id := snowflake.ID(999001)
	if err := bs.PutNodeVersion(types.NodeID(id), 0, deltaMembershipTestNode(id, 0, 10, nil, 1000)); err != nil {
		t.Fatalf("PutNodeVersion v0: %v", err)
	}
	if err := bs.PutNodeVersion(types.NodeID(id), 1, deltaMembershipTestNode(id, 1, 10, []uint16{special}, 1100)); err != nil {
		t.Fatalf("PutNodeVersion v1: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	raw, err := bs.readHistoryNodeRaw(id, 1)
	if err != nil || storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
		t.Fatalf("v1 is not stored as a delta (%v): the test proves nothing", err)
	}
	tokens, err := bs.BuildFormerLabelTokens()
	if want := []uint16{10, special}; err != nil || !slices.Equal(tokens, want) {
		t.Errorf("former labels %v, %v; want %v", tokens, err, want)
	}
	if bs.labelTxMembers != nil {
		t.Error("the call left a sidecar on the handle")
	}
}
