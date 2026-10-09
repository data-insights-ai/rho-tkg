package core

import (
	"errors"
	"math"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestLifeStartOf pins the re-import allocator (backlog 38) on hand-built
// history: the start is one above the HIGHEST version whatever the slice
// order (not the last row, not the tombstone), the predecessor hash is that
// row's, the stamp bound covers TxFrom, TxTo and DeletedAt of every row, an
// empty history is a fresh ID, and a chain at the version ceiling refuses with
// ErrVersionOverflow instead of wrapping to 0 (which would overwrite the
// genesis row).
func TestLifeStartOf(t *testing.T) {
	t.Parallel()
	row := func(v uint32, hash string, tm types.TemporalMetadata) *types.Node {
		n := types.NewNode(types.NodeID(7), 1, nil)
		n.SetVersion(v)
		n.SetIntegrity(&types.NodeIntegrity{Hash: hash})
		n.SetTemporal(&tm)
		return n
	}
	hash := nodeIntegrityHash

	if ls, err := lifeStartOf([]*types.Node(nil), hash); err != nil || ls != (lifeStart{}) {
		t.Fatalf("empty history: %+v, %v; want the zero lifeStart", ls, err)
	}

	// Unordered: the tombstone (v1) is neither the top version nor the last
	// row; the cascade row v3 is the top; the largest stamp is a DeletedAt
	// above its row's TxTo (a stored chain may carry it).
	hist := []*types.Node{
		row(3, "h3", types.TemporalMetadata{TxFrom: 40}),
		row(0, "h0", types.TemporalMetadata{TxFrom: 10, TxTo: 20}),
		row(1, "h1", types.TemporalMetadata{TxFrom: 20, TxTo: 50, DeletedAt: 55}),
		row(2, "h2", types.TemporalMetadata{TxFrom: 30}),
	}
	ls, err := lifeStartOf(hist, hash)
	if err != nil || ls.version != 4 || ls.prevHash != "h3" || ls.maxStamp != 55 {
		t.Fatalf("lifeStartOf = %+v, %v; want version 4, prevHash h3, maxStamp 55", ls, err)
	}

	if _, err := lifeStartOf([]*types.Node{row(math.MaxUint32, "hm", types.TemporalMetadata{TxFrom: 1})}, hash); !errors.Is(err, ErrVersionOverflow) {
		t.Fatalf("ceiling: err = %v; want ErrVersionOverflow", err)
	}

	// A caller instant must exceed maxStamp; 0 (the plain door) is not checked.
	for _, at := range []types.Instant{1, 54, 55} {
		if err := ls.checkCallerTx(at); !errors.Is(err, ErrTxOrder) || !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("checkCallerTx(t=%d) = %v; want ErrTxOrder wrapping ErrInvalidTxFrom", at, err)
		}
	}
	for _, at := range []types.Instant{0, 56} {
		if err := ls.checkCallerTx(at); err != nil {
			t.Fatalf("checkCallerTx(t=%d) = %v", at, err)
		}
	}
	// The plain door's stamp: the clock, or one past a chain stamp ahead of it.
	if got := ls.txFrom(100); got != 100 {
		t.Fatalf("txFrom(100) = %d; want the clock 100", got)
	}
	if got := ls.txFrom(55); got != 56 {
		t.Fatalf("txFrom(55) = %d; want 56 (one past the chain's stamp)", got)
	}
	if got := (lifeStart{}).txFrom(7); got != 7 {
		t.Fatalf("fresh ID txFrom(7) = %d; want 7", got)
	}
}

// TestStubLifeStart: an ID whose rows are gone but whose compaction stub
// remains continues above the trimmed versions, linked to the last trimmed
// hash; no stub, a graph that never compacted, a failing stub read and the
// version ceiling each answer as stated.
func TestStubLifeStart(t *testing.T) {
	t.Parallel()
	c := &Core{}
	stub := compactionStub{TrimmedThroughVersion: 4, LastTrimmedHash: "h4", LastTrimmedTxTo: 77}
	found := func() (compactionStub, bool, error) { return stub, true, nil }
	if ls, err := c.stubLifeStart(found); err != nil || ls != (lifeStart{}) {
		t.Fatalf("never compacted: %+v, %v; want the zero lifeStart (no probe)", ls, err)
	}
	c.compactedThroughTx.Store(1)
	if ls, err := c.stubLifeStart(found); err != nil || ls != (lifeStart{version: 5, prevHash: "h4", maxStamp: 77}) {
		t.Fatalf("stub: %+v, %v; want version 5, prevHash h4, maxStamp 77", ls, err)
	}
	if ls, err := c.stubLifeStart(func() (compactionStub, bool, error) { return compactionStub{}, false, nil }); err != nil || ls != (lifeStart{}) {
		t.Fatalf("no stub: %+v, %v", ls, err)
	}
	boom := errors.New("stub read failed")
	if _, err := c.stubLifeStart(func() (compactionStub, bool, error) { return compactionStub{}, false, boom }); !errors.Is(err, boom) {
		t.Fatalf("stub read error: %v", err)
	}
	if _, err := c.stubLifeStart(func() (compactionStub, bool, error) {
		return compactionStub{TrimmedThroughVersion: math.MaxUint32}, true, nil
	}); !errors.Is(err, ErrVersionOverflow) {
		t.Fatalf("ceiling: %v; want ErrVersionOverflow", err)
	}
}

// lifeFaultStore fails the history reads the re-import allocator makes.
// Embedding MandatoryStore hides HistoryPresenceCapability; lifePresenceStore
// adds it back with a failing probe.
type lifeFaultStore struct {
	storepkg.MandatoryStore
	err error
}

func (s *lifeFaultStore) GetNodeHistory(types.NodeID) ([]*types.Node, error) { return nil, s.err }
func (s *lifeFaultStore) GetRelHistory(types.RelID) ([]*types.Relationship, error) {
	return nil, s.err
}

type lifePresenceStore struct {
	*lifeFaultStore
}

func (s *lifePresenceStore) HasNodeHistory(types.NodeID) (bool, error) { return false, s.err }
func (s *lifePresenceStore) HasRelHistory(types.RelID) (bool, error)   { return false, s.err }

// TestLifeStartStoreErrors: a failing history read refuses the re-import
// (never "no history", which would restart at version 0 over the stored
// rows); a relationship slot that is not local holds no history here.
func TestLifeStartStoreErrors(t *testing.T) {
	t.Parallel()
	boom := errors.New("history read failed")
	for _, presence := range []bool{false, true} {
		for _, tc := range []struct {
			err     error
			wantErr bool
			relOnly bool // ErrSlotNotLocal: the rel allocator answers "no history"
		}{
			{boom, true, false},
			{storepkg.ErrSlotNotLocal, false, true},
		} {
			fs := &lifeFaultStore{MandatoryStore: memory.New(), err: tc.err}
			c := &Core{store: fs}
			if presence {
				c.store = &lifePresenceStore{fs}
			}
			_, nerr := c.nodeLifeStart(7)
			if !tc.relOnly && !errors.Is(nerr, tc.err) {
				t.Fatalf("presence=%v node: err = %v; want %v", presence, nerr, tc.err)
			}
			ls, rerr := c.relLifeStart(9)
			switch {
			case tc.wantErr && !errors.Is(rerr, tc.err):
				t.Fatalf("presence=%v rel: err = %v; want %v", presence, rerr, tc.err)
			case !tc.wantErr && (rerr != nil || ls != (lifeStart{})):
				t.Fatalf("presence=%v rel slot not local: %+v, %v; want no history", presence, ls, rerr)
			}
		}
	}
}
