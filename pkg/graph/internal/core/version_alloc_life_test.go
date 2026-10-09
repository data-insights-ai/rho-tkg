package core

import (
	"errors"
	"math"
	"testing"

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

	// begin: a caller instant must exceed maxStamp; the plain door raises the
	// clock floor past it.
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	for _, at := range []types.Instant{1, 54, 55} {
		if err := ls.begin(g, at); !errors.Is(err, ErrTxOrder) || !errors.Is(err, ErrInvalidTxFrom) {
			t.Fatalf("begin(t=%d) = %v; want ErrTxOrder wrapping ErrInvalidTxFrom", at, err)
		}
	}
	if err := ls.begin(g, 56); err != nil {
		t.Fatalf("begin(t=56) = %v", err)
	}
	ahead := lifeStart{maxStamp: g.now() + 3_600_000}
	if err := ahead.begin(g, 0); err != nil {
		t.Fatalf("begin(plain) = %v", err)
	}
	if now := g.now(); now <= ahead.maxStamp {
		t.Fatalf("plain begin left the clock at %d, not past the chain's stamp %d", now, ahead.maxStamp)
	}
}
