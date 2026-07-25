package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RecordForeignIncoming stores a row stamped with a FOREIGN transaction time and
// never advances the commit-clock floor.
//
// The stub's temporal metadata is carried verbatim from the authoritative
// cross-machine edge (applyRelCreateTemporal(stub, edge.ValidFrom, edge.ValidTo,
// edge.CreatedAt, edge.TxFrom) — relationship_foreign.go), exactly like the
// replica-apply and import doors. Unlike both of those, no advance accompanies
// it. c.now() is called only for the dispatched EVENT timestamp, which never
// touches the stored row.
//
// So a peer whose clock is ahead of this host — or simply a peer mid-burst,
// where TxFrom outruns the wall by design — leaves a durably stored
// relationship whose TxFrom sits ABOVE NowTx(). Every AS-OF read at the pin
// silently drops that edge, which is precisely the lesson-71 anachronism the
// floor exists to close, on the one door that is BOTH public and foreign-stamped.
//
// The stamp is untrusted (a directly callable public API taking another
// machine's value, as BACKLOG 9h already established for names and properties),
// so it must be bounded like the import doors — but within the bound it must
// advance, because the row is stored either way.
func TestInstantFloor_RecordForeignIncomingCoversItsStoredStamp(t *testing.T) {
	g, endID := newForeignIncomingTestGraph(t, ValidationLimits{})

	// Plausible but ahead of this host: a peer one hour fast, or mid-burst.
	// Well inside the ~10y corruption bound, so this is a stamp that must be
	// ACCEPTED and covered, not refused.
	foreign := types.Instant(time.Now().Add(time.Hour).UnixMilli())

	edge := baseForeignEdge(endID)
	edge.TxFrom = foreign
	edge.AttestTx = foreign

	if err := g.Rels.RecordForeignIncoming(context.Background(), edge); err != nil {
		t.Fatalf("RecordForeignIncoming: %v", err)
	}

	// The stub really is stored with the foreign stamp.
	in, err := g.Rels.Incoming(endID, "KNOWS")
	if err != nil || len(in) == 0 {
		t.Fatalf("precondition: stub not recorded (in=%d err=%v)", len(in), err)
	}
	if got := in[0].Temporal().TxFrom; got != foreign {
		t.Fatalf("precondition: stored TxFrom = %d, want the foreign stamp %d", got, foreign)
	}

	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	if pin < foreign {
		t.Fatalf("NowTx = %d < the stored stub's TxFrom %d — RecordForeignIncoming persisted a row "+
			"stamped with a FOREIGN transaction time but never advanced the commit-clock floor, so "+
			"every AS-OF read at the pin silently drops the edge it just recorded. The import and "+
			"replica-apply doors both cover their stamps; this one does not.", pin, foreign)
	}
}
