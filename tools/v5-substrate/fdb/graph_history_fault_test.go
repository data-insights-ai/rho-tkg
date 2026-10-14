package main

import (
	"crypto/sha256"
	"errors"
	"testing"
)

// Authored source guards; the separate launcher supplies actual process loss.
func TestGraphHistoryProcessIdentityAndCommittedPageGuards(t *testing.T) {
	if err := (*graphHistoryRead)(nil).open(t.Context(), graphHistoryToken{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil caller acquired source", err)
	}
	if _, err := historyFaultOwner(nil, historyFaultState{}); !errors.Is(err, errInvalid) {
		t.Fatal("missing factory identity admitted", err)
	}
	if historyFaultPartial(historyFaultPage{Result: graphHistoryPage{Done: true, Deleted: [2]uint64{1, 0}}}) {
		t.Fatal("terminal seal relabelled partial")
	}
	if historyFaultPartial(historyFaultPage{Result: graphHistoryPage{Phase: historyCensus}}) {
		t.Fatal("census-only page relabelled physical collection")
	}
	if !historyFaultPartial(historyFaultPage{Result: graphHistoryPage{Phase: historyDeleting, Deleted: [2]uint64{1, 0}}, After: historyFaultObservation{State: graphHistoryState{Phase: historyDeleting, Cursor: []byte("actual")}}}) {
		t.Fatal("actual nonterminal deletion rejected")
	}
	token := graphHistoryToken{Request: "pin", Cut: certificate{Scope: []uint8{0, 1}}}
	body, err := wire(token)
	if err != nil {
		t.Fatal(err)
	}
	owner := &graphHistoryRead{s: &graphHistoryRetention{}, id: graphHistoryOwner{Kind: "read", Request: "saved-reader", State: historyActive, Secret: [32]byte{1}, Cut: token.Cut, PinRequest: token.Request, PinDigest: sha256.Sum256(body)}}
	wrong := token
	wrong.Secret[0] = 1
	if err := owner.open(t.Context(), wrong); !errors.Is(err, errMismatch) {
		t.Fatal("substituted token reached native acquisition", err)
	}
	if _, err := historyFaultOwner(&graphHistoryRetention{}, historyFaultState{Owner: owner.id, Token: wrong, Old: token.Cut}); !errors.Is(err, errInvalid) {
		t.Fatal("replacement handle lost exact original pin binding", err)
	}
}
