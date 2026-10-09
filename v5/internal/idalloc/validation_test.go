package idalloc

import (
	"errors"
	"math"
	"testing"
)

func TestSharedGrantShapeValidation(t *testing.T) {
	session := RecipientSession{ID: [16]byte{1}, Incarnation: [16]byte{2}, Epoch: 1}
	request := GrantRequest{Graph: GraphID{3}, Session: session, Sequence: 17, Count: 2}
	authority, _ := NewAuthority([16]byte{4}, 1)
	grant := Grant{Request: request, Reservation: Reservation{Request: Request{Graph: request.Graph, Authority: authority, Sequence: 1, Count: 2}, First: 1, Last: 2}}
	if err := ValidateRecipientSession(session); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGrantRequest(request); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGrant(grant); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []RecipientSession{{}, {ID: session.ID, Epoch: 1}, {Incarnation: session.Incarnation, Epoch: 1}, {ID: session.ID, Incarnation: session.Incarnation}} {
		if err := ValidateRecipientSession(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, edit := range []func(*GrantRequest){func(r *GrantRequest) { r.Graph = GraphID{} }, func(r *GrantRequest) { r.Session = RecipientSession{} }, func(r *GrantRequest) { r.Sequence = 0 }, func(r *GrantRequest) { r.Count = 0 }, func(r *GrantRequest) { r.Count = MaxBlockSize + 1 }} {
		bad := request
		edit(&bad)
		if err := ValidateGrantRequest(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, edit := range []func(*Grant){func(g *Grant) { g.Request.Session = RecipientSession{} }, func(g *Grant) { g.Reservation.Request.Graph = GraphID{9} }, func(g *Grant) { g.Reservation.Request.Count++ }, func(g *Grant) { g.Reservation.First = 0 }, func(g *Grant) { g.Reservation.Last = 0 }, func(g *Grant) { g.Reservation.Last++ }, func(g *Grant) { g.Reservation.Request.Sequence = 3 }, func(g *Grant) { g.Reservation.Request.Authority = Authority{} }} {
		bad := grant
		edit(&bad)
		if err := ValidateGrant(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal(bad, err)
		}
	}
	final := grant
	final.Request.Count = 1
	final.Reservation.Request.Count = 1
	final.Reservation.Request.Sequence = math.MaxUint64
	final.Reservation.First = math.MaxUint64
	final.Reservation.Last = math.MaxUint64
	if err := ValidateGrant(final); err != nil {
		t.Fatal(err)
	}
	final.Reservation.First--
	if err := ValidateGrant(final); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestRecipientDeliveryPreservesSharedInvalidCause(t *testing.T) {
	s := RecipientSession{ID: [16]byte{1}, Incarnation: [16]byte{2}, Epoch: 1}
	r := GrantRequest{Graph: GraphID{3}, Session: s, Sequence: 1, Count: 1}
	i, err := NewRecipient(r.Graph, s)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := i.Acquire(t.Context(), r, func(GrantRequest) (Grant, error) { return Grant{Request: r}, nil })
	if cursor != nil || !errors.Is(err, ErrPayloadMismatch) || !errors.Is(err, ErrInvalid) {
		t.Fatal(cursor, err)
	}
}
