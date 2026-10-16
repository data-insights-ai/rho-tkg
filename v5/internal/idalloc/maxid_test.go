package idalloc

import (
	"context"
	"math"
	"testing"
)

func TestReserveRefusesPastMaxPublicID(t *testing.T) {
	if MaxPublicID != math.MaxInt64 {
		t.Fatal("MaxPublicID must equal the signed ID bound")
	}
	s, r := fixture(t)
	s.high = MaxPublicID - 4
	next, blk, replay, err := Reserve(&s, r) // Count 4 ends exactly at the bound
	if err != nil || replay || blk.Last != MaxPublicID {
		t.Fatalf("block ending at bound: %+v %v", blk, err)
	}
	over := r
	over.Sequence, over.Count = 2, 1
	if _, _, _, err := Reserve(&next, over); err != ErrExhausted {
		t.Fatalf("+1 past bound: %v", err)
	}
	over.Count = 0
	if _, _, _, err := Reserve(&next, over); err != ErrInvalid {
		t.Fatalf("count 0: %v", err)
	}
	if n2, b2, replay, err := Reserve(&next, r); err != nil || !replay || b2 != blk || n2 != next {
		t.Fatalf("replay at bound: %+v %v %v", b2, replay, err)
	}
	s, r = fixture(t) // straddling block refused whole
	s.high = MaxPublicID - 3
	if _, _, _, err := Reserve(&s, r); err != ErrExhausted {
		t.Fatalf("straddle: %v", err)
	}
}

func TestAcquireRefusesGrantPastMaxPublicID(t *testing.T) {
	s, r := fixture(t)
	r.Sequence, r.Count = 7, 2
	bad := Reservation{Request: r, First: MaxPublicID, Last: MaxPublicID + 1}
	i := issuer(t, r)
	if _, err := i.Acquire(t.Context(), r, func(context.Context, Request) (Reservation, bool, error) { return bad, false, nil }); err == nil {
		t.Fatal("issuer accepted a grant past MaxPublicID")
	}
	_ = s
}

func TestRecipientRefusesGrantPastMaxPublicID(t *testing.T) {
	graph := GraphID{1}
	a, _ := NewAuthority([16]byte{2}, 1)
	session := RecipientSession{ID: [16]byte{3}, Incarnation: [16]byte{4}, Epoch: 1}
	req := GrantRequest{Graph: graph, Session: session, Sequence: 1, Count: 2}
	for _, c := range []struct {
		first uint64
		ok    bool
	}{{MaxPublicID - 1, true}, {MaxPublicID, false}} {
		rcp, err := NewRecipient(graph, session)
		if err != nil {
			t.Fatal(err)
		}
		g := Grant{Request: req, Reservation: Reservation{Request: Request{Graph: graph, Authority: a, Sequence: 1, Count: 2}, First: c.first, Last: c.first + 1}}
		cur, err := rcp.Acquire(t.Context(), req, func(GrantRequest) (Grant, error) { return g, nil })
		if (err == nil) != c.ok {
			t.Fatalf("first=%d: %v", c.first, err)
		}
		if c.ok {
			if id, err := cur.Next(); err != nil || id != c.first {
				t.Fatal(id, err)
			}
		}
	}
}
