package idalloc

import (
	"context"
	"errors"
	"testing"
)

func TestRecipientUnknownReplayAndIndependentRanges(t *testing.T) {
	graph := GraphID{1}
	a, _ := NewAuthority([16]byte{2}, 1)
	s, _ := NewState(graph, a, 8)
	r1 := RecipientSession{ID: [16]byte{3}, Incarnation: [16]byte{4}, Epoch: 1}
	r2 := RecipientSession{ID: [16]byte{5}, Incarnation: [16]byte{6}, Epoch: 1}
	i1, e := NewRecipient(graph, r1)
	if e != nil {
		t.Fatal(e)
	}
	i2, e := NewRecipient(graph, r2)
	if e != nil {
		t.Fatal(e)
	}
	request1 := GrantRequest{Graph: graph, Session: r1, Sequence: 1, Count: 2}
	request2 := GrantRequest{Graph: graph, Session: r2, Sequence: 1, Count: 2}
	n, b, _, e := Reserve(&s, Request{Graph: graph, Authority: a, Sequence: 1, Count: 2})
	if e != nil {
		t.Fatal(e)
	}
	s = n
	if _, e = i1.Acquire(t.Context(), request1, func(GrantRequest) (Grant, error) { return Grant{}, ErrUnknown }); !errors.Is(e, ErrUnknown) {
		t.Fatal(e)
	}
	if _, e = i1.Acquire(t.Context(), request1, func(GrantRequest) (Grant, error) { return Grant{request1, b}, nil }); !errors.Is(e, ErrAlreadyReserved) {
		t.Fatal("unknown attempt was reused", e)
	}
	_, b2, _, e := Reserve(&s, Request{Graph: graph, Authority: a, Sequence: 2, Count: 2})
	if e != nil {
		t.Fatal(e)
	}
	c, e := i2.Acquire(t.Context(), request2, func(GrantRequest) (Grant, error) { return Grant{request2, b2}, nil })
	if e != nil {
		t.Fatal(e)
	}
	id, e := c.Next()
	if e != nil || id != 3 {
		t.Fatal(id, e)
	}
	if _, e = i2.Acquire(t.Context(), request2, func(GrantRequest) (Grant, error) { t.Fatal("replayed callback"); return Grant{}, nil }); !errors.Is(e, ErrAlreadyReserved) {
		t.Fatal(e)
	}
	request2.Sequence = 2
	if _, e = i2.Acquire(t.Context(), request2, func(GrantRequest) (Grant, error) { return Grant{request2, b2}, nil }); !errors.Is(e, ErrPayloadMismatch) {
		t.Fatal("overlapping range accepted", e)
	}
}

func TestRecipientRejectsMalformedAndCancelledDelivery(t *testing.T) {
	graph := GraphID{1}
	session := RecipientSession{ID: [16]byte{2}, Incarnation: [16]byte{3}, Epoch: 1}
	if _, e := NewRecipient(GraphID{}, session); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	i, _ := NewRecipient(graph, session)
	r := GrantRequest{Graph: graph, Session: session, Sequence: 1, Count: 1}
	var nilI *Recipient
	if _, e := nilI.Acquire(t.Context(), r, func(GrantRequest) (Grant, error) { return Grant{}, nil }); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	wrong := r
	wrong.Session.Epoch++
	if _, e := i.Acquire(t.Context(), wrong, func(GrantRequest) (Grant, error) { return Grant{}, nil }); !errors.Is(e, ErrStaleAuthority) {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, e := i.Acquire(ctx, r, func(GrantRequest) (Grant, error) { t.Fatal("cancel submitted"); return Grant{}, nil }); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e := i.Acquire(t.Context(), r, func(GrantRequest) (Grant, error) { return Grant{Request: r}, nil }); !errors.Is(e, ErrPayloadMismatch) {
		t.Fatal(e)
	}
}
