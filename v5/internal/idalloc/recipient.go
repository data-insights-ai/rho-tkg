package idalloc

import (
	"context"
	"sync"
)

// RecipientSession identifies one independently fenced cached-block recipient.
// It is distinct from the graph-wide reservation service Authority. The embedding
// commits activation and permits exactly one live instance per incarnation.
type RecipientSession struct {
	ID, Incarnation [16]byte
	Epoch           uint64
}

// GrantRequest has recipient-local sequencing; global block sequencing remains
// exclusively in State/Reserve. Unknown attempts are abandoned, never retried for
// fresh issuance; the embedding can recover their retained range separately.
type GrantRequest struct {
	Graph           GraphID
	Session         RecipientSession
	Sequence, Count uint64
}

// Grant binds a recipient request to a block from the existing global algorithm.
// It is data, not an acknowledgement of durability.
type Grant struct {
	Request     GrantRequest
	Reservation Reservation
}

// Recipient serializes deliveries for one durable session. Do not copy/restore
// it or open another instance for that incarnation. The embedding owns fencing.
type Recipient struct {
	mu              sync.Mutex
	graph           GraphID
	session         RecipientSession
	attempted, high uint64
}

// NewRecipient initializes a volatile recipient after durable session activation.
// This primitive cannot itself establish that activation or quorum durability.
func NewRecipient(graph GraphID, session RecipientSession) (*Recipient, error) {
	if graph == (GraphID{}) || session.ID == ([16]byte{}) || session.Incarnation == ([16]byte{}) || session.Epoch == 0 {
		return nil, ErrInvalid
	}
	return &Recipient{graph: graph, session: session}, nil
}

// Acquire burns an attempt before trusted delivery. The callback must validate
// actual request-bound quorum evidence and current recipient fencing; it supplies
// no caller boolean. The callback cannot reenter Recipient. Errors burn submitted
// attempts; cancellation BEFORE delivery does not. Global ranges never overlap.
func (i *Recipient) Acquire(ctx context.Context, r GrantRequest, deliver func(GrantRequest) (Grant, error)) (*Cursor, error) {
	if i == nil || ctx == nil || deliver == nil || r.Graph != i.graph || r.Sequence == 0 || r.Count == 0 || r.Count > MaxBlockSize {
		return nil, ErrInvalid
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if r.Session != i.session {
		return nil, ErrStaleAuthority
	}
	if r.Sequence <= i.attempted {
		return nil, ErrAlreadyReserved
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i.attempted = r.Sequence
	g, err := deliver(r)
	if err != nil {
		return nil, err
	}
	b := g.Reservation
	if g.Request != r || b.Request.Graph != r.Graph || b.Request.Count != r.Count || !b.validFor(b.Request) || b.First <= i.high {
		return nil, ErrPayloadMismatch
	}
	i.high = b.Last
	return &Cursor{block: b, next: b.First}, nil
}
