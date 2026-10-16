package txnproto

import (
	"context"
	"crypto/rand"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

// RecipientHandle is one process-owned volatile recipient. It cannot be copied,
// serialized or restored. Recovery activates a new recipient incarnation first.
type RecipientHandle struct {
	host      *Host
	recipient *idalloc.Recipient
	session   idalloc.RecipientSession
}

// OpenRecipient opens exactly one handle from this Host's OWN completed read.
// Source inspection data, another host's proof, and an old process incarnation
// cannot create issuance authority. Service leadership is a separate authority.
func (h *Host) OpenRecipient(active Proof) (*RecipientHandle, error) {
	if h == nil {
		return nil, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrUnavailable
	}
	if e := h.machine.allocationProof(&active.view, RecipientState); e != nil {
		return nil, e
	}
	r := active.view.Recipient
	if active.readID.Session != h.readSession || active.readID.Sequence == 0 || active.view.Group != h.machine.config.Group || r.Home != h.machine.config.Group || r.Phase != "active" || r.Session.Incarnation != h.readSession {
		return nil, ErrStale
	}
	current := h.machine.state.Recipients[recipientKey(r.Session.ID)]
	if !recipientMatches(current, r) || current.Phase != "active" {
		return nil, ErrStale
	}
	if old := h.recipients[r.Session.ID]; old != nil && old.session == r.Session {
		return nil, idalloc.ErrAlreadyReserved
	}
	i, e := idalloc.NewRecipient(h.machine.config.Namespace, r.Session)
	if e != nil {
		return nil, e
	}
	result := &RecipientHandle{host: h, recipient: i, session: r.Session}
	h.recipients[r.Session.ID] = result
	return result, nil
}

// Acquire performs one block refill. Transport must submit the opaque reserve
// proposal to A, relay its source-produced InstallGrant proposal if needed, then
// return the matching HOME Host.Read Reply. The private Proof embeds its actual
// ReadID and question/nonce; rewriting public correlation fields cannot certify
// an old response. Unknown outcomes burn attempts; inspection retry recovers the
// same range but never recreates a cursor. No per-ID remote call is performed.
func (i *RecipientHandle) Acquire(ctx context.Context, sequence, count uint64, transport func(context.Context, Proposal, Query) (Reply, error)) (*idalloc.Cursor, error) {
	if i == nil || ctx == nil || transport == nil {
		return nil, ErrInvalid
	}
	h := i.host
	r := idalloc.GrantRequest{Graph: h.machine.config.Namespace, Session: i.session, Sequence: sequence, Count: count}
	held := false
	cursor, err := i.recipient.Acquire(ctx, r, func(request idalloc.GrantRequest) (idalloc.Grant, error) {
		h.mu.Lock()
		current := h.machine.state.Recipients[recipientKey(i.session.ID)]
		if h.closed || current == nil || current.Phase != "active" || current.Session != i.session {
			h.mu.Unlock()
			return idalloc.Grant{}, ErrStale
		}
		h.mu.Unlock()
		// Service authority is read by the transport; the request itself binds the
		// recipient. A's reducer checks current service authority at application.
		p, e := allocationProposal(allocationCommand{Op: "reserve", Request: &request})
		if e != nil {
			return idalloc.Grant{}, e
		}
		q := grantQuery(request)
		if _, e = rand.Read(q.Nonce[:]); e != nil {
			return idalloc.Grant{}, e
		}
		reply, e := transport(ctx, p, q)
		if e != nil {
			return idalloc.Grant{}, errors.Join(idalloc.ErrUnknown, e)
		}
		if reply.Err != nil {
			return idalloc.Grant{}, errors.Join(idalloc.ErrUnknown, reply.Err)
		}
		proof := reply.Proof
		if reply.Query != q || proof.question != q || reply.ReadID != proof.readID || reply.ReadID.Session != h.readSession || reply.ReadID.Sequence == 0 {
			return idalloc.Grant{}, ErrMismatch
		}
		h.mu.Lock()
		held = true
		current = h.machine.state.Recipients[recipientKey(i.session.ID)]
		if h.closed || current == nil || current.Phase != "active" || current.Session != i.session {
			return idalloc.Grant{}, ErrStale
		}
		if e := h.machine.allocationProof(&proof.view, GrantState); e != nil {
			return idalloc.Grant{}, e
		}
		if proof.view.Group != h.machine.config.Group || proof.view.Grant.Request != request {
			return idalloc.Grant{}, ErrMismatch
		}
		return grantBlock(*proof.view.Grant)
	})
	if held {
		h.mu.Unlock()
	}
	return cursor, err
}

// AuthorizeReserve binds a recipient refill intent to this process's current
// quorum-observed reservation-service incarnation. A reopened service must first
// TransferAllocator; cached recipient cursors do not change authority with it.
func (h *Host) AuthorizeReserve(intent Proposal, service Proof) (Proposal, error) {
	if h == nil {
		return Proposal{}, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return Proposal{}, ErrUnavailable
	}
	a := intent.command.Alloc
	if intent.command.Kind != "allocation" || a == nil || a.Op != "reserve" || a.Request == nil || a.Remote != nil || a.Other != nil || a.Record != nil || a.Owner != ([16]byte{}) || a.Epoch != 0 {
		return Proposal{}, ErrInvalid
	}
	if e := h.machine.allocationProof(&service.view, AllocatorState); e != nil {
		return Proposal{}, e
	}
	if h.machine.config.Group != 0 || service.view.Allocator.Owner != h.readSession || service.readID.Session != h.readSession {
		return Proposal{}, ErrStale
	}
	v := service.View()
	return allocationProposal(allocationCommand{Op: "reserve", Request: new(*a.Request), Remote: &v})
}
