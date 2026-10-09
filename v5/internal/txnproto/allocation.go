package txnproto

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

// Allocation query kinds carry explicit opaque namespace/session identities.
const (
	AllocatorState = "allocator"
	RecipientState = "recipient"
	RecipientAck   = "recipient-ack"
	GrantState     = "grant"
)

// AllocatorView is owned inspection data, never a grant or ownership certificate.
type AllocatorView struct {
	Owner                                [16]byte
	Epoch, HighWater, Sequence, MaxBlock uint64
}

// FenceAck retains a source group's committed fencing position.
type FenceAck struct {
	Group        uint8
	Epoch, Index uint64
}

// RecipientRecord retains trusted B fencing acknowledgement coordinates.
type RecipientRecord struct {
	Ack                        *FenceAck `json:",omitempty"`
	Session                    idalloc.RecipientSession
	Home                       uint8
	Previous                   uint64
	Phase                      string
	LastSequence               uint64
	ReserveBytes, ReserveSlots int
}

// GrantWire explicitly encodes private idalloc.Authority fields. Its range is
// reserved by the EXISTING algorithm; no range arithmetic allocates IDs here.
type GrantWire struct {
	Request                      idalloc.GrantRequest
	Owner                        [16]byte
	Epoch, Sequence, First, Last uint64
}

// IDClaim binds newly supplied IDs to their exact durable recipient grant.
// It does not replace independent partition ownership or graphstate validation.
// Scalar effects do not map claimed IDs to graph records; the graph installation
// layer must separately reject identity reuse.
type IDClaim struct {
	Grant GrantWire
	IDs   []uint64
}
type allocationCommand struct {
	Op            string
	Record        *RecipientRecord
	Request       *idalloc.GrantRequest
	Remote, Other *View
	Owner         [16]byte
	Epoch         uint64
}

func allocationEnabled(c Config) bool { return c.Namespace != (idalloc.GraphID{}) }
func checkAllocationConfig(c Config) error {
	if !allocationEnabled(c) {
		if c.AllocatorOwner != ([16]byte{}) || c.AllocatorEpoch != 0 || c.MaxIDBlock != 0 {
			return ErrInvalid
		}
		return nil
	}
	if c.AllocatorOwner == ([16]byte{}) || c.AllocatorEpoch == 0 || c.MaxIDBlock == 0 || c.MaxIDBlock > idalloc.MaxBlockSize {
		return ErrInvalid
	}
	return nil
}
func (m *Machine) initAllocation() {
	if !allocationEnabled(m.config) {
		return
	}
	m.state.Recipients = map[string]*RecipientRecord{}
	m.state.Grants = map[string]*GrantWire{}
	if m.config.Group == 0 {
		a, _ := idalloc.NewAuthority(m.config.AllocatorOwner, m.config.AllocatorEpoch)
		s, _ := idalloc.NewState(m.config.Namespace, a, m.config.MaxIDBlock)
		m.state.AllocImage, _ = idalloc.MarshalCheckpoint(&s)
	}
}
func recipientKey(id [16]byte) string { return hex.EncodeToString(id[:]) }
func grantKey(r idalloc.GrantRequest) string {
	r.Count = 0
	h := digest(r)
	return hex.EncodeToString(h[:])
}
func grantRequestValid(r idalloc.GrantRequest) bool {
	return r.Graph != (idalloc.GraphID{}) && r.Session.ID != ([16]byte{}) && r.Session.Incarnation != ([16]byte{}) && r.Session.Epoch > 0 && r.Sequence > 0 && r.Count > 0 && r.Count <= idalloc.MaxBlockSize
}
func grantBlock(g GrantWire) (idalloc.Grant, error) {
	a, e := idalloc.NewAuthority(g.Owner, g.Epoch)
	if e != nil {
		return idalloc.Grant{}, e
	}
	r := idalloc.Request{Graph: g.Request.Graph, Authority: a, Sequence: g.Sequence, Count: g.Request.Count}
	if !grantRequestValid(g.Request) || g.Sequence == 0 || g.First < g.Sequence || g.Last < g.First || g.Last-g.First != g.Request.Count-1 {
		return idalloc.Grant{}, ErrInvalid
	}
	return idalloc.Grant{Request: g.Request, Reservation: idalloc.Reservation{Request: r, First: g.First, Last: g.Last}}, nil
}
func isAllocationQuery(kind string) bool {
	return kind == AllocatorState || kind == RecipientState || kind == RecipientAck || kind == GrantState
}
func checkAllocationQuery(q Query) error {
	if q.TxID != "" || q.Round != 0 {
		return ErrInvalid
	}
	switch q.Kind {
	case AllocatorState:
		if q.RecipientID != ([16]byte{}) || q.Incarnation != ([16]byte{}) || q.RecipientEpoch != 0 || q.Sequence != 0 || q.Count != 0 {
			return ErrInvalid
		}
	case RecipientState, RecipientAck:
		if q.RecipientID == ([16]byte{}) || q.Incarnation != ([16]byte{}) || q.RecipientEpoch != 0 || q.Sequence != 0 || q.Count != 0 {
			return ErrInvalid
		}
	case GrantState:
		if !grantRequestValid(queryGrant(q, idalloc.GraphID{1})) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func queryGrant(q Query, g idalloc.GraphID) idalloc.GrantRequest {
	return idalloc.GrantRequest{Graph: g, Session: idalloc.RecipientSession{ID: q.RecipientID, Incarnation: q.Incarnation, Epoch: q.RecipientEpoch}, Sequence: q.Sequence, Count: q.Count}
}
func grantQuery(r idalloc.GrantRequest) Query {
	return Query{Kind: GrantState, RecipientID: r.Session.ID, Incarnation: r.Session.Incarnation, RecipientEpoch: r.Session.Epoch, Sequence: r.Sequence, Count: r.Count}
}
func (m *Machine) allocationAnswer(q Query, index uint64) (View, error) {
	if !allocationEnabled(m.config) {
		return View{}, ErrUnavailable
	}
	v := View{Namespace: m.config.Namespace, Graph: m.config.Graph, Topology: m.config.Topology, Group: m.config.Group, Epoch: m.config.Epochs[m.config.Group], Index: index, Kind: q.Kind}
	switch q.Kind {
	case AllocatorState:
		if m.config.Group != 0 {
			return View{}, ErrUnavailable
		}
		s, e := idalloc.DecodeCheckpoint(m.state.AllocImage)
		if e != nil {
			return View{}, e
		}
		a, e := idalloc.Inspect(&s)
		if e != nil {
			return View{}, e
		}
		v.Allocator = &AllocatorView{a.Authority.Owner(), a.Authority.Epoch(), a.HighWater, a.LastSequence, a.MaxBlock}
	case RecipientState, RecipientAck:
		r := m.state.Recipients[recipientKey(q.RecipientID)]
		if r == nil {
			return View{}, ErrUnavailable
		}
		if q.Kind == RecipientAck && (m.config.Group != 0 || r.Ack == nil) {
			return View{}, ErrUnavailable
		}
		v.Recipient = r
	case GrantState:
		r := queryGrant(q, m.config.Namespace)
		g := m.state.Grants[grantKey(r)]
		if g == nil {
			return View{}, ErrUnavailable
		}
		if g.Request != r {
			return View{}, ErrMismatch
		}
		v.Grant = g
	}
	return clone(v), nil
}
func (m *Machine) allocationProof(v *View, kind string) error {
	if v == nil || v.Kind != kind || v.Namespace != m.config.Namespace || v.Graph != m.config.Graph || v.Topology != m.config.Topology || v.Group > 1 || v.Epoch != m.config.Epochs[v.Group] || v.Index == 0 {
		return ErrStale
	}
	expected := View{Namespace: v.Namespace, Graph: v.Graph, Topology: v.Topology, Group: v.Group, Epoch: v.Epoch, Index: v.Index, Kind: kind}
	switch kind {
	case AllocatorState:
		if v.Group != 0 || v.Allocator == nil {
			return ErrInvalid
		}
		expected.Allocator = v.Allocator
	case RecipientState, RecipientAck:
		if v.Recipient == nil {
			return ErrInvalid
		}
		if kind == RecipientAck && (v.Group != 0 || v.Recipient.Ack == nil || v.Recipient.Ack.Group != 1 || v.Recipient.Ack.Epoch != m.config.Epochs[1] || v.Recipient.Ack.Index == 0) {
			return ErrInvalid
		}
		expected.Recipient = v.Recipient
	case GrantState:
		if v.Grant == nil {
			return ErrInvalid
		}
		expected.Grant = v.Grant
	default:
		return ErrInvalid
	}
	if digest(expected) != digest(*v) {
		return ErrInvalid
	}
	return nil
}
func validAllocationCommand(a allocationCommand) bool {
	e := allocationCommand{Op: a.Op}
	switch a.Op {
	case "service":
		e.Remote = a.Remote
		e.Owner = a.Owner
		e.Epoch = a.Epoch
		if a.Remote == nil {
			return false
		}
	case "begin":
		e.Record = a.Record
		if a.Record == nil {
			return false
		}
	case "fence", "ack", "publish", "install", "claim":
		e.Remote = a.Remote
		if a.Remote == nil {
			return false
		}
	case "activate":
		e.Remote = a.Remote
		e.Other = a.Other
		if a.Remote == nil || a.Other == nil {
			return false
		}
	case "reserve":
		e.Request = a.Request
		e.Remote = a.Remote
		if a.Request == nil || a.Remote == nil {
			return false
		}
	default:
		return false
	}
	return digest(a) == digest(e)
}
func allocationProposal(a allocationCommand) (Proposal, error) {
	return proposal(command{Kind: "allocation", Alloc: &a})
}

// TransferAllocator rotates ONLY reservation-service authority, preserving all
// granted ranges and recipient sessions. A restarted service needs a new epoch;
// it is not a partition ownership handoff and does not revoke cached blocks.
func (h *Host) TransferAllocator(current Proof) (Proposal, error) {
	if h == nil {
		return Proposal{}, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return Proposal{}, ErrUnavailable
	}
	if e := h.machine.allocationProof(&current.view, AllocatorState); e != nil {
		return Proposal{}, e
	}
	if h.machine.config.Group != 0 || current.view.Allocator.Epoch == math.MaxUint64 {
		return Proposal{}, ErrStale
	}
	v := current.View()
	return allocationProposal(allocationCommand{Op: "service", Remote: &v, Owner: h.readSession, Epoch: v.Allocator.Epoch + 1})
}

// BeginRecipient proposes an independent recipient incarnation. Epoch recovery
// rotates this recipient only. Current is zero for its first creation.
func (h *Host) BeginRecipient(id [16]byte, current Proof) (Proposal, error) {
	if h == nil || id == ([16]byte{}) {
		return Proposal{}, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return Proposal{}, ErrUnavailable
	}
	epoch := uint64(0)
	if current.view.Kind != "" {
		if e := h.machine.allocationProof(&current.view, RecipientState); e != nil {
			return Proposal{}, e
		}
		r := current.view.Recipient
		if current.view.Group != 0 && current.view.Group != h.machine.config.Group || r.Session.ID != id || r.Phase != "active" || r.Home != h.machine.config.Group {
			return Proposal{}, ErrStale
		}
		epoch = r.Session.Epoch
	}
	if epoch == math.MaxUint64 {
		return Proposal{}, ErrLimit
	}
	r := &RecipientRecord{Session: idalloc.RecipientSession{ID: id, Incarnation: h.readSession, Epoch: epoch + 1}, Home: h.machine.config.Group, Previous: epoch, Phase: "pending"}
	return allocationProposal(allocationCommand{Op: "begin", Record: r})
}

// FenceRecipient quiesces eligible prepared work before acknowledging a handoff.
func FenceRecipient(begin Proof) (Proposal, error) {
	if begin.view.Kind != RecipientState || begin.view.Recipient == nil {
		return Proposal{}, ErrUnavailable
	}
	v := begin.View()
	return allocationProposal(allocationCommand{Op: "fence", Remote: &v})
}

// ActivateRecipient requires both groups' durable fencing acknowledgements.
func ActivateRecipient(a, b Proof) (Proposal, error) {
	if a.view.Kind != RecipientState || b.view.Kind != RecipientState && b.view.Kind != RecipientAck {
		return Proposal{}, ErrUnavailable
	}
	v, w := a.View(), b.View()
	return allocationProposal(allocationCommand{Op: "activate", Remote: &v, Other: &w})
}

// PublishRecipient installs activation at B; no issuance there before its barrier.
func PublishRecipient(active Proof) (Proposal, error) {
	if active.view.Kind != RecipientState {
		return Proposal{}, ErrUnavailable
	}
	v := active.View()
	return allocationProposal(allocationCommand{Op: "publish", Remote: &v})
}

// InstallGrant transfers trusted source-produced grant data to a recipient's
// group. It never imports Proof or claims freshness of a delivery by itself.
func InstallGrant(g Proof) (Proposal, error) {
	if g.view.Kind != GrantState || g.view.Grant == nil {
		return Proposal{}, ErrUnavailable
	}
	v := g.View()
	return allocationProposal(allocationCommand{Op: "install", Remote: &v})
}
func recipientMatches(a, b *RecipientRecord) bool {
	return a != nil && b != nil && a.Session == b.Session && a.Home == b.Home && a.Previous == b.Previous
}
func relevant(t Tx, id [16]byte) bool {
	return t.Claim != nil && t.Claim.Grant.Request.Session.ID == id
}
func (m *Machine) allocationTransition(s *state, a allocationCommand) (bool, error) {
	if !allocationEnabled(m.config) {
		return false, ErrUnavailable
	}
	switch a.Op {
	case "service":
		if m.config.Group != 0 {
			return false, ErrStale
		}
		if e := m.allocationProof(a.Remote, AllocatorState); e != nil {
			return false, e
		}
		st, e := idalloc.DecodeCheckpoint(s.AllocImage)
		if e != nil {
			return false, e
		}
		v, _ := idalloc.Inspect(&st)
		expected, e := idalloc.NewAuthority(a.Remote.Allocator.Owner, a.Remote.Allocator.Epoch)
		if e != nil {
			return false, e
		}
		replacement, e := idalloc.NewAuthority(a.Owner, a.Epoch)
		if e != nil {
			return false, e
		}
		if v.Authority == replacement {
			return false, nil
		}
		next, e := idalloc.Transfer(&st, expected, replacement)
		if e != nil {
			return false, e
		}
		s.AllocImage, e = idalloc.MarshalCheckpoint(&next)
		return true, e
	case "begin":
		if m.config.Group != 0 {
			return false, ErrStale
		}
		r := a.Record
		if r.Session.ID == ([16]byte{}) || r.Session.Incarnation == ([16]byte{}) || r.Home > 1 || r.Phase != "pending" || r.Session.Epoch == 0 || r.Previous == math.MaxUint64 || r.Session.Epoch != r.Previous+1 || r.Ack != nil || r.LastSequence != 0 || r.ReserveBytes != 0 || r.ReserveSlots != 0 {
			return false, ErrInvalid
		}
		old := s.Recipients[recipientKey(r.Session.ID)]
		if recipientMatches(old, r) {
			return false, nil
		}
		if old != nil && (old.Phase != "active" || old.Session.Epoch != r.Previous || old.Home != r.Home) || old == nil && r.Previous != 0 {
			return false, ErrStale
		}
		ca, cb, e := recipientBudget(m.config, *r)
		if e != nil {
			return false, e
		}
		_ = cb
		r = new(clone(*r))
		r.ReserveBytes = ca
		r.ReserveSlots = 3
		s.Recipients[recipientKey(r.Session.ID)] = r
		return true, nil
	case "fence":
		if e := m.allocationProof(a.Remote, RecipientState); e != nil {
			return false, e
		}
		r := a.Remote.Recipient
		if a.Remote.Group != 0 || r.Phase != "pending" && r.Phase != "fenced" {
			return false, ErrStale
		}
		old := s.Recipients[recipientKey(r.Session.ID)]
		// A delayed fence for this exact session must not undo activation or
		// reset issuance state. A newer session still checks its predecessor below.
		if recipientMatches(old, r) && (old.Phase == "fenced" || old.Phase == "active") {
			return false, nil
		}
		if m.config.Group == 0 && !recipientMatches(old, r) || m.config.Group == 1 && (old == nil && r.Previous != 0 || old != nil && (old.Phase != "active" && old.Phase != "fenced" || old.Session.Epoch != r.Previous || old.Home != r.Home)) {
			return false, ErrStale
		}
		for _, i := range s.Intents {
			if relevant(i.Tx, r.Session.ID) && i.Vote.Yes && !i.Installed {
				return false, ErrPending
			}
		}
		for _, c := range s.Coordinators {
			if relevant(c.Tx, r.Session.ID) && c.Decision == nil {
				return false, ErrPending
			}
		}
		ca, cb, e := recipientBudget(m.config, *r)
		if e != nil {
			return false, e
		}
		next := new(clone(*r))
		next.Phase = "fenced"
		next.LastSequence = 0
		if m.config.Group == 0 {
			next.ReserveBytes = ca - fenceBytes(m.config, *r)
			next.ReserveSlots = 2
		} else {
			next.ReserveBytes = cb
			next.ReserveSlots = 1
		}
		s.Recipients[recipientKey(r.Session.ID)] = next
		return true, nil
	case "ack":
		if m.config.Group != 0 {
			return false, ErrStale
		}
		if e := m.allocationProof(a.Remote, RecipientState); e != nil {
			return false, e
		}
		r := a.Remote.Recipient
		old := s.Recipients[recipientKey(r.Session.ID)]
		if a.Remote.Group != 1 || r.Phase != "fenced" || !recipientMatches(old, r) || old.Phase != "fenced" {
			return false, ErrStale
		}
		ack := FenceAck{Group: 1, Epoch: a.Remote.Epoch, Index: a.Remote.Index}
		if old.Ack != nil {
			if old.Ack.Group != ack.Group || old.Ack.Epoch != ack.Epoch || old.Ack.Index > ack.Index {
				return false, ErrMismatch
			}
			return false, nil
		}
		data, _ := json.Marshal(command{Version: 1, Kind: "allocation", Alloc: &a})
		old.ReserveBytes -= len(data) + 32
		old.ReserveSlots--
		if old.ReserveBytes < 0 || old.ReserveSlots < 1 {
			return false, ErrLimit
		}
		old.Ack = &ack
		return true, nil
	case "activate":
		if a.Remote.Kind != RecipientState || a.Other.Kind != RecipientState && a.Other.Kind != RecipientAck {
			return false, ErrInvalid
		}
		if m.config.Group != 0 {
			return false, ErrStale
		}
		for _, v := range []*View{a.Remote, a.Other} {
			if e := m.allocationProof(v, v.Kind); e != nil {
				return false, e
			}
			if v.Recipient.Phase != "fenced" {
				return false, ErrPending
			}
		}
		if a.Remote.Group != 0 || a.Other.Group != 1 && a.Other.Kind != RecipientAck || !recipientMatches(a.Remote.Recipient, a.Other.Recipient) {
			return false, ErrMismatch
		}
		old := s.Recipients[recipientKey(a.Remote.Recipient.Session.ID)]
		if !recipientMatches(old, a.Remote.Recipient) {
			return false, ErrStale
		}
		if old.Phase == "active" {
			return false, nil
		}
		if old.Phase != "fenced" {
			return false, ErrPending
		}
		old.Phase = "active"
		old.ReserveBytes = 0
		old.ReserveSlots = 0
		return true, nil
	case "publish":
		if m.config.Group != 1 {
			return false, ErrStale
		}
		if e := m.allocationProof(a.Remote, RecipientState); e != nil {
			return false, e
		}
		r := a.Remote.Recipient
		old := s.Recipients[recipientKey(r.Session.ID)]
		if a.Remote.Group != 0 || r.Phase != "active" || !recipientMatches(old, r) {
			return false, ErrStale
		}
		if old.Phase == "active" {
			return false, nil
		}
		if old.Phase != "fenced" {
			return false, ErrPending
		}
		old.Phase = "active"
		old.ReserveBytes = 0
		old.ReserveSlots = 0
		return true, nil
	case "reserve":
		if m.config.Group != 0 {
			return false, ErrStale
		}
		r := *a.Request
		if !grantRequestValid(r) || r.Graph != m.config.Namespace {
			return false, ErrInvalid
		}
		if existing := s.Grants[grantKey(r)]; existing != nil {
			if existing.Request != r {
				return false, ErrMismatch
			}
			return false, nil
		}
		if e := m.allocationProof(a.Remote, AllocatorState); e != nil {
			return false, e
		}
		st, e := idalloc.DecodeCheckpoint(s.AllocImage)
		if e != nil {
			return false, e
		}
		v, _ := idalloc.Inspect(&st)
		if a.Remote.Allocator.Owner != v.Authority.Owner() || a.Remote.Allocator.Epoch != v.Authority.Epoch() {
			return false, ErrStale
		}
		recipient := s.Recipients[recipientKey(r.Session.ID)]
		if recipient == nil || recipient.Phase != "active" || recipient.Session != r.Session {
			return false, ErrStale
		}
		if r.Sequence <= recipient.LastSequence {
			return false, ErrMismatch
		}
		if v.LastSequence == math.MaxUint64 {
			return false, idalloc.ErrExhausted
		}
		next, b, _, e := idalloc.Reserve(&st, idalloc.Request{Graph: r.Graph, Authority: v.Authority, Sequence: v.LastSequence + 1, Count: r.Count})
		if e != nil {
			return false, e
		}
		g := &GrantWire{Request: r, Owner: b.Request.Authority.Owner(), Epoch: b.Request.Authority.Epoch(), Sequence: b.Request.Sequence, First: b.First, Last: b.Last}
		if e := m.grantFrameBudget(*g); e != nil {
			return false, e
		}
		s.Grants[grantKey(r)] = g
		recipient.LastSequence = r.Sequence
		s.AllocImage, e = idalloc.MarshalCheckpoint(&next)
		return true, e
	case "install":
		if e := m.allocationProof(a.Remote, GrantState); e != nil {
			return false, e
		}
		if a.Remote.Group != 0 {
			return false, ErrStale
		}
		g := a.Remote.Grant
		if _, e := grantBlock(*g); e != nil {
			return false, e
		}
		r := s.Recipients[recipientKey(g.Request.Session.ID)]
		if r == nil || r.Phase != "active" || r.Session != g.Request.Session {
			return false, ErrStale
		}
		if old := s.Grants[grantKey(g.Request)]; old != nil {
			if *old != *g {
				return false, ErrMismatch
			}
			return false, nil
		}
		if e := m.grantFrameBudget(*g); e != nil {
			return false, e
		}
		s.Grants[grantKey(g.Request)] = new(clone(*g))
		return true, nil
	}
	return false, ErrInvalid
}
func fenceBytes(c Config, r RecipientRecord) int {
	r.ReserveBytes = 0
	r.ReserveSlots = 0
	v := View{Namespace: c.Namespace, Graph: c.Graph, Topology: c.Topology, Group: 0, Epoch: c.Epochs[0], Index: math.MaxUint64, Kind: RecipientState, Recipient: &r}
	b, _ := json.Marshal(command{Version: 1, Kind: "allocation", Alloc: &allocationCommand{Op: "fence", Remote: &v}})
	return len(b) + 32
}
func recipientBudget(c Config, r RecipientRecord) (int, int, error) {
	r.ReserveBytes = 0
	r.ReserveSlots = 0
	r.Phase = "fenced"
	av := View{Namespace: c.Namespace, Graph: c.Graph, Topology: c.Topology, Group: 0, Epoch: c.Epochs[0], Index: math.MaxUint64, Kind: RecipientState, Recipient: &r}
	bv := av
	bv.Group = 1
	bv.Epoch = c.Epochs[1]
	activate := command{Version: 1, Kind: "allocation", Alloc: &allocationCommand{Op: "activate", Remote: &av, Other: &bv}}
	r.Phase = "active"
	publish := command{Version: 1, Kind: "allocation", Alloc: &allocationCommand{Op: "publish", Remote: &av}}
	ba, _ := json.Marshal(activate)
	bp, _ := json.Marshal(publish)
	f := fenceBytes(c, r)
	// Proofs carry reserved metadata too; fixed finite decimal-width padding covers
	// those three int fields. Refuse before entering a pending handoff.
	if max(len(ba), len(bp), f-32)+512 > c.Limits.CommandBytes {
		return 0, 0, ErrLimit
	}
	return 2*f + len(ba) + 2048, len(bp) + 512, nil
}
func (m *Machine) grantFrameBudget(g GrantWire) error {
	v := View{Namespace: m.config.Namespace, Graph: m.config.Graph, Topology: m.config.Topology, Group: 0, Epoch: m.config.Epochs[0], Index: math.MaxUint64, Kind: GrantState, Grant: &g}
	b, _ := json.Marshal(command{Version: 1, Kind: "allocation", Alloc: &allocationCommand{Op: "install", Remote: &v}})
	if len(b) > m.config.Limits.CommandBytes {
		return ErrLimit
	}
	return nil
}
func (m *Machine) checkIDClaim(s *state, t Tx, a *allocationCommand) error {
	if t.Claim == nil {
		if a != nil {
			return ErrInvalid
		}
		return nil
	}
	if !allocationEnabled(m.config) || t.Namespace != m.config.Namespace {
		return ErrStale
	}
	g := t.Claim.Grant
	if g.Request.Graph != t.Namespace {
		return ErrStale
	}
	if _, e := grantBlock(g); e != nil {
		return e
	}
	if len(t.Claim.IDs) == 0 || len(t.Claim.IDs) > 128 {
		return ErrInvalid
	}
	for j, id := range t.Claim.IDs {
		if id < g.First || id > g.Last || j > 0 && t.Claim.IDs[j-1] >= id {
			return ErrInvalid
		}
	}
	r := s.Recipients[recipientKey(g.Request.Session.ID)]
	if r == nil || r.Phase != "active" || r.Session != g.Request.Session {
		return ErrStale
	}
	if a != nil {
		if a.Op != "claim" {
			return ErrInvalid
		}
		if e := m.allocationProof(a.Remote, GrantState); e != nil {
			return e
		}
		if *a.Remote.Grant != g {
			return ErrMismatch
		}
	}
	if old := s.Grants[grantKey(g.Request)]; old != nil && *old != g {
		return ErrMismatch
	}
	s.Grants[grantKey(g.Request)] = new(g)
	return nil
}
func withIDs(t Tx, g Proof, ids []uint64, kind string) (Proposal, error) {
	if g.view.Kind != GrantState || g.view.Grant == nil {
		return Proposal{}, ErrUnavailable
	}
	if t.Claim != nil {
		return Proposal{}, ErrInvalid
	}
	if e := validateTx(t); e != nil {
		return Proposal{}, e
	}
	if kind == "local" && len(t.Participants) != 1 {
		return Proposal{}, ErrInvalid
	}
	t.Namespace = g.view.Namespace
	t.Claim = &IDClaim{Grant: *g.view.Grant, IDs: slices.Clone(ids)}
	v := g.View()
	return proposal(command{Kind: kind, Tx: &t, Alloc: &allocationCommand{Op: "claim", Remote: &v}})
}

// RegisterIDs binds freshly supplied graph IDs to an actual authoritative grant.
func RegisterIDs(t Tx, g Proof, ids []uint64) (Proposal, error) {
	return withIDs(t, g, ids, "register")
}

// LocalIDs retains the one-entry local path; cached grant validation is local.
func LocalIDs(t Tx, g Proof, ids []uint64) (Proposal, error) { return withIDs(t, g, ids, "local") }

// RecordRecipientFence co-commits B's authoritative acknowledgement at A. This
// lets a source-owned process build activation without importing another Proof.
func RecordRecipientFence(b Proof) (Proposal, error) {
	if b.view.Kind != RecipientState || b.view.Recipient == nil {
		return Proposal{}, ErrUnavailable
	}
	v := b.View()
	return allocationProposal(allocationCommand{Op: "ack", Remote: &v})
}
