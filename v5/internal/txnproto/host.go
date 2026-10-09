package txnproto

import (
	"crypto/rand"
	"encoding/json"
	"sync"
	"unicode/utf8"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// ReadID names one Host.Read invocation. The random session changes on reopen;
// Sequence never wraps. Callers must match this ID, not just the application Query.
// It identifies a request, never a durability or quorum certificate.
type ReadID struct {
	Session  [16]byte
	Sequence uint64
}

type readRequest struct {
	ID    ReadID
	Query Query
}

// Reply is either authoritative request-bound evidence or explicit unavailability.
type Reply struct {
	ReadID ReadID
	Query  Query
	Proof  Proof
	Err    error
}

// Event owns consensus traffic and completed authoritative application reads.
// Submit acceptance and Applied progress are never transaction acknowledgements.
type Event struct {
	// ReadID is nonzero only for a successfully accepted Read invocation. Replies
	// in the same event may belong to earlier reads; each carries its own ID.
	ReadID  ReadID
	Packets []replica.Packet
	Replies []Reply
}

// Host owns the ONLY Proof constructor. Its mutex serializes application query
// capture with every replica event; callers cannot inject arbitrary ReadResults.
// Embedding transport must authenticate group/peer identity under the crash-only
// model. These opaque in-process capabilities are not Byzantine signatures.
type Host struct {
	closed       bool
	recipients   map[[16]byte]*RecipientHandle
	mu           sync.Mutex
	driver       *replica.Driver
	machine      *Machine
	readSession  [16]byte
	readSequence uint64
}

// OpenHost attaches the protocol to an already initialized durable replica store.
func OpenHost(c Config, s *raftlog.Store, id uint64) (*Host, error) {
	m, err := New(c)
	if err != nil {
		return nil, err
	}
	d, err := replica.Open(replica.Config{ID: id, Store: s, Machine: m})
	if err != nil {
		return nil, err
	}
	h := &Host{driver: d, machine: m, recipients: map[[16]byte]*RecipientHandle{}}
	if _, err := rand.Read(h.readSession[:]); err != nil {
		return nil, err
	}
	return h, nil
}
func (h *Host) event(out replica.Output, err error) (Event, error) {
	if err != nil {
		return Event{}, err
	}
	e := Event{Packets: out.Packets}
	proofBytes := 0
	for _, r := range out.Reads {
		request, err := decode[readRequest](r.Context, 1024)
		if err != nil || request.ID.Session != h.readSession || request.ID.Sequence == 0 || request.ID.Sequence > h.readSequence || r.Index > out.Applied || h.driver.Applied() < r.Index {
			return Event{}, ErrInvalid
		}
		q := request.Query
		v, err := h.machine.answer(q, out.Applied)
		if err == nil {
			b, _ := json.Marshal(v)
			if len(b) > h.machine.config.Limits.CommandBytes-proofBytes {
				err = ErrLimit
			} else {
				proofBytes += len(b)
			}
		}
		reply := Reply{ReadID: request.ID, Query: q, Err: err}
		if err == nil {
			reply.Proof = Proof{view: v, readID: request.ID, question: q}
		}
		e.Replies = append(e.Replies, reply)
	}
	return e, nil
}

// Campaign runs the established Raft election; it creates no application proof.
func (h *Host) Campaign() (Event, error) {
	if h == nil {
		return Event{}, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	o, e := h.driver.Campaign()
	return h.event(o, e)
}

// Tick supplies a logical liveness tick, never a timeout-based abort decision.
func (h *Host) Tick() (Event, error) {
	if h == nil {
		return Event{}, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	o, e := h.driver.Tick()
	return h.event(o, e)
}

// Step delivers owned consensus traffic through the established driver.
func (h *Host) Step(p replica.Packet) (Event, error) {
	if h == nil {
		return Event{}, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	o, e := h.driver.Step(p)
	return h.event(o, e)
}

// Submit proposes an opaque entry. Lost responses must recover by request/Tx ID;
// a returned Event does not assert that the proposal committed.
func (h *Host) Submit(p Proposal) (Event, error) {
	if h == nil {
		return Event{}, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := validateCommand(p.command); err != nil {
		return Event{}, err
	}
	b, err := json.Marshal(p.command)
	if err != nil {
		return Event{}, err
	}
	if len(b) > h.machine.config.Limits.CommandBytes {
		return Event{}, ErrLimit
	}
	o, e := h.driver.Propose(b)
	return h.event(o, e)
}

// Read requests a leader-only, request-bound quorum barrier. No returned Reply
// until Driver.ReadIndex has completed AND the barrier is applied. Quorum loss
// returns/preserves unavailability; a caller cannot assert a quorum boolean.
// Each invocation gets a distinct Event.ReadID, including identical questions.
// Match Reply.ReadID exactly; an older reply cannot satisfy a later invocation.
func (h *Host) Read(q Query) (Event, error) {
	if h == nil {
		return Event{}, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := checkQuery(q); err != nil {
		return Event{}, err
	}
	if h.readSequence == ^uint64(0) {
		return Event{}, ErrLimit
	}
	h.readSequence++
	id := ReadID{Session: h.readSession, Sequence: h.readSequence}
	b, _ := json.Marshal(readRequest{ID: id, Query: q})
	o, e := h.driver.ReadIndex(b)
	out, err := h.event(o, e)
	if err != nil {
		return Event{}, err
	}
	out.ReadID = id
	return out, nil
}

// SaveCheckpoint publishes the complete reducer image at the driver's applied
// position. It does not create a graph cut or reclaim retained metadata.
func (h *Host) SaveCheckpoint() error {
	if h == nil {
		return ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.driver.SaveCheckpoint()
}

// Close releases the replica without silently checkpointing tentative effects.
func (h *Host) Close() error {
	if h == nil {
		return ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	return h.driver.Close()
}

func checkQuery(q Query) error {
	if isAllocationQuery(q.Kind) {
		return checkAllocationQuery(q)
	}
	if q.RecipientID != ([16]byte{}) || q.Incarnation != ([16]byte{}) || q.Nonce != ([16]byte{}) || q.RecipientEpoch != 0 || q.Sequence != 0 || q.Count != 0 {
		return ErrInvalid
	}
	if !utf8.ValidString(q.TxID) {
		return ErrInvalid
	}
	switch q.Kind {
	case Registration, Prepared, Decided:
		if q.TxID == "" || len(q.TxID) > 128 || q.Round != 0 {
			return ErrInvalid
		}
	case Certified:
		if q.Round == 0 || q.TxID != "" {
			return ErrInvalid
		}
	case Collection, Current:
		if q.Round != 0 || q.TxID != "" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func (m *Machine) answer(q Query, index uint64) (View, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := checkQuery(q); err != nil {
		return View{}, err
	}
	if index < m.applied {
		return View{}, ErrUnavailable
	}
	c, s := m.config, m.state
	v := View{Namespace: c.Namespace, Graph: c.Graph, Topology: c.Topology, Group: c.Group, Epoch: c.Epochs[c.Group], Index: index, Kind: q.Kind}
	if isAllocationQuery(q.Kind) {
		return m.allocationAnswer(q, index)
	}
	switch q.Kind {
	case Registration, Decided:
		r := s.Coordinators[q.TxID]
		if r == nil {
			return View{}, ErrUnavailable
		}
		v.Tx = &r.Tx
		if q.Kind == Decided {
			if r.Decision == nil {
				return View{}, ErrPending
			}
			v.Decision = r.Decision
		}
	case Prepared:
		i := s.Intents[q.TxID]
		if i == nil {
			return View{}, ErrUnavailable
		}
		v.Tx = &i.Tx
		v.Vote = &i.Vote
	case Collection:
		v.Floor = max(s.Floor, s.Fence)
		// Every unresolved yes intent must fetch coordinator authority, even outside
		// the requested data scope. Applied local high-water alone is insufficient.
		for _, i := range s.Intents {
			if i.Vote.Yes && !i.Installed {
				v.Pending = append(v.Pending, i.Tx)
			}
		}
		sortTxs(v.Pending)
	case Current:
		x := snapshot(s, mathMaxRound)
		v.State = &x
	case Certified:
		cert, ok := s.Certificates[q.Round]
		if !ok {
			return View{}, ErrPending
		}
		v.Certificate = &cert
	}
	return clone(v), nil
}

// TransferLeader asks the established consensus driver to transfer leadership.
// Application ownership epochs are unchanged; this is not bucket transfer.
func (h *Host) TransferLeader(id uint64) (Event, error) {
	if h == nil {
		return Event{}, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	o, e := h.driver.TransferLeader(id)
	return h.event(o, e)
}

// At serves a retained certificate on leader OR follower after local application
// through its certified log position. This is a historical read, not a new quorum
// assertion; configuration-entry gaps are included in Driver.Applied.
func (h *Host) At(c Cut) (Snapshot, error) {
	if h == nil {
		return Snapshot{}, ErrInvalid
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, p := range c.proofs {
		if p.view.Group == h.machine.config.Group && p.view.Certificate != nil && p.view.Certificate.Index > h.driver.Applied() {
			return Snapshot{}, ErrUnavailable
		}
	}
	return h.machine.At(c)
}
