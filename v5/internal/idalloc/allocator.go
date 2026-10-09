// Package idalloc provides a deterministic ID-block reservation reducer and a
// local cursor. It supplies no storage, consensus, clock, or routing policy.
//
// One graph has one serialized allocation request authority. Authority identifies
// the allocator, not an entity owner or physical location. The embedding protocol
// must durably commit reducer results before granting issuance, fence old issuer
// sessions, and check authority again when committing graph writes. Local minting
// cannot fence a disconnected former authority.
package idalloc

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"sync"
)

var (
	// ErrInvalid rejects malformed input or a nil public pointer.
	ErrInvalid = errors.New("idalloc: invalid input")
	// ErrStaleAuthority rejects a mismatched allocation fencing generation.
	ErrStaleAuthority = errors.New("idalloc: stale authority")
	// ErrExpired means the bounded request receipt is no longer retained.
	ErrExpired = errors.New("idalloc: request receipt expired")
	// ErrRequestOrder requires the exact next serialized request sequence.
	ErrRequestOrder = errors.New("idalloc: request sequence out of order")
	// ErrPayloadMismatch rejects a reused identity with different payload/result.
	ErrPayloadMismatch = errors.New("idalloc: request payload mismatch")
	// ErrExhausted means no further ID/block or authority epoch is representable.
	ErrExhausted = errors.New("idalloc: exhausted")
	// ErrAlreadyReserved denies issuance from a replay or a burned grant attempt.
	ErrAlreadyReserved = errors.New("idalloc: grant unavailable; reserved range must be abandoned")
	// ErrUnknown requires explicit outcome recovery, never an implicit new block.
	ErrUnknown = errors.New("idalloc: reservation outcome unknown")
	// ErrUnreserved is a trusted embedding guarantee: no reservation committed
	// and no accepted/in-flight work can later commit for this attempt. It is
	// not a cryptographic certificate and cannot be inferred from a timeout.
	ErrUnreserved = errors.New("idalloc: attempt definitively unreserved")
	// ErrCorrupt rejects unknown versions, corruption and invalid state images.
	ErrCorrupt = errors.New("idalloc: corrupt checkpoint")
)

// MaxBlockSize bounds a single reservation; blocks require no per-ID storage.
const MaxBlockSize uint64 = 1 << 20

// GraphID qualifies local IDs. Zero GraphID and local ID zero are invalid.
type GraphID [16]byte

// Authority is immutable and distinct from graph identity and record placement.
// Epoch changes require a committed Transfer, not wall time or local election.
type Authority struct {
	owner [16]byte
	epoch uint64
}

// NewAuthority rejects a zero owner identity or epoch.
func NewAuthority(owner [16]byte, epoch uint64) (Authority, error) {
	a := Authority{owner: owner, epoch: epoch}
	if !a.valid() {
		return Authority{}, ErrInvalid
	}
	return a, nil
}

// Owner returns the serialized allocation request authority's identity.
func (a Authority) Owner() [16]byte { return a.owner }

// Epoch returns its nonzero fencing generation.
func (a Authority) Epoch() uint64 { return a.epoch }

func (a Authority) valid() bool { return a.owner != [16]byte{} && a.epoch != 0 }

// Request has a fixed identity (Graph, Authority, Sequence) and payload Count.
// Only the single serialized authority may submit sequences. Each successful
// reservation advances Sequence by one. Retention lasts until the next successful
// reservation or authority transfer. Expired/unknown requests never allocate a
// replacement implicitly; retry the exact request or resolve it externally.
type Request struct {
	Graph     GraphID
	Authority Authority
	Sequence  uint64
	Count     uint64
}

func (r Request) valid() bool {
	return r.Graph != GraphID{} && r.Authority.valid() && r.Sequence != 0 && r.Count != 0 && r.Count <= MaxBlockSize
}

// Digest binds graph, caller authority, epoch, request sequence and count.
func (r Request) Digest() [32]byte {
	var b [56]byte
	copy(b[:16], r.Graph[:])
	copy(b[16:32], r.Authority.owner[:])
	binary.BigEndian.PutUint64(b[32:40], r.Authority.epoch)
	binary.BigEndian.PutUint64(b[40:48], r.Sequence)
	binary.BigEndian.PutUint64(b[48:], r.Count)
	return sha256.Sum256(b[:])
}

// Reservation is a tentative reducer result, NOT a durability acknowledgement
// and NOT a cursor source. Its inclusive range can end at math.MaxUint64.
type Reservation struct {
	Request     Request
	First, Last uint64
}

func (b Reservation) validFor(r Request) bool {
	return r.valid() && b.Request == r && b.First != 0 && b.Last >= b.First && b.Last-b.First == r.Count-1
}

// State is immutable, fixed-size derived replicated state. Its high-water must
// never be reset for an existing graph. Only the latest request receipt is kept.
type State struct {
	graph                                  GraphID
	authority                              Authority
	high, maxBlock, sequence, count, first uint64
	digest                                 [32]byte
}

// View reports allocation metadata, not routing or a durability certificate.
type View struct {
	Graph                             GraphID
	Authority                         Authority
	HighWater, MaxBlock, LastSequence uint64
}

// Inspect returns immutable metadata for recovery and explicit next requests.
func Inspect(s *State) (View, error) {
	if !s.valid() {
		return View{}, ErrInvalid
	}
	return View{Graph: s.graph, Authority: s.authority, HighWater: s.high, MaxBlock: s.maxBlock, LastSequence: s.sequence}, nil
}

// NewState creates the initial reducer state. Initialization of an existing
// graph with missing state is unsafe; recover its checkpoint/log instead.
func NewState(graph GraphID, authority Authority, maxBlock uint64) (State, error) {
	s := State{graph: graph, authority: authority, maxBlock: maxBlock}
	if !s.valid() {
		return State{}, ErrInvalid
	}
	return s, nil
}

func (s *State) valid() bool {
	if s == nil || s.graph == (GraphID{}) || !s.authority.valid() || s.maxBlock == 0 || s.maxBlock > MaxBlockSize {
		return false
	}
	if s.sequence == 0 {
		return s.count == 0 && s.first == 0 && s.digest == ([32]byte{})
	}
	r := Request{Graph: s.graph, Authority: s.authority, Sequence: s.sequence, Count: s.count}
	return s.count <= s.maxBlock && (Reservation{Request: r, First: s.first, Last: s.high}).validFor(r) && s.digest == r.Digest()
}

// Reserve computes a candidate next state and tentative block. replay is true
// only for the latest identical request. Neither path acknowledges durability.
// Commit next state and its log entry atomically under the current authority;
// issuance must wait for the embedding protocol's durable fresh grant.
func Reserve(s *State, r Request) (next State, block Reservation, replay bool, err error) {
	if !s.valid() || !r.valid() || r.Graph != s.graph {
		return State{}, Reservation{}, false, ErrInvalid
	}
	if r.Authority != s.authority {
		return State{}, Reservation{}, false, ErrStaleAuthority
	}
	if r.Sequence < s.sequence {
		return State{}, Reservation{}, false, ErrExpired
	}
	if r.Sequence == s.sequence {
		if r.Digest() != s.digest {
			return State{}, Reservation{}, false, ErrPayloadMismatch
		}
		return *s, Reservation{Request: r, First: s.first, Last: s.high}, true, nil
	}
	if s.sequence == math.MaxUint64 {
		return State{}, Reservation{}, false, ErrExhausted
	}
	if r.Sequence != s.sequence+1 {
		return State{}, Reservation{}, false, ErrRequestOrder
	}
	if r.Count > s.maxBlock {
		return State{}, Reservation{}, false, ErrInvalid
	}
	if r.Count > math.MaxUint64-s.high {
		return State{}, Reservation{}, false, ErrExhausted
	}
	next = *s
	next.first = s.high + 1
	next.high = s.high + r.Count
	next.sequence, next.count, next.digest = r.Sequence, r.Count, r.Digest()
	return next, Reservation{Request: r, First: next.first, Last: next.high}, false, nil
}

// Transfer computes an epoch-fenced candidate handoff. It preserves every
// reserved ID, including abandoned blocks; the receipt expires. The embedding
// protocol must commit the handoff before activating its new issuer. A transfer
// retry with the old expected authority fails closed; recover current state.
func Transfer(s *State, expected, replacement Authority) (State, error) {
	if !s.valid() || !expected.valid() || !replacement.valid() {
		return State{}, ErrInvalid
	}
	if expected != s.authority {
		return State{}, ErrStaleAuthority
	}
	if expected.epoch == math.MaxUint64 {
		return State{}, ErrExhausted
	}
	if replacement.epoch != expected.epoch+1 {
		return State{}, ErrInvalid
	}
	next := *s
	next.authority = replacement
	next.sequence, next.count, next.first, next.digest = 0, 0, 0, [32]byte{}
	return next, nil
}

// Committer is the trusted embedding boundary; it must not reenter its issuer.
// It must serialize Reserve under
// current authority, commit the high-water/receipt with declared durability, and
// deliver replay=false at most ONCE for that request across ALL issuer sessions.
// A persisted response cannot retain replay=false when redelivered. Recovered,
// repeated or uncertain grants must return replay=true or an error (ErrUnknown
// for unknown outcome). ErrUnreserved may accompany a definitive rejection ONLY
// if no reservation committed and no accepted/in-flight work can later commit
// for this attempt. It must reject an old authority acknowledgement arriving
// after handoff. No allocator method can infer quorum durability from a callback.
type Committer func(context.Context, Request) (block Reservation, replay bool, err error)

// Issuer serializes grants in a single active authority session. Do not copy it.
// Recovery discards all cursors and activates a new durably transferred epoch,
// fencing every old session and acknowledgement. Never recreate this issuer in
// the same epoch after crash/unknown outcome. The embedding protocol must fence
// concurrent sessions; there is no exported cursor constructor/restore.
type Issuer struct {
	mu        sync.Mutex
	graph     GraphID
	authority Authority
	attempted uint64
}

// NewIssuer starts a volatile session for a newly durably activated authority
// epoch; it reserves or issues no IDs itself. This function cannot validate the
// external durable activation/single-session guarantee.
func NewIssuer(graph GraphID, authority Authority) (*Issuer, error) {
	if graph == (GraphID{}) || !authority.valid() {
		return nil, ErrInvalid
	}
	return &Issuer{graph: graph, authority: authority}, nil
}

// Acquire burns the attempt before calling commit and opens a cursor only for
// a validated, fresh durable grant. Local cancellation before submission leaves
// it reusable. Only an unambiguous ErrUnreserved releases a submitted attempt;
// other errors/replays burn it. If an unknown attempt did not commit, advancing
// sequence would skip the reducer's next request: recover under a new committed
// epoch instead. A committed unknown block can be explicitly abandoned for the
// next sequence. Acquire never advances/retries requests automatically.
func (i *Issuer) Acquire(ctx context.Context, r Request, commit Committer) (*Cursor, error) {
	if i == nil || ctx == nil || commit == nil || !r.valid() {
		return nil, ErrInvalid
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if r.Graph != i.graph {
		return nil, ErrInvalid
	}
	if r.Authority != i.authority {
		return nil, ErrStaleAuthority
	}
	if r.Sequence <= i.attempted {
		return nil, ErrAlreadyReserved
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	previous := i.attempted
	i.attempted = r.Sequence
	b, replay, err := commit(ctx, r)
	if err != nil {
		ambiguous := errors.Is(err, ErrUnknown) || errors.Is(err, ErrAlreadyReserved) || errors.Is(err, ErrExpired) || errors.Is(err, ErrPayloadMismatch)
		if errors.Is(err, ErrUnreserved) && !ambiguous && !replay && b == (Reservation{}) {
			i.attempted = previous
		}
		return nil, err
	}
	if !b.validFor(r) {
		return nil, ErrPayloadMismatch
	}
	if replay {
		return nil, ErrAlreadyReserved
	}
	return &Cursor{block: b, next: b.First}, nil
}

// Cursor mints from one fresh grant without per-ID remote calls. It is
// concurrency-safe and cannot be copied or restored. Its IDs are opaque 64-bit
// values qualified by Block().Request.Graph. Authority still fences graph
// COMMIT, not Next: an old cursor can mint unique IDs but cannot authorize writes.
type Cursor struct {
	mu    sync.Mutex
	block Reservation
	next  uint64
	done  bool
}

// Next returns a nonzero local ID; exhaustion never wraps to zero.
func (c *Cursor) Next() (uint64, error) {
	if c == nil {
		return 0, ErrInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.block.validFor(c.block.Request) {
		return 0, ErrInvalid
	}
	if c.done {
		return 0, ErrExhausted
	}
	id := c.next
	if id == c.block.Last {
		c.done = true
	} else {
		c.next++
	}
	return id, nil
}

// Block returns the graph-qualified reservation and immutable write fence.
func (c *Cursor) Block() (Reservation, error) {
	if c == nil {
		return Reservation{}, ErrInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.block.validFor(c.block.Request) {
		return Reservation{}, ErrInvalid
	}
	return c.block, nil
}
