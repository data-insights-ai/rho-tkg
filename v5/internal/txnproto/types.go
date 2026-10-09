// Package txnproto is a bounded, provisional V2 application protocol. It is not
// a graph engine, production storage layout, transport service, or a V2 verdict.
// Two fixed logical groups use established replicated logs. Metadata never
// expires: finite quotas backpressure new work instead of recycling request IDs.
package txnproto

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

// Protocol sentinels distinguish rejection, pending work and retained limits.
var (
	ErrInvalid     = errors.New("txnproto: invalid input")
	ErrMismatch    = errors.New("txnproto: immutable binding mismatch")
	ErrStale       = errors.New("txnproto: stale graph/topology/epoch")
	ErrRetry       = errors.New("txnproto: validation/conflict; new request required")
	ErrPending     = errors.New("txnproto: unresolved transaction")
	ErrUnavailable = errors.New("txnproto: authoritative evidence unavailable")
	ErrLimit       = errors.New("txnproto: retained resource limit")
)

// Limits are finite correctness-spike limits, not production capacity targets.
// Reservations protect application journal/history admission only. Backing
// log/disk quotas and operational checkpoint/compaction supervision remain
// separate integration obligations; ENOSPC cannot manufacture success.
// JournalBytes bounds retained command bytes; checkpoint encoding adds at most
// 2x that bound plus fixed metadata. Full-state clones/replay are provisional.
type Limits struct{ JournalBytes, Transitions, Transactions, Versions, Certificates, CommandBytes int }

// DefaultLimits retains all metadata and rejects new work when full.
func DefaultLimits() Limits { return Limits{4 << 20, 4096, 256, 4096, 256, 64 << 10} }

// Config pins one graph, topology and both authority epochs. This slice rejects
// stale epochs but does not implement ownership transfer/catalog activation.
type Config struct {
	Namespace      idalloc.GraphID `json:",omitzero"`
	AllocatorOwner [16]byte        `json:",omitzero"`
	AllocatorEpoch uint64          `json:",omitzero"`
	MaxIDBlock     uint64          `json:",omitzero"`
	Graph          string
	Topology       uint64
	Group          uint8
	Epochs         [2]uint64
	Limits         Limits
}

// Read validates revision identity, including tombstones and value ABA.
type Read struct {
	Key     string
	Version uint64
}

// Effect is a scalar mutation; Delete retains a historical tombstone.
type Effect struct {
	Key    string
	Value  int64
	Delete bool
}

// Participant includes the COMPLETE partition read/predicate footprint. The
// mandatory generation check covers absent keys and empty/temporal predicates
// conservatively by invalidating on any partition mutation. This is not a
// validator for an arbitrary graph planner's omitted dependencies.
type Participant struct {
	Group             uint8
	Epoch, Generation uint64
	Reads             []Read
	Effects           []Effect
}

// Tx freezes the request, dependencies, participants and scalar effects.
// IDs are supplied by the caller's separate durable allocator, never minted here.
type Tx struct {
	Namespace    idalloc.GraphID `json:",omitzero"`
	Claim        *IDClaim        `json:",omitempty"`
	Graph        string
	Topology     uint64
	ID, Request  string
	Coordinator  uint8
	Dependency   uint64
	Participants []Participant
}

// Vote binds the complete Tx and this participant's effect/read digest and floor.
type Vote struct {
	Group           uint8
	Epoch           uint64
	Digest, Effects [32]byte
	Floor           uint64
	Yes             bool
	Reason          string
}

// Decision is immutable. Round zero means abort, never an unknown commit.
type Decision struct {
	Commit bool
	Round  uint64
	Digest [32]byte
	Votes  []Vote
	Reason string
}

// Value retains tombstone versions; absent keys have revision zero.
type Value struct {
	Value          int64
	Version, Round uint64
	Deleted        bool
	TxID           string
}

// Snapshot is an owned exact scalar state, excluding deleted values.
type Snapshot struct {
	Values     map[string]Value
	Generation uint64
}

// Query kinds select one exact authoritative application observation.
const (
	Registration = "registration"
	Prepared     = "prepared"
	Decided      = "decided"
	Collection   = "collection"
	Certified    = "certified"
	Current      = "current"
)

// Query identifies the application question. Host.ReadID separately binds each
// invocation; identical questions must never be used as caller correlation IDs.
type Query struct {
	RecipientID, Incarnation, Nonce [16]byte `json:",omitzero"`
	RecipientEpoch, Sequence, Count uint64   `json:",omitzero"`
	Kind, TxID                      string
	Round                           uint64
}

// Certificate covers one fixed participant at a closed round and applied position.
type Certificate struct{ Round, Index uint64 }

// View is owned inspection data. Constructing a View does not construct Proof.
type View struct {
	Namespace    idalloc.GraphID  `json:",omitzero"`
	Allocator    *AllocatorView   `json:",omitempty"`
	Recipient    *RecipientRecord `json:",omitempty"`
	Grant        *GrantWire       `json:",omitempty"`
	Graph        string
	Topology     uint64
	Group        uint8
	Epoch, Index uint64
	Kind         string
	Tx           *Tx
	Vote         *Vote
	Decision     *Decision
	Floor        uint64
	Pending      []Tx
	State        *Snapshot
	Certificate  *Certificate
}

// Proof has no public constructor. Only Host's completed, request-bound
// ReadIndex responses produce it. Serialized proofs in replay are trusted
// crash-fault protocol data, NOT cryptographic or Byzantine quorum certificates.
type Proof struct {
	view     View
	readID   ReadID
	question Query
}

// View returns an owned copy; changing it cannot change the proof.
func (p Proof) View() View { return clone(p.view) }

// Proposal is an opaque provisional application entry, never a durable receipt.
type Proposal struct{ command command }

// Cut retains authoritative per-partition certificates and explicit scope.
// No expiry/reclamation is implemented; quota rejection preserves old cuts.
type Cut struct{ proofs []Proof }

func clone[T any](v T) T    { b, _ := json.Marshal(v); var r T; _ = json.Unmarshal(b, &r); return r }
func digest(v any) [32]byte { b, _ := json.Marshal(v); return sha256.Sum256(b) }
func part(t Tx, g uint8) (Participant, bool) {
	for _, p := range t.Participants {
		if p.Group == g {
			return p, true
		}
	}
	return Participant{}, false
}
func checkConfig(c Config) error {
	if err := checkAllocationConfig(c); err != nil {
		return err
	}
	l := c.Limits
	if !utf8.ValidString(c.Graph) || c.Graph == "" || len(c.Graph) > 128 || c.Topology == 0 || c.Group > 1 || c.Epochs[0] == 0 || c.Epochs[1] == 0 || l.JournalBytes < 8192 || l.JournalBytes > 16<<20 || l.Transitions < 8 || l.Transitions > 16384 || l.Transactions < 1 || l.Transactions > 1024 || l.Versions < 1 || l.Versions > 65536 || l.Certificates < 1 || l.Certificates > 1024 || l.CommandBytes < 1024 || l.CommandBytes > 64<<10 {
		return ErrInvalid
	}
	return nil
}
func validateTx(t Tx) error {
	if !utf8.ValidString(t.Graph) || !utf8.ValidString(t.ID) || !utf8.ValidString(t.Request) || t.Graph == "" || len(t.Graph) > 128 || t.Topology == 0 || t.ID == "" || len(t.ID) > 128 || t.Request == "" || len(t.Request) > 128 || t.Coordinator > 1 || len(t.Participants) < 1 || len(t.Participants) > 2 {
		return ErrInvalid
	}
	found := false
	for i, p := range t.Participants {
		if p.Group > 1 || p.Epoch == 0 || i > 0 && t.Participants[i-1].Group >= p.Group || len(p.Reads) > 128 || len(p.Effects) > 128 {
			return ErrInvalid
		}
		if p.Group == t.Coordinator {
			found = true
		}
		for j, r := range p.Reads {
			if !utf8.ValidString(r.Key) || r.Key == "" || len(r.Key) > 128 || j > 0 && p.Reads[j-1].Key >= r.Key {
				return ErrInvalid
			}
		}
		for j, e := range p.Effects {
			if !utf8.ValidString(e.Key) || e.Key == "" || len(e.Key) > 128 || e.Delete && e.Value != 0 || j > 0 && p.Effects[j-1].Key >= e.Key {
				return ErrInvalid
			}
		}
	}
	if !found {
		return ErrInvalid
	}
	return nil
}

type command struct {
	Alloc         *allocationCommand `json:",omitempty"`
	Version       int
	Kind          string
	Tx            *Tx
	Remote, Prior *View
	Commit        bool
	Round         uint64
}

func proposal(c command) (Proposal, error) {
	c.Version = 1
	b, _ := json.Marshal(c)
	if len(b) > 64<<10 {
		return Proposal{}, ErrLimit
	}
	return Proposal{clone(c)}, nil
}

// Register proposes the immutable coordinator/request binding before prepare.
func Register(t Tx) (Proposal, error) {
	if t.Claim != nil {
		return Proposal{}, ErrInvalid
	}
	if err := validateTx(t); err != nil {
		return Proposal{}, err
	}
	return proposal(command{Kind: "register", Tx: &t})
}

// Local validates, decides and installs a single-participant transaction in ONE
// application entry; no other group/coordinator service participates.
func Local(t Tx) (Proposal, error) {
	if t.Claim != nil {
		return Proposal{}, ErrInvalid
	}
	if err := validateTx(t); err != nil {
		return Proposal{}, err
	}
	if len(t.Participants) != 1 {
		return Proposal{}, ErrInvalid
	}
	return proposal(command{Kind: "local", Tx: &t})
}

// Prepare requires authoritative registration and, for the second participant,
// the preceding yes-vote. Acquisition is ordered; conflicts vote NO immediately.
// Retry means coordinator abort/recovery then a new request, never waiting locks.
func Prepare(reg Proof, prior Proof) (Proposal, error) {
	if reg.view.Kind != Registration || reg.view.Tx == nil {
		return Proposal{}, ErrUnavailable
	}
	var prev *View
	if prior.view.Kind != "" {
		v := prior.View()
		prev = &v
	}
	v := reg.View()
	return proposal(command{Kind: "prepare", Remote: &v, Prior: prev})
}

// RecordVote proposes a participant's authoritative durable vote at its coordinator.
func RecordVote(v Proof) (Proposal, error) {
	if v.view.Kind != Prepared || v.view.Vote == nil {
		return Proposal{}, ErrUnavailable
	}
	r := v.View()
	return proposal(command{Kind: "vote", Remote: &r})
}

// Decide proposes coordinator commit or explicit abort. Timeouts must query and
// recover this record; they must NEVER resolve participants as unilateral abort.
func Decide(reg Proof, commit bool) (Proposal, error) {
	if reg.view.Kind != Registration || reg.view.Tx == nil {
		return Proposal{}, ErrUnavailable
	}
	v := reg.View()
	return proposal(command{Kind: "decide", Remote: &v, Commit: commit})
}

// Resolve installs all local effects atomically from an authoritative decision.
func Resolve(dec Proof) (Proposal, error) {
	if dec.view.Kind != Decided || dec.view.Decision == nil {
		return Proposal{}, ErrUnavailable
	}
	v := dec.View()
	return proposal(command{Kind: "resolve", Remote: &v})
}

// Fence promises later prepare/local allocation above the requested round.
func Fence(round uint64) (Proposal, error) {
	if round == 0 {
		return Proposal{}, ErrInvalid
	}
	return proposal(command{Kind: "fence", Round: round})
}

// Certify additionally requires every eligible prior intent decided and installed.
func Certify(round uint64) (Proposal, error) {
	if round == 0 {
		return Proposal{}, ErrInvalid
	}
	return proposal(command{Kind: "certify", Round: round})
}
