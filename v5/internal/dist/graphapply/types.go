// Package graphapply contains the private bounded single-partition materializer
// and allocation/request reducers. It supplies no public Host, historical-read
// transaction door, distributed activation or durability acknowledgement. The
// serialized replica driver installs its one atomic application batch.
package graphapply

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

var (
	errInvalid        = errors.New("graphapply: invalid input")
	errCorrupt        = errors.New("graphapply: corrupt allocation/request record")
	errLimit          = errors.New("graphapply: resource limit")
	errNotInitialized = errors.New("graphapply: ordinary allocation requests unavailable before initialization")
)

type namespace struct {
	graph     idalloc.GraphID
	partition uint64
}

func (n namespace) valid() bool { return n.graph != (idalloc.GraphID{}) && n.partition != 0 }

type requestID [16]byte

// BootstrapAttemptID is intentionally not an ordinary deduplicated transaction.
// A rejected uninitialized attempt writes only a retained outcome envelope and
// is recovered by applied coordinate+attempt ID+payload hash, not by ID alone.
type bootstrapAttemptID [16]byte

type commandKind byte

const (
	initAllocator commandKind = iota + 1
	transferAllocator
	activateRecipient
	reserveGrant
)

type request struct {
	ns                                             namespace
	id                                             requestID
	attempt                                        bootstrapAttemptID
	kind                                           commandKind
	authority, replacement                         idalloc.Authority
	session                                        idalloc.RecipientSession
	expectedEpoch, home, sequence, count, maxBlock uint64
}

func (r request) identity() [16]byte {
	if r.kind == initAllocator || r.kind == initGraph {
		return [16]byte(r.attempt)
	}
	return [16]byte(r.id)
}
func (r request) validate() error {
	if !r.ns.valid() || r.identity() == ([16]byte{}) {
		return errInvalid
	}
	if r.kind == initAllocator {
		if r.id != (requestID{}) {
			return errInvalid
		}
	} else if r.attempt != (bootstrapAttemptID{}) {
		return errInvalid
	}
	switch r.kind {
	case initAllocator:
		if r.authority.Owner() == ([16]byte{}) || r.authority.Epoch() == 0 || r.maxBlock == 0 || r.maxBlock > idalloc.MaxBlockSize {
			return errInvalid
		}
	case transferAllocator:
		if r.authority.Owner() == ([16]byte{}) || r.authority.Epoch() == 0 || r.replacement.Owner() == ([16]byte{}) || r.replacement.Epoch() == 0 {
			return errInvalid
		}
	case activateRecipient:
		if idalloc.ValidateRecipientSession(r.session) != nil || r.home == 0 {
			return errInvalid
		}
	case reserveGrant:
		if r.authority.Owner() == ([16]byte{}) || r.authority.Epoch() == 0 || idalloc.ValidateGrantRequest(r.grantRequest()) != nil {
			return errInvalid
		}
	default:
		return errInvalid
	}
	// Reject irrelevant union fields rather than silently discarding them.
	expected := request{ns: r.ns, id: r.id, attempt: r.attempt, kind: r.kind}
	switch r.kind {
	case initAllocator:
		expected.authority, expected.maxBlock = r.authority, r.maxBlock
	case transferAllocator:
		expected.authority, expected.replacement = r.authority, r.replacement
	case activateRecipient:
		expected.session, expected.expectedEpoch, expected.home = r.session, r.expectedEpoch, r.home
	case reserveGrant:
		expected.authority, expected.session, expected.sequence, expected.count = r.authority, r.session, r.sequence, r.count
	}
	if r != expected {
		return errInvalid
	}
	return nil
}
func (r request) grantRequest() idalloc.GrantRequest {
	return idalloc.GrantRequest{Graph: r.ns.graph, Session: r.session, Sequence: r.sequence, Count: r.count}
}

type disposition byte

const (
	applied disposition = iota + 1 // a computed effect, never cursor/issuance authority
	requestReplay
	grantRecovery
	controlRecovery
)

type reason uint16

const (
	reasonNone reason = iota
	reasonInvalid
	reasonStale
	reasonMismatch
	reasonExhausted
	reasonAlreadyInitialized
	reasonLimit
	reasonReadConflict reason = 8 // 7 remains reserved/invalid.
)

type outcome struct {
	ns          namespace
	kind        commandKind
	identity    [16]byte
	hash        [32]byte
	index       uint64 // retained original request outcome coordinate, not a graph cut
	disposition disposition
	reason      reason
	grantIndex  uint64
	grant       idalloc.Grant // inspection data; no callback/cursor authority
}
type recipientRecord struct {
	session               idalloc.RecipientSession
	home, index, sequence uint64
}
type grantRecord struct {
	grant idalloc.Grant
	index uint64
}

type limits struct{ inputBytes, readRows, readBytes, stageRows, stageBytes, outputBytes int }

func defaultLimits() limits { return limits{512, 8, 8192, 4, 8192, 4096} }
func (l limits) validate() error {
	for _, n := range []int{l.inputBytes, l.readRows, l.readBytes, l.stageRows, l.stageBytes, l.outputBytes} {
		if n < 1 || n > 64<<20 {
			return errInvalid
		}
	}
	if l.readRows > 65536 || l.stageRows > 4096 {
		return errInvalid
	}
	return nil
}

// allocationEffects is private output, never caller KV/Delta admission. It
// preserves the borrowed root image and owns only bounded touched records.
// The outer materializer alone changes the graph root, reserves admission,
// co-composes other validated effects and installs/acknowledges the batch.
type allocationEffects struct {
	base               raftlog.ApplicationRoot
	writes             []raftlog.KV
	outcome            outcome
	envelope           []byte
	changes            []byte
	bootstrapRejection bool
}
