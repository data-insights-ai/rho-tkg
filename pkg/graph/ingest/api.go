// Package ingest is a sub-API accessor for the ADR-0006 ingest pipeline — a
// prepare-parallel / apply-sequential write door that sits beside the
// interactive transaction door (g.Tx()) on the same core. Producer sessions
// validate, build property slices, precompute content hashes, and mint
// snowflake IDs on the caller thread, fully parallel; a single applier
// goroutine drains prepared intents in commit groups and applies each group
// through the tested batch machinery (one c.txMu + c.mu.Lock, one TxFrom stamp,
// one co-committed change-log LSN run, one flush).
//
// The pipeline is the throughput door for insert-dominated workloads. For
// read-modify-write logic (MERGE, velocity read-then-write) keep using g.Tx().
package ingest

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/core"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
)

// IngestOptions configures a producer session (freshness mode, pre-declared
// vocabulary, queue bound). See core.IngestOptions.
type IngestOptions = core.IngestOptions

// SubmitToken is the (lane, seq) watermark a Submit returns; an async reader
// compares it against AppliedSeq / WaitApplied for read-your-writes.
type SubmitToken = core.SubmitToken

// Session is a producer-side prepare handle. See core.Session.
type Session = core.Session

// Ops is the subset of *core.IngestOps the ingest sub-API forwards to.
type Ops interface {
	NewSession(opts core.IngestOptions) (*core.Session, error)
	AppliedSeq() uint64
	WaitApplied(token core.SubmitToken) error
}

// API is the ingest sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs an ingest sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// NewSession creates a producer session. Sessions are goroutine-parallel: each
// prepares on its own caller thread. The first session lazily starts the single
// applier goroutine. Returns ErrReadOnlyReplica on a read-only replica and
// ErrGraphClosed after Close.
func (a *API) NewSession(opts IngestOptions) (*Session, error) {
	ops, err := a.ready()
	if err != nil {
		return nil, err
	}
	return ops.NewSession(opts)
}

// AppliedSeq returns the highest submit-token sequence the applier has PROCESSED
// (attempted to commit) — not necessarily committed: a rejected intent still
// advances it. For a KNOWN-GOOD async write, AppliedSeq() ≥ its SubmitToken.Seq
// is a read-your-writes signal; for a write that can be rejected, use
// WaitApplied (the truth channel).
func (a *API) AppliedSeq() uint64 {
	ops, err := a.ready()
	if err != nil {
		return 0
	}
	return ops.AppliedSeq()
}

// WaitApplied blocks until the applier has PROCESSED the given submit token
// (appliedSeq ≥ token.Seq), then returns that token's apply outcome: nil if it
// committed, or the intent's real apply error if it was REJECTED (e.g.
// ErrUniqueViolation — the async failure truth channel, pruned on read). A zero
// token returns immediately; a pipeline closed before the token is reached
// returns ErrIngestClosed.
func (a *API) WaitApplied(token SubmitToken) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.WaitApplied(token)
}
