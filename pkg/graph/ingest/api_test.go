package ingest

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/core"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
)

// BACKLOG 7g: pkg/graph/ingest had zero direct tests — subapi_zero_value_test.go
// (which covers every OTHER sub-API's nil-receiver ErrNilGraph contract) never
// mentions ingest at all, so the ready()->ErrNilGraph branch of
// NewSession/AppliedSeq/WaitApplied was untested (Rule 1 violation), and the
// forwarding behavior for a real Ops implementation had no direct coverage
// either. This file follows the exact pattern pkg/graph/tier/api_test.go
// already established for the same kind of sub-API (nil-receiver battery +
// spy-backed forwarding battery).

type ingestOpsSpy struct {
	newSessionCalls  int
	appliedSeqCalls  int
	waitAppliedCalls int
	lastOpts         core.IngestOptions
	lastToken        core.SubmitToken
	newSessionErr    error
	waitAppliedErr   error
}

func (o *ingestOpsSpy) NewSession(opts core.IngestOptions) (*core.Session, error) {
	o.newSessionCalls++
	o.lastOpts = opts
	if o.newSessionErr != nil {
		return nil, o.newSessionErr
	}
	return &core.Session{}, nil
}

func (o *ingestOpsSpy) AppliedSeq() uint64 {
	o.appliedSeqCalls++
	return 42
}

func (o *ingestOpsSpy) WaitApplied(token core.SubmitToken) error {
	o.waitAppliedCalls++
	o.lastToken = token
	return o.waitAppliedErr
}

func TestAPINilReceiversReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	if _, err := nilAPI.NewSession(IngestOptions{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil API.NewSession = %v, want ErrNilGraph", err)
	}
	if got := nilAPI.AppliedSeq(); got != 0 {
		t.Fatalf("nil API.AppliedSeq = %d, want 0", got)
	}
	if err := nilAPI.WaitApplied(SubmitToken{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil API.WaitApplied = %v, want ErrNilGraph", err)
	}

	var zero API
	if _, err := zero.NewSession(IngestOptions{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("zero-value API.NewSession = %v, want ErrNilGraph", err)
	}
	if got := zero.AppliedSeq(); got != 0 {
		t.Fatalf("zero-value API.AppliedSeq = %d, want 0", got)
	}
	if err := zero.WaitApplied(SubmitToken{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("zero-value API.WaitApplied = %v, want ErrNilGraph", err)
	}

	// Typed-nil Ops: New's !grapherr.IsNil(ops) guard must also catch a
	// non-nil-interface-holding-nil-pointer Ops value.
	api := New((*ingestOpsSpy)(nil))
	if _, err := api.NewSession(IngestOptions{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil-ops API.NewSession = %v, want ErrNilGraph", err)
	}
}

func TestAPIForwardsEveryMethod(t *testing.T) {
	t.Parallel()

	ops := &ingestOpsSpy{}
	api := New(ops)

	opts := IngestOptions{Sync: true, DeclareLabels: []string{"Person"}}
	if _, err := api.NewSession(opts); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if ops.newSessionCalls != 1 {
		t.Fatalf("NewSession forwarded %d times, want 1", ops.newSessionCalls)
	}
	if !ops.lastOpts.Sync || len(ops.lastOpts.DeclareLabels) != 1 {
		t.Fatalf("NewSession did not forward opts verbatim: %+v", ops.lastOpts)
	}

	if got := api.AppliedSeq(); got != 42 {
		t.Fatalf("AppliedSeq = %d, want 42 (forwarded spy value)", got)
	}
	if ops.appliedSeqCalls != 1 {
		t.Fatalf("AppliedSeq forwarded %d times, want 1", ops.appliedSeqCalls)
	}

	token := SubmitToken{Lane: 3, Seq: 7}
	if err := api.WaitApplied(token); err != nil {
		t.Fatalf("WaitApplied: %v", err)
	}
	if ops.waitAppliedCalls != 1 || ops.lastToken != (core.SubmitToken{Lane: 3, Seq: 7}) {
		t.Fatalf("WaitApplied did not forward token verbatim: calls=%d token=%+v", ops.waitAppliedCalls, ops.lastToken)
	}

	sentinel := errors.New("spy: forced NewSession failure")
	ops.newSessionErr = sentinel
	if _, err := api.NewSession(IngestOptions{}); !errors.Is(err, sentinel) {
		t.Fatalf("NewSession error = %v, want forwarded sentinel", err)
	}
}

// TestSessionNilReceiverReturnsErrNilSession is BACKLOG 7b's actual
// regression proof: ingest.Session is a TYPE ALIAS for core.Session (see
// api.go), not a wrapper struct, so a caller holding a nil *ingest.Session
// reaches core.Session's own nil-receiver guard (lockOpen, and Close's own
// explicit check) directly through the public pkg/graph/ingest surface —
// this sentinel was previously (incorrectly) treated as internal-core-only
// and excluded from pkg/graph/errors.go's re-export.
func TestSessionNilReceiverReturnsErrNilSession(t *testing.T) {
	t.Parallel()

	var nilSession *Session

	if _, err := nilSession.AddNode(nil, nil); !errors.Is(err, core.ErrNilSession) {
		t.Fatalf("nil Session.AddNode = %v, want ErrNilSession — BACKLOG 7b regression", err)
	}
	if err := nilSession.AddNodes(nil, nil, 1); !errors.Is(err, core.ErrNilSession) {
		t.Fatalf("nil Session.AddNodes = %v, want ErrNilSession — BACKLOG 7b regression", err)
	}
	if _, err := nilSession.AddRelationship("KNOWS", nil, nil, nil); !errors.Is(err, core.ErrNilSession) {
		t.Fatalf("nil Session.AddRelationship = %v, want ErrNilSession — BACKLOG 7b regression", err)
	}
	if err := nilSession.UpdateNode(0, nil); !errors.Is(err, core.ErrNilSession) {
		t.Fatalf("nil Session.UpdateNode = %v, want ErrNilSession — BACKLOG 7b regression", err)
	}
	if err := nilSession.UpdateRelationship(0, nil); !errors.Is(err, core.ErrNilSession) {
		t.Fatalf("nil Session.UpdateRelationship = %v, want ErrNilSession — BACKLOG 7b regression", err)
	}
	if err := nilSession.DeleteNode(0); !errors.Is(err, core.ErrNilSession) {
		t.Fatalf("nil Session.DeleteNode = %v, want ErrNilSession — BACKLOG 7b regression", err)
	}
	if err := nilSession.DeleteRelationship(0); !errors.Is(err, core.ErrNilSession) {
		t.Fatalf("nil Session.DeleteRelationship = %v, want ErrNilSession — BACKLOG 7b regression", err)
	}
	if _, err := nilSession.Submit(); !errors.Is(err, core.ErrNilSession) {
		t.Fatalf("nil Session.Submit = %v, want ErrNilSession — BACKLOG 7b regression", err)
	}
	if err := nilSession.Close(); !errors.Is(err, core.ErrNilSession) {
		t.Fatalf("nil Session.Close = %v, want ErrNilSession — BACKLOG 7b regression", err)
	}
	if got := nilSession.Pending(); got != 0 {
		t.Fatalf("nil Session.Pending = %d, want 0 (nil-safe no-error method)", got)
	}
}
