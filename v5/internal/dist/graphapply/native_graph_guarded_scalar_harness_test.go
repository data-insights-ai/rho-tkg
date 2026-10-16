package graphapply

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeGuardedFeedFormat = "native-snapshot-feed-guarded-endpoints-workload-v2"
const nativeGuardedSelectorsFormat = "native-snapshot-selectors-guarded-endpoints-workload-v2"

// These are explicit workload agreements, not changes to ordinary Set (SEM06).
// The production semantic contracts are the same real retained-C feed / durable
// column contracts; the immutable admission distinguishes the extra predicate.
func nativeGuardedScalarValidate(s nativeGraphCostSpec) error {
	if s.Version != 13 && s.Version != 14 {
		return errInvalid
	}
	s.Version = 5 // Dimension validation only; this supplies no native authority.
	return nativeFeedValidate(s)
}
func nativeGuardedScalarFrames(s nativeGraphCostSpec) (uint64, error) {
	if err := nativeGuardedScalarValidate(s); err != nil {
		return 0, err
	}
	if s.Version == 13 {
		s.Version = 5
		return nativeFeedFrames(s)
	}
	s.Version = 6
	return nativeSelectorsFrames(s)
}
func nativeGuardedScalarProfile(s nativeGraphCostSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, g *raftlog.ApplicationGenerationLimits) error {
	frames, err := nativeGuardedScalarFrames(s)
	if err != nil {
		return err
	}
	// Correct adds one presence row, not another native entry. The existing
	// per-entry256-write /260-record reservation and aggregate16-row decoder
	// allowance already fund it. Actual row/output/retained bytes remain charged.
	return nativeScalarReadProfile(s, frames, init, l, p, g)
}
func nativeGuardedFeedSpecFromEnv() (s nativeFeedSpec, err error) {
	body := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(body) == 0 || len(body) > 1024 || nativeFeedDecodeJSON(body, &s) != nil || s.Version != 2 || s.Family != nativeFeedFamily || s.Layout != "kv" && s.Layout != "columns" || nativeGuardedScalarValidate(s.cost()) != nil {
		return s, errInvalid
	}
	return s, nil
}
func nativeGuardedSelectorsSpecFromEnv() (s nativeSelectorsSpec, err error) {
	body := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(body) == 0 || len(body) > 1024 || nativeFeedDecodeJSON(body, &s) != nil || s.Version != 2 || s.Family != nativeSelectorsFamily || s.Layout != "kv" && s.Layout != "columns" || nativeGuardedScalarValidate(s.cost()) != nil {
		return s, errInvalid
	}
	return s, nil
}
func nativeGuardedFeedAdmissionFromEnv(s nativeFeedSpec) (nativeFeedAdmission, [32]byte, error) {
	if s.Version != 2 || s.Family != nativeFeedFamily || nativeGuardedScalarValidate(s.cost()) != nil {
		return nativeFeedAdmission{}, [32]byte{}, errInvalid
	}
	return nativeScalarFeedAdmissionFromEnv(s)
}
func nativeGuardedSelectorsAdmissionFromEnv(s nativeSelectorsSpec) (nativeSelectorsAdmission, [32]byte, error) {
	if s.Version != 2 || s.Family != nativeSelectorsFamily || nativeGuardedScalarValidate(s.cost()) != nil {
		return nativeSelectorsAdmission{}, [32]byte{}, errInvalid
	}
	return nativeScalarReadAdmissionFromEnv(s, nativeSelectorsFamily)
}
func nativeGuardedScalarProcessProfile(s *nativeGraphCostSpec, feed bool) (bool, error) {
	flag, version := "RHO_GRAPH_SELECTORS_PROCESS", 14
	if feed {
		flag, version = "RHO_GRAPH_FEED_PROCESS", 13
	}
	if os.Getenv(flag) != "2" || s == nil || s.Version != version || nativeGuardedScalarValidate(*s) != nil || os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") != "1" || os.Getenv("RHO_GRAPH_SOURCE_PROCESS") != "1" {
		return false, errInvalid
	}
	for _, other := range []string{"RHO_GRAPH_FEED_PROCESS", "RHO_GRAPH_SELECTORS_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_FEED_RETAINED_PROCESS", "RHO_GRAPH_CREATE_UPDATE_PROCESS", "RHO_GRAPH_IMMUTABLE_COLUMNS_PROCESS", "RHO_GRAPH_LATE_CORRECTION_PROCESS", "RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_NATIVE_FAULT_FAMILY"} {
		if value := os.Getenv(other); other != flag && value != "" && value != "0" {
			return false, errInvalid
		}
	}
	if feed {
		spec, err := nativeGuardedFeedSpecFromEnv()
		if err != nil || spec.cost() != *s {
			return false, errors.Join(errInvalid, err)
		}
		_, _, err = nativeGuardedFeedAdmissionFromEnv(spec)
		return err == nil, err
	}
	spec, err := nativeGuardedSelectorsSpecFromEnv()
	if err != nil || spec.cost() != *s {
		return false, errors.Join(errInvalid, err)
	}
	_, _, err = nativeGuardedSelectorsAdmissionFromEnv(spec)
	return err == nil, err
}
func nativeGuardedFeedColumnPolicy(h *allocationHost, s nativeFeedSpec, a nativeFeedAdmission, digest [32]byte) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != retainedFeedSemanticContractID() || s.Version != 2 || s.Family != nativeFeedFamily || a.Spec != s || digest == ([32]byte{}) {
		return nativeColumnPolicy{}, errInvalid
	}
	body, err := json.Marshal(a)
	if err != nil || sha256.Sum256(body) != digest {
		return nativeColumnPolicy{}, errors.Join(errInvalid, err)
	}
	frames, err := nativeGuardedScalarFrames(s.cost())
	if err != nil {
		return nativeColumnPolicy{}, err
	}
	return nativeScalarReadColumnPolicy(h, frames, a.BudgetSeconds, digest)
}
func nativeGuardedSelectorsReadPolicy(s nativeSelectorsSpec, a nativeSelectorsAdmission) (nativeSelectorsReadPolicy, error) {
	if s.Version != 2 || s.Family != nativeSelectorsFamily || a.Spec != s || a.Family != nativeSelectorsFamily {
		return nativeSelectorsReadPolicy{}, errInvalid
	}
	frames, err := nativeGuardedScalarFrames(s.cost())
	if err != nil {
		return nativeSelectorsReadPolicy{}, err
	}
	return nativeScalarReadAllowance(s.Facts, a.BudgetSeconds, a.DeadlineUnixNanos, frames, nativeSelectorsQueries)
}
func nativeGuardedSelectorsColumnPolicy(h *allocationHost, s nativeSelectorsSpec, a nativeSelectorsAdmission, digest [32]byte) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || a.Spec != s || s.Version != 2 || s.Family != nativeSelectorsFamily || digest == ([32]byte{}) {
		return nativeColumnPolicy{}, errInvalid
	}
	body, err := json.Marshal(a)
	if err != nil || sha256.Sum256(body) != digest {
		return nativeColumnPolicy{}, errors.Join(errInvalid, err)
	}
	frames, err := nativeGuardedScalarFrames(s.cost())
	if err != nil {
		return nativeColumnPolicy{}, err
	}
	return nativeScalarReadColumnPolicy(h, frames, a.BudgetSeconds, digest)
}
func newNativeGuardedFeedSessions(ctx context.Context, c *crossProcessCoordinator, cost *nativeGraphCostSpec) (*nativeFeedSessions, error) {
	if ctx == nil || cost == nil || cost.Version != 13 || c == nil || c.host == nil || c.host.machine == nil || c.host.machine.SemanticContractID() != retainedFeedSemanticContractID() {
		return nil, errInvalid
	}
	s, err := nativeGuardedFeedSpecFromEnv()
	if err != nil || s.cost() != *cost {
		return nil, errors.Join(errInvalid, err)
	}
	a, _, err := nativeGuardedFeedAdmissionFromEnv(s)
	if err != nil || [16]byte(c.host.machine.ns.graph) != a.Graph || c.host.machine.declaration.Digest() != a.Declaration || c.host.machine.declaration.TopologyEpoch() != a.Topology {
		return nil, errors.Join(errInvalid, err)
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.UnixNano() != a.DeadlineUnixNanos {
		return nil, errInvalid
	}
	cleanup, cancelCleanup := context.WithDeadline(context.WithoutCancel(ctx), deadline.Add(30*time.Second))
	return &nativeFeedSessions{ctx: ctx, coordinator: c, spec: s, admission: a, cleanup: cleanup, cancelCleanup: cancelCleanup}, nil
}
func newNativeGuardedSelectorsSessions(ctx context.Context, c *crossProcessCoordinator, local func(context.Context, graphCutQuery) (graphCutReply, error)) (*nativeSelectorsSessions, error) {
	s, err := nativeGuardedSelectorsSpecFromEnv()
	if err != nil {
		return nil, err
	}
	a, digest, err := nativeGuardedSelectorsAdmissionFromEnv(s)
	if err != nil {
		return nil, err
	}
	policy, err := nativeGuardedSelectorsReadPolicy(s, a)
	if err != nil {
		return nil, err
	}
	return newNativeScalarQuerySessions(ctx, c, local, s, a, digest, policy)
}
func nativeGuardedScalarCheckPoint(s nativeFeedSpec, p nativeFeedPoint, value int64) error {
	if s.Version != 2 || s.Family != nativeFeedFamily && s.Family != nativeSelectorsFamily {
		return errInvalid
	}
	return nativeCreateUpdateCheckPoint(s, p, value)
}

// These walls surround the actual worker phase, including queue admission,
// immutable retries, replans and worker drain. Setup and diagnostics are apart.
type nativeGuardedScalarPhase struct {
	Phase                            string
	StartNanos, StopNanos, WallNanos int64
	Useful                           uint64
}

func nativeGuardedScalarPhaseStop(phase string, began, started time.Time, useful uint64) nativeGuardedScalarPhase {
	stop := time.Now()
	return nativeGuardedScalarPhase{phase, began.Sub(started).Nanoseconds(), stop.Sub(started).Nanoseconds(), stop.Sub(began).Nanoseconds(), useful}
}

// Only canonical terminal framing may use this fixed invocation lane. Actual
// native terminal work spends the original owner's prepaid pin cleanup pools;
// this classifier creates neither a read arena nor another source allowance.
const nativeGuardedFeedCleanupCalls = 16 * 8

func nativeGuardedFeedTerminalAction(action string) bool {
	return action == "close" || action == "snapshot-close" || action == "release" || action == "expire"
}
func nativeGuardedFeedTerminalControl(r processRequest) bool {
	q, err := nativeFeedInputDecode(r)
	return err == nil && nativeGuardedFeedTerminalAction(q.Action)
}
