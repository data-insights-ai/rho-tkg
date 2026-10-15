package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeCreateUpdateFamily = "partition-local-create-update"
const nativeCreateUpdateFormat = "native-create-update-workload-v1"

// Set requires the existing relation before Correct checks both live endpoints.
// One atomic plan publishes neither operation when either predicate refuses.
// Ordinary Set retains SEM06; only explicit workload callers opt into this pair.
func nativeScalarGuardedUpdate(ops []graphstate.Operation, set graphstate.Operation) []graphstate.Operation {
	return append(ops, set, graphstate.Operation{Kind: graphstate.Correct, Owner: set.Owner, Life: set.Life, Scope: set.Scope, Present: true})
}

func nativeCreateUpdateCost(s nativeSelectorsSpec) nativeGraphCostSpec {
	return nativeGraphCostSpec{9, s.Facts, s.CrossPercent, s.Writers}
}
func nativeCreateUpdateValidate(s nativeGraphCostSpec) error {
	if s.Version != 9 {
		return errInvalid
	}
	// Common dimension validation only; this creates no feed/profile authority.
	s.Version = 5
	return nativeFeedValidate(s)
}
func nativeCreateUpdateSpecFromEnv() (s nativeSelectorsSpec, err error) {
	body := []byte(os.Getenv("RHO_GRAPH_WORKLOAD_SPEC"))
	if len(body) == 0 || len(body) > 1024 || nativeFeedDecodeJSON(body, &s) != nil || s.Version != 1 || s.Family != nativeCreateUpdateFamily || s.Layout != "kv" && s.Layout != "columns" || nativeCreateUpdateValidate(nativeCreateUpdateCost(s)) != nil {
		return s, errInvalid
	}
	return s, nil
}
func nativeCreateUpdateAdmissionFromEnv(s nativeSelectorsSpec) (nativeSelectorsAdmission, [32]byte, error) {
	if nativeCreateUpdateValidate(nativeCreateUpdateCost(s)) != nil || s.Family != nativeCreateUpdateFamily {
		return nativeSelectorsAdmission{}, [32]byte{}, errInvalid
	}
	return nativeScalarReadAdmissionFromEnv(s, nativeCreateUpdateFamily)
}
func nativeCreateUpdateProcessProfile(s *nativeGraphCostSpec) (bool, error) {
	flag := os.Getenv("RHO_GRAPH_CREATE_UPDATE_PROCESS")
	if flag == "" {
		if s != nil && s.Version == 9 {
			return false, errInvalid
		}
		return false, nil
	}
	if flag != "1" || s == nil || nativeCreateUpdateValidate(*s) != nil || os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") != "1" || os.Getenv("RHO_GRAPH_SOURCE_PROCESS") != "1" {
		return false, errInvalid
	}
	for _, name := range []string{"RHO_GRAPH_SELECTORS_PROCESS", "RHO_GRAPH_FEED_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_NATIVE_FAULT_FAMILY"} {
		if v := os.Getenv(name); v != "" && v != "0" {
			return false, errInvalid
		}
	}
	spec, err := nativeCreateUpdateSpecFromEnv()
	if err != nil || nativeCreateUpdateCost(spec) != *s {
		return false, errors.Join(errInvalid, err)
	}
	_, _, err = nativeCreateUpdateAdmissionFromEnv(spec)
	return err == nil, err
}
func nativeCreateUpdateFrames(s nativeGraphCostSpec) (uint64, error) {
	if err := nativeCreateUpdateValidate(s); err != nil {
		return 0, err
	}
	// Same finite64-extra whole-cell pool and <=16N+3 actual captures. Fixed
	// control4096 funds five read owners, complete raw cohorts and columns.
	return (16*s.Facts+3)*10 + 64*10 + 4096, nil
}
func nativeCreateUpdateProfile(s nativeGraphCostSpec, init *declaredInitCommand, l *raftlog.Limits, p *raftlog.ApplicationPolicy, g *raftlog.ApplicationGenerationLimits) error {
	if err := nativeCreateUpdateValidate(s); err != nil {
		return err
	}
	frames, err := nativeCreateUpdateFrames(s)
	if err != nil {
		return err
	}
	return nativeScalarReadProfile(s, frames, init, l, p, g)
}
func nativeCreateUpdateReadPolicy(s nativeSelectorsSpec, a nativeSelectorsAdmission) (nativeSelectorsReadPolicy, error) {
	if nativeCreateUpdateValidate(nativeCreateUpdateCost(s)) != nil || a.Spec != s || a.Family != nativeCreateUpdateFamily || a.DeadlineUnixNanos <= 0 || a.BudgetSeconds == 0 || a.BudgetSeconds > 365*24*60*60 {
		return nativeSelectorsReadPolicy{}, errInvalid
	}
	frames, err := nativeCreateUpdateFrames(nativeCreateUpdateCost(s))
	if err != nil {
		return nativeSelectorsReadPolicy{}, err
	}
	return nativeScalarReadAllowance(s.Facts, a.BudgetSeconds, a.DeadlineUnixNanos, frames, 5)
}
func nativeCreateUpdateColumnPolicy(h *allocationHost, s nativeSelectorsSpec, a nativeSelectorsAdmission, digest [32]byte) (nativeColumnPolicy, error) {
	if h == nil || h.machine == nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || a.Spec != s || a.Family != nativeCreateUpdateFamily || digest == ([32]byte{}) {
		return nativeColumnPolicy{}, errInvalid
	}
	frames, err := nativeCreateUpdateFrames(nativeCreateUpdateCost(s))
	if err != nil {
		return nativeColumnPolicy{}, err
	}
	return nativeScalarReadColumnPolicy(h, frames, a.BudgetSeconds, digest)
}
func newNativeCreateUpdateSessions(ctx context.Context, c *crossProcessCoordinator, local func(context.Context, graphCutQuery) (graphCutReply, error)) (*nativeSelectorsSessions, error) {
	spec, err := nativeCreateUpdateSpecFromEnv()
	if err != nil {
		return nil, err
	}
	a, digest, err := nativeCreateUpdateAdmissionFromEnv(spec)
	if err != nil {
		return nil, err
	}
	p, err := nativeCreateUpdateReadPolicy(spec, a)
	if err != nil {
		return nil, err
	}
	return newNativeScalarQuerySessions(ctx, c, local, spec, a, digest, p)
}

func nativeCreateUpdateChildEnv(s nativeSelectorsSpec, a nativeSelectorsAdmission, reopen bool) ([]string, error) {
	cost, err := json.Marshal(nativeCreateUpdateCost(s))
	if err != nil {
		return nil, err
	}
	spec, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	transport, err := json.Marshal(a.Transport)
	if err != nil {
		return nil, err
	}
	env := []string{"RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_CREATE_UPDATE_PROCESS=1", "RHO_GRAPH_SELECTORS_PROCESS=", "RHO_GRAPH_FEED_PROCESS=", "RHO_GRAPH_NATIVE_WORKLOAD=", "RHO_GRAPH_DURABLE_COLUMN_PROCESS=", "RHO_GRAPH_EDGE_UNIQUE_PROCESS=", "RHO_GRAPH_RANGE_PROCESS=", "RHO_GRAPH_CONDITIONAL_PROCESS=", "RHO_GRAPH_RETENTION_PROCESS=", "RHO_GRAPH_NATIVE_FAULT_FAMILY=", "RHO_GRAPH_COST_SPEC=" + string(cost), "RHO_GRAPH_WORKLOAD_SPEC=" + string(spec), "RHO_GRAPH_WORKLOAD_RUN=" + a.Run, "RHO_GRAPH_WORKLOAD_SECONDS=" + strconv.FormatUint(a.BudgetSeconds, 10), "RHO_GRAPH_WORKLOAD_DEADLINE=" + strconv.FormatInt(a.DeadlineUnixNanos, 10), "RHO_GRAPH_WORKLOAD_TRANSPORT=" + string(transport)}
	if reopen {
		env = append(env, "RHO_GRAPH_PROCESS_REOPEN=1")
	}
	return env, nil
}
func nativeCreateUpdateAttach(t *testing.T, n *certifiedProcessNetwork, s nativeSelectorsSpec, a nativeSelectorsAdmission) {
	t.Helper()
	frames, err := nativeCreateUpdateFrames(nativeCreateUpdateCost(s))
	if s.Family == nativeImmutableColumnsFamily {
		frames, err = nativeImmutableColumnsFrames(nativeImmutableColumnsCost(s))
	}
	if s.Family == nativeLateCorrectionFamily {
		frames, err = nativeLateCorrectionFrames(nativeLateCorrectionCost(s))
	}
	if err != nil || frames > (1<<41)/1024 || frames > uint64(^uint(0)>>1)/1024 {
		t.Fatal("finite actual source/transport frame grant", err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, a.DeadlineUnixNanos))
	t.Cleanup(cancel)
	transport, err := newNativeWorkloadTransport(ctx, a.Transport, frames*1024)
	if err != nil {
		t.Fatal(err)
	}
	attachGraphProcessMuxesWithWorkload(t, n.all, int(frames*1024), transport) // #nosec G115 -- checked native int bound, numeric grant only.
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
}

type nativeCreateUpdateDriver struct {
	mu        sync.Mutex
	extra     int
	originals uint64
	spec      nativeSelectorsSpec
}

// This fixed-size observation is only a reported census. The independent raw
// original/source checker proves the actual business footprint separately.
func nativeCreateUpdateCount(s nativeSelectorsSpec, phase *nativeCreateUpdatePhase, sequence *uint64, q nativeSelectorsInput, r nativeSelectorsReply) error {
	if phase == nil || phase.Phase != "create" && phase.Phase != "update" || sequence == nil || nativeCreateUpdateValidate(nativeCreateUpdateCost(s)) != nil || q.Action != "scan" || q.Sequence != *sequence+1 || r.Sequence != q.Sequence || len(r.Points) != len(r.Candidates) || len(r.Points) > 16 {
		return errInvalid
	}
	var single, multiple uint64
	for i, entity := range r.Points {
		if !entity.Found || !entity.Effective || entity.Kind != 2 || entity.Type != "edge" || entity.Mode != 1 || entity.Source != 1 || entity.ID < 7 || (entity.ID-7)%2 != 0 || (entity.ID-7)/2 >= s.Facts || entity.EffectiveLife != entity.ID+1 || r.Candidates[i] != entity.ID {
			return errCorrupt
		}
		switch entity.Target {
		case 3:
			single++
		case nativeCreateUpdateCost(s).remoteNode():
			multiple++
		default:
			return errCorrupt
		}
	}
	*sequence = q.Sequence
	phase.SingleParticipant += single
	phase.MultiParticipant += multiple
	phase.ReadParticipants[0] += single + multiple
	phase.ReadParticipants[1] += multiple
	phase.WriteParticipants[0] += single + multiple
	if phase.Phase == "create" {
		phase.WriteParticipants[1] += multiple
	}
	return nil
}

func nativeCreateUpdateCheckPoint(s nativeFeedSpec, p nativeFeedPoint, value int64) error {
	if s.Family == nativeLateCorrectionFamily {
		value = nativeScalarCorrectedValue(nativeGraphCostSpec{11, s.Facts, s.CrossPercent, s.Writers}, value, p.Fact)
	}
	id, life, target, _ := s.cost().edge(p.Fact)
	r := p.Entity
	presence := 4 + p.Fact
	if value == 6 {
		presence += s.Facts
	}
	if p.Fact >= s.Facts || r.ID != id || !r.Found || !r.Effective || r.Kind != 2 || r.Type != "edge" || r.Mode != 1 || r.Source != 1 || r.Target != target || r.EffectiveLife != life || len(r.Labels) != 0 || !r.Presence.Present || r.Presence.Null || r.Presence.Kind != "life" || r.Presence.Handle != life || r.Presence.I64 != 0 || r.Presence.Revision == 0 || r.Presence.Provenance != presence || !r.Property.Present || r.Property.Null || r.Property.Kind != "i64" || r.Property.I64 != value || r.Property.Handle != uint64(value) || r.Property.Revision == 0 || r.Property.Provenance != 4+p.Fact+boolUint(value == 6)*s.Facts {
		return errCorrupt
	}
	return nil
}

func nativeCreateUpdatePresenceRow(s nativeGraphCostSpec, row nativeColumnRow) (int, error) {
	if s.Version != 9 {
		return 0, errCorrupt
	}
	return nativeScalarGuardedPresenceRow(s, row)
}

func (d *nativeCreateUpdateDriver) retry() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.extra >= 64 {
		return errLimit
	}
	d.extra++
	return nil
}
func (d *nativeCreateUpdateDriver) beginOriginal() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.originals >= 16*d.spec.Facts+3 {
		return errLimit
	}
	d.originals++
	return nil
}

// Actual same-six-directory restart reuses the immutable original admission,
// never a replacement deadline or copied root as authority.
func nativeCreateUpdateRestart(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeSelectorsSpec, a nativeSelectorsAdmission, out *json.Encoder) nativeWorkloadTransportReceipt {
	t.Helper()
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal(err)
	}
	var before [6]processResponse
	for id := uint64(1); id <= 6; id++ {
		var err error
		before[id-1], err = nativeGraphCostExchange(ctx, n.all[id], processRequest{op: processStateOp})
		if err != nil {
			t.Fatal(err)
		}
	}
	old := n.all[1].mux.hub
	if nativeGraphCostStop(t, n, 1, out) != 6 {
		t.Fatal("actual old cohort not reaped")
	}
	receipt := old.workload.receipt(old)
	env, err := nativeScalarCreateUpdateChildEnv(s, a, true)
	if err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(n.dir, fmt.Sprint(id)), id, env...)
	}
	nativeCreateUpdateAttach(t, n, s, a)
	for id := uint64(1); id <= 6; id++ {
		restored, err := nativeGraphCostExchange(ctx, n.all[id], processRequest{op: processStateOp})
		if err != nil {
			t.Fatal(err)
		}
		previous := before[id-1]
		if restored.incarnation == previous.incarnation || restored.applied != previous.applied || !bytes.Equal(restored.image, previous.image) || restored.stats[statViews] != 0 || restored.stats[statPinnedBytes] != 0 || restored.stats[statGeneration] != previous.stats[statGeneration] || restored.stats[statCommit] != previous.stats[statCommit] {
			t.Fatal("actual native restored prefix/bank/owners changed")
		}
		b, err := encodeProcessResponse(previous)
		if err != nil {
			t.Fatal(err)
		}
		r, err := encodeProcessResponse(restored)
		if err != nil {
			t.Fatal(err)
		}
		if err := out.Encode(struct {
			Stage            string
			Member           uint64
			Before, Restored []byte
		}{"full-cohort-restore-before-election", id, b, r}); err != nil {
			t.Fatal(err)
		}
	}
	for group, leader := range n.leaders {
		n.groups[group].event(t, leader, processRequest{op: processCampaign})
		n.read(t, leader, observeConfiguration, 0)
	}
	return receipt
}
