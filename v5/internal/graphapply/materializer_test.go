package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type graphFixture struct {
	s   *raftlog.Store
	m   *materializer
	n   namespace
	cfg raftlog.Config
}

func openGraphFixture(t *testing.T) *graphFixture {
	t.Helper()
	cfg := raftlog.Config{Dir: "graphapply", FS: vfs.NewMem(), Create: true, Application: raftlog.DefaultApplicationPolicy(1)}
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	n := codecNamespace()
	if err := graphstore.BootstrapSinglePartition(s, graphstore.Namespace{Graph: [16]byte(n.graph), Partition: n.partition}, 1); err != nil {
		t.Fatal(err)
	}
	m, err := newMaterializer(s, n, 1, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	return &graphFixture{s: s, m: m, n: n, cfg: cfg}
}
func (f *graphFixture) stage(t *testing.T, data []byte) (raftlog.ApplicationBatch, error) {
	t.Helper()
	index, _, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	v, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	base, err := v.Root()
	v.Close()
	if err != nil {
		t.Fatal(err)
	}
	return f.m.Stage(replica.Entry{Index: index + 1, Generation: base.Generation, Term: 2, Data: data}, f.s.ApplicationBudget())
}
func (f *graphFixture) install(t *testing.T, data []byte, b raftlog.ApplicationBatch) uint64 {
	t.Helper()
	index := b.BaseIndex + 1
	entry := &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: owned(data)}
	if err := f.s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(index)}, Entries: []*pb.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	if err := f.s.InstallApplication(index, b); err != nil {
		t.Fatal(err)
	}
	if err := f.m.Restore(index, b.Image); err != nil {
		t.Fatal(err)
	}
	return index
}
func (f *graphFixture) commit(t *testing.T, r graphRequest) (outcome, raftlog.ApplicationBatch) {
	t.Helper()
	data, err := encodeGraphRequest(r, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.stage(t, data)
	if err != nil {
		t.Fatal(err)
	}
	f.install(t, data, b)
	o, err := decodeAnyOutcome(b.Outcome, f.n)
	if err != nil {
		t.Fatal(err)
	}
	return o, b
}
func graphInit(t *testing.T) graphRequest {
	t.Helper()
	r := codecRequests(t)[0]
	return graphRequest{ns: r.ns, kind: initGraph, attempt: r.attempt, authority: r.authority, maxBlock: r.maxBlock}
}
func TestMaterializerOnlyCoInitializesGraphAndAllocator(t *testing.T) {
	f := openGraphFixture(t)
	_, seed, _ := f.s.Checkpoint()
	legacy := codecRequests(t)[0]
	data, err := encodeRequest(legacy, defaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.stage(t, data)
	if err != nil || len(b.Writes) != 0 || !bytes.Equal(seed, b.Image) {
		t.Fatal(b, err)
	}
	f.install(t, data, b)
	o, b := f.commit(t, graphInit(t))
	if o.reason != reasonNone || len(b.Writes) < 6 {
		t.Fatal(o, len(b.Writes))
	}
	root, err := graphstore.DecodeRoot(b.Image)
	if err != nil || root.SemanticEpoch() != 1 {
		t.Fatal(root, err)
	}
	cdc, err := decodeChangeEnvelope(b.Changes, o, f.m.limits)
	if err != nil || !cdc.initialized || cdc.topology != 1 || cdc.schema != 1 {
		t.Fatal(cdc, err)
	}
	replay, replayed := f.commit(t, graphInit(t))
	if replay.disposition != requestReplay || replay.index != o.index || len(replayed.Writes) != 0 || len(replayed.Changes) != 0 || !bytes.Equal(replayed.Image, b.Image) {
		t.Fatal(replay, replayed)
	}
}
func TestGraphCodecMetadataAllowancesCoverActualTypedResults(t *testing.T) {
	for _, v := range []struct {
		name      string
		size      uintptr
		allowance int
	}{{"request", unsafe.Sizeof(graphRequest{}), graphRequestMetadataBytes}, {"operation", unsafe.Sizeof(graphCodecRequest(t).operations[0]), operationMetadataBytes}, {"claim", unsafe.Sizeof(freshBinding{}), claimMetadataBytes}, {"result+outcome", unsafe.Sizeof(raftlog.ApplicationBatch{}) + unsafe.Sizeof(outcome{}), graphResultMetadataBytes}} {
		if int(v.size) > v.allowance {
			t.Fatal(v.name, v.size, v.allowance)
		}
	}
}
func TestMaterializerNilAndClosedInputs(t *testing.T) {
	if _, err := newMaterializer(nil, codecNamespace(), 1, defaultMaterializerLimits()); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	var m *materializer
	if err := m.Restore(1, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := m.Stage(replica.Entry{}, raftlog.ApplicationBudget{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}

func (f *graphFixture) control(t *testing.T, r request) (outcome, raftlog.ApplicationBatch) {
	t.Helper()
	wire, err := encodeRequest(r, defaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.stage(t, wire)
	if err != nil {
		t.Fatal(err)
	}
	f.install(t, wire, b)
	o, err := decodeAnyOutcome(b.Outcome, f.n)
	if err != nil {
		t.Fatal(err)
	}
	return o, b
}
func readyGraphFixture(t *testing.T) *graphFixture {
	t.Helper()
	f := openGraphFixture(t)
	init := graphInit(t)
	init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}
	f.commit(t, init)
	f.control(t, codecRequests(t)[2])
	reserve := codecRequests(t)[3]
	reserve.count = 16
	reserve.sequence = 1
	f.control(t, reserve)
	return f
}
func graphProjection(t *testing.T, f *graphFixture, index uint64, id graphstate.EntityID, scope temporal.Scope) graphstate.Projection {
	t.Helper()
	v, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	c, err := graphstore.OpenCatalog(v, graphstore.Namespace{Graph: graphstate.GraphID(f.n.graph), Partition: f.n.partition}, 1, f.m.limits.catalog)
	if err != nil {
		t.Fatal(err)
	}
	p, err := temporal.RationalPosition(scope.Axis(), temporal.RationalInt64(0))
	if err != nil {
		t.Fatal(err)
	}
	out, _, err := graphstore.Project(t.Context(), c, id, p, graphstate.Effective, f.m.limits.graph)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func TestGraphMaterializerDurableCurrentAndHistoricalProjection(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	o, b := f.commit(t, r)
	if o.reason != reasonNone {
		t.Fatal(o)
	}
	oldIndex := o.index
	p := graphProjection(t, f, oldIndex, 11, r.operations[0].Scope)
	if !p.Exists || !p.Active || p.Life != 12 || len(p.Properties) != 1 || p.Properties[0].Name != "answer" {
		t.Fatal(p)
	}
	changes, err := decodeChangeEnvelope(b.Changes, o, f.m.limits)
	if err != nil || len(changes.entities) != 1 || len(changes.lives) != 1 || len(changes.values) != 1 || len(changes.groups) != 2 {
		t.Fatal(changes, err)
	}
	closeRequest := r
	closeRequest.id = requestID{33}
	closeRequest.operations = []graphstate.Operation{{Kind: graphstate.Close, Owner: 11, Life: 12, Scope: r.operations[0].Scope}}
	closeRequest.claims = nil
	closeRequest.revision, _ = state.NewRevision(4, 0)
	closed, closedBatch := f.commit(t, closeRequest)
	if closed.reason != reasonNone {
		t.Fatal(closed)
	}
	historical := graphProjection(t, f, oldIndex, 11, r.operations[0].Scope)
	current := graphProjection(t, f, closed.index, 11, r.operations[0].Scope)
	if !historical.Active || current.Active {
		t.Fatal(historical, current)
	}
	if _, err := newMaterializer(f.s, f.n, 1, f.m.limits); err != nil {
		t.Fatal(err)
	}
	// Reopen actual persistence and repeat both temporal phases.
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := f.cfg
	cfg.Create = false
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.s = s
	f.m, err = newMaterializer(s, f.n, 1, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	historical = graphProjection(t, f, oldIndex, 11, r.operations[0].Scope)
	current = graphProjection(t, f, closed.index, 11, r.operations[0].Scope)
	if !historical.Active || current.Active {
		t.Fatal(historical, current)
	}
	replay, replayed := f.commit(t, r)
	if replay.disposition != requestReplay || replay.index != oldIndex || len(replayed.Writes) != 0 || len(replayed.Changes) != 0 || !bytes.Equal(replayed.Image, closedBatch.Image) {
		t.Fatal(replay, replayed)
	}
}
func TestMaterializerResourceRefusalsAreOperationalAndRetryAfterRelief(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	wire, err := encodeGraphRequest(r, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := f.stage(t, wire)
	if err != nil {
		t.Fatal(err)
	}
	original := f.m.limits
	beforeIndex, beforeImage, _ := f.s.Checkpoint()
	view, err := f.s.ApplicationView(beforeIndex)
	if err != nil {
		t.Fatal(err)
	}
	beforeRow, beforeFound, err := view.Get(t.Context(), outcomeKey(request{ns: r.ns, kind: r.kind, id: r.id}), 1024)
	view.Close()
	if err != nil {
		t.Fatal(err)
	}
	for _, dimension := range []string{"name", "planner-name", "source", "stage", "output", "decode"} {
		t.Run(dimension, func(t *testing.T) {
			f.m.limits = original
			switch dimension {
			case "name":
				f.m.limits.catalog.MaxNameBytes = 1
			case "planner-name":
				f.m.limits.graph.Planner.MaxNameBytes = 1
			case "source":
				f.m.limits.graph.MaxSourceRows = 1
			case "stage":
				f.m.limits.stageRows = 1
			case "output":
				f.m.limits.graph.MaxOutputBytes = 1
			case "decode":
				f.m.limits.commandOwnedBytes = 1
			}
			b, err := f.stage(t, wire)
			expectedSentinel := errLimit
			if dimension == "source" || dimension == "output" {
				expectedSentinel = graphstore.ErrResourceLimit
			}
			if !errors.Is(err, expectedSentinel) || len(b.Writes) != 0 || len(b.Outcome) != 0 || len(b.Image) != 0 || len(b.Changes) != 0 {
				t.Fatal("local resource refusal emitted durable effects", b, err)
			}
			afterIndex, afterImage, err := f.s.Checkpoint()
			if err != nil || afterIndex != beforeIndex || !bytes.Equal(afterImage, beforeImage) {
				t.Fatal("resource refusal changed root", err)
			}
			view, err := f.s.ApplicationView(afterIndex)
			if err != nil {
				t.Fatal(err)
			}
			afterRow, afterFound, err := view.Get(t.Context(), outcomeKey(request{ns: r.ns, kind: r.kind, id: r.id}), 1024)
			view.Close()
			if err != nil || beforeFound != afterFound || !reflect.DeepEqual(beforeRow, afterRow) {
				t.Fatal("resource refusal changed request mapping", err)
			}
			f.m.limits = original
			actual, err := f.stage(t, wire)
			if err != nil || !reflect.DeepEqual(actual, expected) {
				t.Fatal("resource relief changed deterministic effects", err)
			}
		})
	}
	f.m.limits = original
}
func TestSharedControlGraphDedupNeverOverwritesOriginal(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	o, b := f.commit(t, r)
	if o.reason != reasonNone {
		t.Fatal(o)
	}
	control := codecRequests(t)[1]
	control.id = r.id
	rejected, rejectBatch := f.control(t, control)
	if rejected.reason != reasonMismatch || len(rejectBatch.Writes) != 0 || !bytes.Equal(rejectBatch.Image, b.Image) {
		t.Fatal(rejected, rejectBatch)
	}
	if replay, _ := f.commit(t, r); replay.index != o.index || replay.disposition != requestReplay {
		t.Fatal(replay)
	}
	control.id = requestID{42}
	controlOutcome, controlBatch := f.control(t, control)
	if controlOutcome.reason != reasonNone {
		t.Fatal(controlOutcome)
	}
	r.id = control.id
	rejected, rejectBatch = f.commit(t, r)
	if rejected.reason != reasonMismatch || len(rejectBatch.Writes) != 0 || !bytes.Equal(rejectBatch.Image, controlBatch.Image) {
		t.Fatal(rejected, rejectBatch)
	}
	if recovered, _ := f.control(t, control); recovered.index != controlOutcome.index || recovered.disposition != requestReplay {
		t.Fatal(recovered)
	}
	root, err := graphstore.DecodeRoot(controlBatch.Image)
	beforeRoot, _ := graphstore.DecodeRoot(b.Image)
	if err != nil || root.SemanticEpoch() != beforeRoot.SemanticEpoch() || root.EffectDigest() != beforeRoot.EffectDigest() {
		t.Fatal(root, err)
	}
}

func TestActualQualifiedBindingsOnlyAndCachedGrantFences(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	original, created := f.commit(t, r)
	if original.reason != reasonNone {
		t.Fatal(original)
	}
	// Canonical reuse binds no new ValueID. An unused candidate needs no claim.
	reuse := r
	reuse.id = requestID{9}
	reuse.operations = []graphstate.Operation{r.operations[1]}
	reuse.operations[0].ValueID = 999
	reuse.claims = nil
	noOp, noOpBatch := f.commit(t, reuse)
	if noOp.reason != reasonNone || len(noOpBatch.Writes) != 1 || len(noOpBatch.Changes) != 0 || !bytes.Equal(noOpBatch.Image, created.Image) {
		t.Fatal(noOp, noOpBatch)
	}
	reuse.id = requestID{10}
	reuse.revision, _ = state.NewRevision(5, 0)
	changed, changedBatch := f.commit(t, reuse)
	if changed.reason != reasonNone {
		t.Fatal(changed)
	}
	cdc, err := decodeChangeEnvelope(changedBatch.Changes, changed, f.m.limits)
	if err != nil || len(cdc.values) != 0 || len(cdc.groups) != 1 {
		t.Fatal(cdc, err)
	}
	// Rotation of the allocation service does not invalidate cached ranges.
	f.control(t, codecRequests(t)[1])
	create := r
	create.id = requestID{11}
	create.operations = []graphstate.Operation{r.operations[0]}
	create.operations[0].Owner = 14
	create.operations[0].Record.ID = 14
	create.operations[0].Life = 12 // legitimate owner-qualified reuse of numericLife12
	create.claims = []freshBinding{{role: entityBinding, id: 14, grant: r.claims[0].grant}, {role: lifeBinding, owner: 14, id: 12, grant: r.claims[1].grant}}
	accepted, _ := f.commit(t, create)
	if accepted.reason != reasonNone {
		t.Fatal(accepted)
	}
	// Fresh identities still need actual retained grant admission; rejected IDs
	// acquire no qualified catalog record, page reservation or graph digest.
	create.id = requestID{12}
	create.operations[0].Owner = 15
	create.operations[0].Record.ID = 15
	create.claims = nil
	beforeIndex, beforeImage, _ := f.s.Checkpoint()
	rejected, b := f.commit(t, create)
	if rejected.reason != reasonInvalid || len(b.Writes) != 1 || len(b.Changes) != 0 || !bytes.Equal(b.Image, beforeImage) {
		t.Fatal(rejected, b)
	}
	v, err := f.s.ApplicationView(beforeIndex + 1)
	if err != nil {
		t.Fatal(err)
	}
	c, err := graphstore.OpenCatalog(v, graphstore.Namespace{Graph: graphstate.GraphID(f.n.graph), Partition: f.n.partition}, 1, f.m.limits.catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := c.Entity(t.Context(), graphstore.EntityRef{Graph: graphstate.GraphID(f.n.graph), ID: 15}); err != nil || found {
		t.Fatal("rejected identity retained", found, err)
	}
	v.Close()
	// Independently current recipient fences cached grant admission. Exact old
	// request recovery still precedes those fences.
	activate := codecRequests(t)[2]
	activate.id = requestID{13}
	activate.expectedEpoch = 1
	activate.session.Epoch = 2
	activate.session.Incarnation = [16]byte{99}
	f.control(t, activate)
	create.id = requestID{14}
	create.claims = []freshBinding{{role: entityBinding, id: 15, grant: r.claims[0].grant}, {role: lifeBinding, owner: 15, id: 12, grant: r.claims[1].grant}}
	stale, b := f.commit(t, create)
	if stale.reason != reasonStale || len(b.Writes) != 1 || len(b.Changes) != 0 {
		t.Fatal(stale, b)
	}
	replay, _ := f.commit(t, r)
	if replay.index != original.index || replay.disposition != requestReplay {
		t.Fatal(replay)
	}
}
func TestOrderedTypedCDCIncludesSequentialBeforeAfter(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	p, err := temporal.RationalPosition(r.operations[0].Scope.Axis(), temporal.RationalInt64(1))
	if err != nil {
		t.Fatal(err)
	}
	point, err := temporal.Point(p)
	if err != nil {
		t.Fatal(err)
	}
	second := r.operations[1]
	second.Value = graphstate.ScopeValue(point)
	second.ValueID = 14
	r.operations = append(r.operations, second)
	r.claims = append(r.claims, freshBinding{role: valueBinding, id: 14, grant: r.claims[2].grant})
	o, b := f.commit(t, r)
	if o.reason != reasonNone {
		t.Fatal(o)
	}
	cdc, err := decodeChangeEnvelope(b.Changes, o, f.m.limits)
	if err != nil || len(cdc.groups) != 3 || len(cdc.values) != 2 {
		t.Fatal(cdc, err)
	}
	first, secondChange := cdc.groups[1].Changes[0], cdc.groups[2].Changes[0]
	if first.Before().Present() || first.After().Value().ID() != 13 || secondChange.Before().Value().ID() != 13 || secondChange.After().Value().ID() != 14 {
		t.Fatal(first, secondChange)
	}
}
func TestBootstrapMalformedAndLocalLimitsPreserveEligibility(t *testing.T) {
	f := openGraphFixture(t)
	init := graphInit(t)
	wire, err := encodeGraphRequest(init, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	_, seed, _ := f.s.Checkpoint()
	bad := owned(wire)
	bad[len(bad)-1] ^= 1
	b, err := f.stage(t, bad)
	if err != nil || len(b.Writes) != 0 || len(b.Changes) != 0 || !bytes.Equal(b.Image, seed) {
		t.Fatal(b, err)
	}
	f.install(t, bad, b)
	firstIndex := b.BaseIndex + 1
	firstEnvelope := owned(b.Outcome)
	b, err = f.stage(t, bad)
	if err != nil || len(b.Writes) != 0 {
		t.Fatal(b, err)
	}
	f.install(t, bad, b)
	o, err := decodeGraphOutcome(b.Outcome, f.n)
	if err != nil || o.disposition == requestReplay || o.index == firstIndex {
		t.Fatal(o, err)
	}
	original := f.m.limits
	for _, dimension := range []string{"decode", "stage", "output"} {
		f.m.limits = original
		switch dimension {
		case "decode":
			f.m.limits.commandOwnedBytes = 1
		case "stage":
			f.m.limits.stageRows = 1
		case "output":
			f.m.limits.graph.MaxOutputBytes = 1
		}
		b, err = f.stage(t, wire)
		if err == nil || len(b.Writes)+len(b.Outcome)+len(b.Image) != 0 {
			t.Fatal(dimension, b, err)
		}
	}
	f.m.limits = original
	init.attempt = bootstrapAttemptID{44}
	success, _ := f.commit(t, init)
	if success.reason != reasonNone {
		t.Fatal(success)
	}
	old, err := f.s.ApplicationRecord(t.Context(), firstIndex, true, 1024)
	if err != nil || !bytes.Equal(old, firstEnvelope) {
		t.Fatal("old attempt overwritten", err)
	}
}
func TestProductionControlResourceLimitsNeverBecomeOutcomes(t *testing.T) {
	f := readyGraphFixture(t)
	reserve := codecRequests(t)[3]
	reserve.id = requestID{20}
	reserve.sequence = 2
	wire, err := encodeRequest(reserve, defaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	expected, err := f.stage(t, wire)
	if err != nil {
		t.Fatal(err)
	}
	original := f.m.limits
	f.m.limits.stageRows = 1
	b, err := f.stage(t, wire)
	if !errors.Is(err, errLimit) || len(b.Writes)+len(b.Outcome)+len(b.Image) != 0 {
		t.Fatal(b, err)
	}
	f.m.limits = original
	actual, err := f.stage(t, wire)
	if err != nil || !reflect.DeepEqual(actual, expected) {
		t.Fatal(err)
	}
	// A wrapped local resource error takes precedence over a constraint sentinel.
	for _, e := range []error{errors.Join(graphstate.ErrUniqueOverlap, graphstate.ErrResourceLimit), errors.Join(graphstate.ErrInvalidInput, raftlog.ErrLimit), errors.Join(graphstate.ErrTypeMismatch, graphstore.ErrResourceLimit), errors.Join(graphstate.ErrUniqueOverlap, state.ErrResourceLimit), errors.Join(graphstate.ErrOwnerValidity, temporal.ErrResourceLimit), errors.Join(graphstate.ErrLifecycleOverlap, errLimit)} {
		if _, ok := businessGraphReason(e); ok {
			t.Fatal("local budget became decision", e)
		}
	}
}

func TestDriverStagesAndInstallsOneAtomicGraphBatch(t *testing.T) {
	f := openGraphFixture(t)
	d, err := replica.Open(replica.Config{ID: 1, Store: f.s, ApplicationMachine: f.m})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Campaign(); err != nil {
		t.Fatal(err)
	}
	wire, err := encodeGraphRequest(graphInit(t), f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Propose(wire); err != nil {
		t.Fatal(err)
	}
	index, image, err := f.s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	r, err := graphstore.DecodeRoot(image)
	if err != nil || r.SemanticEpoch() != 1 {
		t.Fatal(r, err)
	}
	outcomeBytes, err := f.s.ApplicationRecord(t.Context(), index, true, 1024)
	if err != nil {
		t.Fatal(err)
	}
	o, err := decodeGraphOutcome(outcomeBytes, f.n)
	if err != nil || o.reason != reasonNone {
		t.Fatal(o, err)
	}
	v, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	base, _ := v.Root()
	q := reader{ctx: t.Context(), view: v, ns: f.n, base: base, limits: f.m.limits.allocation}
	if _, found, err := q.allocator(); err != nil || !found {
		t.Fatal(found, err)
	}
	c, _, err := f.m.catalog(v)
	if err != nil {
		t.Fatal(err)
	}
	view, err := graphstore.OpenReadView(t.Context(), c, f.m.limits.graph)
	if err != nil {
		t.Fatal(err)
	}
	view.Close()
}
func TestPhysicalEffectsCannotChangeLogicalDigest(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	index, _, _ := f.s.Checkpoint()
	v, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	base, _ := v.Root()
	c, _, err := f.m.catalog(v)
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphstore.StageOperations(t.Context(), c, r.operations, r.revision, f.m.limits.graph)
	if err != nil {
		t.Fatal(err)
	}
	changes := graphChanges{ns: f.n, entities: g.Delta.Entities, lives: g.Delta.Lives, values: g.Delta.Values, groups: g.Groups}
	wire, err := encodeGraphRequest(r, f.m.limits)
	if err != nil {
		t.Fatal(err)
	}
	o := outcome{ns: f.n, kind: graphOperations, identity: r.identity(), index: index + 1, disposition: applied}
	hash := sha256.Sum256(wire)
	var originalDigest [32]byte
	var originalCDC []byte
	for _, generation := range []uint64{0, 9} {
		for _, physical := range []uint64{1, 1000} {
			for _, ordinal := range []uint64{1, 600} {
				variant := g
				variant.Base = base
				variant.Base.Generation = generation
				variant.Root, _, err = g.Root.ReservePhysical(physical)
				if err != nil {
					t.Fatal(err)
				}
				variant.Writes = make([]raftlog.KV, len(g.Writes))
				copy(variant.Writes, g.Writes)
				found := false
				// Mutate only the concrete value record's physical bucket ordinal in this
				// non-installed private-effect fixture. Stable qualifiedValue13 stays fixed.
				for i, w := range variant.Writes {
					if len(w.Value) >= 44 && bytes.Equal(w.Value[:4], []byte{'G', 'C', 1, 5}) {
						variant.Writes[i].Value = owned(w.Value)
						binary.BigEndian.PutUint64(variant.Writes[i].Value[36:44], ordinal)
						found = true
					}
				}
				if !found {
					t.Fatal("value record fixture missing")
				}
				q := reader{ctx: t.Context(), view: v, ns: f.n, base: variant.Base, limits: f.m.limits.allocation, stageBytes: 128}
				b, err := f.m.finish(&q, o, hash, false, f.s.ApplicationBudget(), &variant, changes)
				if err != nil {
					t.Fatal(err)
				}
				root, err := graphstore.DecodeRoot(b.Image)
				if err != nil {
					t.Fatal(err)
				}
				if originalDigest == ([32]byte{}) {
					originalDigest = root.EffectDigest()
					originalCDC = owned(b.Changes)
				} else if originalDigest != root.EffectDigest() || !bytes.Equal(originalCDC, b.Changes) {
					t.Fatal("physical generation/page/ordinal entered semantic CDC")
				}
			}
		}
	}
}

type crashMaterializer struct {
	m                  *materializer
	mode               string
	target             commandKind
	epoch              uint64
	entered, completed bool
	lastIndex          uint64
	lastKind           commandKind
}

func (c *crashMaterializer) Restore(index uint64, image []byte) error {
	root, err := graphstore.DecodeRoot(image)
	if err != nil {
		return err
	}
	// Installation is already synced by Driver; kill BEFORE volatile publication.
	if c.mode == "installed-before-restore" && root.SemanticEpoch() == c.epoch {
		return materializerKillSelfForCrashTest()
	}
	return c.m.Restore(index, image)
}
func (c *crashMaterializer) Stage(e replica.Entry, b raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	c.lastIndex = e.Index
	c.lastKind = 0
	if len(e.Data) > 4 {
		c.lastKind = commandKind(e.Data[4])
	}
	selected := len(e.Data) > 4 && c.lastKind == c.target
	if selected {
		c.entered = true
		c.completed = false
	}
	if selected && c.mode == "committed-before-stage" {
		return raftlog.ApplicationBatch{}, materializerKillSelfForCrashTest()
	}
	out, err := c.m.Stage(e, b)
	if err != nil {
		return out, fmt.Errorf("materializer Stage: %w; %s", err, c.diagnostics())
	}
	if selected {
		c.completed = true
	}
	if selected && c.mode == "staged-before-install" {
		return raftlog.ApplicationBatch{}, materializerKillSelfForCrashTest()
	}
	return out, nil
}

// Signal delivery may return before OS termination. Do not return an empty
// successful batch to Driver or run cleanup after a successful request. Timer
// sleeps avoid a Go deadlock exit; the parent deadline bounds failure.
func materializerKillSelfForCrashTest() error {
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		return err
	}
	for {
		time.Sleep(time.Hour)
	}
}

// Safe, read-only failure evidence. No retry or state change hides the seam.
func (c *crashMaterializer) diagnostics() string {
	index, image, checkpointErr := c.m.store.Checkpoint()
	usage, usageErr := c.m.store.ApplicationUsage()
	hard, _, hardErr := c.m.store.InitialState()
	return fmt.Sprintf("target=%d entered=%v completed=%v lastEntry=%d kind=%d checkpoint=%d hash=%x checkpointErr=%v generation=%d committed=%d hardErr=%v usage=%+v usageErr=%v", c.target, c.entered, c.completed, c.lastIndex, c.lastKind, index, sha256.Sum256(image), checkpointErr, c.m.store.ApplicationGeneration(), hard.GetCommit(), hardErr, usage, usageErr)
}
func TestMaterializerCrashChild(t *testing.T) {
	mode := os.Getenv("RHO_GRAPH_APPLY_CRASH_MODE")
	if mode == "" {
		t.Skip("subprocess only")
	}
	dir := os.Getenv("RHO_GRAPH_APPLY_CRASH_DIR")
	if dir == "" {
		t.Fatal("missing subprocess directory")
	}
	n := codecNamespace()
	s, err := raftlog.Open(raftlog.Config{Dir: dir, Create: true, Application: raftlog.DefaultApplicationPolicy(1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := graphstore.BootstrapSinglePartition(s, graphstore.Namespace{Graph: graphstate.GraphID(n.graph), Partition: n.partition}, 1); err != nil {
		t.Fatal(err)
	}
	m, err := newMaterializer(s, n, 1, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	target := initGraph
	epoch := uint64(1)
	operations := os.Getenv("RHO_GRAPH_APPLY_CRASH_TARGET") == "operations"
	if operations {
		target = graphOperations
		epoch = 2
	}
	machine := &crashMaterializer{m: m, mode: mode, target: target, epoch: epoch}
	d, err := replica.Open(replica.Config{ID: 1, Store: s, ApplicationMachine: machine})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Campaign(); err != nil {
		t.Fatal(err)
	}
	init := graphInit(t)
	if operations {
		init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}
	}
	wire, err := encodeGraphRequest(init, m.limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Propose(wire); err != nil {
		t.Fatalf("InitGraph Propose: %v; %s", err, machine.diagnostics())
	}
	if operations {
		activate, _ := encodeRequest(codecRequests(t)[2], defaultLimits())
		if _, err := d.Propose(activate); err != nil {
			t.Fatalf("ActivateRecipient Propose: %v; %s", err, machine.diagnostics())
		}
		reserve := codecRequests(t)[3]
		reserve.count = 16
		reserve.sequence = 1
		wire, err = encodeRequest(reserve, defaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Propose(wire); err != nil {
			t.Fatalf("allocation/graph Propose: %v; %s", err, machine.diagnostics())
		}
		wire, err = encodeGraphRequest(graphCodecRequest(t), m.limits)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Propose(wire); err != nil {
			t.Fatalf("allocation/graph Propose: %v; %s", err, machine.diagnostics())
		}
	}
	t.Fatal("SIGKILL seam was not reached")
}
func TestMaterializerCommittedCrashNeverBecomesAbort(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL evidence requires Unix")
	}
	for _, mode := range []string{"committed-before-stage", "staged-before-install", "installed-before-restore"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMaterializerCrashChild$") // #nosec G204 -- fixed current test binary/selector; temp fixture directory only.
			cmd.Env = append(os.Environ(), "RHO_GRAPH_APPLY_CRASH_MODE="+mode, "RHO_GRAPH_APPLY_CRASH_DIR="+dir)
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatal("timeout cannot count as target SIGKILL", ctx.Err(), string(output))
			}
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok {
				t.Fatal(err, string(output))
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("child failed before crash seam", err, string(output))
			}
			s, err := raftlog.Open(raftlog.Config{Dir: dir, Application: raftlog.DefaultApplicationPolicy(1)})
			if err != nil {
				t.Fatal(err)
			}
			n := codecNamespace()
			m, err := newMaterializer(s, n, 1, defaultMaterializerLimits())
			if err != nil {
				t.Fatal(err)
			}
			d, err := replica.Open(replica.Config{ID: 1, Store: s, ApplicationMachine: m})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if _, err := d.Tick(); err != nil {
				t.Fatal(err)
			}
			index, image, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			r, err := graphstore.DecodeRoot(image)
			if err != nil || r.SemanticEpoch() != 1 {
				t.Fatal(index, r, err)
			}
			// The original committed init is accepted once, never replaced by abort or
			// partial graph readiness. Request recovery survives the physical crash.
			originalBytes, err := s.ApplicationRecord(t.Context(), 3, true, 1024)
			if err != nil {
				t.Fatal(err)
			}
			original, err := decodeGraphOutcome(originalBytes, n)
			if err != nil || original.reason != reasonNone || original.index != 3 {
				t.Fatal(original, err)
			}
			v, err := s.ApplicationView(index)
			if err != nil {
				t.Fatal(err)
			}
			base, _ := v.Root()
			q := reader{ctx: t.Context(), view: v, ns: n, base: base, limits: m.limits.allocation}
			if _, found, err := q.allocator(); err != nil || !found {
				t.Fatal(found, err)
			}
			v.Close()
			if _, err := d.Campaign(); err != nil {
				t.Fatal(err)
			}
			wire, _ := encodeGraphRequest(graphInit(t), m.limits)
			if _, err := d.Propose(wire); err != nil {
				t.Fatal(err)
			}
			replayBytes, err := s.ApplicationRecord(t.Context(), d.Applied(), true, 1024)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := decodeGraphOutcome(replayBytes, n)
			if err != nil || replay.disposition != requestReplay || replay.index != original.index || replay.reason != reasonNone {
				t.Fatal(replay, err)
			}
		})
	}
}
func TestCommittedResourceRefusalStopsThenRecoversExactCommand(t *testing.T) {
	f := openGraphFixture(t)
	d, err := replica.Open(replica.Config{ID: 1, Store: f.s, ApplicationMachine: f.m})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Campaign(); err != nil {
		t.Fatal(err)
	}
	f.m.limits.graph.MaxOutputBytes = 1
	wire, _ := encodeGraphRequest(graphInit(t), f.m.limits)
	if _, err := d.Propose(wire); !errors.Is(err, graphstore.ErrResourceLimit) {
		t.Fatal(err)
	}
	index, seed, err := f.s.Checkpoint()
	if err != nil || index != 2 {
		t.Fatal(index, err)
	}
	root, err := graphstore.DecodeRoot(seed)
	if err != nil || root.SemanticEpoch() != 0 {
		t.Fatal(root, err)
	}
	if _, err := d.Tick(); !errors.Is(err, replica.ErrStopped) || !errors.Is(err, graphstore.ErrResourceLimit) {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := f.cfg
	cfg.Create = false
	s, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.s = s
	t.Cleanup(func() { s.Close() })
	m, err := newMaterializer(s, f.n, 1, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := replica.Open(replica.Config{ID: 1, Store: s, ApplicationMachine: m})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if _, err := recovered.Tick(); err != nil {
		t.Fatal(err)
	}
	envelope, err := s.ApplicationRecord(t.Context(), 3, true, 1024)
	if err != nil {
		t.Fatal(err)
	}
	o, err := decodeGraphOutcome(envelope, f.n)
	if err != nil || o.reason != reasonNone || o.index != 3 {
		t.Fatal(o, err)
	}
}

func TestMaterializerConcreteOwnershipAndClosedSentinels(t *testing.T) {
	f := openGraphFixture(t)
	if _, err := newMaterializer(f.s, f.n, 2, defaultMaterializerLimits()); !errors.Is(err, graphstore.ErrStaleOwner) {
		t.Fatal(err)
	}
	if _, err := newMaterializer(f.s, f.n, 0, defaultMaterializerLimits()); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	index, image, _ := f.s.Checkpoint()
	bad := owned(image)
	bad[0] ^= 1
	if err := f.m.Restore(index, bad); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.m.Stage(replica.Entry{Index: index + 1}, f.s.ApplicationBudget()); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
	if err := f.m.Restore(index, image); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}

func TestGraphMutationCrashRecoversFactsOutcomeCDCAndHistory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL evidence requires Unix")
	}
	for _, mode := range []string{"committed-before-stage", "staged-before-install", "installed-before-restore"} {
		t.Run(mode, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMaterializerCrashChild$") // #nosec G204 -- fixed current test binary/selector; temp fixture directory only.
			cmd.Env = append(os.Environ(), "RHO_GRAPH_APPLY_CRASH_MODE="+mode, "RHO_GRAPH_APPLY_CRASH_DIR="+dir, "RHO_GRAPH_APPLY_CRASH_TARGET=operations")
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatal("timeout cannot count as target SIGKILL", ctx.Err(), string(output))
			}
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok {
				t.Fatal(err, string(output))
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal(err, string(output))
			}
			cfg := raftlog.Config{Dir: dir, Application: raftlog.DefaultApplicationPolicy(1)}
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			n := codecNamespace()
			m, err := newMaterializer(s, n, 1, defaultMaterializerLimits())
			if err != nil {
				t.Fatal(err)
			}
			d, err := replica.Open(replica.Config{ID: 1, Store: s, ApplicationMachine: m})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if _, err := d.Tick(); err != nil {
				t.Fatal(err)
			}
			r := graphCodecRequest(t)
			index, image, err := s.Checkpoint()
			if err != nil || index != 6 {
				t.Fatal(index, err)
			}
			root, err := graphstore.DecodeRoot(image)
			if err != nil || root.SemanticEpoch() != 2 {
				t.Fatal(root, err)
			}
			originalBytes, err := s.ApplicationRecord(t.Context(), 6, true, 1024)
			if err != nil {
				t.Fatal(err)
			}
			original, err := decodeGraphOutcome(originalBytes, n)
			if err != nil || original.reason != reasonNone {
				t.Fatal(original, err)
			}
			cdcBytes, err := s.ApplicationRecord(t.Context(), 6, false, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			cdc, err := decodeChangeEnvelope(cdcBytes, original, m.limits)
			if err != nil || len(cdc.entities) != 1 || len(cdc.lives) != 1 || len(cdc.values) != 1 || len(cdc.groups) != 2 {
				t.Fatal(cdc, err)
			}
			f := &graphFixture{s: s, m: m, n: n, cfg: cfg}
			projected := graphProjection(t, f, 6, 11, r.operations[0].Scope)
			if !projected.Exists || !projected.Active || projected.Life != 12 || len(projected.Properties) != 1 || projected.Properties[0].Name != "answer" {
				t.Fatal(projected)
			}
			// Later mutation must not replace the recovered historical answer.
			if _, err := d.Campaign(); err != nil {
				t.Fatal(err)
			}
			closeRequest := r
			closeRequest.id = requestID{33}
			closeRequest.revision, _ = state.NewRevision(4, 0)
			closeRequest.operations = []graphstate.Operation{{Kind: graphstate.Close, Owner: 11, Life: 12, Scope: r.operations[0].Scope}}
			closeRequest.claims = nil
			wire, err := encodeGraphRequest(closeRequest, m.limits)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := d.Propose(wire); err != nil {
				t.Fatal(err)
			}
			closedIndex := d.Applied()
			old := graphProjection(t, f, 6, 11, r.operations[0].Scope)
			now := graphProjection(t, f, closedIndex, 11, r.operations[0].Scope)
			if !old.Active || len(old.Properties) != 1 || now.Active {
				t.Fatal(old, now)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			s2, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			m2, err := newMaterializer(s2, n, 1, defaultMaterializerLimits())
			if err != nil {
				t.Fatal(err)
			}
			d2, err := replica.Open(replica.Config{ID: 1, Store: s2, ApplicationMachine: m2})
			if err != nil {
				t.Fatal(err)
			}
			defer d2.Close()
			f.s, f.m = s2, m2
			old = graphProjection(t, f, 6, 11, r.operations[0].Scope)
			now = graphProjection(t, f, closedIndex, 11, r.operations[0].Scope)
			if !old.Active || now.Active {
				t.Fatal(old, now)
			}
			if _, err := d2.Campaign(); err != nil {
				t.Fatal(err)
			}
			wire, err = encodeGraphRequest(r, m2.limits)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := d2.Propose(wire); err != nil {
				t.Fatal(err)
			}
			replayed, err := s2.ApplicationRecord(t.Context(), d2.Applied(), true, 1024)
			if err != nil {
				t.Fatal(err)
			}
			o, err := decodeGraphOutcome(replayed, n)
			if err != nil || o.disposition != requestReplay || o.index != 6 {
				t.Fatal(o, err)
			}
			current := graphProjection(t, f, d2.Applied(), 11, r.operations[0].Scope)
			if current.Active {
				t.Fatal("retry reapplied recovered creation", current)
			}
			retained, err := s2.ApplicationRecord(t.Context(), 6, false, 1<<20)
			if err != nil || !bytes.Equal(retained, cdcBytes) {
				t.Fatal("original CDC changed", err)
			}
		})
	}
}
func TestCombinedOwnedOutputPreflightsEachNewBuffer(t *testing.T) {
	f := readyGraphFixture(t)
	r := graphCodecRequest(t)
	wire, _ := encodeGraphRequest(r, f.m.limits)
	expected, err := f.stage(t, wire)
	if err != nil {
		t.Fatal(err)
	}
	index, _, _ := f.s.Checkpoint()
	v, err := f.s.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	base, _ := v.Root()
	c, _, err := f.m.catalog(v)
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphstore.StageOperations(t.Context(), c, r.operations, r.revision, f.m.limits.graph)
	if err != nil {
		t.Fatal(err)
	}
	changes := graphChanges{ns: f.n, entities: g.Delta.Entities, lives: g.Delta.Lives, values: g.Delta.Values, groups: g.Groups}
	o := outcome{ns: f.n, kind: graphOperations, identity: r.identity(), index: index + 1, disposition: applied}
	hash := sha256.Sum256(wire)
	metadata := int(unsafe.Sizeof(reader{})) + int(unsafe.Sizeof(allocationEffects{})) + int(unsafe.Sizeof(raftlog.ApplicationBatch{})) + int(unsafe.Sizeof(outcome{})) + int(unsafe.Sizeof(graphChanges{}))
	if metadata > compositionMetadataBytes {
		t.Fatal("composition metadata too small", metadata)
	}
	stage := func(capacity int) (raftlog.ApplicationBatch, error) {
		f.m.limits.outputBytes = capacity
		q := reader{ctx: t.Context(), view: v, ns: f.n, base: base, limits: f.m.limits.allocation, stageBytes: 128}
		return f.m.finish(&q, o, hash, true, f.s.ApplicationBudget(), &g, changes)
	}
	individuallyFits := max(g.OwnedBytes, materializerOutputCost(expected)) + 1
	b, err := stage(individuallyFits)
	if !errors.Is(err, errLimit) || len(b.Writes)+len(b.Outcome)+len(b.Image) != 0 {
		t.Fatal("combined retained layers were not charged", g.OwnedBytes, materializerOutputCost(expected), err)
	}
	lo, hi := individuallyFits, defaultMaterializerLimits().outputBytes
	for lo < hi {
		mid := (lo + hi) / 2
		if _, err := stage(mid); err == nil {
			hi = mid
		} else {
			if !errors.Is(err, errLimit) {
				t.Fatal(err)
			}
			lo = mid + 1
		}
	}
	for _, delta := range []int{-1, 0, 1} {
		b, err := stage(lo + delta)
		if delta < 0 {
			if !errors.Is(err, errLimit) || len(b.Outcome) != 0 {
				t.Fatal(err)
			}
		} else if err != nil || !reflect.DeepEqual(b, expected) {
			t.Fatal(delta, err)
		}
	}
	f.m.limits = defaultMaterializerLimits()
}

func TestRelationshipMaterializationRetainsDeclaredAndEffectiveHistory(t *testing.T) {
	f := openGraphFixture(t)
	init := graphInit(t)
	init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Relationship, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}
	f.commit(t, init)
	f.control(t, codecRequests(t)[2])
	reserve := codecRequests(t)[3]
	reserve.sequence = 1
	reserve.count = 16
	f.control(t, reserve)
	r := graphCodecRequest(t)
	scope := r.operations[0].Scope
	r.operations = []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: scope}, {Kind: graphstate.CreateNode, Owner: 3, Life: 4, Scope: scope}, {Kind: graphstate.CreateRelationship, Owner: 5, Life: 6, Scope: scope, Record: graphstate.EntityRecord{ID: 5, Type: "links", Source: 1, Target: 3, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 4}}, {Kind: graphstate.Set, Owner: 5, Life: 6, Scope: scope, Name: "answer", Value: graphstate.ScopeValue(scope), ValueID: 7}}
	grant := r.claims[0].grant
	r.claims = nil
	for _, id := range []uint64{1, 3, 5} {
		r.claims = append(r.claims, freshBinding{role: entityBinding, id: id, grant: grant})
	}
	for _, life := range []struct {
		owner graphstate.EntityID
		id    uint64
	}{{1, 2}, {3, 4}, {5, 6}} {
		r.claims = append(r.claims, freshBinding{role: lifeBinding, owner: life.owner, id: life.id, grant: grant})
	}
	r.claims = append(r.claims, freshBinding{role: valueBinding, id: 7, grant: grant})
	created, b := f.commit(t, r)
	if created.reason != reasonNone {
		t.Fatal(created)
	}
	cdc, err := decodeChangeEnvelope(b.Changes, created, f.m.limits)
	if err != nil || len(cdc.entities) != 3 || cdc.entities[2].Kind != graphstate.Relationship || cdc.entities[2].Source != 1 || len(cdc.lives) != 3 || cdc.lives[2].SourceLife != 2 {
		t.Fatal(cdc, err)
	}
	closeRequest := r
	closeRequest.id = requestID{50}
	closeRequest.revision, _ = state.NewRevision(4, 0)
	closeRequest.operations = []graphstate.Operation{{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: scope}}
	closeRequest.claims = nil
	closed, _ := f.commit(t, closeRequest)
	if closed.reason != reasonNone {
		t.Fatal(closed)
	}
	old := graphProjection(t, f, created.index, 5, scope)
	current := graphProjection(t, f, closed.index, 5, scope)
	unaffected := graphProjection(t, f, closed.index, 3, scope)
	phantom := graphProjection(t, f, closed.index, 99, scope)
	if !old.Active || old.Record.Kind != graphstate.Relationship || len(old.Properties) != 1 || current.Active || !unaffected.Active || phantom.Exists {
		t.Fatal(old, current, unaffected, phantom)
	}
	v, err := f.s.ApplicationView(closed.index)
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := f.m.catalog(v)
	if err != nil {
		t.Fatal(err)
	}
	p, err := temporal.RationalPosition(scope.Axis(), temporal.RationalInt64(0))
	if err != nil {
		t.Fatal(err)
	}
	declared, _, err := graphstore.Project(t.Context(), c, 5, p, graphstate.Declared, f.m.limits.graph)
	v.Close()
	if err != nil || !declared.Active || len(declared.Properties) != 1 {
		t.Fatal(declared, err)
	}
}

func installIncompleteFixture(t *testing.T, f *graphFixture, b raftlog.ApplicationBatch) {
	t.Helper()
	index := b.BaseIndex + 1
	entry := &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryNormal.Enum()}
	if err := f.s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(index)}, Entries: []*pb.Entry{entry}}); err != nil {
		t.Fatal(err)
	}
	if err := f.s.InstallApplication(index, b); err != nil {
		t.Fatal(err)
	}
}
func TestMaterializerMissingAllocatorAndHiddenBootstrapDataFailClosed(t *testing.T) {
	for _, partial := range []string{"hidden-bootstrap", "full-without-allocator"} {
		t.Run(partial, func(t *testing.T) {
			f := openGraphFixture(t)
			v, err := f.s.ApplicationView(1)
			if err != nil {
				t.Fatal(err)
			}
			base, _ := v.Root()
			b := batchAt(base)
			if partial == "hidden-bootstrap" {
				b.Writes = []raftlog.KV{{Key: []byte("unrelated-hidden-data"), Value: []byte{1}}}
			} else {
				c, _, err := f.m.catalog(v)
				if err != nil {
					t.Fatal(err)
				}
				g, err := graphstore.InitializeGraphIndexes(t.Context(), c, nil, f.m.limits.graph)
				if err != nil {
					t.Fatal(err)
				}
				changes := graphChanges{ns: f.n, initialized: true, topology: 1, schema: 1}
				logical, err := encodeGraphChanges(changes, f.m.limits)
				if err != nil {
					t.Fatal(err)
				}
				root, err := g.Root.AdvanceEffects(graphEffectDigest(g.Root.EffectDigest(), logical))
				if err != nil {
					t.Fatal(err)
				}
				b.Image, err = graphstore.EncodeRoot(root)
				if err != nil {
					t.Fatal(err)
				}
				b.Writes = g.Writes
			}
			v.Close()
			installIncompleteFixture(t, f, b)
			expected := errCorrupt
			if partial == "hidden-bootstrap" {
				expected = raftlog.ErrInvalid
			}
			if _, err := newMaterializer(f.s, f.n, 1, f.m.limits); !errors.Is(err, expected) {
				t.Fatal("constructor invented allocator", err)
			}
			var wire []byte
			if partial == "hidden-bootstrap" {
				wire, _ = encodeGraphRequest(graphInit(t), f.m.limits)
			} else {
				wire, _ = encodeGraphRequest(graphCodecRequest(t), f.m.limits)
			}
			staged, err := f.stage(t, wire)
			if !errors.Is(err, expected) || len(staged.Writes)+len(staged.Outcome)+len(staged.Image) != 0 {
				t.Fatal(staged, err)
			}
		})
	}
}

func TestMaterializerFreshnessFramingAndMinimumBudgetRefusals(t *testing.T) {
	f := readyGraphFixture(t)
	index, image, _ := f.s.Checkpoint()
	r := graphCodecRequest(t)
	wire, _ := encodeGraphRequest(r, f.m.limits)
	budget := f.s.ApplicationBudget()
	for _, position := range []replica.Entry{{Index: index + 2, Data: wire}, {Index: index + 1, Generation: 1, Data: wire}} {
		b, err := f.m.Stage(position, budget)
		if !errors.Is(err, errInvalid) || len(b.Image)+len(b.Outcome)+len(b.Writes) != 0 {
			t.Fatal(b, err)
		}
	}
	foreign := r
	foreign.ns.graph[0] = 9
	otherWire, _ := encodeGraphRequest(foreign, f.m.limits)
	if b, err := f.stage(t, otherWire); !errors.Is(err, errInvalid) || len(b.Outcome) != 0 {
		t.Fatal(b, err)
	}
	malformed := owned(wire)
	malformed[4] = 99
	if b, err := f.stage(t, malformed); !errors.Is(err, errCorrupt) || len(b.Outcome) != 0 {
		t.Fatal(b, err)
	}
	tiny := budget
	tiny.ImageBytes = 1
	if b, err := f.m.Stage(replica.Entry{Index: index + 1}, tiny); !errors.Is(err, errLimit) || len(b.Image)+len(b.Outcome)+len(b.Writes) != 0 {
		t.Fatal(b, err)
	}
	tiny = budget
	tiny.OutcomeBytes = graphOutcomeBytes - 1
	if b, err := f.m.Stage(replica.Entry{Index: index + 1, Data: wire}, tiny); !errors.Is(err, errLimit) || len(b.Outcome)+len(b.Writes) != 0 {
		t.Fatal(b, err)
	}
	after, afterImage, _ := f.s.Checkpoint()
	if after != index || !bytes.Equal(afterImage, image) {
		t.Fatal("operational refusals changed root")
	}
	// A distinct init attempt on the initialized graph is a durable semantic
	// rejection; it cannot replace the original successful attempt mapping.
	later := graphInit(t)
	later.attempt = bootstrapAttemptID{91}
	o, b := f.commit(t, later)
	if o.reason != reasonAlreadyInitialized || len(b.Writes) != 1 || len(b.Changes) != 0 || !bytes.Equal(b.Image, image) {
		t.Fatal(o, b)
	}
	original, _ := f.commit(t, graphRequest{ns: f.n, kind: initGraph, attempt: graphInit(t).attempt, authority: graphInit(t).authority, maxBlock: 16, schemas: []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}})
	if original.disposition != requestReplay || original.reason != reasonNone {
		t.Fatal(original)
	}
}
