package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type p06Observation struct {
	Cut    lifecycleRawCut
	View   lifecycleView
	Native nativeLifecycleObservation
}
type p06LocalRecord struct {
	Partition, Index       uint64
	Proposal, Outcome, CDC []byte
}
type p06Phase struct {
	Origin, Partition uint64
	Request, Reply    []byte
}
type p06Artifact struct {
	Version                               int
	Coordinator                           uint64
	Commit                                bool
	Original, Manifest, Pending, Decision []byte
	FirstYES                              pairedFaultBoundary
	Processes                             []pairedFaultProcess
	Refused, Closed                       p06LocalRecord
	Changed                               *p06LocalRecord
	Duplicates                            []p06LocalRecord
	Phases                                []p06Phase
	Before, Recovered, Current, Retained  p06Observation
	From, Through                         [2]uint64
	Records                               []lifecycleRawRecord
}

func p06ProcessWire(t *testing.T, response processResponse) []byte {
	t.Helper()
	wire, err := encodeProcessResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}
func p06ReplyBody(t *testing.T, wire []byte) []byte {
	t.Helper()
	reply, err := decodeGraphWireResponse(wire)
	if err != nil || reply.err != nil {
		t.Fatal("actual peer reply", reply.err, err)
	}
	return reply.body
}
func p06SourceProposal(t *testing.T, wire []byte) allocationProposal {
	t.Helper()
	request, err := decodeGraphSourceRequest(wire)
	if err != nil || request.kind != sourceSubmit {
		t.Fatal("source-produced phase request", err)
	}
	p, err := decodeGraphSourceProposal(request.command)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func p06FirstYESGate(f *pairedGraphFaultNetwork, actor uint64) (<-chan pairedFaultBoundary, func()) {
	entered := make(chan pairedFaultBoundary, 1)
	var once sync.Once
	hub := f.n.all[actor].mux.hub
	hub.mu.Lock()
	hub.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		if origin != actor || partition != 3 {
			return nil
		}
		q, err := decodeGraphSourceRequest(request)
		if err != nil || q.kind != sourceSubmit {
			return err
		}
		p, err := decodeGraphSourceProposal(q.command)
		if err != nil || !isCrossPartitionWire(p.wire) {
			return err
		}
		command, _, err := decodeCrossCommand(p.wire, defaultMaterializerLimits())
		if err != nil || command.phase != crossPrepare {
			return err
		}
		once.Do(func() { entered <- pairedFaultBoundary{bytes.Clone(request), bytes.Clone(reply)} })
		<-ctx.Done()
		// The service already completed. Retire the valid reply when the caller
		// cancels; no nonzero transport mask is paired with an existing body.
		return nil
	}
	hub.mu.Unlock()
	return entered, sync.OnceFunc(func() { hub.mu.Lock(); hub.afterRelay = nil; hub.mu.Unlock() })
}
func p06PrepareBoundary(t *testing.T, boundary pairedFaultBoundary, coordinator uint64) (allocationProposal, crossStatus, crossManifest) {
	t.Helper()
	proposal := p06SourceProposal(t, boundary.Request)
	command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
	if err != nil || command.phase != crossPrepare || command.ns.partition != 3 {
		t.Fatal("first native prepare", err)
	}
	arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeCrossManifest(command.manifest, defaultMaterializerLimits(), arena)
	if err != nil || manifest.coordinator != coordinator || len(manifest.parts) != 2 || manifest.parts[0].binding.Partition != 3 || manifest.parts[1].binding.Partition != 8 {
		t.Fatal("actual frozen original manifest", err)
	}
	reply, err := decodeGraphWireResponse(boundary.Reply)
	request, requestErr := decodeGraphSourceRequest(boundary.Request)
	if err != nil || requestErr != nil || reply.err != nil || reply.peer.voter != 1 || reply.peer.scope.partition != 3 || reply.sequence != request.sequence || reply.nonce != request.nonce {
		t.Fatal("first YES authenticated source correlation", err, requestErr)
	}
	vote, err := decodeCrossStatus(reply.body)
	if err != nil || !vote.yes || vote.phase != crossPrepare || vote.tx != manifest.tx || vote.digest != manifest.digest {
		t.Fatal("first actual YES", vote, err)
	}
	return proposal, vote, manifest
}
func p06BindBusiness(t *testing.T, original allocationProposal, manifest crossManifest) {
	t.Helper()
	command, err := decodePartitionGraphCommand(original.wire, defaultMaterializerLimits())
	if err != nil || command.configuration != manifest.configuration || command.declaration != manifest.declaration || command.routing != manifest.routing || command.request.id != manifest.request.id || command.request.ns.graph != manifest.request.ns.graph || command.request.revision.Provenance() != manifest.request.revision.Provenance() || !reflect.DeepEqual(command.request.operations, manifest.request.operations) || !reflect.DeepEqual(command.request.claims, manifest.request.claims) {
		t.Fatal("retained original business/claims/configuration changed", err)
	}
}
func p06CaptureRecovery(f *pairedGraphFaultNetwork, actor uint64, tx [16]byte) func() []p06Phase {
	var mu sync.Mutex
	var phases []p06Phase
	hub := f.n.all[actor].mux.hub
	hub.mu.Lock()
	hub.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		if origin != actor {
			return nil
		}
		q, err := decodeGraphSourceRequest(request)
		if err != nil || q.kind != sourceSubmit {
			return err
		}
		p, err := decodeGraphSourceProposal(q.command)
		if err != nil || !isCrossPartitionWire(p.wire) {
			return err
		}
		command, _, err := decodeCrossCommand(p.wire, defaultMaterializerLimits())
		if err != nil {
			return err
		}
		arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
		if err != nil {
			return err
		}
		manifest, err := decodeCrossManifest(command.manifest, defaultMaterializerLimits(), arena)
		if err != nil || manifest.tx != tx {
			return errors.Join(errInvalid, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(phases) >= 5 {
			return errLimit
		}
		phases = append(phases, p06Phase{origin, partition, bytes.Clone(request), bytes.Clone(reply)})
		return nil
	}
	hub.mu.Unlock()
	return func() []p06Phase {
		hub.mu.Lock()
		hub.afterRelay = nil
		hub.mu.Unlock()
		mu.Lock()
		defer mu.Unlock()
		return append([]p06Phase(nil), phases...)
	}
}
func p06CheckRecoveryPhases(t *testing.T, phases []p06Phase, original allocationProposal, manifest crossManifest, decision crossStatus, commit bool) {
	t.Helper()
	if len(phases) != 5 {
		t.Fatal("not two Prepare/one Decide/two Resolve", len(phases))
	}
	first, _, err := decodeCrossCommand(original.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	for i, phase := range phases {
		proposal := p06SourceProposal(t, phase.Request)
		command, _, err := decodeCrossCommand(proposal.wire, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		want := []crossPhase{crossPrepare, crossPrepare, crossDecide, crossResolve, crossResolve}[i]
		if command.phase != want || !bytes.Equal(command.manifest, first.manifest) {
			t.Fatal("recovery replanned/reordered original")
		}
		status, err := decodeCrossStatus(p06ReplyBody(t, phase.Reply))
		if err != nil || status.tx != manifest.tx || status.digest != manifest.digest || status.phase != want {
			t.Fatal("native original phase reply", err)
		}
		if i == 0 && !status.yes || i == 1 && status.yes != commit || i >= 2 && (status.commit != commit || status.round != decision.round) {
			t.Fatal("recovery votes/decision changed", status)
		}
	}
}
func p06LocalProposal(f *pairedGraphFaultNetwork, actor, kind, owner uint64, data []byte) allocationProposal {
	r := f.n.all[actor].call(f.t, processRequest{op: processCertifiedAction, entity: kind, index: owner, data: data})
	p, err := decodeCertifiedProcessProposal(r.aux)
	if err != nil {
		f.t.Fatal(err)
	}
	return p
}
func p06Submit(f *pairedGraphFaultNetwork, actor uint64, proposal allocationProposal) p06LocalRecord {
	t := f.t
	wire, err := encodeCertifiedProcessProposal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	group := f.n.group(actor)
	prior := group.blocked
	group.blocked = map[uint64]bool{}
	f.mu.Lock()
	for id, down := range f.down {
		group.blocked[id] = down
	}
	f.mu.Unlock()
	defer func() { group.blocked = prior }()
	group.event(t, actor, processRequest{op: processPropose, data: wire})
	f.drain()
	state := f.n.all[actor].call(t, processRequest{op: processStateOp})
	r := f.n.all[actor].call(t, processRequest{op: processEntryRecord, index: state.applied})
	for id, voter := range group.nodes {
		if group.blocked[id] {
			continue
		}
		got := voter.call(t, processRequest{op: processEntryRecord, index: state.applied})
		if !bytes.Equal(got.outcome, r.outcome) || !bytes.Equal(got.changes, r.changes) {
			t.Fatal("original voter record differs", id)
		}
	}
	return p06LocalRecord{proposal.target.partition, state.applied, bytes.Clone(proposal.wire), bytes.Clone(r.outcome), bytes.Clone(r.changes)}
}
func p06SubmitLocal(f *pairedGraphFaultNetwork, actor uint64, proposal allocationProposal, want reason) p06LocalRecord {
	before := f.n.all[actor].call(f.t, processRequest{op: processStateOp})
	r := p06Submit(f, actor, proposal)
	ns, _ := certifiedProcessNamespace(actor)
	o, err := decodePartitionGraphOutcome(r.Outcome, ns)
	if err != nil || o.reason != want || o.disposition != applied || o.hash != sha256.Sum256(proposal.wire) || o.index != r.Index {
		f.t.Fatal("local original outcome", o, err)
	}
	after := f.n.all[actor].call(f.t, processRequest{op: processStateOp})
	if want != reasonNone && (len(r.CDC) != 0 || !bytes.Equal(before.image, after.image)) {
		f.t.Fatal("prepared-lock refusal changed root/CDC")
	}
	if want == reasonNone && len(r.CDC) == 0 {
		f.t.Fatal("local mutation has no complete CDC")
	}
	return r
}
func p06LocalRound(t *testing.T, record p06LocalRecord) uint64 {
	t.Helper()
	actor := uint64(2)
	if record.Partition == 8 {
		actor = 4
	}
	ns, _ := certifiedProcessNamespace(actor)
	o, err := decodePartitionGraphOutcome(record.Outcome, ns)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := decodeTypedChangeEnvelope(record.CDC, o, defaultMaterializerLimits(), true)
	if err != nil || len(logical.groups) != 1 || len(logical.groups[0].Changes) != 1 {
		t.Fatal("actual local revision binding", err)
	}
	return logical.groups[0].Changes[0].After().Revision().ID()
}
func p06ReplayPhase(f *pairedGraphFaultNetwork, proposal allocationProposal, tx [16]byte) p06LocalRecord {
	actor := f.n.leaders[0]
	if proposal.target.partition == 8 {
		actor = f.n.leaders[1]
	}
	before := f.n.all[actor].call(f.t, processRequest{op: processStateOp})
	r := p06Submit(f, actor, proposal)
	status, err := decodeCrossStatus(r.Outcome)
	after := f.n.all[actor].call(f.t, processRequest{op: processStateOp})
	if err != nil || status.tx != tx || status.phase != crossResolve || status.index >= r.Index || len(r.CDC) != 0 || !bytes.Equal(before.image, after.image) {
		f.t.Fatal("phase duplicate lost original or duplicated effect", status, err)
	}
	return r
}
func p06Observe(f *pairedGraphFaultNetwork, actor uint64, mode graphReadMode, cut graphCut) p06Observation {
	handle, actual := f.open(actor, mode, cut)
	raw := nativeLifecycleRead(f.t, f.n, actor, handle, 0)
	f.n.all[actor].call(f.t, processRequest{op: processSourceCertifiedClose, index: handle})
	handle, _ = f.open(actor, graphReadAt, actual)
	raw.Census = nativeLifecycleRead(f.t, f.n, actor, handle, 2).Census
	f.n.all[actor].call(f.t, processRequest{op: processSourceCertifiedClose, index: handle})
	view, err := lifecycleNormalizeView(raw, f.trace.IdentityMapping, f.trace.OperationMapping)
	if err != nil {
		f.t.Fatal(err)
	}
	wire, err := lifecycleRawCertificate(actual)
	if err != nil {
		f.t.Fatal(err)
	}
	return p06Observation{wire, view, raw}
}
func p06AssertView(t *testing.T, got lifecycleView, edge, remoteClosed, closed bool) {
	t.Helper()
	want := literalLifecycleView(2)
	if edge {
		want = literalLifecycleView(3)
	}
	if remoteClosed {
		want.Entities[1].Presence = lifecycleAbsent("node-b-close", 41)
		want.Entities[1].Effective, want.Entities[1].EffectiveLife = false, ""
	}
	if closed {
		want.Entities[0].Presence = lifecycleAbsent("node-close", 41)
		want.Entities[0].Effective, want.Entities[0].EffectiveLife = false, ""
		if edge {
			want.Entities[2].Effective = false
		}
		for i := range want.Postings {
			want.Postings[i].Effective = []string{}
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("exact P06 graph/index view differs\ngot%+v\nwant%+v", got, want)
	}
}
func p06Records(f *pairedGraphFaultNetwork, from, through [2]uint64) []lifecycleRawRecord {
	var records []lifecycleRawRecord
	for group, leader := range f.n.leaders {
		if through[group] < from[group] || through[group]-from[group] > 256 {
			f.t.Fatal("P06 record range exceeds finite cell")
		}
		for index := from[group] + 1; index <= through[group]; index++ {
			r := f.n.all[leader].call(f.t, processRequest{op: processEntryRecord, index: index})
			partition := uint64(3)
			if group == 1 {
				partition = 8
			}
			records = append(records, lifecycleRawRecord{partition, index, bytes.Clone(r.outcome), bytes.Clone(r.changes)})
		}
	}
	return records
}
func p06CheckCDC(t *testing.T, records []lifecycleRawRecord, manifest crossManifest, commit bool, changed *p06LocalRecord, refused, closed p06LocalRecord) {
	t.Helper()
	resolved, groups := 0, 0
	for _, record := range records {
		p06CheckRawRecord(t, record, manifest, refused, changed, closed)
		status, err := decodeCrossStatus(record.Outcome)
		if err != nil || status.tx != manifest.tx || status.phase != crossResolve || status.index != record.Index {
			continue
		}
		resolved++
		if status.commit != commit || status.digest != manifest.digest {
			t.Fatal("original Resolve differs")
		}
		if !commit {
			if len(record.CDC) != 0 {
				t.Fatal("ABORT emitted business CDC")
			}
			continue
		}
		original := outcome{ns: status.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
		logical, err := decodeTypedChangeEnvelope(record.CDC, original, defaultMaterializerLimits(), true)
		if err != nil {
			t.Fatal("complete original Resolve CDC", err)
		}
		groups += len(logical.groups)
		if record.Partition == 8 && (len(logical.groups)+len(logical.entities)+len(logical.lives)+len(logical.values) != 0) {
			t.Fatal("validation-only B participant contains logical effects")
		}
		if record.Partition == 3 {
			if len(logical.groups) != 2 || len(logical.entities) != 1 || len(logical.lives) != 1 || len(logical.values) != 1 || logical.entities[0].ID != 3 || logical.entities[0].Source != 1 || logical.entities[0].Target != 17 || logical.lives[0].Life != 4 || logical.lives[0].SourceLife != 2 || logical.lives[0].TargetLife != 18 || logical.values[0].ID != 5 {
				t.Fatal("complete edge declarations/groups omitted or duplicated")
			}
			value, ok := logical.values[0].Value.Int64Value()
			if !ok || value != 5 {
				t.Fatal("original edge dictionary value changed")
			}
			for i, g := range logical.groups {
				key, value := graphstate.ComponentKey{Owner: 3, Kind: graphstate.Presence}, uint64(4)
				if i == 1 {
					key, value = graphstate.ComponentKey{Owner: 3, Life: 4, Kind: graphstate.ScalarProperty, Name: "p"}, 5
				}
				if g.Key != key || len(g.Changes) != 1 || g.Changes[0].Before().Present() || g.Changes[0].Before().Revision().ID() != 0 || !g.Changes[0].After().Present() || g.Changes[0].After().Value().ID() != value || g.Changes[0].After().Revision().Provenance() != 3 || g.Changes[0].After().Revision().ID() != status.round {
					t.Fatal("original edge before/after changed")
				}
				p06CheckScope(t, g.Changes[0].Scope())
			}
		}
	}
	if resolved != 2 || commit && groups != 2 || !commit && groups != 0 {
		t.Fatal("incomplete original business participant set")
	}
	for _, local := range []p06LocalRecord{closed} {
		ns, _ := certifiedProcessNamespace(2)
		o, err := decodePartitionGraphOutcome(local.Outcome, ns)
		if err != nil {
			t.Fatal(err)
		}
		logical, err := decodeTypedChangeEnvelope(local.CDC, o, defaultMaterializerLimits(), true)
		if err != nil || len(logical.groups) != 1 || len(logical.entities)+len(logical.lives)+len(logical.values) != 0 {
			t.Fatal("endpoint Close complete CDC", err)
		}
		g := logical.groups[0]
		if g.Key != (graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}) || len(g.Changes) != 1 || !g.Changes[0].Before().Present() || g.Changes[0].Before().Value().ID() != 2 || g.Changes[0].Before().Revision().Provenance() != 1 || g.Changes[0].After().Present() || g.Changes[0].After().Revision().Provenance() != 41 {
			t.Fatal("Close lost endpoint life/original before/after")
		}
		p06CheckScope(t, g.Changes[0].Scope())
	}
	if len(refused.CDC) != 0 {
		t.Fatal("refused conflict emitted CDC")
	}
	if changed != nil {
		ns, _ := certifiedProcessNamespace(4)
		o, err := decodePartitionGraphOutcome(changed.Outcome, ns)
		if err != nil {
			t.Fatal(err)
		}
		logical, err := decodeTypedChangeEnvelope(changed.CDC, o, defaultMaterializerLimits(), true)
		if err != nil || len(logical.groups) != 1 || len(logical.entities)+len(logical.lives)+len(logical.values) != 0 {
			t.Fatal("real B mutation complete CDC", err)
		}
		g := logical.groups[0]
		if g.Key != (graphstate.ComponentKey{Owner: 17, Kind: graphstate.Presence}) || len(g.Changes) != 1 || !g.Changes[0].Before().Present() || g.Changes[0].Before().Value().ID() != 18 || g.Changes[0].Before().Revision().Provenance() != 2 || g.Changes[0].After().Present() || g.Changes[0].After().Revision().Provenance() != 41 {
			t.Fatal("B mutation witness differs")
		}
		p06CheckScope(t, g.Changes[0].Scope())
	}
}
func p06CheckScope(t *testing.T, scope temporal.Scope) {
	t.Helper()
	if scope.Kind() != temporal.ScopeAll || scope.Axis().Descriptor() != graphGenesisBinding(t).Axis().Descriptor() {
		t.Fatal("P06 CDC changed temporal scope/axis")
	}
}
func p06CheckRawRecord(t *testing.T, record lifecycleRawRecord, manifest crossManifest, refused p06LocalRecord, changed *p06LocalRecord, closed p06LocalRecord) {
	t.Helper()
	if len(record.Outcome) == 0 {
		if len(record.CDC) != 0 {
			t.Fatal("CDC without actual original outcome")
		}
		// Actual ApplicationRecord reads succeeded for both outputs. This is
		// no-application-output, never a fabricated Raft-noop identity.
		return
	}
	if status, err := decodeCrossStatus(record.Outcome); err == nil {
		if status.tx != manifest.tx || status.digest != manifest.digest || status.ns.partition != record.Partition || status.index > record.Index || len(record.CDC) != 0 && (status.phase != crossResolve || !status.commit || status.index != record.Index) {
			t.Fatal("unknown/nonoriginal cross record")
		}
		return
	}
	if control, err := decodeGraphCutResult(record.Outcome); err == nil {
		if control.index != record.Index || len(record.CDC) != 0 {
			t.Fatal("read control has business CDC/wrong index")
		}
		return
	}
	known := []p06LocalRecord{refused, closed}
	if changed != nil {
		known = append(known, *changed)
	}
	for _, original := range known {
		if original.Partition == record.Partition && original.Index == record.Index && bytes.Equal(original.Outcome, record.Outcome) && bytes.Equal(original.CDC, record.CDC) {
			return
		}
	}
	t.Fatal("unknown local/nonzero record in complete P06 prefix")
}
func p06WriteArtifact(t *testing.T, f *pairedGraphFaultNetwork, artifact p06Artifact) {
	t.Helper()
	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > 4<<20 {
		t.Fatal("P06 artifact exceeds finite cell")
	}
	path := filepath.Join(f.n.dir, "p06-original-recovery.json")
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(append(data, '\n'))
	t.Log("P06 raw source/process/history/CDC artifact", path, "SHA256", hex.EncodeToString(digest[:]))
}
