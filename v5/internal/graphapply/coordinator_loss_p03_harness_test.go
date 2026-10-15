package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

func p03GateCommittedReply(f *pairedGraphFaultNetwork, actor, partition uint64, campaign ...bool) (<-chan pairedFaultBoundary, func()) {
	if len(campaign) > 1 || len(campaign) == 1 && (!campaign[0] || f.parent == nil) {
		f.t.Fatal("invalid fresh P03 relay ownership")
	}
	entered := make(chan pairedFaultBoundary, 1)
	released := make(chan struct{})
	var first sync.Once
	hub := f.n.all[actor].mux.hub
	hub.mu.Lock()
	prior := hub.afterRelay
	hub.afterRelay = func(ctx context.Context, origin, part uint64, request, reply []byte) error {
		if len(campaign) == 1 && prior != nil {
			if err := prior(ctx, origin, part, request, reply); err != nil {
				return err
			}
		}
		if origin != actor || part != partition || !pairedFaultIsDecide(request) {
			return nil
		}
		if len(campaign) == 1 && (len(request) > nativeProtocolReplyBytes || len(reply) > nativeProtocolReplyBytes-len(request)) {
			return nil // Unmatched diagnostic cannot alter the actual native reply.
		}
		first.Do(func() {
			if len(campaign) == 0 {
				entered <- pairedFaultBoundary{bytes.Clone(request), bytes.Clone(reply)}
				return
			}
			ownedRequest, ownedReply := make([]byte, len(request)), make([]byte, len(reply))
			copy(ownedRequest, request)
			copy(ownedReply, reply)
			entered <- pairedFaultBoundary{ownedRequest, ownedReply}
		})
		select {
		case <-released:
		case <-ctx.Done():
		}
		// The service already produced a valid reply. Cancellation retires the
		// exact original worker; it must not invent an error with that body.
		return nil
	}
	hub.mu.Unlock()
	release := sync.OnceFunc(func() {
		close(released)
		hub.mu.Lock()
		if len(campaign) == 1 {
			hub.afterRelay = prior
		} else {
			hub.afterRelay = nil
		}
		hub.mu.Unlock()
	})
	return entered, release
}

func p03BusinessDigest(p allocationProposal, ns namespace, t *testing.T) [32]byte {
	t.Helper()
	c, err := decodePartitionGraphCommand(p.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	c.request.ns = ns
	// Run's inert business key uses the PGQ1 encoding, independently of the
	// producer's PGQ2 submission frame. It carries no captured read guard.
	c.version = 0
	c.request.revision, err = state.NewRevision(1, c.request.revision.Provenance())
	if err != nil {
		t.Fatal(err)
	}
	business, err := encodePartitionGraphCommand(c, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(business)
}

func p03CommittedDecision(f *pairedGraphFaultNetwork, b pairedFaultBoundary, original allocationProposal, leader, partition uint64) (crossStatus, allocationScope, pairedFaultProcess) {
	f.t.Helper()
	q, err := decodeGraphSourceRequest(b.Request)
	if err != nil || q.kind != sourceSubmit {
		f.t.Fatal("P03 actual service submission", err)
	}
	p, err := decodeGraphSourceProposal(q.command)
	if err != nil {
		f.t.Fatal(err)
	}
	command, _, err := decodeCrossCommand(p.wire, defaultMaterializerLimits())
	if err != nil || command.phase != crossDecide || command.ns.partition != partition {
		f.t.Fatal("P03 actual coordinator Decide command", err, command.phase, command.ns)
	}
	reply, err := decodeGraphWireResponse(b.Reply)
	if err != nil || reply.err != nil || reply.peer.voter != leader || reply.peer.scope != p.target ||
		reply.kind != q.kind || reply.sequence != q.sequence || reply.nonce != q.nonce {
		f.t.Fatal("P03 configured service reply correlation", err, reply.err, reply.peer)
	}
	decision, err := decodeCrossStatus(reply.body)
	if err != nil || decision.phase != crossDecide || !decision.commit || decision.ns != command.ns ||
		decision.round == 0 || decision.digest != sha256.Sum256(command.manifest) {
		f.t.Fatal("P03 actual durable COMMIT outcome", err, decision)
	}
	limits := defaultMaterializerLimits()
	arena, err := graphstate.NewOutputBudget(limits.outputBytes)
	if err != nil {
		f.t.Fatal(err)
	}
	manifest, err := decodeCrossManifest(command.manifest, limits, arena)
	if err != nil || manifest.coordinator != partition || manifest.tx != decision.tx || len(manifest.parts) != 2 {
		f.t.Fatal("P03 exact original committed manifest", err, manifest.coordinator, manifest.tx)
	}
	if err := validateCrossPhaseProofs(command, manifest); err != nil {
		f.t.Fatal("P03 original complete prepare evidence", err)
	}
	business := partitionGraphCommand{configuration: manifest.configuration, declaration: manifest.declaration,
		routing: manifest.routing, request: manifest.request}
	business.request.revision, err = state.NewRevision(1, manifest.request.revision.Provenance())
	if err != nil {
		f.t.Fatal(err)
	}
	wire, err := encodePartitionGraphCommand(business, limits)
	if err != nil || sha256.Sum256(wire) != p03BusinessDigest(original, decision.ns, f.t) {
		f.t.Fatal("P03 changed original business before Decide", err)
	}
	f.packetDrain()
	records := p03AssertDecisionRecords(f, decision, b, true)
	raw, err := json.Marshal(struct {
		Request, Response, Manifest, VoterRecords []byte
	}{b.Request, b.Reply, command.manifest, records})
	if err != nil {
		f.t.Fatal(err)
	}
	result := pairedFaultResult{Coordinator: partition, Request: hex.EncodeToString(manifest.request.id[:]),
		Transaction: hex.EncodeToString(decision.tx[:]), Round: decision.round}
	child := f.n.all[leader]
	event := pairedFaultProcess{EventID: f.event(), Action: "commit", Role: "replica", Member: leader,
		Instance: pairedFaultInstance(leader, child), Volume: filepath.Join(f.n.dir, "1"), Raw: raw, Result: &result}
	if leader == 4 {
		event.Volume = filepath.Join(f.n.dir, "4")
	}
	return decision, p.target, event
}

func p03AssertDecisionRecords(f *pairedGraphFaultNetwork, decision crossStatus, b pairedFaultBoundary, all bool) []byte {
	f.t.Helper()
	reply, err := decodeGraphWireResponse(b.Reply)
	if err != nil {
		f.t.Fatal(err)
	}
	members := []uint64{1, 2, 3}
	if decision.ns.partition == 8 {
		members = []uint64{4, 5, 6}
	}
	var records []struct {
		Member   uint64
		Instance string
		Record   []byte
	}
	for _, id := range members {
		f.mu.Lock()
		down := f.down[id]
		f.mu.Unlock()
		if down && !all {
			continue
		}
		if down {
			f.t.Fatal("P03 missing required coordinator voter", id)
		}
		child := f.n.all[id]
		r := f.call(child, processRequest{op: processEntryRecord, index: decision.index})
		if !bytes.Equal(r.outcome, reply.body) || len(r.changes) != 0 ||
			r.applied < decision.index || r.stats[statCheckpoint] < decision.index || r.stats[statCommit] < decision.index {
			f.t.Fatal("P03 original Decide not durably retained unchanged", id, r.applied, decision.index)
		}
		wire, err := encodeProcessResponse(r)
		if err != nil {
			f.t.Fatal(err)
		}
		records = append(records, struct {
			Member   uint64
			Instance string
			Record   []byte
		}{id, pairedFaultInstance(id, child), wire})
	}
	if len(records) != 3 && (all || len(records) != 2) {
		f.t.Fatal("P03 incomplete coordinator-voter decision census", len(records))
	}
	wire, err := json.Marshal(records)
	if err != nil {
		f.t.Fatal(err)
	}
	return wire
}

// New peers must be unused for this actual origin. Other origins' closed replay
// slots remain intact; the native child reconnect still refuses active owners.
func p03Reconnect(f *pairedGraphFaultNetwork, actor uint64) {
	f.t.Helper()
	f.drain()
	f.packetDrain()
	hub := f.n.all[actor].mux.hub
	for index, partition := range []uint64{3, 8} {
		previous := []uint64{1, 4}[index]
		hub.mu.Lock()
		if route := hub.relayTargets[actor]; route != nil && route[partition] != 0 {
			previous = route[partition]
		}
		hub.mu.Unlock()
		target := f.n.leaders[index]
		if target == previous {
			continue
		}
		r := f.n.all[target].call(f.t, processRequest{op: processPairedConnections, entity: actor})
		cursor := graphCursor{b: r.aux}
		present := cursor.tag()
		cursor.u64() // Closed source lease replay slots are not live owners.
		s := f.n.all[target].call(f.t, processRequest{op: processStateOp})
		if cursor.err != nil || len(cursor.b) != 0 || present != 0 || s.stats[statViews] != 0 ||
			s.stats[statPinnedBytes] != 0 || len(s.handles) != 2 || s.handles[0] != 0 || s.handles[1] != 0 {
			f.t.Fatal("P03 new authenticated peer is used or owns live work", actor, target, present, cursor.err)
		}
	}
	f.n.all[actor].call(f.t, processRequest{op: processPairedReconnect, data: allocationP08Words(f.n.leaders[0], f.n.leaders[1])})
	hub.mu.Lock()
	if hub.relayTargets == nil {
		hub.relayTargets = make(map[uint64]map[uint64]uint64)
	}
	hub.relayTargets[actor] = map[uint64]uint64{3: f.n.leaders[0], 8: f.n.leaders[1]}
	hub.mu.Unlock()
}

// Preserve the shared exporter/model while selecting a live observing origin.
// The older complete helper deliberately fixes origin4 and cannot serve P03/B.
func p03Complete(f *pairedGraphFaultNetwork, observer uint64, name string, p allocationProposal, result crossProcessResult, from [2]uint64, before pairedFaultObservation) pairedFaultStep {
	f.t.Helper()
	command, err := decodePartitionGraphCommand(p.wire, defaultMaterializerLimits())
	if err != nil {
		f.t.Fatal(err)
	}
	op := lifecycleOperationMap{Logical: name, Coordinator: result.decision.ns.partition, Request: hex.EncodeToString(command.request.id[:]),
		Transaction: hex.EncodeToString(result.decision.tx[:]), Round: result.decision.round}
	f.trace.OperationMapping = append(f.trace.OperationMapping, op)
	records := f.records(from)
	after := f.observe(observer, graphReadFresh, graphCut{}, false)
	step := lifecycleStep{Operation: name, Coordinator: "B", Before: before.View, After: after.View, Cut: after.Cut, RawObservation: after.RawObservation}
	if op.Coordinator == 3 {
		step.Coordinator = "A"
	}
	decoded := make([]nativeLifecycleRecord, len(records))
	for index, record := range records {
		decoded[index] = record.nativeLifecycleRecord
	}
	step.LogicalCDC, step.NewEntities, step.NewLives, step.NewValues, err = nativeLifecycleNormalizeRecords(decoded, f.trace.IdentityMapping, f.trace.OperationMapping)
	if err != nil {
		f.t.Fatal(err)
	}
	wrapped := pairedFaultStep{lifecycleStep: step, RawRecords: pairedRawRecords(records),
		OriginalRequest: pairedFaultOriginalOf(f.t, p, op.Coordinator, result)}
	f.trace.Steps = append(f.trace.Steps, wrapped)
	return wrapped
}

func p03OldRead(f *pairedGraphFaultNetwork, observer uint64, stage int, checkpoint string) {
	f.t.Helper()
	step := f.trace.Steps[stage-1]
	cut, err := pairedFaultCut(step.Cut)
	if err != nil {
		f.t.Fatal(err)
	}
	old := f.observe(observer, graphReadAt, cut, false)
	f.assertView(old.View, stage)
	assertLifecycleCandidates(f.t, old.RawObservation, min(stage, 3))
	if !reflect.DeepEqual(old.Cut, step.Cut) || !reflect.DeepEqual(old.View, step.After) {
		f.t.Fatal("P03 old complete cut substituted current graph", stage)
	}
	f.trace.OldReads = append(f.trace.OldReads, pairedFaultOldRead{lifecycleOldRead: lifecycleOldRead{
		Operation: step.Operation, Mode: "reopened-at", Cut: old.Cut, View: old.View, RawObservation: old.RawObservation},
		Checkpoint: checkpoint, EventID: f.event()})
}
