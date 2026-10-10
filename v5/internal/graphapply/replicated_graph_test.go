package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"google.golang.org/protobuf/proto"
)

type processSnapshotPacket struct {
	packet replica.Packet
	owner  uint64
}

type tinyGraphTransport struct {
	blocked                                              map[uint64]bool
	snapshots                                            []processSnapshotPacket
	nodes                                                map[uint64]*graphProcess
	queue                                                []replica.Packet
	queuedBytes, maxQueueBytes, maxQueueCount, delivered int
	reads                                                []replica.ReadResult
}

func (n *tinyGraphTransport) enqueue(t *testing.T, out processResponse) {
	t.Helper()
	snapshotAt := 0
	for _, p := range out.packets {
		if p.Snapshot {
			if snapshotAt >= len(out.handles) || len(n.snapshots) >= 2 {
				t.Fatal("snapshot lost bounded exact owner")
			}
			n.snapshots = append(n.snapshots, processSnapshotPacket{p, out.handles[snapshotAt]})
			snapshotAt++
			continue
		}
		cost := cap(p.Payload) + 64 // Conservative: borrowed frame tails are charged per packet.
		if len(n.queue) >= processPacketCount || cost > processQueueBytes-n.queuedBytes {
			t.Fatal("bounded packet queue exhausted")
		}
		if n.nodes[p.From] == nil || n.nodes[p.To] == nil {
			t.Fatal("traffic crosses distinct graph group", p.From, p.To)
		}
		n.queue = append(n.queue, p)
		n.queuedBytes += cost
		n.maxQueueCount = max(n.maxQueueCount, len(n.queue))
		n.maxQueueBytes = max(n.maxQueueBytes, n.queuedBytes)
	}
	if snapshotAt != len(out.handles) {
		t.Fatal("snapshot owner without packet")
	}
	if len(out.reads) > 16-len(n.reads) {
		t.Fatal("bounded read result collection")
	}
	n.reads = append(n.reads, out.reads...)
}
func (n *tinyGraphTransport) drain(t *testing.T) {
	t.Helper()
	for len(n.queue) > 0 {
		if n.delivered == processDeliveries {
			t.Fatal("bounded packet schedule exhausted")
		}
		p := n.queue[0]
		n.queue[0] = replica.Packet{}
		n.queue = n.queue[1:]
		n.queuedBytes -= cap(p.Payload) + 64
		n.delivered++
		if n.blocked[p.From] || n.blocked[p.To] {
			continue
		}
		n.enqueue(t, n.nodes[p.To].call(t, processRequest{op: processStep, packet: p}))
	}
}
func (n *tinyGraphTransport) event(t *testing.T, id uint64, r processRequest) {
	t.Helper()
	n.enqueue(t, n.nodes[id].call(t, r))
	n.drain(t)
}
func (n *tinyGraphTransport) commit(t *testing.T, follower, leader uint64, wire []byte, ns namespace) processResponse {
	t.Helper()
	n.event(t, follower, processRequest{op: processPropose, data: wire})
	r := n.nodes[leader].call(t, processRequest{op: processLookup, data: wire})
	o, err := decodeAnyOutcome(r.outcome, ns)
	if err != nil || o.reason != reasonNone || o.hash != sha256.Sum256(wire) || o.disposition != applied {
		t.Fatal("not original durable success", o, err)
	}
	for id, p := range n.nodes {
		if n.blocked[id] {
			continue
		}
		got := p.call(t, processRequest{op: processLookup, data: wire})
		if !bytes.Equal(got.outcome, r.outcome) || !bytes.Equal(got.changes, r.changes) {
			t.Fatal("replicated outcome/ordered CDC mismatch", id)
		}
	}
	return r
}

type snapshotGraphCommands struct{ init, recipient, grant, created, changed, tail []byte }

func tinySnapshotCommands(t *testing.T, n namespace) snapshotGraphCommands {
	t.Helper()
	encode := func(r graphRequest) []byte {
		b, err := encodeGraphRequest(r, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	init := graphInit(t)
	init.ns = n
	init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}}
	commands := snapshotGraphCommands{init: encode(init)}
	controls := codecRequests(t)
	for _, control := range []request{controls[2], controls[3]} {
		control.ns = n
		if control.kind == activateRecipient {
			control.home = n.partition
		} else {
			control.sequence = 1
			control.count = 16
		}
		wire, err := encodeRequest(control, defaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if control.kind == activateRecipient {
			commands.recipient = wire
		} else {
			commands.grant = wire
		}
	}
	r := graphCodecRequest(t)
	r.ns = n
	r.id = requestID{8}
	scope := r.operations[0].Scope
	r.operations = nil
	r.claims = nil
	grant := grantReference{session: codecSession(), sequence: 1}
	for _, node := range []struct{ entity, life uint64 }{{1, 2}, {3, 4}} {
		r.operations = append(r.operations, graphstate.Operation{Kind: graphstate.CreateNode, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Record: graphstate.EntityRecord{ID: graphstate.EntityID(node.entity), Kind: graphstate.Node, Axis: scope.Axis()}}, graphstate.Operation{Kind: graphstate.AddLabel, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Name: "X"}, graphstate.Operation{Kind: graphstate.Set, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Name: "answer", Value: graphstate.String("old"), ValueID: 5})
		r.claims = append(r.claims, freshBinding{role: entityBinding, id: node.entity, grant: grant}, freshBinding{role: lifeBinding, owner: graphstate.EntityID(node.entity), id: node.life, grant: grant})
	}
	r.claims = append(r.claims, freshBinding{role: valueBinding, id: 5, grant: grant})
	commands.created = encode(r)
	r.id = requestID{9}
	var err error
	r.revision, err = state.NewRevision(4, 0)
	if err != nil {
		t.Fatal(err)
	}
	r.operations = []graphstate.Operation{{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: scope}, {Kind: graphstate.RemoveLabel, Owner: 3, Life: 4, Scope: scope, Name: "X"}, {Kind: graphstate.AddLabel, Owner: 3, Life: 4, Scope: scope, Name: "Y"}, {Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "answer", Value: graphstate.String("new"), ValueID: 6}}
	r.claims = []freshBinding{{role: valueBinding, id: 6, grant: grant}}
	commands.changed = encode(r)
	r.id = requestID{10}
	r.revision, err = state.NewRevision(5, 0)
	if err != nil {
		t.Fatal(err)
	}
	r.operations = []graphstate.Operation{{Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "answer", Value: graphstate.String("tail"), ValueID: 7}}
	r.claims = []freshBinding{{role: valueBinding, id: 7, grant: grant}}
	commands.tail = encode(r)
	return commands
}

func processExpectRefusal(t *testing.T, p *graphProcess, r processRequest, want error) {
	t.Helper()
	before := p.call(t, processRequest{op: processStateOp})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	refused, err := p.exchange(ctx, r)
	if err != nil {
		t.Fatal("IPC refusal transport", err)
	}
	if err := processResponseError(refused); !errors.Is(err, want) {
		t.Fatal("wrong call-layer refusal", err, want)
	}
	after := p.call(t, processRequest{op: processStateOp})
	if before.stats != after.stats || before.cutID != after.cutID || before.manifestID != after.manifestID || !bytes.Equal(before.image, after.image) {
		t.Fatal("refusal changed active checkpoint/HardState/audit/transfer ledger", before.stats, after.stats)
	}
}
func assertSnapshotFacts(t *testing.T, p *graphProcess, index, barrier, generation uint64, oracle *tinyGraphOracle) {
	t.Helper()
	var active []uint64
	for _, id := range []uint64{1, 3, 99} {
		r := p.call(t, processRequest{op: processProject, index: index, entity: id})
		if r.applied < barrier || r.readGeneration != generation || r.stats[statGeneration] != generation {
			t.Fatal("fresh read base/barrier", r.applied, barrier, r.readGeneration, r.stats)
		}
		if err := oracle.compare(index, id, r.fact); err != nil {
			t.Fatal(err)
		}
		if r.fact.Active {
			active = append(active, id)
		}
	}
	var want []uint64
	for _, id := range []uint64{1, 3, 99} {
		if oracle.versions[index][id].Active {
			want = append(want, id)
		}
	}
	if !reflect.DeepEqual(active, want) {
		t.Fatal("exact snapshot active set", active, want)
	}
}
func assertHeldSnapshotFacts(t *testing.T, p *graphProcess, handle, index, activeGeneration uint64, oracle *tinyGraphOracle) []byte {
	t.Helper()
	var image []byte
	for _, id := range []uint64{1, 3, 99} {
		r := p.call(t, processRequest{op: processHeldProject, index: handle, entity: id})
		if r.readGeneration != 1 || r.stats[statGeneration] != activeGeneration || r.stats[statViews] != 1 {
			t.Fatal("held view rebound or leaked", r.readGeneration, r.stats)
		}
		if err := oracle.compare(index, id, r.fact); err != nil {
			t.Fatal(err)
		}
		t.Logf("held generation1 observation: activeGeneration=%d originalIndex=%d entity=%d cumulativeRecords=%d cumulativeBytes=%d", activeGeneration, index, id, r.stats[statHeldRows], r.stats[statHeldBytes])
		if image == nil {
			image = r.image
		} else if !bytes.Equal(image, r.image) {
			t.Fatal("held root changed")
		}
	}
	return image
}

// A real checked absent-entity read on the SAME held Full handle supplies the
// refusal positive control without repeating three complete projections and
// exhausting its intentionally cumulative source ledger.
func assertHeldPhantom(t *testing.T, p *graphProcess, handle uint64, image []byte) {
	t.Helper()
	r := p.call(t, processRequest{op: processHeldProject, index: handle, entity: 99})
	if err := compareProcessFact(r.fact, processFact{}); err != nil {
		t.Fatal(err)
	}
	if r.readGeneration != 1 || r.stats[statGeneration] != 1 || !bytes.Equal(r.image, image) {
		t.Fatal("held refusal control changed view")
	}
	t.Logf("held generation1 refusal control: entity99 cumulativeRecords=%d cumulativeBytes=%d", r.stats[statHeldRows], r.stats[statHeldBytes])
}

func TestReplicatedGraphSnapshotHeldViewTailAndReopen(t *testing.T) {
	if os.Getenv("RHO_GRAPH_PROCESS_CHILD") == "1" {
		t.Skip("parent only")
	}
	start := time.Now()
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-graph-snapshot-stores-")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("retained snapshot fixture directory", dir)
	all := map[uint64]*graphProcess{}
	var launched []*graphProcess
	for id := uint64(1); id <= 6; id++ {
		all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id)
		launched = append(launched, all[id])
	}
	a := tinyGraphTransport{nodes: map[uint64]*graphProcess{1: all[1], 2: all[2], 3: all[3]}, blocked: map[uint64]bool{}}
	b := tinyGraphTransport{nodes: map[uint64]*graphProcess{4: all[4], 5: all[5], 6: all[6]}}
	an, _, _ := processNamespace(1)
	bn, _, _ := processNamespace(4)
	ac := tinySnapshotCommands(t, an)
	bc := tinySnapshotCommands(t, bn)
	var originals []struct {
		wire     []byte
		response processResponse
	}
	var bOriginals []struct {
		wire     []byte
		response processResponse
	}
	var createdA, createdB uint64
	for _, group := range []struct {
		network          *tinyGraphTransport
		leader, follower uint64
		ns               namespace
		commands         snapshotGraphCommands
	}{{&a, 1, 3, an, ac}, {&b, 4, 6, bn, bc}} {
		group.network.event(t, group.leader, processRequest{op: processCampaign})
		for _, wire := range [][]byte{group.commands.init, group.commands.recipient, group.commands.grant, group.commands.created} {
			r := group.network.commit(t, group.follower, group.leader, wire, group.ns)
			o, err := decodeAnyOutcome(r.outcome, group.ns)
			if err != nil {
				t.Fatal(err)
			}
			if group.leader == 1 {
				originals = append(originals, struct {
					wire     []byte
					response processResponse
				}{wire, r})
				createdA = o.index
			} else {
				createdB = o.index
				bOriginals = append(bOriginals, struct {
					wire     []byte
					response processResponse
				}{wire, r})
			}
		}
	}
	oracle := newTinyGraphOracle()
	oracle.created(createdA)
	held := all[3].call(t, processRequest{op: processHold, index: createdA})
	if len(held.handles) != 1 {
		t.Fatal("held owner missing")
	}
	heldID := held.handles[0]
	oldHeldImage := assertHeldSnapshotFacts(t, all[3], heldID, createdA, 1, oracle)
	bBefore := all[4].call(t, processRequest{op: processStateOp})
	a.blocked[3] = true
	changed := a.commit(t, 2, 1, ac.changed, an)
	o, err := decodeAnyOutcome(changed.outcome, an)
	if err != nil {
		t.Fatal(err)
	}
	changedIndex := o.index
	oracle.changed(changedIndex)
	originals = append(originals, struct {
		wire     []byte
		response processResponse
	}{ac.changed, changed})
	if all[3].call(t, processRequest{op: processStateOp}).applied >= changedIndex {
		t.Fatal("isolated follower invented change")
	}
	all[1].call(t, processRequest{op: processPublish})
	a.blocked[3] = false
	buildCalls := 0
	for ticks := 0; len(a.snapshots) == 0 && ticks < 20; ticks++ {
		a.event(t, 1, processRequest{op: processTick})
		if len(a.snapshots) > 0 {
			break
		}
		offers := all[1].call(t, processRequest{op: processOffers})
		for _, id := range offers.handles {
			ready := false
			for !ready && buildCalls < processDeliveries {
				r := all[1].call(t, processRequest{op: processBuild, index: id})
				buildCalls++
				ready = r.ready
			}
			if !ready {
				t.Fatal("bounded manifest build exhausted")
			}
			all[1].call(t, processRequest{op: processSeal, index: id})
		}
	}
	if len(a.snapshots) != 1 {
		t.Fatal("one actual snapshot packet required", len(a.snapshots))
	}
	send := a.snapshots[0]
	a.snapshots = nil
	if send.packet.From != 1 || send.packet.To != 3 || !send.packet.Snapshot {
		t.Fatal("wrong actual snapshot peers", send.packet)
	}
	manifestWire := all[1].call(t, processRequest{op: processManifest, index: send.owner}).aux
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(an.graph), Partition: an.partition, Group: [16]byte{41}}, SemanticContractID: SemanticContractID()}
	manifest, err := raftlog.DecodeApplicationSnapshotManifestForBinding(manifestWire, 64<<10, binding)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 3 || manifest.Index != changedIndex || manifest.SemanticContractID != SemanticContractID() || !reflect.DeepEqual(manifest.ConfState.GetVoters(), []uint64{1, 2, 3}) {
		t.Fatal("unexpected portable source manifest", manifest)
	}
	// These are binding-aware CODEC refusals, before BeginImport; do not claim
	// that rechecksummed/foreign manifest admission reached the Store boundary.
	semanticOffset := len(manifestWire) - len(manifest.Image) - proto.Size(manifest.ConfState) - 32
	for _, offset := range []int{4, 27, 28, semanticOffset} {
		bad := bytes.Clone(manifestWire)
		bad[offset] ^= 0x40
		processExpectRefusal(t, all[3], processRequest{op: processBeginImport, data: bad}, raftlog.ErrInvalid)
		assertHeldPhantom(t, all[3], heldID, oldHeldImage)
	}
	imported := all[3].call(t, processRequest{op: processBeginImport, data: manifestWire})
	if len(imported.handles) != 1 || len(imported.aux) != 32 {
		t.Fatal("missing same-child import owner")
	}
	importID := imported.handles[0]
	var manifestID [32]byte
	copy(manifestID[:], imported.aux)
	pages := 0
	maxRows, maxVisitedBytes, maxDataBytes := uint64(0), uint64(0), 0
	for {
		if buildCalls+pages+a.delivered+b.delivered >= processDeliveries {
			t.Fatal("bounded total snapshot schedule")
		}
		r := all[1].call(t, processRequest{op: processNext, index: send.owner})
		chunk, err := decodeProcessChunk(r.aux)
		if err != nil {
			t.Fatal(err)
		}
		if chunk.Version != 3 || chunk.CutID != manifest.CutID || chunk.ManifestID != manifestID || chunk.Visited > 64 || chunk.VisitedBytes > 256<<10 {
			t.Fatal("snapshot page binding/budget", chunk)
		}
		maxRows = max(maxRows, chunk.Visited)
		maxVisitedBytes = max(maxVisitedBytes, chunk.VisitedBytes)
		maxDataBytes = max(maxDataBytes, len(chunk.Data))
		pages++
		all[3].call(t, processRequest{op: processAppend, index: importID, data: r.aux})
		if chunk.Final {
			break
		}
	}
	all[3].call(t, processRequest{op: processVerify, index: importID})
	prepared := all[3].call(t, processRequest{op: processPrepare, index: importID})
	if prepared.stats[statGeneration] != 1 || prepared.stats[statPrepared] != 1 || prepared.stats[statImport] != 1 || prepared.stats[statVerified] != 1 {
		t.Fatal("dormant prepared owner activated early", prepared.stats)
	}
	raw, err := replica.DecodeApplicationPacket(binding, send.packet, processFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"group", "semantic"} {
		foreign := binding
		if variant == "group" {
			foreign.Identity.Group[0] ^= 1
		} else {
			foreign.SemanticContractID[0] ^= 1
		}
		wrong, err := replica.EncodeApplicationPacket(foreign, raw, processFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		processExpectRefusal(t, all[3], processRequest{op: processActivate, index: importID, packet: wrong}, replica.ErrInvalid)
		assertHeldPhantom(t, all[3], heldID, oldHeldImage)
	}
	processExpectRefusal(t, all[3], processRequest{op: processActivate, packet: send.packet}, raftlog.ErrInvalid)
	activated := all[3].call(t, processRequest{op: processActivate, index: importID, packet: send.packet})
	if activated.stats[statGeneration] != 2 || activated.stats[statCheckpoint] != manifest.Index || activated.stats[statAuditIndex] != manifest.Index || activated.stats[statAuditTerm] != manifest.Term || activated.cutID != manifest.CutID || activated.manifestID != manifestID {
		t.Fatal("activation identity mismatch", activated.stats, activated.cutID, activated.manifestID)
	}
	installed := all[3].call(t, processRequest{op: processStateOp})
	if !bytes.Equal(installed.image, manifest.Image) {
		t.Fatal("installed checkpoint differs from exact donor image")
	}
	// Keep receiver response packets until exact-owner sender feedback; otherwise
	// its acknowledgement may make pending-snapshot feedback legitimately stale.
	a.enqueue(t, all[1].call(t, processRequest{op: processFeedback, index: send.owner, entity: 1}))
	a.enqueue(t, activated)
	a.drain(t)
	all[3].call(t, processRequest{op: processImportClose, index: importID})
	barrier := a.barrier(t, 1, []byte("after-activation"))
	if !bytes.Equal(oldHeldImage, assertHeldSnapshotFacts(t, all[3], heldID, createdA, 2, oracle)) {
		t.Fatal("held historical root moved")
	}
	assertSnapshotFacts(t, all[3], createdA, barrier, 2, oracle)
	assertSnapshotFacts(t, all[3], changedIndex, barrier, 2, oracle)
	for _, original := range originals {
		r := all[3].call(t, processRequest{op: processLookup, data: original.wire})
		if !bytes.Equal(r.outcome, original.response.outcome) || !bytes.Equal(r.changes, original.response.changes) {
			t.Fatal("imported original outcome/CDC mismatch")
		}
	}
	source := all[1].call(t, processRequest{op: processStateOp})
	if source.stats[statGeneration] != 1 || !bytes.Equal(source.image, manifest.Image) {
		t.Fatal("portable source changed generation/root")
	}
	tail := a.commit(t, 2, 1, ac.tail, an)
	o, err = decodeAnyOutcome(tail.outcome, an)
	if err != nil {
		t.Fatal(err)
	}
	tailIndex := o.index
	oracle.tail(tailIndex)
	originals = append(originals, struct {
		wire     []byte
		response processResponse
	}{ac.tail, tail})
	barrier = a.barrier(t, 1, []byte("after-tail"))
	assertSnapshotFacts(t, all[3], tailIndex, barrier, 2, oracle)
	if !bytes.Equal(oldHeldImage, assertHeldSnapshotFacts(t, all[3], heldID, createdA, 2, oracle)) {
		t.Fatal("tail mutated held generation")
	}
	all[3].call(t, processRequest{op: processHoldClose, index: heldID})
	clean := all[3].call(t, processRequest{op: processStateOp})
	if clean.stats[statViews] != 0 || clean.stats[statImport] != 0 || clean.stats[statPrepared] != 0 || clean.stats[statClaims] != 0 || clean.stats[statPinnedBytes] != 0 {
		t.Fatal("receiver owners leaked", clean.stats)
	}
	oldNonce := all[3].incarnation
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	err = all[3].shutdown(ctx, false)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	all[3] = startGraphProcess(t, filepath.Join(dir, "3"), 3, "RHO_GRAPH_PROCESS_REOPEN=1")
	launched = append(launched, all[3])
	a.nodes[3] = all[3]
	reopened := all[3].call(t, processRequest{op: processStateOp})
	if reopened.incarnation == oldNonce || reopened.stats[statGeneration] != 2 || reopened.cutID != manifest.CutID || reopened.manifestID != manifestID || reopened.stats[statViews] != 0 || reopened.stats[statImport] != 0 {
		t.Fatal("reopen resurrected volatile owners or changed durable activation", reopened.stats)
	}
	processExpectRefusal(t, all[3], processRequest{op: processHeldProject, index: heldID, entity: 3}, errInvalid)
	if offers := all[3].call(t, processRequest{op: processOffers}); len(offers.handles) != 0 {
		t.Fatal("restart restored sender IPC handles")
	}
	barrier = a.barrier(t, 1, []byte("after-reopen"))
	for _, index := range []uint64{createdA, changedIndex, tailIndex} {
		assertSnapshotFacts(t, all[3], index, barrier, 2, oracle)
	}
	for _, original := range originals {
		r := all[3].call(t, processRequest{op: processLookup, data: original.wire})
		if !bytes.Equal(r.outcome, original.response.outcome) || !bytes.Equal(r.changes, original.response.changes) {
			t.Fatal("reopen original outcome/CDC mismatch")
		}
	}
	bAfter := all[4].call(t, processRequest{op: processStateOp})
	if bBefore.stats != bAfter.stats || !bytes.Equal(bBefore.image, bAfter.image) || bAfter.cutID != ([32]byte{}) || bAfter.manifestID != ([32]byte{}) {
		t.Fatal("distinct graph group changed")
	}
	for _, original := range bOriginals {
		for _, id := range []uint64{4, 5, 6} {
			r := all[id].call(t, processRequest{op: processLookup, data: original.wire})
			if !bytes.Equal(r.outcome, original.response.outcome) || !bytes.Equal(r.changes, original.response.changes) {
				t.Fatal("distinct graph original outcome/CDC changed", id)
			}
		}
	}
	boracle := newTinyGraphOracle()
	boracle.created(createdB)
	bbarrier := b.barrier(t, 4, []byte("distinct-group-after-A"))
	for _, id := range []uint64{4, 5, 6} {
		assertSnapshotFacts(t, all[id], createdB, bbarrier, 1, boracle)
	}
	measurement := tinyProcessMeasurement{Scope: "ONE AS3 catch-up + retained Q-position0 view + tail/reopen; two DISTINCT sole-partition graphs; no crash/cut/2PC or independent CDC-oracle claim", Processes: 6, StoreDirectory: dir, Packets: a.delivered + b.delivered, MaxQueueCount: max(a.maxQueueCount, b.maxQueueCount), MaxQueueBytes: max(a.maxQueueBytes, b.maxQueueBytes)}
	var rpcCount, maxPinned, maxOffers, maxImports, maxViews uint64
	for _, p := range launched {
		rpcCount += p.sequence
		measurement.MaxIPCFrameBytes = max(measurement.MaxIPCFrameBytes, p.maxFrame)
		maxPinned = max(maxPinned, p.maxPinned)
		maxOffers = max(maxOffers, p.maxOffers)
		maxImports = max(maxImports, p.maxImports)
		maxViews = max(maxViews, p.maxHeld)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := p.shutdown(ctx, false)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	if rpcCount+uint64(buildCalls+pages) >= processDeliveries {
		t.Fatal("bounded RPC/page count exceeded")
	}
	measurement.ClosedStoreFileBytes, err = processStoreBytes(dir)
	if err != nil {
		t.Fatal(err)
	}
	measurement.WallSeconds = time.Since(start).Seconds()
	report := struct {
		tinyProcessMeasurement
		BuildCalls, ReplayPages, MaxChunkDataBytes                                                                                       int
		MaxChunkVisited, MaxChunkVisitedBytes, ObservedMaxPinnedBytes, ObservedMaxOffers, ObservedMaxImports, ObservedMaxViews, RPCCount uint64
	}{measurement, buildCalls, pages, maxDataBytes, maxRows, maxVisitedBytes, maxPinned, maxOffers, maxImports, maxViews, rpcCount}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "measurement.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("snapshot scenario measurement: %s", data)
}
func (n *tinyGraphTransport) barrier(t *testing.T, leader uint64, context []byte) uint64 {
	t.Helper()
	n.reads = nil
	n.event(t, leader, processRequest{op: processRead, data: context})
	if len(n.reads) != 1 || !bytes.Equal(n.reads[0].Context, context) {
		t.Fatal("request-bound quorum barrier missing", n.reads)
	}
	index := n.reads[0].Index
	n.reads = nil
	return index
}

type tinyProcessMeasurement struct {
	Scope                                                   string  `json:"scope"`
	WallSeconds                                             float64 `json:"wall_seconds"`
	Processes                                               int     `json:"processes"`
	Packets, MaxQueueCount, MaxQueueBytes, MaxIPCFrameBytes int
	ClosedStoreFileBytes                                    uint64 `json:"closed_store_file_bytes"`
	StoreDirectory                                          string `json:"store_directory"`
}

func TestReplicatedGraphSixProcessesOrdinaryAndHistory(t *testing.T) {
	if os.Getenv("RHO_GRAPH_PROCESS_CHILD") == "1" {
		t.Skip("parent only")
	}
	start := time.Now()
	parentDir := os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT")
	dir, err := os.MkdirTemp(parentDir, "rho-graph-six-stores-")
	if err != nil {
		t.Fatal(err)
	}
	// Keep task-owned stores and diagnostics after any failure, rather than
	// hiding them in testing.T's automatic temporary-directory cleanup.
	t.Log("retained fixture directory", dir)
	all := make(map[uint64]*graphProcess)
	for id := uint64(1); id <= 6; id++ {
		all[id] = startGraphProcess(t, filepath.Join(dir, string(rune('0'+id))), id)
	}
	measurement := tinyProcessMeasurement{Scope: "Two DISTINCT graphs, each one sole partition with three replicas; no multipartition/cut/2PC acceptance", Processes: 6, StoreDirectory: dir}
	for _, leader := range []uint64{1, 4} {
		ns, voters, _ := processNamespace(leader)
		network := tinyGraphTransport{nodes: map[uint64]*graphProcess{voters[0]: all[voters[0]], voters[1]: all[voters[1]], voters[2]: all[voters[2]]}}
		network.event(t, leader, processRequest{op: processCampaign})
		init := graphInit(t)
		init.ns = ns
		init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarString, Cardinality: graphstate.ScalarCardinality}}
		wire, err := encodeGraphRequest(init, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		network.commit(t, voters[2], leader, wire, ns)
		controls := codecRequests(t)
		for _, r := range []request{controls[2], controls[3]} {
			r.ns = ns
			if r.kind == activateRecipient {
				r.home = ns.partition
			} else {
				r.sequence = 1
				r.count = 16
			}
			wire, err = encodeRequest(r, defaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			network.commit(t, voters[2], leader, wire, ns)
		}
		r := graphCodecRequest(t)
		r.ns = ns
		r.id = requestID{8}
		scope := r.operations[0].Scope
		r.operations = nil
		r.claims = nil
		grant := grantReference{session: codecSession(), sequence: 1}
		for _, node := range []struct{ entity, life uint64 }{{1, 2}, {3, 4}} {
			r.operations = append(r.operations, graphstate.Operation{Kind: graphstate.CreateNode, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Record: graphstate.EntityRecord{ID: graphstate.EntityID(node.entity), Kind: graphstate.Node, Axis: scope.Axis()}}, graphstate.Operation{Kind: graphstate.AddLabel, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Name: "X"}, graphstate.Operation{Kind: graphstate.Set, Owner: graphstate.EntityID(node.entity), Life: graphstate.LifeID(node.life), Scope: scope, Name: "answer", Value: graphstate.String("old"), ValueID: 5})
			r.claims = append(r.claims, freshBinding{role: entityBinding, id: node.entity, grant: grant}, freshBinding{role: lifeBinding, owner: graphstate.EntityID(node.entity), id: node.life, grant: grant})
		}
		r.claims = append(r.claims, freshBinding{role: valueBinding, id: 5, grant: grant})
		wire, err = encodeGraphRequest(r, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		created := network.commit(t, voters[2], leader, wire, ns)
		original, err := decodeAnyOutcome(created.outcome, ns)
		if err != nil {
			t.Fatal(err)
		}
		before := original.index
		r.id = requestID{9}
		r.revision, err = state.NewRevision(4, 0)
		if err != nil {
			t.Fatal(err)
		}
		r.operations = []graphstate.Operation{{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: scope}, {Kind: graphstate.RemoveLabel, Owner: 3, Life: 4, Scope: scope, Name: "X"}, {Kind: graphstate.AddLabel, Owner: 3, Life: 4, Scope: scope, Name: "Y"}, {Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "answer", Value: graphstate.String("new"), ValueID: 6}}
		r.claims = []freshBinding{{role: valueBinding, id: 6, grant: grant}}
		wire, err = encodeGraphRequest(r, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		changed := network.commit(t, voters[2], leader, wire, ns)
		last, err := decodeAnyOutcome(changed.outcome, ns)
		if err != nil {
			t.Fatal(err)
		}
		barrier := network.barrier(t, leader, []byte{byte(leader), 99}) // Not a certified database cut.
		if barrier < last.index {
			t.Fatal("barrier behind original mutation", barrier, last.index)
		}
		oracle := newTinyGraphOracle()
		oracle.created(before)
		oracle.changed(last.index)
		var currentRoot graphstore.Root
		for _, id := range voters {
			for _, version := range []uint64{before, last.index} {
				var active []uint64
				for _, entity := range []uint64{1, 3, 99} {
					got := all[id].call(t, processRequest{op: processProject, index: version, entity: entity})
					if got.applied < barrier {
						t.Fatal("replica has not applied barrier", id, got.applied, barrier)
					}
					if err := oracle.compare(version, entity, got.fact); err != nil {
						t.Fatal(id, version, entity, err)
					}
					if got.fact.Active {
						active = append(active, entity)
					}
					root, err := graphstore.DecodeRoot(got.image)
					if err != nil || root.Namespace().Graph != graphstate.GraphID(ns.graph) || root.Namespace().Partition != ns.partition {
						t.Fatal("wrong graph scope", id, root, err)
					}
					if version == last.index {
						if currentRoot.SemanticEpoch() == 0 {
							currentRoot = root
						} else if root.SemanticEpoch() != currentRoot.SemanticEpoch() || root.EffectDigest() != currentRoot.EffectDigest() {
							t.Fatal("logical root mismatch", id)
						}
					}
				}
				want := []uint64{1, 3}
				if version == last.index {
					want = []uint64{3}
				}
				if len(active) != len(want) {
					t.Fatal("exact active set", active, want)
				}
				for i := range want {
					if active[i] != want[i] {
						t.Fatal("exact active set", active, want)
					}
				}
			}
		}
		measurement.Packets += network.delivered
		measurement.MaxQueueCount = max(measurement.MaxQueueCount, network.maxQueueCount)
		measurement.MaxQueueBytes = max(measurement.MaxQueueBytes, network.maxQueueBytes)
	}
	for _, p := range all {
		measurement.MaxIPCFrameBytes = max(measurement.MaxIPCFrameBytes, p.maxFrame)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := p.shutdown(ctx, false)
		cancel()
		if err != nil {
			t.Fatal("child shutdown", err)
		}
	}
	measurement.ClosedStoreFileBytes, err = processStoreBytes(dir)
	if err != nil {
		t.Fatal("closed file-length budget", err)
	}
	measurement.WallSeconds = time.Since(start).Seconds()
	data, err := json.MarshalIndent(measurement, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "measurement.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("tiny six-process measurement: %s", data)
}
