package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Embedding preserves every declared materializer interface and lifetime. Only
// the exact armed fence's genuine Stage entry can pause; no floor is supplied.
type p02StageMachine struct{ *declaredMaterializer }
type p02StageMarker struct {
	Member, PID, Index, Term, HardCommit, Checkpoint uint64
	Command, RetainedEntry, Image                    []byte
	SHA256                                           string
}
type p02FenceArtifact struct {
	Schedule               string
	Trace                  pairedFaultTrace
	Phases                 []p06Phase
	SourceCaptures         []p06Phase
	Encounters             []p06Phase
	Fences                 []p06LocalRecord
	Stage                  *p02StageMarker
	Processes              []pairedFaultProcess
	Old, Current, Retained p06Observation
	DelayedLocal           *p06LocalRecord
	DelayedLocalReplay     *p06LocalRecord
	Retries                []pairedFaultRetry
	PendingCaller          []byte
	Records                []lifecycleRawRecord
}

func p02OpenAllocationHost(m *declaredMaterializer, id uint64, cfg replica.Config) (*allocationHost, error) {
	if os.Getenv("RHO_GRAPH_P02_STAGE") != "1" {
		return openAllocationHost(m, id, cfg)
	}
	// Keep openAllocationHost's complete validation and initialization, replacing
	// only the actual application callback with the interface-preserving wrapper.
	if m == nil || m.store == nil || cfg.Store != m.store || cfg.ApplicationMachine != nil || cfg.Machine != nil {
		return nil, errInvalid
	}
	cfg.ID, cfg.ApplicationMachine = id, &p02StageMachine{m}
	d, err := replica.Open(cfg)
	if err != nil {
		return nil, err
	}
	h := &allocationHost{driver: d, machine: m, pending: make(map[allocationReadID]allocationQuery), recipients: make(map[[16]byte]*allocationRecipientHandle)}
	if _, err := rand.Read(h.session[:]); err != nil {
		return nil, errors.Join(err, d.Close())
	}
	return h, nil
}

func (m *p02StageMachine) Stage(entry replica.Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	arm, err := os.ReadFile(filepath.Join(os.Getenv("RHO_GRAPH_PROCESS_DIR"), "p02-fence-arm.json"))
	if errors.Is(err, os.ErrNotExist) {
		return m.declaredMaterializer.Stage(entry, budget)
	}
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	var command []byte
	if err := json.Unmarshal(arm, &command); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if !bytes.Equal(entry.Data, command) {
		return m.declaredMaterializer.Stage(entry, budget)
	}
	fence, err := decodeGraphCutCommand(command)
	if err != nil || fence.phase != graphCutFence || fence.scope != m.scope() {
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	hard, _, err := m.store.InitialState()
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	checkpoint, image, err := m.store.Checkpoint()
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	entries, err := m.store.Entries(entry.Index, entry.Index+1, 1024)
	if err != nil || len(entries) != 1 || entries[0].GetIndex() != entry.Index || entries[0].GetTerm() != entry.Term || !bytes.Equal(entries[0].GetData(), entry.Data) || hard.GetCommit() < entry.Index || checkpoint >= entry.Index {
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	retained, err := proto.Marshal(entries[0])
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	digest := sha256.Sum256(command)
	marker := p02StageMarker{Member: m.store.ApplicationLimits().LocalVoter, PID: uint64(os.Getpid()), Index: entry.Index, Term: entries[0].GetTerm(), HardCommit: hard.GetCommit(), Checkpoint: checkpoint, Command: bytes.Clone(command), RetainedEntry: retained, Image: bytes.Clone(image), SHA256: hex.EncodeToString(digest[:])}
	wire, err := json.Marshal(marker)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	path := filepath.Join(os.Getenv("RHO_GRAPH_PROCESS_DIR"), "p02-fence-stage.json")
	if err := os.WriteFile(path+".tmp", wire, 0600); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	// Parent observes this marker then kills/reaps this actual incarnation. A
	// missing kill becomes a finite operation failure, never an accepted pause.
	<-time.After(30 * time.Second)
	return raftlog.ApplicationBatch{}, errLimit
}

func p02Phase(f *pairedGraphFaultNetwork, actor uint64, after bool) (<-chan pairedFaultBoundary, func(), func() ([]p06Phase, []p06Phase, []p06Phase)) {
	entered, release := make(chan pairedFaultBoundary, 1), make(chan struct{})
	var mu sync.Mutex
	var phases []p06Phase
	var sources []p06Phase
	var encounters []p06Phase
	var first sync.Once
	hub := f.n.all[actor].mux.hub
	hub.mu.Lock()
	hub.beforeRelay = func(ctx context.Context, origin, partition uint64, request []byte) error {
		if after || origin != actor || partition != 3 {
			return nil
		}
		p := p02MaybePhase(request)
		if p != crossPrepare {
			return nil
		}
		first.Do(func() { entered <- pairedFaultBoundary{Request: bytes.Clone(request)} })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	hub.afterRelay = func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		if origin != actor {
			return nil
		}
		q, err := decodeGraphSourceRequest(request)
		if err != nil {
			return err
		}
		if q.kind == sourceCrossRead || q.kind == sourceRequestLookup || q.kind == sourceSubmit {
			mu.Lock()
			if len(encounters) >= 64 {
				mu.Unlock()
				return errLimit
			}
			encounters = append(encounters, p06Phase{origin, partition, bytes.Clone(request), bytes.Clone(reply)})
			mu.Unlock()
		}
		if q.kind == sourceOpen {
			mu.Lock()
			defer mu.Unlock()
			if len(sources) >= 2 {
				return errLimit
			}
			sources = append(sources, p06Phase{origin, partition, bytes.Clone(request), bytes.Clone(reply)})
			return nil
		}
		if p02MaybePhase(request) == 0 {
			return nil
		}
		mu.Lock()
		if len(phases) >= 6 {
			mu.Unlock()
			return errLimit
		}
		phases = append(phases, p06Phase{origin, partition, bytes.Clone(request), bytes.Clone(reply)})
		mu.Unlock()
		if after && partition == 3 && p02MaybePhase(request) == crossPrepare {
			first.Do(func() { entered <- pairedFaultBoundary{bytes.Clone(request), bytes.Clone(reply)} })
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return nil
	}
	hub.mu.Unlock()
	resume := sync.OnceFunc(func() { close(release) })
	finish := func() ([]p06Phase, []p06Phase, []p06Phase) {
		hub.mu.Lock()
		hub.beforeRelay, hub.afterRelay = nil, nil
		hub.mu.Unlock()
		mu.Lock()
		defer mu.Unlock()
		return append([]p06Phase(nil), phases...), append([]p06Phase(nil), sources...), append([]p06Phase(nil), encounters...)
	}
	f.t.Cleanup(func() { resume(); finish() })
	return entered, resume, finish
}
func p02BindSources(t *testing.T, captures []p06Phase, manifest crossManifest, marker *p02StageMarker) {
	t.Helper()
	if len(captures) != 2 {
		t.Fatal("P02 actual source capture inventory", len(captures))
	}
	seen := make(map[uint64]bool)
	for _, capture := range captures {
		q, err := decodeGraphSourceRequest(capture.Request)
		reply, rerr := decodeGraphWireResponse(capture.Reply)
		info, ierr := decodeGraphSourceInfo(reply.body)
		part, found := manifest.part(capture.Partition)
		if err != nil || rerr != nil || ierr != nil || reply.err != nil || q.kind != sourceOpen || reply.kind != q.kind || reply.sequence != q.sequence || reply.nonce != q.nonce || info.nonce != q.nonce || info.cookie.issuer != reply.issuer || info.readID.session != reply.issuer || info.observation.source.scope != reply.peer.scope || !found || seen[capture.Partition] || part.floor != info.floor || part.binding.CapturedBase.Index != info.root.Index || part.binding.CapturedBase.Generation != info.root.Generation || part.binding.CapturedBase.ImageHash != info.root.ImageHash || !bytes.Equal(part.binding.CapturedBase.Image, info.root.Image) {
			t.Fatal("P02 manifest installed floor/base differs from genuine source", err, rerr, ierr)
		}
		seen[capture.Partition] = true
		if marker != nil && capture.Partition == 3 && (info.observation.source.index < marker.Index || info.root.Index < marker.Index) {
			t.Fatal("P02 planning source capture preceded retained fence application")
		}
	}
	if !seen[3] || !seen[8] {
		t.Fatal("P02 source participant omission")
	}
}
func p02MaybePhase(wire []byte) crossPhase {
	q, err := decodeGraphSourceRequest(wire)
	if err != nil || q.kind != sourceSubmit {
		return 0
	}
	p, err := decodeGraphSourceProposal(q.command)
	if err != nil || !isCrossPartitionWire(p.wire) {
		return 0
	}
	c, _, err := decodeCrossCommand(p.wire, defaultMaterializerLimits())
	if err != nil {
		return 0
	}
	return c.phase
}
func p02Fence(f *pairedGraphFaultNetwork, actor, bound uint64) p06LocalRecord {
	p := p06LocalProposal(f, actor, certifiedPrepareFence, bound, nil)
	r := p02SubmitWhilePaused(f, actor, p)
	g, err := decodeGraphCutResult(r.Outcome)
	if err != nil || g.phase != graphCutFence || g.status != graphCutApplied || g.round != bound || g.index != r.Index || len(r.CDC) != 0 {
		f.t.Fatal("P02 genuine retained fence", g, err)
	}
	return r
}

// Intentional source gates remain pending. Drain actual native packets only;
// waiting for every caller/relay owner would retire this gate by timeout.
func p02SubmitWhilePaused(f *pairedGraphFaultNetwork, actor uint64, p allocationProposal) p06LocalRecord {
	wire, err := encodeCertifiedProcessProposal(p)
	if err != nil {
		f.t.Fatal(err)
	}
	group := f.n.group(actor)
	if f.parent == nil {
		group.event(f.t, actor, processRequest{op: processPropose, data: wire})
	} else {
		// The live semantic relay is intentionally held. Use the same native
		// queue and original outer deadline without draining that relay owner.
		group.enqueue(f.t, f.call(f.n.all[actor], processRequest{op: processPropose, data: wire}))
		f.drainGroup(group)
	}
	f.packetDrain()
	state := f.call(f.n.all[actor], processRequest{op: processStateOp})
	r := f.call(f.n.all[actor], processRequest{op: processEntryRecord, index: state.applied})
	for id, voter := range group.nodes {
		if f.parent != nil {
			f.mu.Lock()
			dead := f.down[id]
			f.mu.Unlock()
			if dead {
				continue // Inspect every genuine surviving voter, never a dead pipe.
			}
		}
		got := f.call(voter, processRequest{op: processEntryRecord, index: state.applied})
		if !bytes.Equal(got.outcome, r.outcome) || !bytes.Equal(got.changes, r.changes) || got.stats[statCommit] < state.applied {
			f.t.Fatal("P02 native progress original differs", id)
		}
	}
	return p06LocalRecord{p.target.partition, state.applied, bytes.Clone(p.wire), bytes.Clone(r.outcome), bytes.Clone(r.changes)}
}
func p02CheckLocalCloseCDC(t *testing.T, record p06LocalRecord, round uint64) {
	t.Helper()
	ns, _ := certifiedProcessNamespace(2)
	o, err := decodePartitionGraphOutcome(record.Outcome, ns)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := decodeTypedChangeEnvelope(record.CDC, o, defaultMaterializerLimits(), true)
	if err != nil || len(logical.groups) != 1 || len(logical.entities)+len(logical.lives)+len(logical.values) != 0 {
		t.Fatal("P02 delayed local complete CDC", err)
	}
	g := logical.groups[0]
	if g.Key != (graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}) || len(g.Changes) != 1 || !g.Changes[0].Before().Present() || g.Changes[0].Before().Value().ID() != 2 || g.Changes[0].Before().Revision().Provenance() != 1 || g.Changes[0].After().Present() || g.Changes[0].After().Revision().Provenance() != 41 || g.Changes[0].After().Revision().ID() != round {
		t.Fatal("P02 delayed local original before/after changed")
	}
	p06CheckScope(t, g.Changes[0].Scope())
}
func p02ElectLiveLeader(f *pairedGraphFaultNetwork, prior, next uint64) {
	// Isolate actual Raft packets, while retaining the authenticated service and
	// process incarnation. Restore traffic before old peer forwards the request.
	hub := f.n.all[prior].mux.hub
	hub.mu.Lock()
	previous := hub.packetFilter
	hub.packetFilter = func(packet replica.Packet) (bool, error) {
		if previous != nil {
			drop, err := previous(packet)
			if drop || err != nil {
				return drop, err
			}
		}
		return packet.From == prior || packet.To == prior, nil
	}
	hub.mu.Unlock()
	defer func() { hub.mu.Lock(); hub.packetFilter = previous; hub.mu.Unlock() }()
	f.elect(prior, next)
	hub.mu.Lock()
	hub.packetFilter = previous
	hub.mu.Unlock()
	// These nearby live voters need no snapshot transfer. Keep the intentional
	// original relay pending while actual native checkpoints catch up.
	group := f.n.group(next)
	for range 32 {
		group.event(f.t, next, processRequest{op: processTick})
		group.event(f.t, next, processRequest{op: processCertifiedRead, entity: uint64(observeConfiguration)})
		f.packetDrain()
		base := f.n.all[next].call(f.t, processRequest{op: processStateOp})
		healthy := base.ready
		for _, child := range group.nodes {
			r := child.call(f.t, processRequest{op: processStateOp})
			healthy = healthy && r.applied == base.applied && bytes.Equal(r.image, base.image)
		}
		if healthy {
			return
		}
	}
	f.t.Fatal("P02 live predecessor did not catch up under genuine successor")
}
func p02Votes(f *pairedGraphFaultNetwork, phases []p06Phase, proposal allocationProposal, decision crossStatus) (crossManifest, [2]crossStatus) {
	if len(phases) < 6 || len(phases) > 11 {
		f.t.Fatal("P02 complete Register/Prepare/Decide/Resolve capture", len(phases))
	}
	var manifest crossManifest
	var votes [2]crossStatus
	var original []byte
	seen := make(map[[2]uint64][]byte)
	index := 0
	for _, phase := range phases {
		q, err := decodeGraphSourceRequest(phase.Request)
		p := p06SourceProposal(f.t, phase.Request)
		command, _, e := decodeCrossCommand(p.wire, defaultMaterializerLimits())
		arena, aerr := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
		if err != nil || e != nil || aerr != nil {
			f.t.Fatal(err, e, aerr)
		}
		m, err := decodeCrossManifest(command.manifest, defaultMaterializerLimits(), arena)
		if err != nil {
			f.t.Fatal(err)
		}
		if original == nil {
			manifest, original = m, command.manifest
			p06BindBusiness(f.t, proposal, m)
		}
		reply, err := decodeGraphWireResponse(phase.Reply)
		status, serr := decodeCrossStatus(reply.body)
		if err != nil || serr != nil || reply.err != nil || reply.kind != q.kind || reply.sequence != q.sequence || reply.nonce != q.nonce || reply.peer.scope != p.target || !bytes.Equal(original, command.manifest) || status.phase != command.phase || status.tx != manifest.tx || status.digest != manifest.digest {
			f.t.Fatal("P02 authentic original phase", index, err, serr)
		}
		key := [2]uint64{status.ns.partition, uint64(status.phase)}
		if prior, ok := seen[key]; ok {
			if !bytes.Equal(prior, reply.body) {
				f.t.Fatal("P02 election recovery changed original phase", key)
			}
			continue
		}
		if index >= 6 || command.phase != []crossPhase{crossRegister, crossPrepare, crossPrepare, crossDecide, crossResolve, crossResolve}[index] {
			f.t.Fatal("P02 original phase order", index)
		}
		seen[key] = bytes.Clone(reply.body)
		if index == 1 || index == 2 {
			votes[index-1] = status
			if !status.yes {
				f.t.Fatal("P02 genuine YES refused", status)
			}
		}
		if index >= 3 && (!status.commit || status.round != decision.round) {
			f.t.Fatal("P02 original decision/resolve changed", status, decision)
		}
		members := []uint64{1, 2, 3}
		if status.ns.partition == 8 {
			members = []uint64{4, 5, 6}
		}
		var retainedOriginal []byte
		for _, id := range members {
			r := f.n.all[id].call(f.t, processRequest{op: processEntryRecord, index: status.index})
			matches := bytes.Equal(r.outcome, reply.body)
			if status.phase == crossRegister {
				// observeCrossTransaction projects Register from the original R
				// control record, omitting the stored installed floor. Bind that
				// deliberate projection to the actual captured coordinator floor.
				part, found := manifest.part(status.ns.partition)
				actual, err := decodeCrossStatus(r.outcome)
				want := crossStatus{ns: status.ns, tx: status.tx, digest: status.digest, phase: crossRegister, index: status.index, floor: part.floor}
				projected := want
				projected.floor = 0
				matches = found && err == nil && actual == want && status == projected
			}
			if retainedOriginal == nil {
				retainedOriginal = bytes.Clone(r.outcome)
			}
			if !matches || !bytes.Equal(r.outcome, retainedOriginal) || r.stats[statCommit] < status.index || r.stats[statCheckpoint] < status.index {
				f.t.Fatalf("P02 actual all-voter original member=%d index=%d phase=%d checkpoint=%d commit=%d expected=%x actual=%x", id, status.index, status.phase, r.stats[statCheckpoint], r.stats[statCommit], reply.body, r.outcome)
			}
		}
		index++
	}
	if index != 6 {
		f.t.Fatal("P02 missing genuine original phase", index)
	}
	if decision.round <= votes[0].floor || decision.round <= votes[1].floor || decision.digest != sha256.Sum256(original) {
		f.t.Fatal("P02 COMMIT did not exceed both genuine voted floors", votes, decision)
	}
	return manifest, votes
}

// Hold actual persisted-fence acknowledgements. The auto-pump never blocks in a
// child Stage: one captured ACK is later sent by an independently owned RPC.
func p02HoldFenceACK(f *pairedGraphFaultNetwork, leader, index uint64) (<-chan replica.Packet, func()) {
	packets := make(chan replica.Packet, 2)
	seen := make(map[[3]uint64]bool)
	var mu sync.Mutex
	ns, _ := certifiedProcessNamespace(leader)
	spec := partitionCodecRequest(f.t, 1, defaultMaterializerLimits())
	part, _ := spec.declaration.Partition(ns.partition)
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(ns.graph), Partition: ns.partition, Group: part.Group}, SemanticContractID: certifiedGraphSemanticContractID()}
	hub := f.n.all[leader].mux.hub
	group := f.n.group(leader)
	priorDrop := group.dropPacket
	hold := func(packet replica.Packet) (bool, error) {
		if packet.To != leader {
			return false, nil
		}
		raw, err := replica.DecodeApplicationPacket(binding, packet, processFrameBytes)
		if err != nil {
			return false, err
		}
		message := &pb.Message{}
		if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(raw.Payload, message); err != nil {
			return false, err
		}
		if message.GetType() != pb.MsgAppResp || message.GetReject() || message.GetIndex() < index {
			return false, nil
		}
		key := [3]uint64{packet.From, message.GetTerm(), message.GetIndex()}
		mu.Lock()
		defer mu.Unlock()
		// Exact native cumulative ACK replay does not consume another slot or
		// poison a voter. Keep the first two original packets, with a finite
		// distinct-message ledger; later valid queued ACKs remain withheld.
		if seen[key] {
			return true, nil
		}
		if len(seen) >= 8 {
			return false, errLimit
		}
		seen[key] = true
		packet.Payload = bytes.Clone(packet.Payload)
		select {
		case packets <- packet:
		default:
		}
		return true, nil
	}
	hub.mu.Lock()
	prior := hub.packetFilter
	hub.packetFilter = func(packet replica.Packet) (bool, error) {
		if prior != nil {
			drop, err := prior(packet)
			if drop || err != nil {
				return drop, err
			}
		}
		return hold(packet)
	}
	hub.mu.Unlock()
	group.dropPacket = func(packet replica.Packet) bool {
		if priorDrop != nil && priorDrop(packet) {
			return true
		}
		drop, err := hold(packet)
		if err != nil {
			f.t.Fatal("P02 synchronous native packet error", err)
		}
		return drop
	}
	return packets, sync.OnceFunc(func() { hub.mu.Lock(); hub.packetFilter = prior; hub.mu.Unlock(); group.dropPacket = priorDrop })
}
func p02AwaitStage(f *pairedGraphFaultNetwork, leader uint64, proposal allocationProposal) p02StageMarker {
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	path := filepath.Join(f.n.dir, "1", "p02-fence-stage.json")
	for {
		wire, err := os.ReadFile(path)
		if err == nil {
			var marker p02StageMarker
			if err := json.Unmarshal(wire, &marker); err != nil {
				f.t.Fatal(err)
			}
			digest := sha256.Sum256(proposal.wire)
			entry := &pb.Entry{}
			if err := proto.Unmarshal(marker.RetainedEntry, entry); err != nil {
				f.t.Fatal(err)
			}
			if marker.Member != leader || marker.PID != uint64(f.n.all[leader].cmd.Process.Pid) || marker.HardCommit < marker.Index || marker.Checkpoint >= marker.Index || marker.Index != entry.GetIndex() || marker.Term != entry.GetTerm() || !bytes.Equal(marker.Command, proposal.wire) || !bytes.Equal(entry.GetData(), proposal.wire) || marker.SHA256 != hex.EncodeToString(digest[:]) {
				f.t.Fatal("P02 not actual fence persistence before application", marker)
			}
			return marker
		}
		if !errors.Is(err, os.ErrNotExist) {
			f.t.Fatal(err)
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			f.t.Fatal("P02 actual pre-application Stage marker absent")
		}
	}
}
