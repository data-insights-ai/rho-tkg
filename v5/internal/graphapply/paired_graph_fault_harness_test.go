package graphapply

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func pairedGraphFaultProperty(o *certifiedProcessOwners, r processRequest, id requestID) (allocationProposal, error) {
	if o == nil || o.host == nil || o.recipient == nil || len(r.data) != 8 || r.index < 4 || r.index > 6 {
		return allocationProposal{}, errInvalid
	}
	value := binary.BigEndian.Uint64(r.data)
	if value != 5 && value != 6 {
		return allocationProposal{}, errInvalid
	}
	cfg, err := o.host.currentConfiguration()
	if err != nil {
		return allocationProposal{}, err
	}
	scope, err := temporal.All(cfg.defaultAxis.Axis())
	if err != nil {
		return allocationProposal{}, err
	}
	claims := []freshBinding{}
	// Only the first definition6 needs a fresh range claim. The reused5/6 payloads
	// are canonical dictionary lookups, never restored cursors or new issuance.
	if r.index == 4 {
		claims = append(claims, freshBinding{role: valueBinding, id: 6, grant: grantReference{session: o.recipient.session, sequence: 1}})
	}
	return o.host.PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.Set, Owner: 3, Life: 4, Scope: scope, Name: "p", Value: graphstate.I64(int64(value)), ValueID: graphstate.ValueID(value)}}, r.index, claims)
}
func pairedGraphFaultConnections(origin uint64, services map[uint64]*graphPeerService, server *graphSourceServer) ([]byte, error) {
	if origin < 1 || origin > 6 || server == nil {
		return nil, errInvalid
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	sessions := server.pending
	for _, slot := range server.slots {
		if slot != nil {
			sessions++
		}
	}
	present := services[origin] != nil
	return processEncoding(func(w *boundedWriter) {
		if present {
			w.tag(1)
		} else {
			w.tag(0)
		}
		w.u64(uint64(sessions))
	})
}

// Only production callUsing's authenticated issuer-mismatch invalidation may
// release the old issuer binding. Keep its spent sequence: the replacement
// service may already have accepted that request before the mismatch surfaced.
func pairedFaultReconnectStream(ctx context.Context, prior *graphAuthenticatedStream, retained bool) (*graphAuthenticatedStream, error) {
	if ctx == nil || prior == nil {
		return nil, errInvalid
	}
	if !retained {
		return prior, nil // Literal ordinary reconnect; never reset its stream.
	}
	if err := prior.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer prior.mu.unlock()
	if !prior.invalid {
		return prior, nil
	}
	if prior.sequence == 0 || prior.issuer == ([16]byte{}) {
		return nil, errCorrupt
	}
	return &graphAuthenticatedStream{mu: newGraphRequestGate(), sequence: prior.sequence}, nil
}

func pairedGraphFaultReconnect(ctx context.Context, h *allocationHost, old *crossProcessCoordinator, s *graphProcessCertifiedSessions, move *rangeMoveCoordinator, server *graphSourceServer, r processRequest, workers, queued int, active bool, retainRuns ...bool) (*crossProcessCoordinator, error) {
	if ctx == nil || h == nil || old == nil || s == nil || len(r.data) != 16 || len(retainRuns) > 1 {
		return old, errInvalid
	}
	voters := [2]uint64{binary.BigEndian.Uint64(r.data[:8]), binary.BigEndian.Uint64(r.data[8:])}
	if voters[0] < 1 || voters[0] > 3 || voters[1] < 4 || voters[1] > 6 {
		return old, errInvalid
	}
	if workers != 0 || queued != 0 || active || move != nil {
		return old, errInvalid
	}
	if server == nil {
		return old, errInvalid
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.pending != 0 {
		return old, errInvalid
	}
	for _, slot := range server.slots {
		// Closed leases retain replay records until another capture reuses the
		// slot. Only an unreleased native view owns work that blocks reconnect.
		if slot != nil && !slot.done() {
			return old, errInvalid
		}
	}
	old.mu.Lock()
	defer old.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	retained := len(retainRuns) == 1 && retainRuns[0]
	if !retained && len(old.runs) != 0 || len(s.owners) != 0 || s.pending != 0 {
		return old, errInvalid
	}
	if retained && ctx.Err() != nil {
		return old, errors.Join(errCrossProcessPending, ctx.Err())
	}
	h.mu.Lock()
	busy := h.pendingReadCount() != 0 || h.closed
	h.mu.Unlock()
	if busy {
		return old, errInvalid
	}
	transports := make(map[uint64]graphPeerWireTransport, 2)
	for i, partition := range []uint64{3, 8} {
		prior, ok := old.transports[partition].(*graphProcessChildTransport)
		if !ok {
			return old, errInvalid
		}
		prior.owner.mu.Lock()
		pending := len(prior.owner.pending)
		prior.owner.mu.Unlock()
		if pending != 0 {
			return old, errInvalid
		}
		if prior.peer.voter == voters[i] {
			transports[partition] = prior
		} else {
			transports[partition] = &graphProcessChildTransport{graphPeerIdentity{mustProcessScope(h, partition), voters[i]}, prior.owner}
		}
	}
	next, err := newCrossProcessCoordinator(h, transports)
	if err != nil {
		return old, err
	}
	for _, partition := range []uint64{3, 8} {
		if old.transports[partition].Peer() == next.transports[partition].Peer() {
			next.streams[partition], err = pairedFaultReconnectStream(ctx, old.streams[partition], retained)
			if err != nil {
				return old, err
			}
		}
	}
	if retained {
		if ctx.Err() != nil {
			return old, errors.Join(errCrossProcessPending, ctx.Err())
		}
		if err := nativeConjunctionRetainRoutesLocked(old, next); err != nil {
			return old, err
		}
		return old, nil
	}
	return next, nil
}

// Fresh NCR1 diagnostic route only, after all ordinary reconnect ownership
// gates above. Caller holds old.mu and has drained every native worker/RPC.
// No manifest is decoded or copied: the actual private idle run objects and
// original certified engine remain owned by this SAME coordinator and Host.
func nativeConjunctionRetainRoutesLocked(old, next *crossProcessCoordinator) error {
	if old == nil || next == nil || old == next || old.host == nil || old.host.machine == nil || old.host != next.host || len(old.runs) > 2 || len(next.runs) != 0 || len(old.transports) != old.host.machine.declaration.Len() || len(next.transports) != len(old.transports) || len(next.streams) != len(old.streams) || len(old.streams) != len(old.transports) {
		return errInvalid
	}
	for tx, run := range old.runs {
		if run == nil || run.busy || !run.registrationAttempted || run.resume.tx != tx || tx == ([16]byte{}) || run.resume.request == (requestID{}) || run.resume.business == ([32]byte{}) || run.resume.scope != old.host.machine.scope() || run.tx.owner != old.host || len(run.tx.manifest) == 0 || len(run.tx.manifest) > old.host.machine.limits.commandBytes || run.tx.digest == ([32]byte{}) {
			return errInvalid
		}
	}
	for partition, transport := range old.transports {
		replacement := next.transports[partition]
		if transport == nil || replacement == nil || replacement.Peer().scope != mustProcessScope(old.host, partition) || replacement.Peer().voter == 0 || old.streams[partition] == nil || next.streams[partition] == nil {
			return errInvalid
		}
		unchanged := transport.Peer() == replacement.Peer()
		prior, current := old.streams[partition], next.streams[partition]
		if unchanged && prior != current {
			if !prior.invalid || prior.sequence == 0 || prior.issuer == ([16]byte{}) || current.invalid || current.sequence != prior.sequence || current.issuer != ([16]byte{}) {
				return errInvalid
			}
		} else if !unchanged && prior == current || unchanged && prior.invalid {
			return errInvalid
		}
	}
	// All replacements were checked before either map changes. No native read,
	// new run allowance, original deadline, beforeDecision or cut cache resets.
	old.transports, old.streams = next.transports, next.streams
	return nil
}

type pairedFaultProcess struct {
	EventID                            uint64
	Action, Role                       string
	Member                             uint64
	Instance, PreviousInstance, Volume string
	Reaped                             bool   `json:",omitempty"`
	Ready                              bool   `json:",omitempty"`
	Signal                             string `json:",omitempty"`
	OOMKilled                          *bool
	Raw                                []byte
	RecoveryRaw                        []byte             `json:",omitempty"`
	Result                             *pairedFaultResult `json:",omitempty"`
}
type pairedFaultResult struct {
	Coordinator          uint64
	Request, Transaction string
	Round                uint64
}
type pairedFaultStep struct {
	lifecycleStep
	RawRecords      []pairedFaultRawRecord
	OriginalRequest pairedFaultOriginal
}

type pairedFaultNoOp struct {
	Partition, Index, Term uint64
	Entry                  []byte
}

type pairedFaultRecord struct {
	nativeLifecycleRecord
	NoOp *pairedFaultNoOp `json:",omitempty"`
}

type pairedFaultRawRecord struct {
	lifecycleRawRecord
	OutcomeBytes, CDCBytes int
	NoOp                   *pairedFaultNoOp `json:",omitempty"`
}

// Empty application records qualify only through an actually retained normal
// Raft entry with empty command data. Compaction cannot manufacture evidence;
// the parent may retain a previously inspected exact original entry instead.
func pairedGraphFaultRecordWire(ctx context.Context, h *allocationHost, index uint64) ([]byte, error) {
	record, err := nativeLifecycleRecordRead(ctx, h, index)
	if err != nil {
		return nil, err
	}
	out := pairedFaultRecord{nativeLifecycleRecord: record}
	if len(record.Outcome) == 0 {
		h.mu.Lock()
		entries, readErr := h.machine.store.Entries(index, index+1, 128)
		h.mu.Unlock()
		if readErr != nil && !errors.Is(readErr, raft.ErrCompacted) {
			return nil, readErr
		}
		if readErr == nil {
			if len(entries) != 1 || entries[0].GetIndex() != index || entries[0].GetTerm() == 0 || entries[0].GetType() != pb.EntryNormal || len(entries[0].GetData()) != 0 || len(record.CDC) != 0 {
				return nil, errInvalid
			}
			wire, err := proto.Marshal(entries[0])
			if err != nil || len(wire) > 64 {
				return nil, errors.Join(errInvalid, err)
			}
			out.NoOp = &pairedFaultNoOp{Partition: record.Partition, Index: index, Term: entries[0].GetTerm(), Entry: wire}
		}
	}
	return json.Marshal(out)
}

type pairedFaultOldRead struct {
	lifecycleOldRead
	Checkpoint string
	EventID    uint64
}
type pairedFaultOriginal struct {
	Coordinator                 uint64
	Graph, Request, Transaction string
	Payload                     []byte
}
type pairedFaultObservation struct {
	Cut            lifecycleRawCut
	View           lifecycleView
	RawObservation nativeLifecycleObservation
	RawRecords     []pairedFaultRawRecord
}
type pairedFaultIntermediate struct {
	Classification string
	Raw            []byte
	Cut            *lifecycleRawCut            `json:",omitempty"`
	View           *lifecycleView              `json:",omitempty"`
	RawObservation *nativeLifecycleObservation `json:",omitempty"`
}
type pairedFaultRetry struct {
	Operation     string
	EventID       uint64
	RawResult     []byte
	Payload       []byte
	Result        pairedFaultResult
	Before, After pairedFaultObservation
	NewRawRecords []pairedFaultRawRecord
}
type pairedFaultChanged struct {
	Payload        []byte
	Classification string
	Raw            []byte
	Before, After  pairedFaultObservation
	NewRawRecords  []pairedFaultRawRecord
}
type pairedFaultCase struct {
	Case, Operation, Marker string
	MarkerRaw               []byte
	CallerClassification    string
	OriginalRequest         pairedFaultOriginal
	Observer                pairedFaultObservation
	Intermediate            *pairedFaultIntermediate `json:",omitempty"`
	Retry                   *pairedFaultRetry        `json:",omitempty"`
	ChangedPayload          *pairedFaultChanged      `json:",omitempty"`
	ProcessEvidence         []pairedFaultProcess
	CallerRaw               []byte
	ReopenRetries           []pairedFaultRetry `json:",omitempty"`
}
type pairedFaultTrace struct {
	SchemaVersion      int                     `json:"schemaVersion"`
	FixtureID          string                  `json:"fixtureID"`
	Engine             string                  `json:"engine"`
	NormalizationRules []string                `json:"normalizationRules"`
	IdentityMapping    []lifecycleIDMap        `json:"identityMapping"`
	OperationMapping   []lifecycleOperationMap `json:"operationMapping"`
	InitialCut         lifecycleRawCut         `json:"initialCut"`
	Steps              []pairedFaultStep       `json:"steps"`
	OldReads           []pairedFaultOldRead    `json:"oldReads"`
	Faults             []pairedFaultCase       `json:"faults"`
	RawCounts          map[string]any          `json:"rawCounts"`
	Scope              string                  `json:"scope"`
}
type pairedGraphFaultNetwork struct {
	t                     *testing.T
	n                     *certifiedProcessNetwork
	parent                context.Context             // Optional, immutable before fresh fixture publication.
	conjunctionGate       *nativeConjunctionBatchGate // At most one; guarded by mu.
	protocolReplyHold     *nativeProtocolReplyHold    // Fresh causal hold; at most one, guarded by mu.
	protocolProgram       *nativeProtocolProgram      // Fresh immutable causal/message owner; nil preserves old fixtures.
	nativeMoveCatchup     *nativeProtocolMoveCatchup  // Exact live native move gate; transport catch-up only.
	bootstrapAgreement    [32]byte                    // Actual validated bootstrap proposal; immutable after bootstrap.
	mu                    sync.Mutex
	down                  map[uint64]bool
	dropped               [][2]uint64
	incarnations          []*graphProcess
	intentionalKills      map[*graphProcess]bool
	retiredBootstrap      int
	events                uint64
	reads                 int
	snapshotPages         int
	lastCoordinateRaw     []byte
	lastObservationRaw    []byte
	trace                 pairedFaultTrace
	noops                 map[[2]uint64]*pairedFaultNoOp
	nativeSnapshotHook    func(nativeFaultSnapshotBoundary) bool
	nativeSnapshotArena   *graphstate.OutputBudget
	nativeSnapshotNotices int
	nativeSnapshotState   string // Fresh-only selected pre-call diagnostic; nil hook adds no work.
}

func newPairedGraphFaultNetwork(t *testing.T) *pairedGraphFaultNetwork {
	t.Helper()
	return newPairedGraphFaultNetworkWithParent(t, nil)
}

func newPairedGraphFaultNetworkWithParent(t *testing.T, parent context.Context) *pairedGraphFaultNetwork {
	t.Helper()
	if parent != nil {
		if _, bounded := parent.Deadline(); !bounded || parent.Err() != nil {
			t.Fatal("fresh fixture requires its original live absolute deadline")
		}
	}
	// Bootstrap/grant implementation is reused byte-for-byte, while cleanup and
	// read admission belong to this fault cohort rather than legacy lifecycle.
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-paired-graph-fault-")
	if err != nil {
		t.Fatal(err)
	}
	n := &certifiedProcessNetwork{all: make(map[uint64]*graphProcess), dir: dir, leaders: [2]uint64{1, 4}}
	f := &pairedGraphFaultNetwork{t: t, n: n, down: make(map[uint64]bool), intentionalKills: make(map[*graphProcess]bool), noops: make(map[[2]uint64]*pairedFaultNoOp), trace: pairedFaultTrace{SchemaVersion: 1, FixtureID: "graph-fault-local-v1", Engine: "native-raft", IdentityMapping: lifecycleIDs(), NormalizationRules: []string{"Actual original request/TxID/round correspondence; no equal numeric cross-engine clocks", "Native precommit may retain YES/registration and pending Fresh; unknown caller is not observer authority", "Complete raw per-operation outcomes/CDC and decoded AllScope retained", "Same-cut entity census1..32; empty labels and five endpoint queries only"}, RawCounts: make(map[string]any), Scope: "Paired local functional graph fault comparison only; no physical-host/power-loss/cost or fullV2 acceptance"}}
	f.parent = parent
	for id := uint64(1); id <= 6; id++ {
		f.requireActive()
		n.all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_LIFECYCLE_PROCESS=1", "RHO_GRAPH_PAIRED_PROCESS=1")
		f.incarnations = append(f.incarnations, n.all[id])
	}
	attachGraphProcessMuxes(t, n.all)
	f.installFilter()
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
	t.Cleanup(func() {
		for id, p := range n.all {
			f.mu.Lock()
			dead := f.down[id]
			f.mu.Unlock()
			if !dead && !p.closed {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := p.shutdown(ctx, false)
				cancel()
				if err != nil {
					t.Error("paired current child cleanup", id, err)
				}
			}
		}
		var calls uint64
		success := 0
		allReaped := true
		for _, p := range f.incarnations {
			if !p.closed || p.cmd.ProcessState == nil {
				allReaped = false
				t.Error("paired incarnation not reaped", p.cmd.Process.Pid)
			} else if p.cmd.ProcessState.Success() {
				success++
			} else if !f.intentionalKills[p] {
				f.t.Error("unscheduled child failure", p.cmd.Process.Pid, p.cmd.ProcessState)
			}
			p.mux.mu.Lock()
			calls += p.sequence
			p.mux.mu.Unlock()
		}
		allowance := uint64(processDeliveries + 96*4*1024)
		packets := f.retiredBootstrap + n.groups[0].delivered + n.groups[1].delivered
		if calls+uint64(packets) > allowance {
			t.Error("paired IPC bound", calls, allowance)
		}
		t.Logf("paired cleanup: incarnations=%d all-reaped=%t successful=%d intentional-kills=%d calls=%d bootstrapPackets=%d admitted=%d", len(f.incarnations), allReaped, success, len(f.intentionalKills), calls, packets, allowance)
	})
	for g, id := range []uint64{1, 4} {
		f.groupEvent(n.groups[g], id, processRequest{op: processCampaign})
	}
	f.bootstrapAction(1, certifiedInitialize, 3, nil)
	f.bootstrapRead(1, observeConfiguration, 0)
	f.bootstrapAction(1, certifiedInitialize, 8, nil)
	for _, r := range []struct{ actor, id, partition uint64 }{{1, 1, 3}, {4, 2, 8}} {
		f.bootstrapRead(r.actor, observeConfiguration, 0)
		f.bootstrapAction(r.actor, certifiedBeginRecipient, r.id, nil)
		f.bootstrapRead(1, observeRecipient, r.id)
		f.bootstrapAction(1, certifiedRecipientTransition, 3, []byte{byte(fenceDeclaredRecipient)})
		if r.partition != 3 {
			f.bootstrapAction(1, certifiedRecipientTransition, r.partition, []byte{byte(fenceDeclaredRecipient)})
		}
		f.bootstrapRead(r.actor, observeRecipient, r.id)
		f.bootstrapAction(r.actor, certifiedRecipientTransition, 3, []byte{byte(acknowledgeDeclaredHome)})
		f.bootstrapRead(1, observeRecipient, r.id)
		f.bootstrapAction(1, certifiedRecipientTransition, 3, []byte{byte(activateDeclaredRecipient)})
		f.bootstrapRead(1, observeRecipient, r.id)
		f.bootstrapAction(1, certifiedRecipientTransition, r.partition, []byte{byte(publishDeclaredRecipient)})
		f.bootstrapRead(r.actor, observeRecipient, r.id)
		f.bootstrapAction(r.actor, certifiedOpenRecipient, 0, nil)
		f.bootstrapRead(1, observeRecipient, r.id)
		f.bootstrapRead(1, observeAllocator, 0)
		f.bootstrapAction(1, certifiedReserve, 0, nil)
		f.groupEvent(n.group(1), 1, processRequest{op: processCertifiedRead, entity: uint64(observeGrant)})
		f.bootstrapAction(1, certifiedInstallGrant, r.partition, nil)
	}
	return f
}
func (f *pairedGraphFaultNetwork) installFilter() {
	hub := f.n.all[1].mux.hub
	hub.mu.Lock()
	hub.pairedSnapshots = true
	hub.packetFilter = func(p replica.Packet) (bool, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		drop := f.down[p.From] || f.down[p.To]
		if drop {
			if len(f.dropped) >= processDeliveries {
				return false, errLimit
			}
			f.dropped = append(f.dropped, [2]uint64{p.From, p.To})
		}
		return drop, nil
	}
	hub.mu.Unlock()
}
func (f *pairedGraphFaultNetwork) live() map[uint64]*graphProcess {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[uint64]*graphProcess)
	for id, p := range f.n.all {
		if !f.down[id] {
			out[id] = p
		}
	}
	return out
}
func (f *pairedGraphFaultNetwork) drain() {
	f.t.Helper()
	f.requireActive()
	ctx, cancel := f.operationContext(10 * time.Second)
	defer cancel()
	var err error
	if f.parent == nil {
		err = drainGraphProcessMuxes(ctx, f.live())
	} else {
		err = f.drainLiveOwners(ctx)
	}
	if err != nil {
		f.t.Fatal("paired live-owner drain", err)
	}
	f.requireActive()
}

func (f *pairedGraphFaultNetwork) drainLiveOwners(ctx context.Context) error {
	if ctx == nil || f == nil || f.n == nil || f.n.all[1] == nil || f.n.all[1].mux == nil || f.n.all[1].mux.hub == nil {
		return errInvalid
	}
	hub := f.n.all[1].mux.hub
	if hub.ctx == nil {
		return errInvalid
	}
	for {
		if err := errors.Join(ctx.Err(), hub.ctx.Err()); err != nil {
			return err
		}
		hub.mu.Lock()
		packetIdle := hub.packetBytes == 0 && hub.inFlight == 0 && len(hub.packets) == 0
		delivered := hub.delivered
		hub.mu.Unlock()
		if packetIdle {
			// failedStep authenticates the real packet loss/receipt and updates
			// down BEFORE packetLoop retires inFlight. Never infer that update
			// from EOF or a file, or keep the earlier live-roster snapshot.
			var owners [6]*graphProcess
			f.mu.Lock()
			for member := uint64(1); member <= 6; member++ {
				if !f.down[member] {
					owners[member-1] = f.n.all[member]
				}
			}
			f.mu.Unlock()
			idle, found := true, false
			var liveError error
			for _, p := range owners {
				if p == nil {
					continue
				}
				found = true
				p.mux.mu.Lock()
				liveError = errors.Join(liveError, p.mux.err)
				idle = idle && len(p.mux.pending) == 0 && len(p.mux.relays) == 0
				p.mux.mu.Unlock()
			}
			// An arriving/retiring packet may have changed the roster during
			// those independent mux locks. Only a stable packet-idle sample
			// can publish either successful quiescence or a live owner's error.
			hub.mu.Lock()
			stable := hub.delivered == delivered && hub.packetBytes == 0 && hub.inFlight == 0 && len(hub.packets) == 0
			hub.mu.Unlock()
			if stable {
				if !found {
					return errInvalid
				}
				if liveError != nil || idle {
					return errors.Join(liveError, ctx.Err(), hub.ctx.Err())
				}
			} else {
				continue // Genuine packet progress; no waiting deadline/grant resets.
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hub.ctx.Done():
			return hub.ctx.Err()
		case <-hub.idle:
		}
	}
}
func (f *pairedGraphFaultNetwork) packetDrain() {
	f.t.Helper()
	f.requireActive()
	hub := f.n.all[4].mux.hub
	ctx, cancel := f.operationContext(10 * time.Second)
	defer cancel()
	for {
		hub.mu.Lock()
		idle := hub.packetBytes == 0 && hub.inFlight == 0 && len(hub.packets) == 0
		hub.mu.Unlock()
		if idle {
			f.requireActive()
			return
		}
		select {
		case <-hub.idle:
		case <-ctx.Done():
			f.t.Fatal("paired packet boundary", ctx.Err())
		}
	}
}

// This is the same exact opt-in consumed by the actual child constructor. A
// conditional certificate cannot be relabelled as the ordinary certified
// agreement, including the existing P05 parent without fresh campaign hooks.
func pairedFaultConditionalFixture() bool {
	return os.Getenv("RHO_GRAPH_CONDITIONAL_PROCESS") == "1"
}

func (f *pairedGraphFaultNetwork) open(actor uint64, mode graphReadMode, cut graphCut) (uint64, graphCut) {
	f.t.Helper()
	f.reads++
	if f.reads > 96 {
		f.t.Fatal("paired admitted certified read operations")
	}
	wire, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: cut})
	if err != nil {
		f.t.Fatal(err)
	}
	r := f.call(f.n.all[actor], processRequest{op: processSourceCertifiedOpen, data: wire})
	handle, actual, err := decodeGraphE15ReadToken(r.aux)
	if err != nil || len(actual.certificates) != 2 {
		f.t.Fatal("paired actual certified cut", err)
	}
	for _, cert := range actual.certificates {
		agreement := certifiedGraphSemanticContractID()
		if pairedFaultConditionalFixture() {
			agreement = conditionalGraphSemanticContractID()
		}
		if cert.scope.semantic != agreement {
			f.t.Fatal("paired certified agreement")
		}
	}
	return handle, actual
}
func (f *pairedGraphFaultNetwork) observe(actor uint64, mode graphReadMode, cut graphCut, compact bool) pairedFaultObservation {
	f.t.Helper()
	handle, actual := f.open(actor, mode, cut)
	if f.parent != nil {
		defer f.cleanupCertifiedHandle(f.n.all[actor], &handle)
	}
	selector := uint64(0)
	if compact {
		selector = 1
	}
	response := f.call(f.n.all[actor], processRequest{op: processLifecycleObserve, index: handle, entity: selector})
	raw, decodeErr := decodeNativeLifecycleObservation(response.aux)
	if decodeErr != nil {
		f.t.Fatal(decodeErr)
	}
	f.lastObservationRaw = bytes.Clone(response.aux)
	f.call(f.n.all[actor], processRequest{op: processSourceCertifiedClose, index: handle})
	handle = 0
	if !compact {
		h, c := f.open(actor, graphReadAt, actual)
		if f.parent != nil {
			defer f.cleanupCertifiedHandle(f.n.all[actor], &h)
		}
		if !reflect.DeepEqual(c, actual) {
			f.t.Fatal("paired census cut changed")
		}
		census := f.lifecycleRead(actor, h, 2)
		f.call(f.n.all[actor], processRequest{op: processSourceCertifiedClose, index: h})
		h = 0
		raw.Census = census.Census
	}
	view, err := lifecycleNormalizeView(raw, f.trace.IdentityMapping, f.trace.OperationMapping)
	if err != nil {
		f.t.Fatal("paired actual observation normalization", err)
	}
	wire, err := lifecycleRawCertificate(actual)
	if err != nil {
		f.t.Fatal(err)
	}
	return pairedFaultObservation{Cut: wire, View: view, RawObservation: raw}
}
func pairedFaultCut(c lifecycleRawCut) (graphCut, error) {
	out := graphCut{}
	for _, b := range c.Certificates {
		cert, err := decodeGraphCertificate(b)
		if err != nil {
			return out, err
		}
		out.certificates = append(out.certificates, cert)
	}
	return out, nil
}
func (f *pairedGraphFaultNetwork) indexes() [2]uint64 {
	f.t.Helper()
	return [2]uint64{f.call(f.n.all[f.n.leaders[0]], processRequest{op: processStateOp}).applied, f.call(f.n.all[f.n.leaders[1]], processRequest{op: processStateOp}).applied}
}
func (f *pairedGraphFaultNetwork) records(from [2]uint64) []pairedFaultRecord {
	f.t.Helper()
	var out []pairedFaultRecord
	for g, leader := range f.n.leaders {
		end := f.call(f.n.all[leader], processRequest{op: processStateOp}).applied
		if end < from[g] || end-from[g] > 128 {
			f.t.Fatal("paired record range", from, end)
		}
		for index := from[g] + 1; index <= end; index++ {
			r := f.call(f.n.all[leader], processRequest{op: processLifecycleRecord, index: index})
			var record pairedFaultRecord
			if err := json.Unmarshal(r.aux, &record); err != nil {
				f.t.Fatal(err)
			}
			if len(record.Outcome) == 0 {
				key := [2]uint64{record.Partition, record.Index}
				if record.NoOp != nil {
					if prior := f.noops[key]; prior != nil && !reflect.DeepEqual(prior, record.NoOp) {
						f.t.Fatal("retained canonical no-op entry changed", key)
					}
					f.noops[key] = record.NoOp
				} else {
					record.NoOp = f.noops[key]
				}
				if record.NoOp == nil || len(record.CDC) != 0 || record.NoOp.Partition != record.Partition || record.NoOp.Index != record.Index {
					f.t.Fatal("empty native record lacks original retained no-op evidence", key)
				}
			}
			for id, p := range f.n.groups[g].nodes {
				f.mu.Lock()
				dead := f.down[id]
				f.mu.Unlock()
				if dead {
					continue
				}
				v := f.call(p, processRequest{op: processEntryRecord, index: index})
				if !bytes.Equal(v.outcome, record.Outcome) || !bytes.Equal(v.changes, record.CDC) {
					f.t.Fatal("paired complete voter outcome/CDC", id, index)
				}
			}
			out = append(out, record)
		}
	}
	return out
}
func pairedRawRecords(records []pairedFaultRecord) []pairedFaultRawRecord {
	out := []pairedFaultRawRecord{}
	for _, r := range records {
		out = append(out, pairedFaultRawRecord{lifecycleRawRecord: lifecycleRawRecord{Partition: r.Partition, Index: r.Index, Outcome: r.Outcome, CDC: r.CDC}, OutcomeBytes: len(r.Outcome), CDCBytes: len(r.CDC), NoOp: r.NoOp})
	}
	return out
}
func (f *pairedGraphFaultNetwork) complete(name string, p allocationProposal, result crossProcessResult, from [2]uint64, before pairedFaultObservation) pairedFaultStep {
	f.t.Helper()
	command, err := decodePartitionGraphCommand(p.wire, defaultMaterializerLimits())
	if err != nil {
		f.t.Fatal(err)
	}
	op := lifecycleOperationMap{Logical: name, Coordinator: result.decision.ns.partition, Request: hex.EncodeToString(command.request.id[:]), Transaction: hex.EncodeToString(result.decision.tx[:]), Round: result.decision.round}
	f.trace.OperationMapping = append(f.trace.OperationMapping, op)
	records := f.records(from)
	after := f.observe(4, graphReadFresh, graphCut{}, false)
	step := lifecycleStep{Operation: name, Coordinator: "B", Before: before.View, After: after.View, Cut: after.Cut, RawObservation: after.RawObservation}
	if op.Coordinator == 3 {
		step.Coordinator = "A"
	}
	decoded := make([]nativeLifecycleRecord, len(records))
	for i, record := range records {
		decoded[i] = record.nativeLifecycleRecord
	}
	step.LogicalCDC, step.NewEntities, step.NewLives, step.NewValues, err = nativeLifecycleNormalizeRecords(decoded, f.trace.IdentityMapping, f.trace.OperationMapping)
	if err != nil {
		f.t.Fatal(err)
	}
	wrapped := pairedFaultStep{lifecycleStep: step, RawRecords: pairedRawRecords(records), OriginalRequest: pairedFaultOriginalOf(f.t, p, op.Coordinator, result)}
	f.trace.Steps = append(f.trace.Steps, wrapped)
	return wrapped
}
func (f *pairedGraphFaultNetwork) run(actor uint64, p allocationProposal) crossProcessResult {
	f.t.Helper()
	r := f.call(f.n.all[actor], processRequest{op: processSourceCoordinate, data: p.wire})
	result, err := decodeGraphProcessCoordinatorResult(r)
	f.lastCoordinateRaw = bytes.Clone(r.aux)
	if err != nil || !result.decision.commit {
		f.t.Fatal("paired original completion", result, err)
	}
	f.drain()
	return result
}
func (f *pairedGraphFaultNetwork) kill(id uint64, role string) pairedFaultProcess {
	f.t.Helper()
	f.packetDrain()
	f.requireActive()
	p := f.n.all[id]
	before := fmt.Sprintf("voter%d/pid%d/%x", id, p.cmd.Process.Pid, p.incarnation)
	if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		f.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := p.shutdown(ctx, true)
	cancel()
	if p.cmd.ProcessState == nil {
		f.t.Fatal("paired missing actual reap")
	}
	status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		f.t.Fatal("paired actual SIGKILL/reap", err)
	}
	f.mu.Lock()
	f.down[id] = true
	f.intentionalKills[p] = true
	f.mu.Unlock()
	raw, _ := json.Marshal(map[string]any{"Member": id, "PID": p.cmd.Process.Pid, "Instance": before, "Volume": filepath.Join(f.n.dir, fmt.Sprint(id)), "Reaped": true, "Signaled": status.Signaled(), "Signal": "SIGKILL", "SignalNumber": int(status.Signal()), "WaitStatus": int(status), "ExitCode": p.cmd.ProcessState.ExitCode(), "Success": p.cmd.ProcessState.Success(), "OOMKilled": nil, "Error": fmt.Sprint(err)})
	f.t.Logf("paired SIGKILL voter%d pid%d reaped=true", id, p.cmd.Process.Pid)
	return pairedFaultProcess{EventID: f.event(), Action: "kill", Role: role, Member: id, Instance: before, Reaped: true, Signal: "SIGKILL", Volume: filepath.Join(f.n.dir, fmt.Sprint(id)), Raw: raw}
}
func (f *pairedGraphFaultNetwork) replace(id uint64) pairedFaultProcess {
	return f.replaceBeforeHeal(id, nil)
}

// Optional diagnostic runs through the actual replacement pipe after native
// Restore/Ready, before any fair catch-up can import another voter's history.
func (f *pairedGraphFaultNetwork) replaceBeforeHeal(id uint64, before func(*graphProcess, processResponse), packetOnly ...bool) pairedFaultProcess {
	f.t.Helper()
	if len(packetOnly) > 1 || len(packetOnly) == 1 && (!packetOnly[0] || f.parent == nil) {
		f.t.Fatal("invalid fresh held-reply packet-only replacement")
	}
	prior := f.n.all[id]
	if !prior.closed || prior.cmd.ProcessState == nil {
		f.t.Fatal("paired replacing unreaped owner")
	}
	f.packetDrain()
	f.requireActive()
	hub := prior.mux.hub
	env := []string{"RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_LIFECYCLE_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1", "RHO_GRAPH_PAIRED_PROCESS=1"}
	if f.n.rangeMoves {
		env = append(env, "RHO_GRAPH_RANGE_PROCESS=1")
	}
	replacement := startGraphProcess(f.t, filepath.Join(f.n.dir, fmt.Sprint(id)), id, env...)
	f.t.Cleanup(func() {
		if !replacement.closed {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := replacement.shutdown(ctx, false); err != nil {
				f.t.Error("paired replacement graceful cleanup", id, err)
			}
		}
	})
	mux := &graphProcessMux{p: replacement, id: id, hub: hub, pending: make(map[uint64]chan graphProcessAnswer), relays: make(map[uint64]context.CancelFunc), relayQueue: make(chan graphProcessRelayJob, processSourceWorkers)}
	replacement.mux = mux
	hub.mu.Lock()
	hub.peers[id] = mux
	hub.mu.Unlock()
	f.n.all[id] = replacement
	f.n.group(id).nodes[id] = replacement
	f.incarnations = append(f.incarnations, replacement)
	f.mu.Lock()
	delete(f.down, id)
	f.mu.Unlock()
	hub.wg.Go(mux.readLoop)
	for range processSourceWorkers {
		hub.wg.Go(mux.relayLoop)
	}
	state := f.call(replacement, processRequest{op: processStateOp})
	startup, e := encodeProcessResponse(state)
	if e != nil {
		f.t.Fatal(e)
	}
	if before != nil {
		before(replacement, state)
	}
	leader := f.n.leaders[0]
	if id > 3 {
		leader = f.n.leaders[1]
	}
	f.heal(leader, packetOnly...)
	f.mu.Lock()
	interrupted := f.down[id]
	f.mu.Unlock()
	if interrupted {
		// A fresh D hook may have reaped this replacement during an actual
		// snapshot operation. It cannot be reported as a completed Ready rejoin.
		raw, e := json.Marshal(struct {
			Member, PID uint64
			Ready       bool
			Startup     []byte
		}{id, uint64(replacement.cmd.Process.Pid), false, startup})
		if e != nil {
			f.t.Fatal(e)
		}
		return pairedFaultProcess{EventID: f.event(), Action: "reopen-interrupted", Role: "replica", Member: id, Instance: pairedGraphFaultInstance(id, replacement), PreviousInstance: pairedGraphFaultInstance(id, prior), Volume: filepath.Join(f.n.dir, fmt.Sprint(id)), Raw: raw}
	}
	recovered, e := encodeProcessResponse(f.call(replacement, processRequest{op: processStateOp}))
	if e != nil {
		f.t.Fatal(e)
	}
	readiness := map[string]any{"Member": id, "PID": replacement.cmd.Process.Pid, "Instance": pairedGraphFaultInstance(id, replacement), "PreviousInstance": pairedGraphFaultInstance(id, prior), "Volume": filepath.Join(f.n.dir, fmt.Sprint(id)), "Ready": true, "ReadyKind": "restored-and-caught-up-under-leader-read-index", "LocallyRestoredRaw": startup, "RecoveryRaw": recovered}
	if f.nativeMoveCatchup != nil {
		readiness["ReadyKind"] = "native-committed-prefix-and-applied-checkpoints"
		readiness["Catchup"] = f.nativeMoveCatchup.proof
	}
	raw, _ := json.Marshal(readiness)
	return pairedFaultProcess{EventID: f.event(), Action: "reopen", Role: "replica", Member: id, PreviousInstance: fmt.Sprintf("voter%d/pid%d/%x", id, prior.cmd.Process.Pid, prior.incarnation), Instance: fmt.Sprintf("voter%d/pid%d/%x", id, replacement.cmd.Process.Pid, replacement.incarnation), Ready: true, Volume: filepath.Join(f.n.dir, fmt.Sprint(id)), Raw: raw, RecoveryRaw: recovered}
}

func (f *pairedGraphFaultNetwork) event() uint64 { f.events++; return f.events }
func pairedFaultResultOf(r crossProcessResult) pairedFaultResult {
	return pairedFaultResult{Coordinator: r.decision.ns.partition, Request: hex.EncodeToString(r.resume.request[:]), Transaction: hex.EncodeToString(r.decision.tx[:]), Round: r.decision.round}
}
func pairedFaultLiteral(stage int) lifecycleView {
	if stage <= 3 {
		return literalLifecycleView(stage)
	}
	v := literalLifecycleView(3)
	name := []string{"precommit-update", "postcommit-update", "zone-loss-update"}[stage-4]
	value := int64(6)
	ref := "value6"
	if stage == 5 {
		value = 5
		ref = "value5"
	}
	for i := range v.Entities {
		if v.Entities[i].ID == "edge" {
			v.Entities[i].Property = lifecycleCell{Present: true, Kind: "i64", Reference: ref, I64: value, RevisionOperation: name, Provenance: uint64(stage)}
		}
	}
	return v
}
func (f *pairedGraphFaultNetwork) assertView(got lifecycleView, stage int) {
	f.t.Helper()
	if !reflect.DeepEqual(got, pairedFaultLiteral(stage)) {
		f.t.Fatalf("paired exact lifecycle stage%d\ngot%+v\nwant%+v", stage, got, pairedFaultLiteral(stage))
	}
}
func (f *pairedGraphFaultNetwork) proposal(stage int) allocationProposal {
	f.t.Helper()
	actor := uint64(1)
	if stage == 2 {
		actor = 4
	}
	action := certifiedLifecycle
	var input []byte
	if stage >= 4 {
		action = certifiedPairedFaultProperty
		value := uint64(6)
		if stage == 5 {
			value = 5
		}
		input = binary.BigEndian.AppendUint64(nil, value)
	}
	r := f.call(f.n.all[actor], processRequest{op: processCertifiedAction, entity: action, index: uint64(stage), data: input})
	p, err := decodeCertifiedProcessProposal(r.aux)
	if err != nil {
		f.t.Fatal("paired genuine source-produced proposal", err)
	}
	return p
}
func pairedFaultOriginalOf(t *testing.T, p allocationProposal, coord uint64, result crossProcessResult) pairedFaultOriginal {
	t.Helper()
	c, err := decodePartitionGraphCommand(p.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	return pairedFaultOriginal{Coordinator: coord, Graph: hex.EncodeToString(c.request.ns.graph[:]), Request: hex.EncodeToString(c.request.id[:]), Transaction: hex.EncodeToString(result.resume.tx[:]), Payload: bytes.Clone(p.wire)}
}
func (f *pairedGraphFaultNetwork) heal(leader uint64, packetOnly ...bool) {
	f.t.Helper()
	if len(packetOnly) > 1 || len(packetOnly) == 1 && (!packetOnly[0] || f.parent == nil) {
		f.t.Fatal("invalid fresh held-reply packet-only catch-up")
	}
	group := f.n.group(leader)
	catchup := f.nativeMoveCatchup
	if catchup != nil {
		if err := catchup.check(f, leader); err != nil {
			f.t.Fatal("foreign native move catch-up", err)
		}
	}
	priorFilter := group.dropPacket
	observed := 0
	group.dropPacket = func(packet replica.Packet) bool {
		if (leader == 4 && (packet.From == 5 || packet.To == 5) || leader == 2 && packet.To == 1) && observed < 96 {
			observed++
			spec := partitionCodecRequest(f.t, 1, defaultMaterializerLimits())
			ns, _ := certifiedProcessNamespace(leader)
			part, _ := spec.declaration.Partition(ns.partition)
			binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(spec.declaration.Graph()), Partition: ns.partition, Group: part.Group}, SemanticContractID: f.nativeAgreement()}
			raw, err := replica.DecodeApplicationPacket(binding, packet, processFrameBytes)
			message := &pb.Message{}
			if err == nil {
				err = (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(raw.Payload, message)
			}
			f.t.Logf("paired heal delivered packet%d %d->%d type%v term%d logTerm%d index%d commit%d reject%t hint%d entries%d decode%v", observed, packet.From, packet.To, message.GetType(), message.GetTerm(), message.GetLogTerm(), message.GetIndex(), message.GetCommit(), message.GetReject(), message.GetRejectHint(), len(message.GetEntries()), err)
			for _, entry := range message.GetEntries() {
				prefix := entry.GetData()[:min(5, len(entry.GetData()))]
				f.t.Logf("paired heal actual entry index%d term%d type%v bytes%d prefix%x", entry.GetIndex(), entry.GetTerm(), entry.GetType(), len(entry.GetData()), prefix)
			}

		}
		return priorFilter != nil && priorFilter(packet)
	}
	defer func() { group.dropPacket = priorFilter }()
	sealed := make(map[uint64]bool)
	for round := range 32 {
		f.groupEvent(group, leader, processRequest{op: processTick})
		if catchup == nil {
			f.groupEvent(group, leader, processRequest{op: processCertifiedRead, entity: uint64(observeConfiguration)})
		}
		if len(packetOnly) == 1 {
			// A genuine completed service reply is still held by its original
			// caller. Drain only the SAME native queue/Step path; waiting for all
			// relays would deadlock that live causal predecessor. This mode has
			// no synthetic snapshot or success fallback: native log catch-up must
			// actually finish within the existing finite rounds and queue caps.
			f.drainGroup(group)
		} else {
			f.drain()
			f.snapshotCatchup(leader, sealed)
		}
		base := f.call(f.n.all[leader], processRequest{op: processStateOp, entity: 1})
		if round == 31 {
			f.t.Logf("paired heal final leader%d applied%d ready%t generation%d checkpoint%d term%d commit%d image%x", leader, base.applied, base.ready, base.stats[statGeneration], base.stats[statCheckpoint], base.stats[statTerm], base.stats[statCommit], base.image)
			f.t.Logf("paired heal final leader%d actual raft state%s", leader, base.aux)
		}
		healthy := true
		var voters []nativeProtocolMoveCatchupVoter
		for id, p := range group.nodes {
			f.mu.Lock()
			dead := f.down[id]
			f.mu.Unlock()
			if dead {
				continue
			}
			s := f.call(p, processRequest{op: processStateOp, entity: 1})
			if round == 31 {
				f.t.Logf("paired heal final voter%d applied%d ready%t generation%d checkpoint%d term%d commit%d image%x", id, s.applied, s.ready, s.stats[statGeneration], s.stats[statCheckpoint], s.stats[statTerm], s.stats[statCommit], s.image)
				f.t.Logf("paired heal final voter%d actual raft state%s", id, s.aux)
			}
			if s.applied != base.applied || !bytes.Equal(s.image, base.image) {
				healthy = false
			}
			if catchup != nil {
				if !nativeProtocolMoveCheckpointReady(s) {
					healthy = false
				}
				voters = append(voters, nativeProtocolMoveCatchupVoter{Member: id, PID: uint64(p.cmd.Process.Pid), Instance: pairedGraphFaultInstance(id, p), State: p06ProcessWire(f.t, s)})
			}
		}
		ready := base.ready
		if catchup != nil {
			ready = nativeProtocolMoveCheckpointReady(base) && len(voters) == 3
			// Application selection is already inside committed-entry apply.
			// Ready persistence alone never promises that selected entry survives.
			if catchup.receipt.Control.Kind == raftlog.StableStorageApplication && base.stats[statCommit] < catchup.receipt.Events[0].Index {
				ready = false
			}
		}
		if healthy && ready {
			if catchup != nil {
				catchup.proof = &nativeProtocolMoveCatchupProof{Version: 1, Actor: leader, Arm: catchup.gate.arm, Batch: catchup.receipt, Command: bytes.Clone(catchup.command), ActorState: p06ProcessWire(f.t, base), Voters: voters}
				f.t.Logf("paired native committed/apply catch-up actor%d applied%d", leader, base.applied)
				return
			}
			f.t.Logf("paired actual healthy quorum leader%d applied%d", leader, base.applied)
			return
		}
	}
	f.t.Fatal("paired rejoined voter did not reach actual healthy quorum")
}
func (f *pairedGraphFaultNetwork) elect(prior, next uint64) {
	f.t.Helper()
	restarted := prior == next
	if restarted && f.parent == nil {
		f.t.Fatal("same-member restart election requires the fresh original deadline")
	}
	group := f.n.group(next)
	if restarted {
		// This is the actual replacement, not the still-dead prior OS object.
		// Its own vote/append traffic must reach the live quorum.
		group.blocked = nil
	} else {
		group.blocked = map[uint64]bool{prior: true}
	}
	defer func() { group.blocked = nil }()
	spec := partitionCodecRequest(f.t, 1, defaultMaterializerLimits())
	ns, _ := certifiedProcessNamespace(next)
	part, _ := spec.declaration.Partition(ns.partition)
	binding := raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(ns.graph), Partition: ns.partition, Group: part.Group}, SemanticContractID: f.nativeAgreement()}
	group.dropPacket = func(packet replica.Packet) bool {
		raw, err := replica.DecodeApplicationPacket(binding, packet, processFrameBytes)
		if err != nil {
			f.t.Fatal(err)
		}
		m := &pb.Message{}
		if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(raw.Payload, m); err != nil {
			f.t.Fatal(err)
		}
		return packet.From != next && (m.GetType() == pb.MsgVote || m.GetType() == pb.MsgPreVote)
	}
	defer func() { group.dropPacket = nil }()
	for round := range 64 {
		for id := range group.nodes {
			if id != prior || restarted {
				if restarted {
					f.mu.Lock()
					dead := f.down[id]
					f.mu.Unlock()
					if dead {
						continue
					}
				}
				f.groupEvent(group, id, processRequest{op: processTick})
			}
		}
		f.groupEvent(group, next, processRequest{op: processCampaign})
		ctx, cancel := f.operationContext(10 * time.Second)
		out, err := f.n.all[next].exchange(ctx, processRequest{op: processCertifiedRead, entity: uint64(observeConfiguration)})
		cancel()
		err = errors.Join(err, processResponseError(out))
		if errors.Is(err, replica.ErrUnavailable) {
			continue
		}
		if err != nil {
			f.t.Fatal("paired actual candidate ReadIndex", err)
		}
		group.enqueue(f.t, out)
		f.drainGroup(group)
		if !f.call(f.n.all[next], processRequest{op: processStateOp}).ready {
			f.t.Fatal("paired elected own read incomplete")
		}
		if next <= 3 {
			f.n.leaders[0] = next
		} else {
			f.n.leaders[1] = next
		}
		if restarted {
			f.t.Logf("paired elected actual restarted leader%d round%d", next, round+1)
		} else {
			f.t.Logf("paired elected actual leader%d round%d while%d down", next, round+1, prior)
		}
		return
	}
	f.t.Fatal("paired finite election")
}
func (f *pairedGraphFaultNetwork) reconnect(actor, target uint64) {
	f.t.Helper()
	f.drain()
	f.packetDrain()
	r := f.call(f.n.all[target], processRequest{op: processPairedConnections, entity: actor})
	c := graphCursor{b: r.aux}
	present, sessions := c.tag(), c.u64()
	if c.err != nil || len(c.b) != 0 || present != 0 || sessions != 0 {
		f.t.Fatal("new authenticated connection is not genuinely unused", present, sessions, c.err)
	}
	// Pipe association and parent route change together, only after all owners are
	// quiescent. Existing B transport/sequence is retained inside the child.
	f.call(f.n.all[actor], processRequest{op: processPairedReconnect, data: allocationP08Words(target, 4)})
	hub := f.n.all[actor].mux.hub
	hub.mu.Lock()
	if hub.relayTargets == nil {
		hub.relayTargets = make(map[uint64]map[uint64]uint64)
	}
	hub.relayTargets[actor] = map[uint64]uint64{3: target, 8: 4}
	hub.mu.Unlock()
	f.t.Logf("paired authenticated origin%d fresh A peer%d; B peer4 unchanged", actor, target)
}

type pairedFaultBoundary struct{ Request, Reply []byte }

func pairedFaultIsDecide(wire []byte) bool {
	q, e := decodeGraphSourceRequest(wire)
	if e != nil || q.kind != sourceSubmit {
		return false
	}
	p, e := decodeGraphSourceProposal(q.command)
	if e != nil {
		return false
	}
	c, _, e := decodeCrossCommand(p.wire, defaultMaterializerLimits())
	return e == nil && c.phase == crossDecide
}
func (f *pairedGraphFaultNetwork) gate(actor uint64, after bool) (<-chan pairedFaultBoundary, func()) {
	entered := make(chan pairedFaultBoundary, 1)
	release := make(chan struct{})
	var once sync.Once
	pause := func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		if origin != actor || partition != 8 || !pairedFaultIsDecide(request) {
			return nil
		}
		once.Do(func() { entered <- pairedFaultBoundary{bytes.Clone(request), bytes.Clone(reply)} })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	hub := f.n.all[4].mux.hub
	hub.mu.Lock()
	if after {
		hub.afterRelay = pause
	} else {
		hub.beforeRelay = func(ctx context.Context, o, p uint64, w []byte) error { return pause(ctx, o, p, w, nil) }
	}
	hub.mu.Unlock()
	var end sync.Once
	return entered, func() {
		end.Do(func() { close(release); hub.mu.Lock(); hub.beforeRelay = nil; hub.afterRelay = nil; hub.mu.Unlock() })
	}
}
func (f *pairedGraphFaultNetwork) beginRun(actor uint64, p allocationProposal) (<-chan graphProcessAnswer, context.CancelFunc) {
	f.requireActive()
	ctx, cancel := f.operationContext(20 * time.Second)
	ch := make(chan graphProcessAnswer, 1)
	go func() {
		r, e := f.n.all[actor].exchange(ctx, processRequest{op: processSourceCoordinate, data: p.wire})
		ch <- graphProcessAnswer{response: r, err: e}
	}()
	return ch, cancel
}
func (f *pairedGraphFaultNetwork) awaitBoundary(ch <-chan pairedFaultBoundary) pairedFaultBoundary {
	f.t.Helper()
	if f.parent != nil {
		ctx, cancel := f.operationContext(10 * time.Second)
		defer cancel()
		select {
		case boundary := <-ch:
			f.requireActive()
			return boundary
		case <-ctx.Done():
			f.t.Fatal("actual fault boundary exceeded original deadline", ctx.Err())
			return pairedFaultBoundary{}
		}
	}
	select {
	case b := <-ch:
		return b
	case <-time.After(10 * time.Second):
		f.t.Fatal("actual fault boundary not reached")
	}
	return pairedFaultBoundary{}
}
func (f *pairedGraphFaultNetwork) unknown(ch <-chan graphProcessAnswer) []byte {
	f.t.Helper()
	if f.parent != nil {
		ctx, cancel := f.operationContext(10 * time.Second)
		defer cancel()
		select {
		case actual := <-ch:
			f.requireActive()
			if actual.err == nil {
				f.t.Fatal("killed caller unexpectedly received completion", actual.response)
			}
			wire, err := json.Marshal(map[string]any{"error": actual.err.Error(), "Sequence": actual.response.sequence, "Code": actual.response.code, "CauseMask": actual.response.causeMask, "Detail": actual.response.detail, "ResponseBody": actual.response.aux})
			if err != nil {
				f.t.Fatal(err)
			}
			return wire
		case <-ctx.Done():
			f.t.Fatal("killed caller exceeded original deadline", ctx.Err())
			return nil
		}
	}
	select {
	case a := <-ch:
		if a.err == nil {
			f.t.Fatal("killed caller unexpectedly received completion", a.response)
		}
		raw, _ := json.Marshal(map[string]any{"error": a.err.Error(), "Sequence": a.response.sequence, "Code": a.response.code, "CauseMask": a.response.causeMask, "Detail": a.response.detail, "ResponseBody": a.response.aux})
		return raw
	case <-time.After(10 * time.Second):
		f.t.Fatal("killed caller outstanding reply not retired")
	}
	return nil
}
func (f *pairedGraphFaultNetwork) inventory(base [2]uint64) []pairedFaultRawRecord {
	return pairedRawRecords(f.records(base))
}
func (f *pairedGraphFaultNetwork) replay(actor uint64, name string, p allocationProposal, expected crossProcessResult, base [2]uint64) pairedFaultRetry {
	f.t.Helper()
	before := f.observe(actor, graphReadFresh, graphCut{}, false)
	before.RawRecords = f.inventory(base)
	from := f.indexes()
	r := f.call(f.n.all[actor], processRequest{op: processSourceCoordinate, data: p.wire})
	result, err := decodeGraphProcessCoordinatorResult(r)
	if err != nil || result.decision != expected.decision {
		f.t.Fatal("paired original replay result identity", name, result, expected, err)
	}
	f.drain()
	after := f.observe(actor, graphReadFresh, graphCut{}, false)
	after.RawRecords = f.inventory(base)
	added := f.records(from)
	for _, record := range added {
		if len(record.CDC) != 0 {
			f.t.Fatal("paired original replay duplicated CDC", name)
		}
	}
	if !reflect.DeepEqual(before.View, after.View) {
		f.t.Fatal("paired original replay changed graph")
	}
	raw, e := encodeProcessResponse(r)
	if e != nil {
		f.t.Fatal(e)
	}
	return pairedFaultRetry{Operation: name, EventID: f.event(), Payload: bytes.Clone(p.wire), RawResult: raw, Result: pairedFaultResultOf(result), Before: before, After: after, NewRawRecords: pairedRawRecords(added)}
}
func (f *pairedGraphFaultNetwork) stopAll() []pairedFaultProcess {
	f.t.Helper()
	f.drain()
	f.packetDrain()
	var events []pairedFaultProcess
	for id := uint64(1); id <= 6; id++ {
		p := f.n.all[id]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := p.shutdown(ctx, false)
		cancel()
		if err != nil || p.cmd.ProcessState == nil || !p.cmd.ProcessState.Success() {
			f.t.Fatal("paired cohort graceful stop/reap", id, err)
		}
		wait, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
		if !ok {
			f.t.Fatal("paired missing OS wait status")
		}
		raw, _ := json.Marshal(map[string]any{"Member": id, "PID": p.cmd.Process.Pid, "Instance": pairedGraphFaultInstance(id, p), "Volume": filepath.Join(f.n.dir, fmt.Sprint(id)), "Reaped": true, "Signaled": wait.Signaled(), "Signal": "clean-exit", "WaitStatus": int(wait), "ExitCode": p.cmd.ProcessState.ExitCode(), "Success": p.cmd.ProcessState.Success(), "OOMKilled": nil})
		events = append(events, pairedFaultProcess{EventID: f.event(), Action: "stop", Role: "replica", Member: id, Instance: fmt.Sprintf("voter%d/pid%d/%x", id, p.cmd.Process.Pid, p.incarnation), Volume: filepath.Join(f.n.dir, fmt.Sprint(id)), Reaped: true, Signal: "clean-exit", Raw: raw})
		f.mu.Lock()
		f.down[id] = true
		f.mu.Unlock()
	}
	// Only after every genuine process is reaped do we retire the old pipe hub.
	hub := f.n.all[4].mux.hub
	hub.cancel()
	hub.wg.Wait()
	return events
}
func (f *pairedGraphFaultNetwork) reopenAll() []pairedFaultProcess {
	f.t.Helper()
	var events []pairedFaultProcess
	old := f.n.all
	fresh := make(map[uint64]*graphProcess)
	for id := uint64(1); id <= 6; id++ {
		if !old[id].closed || old[id].cmd.ProcessState == nil {
			f.t.Fatal("paired cohort reopen before complete stop")
		}
		fresh[id] = startGraphProcess(f.t, filepath.Join(f.n.dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_LIFECYCLE_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1", "RHO_GRAPH_PAIRED_PROCESS=1")
		f.incarnations = append(f.incarnations, fresh[id])
		p := fresh[id]
		f.t.Cleanup(func() {
			if !p.closed {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if e := p.shutdown(ctx, false); e != nil {
					f.t.Error("paired final replacement graceful cleanup", e)
				}
			}
		})
	}
	f.retiredBootstrap += f.n.groups[0].delivered + f.n.groups[1].delivered
	attachGraphProcessMuxes(f.t, fresh)
	f.t.Cleanup(func() {
		for id, p := range fresh {
			if !p.closed {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				e := p.shutdown(ctx, false)
				cancel()
				if e != nil {
					f.t.Error("paired reopened-cohort cleanup", id, e)
				}
			}
		}
	})
	f.n.all = fresh
	f.n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: fresh[1], 2: fresh[2], 3: fresh[3]}}
	f.n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: fresh[4], 5: fresh[5], 6: fresh[6]}}
	f.mu.Lock()
	clear(f.down)
	f.mu.Unlock()
	f.installFilter()
	f.elect(3, 1)
	f.elect(6, 4)
	f.heal(1)
	f.heal(4)
	for id := uint64(1); id <= 6; id++ {
		startup, e := encodeProcessResponse(f.call(fresh[id], processRequest{op: processStateOp}))
		if e != nil {
			f.t.Fatal(e)
		}
		raw, _ := json.Marshal(map[string]any{"Member": id, "PID": fresh[id].cmd.Process.Pid, "Instance": pairedGraphFaultInstance(id, fresh[id]), "PreviousInstance": pairedGraphFaultInstance(id, old[id]), "Volume": filepath.Join(f.n.dir, fmt.Sprint(id)), "Ready": true, "ReadyKind": "restored-and-caught-up-under-leader-read-index", "RecoveryRaw": startup})
		events = append(events, pairedFaultProcess{EventID: f.event(), Action: "reopen", Role: "replica", Member: id, Instance: fmt.Sprintf("voter%d/pid%d/%x", id, fresh[id].cmd.Process.Pid, fresh[id].incarnation), PreviousInstance: fmt.Sprintf("voter%d/pid%d/%x", id, old[id].cmd.Process.Pid, old[id].incarnation), Volume: filepath.Join(f.n.dir, fmt.Sprint(id)), Ready: true, Raw: raw, RecoveryRaw: startup})
	}
	return events
}

type pairedFaultPendingDiagnostic struct {
	Pending bool
	Token   []byte
	Error   string
}

func pairedGraphFaultPendingOpen(ctx context.Context, s *graphProcessCertifiedSessions, r processRequest) ([]byte, error) {
	request := r
	request.op = processSourceCertifiedOpen
	token, err := s.serve(ctx, request)
	// This is diagnostic classification of the actual child-owned production
	// error. No bool or encoded data is accepted as a graph/certificate authority.
	raw, e := json.Marshal(pairedFaultPendingDiagnostic{errors.Is(err, errGraphCutPending), token, fmt.Sprint(err)})
	return raw, errors.Join(err, e)
}

// The mux has already correlated this terminal response to the configured child
// pipe, incarnation and original request. Accept only its exact native wire error:
// an errors.AsType match through a joined deadline/EOF would instead admit a
// transport failure. Pending is derived from errGraphCutPending in the child;
// neither an Unavailable mask alone nor a supplied Pending bool proves that.
func pairedGraphFaultPendingReply(r processResponse, transport error, incarnation [16]byte) (pairedFaultIntermediate, error) {
	wireError, exact := transport.(graphWireError)
	code, _ := processError(replica.ErrUnavailable)
	if !exact || wireError.mask != graphErrorMask(replica.ErrUnavailable) || incarnation == ([16]byte{}) || r.incarnation != incarnation || r.sequence == 0 || r.code != code || r.causeMask != processCauseMask(replica.ErrUnavailable) {
		return pairedFaultIntermediate{}, errors.Join(errInvalid, transport)
	}
	var result pairedFaultPendingDiagnostic
	if err := json.Unmarshal(r.aux, &result); err != nil {
		return pairedFaultIntermediate{}, errors.Join(errInvalid, err)
	}
	expected := errors.Join(errGraphCutPending, wireError).Error()
	if !result.Pending || result.Token != nil || result.Error != expected || r.detail != result.Error {
		return pairedFaultIntermediate{}, errInvalid
	}
	canonical, err := json.Marshal(result)
	if err != nil || !bytes.Equal(canonical, r.aux) {
		return pairedFaultIntermediate{}, errors.Join(errInvalid, err)
	}
	return pairedFaultIntermediate{Classification: "pending", Raw: bytes.Clone(r.aux)}, nil
}

func (f *pairedGraphFaultNetwork) pending() pairedFaultIntermediate {
	f.t.Helper()
	f.reads++
	if f.reads > 96 {
		f.t.Fatal("paired admitted certified read operations")
	}
	wire, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadFresh})
	if err != nil {
		f.t.Fatal(err)
	}
	ctx, cancel := f.operationContext(10 * time.Second)
	defer cancel()
	r, transport := f.n.all[6].exchange(ctx, processRequest{op: processPairedPendingOpen, data: wire})
	result, err := pairedGraphFaultPendingReply(r, transport, f.n.all[6].incarnation)
	if err != nil {
		f.t.Fatal("missing exact authenticated terminal pending refusal", err, "sequence", r.sequence, "code", r.code, "causes", r.causeMask, "detail", r.detail, "body", string(r.aux))
	}
	return result
}
func pairedFaultObserver(step pairedFaultStep) pairedFaultObservation {
	return pairedFaultObservation{Cut: step.Cut, View: step.After, RawObservation: step.RawObservation, RawRecords: step.RawRecords}
}
func pairedFaultAssertCDC(t *testing.T, step pairedFaultStep, stage int) {
	t.Helper()
	if stage <= 3 {
		assertLifecycleCDC(t, step.lifecycleStep, stage)
		return
	}
	changes, _, _, values := literalLifecycleCDC(t, 4)
	before, after := pairedFaultLiteral(stage-1), pairedFaultLiteral(stage)
	for _, e := range before.Entities {
		if e.ID == "edge" {
			changes[0].Before = e.Property
		}
	}
	for _, e := range after.Entities {
		if e.ID == "edge" {
			changes[0].After = e.Property
		}
	}
	if stage > 4 {
		values = nil
	}
	if !reflect.DeepEqual(step.LogicalCDC, changes) || len(step.NewEntities) != 0 || len(step.NewLives) != 0 || !reflect.DeepEqual(step.NewValues, values) {
		t.Fatal("paired exact actual property CDC/declarations", stage, step.LogicalCDC, step.NewEntities, step.NewLives, step.NewValues)
	}
}
func (f *pairedGraphFaultNetwork) commitEvent(result crossProcessResult, raw []byte) pairedFaultProcess {
	f.t.Helper()
	id := f.n.leaders[1]
	p := f.n.all[id]
	value := pairedFaultResultOf(result)
	return pairedFaultProcess{EventID: f.event(), Action: "commit", Role: "replica", Member: id, Instance: fmt.Sprintf("voter%d/pid%d/%x", id, p.cmd.Process.Pid, p.incarnation), Volume: filepath.Join(f.n.dir, fmt.Sprint(id)), Raw: raw, Result: &value}
}
func (f *pairedGraphFaultNetwork) changed(p allocationProposal, base [2]uint64) pairedFaultChanged {
	f.t.Helper()
	wire, err := encodeCertifiedProcessProposal(p)
	if err != nil {
		f.t.Fatal(err)
	}
	produced := f.call(f.n.all[4], processRequest{op: processP08ChangedGraph, data: wire})
	changed, err := decodeCertifiedProcessProposal(produced.aux)
	if err != nil || bytes.Equal(changed.wire, p.wire) {
		f.t.Fatal("genuine changed valid payload", err)
	}
	before := f.observe(4, graphReadFresh, graphCut{}, false)
	before.RawRecords = f.inventory(base)
	from := f.indexes()
	ctx, cancel := f.operationContext(10 * time.Second)
	response, exchangeErr := f.n.all[4].exchange(ctx, processRequest{op: processSourceCoordinate, data: changed.wire})
	cancel()
	actual := errors.Join(exchangeErr, processResponseError(response))
	if !errors.Is(actual, errInvalid) {
		f.t.Fatal("same-key changed business accepted", actual)
	}
	f.drain()
	after := f.observe(4, graphReadFresh, graphCut{}, false)
	after.RawRecords = f.inventory(base)
	records := f.records(from)
	for _, r := range records {
		if len(r.CDC) != 0 {
			f.t.Fatal("payload mismatch emitted duplicate graph CDC")
		}
	}
	if !reflect.DeepEqual(before.View, after.View) {
		f.t.Fatal("payload mismatch changed graph")
	}
	raw, e := encodeProcessResponse(response)
	if e != nil {
		f.t.Fatal(e)
	}
	return pairedFaultChanged{Payload: changed.wire, Classification: "mismatch", Raw: raw, Before: before, After: after, NewRawRecords: pairedRawRecords(records)}
}

// Packet filtering is bounded by the exact shared delivery ledger even when a
// scheduled dead peer suppresses its expected delivery. Errors remain errors.
func pairedGraphFaultPacketDisposition(delivered int, filter func(replica.Packet) (bool, error), packet replica.Packet) (bool, error) {
	if delivered > processDeliveries {
		return false, errLimit
	}
	if filter == nil {
		return false, nil
	}
	return filter(packet)
}

func pairedGraphFaultInstance(id uint64, p *graphProcess) string {
	return fmt.Sprintf("voter%d/pid%d/%x", id, p.cmd.Process.Pid, p.incarnation)
}
