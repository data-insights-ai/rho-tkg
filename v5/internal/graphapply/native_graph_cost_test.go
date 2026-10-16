package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type nativeGraphCostSample struct {
	Phase                          string                   `json:"phase"`
	Fact                           uint64                   `json:"fact"`
	Cross                          bool                     `json:"cross"`
	ElapsedNanos                   int64                    `json:"elapsedNanos"`
	ProposalNanos                  int64                    `json:"proposalNanos"`
	AckNanos                       int64                    `json:"ackNanos"`
	DrainNanos                     int64                    `json:"drainNanos"`
	VerificationNanos              int64                    `json:"verificationNanos"`
	ClientAttempts                 int                      `json:"clientAttempts"`
	LocalAckTiming                 nativeGraphCostAckTiming `json:"localAckTiming"`
	Operation                      nativeGraphCostOperation `json:"operation"`
	ClientID                       int                      `json:"clientID"`
	AdmissionNanos                 int64                    `json:"admissionNanos"`
	phase, partition, index, round uint64
	hash, digest                   [32]byte
	tx                             [16]byte
	identity                       [16]byte
	participants                   [2]uint64
}

// These RPC/wait totals partition only the ordinary ACK's measured calls.
// State is a checked volatile progress hint only. Hub queue/Step timings overlap these
// client timings and cannot be added to them or interpreted as pure fsync time.
type nativeGraphCostAckTiming struct {
	ProposeCount, StateCount, PacketWaitCount, EntryRecordCount uint64
	ProposeNanos, StateNanos, PacketWaitNanos, EntryRecordNanos int64
}

const nativeGraphCostAckPollEntity uint64 = 2

// This test-only marker suppresses diagnostics, never supplies ACK authority.
// The sole proof remains the actual original retained Outcome/CDC after polling.
func nativeGraphCostAckPoll(r processRequest, costChild bool) (bool, error) {
	if r.op != processStateOp || r.entity != nativeGraphCostAckPollEntity {
		return false, nil
	}
	if !costChild || r.index != 0 || len(r.data) != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 {
		return false, errInvalid
	}
	return true, nil
}

func (a *nativeGraphCostAckTiming) add(b nativeGraphCostAckTiming) {
	a.ProposeCount += b.ProposeCount
	a.StateCount += b.StateCount
	a.PacketWaitCount += b.PacketWaitCount
	a.EntryRecordCount += b.EntryRecordCount
	a.ProposeNanos += b.ProposeNanos
	a.StateNanos += b.StateNanos
	a.PacketWaitNanos += b.PacketWaitNanos
	a.EntryRecordNanos += b.EntryRecordNanos
}

type nativeGraphCostResult struct {
	Version                                                                          int                        `json:"version"`
	Engine                                                                           string                     `json:"engine"`
	Layout                                                                           string                     `json:"layout,omitempty"`
	ColumnInitialCut                                                                 lifecycleRawCut            `json:"columnInitialCut,omitzero"`
	ColumnScope                                                                      string                     `json:"columnScope,omitempty"`
	ColumnGenerations                                                                [2][2]nativeColumnManifest `json:"columnGenerations,omitzero"`
	Spec                                                                             nativeGraphCostSpec        `json:"spec"`
	Deployment                                                                       string                     `json:"deployment"`
	Directory                                                                        string                     `json:"directory"`
	UsefulFacts                                                                      uint64                     `json:"usefulFacts"`
	SetupNodes                                                                       int                        `json:"setupNodes"`
	GrantBlockSize                                                                   uint64
	IDAdmission                                                                      string
	Creates, Updates                                                                 uint64
	SetupNanos, WritesNanos, VerificationNanos, TotalNanos                           int64
	PersistedFileBytes                                                               uint64 `json:"persistedFileBytes"`
	AllocatedFileBytes                                                               uint64 `json:"allocatedFileBytes"`
	OldCut, FinalCut                                                                 lifecycleRawCut
	CDCFrom, CDCThrough                                                              [2]uint64
	CDCChanges, CDCRecords                                                           uint64
	CompletedCreates, CompletedUpdates                                               uint64
	Reaped                                                                           int                                   `json:"reaped"`
	Runner                                                                           string                                `json:"runner"`
	ClientCompletions                                                                [nativeGraphCostWindowCommands]uint64 `json:"clientCompletions"`
	ClientWindow, LocalAdmissions, CrossAdmissions                                   int
	AdmissionScope                                                                   string
	AdmissionNanos, PhaseWallNanos                                                   int64
	Attempts, AbortedAttempts, SameAttemptRetries                                    uint64
	CheckpointCommands                                                               int                      `json:"checkpointCommands"`
	CheckpointNanos                                                                  int64                    `json:"checkpointNanos"`
	LocalAckTiming                                                                   nativeGraphCostAckTiming `json:"localAckTiming"`
	TransportTiming                                                                  graphProcessCostTiming   `json:"transportTiming"`
	TimingScope                                                                      string                   `json:"timingScope"`
	InitialReaped, ProcessInstances, GrantReplayCommands                             int
	ReopenQualified                                                                  bool
	ReopenPrefixRecords                                                              uint64
	PreReopenVerificationNanos, InitialStopNanos, FinalStopNanos, ReopenRestoreNanos int64
	ReopenAuthorityNanos, ReopenVerificationNanos, GrantReplayNanos                  int64
	RestartTransportTiming                                                           graphProcessCostTiming
}

func newNativeGraphCostNetworkWithBlock(t *testing.T, spec nativeGraphCostSpec, block uint64) *certifiedProcessNetwork {
	t.Helper()
	if err := spec.validate(); err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-native-cost-")
	if err != nil {
		t.Fatal(err)
	}
	grants, err := os.OpenFile(filepath.Join(dir, "grants.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := grants.Close(); err != nil {
			t.Error("cost grant receipt close", err)
		}
	}()
	grantStream := json.NewEncoder(grants)
	n := &certifiedProcessNetwork{all: make(map[uint64]*graphProcess), dir: dir, leaders: [2]uint64{1, 4}}
	t.Log("native cost durable directories", dir)
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_COST_SPEC="+string(wire), "RHO_GRAPH_COST_BLOCK_SIZE="+strconv.FormatUint(block, 10))
	}
	attachGraphProcessMuxesWithLimit(t, n.all, spec.deliveries())
	hub := n.all[1].mux.hub
	hub.mu.Lock()
	hub.costStarted = time.Now()
	hub.costTiming = new(graphProcessCostTiming)
	hub.mu.Unlock()
	t.Cleanup(func() {
		for id := uint64(1); id <= 6; id++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := n.all[id].shutdown(ctx, t.Failed())
			cancel()
			if err != nil && !t.Failed() {
				t.Error("native cost cleanup", id, err)
			}
		}
	})
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
	for g, id := range []uint64{1, 4} {
		n.groups[g].event(t, id, processRequest{op: processCampaign})
	}
	n.action(t, 1, certifiedInitialize, 3, nil)
	n.read(t, 1, observeConfiguration, 0)
	n.action(t, 1, certifiedInitialize, 8, nil)
	for _, r := range []struct{ actor, id, partition uint64 }{{1, 1, 3}, {4, 2, 8}} {
		n.read(t, r.actor, observeConfiguration, 0)
		n.action(t, r.actor, certifiedBeginRecipient, r.id, nil)
		n.read(t, 1, observeRecipient, r.id)
		n.action(t, 1, certifiedRecipientTransition, 3, []byte{byte(fenceDeclaredRecipient)})
		if r.partition != 3 {
			n.action(t, 1, certifiedRecipientTransition, r.partition, []byte{byte(fenceDeclaredRecipient)})
		}
		n.read(t, r.actor, observeRecipient, r.id)
		n.action(t, r.actor, certifiedRecipientTransition, 3, []byte{byte(acknowledgeDeclaredHome)})
		n.read(t, 1, observeRecipient, r.id)
		n.action(t, 1, certifiedRecipientTransition, 3, []byte{byte(activateDeclaredRecipient)})
		n.read(t, 1, observeRecipient, r.id)
		n.action(t, 1, certifiedRecipientTransition, r.partition, []byte{byte(publishDeclaredRecipient)})
		n.read(t, r.actor, observeRecipient, r.id)
		n.action(t, r.actor, certifiedOpenRecipient, 0, nil)
		nativeGraphCostInstallGrants(t, n, spec, r.id, r.partition, block, grantStream)
	}
	return n
}
func nativeGraphCostOpen(t *testing.T, n *certifiedProcessNetwork, mode graphReadMode, cut graphCut) (uint64, graphCut) {
	t.Helper()
	wire, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: cut})
	if err != nil {
		t.Fatal(err)
	}
	response := n.all[2].call(t, processRequest{op: processSourceCertifiedOpen, data: wire})
	handle, actual, err := decodeGraphE15ReadToken(response.aux)
	if err != nil || len(actual.certificates) != 2 {
		t.Fatal("cost actual certified cut", err)
	}
	for _, c := range actual.certificates {
		if c.scope.semantic != certifiedGraphSemanticContractID() {
			t.Fatal("cost cut agreement")
		}
	}
	return handle, actual
}

// One operation context covers proposal preparation and every original exchange
// through its actual ACK. The mux separately bounds canceled-worker cleanup.
func nativeGraphCostCall(t *testing.T, ctx context.Context, p *graphProcess, request processRequest) processResponse {
	t.Helper()
	response, err := p.exchange(ctx, request)
	err = errors.Join(err, processResponseError(response), ctx.Err())
	if err != nil {
		t.Fatalf("cost composed operation op=%d: %v", request.op, err)
	}
	return response
}

// Acknowledgement is the actual immutable committed outcome, not Propose's
// enqueue response. Transport settling and voter/oracle inspection follow the
// timed acknowledgement separately; both remain in the measured total cost.
func nativeGraphCostSubmit(t *testing.T, n *certifiedProcessNetwork, phase, i uint64, cross bool) nativeGraphCostSample {
	t.Helper()
	sample, err := nativeGraphCostClientSubmit(t.Context(), n, nativeGraphCostSpec{}, phase, i, cross, 0, nil, nil)
	if err != nil {
		t.Fatal("cost original Submit", err)
	}
	return sample
}

// A checkpoint first obtains a real two-partition certified cut. The live
// drivers can be ahead because certification writes control entries; every
// page instead checks a genuine retained root at certificate.sourceIndex.
func nativeGraphCostCheckpoint(t *testing.T, n *certifiedProcessNetwork, from *[2]uint64, stream *json.Encoder, spec nativeGraphCostSpec, commands []nativeGraphCostSample) (graphCut, uint64, uint64) {
	t.Helper()
	if len(commands) == 0 || len(commands) > nativeGraphCostWindowCommands {
		t.Fatal("cost checkpoint command bound")
	}
	handle, cut := nativeGraphCostOpen(t, n, graphReadFresh, graphCut{})
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	axis := graphGenesisBinding(t).Axis()
	scope, err := temporal.All(axis)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := temporal.AppendScope(nil, scope, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	expectedScope, err := lifecycleCanonicalScope(wire, axis, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	seen := make([]byte, len(commands))
	var changes, records uint64
	for g, leader := range n.leaders {
		partition := uint64(3)
		if g == 1 {
			partition = 8
		}
		var certificate graphCertificate
		for _, c := range cut.certificates {
			if c.scope.partition == partition {
				certificate = c
			}
		}
		end := certificate.sourceIndex
		if end < from[g] || end-from[g] > uint64(len(commands)*128+16) {
			t.Fatal("bounded checkpoint retained prefix", from[g], end)
		}
		for {
			request := processRequest{op: processCostRecords, index: from[g], entity: end, data: []byte{1}}
			response := n.all[leader].call(t, request)
			page, err := nativeGraphCostDecodePage(response.aux, partition, from[g], certificate)
			if err != nil {
				t.Fatal("actual typed retained page", err)
			}
			request.data[0] = 0
			for id, voter := range n.groups[g].nodes {
				if id == leader {
					continue
				}
				response := voter.call(t, request)
				actual, err := nativeGraphCostDecodePage(response.aux, partition, from[g], certificate)
				if err != nil {
					t.Fatal("exact retained voter certificate boundary", id, err)
				}
				for i, original := range page.Records {
					if !bytes.Equal(actual.Records[i].Outcome, original.Outcome) || !bytes.Equal(actual.Records[i].CDC, original.CDC) {
						t.Fatal("cost raw voter original/CDC mismatch", id, original.Index)
					}
				}
			}
			for _, record := range page.Records {
				if err := nativeGraphCostVerifyRecord(spec, commands, seen, record.nativeLifecycleRecord, expectedScope); err != nil {
					t.Fatal("exact batched original CDC", err, record.Index)
				}
				if err := stream.Encode(record); err != nil {
					t.Fatal(err)
				}
				records++
				changes += uint64(len(record.Changes))
			}
			from[g] = page.Next
			if page.Next == end {
				break
			}
		}
	}
	if err := nativeGraphCostComplete(commands, seen); err != nil {
		t.Fatal("cost exact checkpoint command cardinality", err)
	}
	return cut, changes, records
}

// Low bits retain exact component cardinality; the two high bits separately
// require every original manifest participant, even when its business CDC is empty.
func nativeGraphCostComplete(commands []nativeGraphCostSample, seen []byte) error {
	if len(commands) == 0 || len(commands) > nativeGraphCostWindowCommands || len(seen) != len(commands) {
		return errInvalid
	}
	for i, command := range commands {
		if command.phase < 1 || command.phase > 5 {
			return errInvalid
		}
		want := byte(1 << byte(graphstate.Presence))
		if command.phase == 4 {
			want |= 1 << byte(graphstate.ScalarProperty)
		}
		if command.phase == 5 {
			want = 1 << byte(graphstate.ScalarProperty)
		}
		if command.Cross {
			if command.participants[0] == 0 {
				return errInvalid
			}
			for j, partition := range command.participants {
				if partition == 0 {
					continue
				}
				if partition != 3 && partition != 8 || j > 0 && partition <= command.participants[j-1] {
					return errInvalid
				}
				want |= 1 << (6 + j)
			}
		}
		if seen[i] != want {
			return errCorrupt
		}
	}
	return nil
}

func nativeGraphCostVerifyRecord(spec nativeGraphCostSpec, commands []nativeGraphCostSample, seen []byte, record nativeLifecycleRecord, scope lifecycleAllScope) error {
	if len(commands) == 0 || len(commands) > nativeGraphCostWindowCommands || len(seen) != len(commands) {
		return errInvalid
	}
	// A zero-byte control CDC is not a canonical nonempty business envelope.
	if len(record.CDC) == 0 {
		if len(record.Changes)+len(record.Entities)+len(record.Lives)+len(record.Values) != 0 {
			return errCorrupt
		}
		return nil
	}
	matched := -1
	var bound outcome
	var participantBit byte
	for i, command := range commands {
		var original outcome
		var bit byte
		if command.Cross {
			status, err := decodeCrossStatus(record.Outcome)
			if err != nil || status.phase != crossResolve || !status.commit || status.index != record.Index || status.ns != (namespace{graph: idalloc.GraphID{1}, partition: record.Partition}) || status.tx != command.tx || status.digest != command.digest || status.round != command.round {
				continue
			}
			for j, partition := range command.participants {
				if partition != 0 && partition == record.Partition {
					bit = 1 << (6 + j)
				}
			}
			if bit == 0 {
				continue
			}
			original = outcome{ns: status.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
		} else {
			var err error
			original, err = decodePartitionGraphOutcome(record.Outcome, namespace{graph: idalloc.GraphID{1}, partition: command.partition})
			if err != nil || record.Partition != command.partition || original.identity != command.identity || original.hash != command.hash || original.index != command.index || record.Index != command.index || original.reason != reasonNone || original.disposition != applied {
				continue
			}
		}
		if matched >= 0 {
			return errCorrupt
		}
		matched, bound, participantBit = i, original, bit
	}
	if matched < 0 || participantBit != 0 && seen[matched]&participantBit != 0 {
		return errCorrupt
	}
	if len(record.Changes) == 0 {
		logical, err := decodeTypedChangeEnvelope(record.CDC, bound, defaultMaterializerLimits(), true)
		if err != nil || logical.nonempty() || len(record.Entities)+len(record.Lives)+len(record.Values) != 0 {
			return errors.Join(errCorrupt, err)
		}
	}
	command := commands[matched]
	for _, change := range record.Changes {
		if !nativeGraphCostChangeMatches(spec, command.phase, command.Fact, change, scope) || change.Component >= 6 {
			return errCorrupt
		}
		bit := byte(1 << change.Component)
		if seen[matched]&bit != 0 {
			return errCorrupt
		}
		seen[matched] |= bit
	}
	seen[matched] |= participantBit
	return nil
}
func nativeGraphCostChangeMatches(s nativeGraphCostSpec, phase, fact uint64, c nativeLifecycleChange, expectedScope lifecycleAllScope) bool {
	if c.AllScope != expectedScope || len(c.Scope) == 0 || !c.After.Present || c.After.Null || c.After.Revision == 0 {
		return false
	}
	owner, life, _, _ := s.edge(fact)
	provenance := 4 + fact
	if phase <= 3 {
		owner, life, provenance = 1, 2, phase
		if phase == 2 {
			owner, life = 3, 4
		}
		if phase == 3 {
			owner, life = s.remoteNode(), s.remoteNode()+1
		}
	}
	if phase == 5 {
		provenance += s.Facts
	}
	if c.Owner != owner || c.After.Provenance != provenance {
		return false
	}
	if c.Component == byte(graphstate.Presence) {
		return phase <= 4 && c.Life == 0 && c.Name == "" && c.After.Kind == "life" && c.After.Handle == life && c.Before == (nativeLifecycleCell{Kind: "absent"})
	}
	if c.Component != byte(graphstate.ScalarProperty) || phase < 4 || c.Life != life || c.Name != "p" || c.After.Kind != "i64" {
		return false
	}
	if phase == 4 {
		return c.After.Handle == 5 && c.After.I64 == 5 && c.Before == (nativeLifecycleCell{Kind: "absent"})
	}
	return c.After.Handle == 6 && c.After.I64 == 6 && c.Before.Present && !c.Before.Null && c.Before.Kind == "i64" && c.Before.Handle == 5 && c.Before.I64 == 5 && c.Before.Provenance == 4+fact && c.Before.Revision > 0 && c.After.Revision > c.Before.Revision
}
func nativeGraphCostCheck(t *testing.T, n *certifiedProcessNetwork, s nativeGraphCostSpec, cut graphCut, want int64, stream *json.Encoder) {
	t.Helper()
	for i := uint64(0); i < s.Facts; i++ {
		// A new bounded operation owns its own read ledger; old cuts are genuine
		// persisted certificates, never substituted by current state or a census.
		handle, actual := nativeGraphCostOpen(t, n, graphReadAt, cut)
		if !reflect.DeepEqual(actual, cut) {
			t.Fatal("cost At substituted cut")
		}
		id, life, target, _ := s.edge(i)
		response := n.all[2].call(t, processRequest{op: processCostObserve, index: handle, entity: id})
		n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
		var entity nativeLifecycleEntity
		if err := json.Unmarshal(response.aux, &entity); err != nil {
			t.Fatal(err)
		}
		if entity.ID != id || !entity.Found || !entity.Effective || entity.Kind != byte(graphstate.Relationship) || entity.EffectiveLife != life || entity.Source != 1 || entity.Target != target || entity.Type != "edge" || !entity.Property.Present || entity.Property.Kind != "i64" || entity.Property.I64 != want || entity.Property.Provenance != 4+i+boolUint(want == 6)*s.Facts {
			t.Fatal("exact cost retained oracle", i, want, entity)
		}
		if err := stream.Encode(entity); err != nil {
			t.Fatal(err)
		}
	}
	// Immediately outside the allocated edge interval is an unused value ID.
	handle, _ := nativeGraphCostOpen(t, n, graphReadAt, cut)
	response := n.all[2].call(t, processRequest{op: processCostObserve, index: handle, entity: 5})
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	var phantom nativeLifecycleEntity
	if err := json.Unmarshal(response.aux, &phantom); err != nil || phantom.Found || phantom.Effective || phantom.Property.Present {
		t.Fatal("cost phantom entity", phantom, err)
	}
}
func boolUint(v bool) uint64 {
	if v {
		return 1
	}
	return 0
}

// Both byte measures include every owned voter directory after clean Close:
// file lengths and actual POSIX allocated blocks. They exclude trace outputs.
func nativeGraphCostFileBytes(dir string) (uint64, uint64, error) {
	var total, allocated uint64
	var walk func(string, os.FileInfo) error
	walk = func(path string, info os.FileInfo) (err error) {
		if info.IsDir() {
			f, openErr := os.Open(path)
			if openErr != nil {
				return openErr
			}
			defer func() { err = errors.Join(err, f.Close()) }()
			for {
				// Directory order cannot affect these checked commutative sums.
				// Keep only 32 real entries per active directory, rather than
				// WalkDir's whole flat native column-cache file inventory.
				entries, readErr := f.Readdir(32)
				for _, entry := range entries {
					if err := walk(filepath.Join(path, entry.Name()), entry); err != nil {
						return err
					}
				}
				if errors.Is(readErr, io.EOF) {
					return nil
				}
				if readErr != nil {
					return readErr
				}
			}
		}
		// Readdir/Lstat retain the former WalkDir symlink behavior: charge the
		// link itself and do not follow a link into another directory/tree.
		stats, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errInvalid
		}
		if info.Size() < 0 || stats.Blocks < 0 {
			return errCorrupt
		}
		total, allocated, err = nativeGraphCostAccumulateFileBytes(total, allocated, uint64(info.Size()), uint64(stats.Blocks)) // #nosec G115 -- actual nonnegative POSIX sizes/blocks checked above.
		return err
	}
	info, err := os.Lstat(dir)
	if err == nil {
		err = walk(dir, info)
	}
	return total, allocated, err
}

func nativeGraphCostAccumulateFileBytes(total, allocated, size, blocks uint64) (uint64, uint64, error) {
	if blocks > ^uint64(0)/512 || size > ^uint64(0)-total || blocks*512 > ^uint64(0)-allocated {
		return total, allocated, errLimit
	}
	return total + size, allocated + blocks*512, nil
}
func runNativeGraphCost(t *testing.T, s nativeGraphCostSpec) {
	runNativeGraphCostWithBlock(t, s, min(s.homeCount(), idalloc.MaxBlockSize))
}
func runNativeGraphCostWithBlock(t *testing.T, s nativeGraphCostSpec, block uint64) {
	t.Helper()
	layout, err := nativeGraphCostLayout()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	n := newNativeGraphCostNetworkWithBlock(t, s, block)
	result := nativeGraphCostResult{Version: 1, Runner: "independent-clients-request-bound-ack-v2", CheckpointCommands: nativeGraphCostWindowCommands, Engine: "native-raft", Spec: s, Deployment: "single-machine distributed: two three-voter durable process groups", Directory: n.dir, UsefulFacts: s.Facts, SetupNodes: 3, Creates: s.Facts, Updates: s.Facts}
	if layout == "columns" {
		result.Version, result.Runner, result.Layout = 2, "independent-clients-request-bound-ack-columns-v1", layout
	}
	result.ClientWindow, result.LocalAdmissions, result.CrossAdmissions = nativeGraphCostWindowCommands, nativeGraphCostLocalAdmissions, 1
	if s.Writers == 1 {
		result.ClientWindow = nativeGraphCostBatchCommands
		result.CheckpointCommands = nativeGraphCostBatchCommands
	}
	result.AdmissionScope = "Actual independent clients; at most eight local Submit admissions or one exclusive cross Coordinate admission. Cross admission leaves the second source worker for authenticated service. Native event lane and packetLoop remain serial; unchanged pending16/sourceWorkers2. Operation deadlines include admission wait."
	result.GrantBlockSize = block
	result.IDAdmission = "fixed fixture IDs admitted by actual installed allocator/recipient grants and fresh bindings; no Recipient.Acquire or Cursor.Next issuance"
	result.TimingScope = "WritesNanos sums overlapping client elapsed times including admission and bounded artifact handoff; PhaseWallNanos is actual phase wall time. AdmissionNanos is a fixed aggregate, attempts.jsonl streams actual immutable attempt lineage; no whole-run map. Local request lookup selects the actual retained original mapping, never the latest applied entry. A Step broadcast wakes every waiting client but is never authority. Reopen timings cover pre-stop index/prefix verification, initial six-child stop, six-directory native restore/prefix comparison, completed native configuration ReadIndex, original At verification and bounded original grant replay separately. TransportTiming ends before original-cohort stop; RestartTransportTiming is a separate overlapping second-cohort total. Frozen CDCFrom/CDCThrough and write/ACK timers exclude election/replay protocol tails. LocalAckTiming covers setup/create/update ordinary Submit RPCs and waits; State is a cost-only checked volatile Driver.Applied hint without Checkpoint/full stats, never ACK authority. Original retained Outcome/CDC alone proves ACK. Cost non-State replies omit unrequested checkpoint/full statistics but preserve native results and store errors; explicit default State diagnostics and all non-cost replies remain full. TransportTiming covers all queued hub traffic including setup/checkpoints/oracles, overlaps client timings, and Step includes IPC plus native driver/application work, not pure fsync. Fixed-size totals and one int64 timestamp per queued job preserve the existing 64-byte job allowance."
	// Source records and observations are streamed, never accumulated per fact.
	open := func(name string) (*os.File, *json.Encoder) {
		f, err := os.OpenFile(filepath.Join(n.dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := f.Close(); err != nil {
				t.Error(err)
			}
		})
		return f, json.NewEncoder(f)
	}
	_, samples := open("latency.jsonl")
	_, attempts := open("attempts.jsonl")
	_, records := open("cdc.jsonl")
	_, observations := open("observations.jsonl")
	_, cohorts := open("cohorts.jsonl")
	var columns *json.Encoder
	var initial graphCut
	var columnPairs [2]nativeGraphCostColumnPair
	if layout == "columns" {
		_, columns = open("native-cost-columns.jsonl")
		// A genuine Fresh initial cut precedes every business row. Protocol
		// certification tails remain outside the existing business CDC baseline.
		handle, cut := nativeGraphCostOpen(t, n, graphReadFresh, graphCut{})
		n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
		initial = cut
		result.ColumnInitialCut, err = lifecycleRawCertificate(cut)
		if err != nil {
			t.Fatal(err)
		}
		result.ColumnScope = "Fixed V2 typed native change columns: real partition-local publishers in voters1/4, explicit disjoint masks and exact full-cut/config/peer/policy/generation join of both genuine sealed source EOFs. Native retain-all source remains authoritative; immutable generation and finite cache files are additional owned child storage included in six-directory disk and process costs. Per-chunk raw originals, numeric columns and cache/native parity are streamed; no whole-history materialization or parent Host. Local seals require joined writers and local native source quiescence, not a simultaneous global transaction. One-minute held source ownership is the current comparison boundary; durable pin-backed long-query integration remains required. No derived GC. Prepaid counters are theoretical quotas, separate from actual payload/peer frames and process RSS."
	}
	var from [2]uint64
	for g, id := range n.leaders {
		from[g] = n.all[id].call(t, processRequest{op: processStateOp}).applied
	}
	result.CDCFrom = from
	for phase := uint64(1); phase <= 3; phase++ {
		command := nativeGraphCostSubmit(t, n, phase, 0, false)
		result.LocalAckTiming.add(command.LocalAckTiming)
		_, c, r := nativeGraphCostCheckpoint(t, n, &from, records, s, []nativeGraphCostSample{command})
		result.CDCChanges += c
		result.CDCRecords += r
	}
	result.SetupNanos = time.Since(started).Nanoseconds()
	var old, final graphCut
	for _, phase := range []uint64{4, 5} {
		phaseStarted := time.Now()
		cut := nativeGraphCostRunPhase(t, n, s, phase, &from, samples, attempts, records, &result, started)
		result.PhaseWallNanos += time.Since(phaseStarted).Nanoseconds()

		if phase == 4 {
			old = cut
			var err error
			result.OldCut, err = lifecycleRawCertificate(cut)
			if err != nil {
				t.Fatal(err)
			}
			if columns != nil {
				columnPairs[0] = nativeGraphCostColumnPublish(t, n, s, initial, old, [16]byte{1}, "old-seal", true, false, columns)
				result.ColumnGenerations[0] = columnPairs[0].Manifests
			}
		} else {
			var err error
			final = cut
			result.FinalCut, err = lifecycleRawCertificate(cut)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			result.CDCThrough = from
			nativeGraphCostCheck(t, n, s, old, 5, observations)
			nativeGraphCostCheck(t, n, s, cut, 6, observations)
			result.VerificationNanos = time.Since(start).Nanoseconds()
			if columns != nil {
				columnPairs[1] = nativeGraphCostColumnPublish(t, n, s, initial, final, [16]byte{2}, "final-seal", false, true, columns)
				result.ColumnGenerations[1] = columnPairs[1].Manifests
				nativeGraphCostColumnReacquire(t, n, s, &columnPairs[0], "old-after-mutation", columns, false)
			}
		}
	}
	if s.Facts > uint64(s.Writers) {
		for client := range s.Writers {
			if result.ClientCompletions[client] == 0 {
				t.Fatal("declared independent client never completed", client)
			}
		}
	}
	if result.CDCChanges != 3+3*s.Facts {
		t.Fatal("exact useful CDC cardinality", result.CDCChanges, 3+3*s.Facts)
	}
	nativeGraphCostReopen(t, n, s, block, old, final, &result, cohorts)
	if columns != nil {
		for i := range columnPairs {
			for _, cached := range []bool{false, true} {
				nativeGraphCostColumnReacquire(t, n, s, &columnPairs[i], fmt.Sprintf("reopen-%d-cached-%t", i, cached), columns, cached)
			}
		}
	}
	result.RestartTransportTiming = n.all[1].mux.hub.costTimingSnapshot()
	stopStarted := time.Now()
	result.Reaped = nativeGraphCostStop(t, n, 2, cohorts)
	result.FinalStopNanos = time.Since(stopStarted).Nanoseconds()
	if !result.ReopenQualified || result.InitialReaped != 6 || result.ProcessInstances != 12 || result.Reaped != 6 {
		t.Fatal("cost complete restart qualification missing")
	}
	for id := uint64(1); id <= 6; id++ {
		size, allocated, err := nativeGraphCostFileBytes(filepath.Join(n.dir, fmt.Sprint(id)))
		if err != nil {
			t.Fatal(err)
		}
		result.PersistedFileBytes += size
		result.AllocatedFileBytes += allocated
	}
	result.TotalNanos = time.Since(started).Nanoseconds()
	transport := result.TransportTiming
	if transport.QueueCount == 0 || transport.StepCount == 0 || transport.StepCount > transport.QueueCount || transport.QueueNanos < transport.QueueMaxNanos || transport.QueueMaxNanos < 0 || transport.StepNanos < transport.StepMaxNanos || transport.StepMaxNanos <= 0 {
		t.Fatal("completed cost transport timing counts/scope", transport)
	}
	wire, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(n.dir, "native-cost-result.json"), append(wire, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("native cost actual result", filepath.Join(n.dir, "native-cost-result.json"))
}
func TestNativeGraphCostSmallActualProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, cell := range []struct {
		name         string
		facts, cross uint64
		layout       string
	}{{"0", 17, 0, "kv"}, {"100", 17, 100, "kv"}, {"two-facts-local", 2, 0, "kv"}, {"two-facts-cross", 2, 100, "kv"}, {"two-facts-columns", 2, 100, "columns"}} {
		t.Run(cell.name, func(t *testing.T) {
			t.Setenv("RHO_GRAPH_COST_LAYOUT", cell.layout)
			runNativeGraphCost(t, nativeGraphCostSpec{Version: 1, Facts: cell.facts, CrossPercent: cell.cross, Writers: 1})
		})
	}
}

func TestNativeGraphCostAckPollMarkerRequiresCostChildAndKeepsDefaultDiagnostics(t *testing.T) {
	for _, cost := range []bool{false, true} {
		env := []string{"RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1"}
		if cost {
			env = append(env, `RHO_GRAPH_COST_SPEC={"version":1,"facts":17,"crossPercent":0,"writers":1}`)
		}
		p := startGraphProcess(t, filepath.Join(t.TempDir(), "store"), 1, env...)
		attachGraphProcessMuxes(t, map[uint64]*graphProcess{1: p})
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = p.shutdown(ctx, t.Failed())
		})
		full := p.call(t, processRequest{op: processStateOp})
		if len(full.image) == 0 || full.stats[statGeneration] == 0 || len(full.handles) != 2 {
			t.Fatal("default State lost full diagnostics", cost, full)
		}
		// Genuine native work retains its normal result/error framing. Only the
		// explicit cost embedding omits unrequested diagnostic metadata.
		tick := p.call(t, processRequest{op: processTick})
		if tick.applied != full.applied || tick.ready != full.ready || cost && (tick.stats != ([statCount]uint64{}) || tick.cutID != ([32]byte{}) || tick.manifestID != ([32]byte{})) || !cost && tick.stats[statGeneration] == 0 {
			t.Fatal("cost/non-cost native reply diagnostic contract", cost, tick)
		}
		marked := processRequest{op: processStateOp, entity: 2}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		light, err := p.exchange(ctx, marked)
		cancel()
		err = errors.Join(err, processResponseError(light))
		if !cost {
			if !errors.Is(err, errInvalid) {
				t.Fatal("non-cost child accepted ACK-only marker", light, err)
			}
		} else {
			if err != nil || light.applied != full.applied || light.ready != full.ready || len(light.image)+len(light.handles)+len(light.aux)+len(light.outcome)+len(light.changes) != 0 || light.stats != ([statCount]uint64{}) || light.cutID != ([32]byte{}) || light.manifestID != ([32]byte{}) {
				t.Fatal("cost progress hint acquired diagnostics/authority", light, err)
			}
			for _, mutate := range []func(*processRequest){
				func(r *processRequest) { r.index = 1 },
				func(r *processRequest) { r.data = []byte{1} },
				func(r *processRequest) { r.packet.From = 1 },
				func(r *processRequest) { r.packet.To = 1 },
				func(r *processRequest) { r.packet.Snapshot = true },
				func(r *processRequest) { r.packet.Payload = []byte{1} },
			} {
				bad := marked
				mutate(&bad)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				response, err := p.exchange(ctx, bad)
				cancel()
				if !errors.Is(errors.Join(err, processResponseError(response)), errInvalid) {
					t.Fatal("malformed cost progress marker accepted", response, err)
				}
			}
		}
		ctx, cancel = context.WithTimeout(t.Context(), 10*time.Second)
		if err := p.shutdown(ctx, false); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
}

func TestNativeGraphCostCell(t *testing.T) {
	if testing.Short() {
		t.Skip("explicit cost cell is excluded from routine short CI")
	}
	spec, err := nativeGraphCostSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if spec == nil {
		t.Skip("explicit cost-cell profile required")
	}
	if spec.Facts != 10000 && spec.Facts != 100000 && spec.Facts != 1000000 {
		t.Fatal("benchmark facts outside reviewed matrix")
	}
	runNativeGraphCost(t, *spec)
}
func TestNativeGraphCostProfileAndRouting(t *testing.T) {
	// Compact actual filesystem parity: nested native/cache files and the
	// symlink's own metadata are included, never the outside target tree.
	dir := t.TempDir()
	nested := filepath.Join(dir, "native-column-cache")
	if err := os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	files := []string{filepath.Join(dir, "root.bin"), filepath.Join(nested, "chunk.bin")}
	for _, file := range files {
		if err := os.WriteFile(file, []byte("owned"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(nested, "outside-link")
	if err := os.Symlink(filepath.Join(t.TempDir(), "unowned"), link); err != nil {
		t.Fatal(err)
	}
	files = append(files, link)
	var expected, allocated uint64
	for _, file := range files {
		info, err := os.Lstat(file)
		if err != nil {
			t.Fatal(err)
		}
		stats, ok := info.Sys().(*syscall.Stat_t)
		if !ok || info.Size() < 0 || stats.Blocks < 0 {
			t.Fatal("actual POSIX metadata")
		}
		expected += uint64(info.Size())         // #nosec G115 -- actual nonnegative size checked above.
		allocated += uint64(stats.Blocks) * 512 // #nosec G115 -- actual small nonnegative POSIX block count checked above.
	}
	if bytes, blocks, err := nativeGraphCostFileBytes(dir); err != nil || bytes != expected || blocks != allocated {
		t.Fatal("streamed owned filesystem cost", bytes, blocks, expected, allocated, err)
	}
	if total, allocated, err := nativeGraphCostAccumulateFileBytes(^uint64(0), 0, 1, 0); !errors.Is(err, errLimit) || total != ^uint64(0) || allocated != 0 {
		t.Fatal("file byte sum overflow", total, allocated, err)
	}
	if _, _, err := nativeGraphCostAccumulateFileBytes(0, 0, 0, ^uint64(0)/512+1); !errors.Is(err, errLimit) {
		t.Fatal("file block product overflow", err)
	}
	if size := reflect.TypeFor[graphProcessPacketJob]().Size(); size > 64 {
		t.Fatal("cost timestamp exceeds existing queued-job admission allowance", size)
	}
	if timing := (&graphProcessHub{}).costTimingSnapshot(); timing != (graphProcessCostTiming{}) {
		t.Fatal("ordinary fixtures enabled cost timings", timing)
	}
	for _, pct := range []uint64{0, 10, 50, 100} {
		s := nativeGraphCostSpec{Version: 1, Facts: 10000, CrossPercent: pct, Writers: 1}
		var cross uint64
		for i := uint64(0); i < s.Facts; i++ {
			if s.cross(i) {
				cross++
			}
			edge, life, target, targetLife := s.edge(i)
			if edge != 7+2*i || life != edge+1 || targetLife != target+1 {
				t.Fatal("ID plan collision")
			}
		}
		if cross != s.Facts*pct/100 {
			t.Fatal("exact cross count", cross, pct)
		}
		var init declaredInitCommand
		l := raftlog.DefaultLimits()
		p := raftlog.DefaultApplicationPolicy(1)
		g := raftlog.ApplicationGenerationLimits{}
		before := l
		if err := nativeGraphCostProfile(&s, &init, &l, &p, &g); err != nil {
			t.Fatal(err)
		}
		if l.CacheBytes != before.CacheBytes || l.MemTableBytes != before.MemTableBytes || init.maxBlock != s.homeCount() {
			t.Fatal("cost profile changed resident limits")
		}
	}
	for _, s := range []nativeGraphCostSpec{{}, {Version: 1, Facts: 10000, Writers: 2}, {Version: 1, Facts: 10000, Writers: 1, CrossPercent: 2}} {
		if !errors.Is(s.validate(), errInvalid) {
			t.Fatal("unsupported profile accepted", s)
		}
	}
	t.Setenv("RHO_GRAPH_COST_SPEC", `{"version":1,"facts":2,"writers":1,"extra":1}`)
	if _, err := nativeGraphCostSpecFromEnv(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}

// A later retained SAME-request entry is genuine zero-output replay, while its
// original typed CDC remains at the earlier index after a subsequent mutation.
func TestNativeGraphCostReplayOriginalRetainsActualEntryAndOldCDC(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	h := n.hosts[0][0]
	prepare := func(value int64, id uint64) allocationProposal {
		p, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "p", Value: graphstate.I64(value), ValueID: graphstate.ValueID(id)}}, id+30, []freshBinding{{role: valueBinding, id: id, grant: claims[0].grant}})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	proposal := prepare(5, 5)
	original := n.submitPartitionGraph(0, proposal, reasonNone)
	replay := n.submitPartitionGraph(0, proposal, reasonNone)
	entry := h.driver.Applied()
	if replay.disposition != requestReplay || replay.index != original.index || entry <= original.index {
		t.Fatal(original, replay, entry)
	}
	n.submitPartitionGraph(0, prepare(6, 6), reasonNone)
	raw, err := nativeGraphRecordRead(t.Context(), h, entry, true)
	if err != nil {
		t.Fatal(err)
	}
	got, err := nativeGraphCostInspectRecord(t.Context(), h, raw)
	if err != nil || got.Index != entry || got.Original.Index != original.index || got.CDCClass != "replay-empty" || got.Replay == nil || got.CDC == nil || len(got.CDC)+len(got.Changes) != 0 {
		t.Fatal(got, err)
	}
	old, err := nativeGraphRecordRead(t.Context(), h, original.index, true)
	if err != nil || !bytes.Equal(got.Replay.Outcome, old.Outcome) || !bytes.Equal(got.Replay.CDC, old.CDC) || len(old.Changes) != 1 || old.Changes[0].After.I64 != 5 || got.Replay.Boundary.Index != original.index || got.Replay.Previous.Index != entry-1 || got.Replay.Previous.ImageSHA256 != got.SourceBoundary.ImageSHA256 {
		t.Fatal("replay substituted current original", got, old, err)
	}
	bad := raw
	bad.Outcome = bytes.Clone(raw.Outcome)
	bad.Outcome[0] ^= 1
	if _, err := nativeGraphCostInspectRecord(t.Context(), h, bad); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	tx, plan := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, plan)
	n.crossResolve(decision)
	n.crossResolve(decision)
	for group := range 2 {
		host := n.hosts[group][0]
		raw, err := nativeGraphRecordRead(t.Context(), host, host.driver.Applied(), true)
		if err != nil {
			t.Fatal(err)
		}
		got, err := nativeGraphCostInspectRecord(t.Context(), host, raw)
		if err != nil || got.CDCClass != "replay-empty" || got.Replay == nil || len(got.Replay.Registration) == 0 || got.Original.Index >= got.Index || got.Original.Phase != byte(crossResolve) || got.Replay.CDC == nil || len(got.Replay.CDC) == 0 {
			t.Fatal("cross replay lost original participant envelope", got, err)
		}
	}
}
