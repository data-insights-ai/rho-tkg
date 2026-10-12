package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type nativeGraphCostSample struct {
	Phase             string `json:"phase"`
	Fact              uint64 `json:"fact"`
	Cross             bool   `json:"cross"`
	ElapsedNanos      int64  `json:"elapsedNanos"`
	ProposalNanos     int64  `json:"proposalNanos"`
	AckNanos          int64  `json:"ackNanos"`
	DrainNanos        int64  `json:"drainNanos"`
	VerificationNanos int64  `json:"verificationNanos"`
	ClientAttempts    int    `json:"clientAttempts"`
}
type nativeGraphCostResult struct {
	Version                                                int                 `json:"version"`
	Engine                                                 string              `json:"engine"`
	Spec                                                   nativeGraphCostSpec `json:"spec"`
	Deployment                                             string              `json:"deployment"`
	Directory                                              string              `json:"directory"`
	UsefulFacts                                            uint64              `json:"usefulFacts"`
	SetupNodes                                             int                 `json:"setupNodes"`
	Creates, Updates                                       uint64
	SetupNanos, WritesNanos, VerificationNanos, TotalNanos int64
	PersistedFileBytes                                     uint64 `json:"persistedFileBytes"`
	AllocatedFileBytes                                     uint64 `json:"allocatedFileBytes"`
	OldCut, FinalCut                                       lifecycleRawCut
	CDCChanges, CDCRecords                                 uint64
	CompletedCreates, CompletedUpdates                     uint64
	Reaped                                                 int `json:"reaped"`
}

func newNativeGraphCostNetwork(t *testing.T, spec nativeGraphCostSpec) *certifiedProcessNetwork {
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
	n := &certifiedProcessNetwork{all: make(map[uint64]*graphProcess), dir: dir, leaders: [2]uint64{1, 4}}
	t.Log("native cost durable directories", dir)
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_COST_SPEC="+string(wire))
	}
	attachGraphProcessMuxesWithLimit(t, n.all, spec.deliveries())
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
	for _, r := range []struct{ actor, id, partition, count uint64 }{{1, 1, 3, spec.homeCount()}, {4, 2, 8, 2}} {
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
		n.read(t, 1, observeRecipient, r.id)
		n.read(t, 1, observeAllocator, 0)
		input := graphE17GrantInput(1, r.count)
		n.action(t, 1, certifiedReserve, 0, input)
		n.group(1).event(t, 1, processRequest{op: processCertifiedRead, entity: uint64(observeGrant), data: input})
		if !n.all[1].call(t, processRequest{op: processStateOp}).ready {
			t.Fatal("actual cost grant ReadIndex incomplete")
		}
		n.action(t, 1, certifiedInstallGrant, r.partition, nil)
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

// Acknowledgement is the actual immutable committed outcome, not Propose's
// enqueue response. Transport settling and voter/oracle inspection follow the
// timed acknowledgement separately; both remain in the measured total cost.
func nativeGraphCostSubmit(t *testing.T, n *certifiedProcessNetwork, phase, i uint64, cross bool) nativeGraphCostSample {
	t.Helper()
	sample := nativeGraphCostSample{Fact: i, Cross: cross, ClientAttempts: 1, Phase: "setup"}
	if phase == 4 {
		sample.Phase = "create"
	}
	if phase == 5 {
		sample.Phase = "update"
	}
	actor := uint64(1)
	var data []byte
	if phase == 3 {
		actor = 4
	}
	if phase >= 4 {
		data = make([]byte, 8)
		binary.BigEndian.PutUint64(data, i)
	}
	start := time.Now()
	response := n.all[actor].call(t, processRequest{op: processCertifiedAction, entity: certifiedGraphCost, index: phase, data: data})
	proposal, err := decodeCertifiedProcessProposal(response.aux)
	if err != nil {
		t.Fatal(err)
	}
	sample.ProposalNanos = time.Since(start).Nanoseconds()
	start = time.Now()
	if cross {
		response = n.all[1].call(t, processRequest{op: processSourceCoordinate, data: proposal.wire})
		result, err := decodeGraphProcessCoordinatorResult(response)
		if err != nil || result.decision.phase != crossDecide || !result.decision.commit || result.decision.round == 0 {
			t.Fatal("actual cost durable cross acknowledgement", result, err)
		}
	} else {
		baseline := response.applied
		wire, err := encodeCertifiedProcessProposal(proposal)
		if err != nil {
			t.Fatal(err)
		}
		proposed := n.all[actor].call(t, processRequest{op: processPropose, data: wire})
		if err := n.all[actor].mux.hub.enqueue(actor, proposed.packets); err != nil {
			t.Fatal("cost ordinary caller-owned packetdelivery", err)
		}
		completed := false
		ackCtx, cancelAck := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancelAck()
		var observed uint64
		// The original fixture's finite per-call/queue/time bounds still apply.
		// No arbitrary delay or quiet-transport assertion becomes commit authority.
		for range 128 {
			for id, peer := range n.all {
				peer.mux.mu.Lock()
				failure := peer.mux.err
				peer.mux.mu.Unlock()
				if failure != nil {
					t.Fatal("cost actual recorded transport/child failure", id, failure)
				}
			}
			state := n.all[actor].call(t, processRequest{op: processStateOp})
			observed = state.applied
			if state.applied <= baseline {
				hub := n.all[actor].mux.hub
				hub.mu.Lock()
				idle := hub.inFlight == 0 && hub.packetBytes == 0 && len(hub.packets) == 0
				hub.mu.Unlock()
				if idle {
					// Publication can race the first observation; confirm actual application
					// once after terminal transport, never turn quietness into success.
					state = n.all[actor].call(t, processRequest{op: processStateOp})
					observed = state.applied
					if observed <= baseline {
						t.Fatal("cost terminal idle without durable acknowledgement", actor, phase, baseline, observed)
					}
				} else {
					select {
					case <-n.all[actor].mux.hub.packetCompleted:
					case <-ackCtx.Done():
						t.Fatal("cost localack waiting actualtransport progress", actor, phase, baseline, observed, ackCtx.Err())
					}
					continue
				}
			}
			record := n.all[actor].call(t, processRequest{op: processEntryRecord, index: state.applied})
			ns, _ := certifiedProcessNamespace(actor)
			original, err := decodePartitionGraphOutcome(record.outcome, ns)
			if err != nil || original.hash != sha256.Sum256(proposal.wire) || original.index != state.applied || original.reason != reasonNone || original.disposition != applied || len(record.changes) == 0 {
				t.Fatal("actual cost durable local acknowledgement", original, err)
			}
			completed = true
			break
		}
		if !completed {
			t.Fatal("cost durable local acknowledgement exhausted finite polls", actor, phase, baseline, observed)
		}
	}
	sample.AckNanos = time.Since(start).Nanoseconds()
	sample.ElapsedNanos = sample.ProposalNanos + sample.AckNanos
	start = time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	err = drainGraphProcessMuxes(ctx, n.all)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	sample.DrainNanos = time.Since(start).Nanoseconds()
	start = time.Now()
	for g, leader := range n.leaders {
		state := n.all[leader].call(t, processRequest{op: processStateOp})
		for id, voter := range n.groups[g].nodes {
			actual := voter.call(t, processRequest{op: processStateOp})
			if actual.applied != state.applied || !bytes.Equal(actual.image, state.image) {
				t.Fatal("cost actual voter/root mismatch", id)
			}
		}
	}
	sample.VerificationNanos = time.Since(start).Nanoseconds()
	return sample
}
func nativeGraphCostRecords(t *testing.T, n *certifiedProcessNetwork, from *[2]uint64, stream *json.Encoder, spec nativeGraphCostSpec, phase, fact uint64) (uint64, uint64) {
	t.Helper()
	var changes, records uint64
	seen := make(map[byte]bool, 2)
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

	for g, leader := range n.leaders {
		end := n.all[leader].call(t, processRequest{op: processStateOp}).applied
		if end < from[g] || end-from[g] > 128 {
			t.Fatal("cost per-operation retained range exceeds original128", from, end)
		}
		for index := from[g] + 1; index <= end; index++ {
			r := n.all[leader].call(t, processRequest{op: processCostRecord, index: index})
			var record nativeLifecycleRecord
			if err := json.Unmarshal(r.aux, &record); err != nil || record.Index != index {
				t.Fatal("actual cost CDC", err)
			}
			for _, change := range record.Changes {
				if seen[change.Component] || !nativeGraphCostChangeMatches(spec, phase, fact, change, expectedScope) {
					t.Fatal("exact cost CDC effect/before/after", phase, fact, change)
				}
				seen[change.Component] = true
			}
			for id, p := range n.groups[g].nodes {
				got := p.call(t, processRequest{op: processEntryRecord, index: index})
				if !bytes.Equal(got.outcome, record.Outcome) || !bytes.Equal(got.changes, record.CDC) {
					t.Fatal("cost voter CDC/original mismatch", id, index)
				}
			}
			if err := stream.Encode(record); err != nil {
				t.Fatal(err)
			}
			records++
			changes += uint64(len(record.Changes))
		}
		from[g] = end
	}
	want := uint64(1)
	if phase == 4 {
		want = 2
	}
	if changes != want {
		t.Fatal("cost exact per-operation CDC cardinality", phase, fact, changes, want)
	}
	return changes, records
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
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		stats, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errInvalid
		}
		if info.Size() < 0 || stats.Blocks < 0 {
			return errCorrupt
		}
		total += uint64(info.Size())
		allocated += uint64(stats.Blocks) * 512 // #nosec G115 -- actual nonnegative POSIX sizes/blocks checked above.
		return nil
	})
	return total, allocated, err
}
func runNativeGraphCost(t *testing.T, s nativeGraphCostSpec) {
	t.Helper()
	started := time.Now()
	n := newNativeGraphCostNetwork(t, s)
	result := nativeGraphCostResult{Version: 1, Engine: "native-raft", Spec: s, Deployment: "single-machine distributed: two three-voter durable process groups", Directory: n.dir, UsefulFacts: s.Facts, SetupNodes: 3, Creates: s.Facts, Updates: s.Facts}
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
	_, records := open("cdc.jsonl")
	_, observations := open("observations.jsonl")
	var from [2]uint64
	for g, id := range n.leaders {
		from[g] = n.all[id].call(t, processRequest{op: processStateOp}).applied
	}
	for phase := uint64(1); phase <= 3; phase++ {
		nativeGraphCostSubmit(t, n, phase, 0, false)
		c, r := nativeGraphCostRecords(t, n, &from, records, s, phase, 0)
		result.CDCChanges += c
		result.CDCRecords += r
	}
	result.SetupNanos = time.Since(started).Nanoseconds()
	var old graphCut
	for _, phase := range []uint64{4, 5} {
		for i := uint64(0); i < s.Facts; i++ {
			sample := nativeGraphCostSubmit(t, n, phase, i, s.cross(i))
			result.WritesNanos += sample.ElapsedNanos
			if err := samples.Encode(sample); err != nil {
				t.Fatal(err)
			}
			c, r := nativeGraphCostRecords(t, n, &from, records, s, phase, i)
			result.CDCChanges += c
			result.CDCRecords += r
			if phase == 4 {
				result.CompletedCreates++
			} else {
				result.CompletedUpdates++
			}
			if (i+1)%100 == 0 || i+1 == s.Facts {
				stats := make(map[uint64][statCount]uint64, 6)
				for id, peer := range n.all {
					stats[id] = peer.call(t, processRequest{op: processStateOp}).stats
				}
				progress := struct {
					Phase                                                      uint64
					CompletedCreates, CompletedUpdates, CDCChanges, CDCRecords uint64
					ElapsedNanos                                               int64
					Voters                                                     map[uint64][statCount]uint64
				}{phase, result.CompletedCreates, result.CompletedUpdates, result.CDCChanges, result.CDCRecords, time.Since(started).Nanoseconds(), stats}
				wire, err := json.Marshal(progress)
				if err != nil {
					t.Fatal(err)
				}
				t.Log("native-cost-progress", string(wire))
			}
		}
		handle, cut := nativeGraphCostOpen(t, n, graphReadFresh, graphCut{})
		n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
		if phase == 4 {
			old = cut
			var err error
			result.OldCut, err = lifecycleRawCertificate(cut)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			var err error
			result.FinalCut, err = lifecycleRawCertificate(cut)
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			nativeGraphCostCheck(t, n, s, old, 5, observations)
			nativeGraphCostCheck(t, n, s, cut, 6, observations)
			result.VerificationNanos = time.Since(start).Nanoseconds()
		}
	}
	if result.CDCChanges != 3+3*s.Facts {
		t.Fatal("exact useful CDC cardinality", result.CDCChanges, 3+3*s.Facts)
	}
	for id := uint64(1); id <= 6; id++ {
		p := n.all[id]
		state := p.call(t, processRequest{op: processStateOp})
		leader := n.all[n.leaders[boolIndex(id > 3)]].call(t, processRequest{op: processStateOp})
		if state.applied != leader.applied || !bytes.Equal(state.image, leader.image) || state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 {
			t.Fatal("cost final voter/root/owner mismatch", id)
		}
	}
	for id := uint64(1); id <= 6; id++ {
		p := n.all[id]
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := p.shutdown(ctx, false)
		cancel()
		if err != nil || p.cmd.ProcessState == nil || !p.cmd.ProcessState.Success() {
			t.Fatal("cost child not cleanly reaped", id, err)
		}
		size, allocated, err := nativeGraphCostFileBytes(filepath.Join(n.dir, fmt.Sprint(id)))
		if err != nil {
			t.Fatal(err)
		}
		result.PersistedFileBytes += size
		result.AllocatedFileBytes += allocated
		result.Reaped++
	}
	result.TotalNanos = time.Since(started).Nanoseconds()
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
	for _, cross := range []uint64{0, 100} {
		t.Run(fmt.Sprint(cross), func(t *testing.T) {
			runNativeGraphCost(t, nativeGraphCostSpec{Version: 1, Facts: 2, CrossPercent: cross, Writers: 1})
		})
	}
}
func TestNativeGraphCostCell(t *testing.T) {
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
	for _, s := range []nativeGraphCostSpec{{}, {Version: 1, Facts: 10000, Writers: 8}, {Version: 1, Facts: 10000, Writers: 1, CrossPercent: 2}} {
		if !errors.Is(s.validate(), errInvalid) {
			t.Fatal("unsupported profile accepted", s)
		}
	}
	t.Setenv("RHO_GRAPH_COST_SPEC", `{"version":1,"facts":2,"writers":1,"extra":1}`)
	if _, err := nativeGraphCostSpecFromEnv(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}
