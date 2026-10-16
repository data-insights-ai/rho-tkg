package graphapply

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

func newNativeLifecycleNetwork(t *testing.T) *certifiedProcessNetwork {
	t.Helper()
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-native-lifecycle-")
	if err != nil {
		t.Fatal(err)
	}
	n := &certifiedProcessNetwork{all: make(map[uint64]*graphProcess), dir: dir, leaders: [2]uint64{1, 4}}
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_LIFECYCLE_PROCESS=1")
	}
	attachGraphProcessMuxes(t, n.all)
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
	t.Cleanup(func() {
		var calls uint64
		success := 0
		for id := uint64(1); id <= 6; id++ {
			p := n.all[id]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := p.shutdown(ctx, false)
			cancel()
			if err != nil {
				t.Error("native lifecycle child cleanup", id, err)
			}
			p.mux.mu.Lock()
			calls += p.sequence
			p.mux.mu.Unlock()
			if p.cmd.ProcessState != nil && p.cmd.ProcessState.Success() {
				success++
			} else {
				t.Error("native lifecycle child not successfully reaped", id)
			}
		}
		// The fixed public operations are separately admitted; this is a finite
		// fixture allowance, not a production worst-case or cost acceptance claim.
		const operations = 38
		allowance := uint64(processDeliveries + operations*4*1024)
		packets := n.groups[0].delivered + n.groups[1].delivered
		if calls+uint64(packets) > allowance {
			t.Error("native lifecycle aggregate IPC/packet bound", calls, packets, allowance)
		}
		t.Logf("native lifecycle cleanup: successfully-reaped=%d of6 calls=%d packets=%d allowance=%d", success, calls, packets, allowance)
	})
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
		n.read(t, 1, observeRecipient, r.id)
		n.read(t, 1, observeAllocator, 0)
		n.action(t, 1, certifiedReserve, 0, nil)
		n.group(1).event(t, 1, processRequest{op: processCertifiedRead, entity: uint64(observeGrant)})
		if !n.all[1].call(t, processRequest{op: processStateOp}).ready {
			t.Fatal("native lifecycle actual grant ReadIndex")
		}
		n.action(t, 1, certifiedInstallGrant, r.partition, nil)
	}
	return n
}
func nativeLifecycleOpen(t *testing.T, n *certifiedProcessNetwork, actor uint64, mode graphReadMode, cut graphCut) (uint64, graphCut) {
	t.Helper()
	n.indexReads++
	if n.indexReads > 38 {
		t.Fatal("undeclared native lifecycle certified operation")
	}
	wire, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: cut})
	if err != nil {
		t.Fatal(err)
	}
	response := n.all[actor].call(t, processRequest{op: processSourceCertifiedOpen, data: wire})
	handle, actual, err := decodeGraphE15ReadToken(response.aux)
	if err != nil || len(actual.certificates) != 2 {
		t.Fatal("actual lifecycle certified cut", err)
	}
	for _, certificate := range actual.certificates {
		if certificate.scope.semantic != certifiedGraphSemanticContractID() {
			t.Fatal("lifecycle certified agreement mismatch")
		}
	}
	return handle, actual
}
func nativeLifecycleRead(t *testing.T, n *certifiedProcessNetwork, actor, handle uint64, selector uint64) nativeLifecycleObservation {
	t.Helper()
	out := n.all[actor].call(t, processRequest{op: processLifecycleObserve, index: handle, entity: selector})
	result, err := decodeNativeLifecycleObservation(out.aux)
	if err != nil {
		t.Fatal("actual certified lifecycle observation", err)
	}
	return result
}
func nativeLifecycleSnapshot(t *testing.T, n *certifiedProcessNetwork, ops []lifecycleOperationMap, read func(uint64, uint64, uint64) nativeLifecycleObservation) (nativeLifecycleObservation, lifecycleView, graphCut) {
	t.Helper()
	handle, cut := nativeLifecycleOpen(t, n, 2, graphReadFresh, graphCut{})
	raw := read(2, handle, 0)
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	censusHandle, actual := nativeLifecycleOpen(t, n, 2, graphReadAt, cut)
	if !reflect.DeepEqual(actual, cut) {
		t.Fatal("entity census changed certified cut")
	}
	census := read(2, censusHandle, 2)
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: censusHandle})
	if census.ObservationScope != "entity-census-1-through-32" || census.Graph != raw.Graph {
		t.Fatal("entity census observation binding")
	}
	raw.Census = census.Census
	view, err := lifecycleNormalizeView(raw, lifecycleIDs(), ops)
	if err != nil {
		t.Fatal("canonical actual lifecycle view", err)
	}
	return raw, view, cut
}
func nativeLifecycleRecords(t *testing.T, n *certifiedProcessNetwork, from [2]uint64) ([]nativeLifecycleRecord, [2]uint64) {
	t.Helper()
	var result []nativeLifecycleRecord
	var end [2]uint64
	for g, leader := range n.leaders {
		end[g] = n.all[leader].call(t, processRequest{op: processStateOp}).applied
		if end[g] < from[g] || end[g]-from[g] > 128 {
			t.Fatal("bounded native lifecycle retained record range", from, end)
		}
		for index := from[g] + 1; index <= end[g]; index++ {
			out := n.all[leader].call(t, processRequest{op: processLifecycleRecord, index: index})
			var record nativeLifecycleRecord
			if err := json.Unmarshal(out.aux, &record); err != nil || record.Index != index || record.Partition != uint64([]int{3, 8}[g]) {
				t.Fatal("actual retained native lifecycle record", err, record)
			}
			// Every voter must retain identical complete original outcome/CDC bytes.
			for id, p := range n.groups[g].nodes {
				actual := p.call(t, processRequest{op: processEntryRecord, index: index})
				if !bytes.Equal(actual.outcome, record.Outcome) || !bytes.Equal(actual.changes, record.CDC) {
					t.Fatal("native lifecycle voter original/CDC mismatch", id, index)
				}
			}
			result = append(result, record)
		}
	}
	return result, end
}
func nativeLifecycleNormalizeRecords(records []nativeLifecycleRecord, ids []lifecycleIDMap, ops []lifecycleOperationMap) ([]lifecycleChange, []lifecycleEntity, []lifecycleLife, []lifecycleValue, error) {
	var changes []lifecycleChange
	var entities []lifecycleEntity
	var lives []lifecycleLife
	var values []lifecycleValue
	for _, record := range records {
		for _, c := range record.Changes {
			owner, err := lifecycleID(ids, "entity", c.Owner, 0)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			life, err := lifecycleID(ids, "life", c.Life, c.Owner)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			key := graphstate.ComponentKey{Owner: graphstate.EntityID(c.Owner), Life: graphstate.LifeID(c.Life), Kind: graphstate.ComponentKind(c.Component), Name: c.Name}
			before, err := lifecycleNormalizeCell(c.Before, key, ids, ops)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			after, err := lifecycleNormalizeCell(c.After, key, ids, ops)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			kind := "presence"
			if key.Kind == graphstate.ScalarProperty {
				kind = "property"
			} else if key.Kind != graphstate.Presence {
				return nil, nil, nil, nil, errCorrupt
			}
			changes = append(changes, lifecycleChange{Owner: owner, Life: life, Component: kind, Name: c.Name, Scope: c.Scope, AllScope: c.AllScope, Before: before, After: after})
		}
		for _, e := range record.Entities {
			owner, err := lifecycleID(ids, "entity", e.ID, 0)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			item := lifecycleEntity{ID: owner, Kind: "node"}
			if e.Kind == byte(graphstate.Relationship) {
				item.Kind, item.Mode, item.Type = "relationship", "life-bound", e.Type
				item.Source, err = lifecycleID(ids, "entity", e.Source, 0)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				item.Target, err = lifecycleID(ids, "entity", e.Target, 0)
				if err != nil {
					return nil, nil, nil, nil, err
				}
			}
			entities = append(entities, item)
		}
		for _, l := range record.Lives {
			owner, err := lifecycleID(ids, "entity", l.Owner, 0)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			life, err := lifecycleID(ids, "life", l.Life, l.Owner)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			item := lifecycleLife{Owner: owner, Life: life}
			if l.Owner == 3 {
				item.SourceLife, err = lifecycleID(ids, "life", l.SourceLife, 1)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				item.TargetLife, err = lifecycleID(ids, "life", l.TargetLife, 17)
				if err != nil {
					return nil, nil, nil, nil, err
				}
			}
			lives = append(lives, item)
		}
		for _, v := range record.Values {
			name, err := lifecycleID(ids, "value", v.ID, 0)
			if err != nil {
				return nil, nil, nil, nil, err
			}
			values = append(values, lifecycleValue{Reference: name, Kind: v.Kind, I64: v.I64})
		}
	}
	return changes, entities, lives, values, nil
}

func TestNativeGraphLifecycleSevenStepsCertifiedTrace(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newNativeLifecycleNetwork(t)

	var trafficMu sync.Mutex
	traffic := make(map[uint64][2]int)
	queryTraffic := make(map[uint64][2]int)
	censusTraffic := make(map[uint64][2]int)
	censusOperations := 0
	hub := n.all[1].mux.hub
	hub.mu.Lock()
	hub.beforeRelay = func(ctx context.Context, origin, partition uint64, wire []byte) error {
		request, err := decodeGraphSourceRequest(wire)
		if err != nil {
			return err
		}
		if request.kind == sourceGet || request.kind == sourceScan {
			trafficMu.Lock()
			count := traffic[partition]
			if request.kind == sourceGet {
				count[0]++
			} else {
				count[1]++
			}
			traffic[partition] = count
			trafficMu.Unlock()
		}
		return ctx.Err()
	}
	hub.mu.Unlock()
	observe := func(actor, handle uint64, selector uint64) nativeLifecycleObservation {
		trafficMu.Lock()
		before := map[uint64][2]int{3: traffic[3], 8: traffic[8]}
		trafficMu.Unlock()
		raw := nativeLifecycleRead(t, n, actor, handle, selector)
		if selector == 2 {
			censusOperations++
		}
		trafficMu.Lock()
		for _, partition := range []uint64{3, 8} {
			old, current := before[partition], traffic[partition]
			total := queryTraffic[partition]
			total[0] += current[0] - old[0]
			total[1] += current[1] - old[1]
			queryTraffic[partition] = total
			if selector == 2 {
				c := censusTraffic[partition]
				c[0] += current[0] - old[0]
				c[1] += current[1] - old[1]
				censusTraffic[partition] = c
			}
		}
		trafficMu.Unlock()
		return raw
	}
	trace := lifecycleTrace{SchemaVersion: 1, Fixture: nativeLifecycleFixture, Engine: "native-raft", IdentityMap: lifecycleIDs(), Normalization: []string{"Bijective declared logical IDs; raw graph-qualified IDs retained", "Operation IDs map actual request/TxID/revision rounds; numeric rounds are not cross-engine equality", "Immutable declarations remain separate from presence/effective state", "Raw historical candidate postings are not effective answers", "Property components/dictionary remain retained after own-presence closure", "Raw complete outcome/CDC bytes preserved; dictionary/declaration additions separate from exact component changes"}, Scope: "Fixed sequential seven-step graph lifecycle functional comparison plumbing; not cost, multi-host or substrate selection acceptance"}
	if err := checkLifecycleIDMap(trace.IdentityMap); err != nil {
		t.Fatal(err)
	}
	_, initial, initialCut := nativeLifecycleSnapshot(t, n, nil, observe)
	assertLifecycleView(t, initial, 0)
	var err error
	trace.InitialCut, err = lifecycleRawCertificate(initialCut)
	if err != nil {
		t.Fatal(err)
	}
	var cut3 graphCut
	var held uint64
	var heldFirst nativeLifecycleObservation
	heldClosed := false
	defer func() {
		if held != 0 && !heldClosed {
			n.all[5].call(t, processRequest{op: processSourceCertifiedClose, index: held})
		}
	}()
	names := []string{"node-a", "node-b", "edge-create", "edge-update", "node-close", "edge-close", "node-reopen"}
	coordinators := []uint64{1, 4, 4, 1, 1, 4, 1}
	for i, name := range names {
		_, before, _ := nativeLifecycleSnapshot(t, n, trace.OperationMap, observe)
		var from [2]uint64
		for g, leader := range n.leaders {
			from[g] = n.all[leader].call(t, processRequest{op: processStateOp}).applied
		}
		producer := uint64(1)
		if i == 1 {
			producer = 4
		}
		out := n.all[producer].call(t, processRequest{op: processCertifiedAction, entity: certifiedLifecycle, index: uint64(i + 1)})
		proposal, err := decodeCertifiedProcessProposal(out.aux)
		if err != nil {
			t.Fatal(err)
		}
		command, err := decodePartitionGraphCommand(proposal.wire, defaultMaterializerLimits())
		if err != nil {
			t.Fatal(err)
		}
		result := graphE17Cross(t, n, coordinators[i], proposal)
		if !result.decision.commit {
			t.Fatal("native lifecycle logical operation aborted", name)
		}
		trace.OperationMap = append(trace.OperationMap, lifecycleOperationMap{Logical: name, Coordinator: result.decision.ns.partition, Request: hex.EncodeToString(command.request.id[:]), Transaction: hex.EncodeToString(result.decision.tx[:]), Round: result.decision.round})
		records, _ := nativeLifecycleRecords(t, n, from)
		raw, after, cut := nativeLifecycleSnapshot(t, n, trace.OperationMap, observe)
		assertLifecycleView(t, before, i)
		assertLifecycleView(t, after, i+1)
		assertLifecycleCandidates(t, raw, i+1)
		step := lifecycleStep{Operation: name, Coordinator: []string{"A", "B"}[boolIndex(coordinators[i] == 4)], Before: before, After: after, RawObservation: raw}
		step.Cut, err = lifecycleRawCertificate(cut)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			step.RawRecords = append(step.RawRecords, lifecycleRawRecord{Partition: r.Partition, Index: r.Index, Outcome: r.Outcome, CDC: r.CDC})
		}
		step.LogicalCDC, step.NewEntities, step.NewLives, step.NewValues, err = nativeLifecycleNormalizeRecords(records, trace.IdentityMap, trace.OperationMap)
		if err != nil {
			t.Fatal(err)
		}
		assertLifecycleCDC(t, step, i+1)
		trace.Steps = append(trace.Steps, step)
		if i == 2 {
			cut3 = cut
			held, _ = nativeLifecycleOpen(t, n, 5, graphReadAt, cut3)
			heldFirst = observe(5, held, 1)
		}
	}
	heldAfter := observe(5, held, 1)
	if !reflect.DeepEqual(heldFirst, heldAfter) {
		t.Fatal("held certified old edge substituted current state", heldFirst, heldAfter)
	}
	v, err := lifecycleNormalizeView(heldAfter, trace.IdentityMap, trace.OperationMap)
	if err != nil {
		t.Fatal(err)
	}
	oldCut, err := lifecycleRawCertificate(cut3)
	if err != nil {
		t.Fatal(err)
	}
	trace.OldReads = append(trace.OldReads, lifecycleOldRead{Operation: "edge-create", Mode: "held-old", Cut: oldCut, View: v, RawObservation: heldAfter})
	n.all[5].call(t, processRequest{op: processSourceCertifiedClose, index: held})
	heldClosed = true
	for _, index := range []int{2, 3, 4} {
		var cut graphCut
		for _, wire := range trace.Steps[index].Cut.Certificates {
			cert, err := decodeGraphCertificate(wire)
			if err != nil {
				t.Fatal(err)
			}
			cut.certificates = append(cut.certificates, cert)
		}
		handle, actual := nativeLifecycleOpen(t, n, 2, graphReadAt, cut)
		raw := observe(2, handle, 0)
		n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
		censusHandle, censusCut := nativeLifecycleOpen(t, n, 2, graphReadAt, cut)
		if !reflect.DeepEqual(censusCut, cut) {
			t.Fatal("old census changed certified cut")
		}
		census := observe(2, censusHandle, 2)
		n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: censusHandle})
		raw.Census = census.Census
		view, err := lifecycleNormalizeView(raw, trace.IdentityMap, trace.OperationMap)
		if err != nil {
			t.Fatal(err)
		}
		assertLifecycleView(t, view, index+1)
		if !reflect.DeepEqual(actual, cut) || !reflect.DeepEqual(view, trace.Steps[index].After) {
			t.Fatal("reopened retained lifecycle cut changed", index+1)
		}
		rawCut, err := lifecycleRawCertificate(actual)
		if err != nil {
			t.Fatal(err)
		}
		trace.OldReads = append(trace.OldReads, lifecycleOldRead{Operation: trace.Steps[index].Operation, Mode: "reopened-at", Cut: rawCut, View: view, RawObservation: raw})
	}
	trace.RawCounts = lifecycleTraceCounts{CensusOperations: censusOperations, CensusTraffic: censusTraffic, CertifiedReadOperations: n.indexReads, QueryTraffic: queryTraffic, VoterStats: make(map[uint64][statCount]uint64)}
	for _, step := range trace.Steps {
		trace.RawCounts.ComponentChanges += len(step.LogicalCDC)
		for _, record := range step.RawRecords {
			trace.RawCounts.ApplicationRecords++
			trace.RawCounts.OutcomeBytes += len(record.Outcome)
			trace.RawCounts.CDCBytes += len(record.CDC)
		}
	}
	for id, p := range n.all {
		state := p.call(t, processRequest{op: processStateOp})
		trace.RawCounts.VoterStats[id] = state.stats
		if state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 {
			t.Fatal("native lifecycle retained owners leaked", id, state.stats)
		}
	}
	wire, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) > 2<<20 {
		t.Fatal("native lifecycle complete trace output limit", len(wire))
	}
	path := filepath.Join(n.dir, "native-graph-lifecycle-trace.json")
	if err := os.WriteFile(path, append(wire, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("native lifecycle actual canonical/raw trace", path)
}
func boolIndex(v bool) int {
	if v {
		return 1
	}
	return 0
}

func TestNativeGraphLifecyclePrivateObservationAndMappingsRefuseMalformed(t *testing.T) {
	if out, err := nativeLifecycleObserve(t.Context(), nil, processRequest{}); out != nil || !errors.Is(err, errInvalid) {
		t.Fatal(out, err)
	}
	if _, err := decodeNativeLifecycleObservation(nil); !errors.Is(err, errLimit) {
		t.Fatal(err)
	}
	if _, err := decodeNativeLifecycleObservation([]byte(`{"unexpected":1}`)); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
	positive := nativeLifecycleObservation{Graph: "actual-inert-codec-data", Entities: []nativeLifecycleEntity{{ID: 1, Found: true}}, PhantomEntities: []nativeLifecycleEntity{{ID: 15}}}
	wire, err := json.Marshal(positive)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeNativeLifecycleObservation(wire)
	if err != nil || !reflect.DeepEqual(decoded, positive) {
		t.Fatal(decoded, err)
	}
	for i := range len(wire) {
		if _, err := decodeNativeLifecycleObservation(wire[:i]); i == 0 && !errors.Is(err, errLimit) || i > 0 && !errors.Is(err, errCorrupt) {
			t.Fatal("truncated lifecycle observation", i, err)
		}
	}
	if _, err := decodeNativeLifecycleObservation(append(wire, []byte(" {}")...)); !errors.Is(err, errCorrupt) {
		t.Fatal("trailing observation accepted", err)
	}
	if out, err := nativeLifecycleObserve(t.Context(), &graphProcessCertifiedSessions{}, processRequest{index: 1}); out != nil || !errors.Is(err, errInvalid) {
		t.Fatal("missing real owner", out, err)
	}

	ids := lifecycleIDs()
	if err := checkLifecycleIDMap(ids); err != nil {
		t.Fatal(err)
	}
	ids[len(ids)-1].Raw = 6
	if err := checkLifecycleIDMap(ids); !errors.Is(err, errInvalid) {
		t.Fatal("replacement Life reused typed ValueID", err)
	}
	if _, err := lifecycleID(lifecycleIDs(), "value", 99, 0); !errors.Is(err, errCorrupt) {
		t.Fatal(err)
	}
}
