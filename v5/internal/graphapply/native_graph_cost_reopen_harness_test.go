package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
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
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type nativeGraphCostGrantReceipt struct {
	Partition, Sequence, RequestedCount uint64
	ReserveOutcome, InstallOutcome []byte
	ReserveCDC, InstallCDC []byte
	ReserveProposal, InstallProposal []byte
}

// Capture the actual factory aux BEFORE submission. An outcome cannot recover
// this command, and a new factory invocation would mint another retry identity.
func nativeGraphCostGrantAction(t *testing.T, n *certifiedProcessNetwork, kind, index uint64, data []byte) ([]byte, processResponse) {
	t.Helper()
	if kind != certifiedReserve && kind != certifiedInstallGrant { t.Fatal("unexpected cost grant factory", kind) }
	factory := n.all[1].call(t, processRequest{op: processCertifiedAction, entity: kind, index: index, data: data})
	actual, record, _ := nativeGraphCostControl(t, n, factory.aux, false)
	if kind == certifiedReserve && actual.disposition != applied { t.Fatal("cost reservation did not apply", actual) }
	return bytes.Clone(factory.aux), record
}

func nativeGraphCostControl(t *testing.T, n *certifiedProcessNetwork, wire []byte, replay bool) (outcome, processResponse, processResponse) {
	t.Helper()
	proposal, err := decodeCertifiedProcessProposal(wire)
	if err != nil || proposal.target.graph != (idalloc.GraphID{1}) || proposal.target.semantic != certifiedGraphSemanticContractID() || proposal.target.partition != 3 && proposal.target.partition != 8 {
		t.Fatal("actual cost control proposal topology", err)
	}
	group := 0
	if proposal.target.partition == 8 { group = 1 }
	leader := n.leaders[group]
	before := n.all[leader].call(t, processRequest{op: processStateOp})
	n.groups[group].event(t, leader, processRequest{op: processPropose, data: wire})
	after := n.all[leader].call(t, processRequest{op: processStateOp})
	record := n.all[leader].call(t, processRequest{op: processEntryRecord, index: after.applied})
	ns, _ := certifiedProcessNamespace(leader)
	o, err := decodeDeclaredOutcome(record.outcome, ns)
	if err != nil || o.hash != sha256.Sum256(proposal.wire) || o.reason != reasonNone || after.applied <= before.applied {
		t.Fatal("actual retained cost control", o, err)
	}
	if replay {
		if o.disposition != requestReplay || o.index > before.applied || len(record.changes) != 0 || !bytes.Equal(before.image, after.image) {
			t.Fatal("cost replay changed application state/original coordinate", o)
		}
	} else if o.index != after.applied || o.disposition != applied && o.disposition != controlRecovery {
		t.Fatal("cost initial control refused", o)
	}
	for id, peer := range n.groups[group].nodes {
		state := peer.call(t, processRequest{op: processStateOp})
		actual := peer.call(t, processRequest{op: processEntryRecord, index: after.applied})
		if state.applied != after.applied || !bytes.Equal(state.image, after.image) || !bytes.Equal(actual.outcome, record.outcome) || !bytes.Equal(actual.changes, record.changes) || state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 {
			t.Fatal("cost all-voter control identity/root/CDC/owners", id)
		}
	}
	return o, record, after
}

func nativeGraphCostDrain(t *testing.T, n *certifiedProcessNetwork) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil { t.Fatal(err) }
}

func nativeGraphCostStop(t *testing.T, n *certifiedProcessNetwork, cohort int, stream *json.Encoder) int {
	t.Helper()
	nativeGraphCostDrain(t, n)
	for id := uint64(1); id <= 6; id++ {
		state := n.all[id].call(t, processRequest{op: processStateOp})
		if state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 { t.Fatal("cost cohort owner residue", cohort, id) }
	}
	for id := uint64(1); id <= 6; id++ {
		p := n.all[id]
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := p.shutdown(ctx, false)
		cancel()
		if err != nil || p.cmd.ProcessState == nil || !p.cmd.ProcessState.Success() { t.Fatal("cost cohort not cleanly reaped", cohort, id, err) }
		wait, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
		if !ok { t.Fatal("missing cost OS wait status") }
		if err := stream.Encode(struct {
			Cohort int
			Member uint64
			PID int
			Incarnation [16]byte
			Volume string
			WaitStatus int
			ExitCode int
			Reaped bool
		}{cohort, id, p.cmd.Process.Pid, p.incarnation, filepath.Join(n.dir, fmt.Sprint(id)), int(wait), p.cmd.ProcessState.ExitCode(), true}); err != nil { t.Fatal(err) }
	}
	// Every owned process is terminal before retiring this pipe/worker cohort.
	hub := n.all[1].mux.hub
	hub.cancel()
	hub.wg.Wait()
	return 6
}

// Stream complete present retained frames, including bootstrap and read controls.
// A raw application boundary never becomes a fabricated graph certificate.
func nativeGraphCostPrefix(t *testing.T, n *certifiedProcessNetwork, group int, state processResponse, from uint64, stream *json.Encoder, compare *json.Decoder) uint64 {
	t.Helper()
	partition := uint64(3)
	if group == 1 { partition = 8 }
	var count uint64
	for from < state.applied {
		var page nativeGraphCostRecordPage
		for id := uint64(1 + 3*group); id <= uint64(3 + 3*group); id++ {
			response := n.all[id].call(t, processRequest{op: processCostRecords, index: from, entity: state.applied, data: []byte{0}})
			actual, err := nativeGraphCostDecodeRawPage(response.aux, partition, from, state.applied, state.image)
			if err != nil { t.Fatal("complete raw cost prefix boundary", id, err) }
			if page.Image == nil { page = actual } else if !reflect.DeepEqual(actual, page) { t.Fatal("cost raw prefix differs on voter", id, from) }
		}
		if compare != nil {
			var original nativeGraphCostRecordPage
			if err := compare.Decode(&original); err != nil || !reflect.DeepEqual(original, page) { t.Fatal("same-directory cost original prefix changed", group, from, err) }
		}
		if stream != nil { if err := stream.Encode(page); err != nil { t.Fatal(err) } }
		count += uint64(len(page.Records))
		from = page.Next
	}
	if compare != nil {
		var extra nativeGraphCostRecordPage
		if err := compare.Decode(&extra); !errors.Is(err, io.EOF) { t.Fatal("original prefix has trailing/missing pages", err) }
	}
	return count
}

func nativeGraphCostArtifact(t *testing.T, n *certifiedProcessNetwork, name string) (*os.File, *json.Encoder) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(n.dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) { t.Error(err) } })
	return f, json.NewEncoder(f)
}

func nativeGraphCostEqualArtifacts(t *testing.T, n *certifiedProcessNetwork, left, right string) {
	t.Helper()
	var sums [2][32]byte
	for i, name := range []string{left, right} {
		f, err := os.Open(filepath.Join(n.dir, name))
		if err != nil { t.Fatal(err) }
		h := sha256.New()
		_, err = io.Copy(h, f)
		err = errors.Join(err, f.Close())
		if err != nil { t.Fatal(err) }
		copy(sums[i][:], h.Sum(nil))
	}
	if sums[0] != sums[1] { t.Fatal("cost original answers differ after physical reopen", left, right) }
}

// A fresh authenticated At operation owns each predicate's entire continuation
// and hydration ledger. Pages NEVER reopen a session or reset its native budget.
func nativeGraphCostIndexes(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, cut graphCut, want int64, stream *json.Encoder) {
	t.Helper()
	for _, endpoint := range []uint64{1, 3, spec.remoteNode()} {
		for _, life := range []uint64{0, endpoint+1} {
			handle, actual := nativeGraphCostOpen(t, n, graphReadAt, cut)
			if !reflect.DeepEqual(actual, cut) { t.Fatal("cost index At substituted cut") }
			var cursor, seen, nextFact uint64
			complete := false
			for pageCount := uint64(0); pageCount <= spec.Facts; pageCount++ {
				response := n.all[2].call(t, processRequest{op: processCostObserve, index: handle, entity: endpoint, data: nativeGraphCostIndexInput(1, life, cursor)})
				page, err := nativeGraphCostDecodeIndexPage(response.aux)
				if err != nil || page.Kind != 1 || page.Endpoint != endpoint || page.Life != life || page.Cursor != cursor { t.Fatal("actual cost incident page", err) }
				for _, row := range page.Rows {
					for nextFact < spec.Facts && endpoint != 1 && (endpoint == 3) == spec.cross(nextFact) { nextFact++ }
					if nextFact >= spec.Facts { t.Fatal("cost endpoint index extra candidate", endpoint, row) }
					edge, bound, target, targetLife := spec.edge(nextFact)
					x := row.Entity
					if x.ID != edge || !x.Found || !x.Effective || x.Kind != byte(graphstate.Relationship) || x.Type != "edge" || x.Mode != byte(graphstate.LifeBound) || x.Source != 1 || x.Target != target || x.EffectiveLife != bound || !x.Presence.Present || x.Presence.Kind != "life" || x.Presence.Handle != bound || len(x.Labels) != 0 || row.SourceLife != 2 || row.TargetLife != targetLife || !x.Property.Present || x.Property.Kind != "i64" || x.Property.Revision == 0 || x.Property.I64 != want || x.Property.Handle != uint64(want) || x.Property.Provenance != 4+nextFact+boolUint(want == 6)*spec.Facts { t.Fatal("cost exact indexed retained candidate", endpoint, life, nextFact, row) }
					seen++; nextFact++
				}
				if err := stream.Encode(page); err != nil { t.Fatal(err) }
				if page.Complete { complete = true; break }
				cursor = page.Next
			}
			for nextFact < spec.Facts && endpoint != 1 && (endpoint == 3) == spec.cross(nextFact) { nextFact++ }
			if !complete || nextFact != spec.Facts || endpoint == 1 && seen != spec.Facts { t.Fatal("cost endpoint index incomplete/omitted", endpoint, life, seen) }
			n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
		}
	}
	handle, actual := nativeGraphCostOpen(t, n, graphReadAt, cut)
	if !reflect.DeepEqual(actual, cut) { t.Fatal("cost identity At substituted cut") }
	for _, value := range []uint64{5, 6, 999} {
		response := n.all[2].call(t, processRequest{op: processCostObserve, index: handle, data: nativeGraphCostIndexInput(2, value, 0)})
		page, err := nativeGraphCostDecodeIndexPage(response.aux)
		found := value == 5 || value == 6 && want == 6
		if err != nil || page.Kind != 2 || page.Life != value || !page.Complete || page.ValueFound != found || found && (page.ValueID != value || page.Value != int64(value)) || !found && (page.ValueID != 0 || page.Value != 0) { t.Fatal("cost retained typed identity/phantom", value, page, err) }
		if err := stream.Encode(page); err != nil { t.Fatal(err) }
	}
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
}

type nativeGraphCostIndexRow struct {
	Entity nativeLifecycleEntity
	SourceLife, TargetLife uint64
}
type nativeGraphCostIndexPage struct {
	Kind byte
	Endpoint, Life, Cursor, Next uint64
	Complete bool
	Rows []nativeGraphCostIndexRow
	ValueFound bool
	ValueID uint64
	Value int64
}
func nativeGraphCostIndexInput(kind byte, value, cursor uint64) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(append([]byte("GCIX1"), kind), value), cursor)
}
func nativeGraphCostEncodeIndexPage(page nativeGraphCostIndexPage) ([]byte, error) {
	if len(page.Rows) > 16 { return nil, errLimit }
	return processEncoding(func(w *boundedWriter) {
		w.add([]byte("GCIP1")); w.tag(page.Kind)
		for _, n := range []uint64{page.Endpoint, page.Life, page.Cursor, page.Next, page.ValueID} { w.u64(n) }
		w.u64(uint64(page.Value)) // #nosec G115 -- signed scalar bit-pattern wire round trip.
		if page.Complete { w.tag(1) } else { w.tag(0) }
		if page.ValueFound { w.tag(1) } else { w.tag(0) }
		w.u32(len(page.Rows))
		for _, row := range page.Rows {
			wire, err := json.Marshal(row)
			if err != nil { w.err = err; return }
			if len(wire) > nativeLifecycleBytes { w.err = errLimit; return }
			w.field(wire)
		}
	})
}
func nativeGraphCostDecodeIndexPage(wire []byte) (nativeGraphCostIndexPage, error) {
	var page nativeGraphCostIndexPage
	if len(wire) > nativeGraphCostPageBytes { return page, errLimit }
	c := graphCursor{b: wire}
	if !bytes.Equal(c.take(5), []byte("GCIP1")) { return page, errCorrupt }
	page.Kind = c.tag()
	page.Endpoint, page.Life, page.Cursor, page.Next, page.ValueID = c.u64(), c.u64(), c.u64(), c.u64(), c.u64()
	page.Value = int64(c.u64()) // #nosec G115 -- signed scalar bit-pattern wire round trip.
	complete, found := c.tag(), c.tag()
	page.Complete, page.ValueFound = complete == 1, found == 1
	n := c.count(16, 4)
	if c.err != nil { return page, errors.Join(errCorrupt, c.err) }
	page.Rows = make([]nativeGraphCostIndexRow, n)
	for i := range page.Rows {
		wire := c.field(nativeLifecycleBytes)
		if c.err != nil { return nativeGraphCostIndexPage{}, errors.Join(errCorrupt, c.err) }
		d := json.NewDecoder(bytes.NewReader(wire)); d.DisallowUnknownFields()
		if err := d.Decode(&page.Rows[i]); err != nil { return nativeGraphCostIndexPage{}, errors.Join(errCorrupt, err) }
		if err := d.Decode(new(any)); !errors.Is(err, io.EOF) { return nativeGraphCostIndexPage{}, errors.Join(errCorrupt, err) }
	}
	if c.err != nil || len(c.b) != 0 || complete > 1 || found > 1 || page.Kind < 1 || page.Kind > 2 || page.Complete != (page.Next == 0) || !page.Complete && len(page.Rows) == 0 || page.Kind == 2 && (len(page.Rows) != 0 || page.Endpoint != 0 || page.Cursor != 0 || !page.Complete) { return nativeGraphCostIndexPage{}, errors.Join(errCorrupt, c.err) }
	return page, nil
}

// Cost-only worker branch pending P05's shared-file release. The actual view
// supplies every posting, opaque continuation, typed identity and hydrated cell.
func nativeGraphCostIndexObserve(ctx context.Context, sessions *graphProcessCertifiedSessions, request processRequest) ([]byte, error) {
	if ctx == nil || sessions == nil || len(request.data) != 22 || !bytes.Equal(request.data[:5], []byte("GCIX1")) || request.data[5] < 1 || request.data[5] > 2 || request.packet.From != 0 || request.packet.To != 0 || request.packet.Snapshot || len(request.packet.Payload) != 0 { return nil, errInvalid }
	kind, arg, cursor := request.data[5], binary.BigEndian.Uint64(request.data[6:14]), binary.BigEndian.Uint64(request.data[14:])
	if kind == 1 && request.entity == 0 || kind == 2 && (request.entity != 0 || cursor != 0 || arg > 1000) { return nil, errInvalid }
	sessions.mu.Lock(); owner, found := sessions.owners[request.index]; sessions.mu.Unlock()
	if !found { return nil, errInvalid }
	view, err := owner.read.ReadView()
	if err != nil { return nil, err }
	page := nativeGraphCostIndexPage{Kind: kind, Endpoint: request.entity, Life: arg, Cursor: cursor, Complete: true, Rows: []nativeGraphCostIndexRow{}}
	if kind == 2 {
		value, err := view.ValueIdentity(ctx, graphstate.I64(int64(arg))) // #nosec G115 -- arg<=1000 checked above.
		if err != nil { return nil, err }
		page.ValueFound, page.ValueID = value.Found, uint64(value.ID)
		if value.Found {
			var ok bool
			page.Value, ok = value.Value.Int64Value()
			if !ok { return nil, errCorrupt }
		}
	} else {
		sessions.coordinator.host.mu.Lock(); cfg, e := sessions.coordinator.host.currentConfiguration(); sessions.coordinator.host.mu.Unlock()
		if e != nil { return nil, e }
		window, e := temporal.All(cfg.defaultAxis.Axis())
		if e != nil { return nil, e }
		actual, e := view.IncidentRelationships(ctx, graphstate.IncidentPredicate{Endpoint: graphstate.EntityID(request.entity), Life: graphstate.LifeID(arg), Window: window}, graphstate.Cursor(cursor), graphstate.ReadBudget{Rows: 16, Bytes: 64<<10})
		if e != nil { return nil, e }
		if actual.View != view.Identity() || len(actual.Entities) > 16 { return nil, errCorrupt }
		page.Next, page.Complete = uint64(actual.Next), actual.Complete
		for _, id := range actual.Entities {
			// The existing observer reads the SAME certified owner, not a new cut.
			wire, e := nativeGraphCostObserve(ctx, sessions, processRequest{index: request.index, entity: uint64(id)})
			if e != nil { return nil, e }
			var entity nativeLifecycleEntity
			if e := json.Unmarshal(wire, &entity); e != nil { return nil, e }
			life, e := view.Life(ctx, id, graphstate.LifeID(entity.EffectiveLife))
			if e != nil || !life.Found { return nil, errors.Join(errCorrupt, e) }
			page.Rows = append(page.Rows, nativeGraphCostIndexRow{entity, uint64(life.Record.SourceLife), uint64(life.Record.TargetLife)})
		}
	}
	wire, err := nativeGraphCostEncodeIndexPage(page)
	if len(wire) > nativeGraphCostPageBytes { return nil, errLimit }
	return wire, err
}

// All restore comparisons precede Campaign. Certified queries then wait for a
// genuine completed configuration ReadIndex and retain the original At cuts.
func nativeGraphCostReopen(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, block uint64, old, final graphCut, result *nativeGraphCostResult, cohorts *json.Encoder) {
	t.Helper()
	start := time.Now()
	nativeGraphCostAnchors(t, n, spec, old, final, "anchors-before-reopen.jsonl")
	beforeIndexes, indexes := nativeGraphCostArtifact(t, n, "indexes-before-reopen.jsonl")
	nativeGraphCostIndexes(t, n, spec, old, 5, indexes)
	nativeGraphCostIndexes(t, n, spec, final, 6, indexes)
	if err := beforeIndexes.Close(); err != nil { t.Fatal(err) }
	nativeGraphCostDrain(t, n)
	var states [6]processResponse
	_, restore := nativeGraphCostArtifact(t, n, "reopen-states.jsonl")
	for id := uint64(1); id <= 6; id++ {
		state := n.all[id].call(t, processRequest{op: processStateOp})
		if state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 { t.Fatal("cost before reopen owner residue", id) }
		states[id-1] = state
		raw, err := encodeProcessResponse(state)
		if err != nil { t.Fatal(err) }
		if err := restore.Encode(struct { Stage string; Member uint64; Raw []byte }{"before-stop", id, raw}); err != nil { t.Fatal(err) }
	}
	for group := range 2 {
		state := states[3*group]
		for voter := range 3 {
			x := states[3*group+voter]
			if x.applied != state.applied || !bytes.Equal(x.image, state.image) { t.Fatal("cost pre-stop durable voters differ", group, voter) }
		}
		f, stream := nativeGraphCostArtifact(t, n, fmt.Sprintf("original-prefix-%d.jsonl", group))
		result.ReopenPrefixRecords += nativeGraphCostPrefix(t, n, group, state, 0, stream, nil)
		if err := f.Close(); err != nil { t.Fatal(err) }
	}
	result.PreReopenVerificationNanos = time.Since(start).Nanoseconds()
	result.TransportTiming = n.all[1].mux.hub.costTimingSnapshot()
	start = time.Now()
	result.InitialReaped = nativeGraphCostStop(t, n, 1, cohorts)
	result.InitialStopNanos = time.Since(start).Nanoseconds()
	start = time.Now()
	wire, err := json.Marshal(spec)
	if err != nil { t.Fatal(err) }
	previous := n.all
	fresh := make(map[uint64]*graphProcess, 6)
	for id := uint64(1); id <= 6; id++ {
		if !previous[id].closed || previous[id].cmd.ProcessState == nil { t.Fatal("cost restart before complete original reap", id) }
		fresh[id] = startGraphProcess(t, filepath.Join(n.dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_PROCESS_REOPEN=1", "RHO_GRAPH_COST_SPEC="+string(wire), "RHO_GRAPH_COST_BLOCK_SIZE="+strconv.FormatUint(block, 10))
	}
	// No Initialize, BeginRecipient, Reserve, install, anchor or business replay.
	attachGraphProcessMuxesWithLimit(t, fresh, spec.deliveries())
	hub := fresh[1].mux.hub
	hub.mu.Lock(); hub.costStarted = time.Now(); hub.costTiming = new(graphProcessCostTiming); hub.mu.Unlock()
	n.all = fresh
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: fresh[1], 2: fresh[2], 3: fresh[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: fresh[4], 5: fresh[5], 6: fresh[6]}}
	result.ProcessInstances = 12
	for id := uint64(1); id <= 6; id++ {
		state := fresh[id].call(t, processRequest{op: processStateOp})
		original := states[id-1]
		if state.incarnation == original.incarnation || state.applied != original.applied || !bytes.Equal(state.image, original.image) || state.stats[statGeneration] != original.stats[statGeneration] || state.stats[statCommit] != original.stats[statCommit] || state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 { t.Fatal("cost pre-election same-directory restore differs", id) }
		raw, err := encodeProcessResponse(state)
		if err != nil { t.Fatal(err) }
		if err := restore.Encode(struct { Stage string; Member uint64; Raw []byte }{"restored-before-campaign", id, raw}); err != nil { t.Fatal(err) }
	}
	for group := range 2 {
		f, err := os.Open(filepath.Join(n.dir, fmt.Sprintf("original-prefix-%d.jsonl", group)))
		if err != nil { t.Fatal(err) }
		nativeGraphCostPrefix(t, n, group, states[3*group], 0, nil, json.NewDecoder(f))
		if err := f.Close(); err != nil { t.Fatal(err) }
	}
	result.ReopenRestoreNanos = time.Since(start).Nanoseconds()
	start = time.Now()
	for group, leader := range n.leaders {
		n.groups[group].event(t, leader, processRequest{op: processCampaign})
		n.read(t, leader, observeConfiguration, 0)
		actual := n.all[leader].call(t, processRequest{op: processStateOp})
		if !actual.ready { t.Fatal("cost native completed configuration ReadIndex missing", leader) }
		raw, err := encodeProcessResponse(actual)
		if err != nil { t.Fatal(err) }
		if err := restore.Encode(struct { Stage string; Member uint64; Raw []byte }{"completed-native-configuration-read", leader, raw}); err != nil { t.Fatal(err) }
	}
	result.ReopenAuthorityNanos = time.Since(start).Nanoseconds()
	start = time.Now()
	f, observations := nativeGraphCostArtifact(t, n, "observations-after-reopen.jsonl")
	nativeGraphCostCheck(t, n, spec, old, 5, observations)
	nativeGraphCostCheck(t, n, spec, final, 6, observations)
	if err := f.Close(); err != nil { t.Fatal(err) }
	nativeGraphCostEqualArtifacts(t, n, "observations.jsonl", "observations-after-reopen.jsonl")
	nativeGraphCostAnchors(t, n, spec, old, final, "anchors-after-reopen.jsonl")
	nativeGraphCostEqualArtifacts(t, n, "anchors-before-reopen.jsonl", "anchors-after-reopen.jsonl")
	f, indexes = nativeGraphCostArtifact(t, n, "indexes-after-reopen.jsonl")
	nativeGraphCostIndexes(t, n, spec, old, 5, indexes)
	nativeGraphCostIndexes(t, n, spec, final, 6, indexes)
	if err := f.Close(); err != nil { t.Fatal(err) }
	nativeGraphCostEqualIndexes(t, n)
	result.ReopenVerificationNanos = time.Since(start).Nanoseconds()
	// All certified owners are closed before the bounded six-original replay tail.
	// Its new protocol entries/empty CDC never enter frozen business accounting.
	start = time.Now()
	f, err = os.Open(filepath.Join(n.dir, "grants.jsonl"))
	if err != nil { t.Fatal(err) }
	info, err := f.Stat()
	if err != nil || info.Size() > 6*nativeGraphCostPageBytes { _ = f.Close(); t.Fatal("cost bounded grant receipt", err) }
	d := json.NewDecoder(f); d.DisallowUnknownFields()
	_, replay := nativeGraphCostArtifact(t, n, "grant-replay-tail.jsonl")
	grants := (spec.homeCount()+block-1)/block+1
	if grants < 2 || grants > 3 { t.Fatal("cost replay command count") }
	for i := uint64(0); i < grants; i++ {
		var grant nativeGraphCostGrantReceipt
		if err := d.Decode(&grant); err != nil { t.Fatal(err) }
		partition, sequence := uint64(3), i+1
		if i+1 == grants { partition, sequence = 8, 1 }
		_, count, err := nativeGraphCostGrantPlan(spec, block, partition, sequence)
		if err != nil || grant.Partition != partition || grant.Sequence != sequence || grant.RequestedCount != count { t.Fatal("cost original grant correspondence", err) }
		for j, command := range []struct { Wire, Outcome, CDC []byte }{{grant.ReserveProposal, grant.ReserveOutcome, grant.ReserveCDC}, {grant.InstallProposal, grant.InstallOutcome, grant.InstallCDC}} {
			proposal, err := decodeCertifiedProcessProposal(command.Wire)
			if err != nil { t.Fatal(err) }
			ns := namespace{graph: proposal.target.graph, partition: proposal.target.partition}
			original, err := decodeDeclaredOutcome(command.Outcome, ns)
			if err != nil { t.Fatal(err) }
			got, record, state := nativeGraphCostControl(t, n, command.Wire, true)
			if got.identity != original.identity || got.hash != original.hash || got.index != original.index || got.kind != original.kind || got.reason != original.reason { t.Fatal("cost grant replay lost actual original", got, original) }
			leader := n.leaders[0]
			if proposal.target.partition == 8 { leader = n.leaders[1] }
			retained := n.all[leader].call(t, processRequest{op: processEntryRecord, index: original.index})
			if !bytes.Equal(retained.outcome, command.Outcome) || !bytes.Equal(retained.changes, command.CDC) { t.Fatal("cost grant replay replaced original retained record") }
			if err := replay.Encode(struct { Partition, Sequence uint64; Command int; Proposal, OriginalOutcome, ReplayOutcome, CDC []byte; EntryIndex uint64 }{partition, sequence, j, command.Wire, command.Outcome, record.outcome, record.changes, state.applied}); err != nil { t.Fatal(err) }
			result.GrantReplayCommands++
		}
	}
	var extra nativeGraphCostGrantReceipt
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) { t.Fatal("extra original grants", err) }
	if err := f.Close(); err != nil { t.Fatal(err) }
	if result.GrantReplayCommands != int(2*grants) { t.Fatal("cost replay tail command cardinality") }
	for group, leader := range n.leaders {
		original, err := os.Open(filepath.Join(n.dir, fmt.Sprintf("original-prefix-%d.jsonl", group)))
		if err != nil { t.Fatal(err) }
		nativeGraphCostPrefix(t, n, group, states[3*group], 0, nil, json.NewDecoder(original))
		if err := original.Close(); err != nil { t.Fatal(err) }
		state := n.all[leader].call(t, processRequest{op: processStateOp})
		if !bytes.Equal(state.image, states[3*group].image) { t.Fatal("cost authority/replay tail changed business root", group) }
		f, stream := nativeGraphCostArtifact(t, n, fmt.Sprintf("protocol-tail-%d.jsonl", group))
		nativeGraphCostPrefix(t, n, group, state, states[3*group].applied, stream, nil)
		if err := f.Close(); err != nil { t.Fatal(err) }
	}
	result.GrantReplayNanos = time.Since(start).Nanoseconds()
	result.ReopenQualified = true
}

// Continuation tokens belong to different authenticated session lifetimes.
// Compare exact answer content/cardinality, while retaining both genuine raw
// tokens in the receipts rather than manufacturing equal handles or boundaries.
func nativeGraphCostEqualIndexes(t *testing.T, n *certifiedProcessNetwork) {
	t.Helper()
	var decoders [2]*json.Decoder
	for i, name := range []string{"indexes-before-reopen.jsonl", "indexes-after-reopen.jsonl"} {
		f, err := os.Open(filepath.Join(n.dir, name))
		if err != nil { t.Fatal(err) }
		decoders[i] = json.NewDecoder(f)
		defer func() { if err := f.Close(); err != nil { t.Error(err) } }()
	}
	for {
		var pages [2]nativeGraphCostIndexPage
		left, right := decoders[0].Decode(&pages[0]), decoders[1].Decode(&pages[1])
		if errors.Is(left, io.EOF) && errors.Is(right, io.EOF) { break }
		if left != nil || right != nil { t.Fatal("cost index answer count differs", left, right) }
		x, y := pages[0], pages[1]
		if x.Kind != y.Kind || x.Endpoint != y.Endpoint || x.Life != y.Life || x.Complete != y.Complete || x.ValueFound != y.ValueFound || x.ValueID != y.ValueID || x.Value != y.Value || !reflect.DeepEqual(x.Rows, y.Rows) { t.Fatal("cost retained index answers differ after reopen") }
	}
}

func nativeGraphCostAnchors(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, old, final graphCut, name string) {
	t.Helper()
	f, stream := nativeGraphCostArtifact(t, n, name)
	for _, cut := range []graphCut{old, final} {
		handle, actual := nativeGraphCostOpen(t, n, graphReadAt, cut)
		if !reflect.DeepEqual(actual, cut) { t.Fatal("cost anchor At substituted cut") }
		for _, id := range []uint64{1, 3, spec.remoteNode()} {
			response := n.all[2].call(t, processRequest{op: processCostObserve, index: handle, entity: id})
			var entity nativeLifecycleEntity
			if err := json.Unmarshal(response.aux, &entity); err != nil { t.Fatal(err) }
			if entity.ID != id || !entity.Found || !entity.Effective || entity.Kind != byte(graphstate.Node) || entity.EffectiveLife != id+1 || !entity.Presence.Present || entity.Presence.Handle != id+1 || entity.Source != 0 || entity.Target != 0 || len(entity.Labels) != 0 || entity.Property.Present { t.Fatal("cost retained endpoint anchor changed", id, entity) }
			if err := stream.Encode(entity); err != nil { t.Fatal(err) }
		}
		n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	}
	if err := f.Close(); err != nil { t.Fatal(err) }
}
