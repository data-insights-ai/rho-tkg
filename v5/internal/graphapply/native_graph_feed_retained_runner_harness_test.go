package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

// A stream records every actual attempt, including refusal. Counters are fixed
// per owner; a failed reply cannot mint a new operation, refund a grant or turn
// a copied page/token into an acknowledgement.
type nativeFeedRetainedClient struct {
	t      *testing.T
	ctx    context.Context
	n      *certifiedProcessNetwork
	member uint64
	stream *json.Encoder
	stage  string
	extra  int
	calls  uint64
}

func (c *nativeFeedRetainedClient) exchange(q nativeFeedInput) (reply nativeFeedReply, err error) {
	q.Version = 1
	wire, err := nativeFeedRetainedInputWire(q)
	if err != nil {
		return reply, err
	}
	started := time.Now()
	c.calls++
	for attempt := 1; attempt <= nativeFeedSameInvocations; attempt++ {
		if attempt > 1 {
			if c.extra >= nativeFeedExtraInvocations {
				return reply, errLimit
			}
			c.extra++
		}
		attemptStarted := time.Now()
		response, cause := nativeGraphCostExchange(c.ctx, c.n.all[c.member], processRequest{op: processCostObserve, data: wire})
		reply = nativeFeedReply{}
		if len(response.aux) != 0 {
			if err := nativeFeedDecodeJSON(response.aux, &reply); err != nil {
				return reply, err
			}
		}
		trace := nativeFeedTrace{Stage: c.stage, Member: c.member, Input: q, Reply: reply, Call: c.calls, Attempt: attempt, AttemptNanos: time.Since(attemptStarted).Nanoseconds(), OperationNanos: time.Since(started).Nanoseconds()}
		if cause != nil {
			trace.Error = cause.Error()
		}
		if err := c.stream.Encode(trace); err != nil {
			return reply, err
		}
		if cause == nil {
			if reply.Version != 1 || reply.Action != q.Action || reply.Failure != 0 {
				return reply, errCorrupt
			}
			return reply, nil
		}
		if c.ctx.Err() != nil || errors.Is(cause, errInvalid) || errors.Is(cause, errCorrupt) || errors.Is(cause, errGraphPinReleased) || errors.Is(cause, errGraphPinExpired) || errors.Is(cause, errGraphHistoryExpired) {
			return reply, cause
		}
		if !nativeGraphCostRetrySame(cause) && reply.Failure != 1 && reply.Failure != 7 {
			return reply, cause
		}
	}
	return reply, errRetainedFeedPending
}
func (c *nativeFeedRetainedClient) call(q nativeFeedInput) nativeFeedReply {
	c.t.Helper()
	r, err := c.exchange(q)
	if err != nil {
		c.t.Fatal("retained-only native operation refused", err)
	}
	return r
}

func nativeFeedRetainedChildEnv(s nativeFeedRetainedSpec, a nativeFeedRetainedAdmission, reopen bool) ([]string, error) {
	cost, err := json.Marshal(s.cost())
	if err != nil {
		return nil, err
	}
	spec, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	transport, err := json.Marshal(a.Transport)
	if err != nil {
		return nil, err
	}
	env := []string{"RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_FEED_RETAINED_PROCESS=1", "RHO_GRAPH_FEED_PROCESS=", "RHO_GRAPH_CREATE_UPDATE_PROCESS=", "RHO_GRAPH_SELECTORS_PROCESS=", "RHO_GRAPH_DURABLE_COLUMN_PROCESS=", "RHO_GRAPH_EDGE_UNIQUE_PROCESS=", "RHO_GRAPH_NATIVE_WORKLOAD=", "RHO_GRAPH_STATE_RETENTION_PROCESS=", "RHO_GRAPH_COST_SPEC=" + string(cost), "RHO_GRAPH_WORKLOAD_SPEC=" + string(spec), "RHO_GRAPH_WORKLOAD_RUN=" + a.Run, "RHO_GRAPH_WORKLOAD_SECONDS=" + strconv.FormatUint(a.BudgetSeconds, 10), "RHO_GRAPH_WORKLOAD_DEADLINE=" + strconv.FormatInt(a.DeadlineUnixNanos, 10), "RHO_GRAPH_WORKLOAD_TRANSPORT=" + string(transport)}
	if reopen {
		env = append(env, "RHO_GRAPH_PROCESS_REOPEN=1")
	}
	return env, nil
}
func nativeFeedRetainedAttach(t *testing.T, n *certifiedProcessNetwork, s nativeFeedRetainedSpec, a nativeFeedRetainedAdmission) {
	t.Helper()
	frames, err := nativeFeedRetainedFrames(s.cost())
	if err != nil || frames > (1<<41)/1024 || frames > uint64(^uint(0)>>1)/1024 {
		t.Fatal("finite actual transport grant", err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, a.DeadlineUnixNanos))
	t.Cleanup(cancel)
	transport, err := newNativeWorkloadTransport(ctx, a.Transport, frames*1024)
	if err != nil {
		t.Fatal(err)
	}
	attachGraphProcessMuxesWithWorkload(t, n.all, int(frames*1024), transport) // #nosec G115 -- admitted finite numeric allowance, no queue-sized allocation.
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
}
func nativeFeedRetainedNetwork(t *testing.T, s nativeFeedRetainedSpec, a nativeFeedRetainedAdmission, grants *json.Encoder) *certifiedProcessNetwork {
	t.Helper()
	env, err := nativeFeedRetainedChildEnv(s, a, false)
	if err != nil {
		t.Fatal(err)
	}
	return nativeScalarWorkloadNetwork(t, s.cost(), env, "rho-native-feed-retained-", retainedOnlyFeedSemanticContractID(), grants, func(n *certifiedProcessNetwork) { nativeFeedRetainedAttach(t, n, s, a) })
}

type nativeFeedRetainedDriver struct {
	ctx        context.Context
	n          *certifiedProcessNetwork
	spec       nativeFeedRetainedSpec
	admissions *nativeGraphCostAdmissions
	started    time.Time
	mu         sync.Mutex
	originals  uint64
	extra      int
}

func (d *nativeFeedRetainedDriver) retry() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.extra >= nativeFeedExtraInvocations {
		return errLimit
	}
	d.extra++
	return nil
}
func (d *nativeFeedRetainedDriver) beginOriginal() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.originals >= 16*d.spec.Facts+3 {
		return errLimit
	}
	d.originals++
	return nil
}
func (d *nativeFeedRetainedDriver) phase(t *testing.T, phase, from, through uint64, attempts, operations *json.Encoder, census *[32]uint64) {
	nativeScalarWorkloadPhaseRange(t, d.ctx, d.n, d.spec.cost(), d.admissions, d.started, phase, from, through, attempts, operations, census)
}

type nativeFeedRetainedGCCall struct {
	Member uint64
	Input  nativeRetentionControl
	Reply  nativeRetentionReceipt
	Error  string `json:",omitempty"`
}
type nativeFeedRetainedGCClient struct {
	t        *testing.T
	ctx      context.Context
	n        *certifiedProcessNetwork
	member   uint64
	cookie   [16]byte
	sequence uint64
	stream   *json.Encoder
}

func (c *nativeFeedRetainedGCClient) call(q nativeRetentionControl) nativeRetentionReceipt {
	c.t.Helper()
	q.Version = 1
	if q.Action != "inspect" {
		c.sequence++
		q.Cookie = c.cookie
		q.Sequence = c.sequence
	}
	wire, err := nativeRetentionWire(q)
	if err != nil {
		c.t.Fatal(err)
	}
	r, err := nativeGraphCostExchange(c.ctx, c.n.all[c.member], processRequest{op: processNativeFaultControl, data: wire})
	var reply nativeRetentionReceipt
	if len(r.aux) != 0 {
		if decode := nativeFeedDecodeJSON(r.aux, &reply); decode != nil {
			c.t.Fatal(decode)
		}
	}
	trace := nativeFeedRetainedGCCall{Member: c.member, Input: q, Reply: reply}
	if err != nil {
		trace.Error = err.Error()
	}
	if e := c.stream.Encode(trace); e != nil {
		c.t.Fatal(e)
	}
	if err != nil {
		c.t.Fatal("actual native retention operation refused", q.Action, err)
	}
	if reply.Profile != nativeFeedRetainedFormat || reply.Member != c.member || reply.PID != uint64(c.n.all[c.member].cmd.Process.Pid) || reply.Sequence != q.Sequence || reply.Action != q.Action || reply.DeadlineUnixNanos == 0 {
		c.t.Fatal("actual collector owner identity")
	}
	if q.Action == "inspect" {
		if reply.Cookie == ([16]byte{}) {
			c.t.Fatal("missing collector cookie")
		}
		c.cookie = reply.Cookie
	} else if reply.Cookie != c.cookie {
		c.t.Fatal("collector owner changed")
	}
	if err := c.n.all[c.member].mux.hub.enqueue(c.member, r.packets); err != nil {
		c.t.Fatal(err)
	}
	return reply
}
func nativeFeedRetainedCollect(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, digest [32]byte, cursor retainedFeedCursor, stream *json.Encoder) (plans [6]nativeRetentionReceipt) {
	t.Helper()
	for group, leader := range n.leaders {
		position := cursor.positions[group]
		if position < 3 {
			t.Fatal("ACK anchor not yet advanced")
		}
		c := &nativeFeedRetainedGCClient{t: t, ctx: ctx, n: n, member: leader, stream: stream}
		c.call(nativeRetentionControl{Action: "inspect"})
		q := nativeRetentionControl{Action: "prepare", Request: nativeFeedRequest(digest, "physical-gc", uint64(group)), Floors: [6]uint64{position - 1, position - 1, position, position, position - 1, 1}}
		prepared := c.call(q)
		command, err := decodeGraphRetentionCommand(prepared.Original)
		if err != nil || command.scope.semantic != retainedOnlyFeedSemanticContractID() {
			t.Fatal("actual GGC proposal", err)
		}
		c.call(nativeRetentionControl{Action: "submit"})
		if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
			t.Fatal(err)
		}
		installed := c.call(nativeRetentionControl{Action: "original"})
		result, err := decodeGraphRetentionResult(installed.Outcome)
		if err != nil || result.status != graphRetentionApplied || result.id != sha256.Sum256(prepared.Original) || len(installed.CDC) != 0 {
			t.Fatal("actual immutable GC original", err)
		}
		for member := uint64(1 + 3*group); member <= uint64(3+3*group); member++ {
			collector := c
			if member != leader {
				collector = &nativeFeedRetainedGCClient{t: t, ctx: ctx, n: n, member: member, stream: stream}
				collector.call(nativeRetentionControl{Action: "inspect"})
				recovered := collector.call(nativeRetentionControl{Action: "recover", Original: prepared.Original})
				if !recovered.Found || !bytes.Equal(recovered.Outcome, installed.Outcome) {
					t.Fatal("voter GC original changed")
				}
			}
			for {
				r := collector.call(nativeRetentionControl{Action: "collect"})
				if r.Progress.Complete {
					break
				}
			}
			plans[member-1] = collector.call(nativeRetentionControl{Action: "current"})
			if plans[member-1].Plan.ID != result.id || plans[member-1].Plan.RootFloor != position-1 || plans[member-1].Plan.FeedFloor != position || plans[member-1].Plan.OutcomeFloor != position {
				t.Fatal("actual sparse native plan floors")
			}
		}
	}
	return plans
}

// A bounded raw stream authenticates every actual physical index and each of
// its three voter records. Pre-GC captures remain immutable evidence; after GC
// the supplied starts must join the installed native plan, never a fake genesis.
type nativeFeedRetainedSourceTrace struct {
	Stage  string
	Member uint64
	Page   nativeFeedRetainedRecordPage
	Voters []nativeFeedVoterRecord
}

func nativeFeedRetainedSources(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, from [2]uint64, boundary graphCut, original []byte, stage string, stream *json.Encoder) (rows [2]uint64) {
	t.Helper()
	wire, err := nativeFeedCutWire(boundary)
	if err != nil {
		t.Fatal(err)
	}
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal(err)
	}
	for group, leader := range n.leaders {
		tip, err := nativeGraphCostExchange(ctx, n.all[leader], processRequest{op: processStateOp})
		if err != nil {
			t.Fatal(err)
		}
		after := from[group]
		for {
			question := nativeFeedInput{Version: 1, Action: "records", From: after, Through: tip.applied, Boundary: wire, Original: original}
			request, err := nativeFeedRetainedInputWire(question)
			if err != nil {
				t.Fatal(err)
			}
			response, err := nativeGraphCostExchange(ctx, n.all[leader], processRequest{op: processCostObserve, data: request})
			if err != nil {
				t.Fatal(err)
			}
			var page nativeFeedRetainedRecordPage
			if err := nativeFeedDecodeJSON(response.aux, &page); err != nil {
				t.Fatal(err)
			}
			p := page.Page
			if p.Partition != 3+5*uint64(group) || p.After != after || p.Through != tip.applied || p.Next < after || p.Next-after != uint64(len(p.Frames)) || len(p.Frames) > 4 || len(page.BeforeACK) != len(p.Frames) || p.Complete != (p.Next == p.Through) || !bytes.Equal(p.Root, tip.image) {
				t.Fatal("actual sparse native prefix/tip")
			}
			trace := nativeFeedRetainedSourceTrace{Stage: stage, Member: leader, Page: page, Voters: []nativeFeedVoterRecord{}}
			for offset, frame := range p.Frames {
				if frame.Partition != p.Partition || frame.Index != after+1+uint64(offset) || frame.Outcome == nil || frame.CDC == nil {
					t.Fatal("sparse source gap/missing envelope")
				}
				for member := uint64(1 + 3*group); member <= uint64(3+3*group); member++ {
					actual, err := nativeGraphCostExchange(ctx, n.all[member], processRequest{op: processEntryRecord, index: frame.Index})
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(actual.outcome, frame.Outcome) || !bytes.Equal(actual.changes, frame.CDC) {
						t.Fatal("actual sparse voter mismatch")
					}
					trace.Voters = append(trace.Voters, nativeFeedVoterRecord{member, frame.Index, actual.outcome, actual.changes})
				}
				rows[group] += uint64(len(frame.Rows))
			}
			encoded, err := json.Marshal(trace)
			if err != nil || len(encoded) > 512<<10 {
				t.Fatal("bounded sparse source frame", err)
			}
			if err := stream.Encode(json.RawMessage(encoded)); err != nil {
				t.Fatal(err)
			}
			after = p.Next
			if p.Complete {
				break
			}
			if len(p.Frames) == 0 {
				t.Fatal("sparse source stalled")
			}
		}
	}
	return rows
}

// A post-GC generation starts at a genuine certified checkpoint. It contains
// only that interval's actual delta frames, not the original empty→C history.
// Full final answers are authenticated separately against raw source rows.
func nativeFeedRetainedReadColumns(t *testing.T, clients [2]*nativeGraphEdgeUniqueColumnClient, pair *nativeGraphCostColumnPair, spec nativeGraphCostSpec, at graphCut, cached, fill bool, first uint64) {
	t.Helper()
	var census [3]uint64
	for group, c := range clients {
		r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsRead, At: nativeGraphCostColumnCutInput(t, at), Cached: cached, Fill: fill})
		if !nativeColumnSame(r.Manifest, pair.Manifests[group]) || r.Binding.Version != 2 || r.Binding.Policy.Version != 1 || r.Binding.Policy.Admission == ([32]byte{}) {
			t.Fatal("genuine checkpoint column binding")
		}
		initial, err := decodeGraphCertificate(r.Manifest.Generation.Sources[group].Initial)
		if err != nil || initial != pair.Initial.certificates[group] {
			t.Fatal("changed checkpoint generation origin", err)
		}
		position, ordinal := initial.sourceIndex, uint64(0)
		var chain [32]byte
		var frames, rows, retained uint64
		for {
			r = c.call(nativeGraphCostColumnRequest{Action: nativeColumnsNext})
			if !nativeColumnSame(r.Manifest, pair.Manifests[group]) || r.Ordinal != ordinal || r.Cached != cached || r.Fill != fill {
				t.Fatal("checkpoint column continuation")
			}
			if r.EOF {
				m := pair.Manifests[group]
				if len(r.Body)+len(r.Rows) != 0 || ordinal != m.Chunks || frames != m.Frames || rows != m.Rows || retained != m.Bytes || position != m.Positions[group] || chain != m.Chain {
					t.Fatal("complete checkpoint column EOF")
				}
				break
			}
			decoded, err := decodeNativeColumnChunk(r.Manifest.Generation, ordinal, chain, r.Body, c.binding.Policy)
			if err != nil {
				t.Fatal(err)
			}
			selected := []nativeColumnRow{}
			for _, frame := range decoded {
				if frame.Partition != initial.scope.partition || frame.Index != position+1 || frame.Outcome == nil || frame.CDC == nil {
					t.Fatal("checkpoint physical prefix gap")
				}
				position++
				frames++
				rows += uint64(len(frame.Rows))
				if frame.Index <= at.certificates[group].sourceIndex {
					for _, row := range frame.Rows {
						class, err := nativeGraphCostColumnRow(spec, row)
						if err != nil {
							t.Fatal("checkpoint actual typed row", err)
						}
						if row.Owner < 7 || (row.Owner-7)/2 < first || (row.Owner-7)/2 >= spec.Facts || row.Before == (nativeColumnCell{}) || class == 1 {
							t.Fatal("checkpoint invented initial history")
						}
						census[class]++
						selected = append(selected, row)
					}
				}
			}
			if !reflect.DeepEqual(selected, r.Rows) {
				t.Fatal("checkpoint typed/cache rows changed")
			}
			retained += uint64(len(nativeColumnChunkKey(pair.ID, ordinal)) + len(r.Body))
			chain = nativeColumnChain(chain, r.Body)
			ordinal++
		}
	}
	if census != ([3]uint64{spec.Facts - first, 0, spec.Facts - first}) {
		t.Fatal("exact guarded post-checkpoint update footprint", census)
	}
}
func nativeFeedRetainedPublishColumns(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeGraphCostSpec, initial, final graphCut, id [16]byte, stage string, failed *uint64, stream *json.Encoder, first uint64) nativeGraphCostColumnPair {
	t.Helper()
	pair := nativeGraphCostColumnPair{Initial: initial, Final: final, ID: id, Updated: true}
	clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pair, stage, true, failed, stream)
	for group, c := range clients {
		for page := uint64(0); ; page++ {
			if page > c.binding.Policy.MaxChunks {
				t.Fatal("finite checkpoint generation publication")
			}
			r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsPublish})
			if r.EOF {
				pair.Manifests[group] = r.Manifest
				break
			}
		}
	}
	if err := nativeColumnJoinBoundaries(pair.Bindings, pair.Manifests); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []struct {
		name         string
		cached, fill bool
	}{{"native", false, false}, {"fill", false, true}, {"cached", true, false}} {
		for _, c := range clients {
			c.stage = stage + "/" + mode.name
		}
		nativeFeedRetainedReadColumns(t, clients, &pair, s, final, mode.cached, mode.fill, first)
	}
	nativeGraphEdgeUniqueColumnCloseClients(clients)
	return pair
}
func nativeFeedRetainedRetireColumns(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, pair *nativeGraphCostColumnPair, stage string, failed *uint64, stream *json.Encoder) {
	t.Helper()
	clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, pair, stage, false, failed, stream)
	for _, c := range clients {
		r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsRetire})
		if r.Retention == nil || r.Retention.State != nativeColumnRetired || !r.Closed {
			t.Fatal("C-dependent native generation still live")
		}
		c.terminalProbe(pair.ID, r.Retention.Token)
	}
}
