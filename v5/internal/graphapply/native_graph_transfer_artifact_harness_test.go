package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// All growing observations are streamed. Only fixed per-caller/per-replica
// censuses and the at-most-two HOME move receipts remain in the end record.
type nativeTransferResult struct {
	Version                                                                            int                `json:"version"`
	Format                                                                             string             `json:"format"`
	Engine                                                                             string             `json:"engine"`
	Spec                                                                               nativeTransferSpec `json:"spec"`
	Admission                                                                          nativeTransferAdmission
	AdmissionSHA256                                                                    [32]byte
	Directory, ArtifactDirectory                                                       string
	Initial, Snapshot, Checkpoint, Final, Original, Token                              []byte
	Completed                                                                          [32]uint64
	InitialHOME                                                                        [2]nativeTransferAllocationReceipt
	Remote, Successor                                                                  nativeTransferAllocationReceipt
	Moves                                                                              [2][]byte
	MoveCount                                                                          uint64
	K, KOriginal, ChangedCaller, ChangedReply, LostRequest, LostReply, Frozen, Aborted []byte
	KReopened                                                                          []byte
	GenericAtAfterRelease                                                              string
	ColumnBindings                                                                     [2][2]nativeColumnBinding
	ColumnManifests                                                                    [2][2]nativeColumnManifest
	ColumnFailed                                                                       [2]uint64
	GC                                                                                 [6]nativeRetentionReceipt
	HeldGC                                                                             [6]nativeRetentionReceipt
	HeldSparseFrom                                                                     [2]uint64
	CheckpointFirst                                                                    uint64
	PendingRequest                                                                     [16]byte
	PendingPage                                                                        []byte
	PendingDigest                                                                      [32]byte
	SparseFrom                                                                         [2]uint64
	SourceRows                                                                         [3][2]uint64
	Pages, BusinessPages, ProgressPages, FramesCeiling, OriginalCaptures               uint64
	ExtraInvocations, ControlExtraInvocations, ProcessInstances, Reaped                int
	Transport                                                                          []nativeWorkloadTransportReceipt
	PersistedFileBytes, AllocatedFileBytes                                             uint64
	SetupNanos, BusinessNanos, TransferNanos, DiagnosticNanos, TotalNanos              int64
	BusinessPhases                                                                     [5]nativeCreateUpdatePhase
	Scope, RetentionContract                                                           string
}

type nativeTransferMoveTrace struct {
	Ordinal, Origin, Partition uint64
	Request, Reply             []byte
	CommandSHA256              [32]byte
}

func nativeTransferObserveMoveTraffic(t *testing.T, n *certifiedProcessNetwork, a nativeTransferAdmission, out *json.Encoder) func() {
	t.Helper()
	hub := n.all[1].mux.hub
	var mu sync.Mutex
	var count, owned uint64
	maximum := 8 * (4*a.Grants.MaxPages + 128)
	hub.mu.Lock()
	prior := hub.afterRelay
	hook := func(ctx context.Context, origin, partition uint64, request, reply []byte) error {
		if prior != nil {
			if e := prior(ctx, origin, partition, request, reply); e != nil {
				return e
			}
		}
		q, e := decodeGraphSourceRequest(request)
		if e != nil {
			return e
		}
		if q.kind != sourceSubmit {
			return nil
		}
		proposal, e := decodeGraphSourceProposal(q.command)
		if e != nil {
			return e
		}
		if !isRangeMoveWire(proposal.wire) {
			return nil
		}
		command, e := decodeRangeMoveCommand(proposal.wire)
		if e != nil || command.scope.semantic != rangeMovePooledSemanticContractID() {
			return errors.Join(errCorrupt, e)
		}
		mu.Lock()
		defer mu.Unlock()
		// Native packet and original grants are separate. This bounds only real
		// diagnostic disk output and never reserves or discounts application rows.
		cost := uint64(512 + 2*(len(request)+len(reply)))
		if count >= maximum || cost > (1<<40)-owned {
			return errLimit
		}
		count++
		owned += cost
		return out.Encode(nativeTransferMoveTrace{count, origin, partition, request, reply, sha256.Sum256(proposal.wire)})
	}
	hub.afterRelay = hook
	hub.mu.Unlock()
	return func() { hub.mu.Lock(); hub.afterRelay = prior; hub.mu.Unlock() }
}

func nativeTransferPointShape(s nativeTransferSpec, p nativeFeedPoint, value int64) error {
	return nativeCreateUpdateCheckPoint(nativeFeedSpec{Facts: s.Facts, CrossPercent: s.CrossPercent, Writers: s.Writers}, p, value)
}

type nativeTransferGCClient struct {
	t        *testing.T
	ctx      context.Context
	n        *certifiedProcessNetwork
	member   uint64
	cookie   [16]byte
	sequence uint64
	stream   *json.Encoder
}

type nativeTransferGCCensus struct {
	Type, Stage string
	Member      uint64
	Response    []byte
}

func (c *nativeTransferGCClient) call(q nativeRetentionControl) nativeRetentionReceipt {
	c.t.Helper()
	q.Version = 1
	if q.Action != "inspect" {
		c.sequence++
		q.Cookie, q.Sequence = c.cookie, c.sequence
	}
	w, e := nativeRetentionWire(q)
	if e != nil {
		c.t.Fatal(e)
	}
	r, e := nativeGraphCostExchange(c.ctx, c.n.all[c.member], processRequest{op: processNativeFaultControl, data: w})
	var out nativeRetentionReceipt
	if len(r.aux) > 0 {
		if e := nativeFeedDecodeJSON(r.aux, &out); e != nil {
			c.t.Fatal(e)
		}
	}
	trace := nativeFeedRetainedGCCall{Member: c.member, Input: q, Reply: out}
	if e != nil {
		trace.Error = e.Error()
	}
	if e := c.stream.Encode(trace); e != nil {
		c.t.Fatal(e)
	}
	if e != nil {
		c.t.Fatal("actual pooled native retention operation refused", q.Action, e)
	}
	if out.Profile != nativeTransferFormat || out.Member != c.member || out.PID != uint64(c.n.all[c.member].cmd.Process.Pid) || out.Sequence != q.Sequence || out.Action != q.Action || out.DeadlineUnixNanos == 0 {
		c.t.Fatal("actual native collector identity")
	}
	if q.Action == "inspect" {
		if out.Cookie == ([16]byte{}) {
			c.t.Fatal("missing genuine GC cookie")
		}
		c.cookie = out.Cookie
	} else if out.Cookie != c.cookie {
		c.t.Fatal("collector owner changed")
	}
	if e := c.n.all[c.member].mux.hub.enqueue(c.member, r.packets, r.handles...); e != nil {
		c.t.Fatal(e)
	}
	return out
}

func nativeTransferCollect(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, digest [32]byte, ordinal uint64, floors [2][6]uint64, stream *json.Encoder) (plans [6]nativeRetentionReceipt) {
	t.Helper()
	for group, leader := range n.leaders {
		c := &nativeTransferGCClient{t: t, ctx: ctx, n: n, member: leader, stream: stream}
		c.call(nativeRetentionControl{Action: "inspect"})
		prepared := c.call(nativeRetentionControl{Action: "prepare", Request: nativeFeedRequest(digest, "transfer-physical-gc", 2*ordinal+uint64(group)), Floors: floors[group]})
		command, e := decodeGraphRetentionCommand(prepared.Original)
		if e != nil || command.scope.semantic != rangeMovePooledSemanticContractID() {
			t.Fatal("actual pooled GGC proposal", e)
		}
		c.call(nativeRetentionControl{Action: "submit"})
		if e := drainGraphProcessMuxes(ctx, n.all); e != nil {
			t.Fatal(e)
		}
		installed := c.call(nativeRetentionControl{Action: "original"})
		result, e := decodeGraphRetentionResult(installed.Outcome)
		if e != nil || result.status != graphRetentionApplied || result.id != sha256.Sum256(prepared.Original) || len(installed.CDC) != 0 {
			t.Fatal("genuine installed original before physical GC", e)
		}
		for member := uint64(1 + 3*group); member <= uint64(3+3*group); member++ {
			collector := c
			if member != leader {
				collector = &nativeTransferGCClient{t: t, ctx: ctx, n: n, member: member, stream: stream}
				collector.call(nativeRetentionControl{Action: "inspect"})
				recovered := collector.call(nativeRetentionControl{Action: "recover", Original: prepared.Original})
				if !recovered.Found || !bytes.Equal(recovered.Outcome, installed.Outcome) {
					t.Fatal("native voter plan original changed")
				}
			}
			before, e := nativeGraphCostExchange(ctx, n.all[member], processRequest{op: processStateOp})
			if e != nil {
				t.Fatal(e)
			}
			if e := stream.Encode(nativeTransferGCCensus{"census", fmt.Sprintf("gc-%d-before-collect", ordinal), member, mustNativeTransferResponse(t, before)}); e != nil {
				t.Fatal(e)
			}
			for {
				r := collector.call(nativeRetentionControl{Action: "collect"})
				if r.Progress.Complete {
					break
				}
			}
			after, e := nativeGraphCostExchange(ctx, n.all[member], processRequest{op: processStateOp})
			if e != nil {
				t.Fatal(e)
			}
			if after.stats[statActiveRecords] >= before.stats[statActiveRecords] || after.stats[statActiveBytes] >= before.stats[statActiveBytes] {
				t.Fatal("required actual eligible native collection reclaimed no state", ordinal, member)
			}
			if e := stream.Encode(nativeTransferGCCensus{"census", fmt.Sprintf("gc-%d-after-collect", ordinal), member, mustNativeTransferResponse(t, after)}); e != nil {
				t.Fatal(e)
			}
			plans[member-1] = collector.call(nativeRetentionControl{Action: "current"})
			if plans[member-1].Plan.ID != result.id || plans[member-1].Plan.RootFloor != floors[group][0] || plans[member-1].Plan.FeedFloor != floors[group][2] || plans[member-1].Plan.OutcomeFloor != floors[group][3] {
				t.Fatal("actual retained native floors differ")
			}
		}
	}
	return plans
}

func nativeTransferSnapshot(c *nativeTransferClient, s nativeTransferSpec) {
	c.t.Helper()
	for from := uint64(0); from < s.Facts; {
		r := c.feed(nativeFeedInput{Action: "snapshot", From: from})
		if r.Next != min(from+16, s.Facts) || r.EOF != (r.Next == s.Facts) || uint64(len(r.Points)) != r.Next-from {
			c.t.Fatal("complete held snapshot C page")
		}
		for i, p := range r.Points {
			if p.Fact != from+uint64(i) || nativeTransferPointShape(s, p, 5) != nil {
				c.t.Fatal("held snapshot changed after move/update", p.Fact)
			}
		}
		from = r.Next
	}
}

func nativeTransferQueryOpen(c *nativeTransferClient, q nativeSelectorsInput) (nativeSelectorsReply, graphCut) {
	c.t.Helper()
	r := c.query(q)
	cut, e := nativeTransferCutDecode(r.Cut)
	if e != nil || r.Handle == 0 {
		c.t.Fatal("genuine pooled query boundary", e)
	}
	if q.Action == "prepare" {
		if len(r.Original) == 0 {
			c.t.Fatal("missing exact pending original")
		}
		r.Token = c.query(nativeSelectorsInput{Action: "publish", Handle: r.Handle}).Token
		if _, e := decodeRetainedPinToken(r.Token); e != nil {
			c.t.Fatal(e)
		}
	}
	return r, cut
}

// The native index is the actual incident/type posting stream. Every candidate
// is hydrated under the SAME certified cut and independently raw-joined later.
func nativeTransferObserve(c *nativeTransferClient, s nativeTransferSpec, handle uint64, updated bool) {
	c.t.Helper()
	began := time.Now()
	sequence, count := uint64(0), uint64(0)
	for {
		sequence++
		r := c.query(nativeSelectorsInput{Action: "scan", Handle: handle, Sequence: sequence})
		if len(r.Points) != len(r.Candidates) || len(r.Points) > 16 {
			c.t.Fatal("index candidate/hydration mismatch")
		}
		for i, p := range r.Points {
			want := int64(5)
			if updated {
				want = 6
			}
			if count >= s.Facts || p.ID != 7+2*count || r.Candidates[i] != p.ID || nativeTransferPointShape(s, nativeFeedPoint{Fact: count, Entity: p}, want) != nil {
				c.t.Fatal("exact old/current index/typed point set", count)
			}
			count++
		}
		if r.EOF {
			break
		}
		if sequence > (s.Facts+15)/16+2 {
			c.t.Fatal("bounded index EOF missing")
		}
	}
	if count != s.Facts {
		c.t.Fatal("index omitted native relation", count)
	}
	sequence++
	ids := []uint64{1, 3, s.cost().remoteNode(), 7, 7 + 2*(s.Facts-1), s.cost().homeCount()}
	r := c.query(nativeSelectorsInput{Action: "points", Handle: handle, Sequence: sequence, IDs: ids})
	if len(r.Points) != len(ids) {
		c.t.Fatal("fixed complete point set")
	}
	for i, p := range r.Points {
		if p.ID != ids[i] || p.Found != (i != len(ids)-1) {
			c.t.Fatal("phantom/existing entity mismatch")
		}
	}
	again := c.query(nativeSelectorsInput{Action: "points", Handle: handle, Sequence: sequence, IDs: ids})
	if !nativeColumnSame(r, again) {
		c.t.Fatal("SAME observation changed")
	}
	if e := c.stream.Encode(struct {
		Stage, Type string
		Rows        uint64
		WallNanos   int64
	}{c.stage, "whole-observation-timing", count, time.Since(began).Nanoseconds()}); e != nil {
		c.t.Fatal(e)
	}
}

func nativeTransferDrain(c *nativeTransferClient, boundary graphCut, digest [32]byte, ordinal *uint64, result *nativeTransferResult) {
	nativeTransferDeliver(c, boundary, digest, ordinal, result, false, false, nil)
}

// Move lineage is still emitted, counted and ACKed, but cannot satisfy the
// useful-update stop or select the unread update held across client loss.
func nativeTransferUsefulPage(page retainedFeedPage) bool {
	return page.kind == feedLocalEnvelope || page.kind == feedCrossEnvelope
}

func nativeTransferDeliver(c *nativeTransferClient, boundary graphCut, digest [32]byte, ordinal *uint64, result *nativeTransferResult, stopAfterBusiness, replace bool, processes *json.Encoder) {
	c.t.Helper()
	wire, e := nativeFeedCutWire(boundary)
	if e != nil {
		c.t.Fatal(e)
	}
	for {
		if result.Pages >= nativeTransferPages(result.Spec.cost()) {
			c.t.Fatal("finite complete feed page grant exhausted")
		}
		*ordinal++
		request := nativeFeedRequest(digest, "transfer-page", *ordinal)
		r := c.feed(nativeFeedInput{Action: "page", Request: request, Boundary: wire})
		arena, e := graphstate.NewOutputBudget(1 << 20)
		if e != nil {
			c.t.Fatal(e)
		}
		page, e := decodeRetainedFeedPage(r.Page, 64<<10, arena)
		if e != nil || sha256.Sum256(r.Page) != r.Digest {
			c.t.Fatal("exact native sealed page", e)
		}
		again := c.feed(nativeFeedInput{Action: "page", Request: request, Boundary: wire})
		if !again.Replay || !bytes.Equal(again.Page, r.Page) || again.Digest != r.Digest {
			c.t.Fatal("SAME Page changed")
		}
		if replace && nativeTransferUsefulPage(page) && len(result.PendingPage) == 0 {
			result.PendingRequest, result.PendingPage, result.PendingDigest = request, bytes.Clone(r.Page), r.Digest
			nativeScalarWorkloadReplaceClient(c.t, c.ctx, c.n, processes, func() ([]string, error) { return nativeTransferChildEnv(result.Spec, result.Admission, true) })
			result.ProcessInstances++
			result.Reaped++
			c.stage = "same-page-after-GC-client-loss"
			c.feed(nativeFeedInput{Action: "open", Token: result.Token})
			replay := c.feed(nativeFeedInput{Action: "page", Request: request, Boundary: wire})
			if !replay.Replay || !bytes.Equal(replay.Page, r.Page) || replay.Digest != r.Digest {
				c.t.Fatal("actual pending Page changed after client loss")
			}
		}
		ack := c.feed(nativeFeedInput{Action: "ack", Request: request, Digest: r.Digest})
		ackAgain := c.feed(nativeFeedInput{Action: "ack", Request: request, Digest: r.Digest})
		if !bytes.Equal(ack.Completed, ackAgain.Completed) {
			c.t.Fatal("SAME ACK changed complete cut")
		}
		result.Pages++
		if page.kind == 0 {
			result.ProgressPages++
		} else {
			result.BusinessPages++
		}
		if page.eof {
			actual, e := nativeTransferCutDecode(ack.Completed)
			if e != nil || !feedCutEqual(actual, boundary) {
				c.t.Fatal("half transaction/lineage became completed cut", e)
			}
			if stopAfterBusiness {
				c.t.Fatal("schedule drained the required unread suffix")
			}
			return
		}
		complete, e := nativeTransferCutDecode(ack.Completed)
		if e != nil || feedCutEqual(complete, boundary) {
			c.t.Fatal("nonterminal page advanced complete cut", e)
		}
		if stopAfterBusiness && nativeTransferUsefulPage(page) {
			return
		}
	}
}

func nativeTransferSources(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, from [2]uint64, boundary graphCut, original []byte, stage string, stream *json.Encoder, client *nativeTransferClient) (rows [2]uint64) {
	t.Helper()
	wire, e := nativeFeedCutWire(boundary)
	if e != nil {
		t.Fatal(e)
	}
	if e := drainGraphProcessMuxes(ctx, n.all); e != nil {
		t.Fatal(e)
	}
	for group, leader := range n.leaders {
		tip, e := nativeGraphCostExchange(ctx, n.all[leader], processRequest{op: processStateOp})
		if e != nil {
			t.Fatal(e)
		}
		client.member, client.stage = leader, stage
		for after := from[group]; ; {
			r := client.must(nativeTransferInput{Action: "feed", Feed: &nativeFeedInput{Version: 1, Action: "records", From: after, Through: tip.applied, Boundary: wire, Original: original}})
			var page nativeFeedRetainedRecordPage
			if e := nativeFeedDecodeJSON(r.Body, &page); e != nil {
				t.Fatal(e)
			}
			p := page.Page
			if p.Partition != 3+5*uint64(group) || p.After != after || p.Through != tip.applied || p.Next < after || p.Next-after != uint64(len(p.Frames)) || len(p.Frames) > 4 || len(page.BeforeACK) != len(p.Frames) || p.Complete != (p.Next == p.Through) || !bytes.Equal(p.Root, tip.image) {
				t.Fatal("native authenticated physical prefix")
			}
			trace := nativeFeedRetainedSourceTrace{Stage: stage, Member: leader, Page: page, Voters: []nativeFeedVoterRecord{}}
			for offset, frame := range p.Frames {
				if frame.Partition != p.Partition || frame.Index != after+1+uint64(offset) || frame.Outcome == nil || frame.CDC == nil {
					t.Fatal("native source gap")
				}
				for member := uint64(1 + 3*group); member <= uint64(3+3*group); member++ {
					actual, e := nativeGraphCostExchange(ctx, n.all[member], processRequest{op: processEntryRecord, index: frame.Index})
					if e != nil {
						t.Fatal(e)
					}
					if !bytes.Equal(actual.outcome, frame.Outcome) || !bytes.Equal(actual.changes, frame.CDC) {
						t.Fatal("native voter original mismatch")
					}
					trace.Voters = append(trace.Voters, nativeFeedVoterRecord{Member: member, Index: frame.Index, Outcome: actual.outcome, CDC: actual.changes})
				}
				rows[group] += uint64(len(frame.Rows))
			}
			if e := stream.Encode(trace); e != nil {
				t.Fatal(e)
			}
			after = p.Next
			if p.Complete {
				break
			}
			if len(p.Frames) == 0 {
				t.Fatal("raw source stalled")
			}
		}
	}
}
