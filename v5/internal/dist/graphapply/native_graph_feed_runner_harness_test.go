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
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type nativeFeedTrace struct {
	Stage                        string
	Member                       uint64
	Input                        nativeFeedInput
	Reply                        nativeFeedReply
	Error                        string `json:",omitempty"`
	Call                         uint64
	Attempt                      int
	AttemptNanos, OperationNanos int64
}
type nativeFeedClient struct {
	t      *testing.T
	ctx    context.Context
	n      *certifiedProcessNetwork
	member uint64
	stream *json.Encoder
	stage  string
	extra  *int
	next   uint64
}

func (c *nativeFeedClient) call(q nativeFeedInput) (reply nativeFeedReply) {
	c.t.Helper()
	started := time.Now()
	c.next++
	q.Version = 1
	wire, err := nativeFeedInputWire(q)
	if err != nil {
		c.t.Fatal(err)
	}
	for attempt := 0; attempt < nativeFeedSameInvocations; attempt++ {
		reply = nativeFeedReply{}
		if attempt > 0 {
			if *c.extra >= nativeFeedExtraInvocations {
				c.t.Fatal("finite feed retry pool exhausted")
			}
			*c.extra++
		}
		attemptStarted := time.Now()
		response, e := nativeGraphCostExchange(c.ctx, c.n.all[c.member], processRequest{op: processCostObserve, data: wire})
		trace := nativeFeedTrace{Stage: c.stage, Member: c.member, Input: q, Call: c.next, Attempt: attempt + 1, AttemptNanos: time.Since(attemptStarted).Nanoseconds(), OperationNanos: time.Since(started).Nanoseconds()}
		if len(response.aux) != 0 {
			err = nativeFeedDecodeJSON(response.aux, &reply)
			if err != nil {
				c.t.Fatal(err)
			}
			trace.Reply = reply
		}
		if e != nil {
			trace.Error = e.Error()
		}
		if err := c.stream.Encode(trace); err != nil {
			c.t.Fatal(err)
		}
		if e == nil {
			if reply.Version != 1 || reply.Action != q.Action || reply.Failure != 0 {
				c.t.Fatal("feed reply identity")
			}
			return reply
		}
		if c.ctx.Err() != nil || errors.Is(e, errInvalid) || errors.Is(e, errCorrupt) || errors.Is(e, errGraphPinExpired) || errors.Is(e, errGraphPinReleased) || errors.Is(e, errGraphHistoryExpired) {
			c.t.Fatal("native feed operation refused", e)
		}
		// Only native pending/limit or an inner read-lease deadline can repeat the
		// SAME request. No missing reply becomes ACK, ABORT or a reminted identity.
		if !nativeGraphCostRetrySame(e) && reply.Failure != 1 && reply.Failure != 7 {
			c.t.Fatal("unknown feed operation preserved", e)
		}
	}
	c.t.Fatal("same feed operation remains pending")
	return reply
}
func nativeFeedChildEnv(s nativeFeedSpec, a nativeFeedAdmission, reopen bool) ([]string, error) {
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
	env := []string{"RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_FEED_PROCESS=1", "RHO_GRAPH_NATIVE_WORKLOAD=", "RHO_GRAPH_DURABLE_COLUMN_PROCESS=", "RHO_GRAPH_EDGE_UNIQUE_PROCESS=", "RHO_GRAPH_COST_SPEC=" + string(cost), "RHO_GRAPH_WORKLOAD_SPEC=" + string(spec), "RHO_GRAPH_WORKLOAD_RUN=" + a.Run, "RHO_GRAPH_WORKLOAD_SECONDS=" + strconv.FormatUint(a.BudgetSeconds, 10), "RHO_GRAPH_WORKLOAD_DEADLINE=" + strconv.FormatInt(a.DeadlineUnixNanos, 10), "RHO_GRAPH_WORKLOAD_TRANSPORT=" + string(transport)}
	if s.Version == 2 {
		env[2] = "RHO_GRAPH_FEED_PROCESS=2"
	}
	if reopen {
		env = append(env, "RHO_GRAPH_PROCESS_REOPEN=1")
	}
	return env, nil
}
func nativeFeedAttach(t *testing.T, n *certifiedProcessNetwork, s nativeFeedSpec, a nativeFeedAdmission) {
	t.Helper()
	frames, err := nativeFeedFrames(s.cost())
	if s.Version == 2 {
		frames, err = nativeGuardedScalarFrames(s.cost())
	}
	if err != nil || frames > (1<<41)/1024 || frames > uint64(^uint(0)>>1)/1024 {
		t.Fatal("checked native feed packet grant", err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, a.DeadlineUnixNanos))
	t.Cleanup(cancel)
	// At most1024 actual frames per admitted native entry includes peer native
	// read/submit/control legs, bounded scalar hydration, cache/source traversals
	// and diagnostic pages. Queues remain128frames/8MiB; this allocates no tail.
	transport, err := newNativeWorkloadTransport(ctx, a.Transport, frames*1024)
	if err != nil {
		t.Fatal(err)
	}
	attachGraphProcessMuxesWithWorkload(t, n.all, int(frames*1024), transport) // #nosec G115 -- checked numeric limit above.
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
}
func nativeFeedBootstrapAction(t *testing.T, n *certifiedProcessNetwork, actor, action, index uint64, data []byte, stream *json.Encoder) {
	nativeScalarWorkloadBootstrapAction(t, n, actor, action, index, data, stream, retainedFeedSemanticContractID())
}

func nativeScalarWorkloadBootstrapAction(t *testing.T, n *certifiedProcessNetwork, actor, action, index uint64, data []byte, stream *json.Encoder, semantic raftlog.ApplicationSemanticContractID) {
	t.Helper()
	response := n.all[actor].call(t, processRequest{op: processCertifiedAction, entity: action, index: index, data: data})
	if action == certifiedOpenRecipient {
		if len(response.aux) != 0 {
			t.Fatal("recipient returned proposal")
		}
		return
	}
	proposal, err := decodeCertifiedProcessProposal(response.aux)
	if err != nil || proposal.target.semantic != semantic {
		t.Fatal("feed bootstrap original scope", err)
	}
	group := 0
	if proposal.target.partition == 8 {
		group = 1
	} else if proposal.target.partition != 3 {
		t.Fatal("foreign bootstrap")
	}
	leader := n.leaders[group]
	n.groups[group].event(t, leader, processRequest{op: processPropose, data: response.aux})
	state := n.all[leader].call(t, processRequest{op: processStateOp})
	actual := n.all[leader].call(t, processRequest{op: processEntryRecord, index: state.applied})
	ns, _ := certifiedProcessNamespace(leader)
	original, err := decodeDeclaredOutcome(actual.outcome, ns)
	if err != nil || original.index != state.applied || original.hash != sha256.Sum256(proposal.wire) || original.reason != reasonNone || original.disposition != applied && (action != certifiedInstallGrant || original.disposition != controlRecovery) {
		t.Fatal("native bootstrap refused", err)
	}
	for _, voter := range n.groups[group].nodes {
		replica := voter.call(t, processRequest{op: processEntryRecord, index: state.applied})
		if !bytes.Equal(replica.outcome, actual.outcome) || !bytes.Equal(replica.changes, actual.changes) {
			t.Fatal("bootstrap voter mismatch")
		}
	}
	if stream != nil {
		if err := stream.Encode(struct {
			Actor, Action, Index         uint64
			Proposal, Outcome, CDC, Root []byte
		}{actor, action, state.applied, response.aux, actual.outcome, actual.changes, state.image}); err != nil {
			t.Fatal(err)
		}
	}
}
func nativeFeedNetwork(t *testing.T, s nativeFeedSpec, a nativeFeedAdmission, stream *json.Encoder) *certifiedProcessNetwork {
	t.Helper()
	env, err := nativeFeedChildEnv(s, a, false)
	if err != nil {
		t.Fatal(err)
	}
	return nativeScalarWorkloadNetwork(t, s.cost(), env, "rho-native-feed-", retainedFeedSemanticContractID(), stream, func(n *certifiedProcessNetwork) { nativeFeedAttach(t, n, s, a) })
}

// Profile wrappers supply immutable child environment, exact semantic identity
// and actual owned scheduler. This shares genuine allocator/native bootstrap
// operations without selecting another family's profile or source authority.
func nativeScalarWorkloadNetwork(t *testing.T, spec nativeGraphCostSpec, env []string, prefix string, semantic raftlog.ApplicationSemanticContractID, stream *json.Encoder, attach func(*certifiedProcessNetwork)) *certifiedProcessNetwork {
	t.Helper()
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), prefix)
	if err != nil {
		t.Fatal(err)
	}
	n := &certifiedProcessNetwork{all: make(map[uint64]*graphProcess), dir: dir, leaders: [2]uint64{1, 4}}
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id, env...)
	}
	attach(n)
	t.Cleanup(func() {
		for _, p := range n.all {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := p.shutdown(ctx, t.Failed())
			cancel()
			if err != nil && !t.Failed() {
				t.Error(err)
			}
		}
	})
	for group, leader := range n.leaders {
		n.groups[group].event(t, leader, processRequest{op: processCampaign})
	}
	action := func(actor, kind, index uint64, data []byte) {
		nativeScalarWorkloadBootstrapAction(t, n, actor, kind, index, data, stream, semantic)
	}
	action(1, certifiedInitialize, 3, nil)
	n.read(t, 1, observeConfiguration, 0)
	action(1, certifiedInitialize, 8, nil)
	block, err := nativeGraphCostBlockSize(spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct{ actor, recipient, partition uint64 }{{1, 1, 3}, {4, 2, 8}} {
		n.read(t, r.actor, observeConfiguration, 0)
		action(r.actor, certifiedBeginRecipient, r.recipient, nil)
		n.read(t, 1, observeRecipient, r.recipient)
		action(1, certifiedRecipientTransition, 3, []byte{byte(fenceDeclaredRecipient)})
		if r.partition != 3 {
			action(1, certifiedRecipientTransition, r.partition, []byte{byte(fenceDeclaredRecipient)})
		}
		n.read(t, r.actor, observeRecipient, r.recipient)
		action(r.actor, certifiedRecipientTransition, 3, []byte{byte(acknowledgeDeclaredHome)})
		n.read(t, 1, observeRecipient, r.recipient)
		action(1, certifiedRecipientTransition, 3, []byte{byte(activateDeclaredRecipient)})
		n.read(t, 1, observeRecipient, r.recipient)
		action(1, certifiedRecipientTransition, r.partition, []byte{byte(publishDeclaredRecipient)})
		n.read(t, r.actor, observeRecipient, r.recipient)
		action(r.actor, certifiedOpenRecipient, 0, nil)
		grants := uint64(1)
		if r.partition == 3 {
			grants = (spec.homeCount() + block - 1) / block
		}
		for sequence := uint64(1); sequence <= grants; sequence++ {
			_, count, err := nativeGraphCostGrantPlan(spec, block, r.partition, sequence)
			if err != nil {
				t.Fatal(err)
			}
			n.read(t, 1, observeRecipient, r.recipient)
			n.read(t, 1, observeAllocator, 0)
			input := graphE17GrantInput(sequence, count)
			action(1, certifiedReserve, 0, input)
			n.group(1).event(t, 1, processRequest{op: processCertifiedRead, entity: uint64(observeGrant), data: input})
			if !n.all[1].call(t, processRequest{op: processStateOp}).ready {
				t.Fatal("actual native grant observation incomplete")
			}
			action(1, certifiedInstallGrant, r.partition, nil)
		}
	}
	return n
}
func nativeFeedCut(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, mode graphReadMode, at graphCut) graphCut {
	return nativeScalarWorkloadCut(t, ctx, n, mode, at, retainedFeedSemanticContractID())
}

func nativeScalarWorkloadCut(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, mode graphReadMode, at graphCut, semantic raftlog.ApplicationSemanticContractID) graphCut {
	t.Helper()
	body, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: at})
	if err != nil {
		t.Fatal(err)
	}
	r, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCertifiedOpen, data: body})
	if err != nil {
		t.Fatal(err)
	}
	handle, cut, err := decodeGraphE15ReadToken(r.aux)
	if err != nil || handle == 0 || len(cut.certificates) != 2 {
		t.Fatal("native complete cut", err)
	}
	for _, cert := range cut.certificates {
		if cert.scope.semantic != semantic {
			t.Fatal("feed cut agreement")
		}
	}
	_, err = nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCertifiedClose, index: handle})
	if err != nil {
		t.Fatal(err)
	}
	return cut
}

type nativeFeedBusiness struct {
	Sample                              nativeGraphCostSample
	StartNanos, StopNanos, ElapsedNanos int64
}
type nativeFeedDriver struct {
	ctx        context.Context
	n          *certifiedProcessNetwork
	spec       nativeFeedSpec
	admissions *nativeGraphCostAdmissions
	mu         sync.Mutex
	extra      int
	originals  uint64
	started    time.Time
}

func (d *nativeFeedDriver) retry() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.extra >= nativeFeedExtraInvocations {
		return errLimit
	}
	d.extra++
	return nil
}
func (d *nativeFeedDriver) beginOriginal() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.originals >= nativeFeedOriginals(d.spec.cost()) {
		return errLimit
	}
	d.originals++
	return nil
}
func (d *nativeFeedDriver) phase(t *testing.T, phase uint64, from uint64, attempts, completed *json.Encoder, census *[32]uint64) {
	nativeScalarWorkloadPhase(t, d.ctx, d.n, d.spec.cost(), d.admissions, d.started, phase, from, attempts, completed, census)
}

// Both wrappers have already admitted their exact fresh profile. This is only
// the common scalar dispatcher; actual factories/proposals/native originals
// retain their profile-specific validation and semantic scope.
func nativeScalarWorkloadPhase(t *testing.T, parent context.Context, n *certifiedProcessNetwork, spec nativeGraphCostSpec, admissions *nativeGraphCostAdmissions, started time.Time, phase, from uint64, attempts, completed *json.Encoder, census *[32]uint64) (last nativeGraphCostSample) {
	return nativeScalarWorkloadPhaseRange(t, parent, n, spec, admissions, started, phase, from, spec.Facts, attempts, completed, census)
}

func nativeScalarWorkloadPhaseRange(t *testing.T, parent context.Context, n *certifiedProcessNetwork, spec nativeGraphCostSpec, admissions *nativeGraphCostAdmissions, started time.Time, phase, from, end uint64, attempts, completed *json.Encoder, census *[32]uint64) (last nativeGraphCostSample) {
	return nativeScalarWorkloadPhaseWithSubmit(t, parent, spec, started, phase, from, end, attempts, completed, census, func(ctx context.Context, fact uint64, client int, events chan<- nativeGraphCostClientEvent) (nativeGraphCostSample, error) {
		return nativeGraphCostClientSubmit(ctx, n, spec, phase, fact, phase >= 4 && spec.cross(fact), client, admissions, events)
	})
}

// A fresh admitted family may select a genuine physical business owner after
// movement. Queueing, full attempt joins, caller ordinals and whole-operation
// clocks remain the same; old wrappers pass their literal existing submitter.
func nativeScalarWorkloadPhaseWithSubmit(t *testing.T, parent context.Context, spec nativeGraphCostSpec, started time.Time, phase, from, end uint64, attempts, completed *json.Encoder, census *[32]uint64, submit func(context.Context, uint64, int, chan<- nativeGraphCostClientEvent) (nativeGraphCostSample, error)) (last nativeGraphCostSample) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	type job struct {
		fact   uint64
		queued time.Time
	}
	type result struct {
		value nativeFeedBusiness
		err   error
	}
	queues := make([]chan job, spec.Writers)
	results := make(chan result, 32)
	events := make(chan nativeGraphCostClientEvent, 32)
	var workers sync.WaitGroup
	for client := range queues {
		queues[client] = make(chan job, 1)
		workers.Go(func() {
			for j := range queues[client] {
				sample, err := submit(ctx, j.fact, client, events)
				stop := time.Now()
				results <- result{nativeFeedBusiness{sample, j.queued.Sub(started).Nanoseconds(), stop.Sub(started).Nanoseconds(), stop.Sub(j.queued).Nanoseconds()}, err}
			}
		})
	}
	var feederDone <-chan struct{}
	defer func() {
		cancel()
		if feederDone != nil {
			<-feederDone
		}
		for _, q := range queues {
			close(q)
		}
		done := make(chan struct{})
		go func() { workers.Wait(); close(done) }()
		for {
			select {
			case event := <-events:
				if event.attempt != nil {
					if err := attempts.Encode(event.attempt); err != nil {
						t.Error(err)
					}
				}
			case <-results:
			case <-done:
				for {
					select {
					case event := <-events:
						if event.attempt != nil {
							if err := attempts.Encode(event.attempt); err != nil {
								t.Error(err)
							}
						}
					default:
						return
					}
				}
			}
		}
	}()
	if phase <= 3 {
		end = from + 1
	}
	for first := from; first < end; {
		count := min(uint64(32), end-first)
		if first == 0 || phase <= 3 {
			count = 1
		} // Actual ValueID5/6 and anchors precede dependent factories.
		joins := make(map[nativeGraphCostOperation]nativeGraphCostAttemptJoin, count)
		submitted := make(chan struct{})
		feederDone = submitted
		go func() {
			defer close(submitted)
			for i := uint64(0); i < count; i++ {
				fact := first + i
				ordinal := nativeGraphCostOperationID(spec, phase, fact).Ordinal
				client := int((ordinal - 1) % uint64(spec.Writers))
				select {
				case queues[client] <- job{fact, time.Now()}:
				case <-ctx.Done():
					return
				}
			}
		}()
		remaining := count
		for remaining > 0 {
			select {
			case event := <-events:
				if event.attempt == nil {
					t.Fatal("unexpected native attempt event")
				}
				a := *event.attempt
				join, err := nativeGraphCostJoinAttemptBound(joins[a.Operation], a, nativeFeedMaxOriginals)
				if err != nil {
					t.Fatal("complete immutable original lineage", err)
				}
				joins[a.Operation] = join
				if err := attempts.Encode(a); err != nil {
					t.Fatal(err)
				}
			case done := <-results:
				if done.err != nil {
					t.Fatal("native useful original refused", done.err)
				}
				a := done.value.Sample
				// The attempt event was emitted before result; select may choose result
				// first, so drain until the actual matching original has been observed.
				for !joins[a.Operation].matches(a) {
					var event nativeGraphCostClientEvent
					select {
					case event = <-events:
					case <-ctx.Done():
						t.Fatal("matching attempt unavailable", ctx.Err())
					}
					if event.attempt == nil {
						t.Fatal("missing native attempt")
					}
					next := *event.attempt
					join, err := nativeGraphCostJoinAttemptBound(joins[next.Operation], next, nativeFeedMaxOriginals)
					if err != nil {
						t.Fatal(err)
					}
					joins[next.Operation] = join
					if err := attempts.Encode(next); err != nil {
						t.Fatal(err)
					}
				}
				if a.ClientID != int((a.Operation.Ordinal-1)%uint64(spec.Writers)) || a.Fact < first || a.Fact >= first+count {
					t.Fatal("logical caller assignment")
				}
				census[a.ClientID]++
				last = a
				if err := completed.Encode(done.value); err != nil {
					t.Fatal(err)
				}
				remaining--
			}
		}
		<-submitted
		feederDone = nil
		first += count
	}
	return last
}

type nativeFeedSourceTrace struct {
	Stage  string
	Member uint64
	Page   nativeFeedRecordPage
	Voters []nativeFeedVoterRecord
}
type nativeFeedVoterRecord struct {
	Member, Index uint64
	Outcome, CDC  []byte
}

func nativeFeedSources(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, initial, cut graphCut, stage string, stream *json.Encoder) (rows [2]uint64) {
	return nativeScalarWorkloadSources(t, ctx, n, initial, cut, stage, stream, func(after, through uint64, boundary []byte) ([]byte, error) {
		return nativeFeedInputWire(nativeFeedInput{Version: 1, Action: "records", From: after, Through: through, Boundary: boundary})
	})
}

// Framing is supplied only by an exact admitted profile wrapper; census,
// native records, physical tips and three-voter equality stay one algorithm.
func nativeScalarWorkloadSources(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, initial, cut graphCut, stage string, stream *json.Encoder, input func(uint64, uint64, []byte) ([]byte, error)) (rows [2]uint64) {
	t.Helper()
	boundary, err := nativeFeedCutWire(cut)
	if err != nil {
		t.Fatal(err)
	}
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal(err)
	}
	for group, leader := range n.leaders {
		state, err := nativeGraphCostExchange(ctx, n.all[leader], processRequest{op: processStateOp})
		if err != nil {
			t.Fatal(err)
		}
		after := initial.certificates[group].sourceIndex
		for {
			wire, err := input(after, state.applied, boundary)
			if err != nil {
				t.Fatal(err)
			}
			response, err := nativeGraphCostExchange(ctx, n.all[leader], processRequest{op: processCostObserve, data: wire})
			if err != nil {
				t.Fatal(err)
			}
			var page nativeFeedRecordPage
			if err := nativeFeedDecodeJSON(response.aux, &page); err != nil {
				t.Fatal(err)
			}
			if page.After != after || page.Through != state.applied || page.Next < after || page.Next > page.Through || page.Next-after != uint64(len(page.Frames)) || len(page.Frames) > 4 || !bytes.Equal(page.Root, state.image) || page.Complete != (page.Next == page.Through) {
				t.Fatal("complete physical source page")
			}
			voters := make([]nativeFeedVoterRecord, 0, 3*len(page.Frames))
			for offset, frame := range page.Frames {
				if frame.Partition != 3+5*uint64(group) || frame.Index != after+1+uint64(offset) || frame.Outcome == nil || frame.CDC == nil {
					t.Fatal("native physical frame gap")
				}
				for member := uint64(1 + 3*group); member <= uint64(3+3*group); member++ {
					voter := n.groups[group].nodes[member]
					actual, err := nativeGraphCostExchange(ctx, voter, processRequest{op: processEntryRecord, index: frame.Index})
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(actual.outcome, frame.Outcome) || !bytes.Equal(actual.changes, frame.CDC) {
						t.Fatal("native voter CDC/original mismatch", member, frame.Index)
					}
					voters = append(voters, nativeFeedVoterRecord{member, frame.Index, actual.outcome, actual.changes})
				}
				rows[group] += uint64(len(frame.Rows))
			}
			body, err := json.Marshal(nativeFeedSourceTrace{stage, leader, page, voters})
			if err != nil || len(body) > 512<<10 {
				t.Fatal("bounded actual voter evidence", err)
			}
			if err := stream.Encode(json.RawMessage(body)); err != nil {
				t.Fatal(err)
			}
			after = page.Next
			if page.Complete {
				break
			}
			if len(page.Frames) == 0 {
				t.Fatal("source page stalled before EOF")
			}
		}
	}
	return rows
}
func nativeFeedPoints(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeFeedSpec, at graphCut, stage string, stream *json.Encoder) {
	t.Helper()
	check := nativeFeedCheckPoint
	if s.Version == 2 && s.Family == nativeFeedFamily {
		check = nativeGuardedScalarCheckPoint
	}
	for first := uint64(0); first < s.Facts; first += 16 {
		wire, err := nativeFeedCutWire(at)
		if err != nil {
			t.Fatal(err)
		}
		response, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCertifiedOpen, data: wire})
		if err != nil {
			t.Fatal(err)
		}
		handle, actual, err := decodeGraphE15ReadToken(response.aux)
		if err != nil || !feedCutEqual(actual, at) {
			t.Fatal("actual exact-cut read", err)
		}
		for fact := first; fact < min(first+16, s.Facts); fact++ {
			entity, _, _, _ := s.cost().edge(fact)
			response, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processCostObserve, index: handle, entity: entity})
			if err != nil {
				t.Fatal(err)
			}
			var point nativeLifecycleEntity
			if err := nativeFeedDecodeJSON(response.aux, &point); err != nil {
				t.Fatal(err)
			}
			value := int64(6)
			if stage == "old-after-update" || stage == "reopened-old" {
				value = 5
			}
			if check(s, nativeFeedPoint{fact, point}, value) != nil || point.Property.Revision > at.certificates[0].sourceRound || point.Presence.Revision > at.certificates[0].sourceRound {
				t.Fatal("exact-cut point shape/round", fact)
			}
			if err := stream.Encode(struct {
				Stage string
				Cut   []byte
				Point nativeFeedPoint
			}{stage, wire, nativeFeedPoint{fact, point}}); err != nil {
				t.Fatal(err)
			}
		}
		_, err = nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCertifiedClose, index: handle})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Reap and replace the actual coordinator/client process while its native page
// is sealed but unacknowledged. The unchanged hub owns every other process and
// queue; the replacement gets no copied peer/read/pin capability from the parent.
func nativeFeedReplaceClient(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeFeedSpec, a nativeFeedAdmission, stream *json.Encoder) {
	nativeScalarWorkloadReplaceClient(t, ctx, n, stream, func() ([]string, error) { return nativeFeedChildEnv(s, a, true) })
}

func nativeScalarWorkloadReplaceClient(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, stream *json.Encoder, childEnv func() ([]string, error)) {
	t.Helper()
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal(err)
	}
	prior := n.all[2]
	before, err := nativeGraphCostExchange(ctx, prior, processRequest{op: processStateOp})
	if err != nil {
		t.Fatal(err)
	}
	cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	waitErr := prior.shutdown(cleanup, true)
	cancel()
	if prior.cmd.ProcessState == nil {
		t.Fatal("client was not reaped")
	}
	status, ok := prior.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("actual feed client SIGKILL/reap", waitErr)
	}
	hub := prior.mux.hub
	env, err := childEnv()
	if err != nil {
		t.Fatal(err)
	}
	replacement := startGraphProcess(t, filepath.Join(n.dir, "2"), 2, env...)
	mux := &graphProcessMux{p: replacement, id: 2, hub: hub, pending: make(map[uint64]chan graphProcessAnswer), relays: make(map[uint64]context.CancelFunc), relayQueue: make(chan graphProcessRelayJob, processSourceWorkers)}
	replacement.mux = mux
	hub.mu.Lock()
	hub.peers[2] = mux
	hub.mu.Unlock()
	n.all[2], n.groups[0].nodes[2] = replacement, replacement
	hub.wg.Go(mux.readLoop)
	for range processSourceWorkers {
		hub.wg.Go(mux.relayLoop)
	}
	restored, err := nativeGraphCostExchange(ctx, replacement, processRequest{op: processStateOp})
	if err != nil {
		t.Fatal(err)
	}
	if before.incarnation == restored.incarnation || before.applied != restored.applied || !bytes.Equal(before.image, restored.image) || restored.stats[statViews] != 0 || restored.stats[statPinnedBytes] != 0 {
		t.Fatal("same-directory native feed restore")
	}
	b, err := encodeProcessResponse(before)
	if err != nil {
		t.Fatal(err)
	}
	r, err := encodeProcessResponse(restored)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Encode(struct {
		Stage               string
		Member              uint64
		PID, ReplacementPID int
		Signal              string
		Reaped              bool
		Before, Restored    []byte
	}{"pending-page-client-replacement", 2, prior.cmd.Process.Pid, replacement.cmd.Process.Pid, "SIGKILL", true, b, r}); err != nil {
		t.Fatal(err)
	}
	n.read(t, 1, observeConfiguration, 0)
}
func nativeFeedRestart(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeFeedSpec, a nativeFeedAdmission, stream *json.Encoder) nativeWorkloadTransportReceipt {
	return nativeScalarWorkloadRestart(t, ctx, n, stream, func() ([]string, error) { return nativeFeedChildEnv(s, a, true) }, func() { nativeFeedAttach(t, n, s, a) })
}

func nativeScalarWorkloadRestart(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, stream *json.Encoder, childEnv func() ([]string, error), attach func()) nativeWorkloadTransportReceipt {
	t.Helper()
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal(err)
	}
	var before [6]processResponse
	for id := uint64(1); id <= 6; id++ {
		var err error
		before[id-1], err = nativeGraphCostExchange(ctx, n.all[id], processRequest{op: processStateOp})
		if err != nil {
			t.Fatal(err)
		}
	}
	old := n.all[1].mux.hub
	if nativeGraphCostStop(t, n, 1, stream) != 6 {
		t.Fatal("six native process owners not reaped")
	}
	receipt := old.workload.receipt(old)
	env, err := childEnv()
	if err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(n.dir, fmt.Sprint(id)), id, env...)
	}
	attach()
	for id := uint64(1); id <= 6; id++ {
		restored, err := nativeGraphCostExchange(ctx, n.all[id], processRequest{op: processStateOp})
		if err != nil {
			t.Fatal(err)
		}
		previous := before[id-1]
		if restored.incarnation == previous.incarnation || restored.applied != previous.applied || !bytes.Equal(restored.image, previous.image) || restored.stats[statViews] != 0 || restored.stats[statPinnedBytes] != 0 || restored.stats[statGeneration] != previous.stats[statGeneration] || restored.stats[statCommit] != previous.stats[statCommit] {
			t.Fatal("actual persisted native feed prefix changed", id)
		}
		b, err := encodeProcessResponse(previous)
		if err != nil {
			t.Fatal(err)
		}
		r, err := encodeProcessResponse(restored)
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Encode(struct {
			Stage            string
			Member           uint64
			Before, Restored []byte
		}{"full-cohort-restore-before-election", id, b, r}); err != nil {
			t.Fatal(err)
		}
	}
	for group, leader := range n.leaders {
		n.groups[group].event(t, leader, processRequest{op: processCampaign})
		n.read(t, leader, observeConfiguration, 0)
	}
	return receipt
}

func nativeFeedReadColumns(t *testing.T, clients [2]*nativeGraphEdgeUniqueColumnClient, pair *nativeGraphCostColumnPair, s nativeGraphCostSpec, at graphCut, cached, fill bool) {
	nativeScalarReadColumns(t, clients, pair, s, at, cached, fill, nil, false)
}

func nativeScalarReadColumns(t *testing.T, clients [2]*nativeGraphEdgeUniqueColumnClient, pair *nativeGraphCostColumnPair, s nativeGraphCostSpec, at graphCut, cached, fill bool, observer *nativeImmutableColumnsObservations, points bool) {
	t.Helper()
	began := time.Now()
	var probes *nativeImmutableColumnsProbes
	if points {
		if observer == nil {
			t.Fatal("fresh column probe admission required")
		}
		var err error
		probes, err = newNativeImmutableColumnsProbes(s)
		if err != nil {
			t.Fatal(err)
		}
	}
	var census [3]uint64
	for group, c := range clients {
		r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsRead, At: nativeGraphCostColumnCutInput(t, at), Cached: cached, Fill: fill})
		if !nativeColumnSame(r.Manifest, pair.Manifests[group]) || r.Binding.Version != 2 || r.Binding.Policy.Version != 1 || r.Binding.Policy.Admission == ([32]byte{}) {
			t.Fatal("actual durable native feed column binding")
		}
		initial, err := decodeGraphCertificate(r.Manifest.Generation.Sources[group].Initial)
		if err != nil {
			t.Fatal(err)
		}
		position, ordinal := initial.sourceIndex, uint64(0)
		var chain [32]byte
		var frames, rows, retained uint64
		for {
			r = c.call(nativeGraphCostColumnRequest{Action: nativeColumnsNext})
			if !nativeColumnSame(r.Manifest, pair.Manifests[group]) || r.Ordinal != ordinal || r.Cached != cached || r.Fill != fill {
				t.Fatal("column continuation changed generation")
			}
			if r.EOF {
				m := pair.Manifests[group]
				if len(r.Body)+len(r.Rows) != 0 || ordinal != m.Chunks || frames != m.Frames || rows != m.Rows || retained != m.Bytes || position != m.Positions[group] || chain != m.Chain {
					t.Fatal("actual native column/cache EOF")
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
					t.Fatal("native column omitted retained physical frame")
				}
				position++
				frames++
				rows += uint64(len(frame.Rows))
				if frame.Index <= at.certificates[group].sourceIndex {
					for _, row := range frame.Rows {
						class, err := nativeGraphCostColumnRow(s, row)
						if err != nil {
							t.Fatal("native feed actual typed lifecycle", err)
						}
						census[class]++
						if probes != nil {
							if err := probes.accept(row); err != nil {
								t.Fatal(err)
							}
						}
						selected = append(selected, row)
					}
				}
			}
			if !reflect.DeepEqual(selected, r.Rows) {
				t.Fatal("typed native/cache output mismatch")
			}
			retained += uint64(len(nativeColumnChunkKey(pair.ID, ordinal)) + len(r.Body))
			chain = nativeColumnChain(chain, r.Body)
			ordinal++
		}
	}
	updates := uint64(0)
	if pair.Updated && feedCutEqual(at, pair.Final) {
		updates = s.Facts
		if s.Version == 11 {
			updates /= 2
		}
	}
	presence := 3 + s.Facts
	if s.Version == 9 || s.Version == 10 || s.Version == 11 || s.Version == 12 || s.Version == 13 || s.Version == 14 {
		presence += updates
	}
	if census != ([3]uint64{presence, s.Facts, updates}) {
		t.Fatal("complete native typed scalar row census", census)
	}
	if observer != nil {
		observer.read(t, clients[0].stage, pair, at, cached, fill, began, census, probes)
	}
}
func nativeFeedPublishColumns(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeGraphCostSpec, initial, final graphCut, id [16]byte, stage string, updated bool, failed *uint64, stream *json.Encoder) nativeGraphCostColumnPair {
	return nativeScalarPublishColumns(t, ctx, n, s, initial, final, id, stage, updated, failed, stream, nil)
}

func nativeScalarPublishColumns(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeGraphCostSpec, initial, final graphCut, id [16]byte, stage string, updated bool, failed *uint64, stream *json.Encoder, observer *nativeImmutableColumnsObservations) nativeGraphCostColumnPair {
	t.Helper()
	began := time.Now()
	pair := nativeGraphCostColumnPair{Initial: initial, Final: final, ID: id, Updated: updated}
	clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pair, stage, true, failed, stream)
	for group, c := range clients {
		for page := uint64(0); ; page++ {
			if page > c.binding.Policy.MaxChunks {
				t.Fatal("finite native column publication ceiling")
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
	if observer != nil {
		observer.published(t, stage, &pair, began)
	}
	for _, mode := range []struct {
		name         string
		cached, fill bool
	}{{"native", false, false}, {"fill", false, true}, {"cached", true, false}} {
		for _, c := range clients {
			c.stage = stage + "/" + mode.name
		}
		nativeScalarReadColumns(t, clients, &pair, s, final, mode.cached, mode.fill, observer, false)
	}
	nativeGraphEdgeUniqueColumnCloseClients(clients)
	return pair
}
