package graphapply

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
)

func nativeTransferProcessProfile(s *nativeGraphCostSpec) (bool, error) {
	flag := os.Getenv(nativeTransferProcessFlag)
	if flag == "" {
		if s != nil && s.Version == 12 {
			return false, errInvalid
		}
		return false, nil
	}
	if flag != "1" || s == nil || nativeTransferValidate(*s) != nil || os.Getenv("RHO_GRAPH_CERTIFIED_PROCESS") != "1" || os.Getenv("RHO_GRAPH_SOURCE_PROCESS") != "1" {
		return false, errInvalid
	}
	for _, name := range []string{"RHO_GRAPH_CREATE_UPDATE_PROCESS", "RHO_GRAPH_IMMUTABLE_COLUMNS_PROCESS", "RHO_GRAPH_LATE_CORRECTION_PROCESS", "RHO_GRAPH_SELECTORS_PROCESS", "RHO_GRAPH_FEED_PROCESS", "RHO_GRAPH_FEED_RETAINED_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_FAULT_FAMILY", "RHO_GRAPH_PROTOCOL_STATE_PROCESS", "RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "RHO_GRAPH_STATE_RETENTION_PROCESS"} {
		if value := os.Getenv(name); value != "" && value != "0" && value != "off" {
			return false, errInvalid
		}
	}
	spec, err := nativeTransferSpecFromEnv()
	if err != nil || spec.cost() != *s {
		return false, errors.Join(errInvalid, err)
	}
	_, _, err = nativeTransferAdmissionFromEnv(spec)
	return err == nil, err
}

type nativeTransferCursor struct {
	cursor                *idalloc.Cursor
	grant                 grantReference
	first, last, consumed uint64
	nextSequence          uint64
	lastCount             int
	lastReceipt           []byte // bounded <=32 actual Next IDs, not caller authority
}
type nativeTransferAllocationReceipt struct {
	Request                       idalloc.GrantRequest
	Cursor, First, Last, Consumed uint64
	IDs                           []uint64 `json:",omitzero"`
}

// Every cursor is returned by the actual HOME Recipient.Acquire. The native
// child pump performs the matching ReadIndex after the source-produced grant
// has been installed. No callback accepts a caller reply or observation DTO.
// Two initial HOME blocks plus one successor block fit this fixed local array;
// the remote recipient owns its independent one-block array in its own child.
// Only consumed-prefix scalars survive calls, never a dataset-sized issued map.
type nativeTransferAllocator struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	owners  *certifiedProcessOwners
	read    func(context.Context, allocationQuery) (allocationReply, error)
	cursors [3]nativeTransferCursor
	count   int
	closed  bool
}

func (s *nativeTransferAllocator) acquire(sequence, count uint64) ([]byte, error) {
	if s == nil || s.ctx == nil || s.read == nil || s.owners == nil || s.owners.recipient == nil || sequence == 0 || count == 0 || count > idalloc.MaxBlockSize {
		return nil, errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errLimit
	}
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	for slot, c := range s.cursors[:s.count] {
		if c.grant.session == s.owners.recipient.session && c.grant.sequence == sequence {
			if count != c.last-c.first+1 {
				return nil, errInvalid
			}
			request := idalloc.GrantRequest{Graph: s.owners.host.machine.ns.graph, Session: c.grant.session, Sequence: sequence, Count: count}
			return json.Marshal(nativeTransferAllocationReceipt{Request: request, Cursor: uint64(slot + 1), First: c.first, Last: c.last}) // #nosec G115 -- three owned slots.
		}
	}
	if s.count == len(s.cursors) {
		return nil, errLimit
	}
	var installed idalloc.Grant
	cursor, err := s.owners.recipient.Acquire(s.ctx, sequence, count, func(ctx context.Context, request idalloc.GrantRequest, question allocationQuery) (allocationReply, error) {
		actual, err := s.read(ctx, question)
		if err == nil && actual.err == nil {
			installed = actual.proof.observation.grant.grant
		}
		return actual, err
	})
	if err != nil {
		return nil, err
	}
	// Acquire itself checks own completed request-bound proof, current session
	// and genuine reservation. This check binds the descriptive receipt to it.
	if cursor == nil || installed.Request.Session != s.owners.recipient.session || installed.Request.Sequence != sequence || installed.Request.Count != count {
		return nil, errCorrupt
	}
	s.cursors[s.count] = nativeTransferCursor{cursor: cursor, grant: grantReference{session: installed.Request.Session, sequence: sequence}, first: installed.Reservation.First, last: installed.Reservation.Last}
	s.count++
	return json.Marshal(nativeTransferAllocationReceipt{Request: installed.Request, Cursor: uint64(s.count), First: installed.Reservation.First, Last: installed.Reservation.Last}) // #nosec G115 -- fixed three-slot owner.
}

func (s *nativeTransferAllocator) next(alias, sequence uint64, count int) ([]byte, error) {
	if s == nil || alias == 0 || alias > 3 || sequence == 0 || count < 1 || count > 32 {
		return nil, errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || alias > uint64(s.count) {
		return nil, errInvalid
	} // #nosec G115 -- fixed three slots.
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	c := &s.cursors[alias-1]
	if sequence == c.nextSequence {
		if count != c.lastCount || len(c.lastReceipt) == 0 {
			return nil, errInvalid
		}
		return append([]byte(nil), c.lastReceipt...), nil
	}
	if sequence != c.nextSequence+1 {
		return nil, errInvalid
	}
	if uint64(count) > c.last-c.first+1-c.consumed {
		return nil, idalloc.ErrExhausted
	} // #nosec G115 -- positive <=32.
	receipt := nativeTransferAllocationReceipt{Cursor: alias, First: c.first, Last: c.last, IDs: make([]uint64, 0, count)}
	for range count {
		id, err := c.cursor.Next()
		if err != nil {
			return nil, err
		}
		if id != c.first+c.consumed {
			return nil, errCorrupt
		}
		c.consumed++
		receipt.IDs = append(receipt.IDs, id)
	}
	receipt.Consumed = c.consumed
	wire, err := json.Marshal(receipt)
	if err != nil {
		return nil, err
	}
	c.nextSequence, c.lastCount, c.lastReceipt = sequence, count, append([]byte(nil), wire...)
	return wire, nil
}
func (s *nativeTransferAllocator) issued(id uint64) (grantReference, error) {
	if s == nil || id == 0 {
		return grantReference{}, errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return grantReference{}, errInvalid
	}
	for _, c := range s.cursors[:s.count] {
		if id >= c.first && id-c.first < c.consumed {
			return c.grant, nil
		}
	}
	return grantReference{}, errInvalid
}
func (s *nativeTransferAllocator) close() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}

// Full source-authored workload parent. Capacity refusal remains a failed cell;
// this path never converts ABORT/limit or a copied recipe into successful moves.
func TestNativeGraphTransferWorkloadCell(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	if testing.Short() || os.Getenv("RHO_GRAPH_NATIVE_WORKLOAD") != nativeTransferFormat {
		t.Skip("explicit pooled native transfer workload required")
	}
	spec, err := nativeTransferSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	a, digest, err := nativeTransferAdmissionFromEnv(spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, a.DeadlineUnixNanos))
	defer cancel()
	started := time.Now()
	output := os.Getenv("RHO_GRAPH_WORKLOAD_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("absolute fresh owned artifact path required")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RHO_GRAPH_PROCESS_ARTIFACT", output)
	open := func(name string) *json.Encoder {
		f, e := os.OpenFile(filepath.Join(output, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() {
			if e := f.Close(); e != nil {
				t.Error(e)
			}
		})
		return json.NewEncoder(f)
	}
	grants, attempts, operations := open("grants.jsonl"), open("attempts.jsonl"), open("operations.jsonl")
	controls, moves, sources := open("controls.jsonl"), open("moves.jsonl"), open("sources.jsonl")
	processes, columns, gc := open("processes.jsonl"), open("columns.jsonl"), open("gc.jsonl")
	env, err := nativeTransferChildEnv(spec, a, false)
	if err != nil {
		t.Fatal(err)
	}
	n := nativeScalarWorkloadNetwork(t, spec.cost(), env, "rho-native-transfer-", rangeMovePooledSemanticContractID(), grants, func(n *certifiedProcessNetwork) { nativeTransferAttach(t, n, a) })
	result := nativeTransferResult{Version: 1, Format: nativeTransferFormat, Engine: "native-raft", Spec: spec, Admission: a, AdmissionSHA256: digest, Directory: n.dir, ArtifactDirectory: output, ProcessInstances: 6, RetentionContract: "held-C-through-BOTH-HOME-target-restart-and-eligible-old-owner-GC-then-rolling-ACK-feed-only-v1"}
	result.FramesCeiling, err = nativeTransferFrames(spec.cost(), a.Grants)
	if err != nil {
		t.Fatal(err)
	}
	extra := 0
	client := &nativeTransferClient{t: t, ctx: ctx, n: n, member: 1, stage: "allocation-home", stream: controls, extra: &extra}
	block, err := nativeGraphCostBlockSize(spec.cost())
	if err != nil {
		t.Fatal(err)
	}
	homes := (spec.cost().homeCount() + block - 1) / block
	if homes < 1 || homes > 2 || spec.Facts == 1_000_000 && homes != 2 {
		t.Fatal("required actual HOME publication geometry")
	}
	for i := uint64(0); i < homes; i++ {
		first, count, e := nativeTransferGrantPlan(spec.cost(), block, 3, i+1)
		if e != nil {
			t.Fatal(e)
		}
		r := client.acquire(i+1, count)
		if r.First != first {
			t.Fatal("actual HOME reservation differs")
		}
		result.InitialHOME[i] = r
	}
	client.member, client.stage = 4, "allocation-remote"
	result.Remote = client.acquire(1, 2)
	if result.Remote.First != spec.cost().remoteNode() {
		t.Fatal("actual remote reservation differs")
	}
	initial := nativeScalarWorkloadCut(t, ctx, n, graphReadFresh, graphCut{}, rangeMovePooledSemanticContractID())
	wire := func(c graphCut) []byte {
		w, e := nativeFeedCutWire(c)
		if e != nil {
			t.Fatal(e)
		}
		return w
	}
	result.Initial = wire(initial)
	driver := &nativeTransferDriver{spec: spec}
	admissions := newNativeTransferAdmissions()
	admissions.originals, admissions.same = 64, 8
	admissions.extra, admissions.beginOriginal = driver.retry, driver.beginOriginal
	phase := func(number, from, through uint64, postMove bool) {
		if postMove {
			nativeScalarWorkloadPhaseWithSubmit(t, ctx, spec.cost(), started, number, from, through, attempts, operations, &result.Completed, func(call context.Context, fact uint64, actor int, events chan<- nativeGraphCostClientEvent) (nativeGraphCostSample, error) {
				return nativeTransferLocalUpdate(call, n, spec, fact, actor, admissions, events)
			})
		} else {
			nativeScalarWorkloadPhaseRange(t, ctx, n, spec.cost(), admissions, started, number, from, through, attempts, operations, &result.Completed)
		}
	}
	for setup := uint64(1); setup <= 3; setup++ {
		phase(setup, setup-1, setup, false)
	}
	result.SetupNanos = time.Since(started).Nanoseconds()
	business := time.Now()
	phase(4, 0, spec.Facts, false)
	result.BusinessPhases[0] = nativeCreateUpdatePhaseStop("create", business, started)
	client.member, client.stage = 2, "held-C-before-K"
	begin := client.feed(nativeFeedInput{Action: "begin", Request: nativeFeedRequest(digest, "transfer-snapshot", 0)})
	result.Original = begin.Original
	old, err := nativeTransferCutDecode(begin.Initial)
	if err != nil {
		t.Fatal(err)
	}
	result.Snapshot = begin.Initial
	result.Token = client.feed(nativeFeedInput{Action: "publish"}).Token
	nativeTransferSnapshot(client, spec)
	client.feed(nativeFeedInput{Action: "snapshot-close"})
	var pair [2]nativeGraphCostColumnPair
	if spec.Layout == "columns" {
		pair[0] = nativeFeedPublishColumns(t, ctx, n, spec.cost(), initial, old, nativeFeedRequest(digest, "old-columns", 0), "old-seal", false, &result.ColumnFailed[0], columns)
	}
	business = time.Now()
	k, kResult := nativeTransferLostK(t, ctx, n, spec, driver, admissions, started, attempts, operations, &result)
	result.BusinessPhases[1] = nativeCreateUpdatePhaseStop("K", business, started)
	kBoundary := nativeScalarWorkloadCut(t, ctx, n, graphReadFresh, graphCut{}, rangeMovePooledSemanticContractID())
	pageSequence := uint64(0)
	client.member, client.stage = 2, "K-complete-feed"
	nativeTransferDrain(client, kBoundary, digest, &pageSequence, &result)
	// Value6 now genuinely exists, so this actual Set→Correct plan can be
	// captured before Fence without inventing an undeclared dictionary value.
	if err := driver.beginOriginal(); err != nil {
		t.Fatal(err)
	}
	frozen := nativeTransferPrepare(t, ctx, n, 1, certifiedGraphCost, 5, binary.BigEndian.AppendUint64(nil, 1))
	client.stage = "frozen-before-fence"
	result.Frozen = client.must(nativeTransferInput{Action: "freeze", Original: frozen.wire}).Body
	stopMoveTrace := nativeTransferObserveMoveTraffic(t, n, a, moves)
	defer stopMoveTrace()
	transferStarted := time.Now()
	for i := uint64(0); i < homes; i++ {
		client.member, client.stage = 1, fmt.Sprintf("move-%d", i+1)
		prepared := client.must(nativeTransferInput{Action: "move-prepare", Request: nativeFeedRequest(digest, "HOME-move", i), Range: result.InitialHOME[i].First})
		intent, e := decodeRangeMoveIntent(prepared.Body)
		if e != nil || intent.source.semantic != rangeMovePooledSemanticContractID() || intent.source.partition != 3 || intent.target.partition != 8 || intent.publication.RangeID != result.InitialHOME[i].First || intent.pools != a.Grants.pools() {
			t.Fatal("actual HOME-qualified pooled intent", e)
		}
		done := client.move(prepared.Body, a)
		actual, e := decodeGraphE17MoveResult(processResponse{aux: done.Body})
		if e != nil || !actual.committed || actual.source.phase != rangeMoveFinish || actual.target.phase != rangeMoveFinish {
			t.Fatal("BOTH-HOME move did not actually succeed", e)
		}
		result.Moves[i], result.MoveCount = bytes.Clone(done.Body), result.MoveCount+1
	}
	result.TransferNanos = time.Since(transferStarted).Nanoseconds()
	stopMoveTrace()
	lookupK := func(stage string) {
		client.member, client.stage = 1, stage
		key := crossControlKey(kResult.decision.ns, kResult.decision.tx, 'D')
		response := client.must(nativeTransferInput{Action: "lookup", Original: result.Moves[homes-1], Origin: 3, Control: true, Key: key})
		var observed nativeTransferOriginalObservation
		exact, e := encodeCrossStatus(kResult.decision)
		if e != nil {
			t.Fatal(e)
		}
		if e := nativeFeedDecodeJSON(response.Body, &observed); e != nil || !observed.Found || observed.Origin != 3 || !bytes.Equal(observed.Key, key) || !bytes.Equal(observed.Value, exact) || observed.EvaluationIndex == 0 || observed.OriginalIndex != 0 && observed.OriginalIndex != kResult.decision.index {
			t.Fatal("actual moved K original lookup changed namespace/result", e)
		}
	}
	lookupK("K-original-after-BOTH-HOME")
	client.member, client.stage = 2, "held-C-after-moves"
	nativeTransferSnapshot(client, spec)
	client.feed(nativeFeedInput{Action: "snapshot-close"})
	client.feed(nativeFeedInput{Action: "close"})
	if spec.Layout == "columns" {
		c := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pair[0], "old-after-BOTH-moves", false, &result.ColumnFailed[0], columns)
		nativeFeedReadColumns(t, c, &pair[0], spec.cost(), old, false, false)
		nativeGraphEdgeUniqueColumnCloseClients(c)
	}
	nativeTransferReplaceTarget(t, ctx, n, a, processes)
	result.ProcessInstances++
	result.Reaped++
	// Actual routes and streams are refreshed while all transient sources are
	// closed, preserving existing originals and the same retained feed token.
	route := binary.BigEndian.AppendUint64(nil, 1)
	route = binary.BigEndian.AppendUint64(route, 4)
	for member := uint64(1); member <= 6; member++ {
		if _, e := nativeGraphCostExchange(ctx, n.all[member], processRequest{op: processPairedReconnect, data: route}); e != nil {
			t.Fatal("actual peer reconnect", member, e)
		}
	}
	client.member, client.stage = 2, "held-C-after-restart"
	client.feed(nativeFeedInput{Action: "open", Token: result.Token})
	nativeTransferSnapshot(client, spec)
	client.feed(nativeFeedInput{Action: "snapshot-close"})
	retry, e := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCoordinate, data: k.wire})
	if e != nil {
		t.Fatal(e)
	}
	againK, e := decodeGraphProcessCoordinatorResult(retry)
	if e != nil || againK.decision != kResult.decision {
		t.Fatal("K original changed across BOTH HOME+target restart", e)
	}
	changed, e := encodeCertifiedProcessProposal(k)
	if e != nil {
		t.Fatal(e)
	}
	r, e := nativeGraphCostExchange(ctx, n.all[1], processRequest{op: processP08ChangedGraph, data: changed})
	if e != nil {
		t.Fatal(e)
	}
	changedProposal, e := decodeCertifiedProcessProposal(r.aux)
	if e != nil {
		t.Fatal(e)
	}
	refused, cause := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCoordinate, data: changedProposal.wire})
	if !errors.Is(cause, errInvalid) {
		t.Fatal("changed same-request K became an effect/result", cause)
	}
	result.ChangedCaller = changedProposal.wire
	result.ChangedReply, e = encodeProcessResponse(refused)
	if e != nil {
		t.Fatal(e)
	}
	client.stage = "fenced-original"
	client.must(nativeTransferInput{Action: "register", Original: result.Frozen})
	aborted, e := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCoordinate, data: frozen.wire})
	if e != nil {
		t.Fatal(e)
	}
	abort, e := decodeGraphProcessCoordinatorResult(aborted)
	if e != nil || abort.decision.commit || abort.decision.phase != crossDecide {
		t.Fatal("real pre-Fence original not durably ABORTed", e)
	}
	result.Aborted, e = encodeProcessResponse(aborted)
	if e != nil {
		t.Fatal(e)
	}
	// Rotate actual allocator service authority, reject one queued old request,
	// and consume a real successor block under the unchanged logical HOME.
	nativeTransferRead(t, ctx, n, 1, observeRecipient, 1)
	nativeTransferRead(t, ctx, n, 1, observeAllocator, 0)
	input := graphE17GrantInput(homes+1, 2)
	queued := nativeTransferPrepare(t, ctx, n, 1, certifiedReserve, 0, input)
	nativeTransferRead(t, ctx, n, 1, observeAllocator, 0)
	nativeTransferInstallControl(t, ctx, n, 1, certifiedP08Transfer, nativeTransferPrepare(t, ctx, n, 1, certifiedP08Transfer, 0, nil), reasonNone, grants)
	nativeTransferInstallControl(t, ctx, n, 1, certifiedReserve, queued, reasonStale, grants)
	nativeTransferRead(t, ctx, n, 1, observeRecipient, 1)
	nativeTransferRead(t, ctx, n, 1, observeAllocator, 0)
	nativeTransferInstallControl(t, ctx, n, 1, certifiedReserve, nativeTransferPrepare(t, ctx, n, 1, certifiedReserve, 0, input), reasonNone, grants)
	nativeTransferNativeEvent(t, ctx, n, 1, processRequest{op: processCertifiedRead, entity: uint64(observeGrant), data: input})
	nativeTransferInstallControl(t, ctx, n, 1, certifiedInstallGrant, nativeTransferPrepare(t, ctx, n, 1, certifiedInstallGrant, 3, nil), reasonNone, grants)
	client.member, client.stage = 1, "allocation-successor"
	result.Successor = client.acquire(homes+1, 2)
	if result.Successor.First != spec.cost().remoteNode()+2 {
		t.Fatal("successor epoch reused a prior ID")
	}
	if spec.Layout == "columns" {
		nativeFeedRetainedRetireColumns(t, ctx, n, &pair[0], "retire-all-C-dependent-columns", &result.ColumnFailed[0], columns)
	}
	half := spec.Facts / 2
	business = time.Now()
	phase(5, 1, half, true)
	result.BusinessPhases[2] = nativeCreateUpdatePhaseStop("first-half-update", business, started)
	mid := nativeScalarWorkloadCut(t, ctx, n, graphReadFresh, graphCut{}, rangeMovePooledSemanticContractID())
	// K/GML boundaries are already complete; ACK just one new update, so an
	// actual existing business suffix remains unread through both GC phases.
	client.member, client.stage = 2, "first-update-before-GC"
	nativeTransferDeliver(client, mid, digest, &pageSequence, &result, true, false, nil)
	from := [2]uint64{initial.certificates[0].sourceIndex, initial.certificates[1].sourceIndex}
	result.SourceRows[0] = nativeTransferSources(t, ctx, n, from, mid, result.Original, "full-before-held-gc", sources, client)
	client.member, client.stage = 2, "actual-ACK-vector-before-held-GC"
	state := client.feed(nativeFeedInput{Action: "state"})
	cursor, e := decodeRetainedFeedCursor(state.Cursor)
	if e != nil || cursor.pending != ([32]byte{}) {
		t.Fatal("actual settled ACK vector", e)
	}
	var floors [2][6]uint64
	for group, cert := range old.certificates {
		floors[group] = [6]uint64{cert.sourceIndex, cert.sourceIndex, cursor.positions[group], cursor.positions[group], cert.sourceIndex, cert.index}
	}
	result.HeldGC = nativeTransferCollect(t, ctx, n, digest, 0, floors, gc)
	for group := range 2 {
		plan := result.HeldGC[3*group].Plan
		// C's root/data remain held, but acknowledged raw Outcome/CDC may
		// already be expired. Export only the actual retained raw suffix.
		result.HeldSparseFrom[group] = max(plan.RootFloor, plan.FeedFloor-1, plan.OutcomeFloor-1)
	}
	client.member, client.stage = 2, "held-C-after-actual-GC"
	nativeTransferSnapshot(client, spec)
	client.feed(nativeFeedInput{Action: "snapshot-close"})
	result.SourceRows[1] = nativeTransferSources(t, ctx, n, result.HeldSparseFrom, mid, result.Original, "after-held-gc-suffix", sources, client)
	client.member, client.stage = 2, "snapshot-release"
	client.feed(nativeFeedInput{Action: "retain", Request: nativeFeedRequest(digest, "snapshot-release", 0)})
	client.stage = "snapshot-refused-after-release"
	if _, e := client.exchange(nativeTransferInput{Action: "feed", Feed: &nativeFeedInput{Version: 1, Action: "snapshot"}}); !errors.Is(e, errGraphHistoryExpired) {
		t.Fatal("retained-only snapshot token unexpectedly opened", e)
	}
	for group := range 2 {
		position := cursor.positions[group]
		if position < 3 {
			t.Fatal("ACK anchor did not advance")
		}
		floors[group] = [6]uint64{position - 1, position - 1, position, position, position - 1, 1}
	}
	result.GC = nativeTransferCollect(t, ctx, n, digest, 1, floors, gc)
	result.SparseFrom = [2]uint64{result.GC[0].Plan.RootFloor, result.GC[3].Plan.RootFloor}
	client.member, client.stage = 2, "generic-At-C-after-release"
	probe, cause := client.exchange(nativeTransferInput{Action: "query", Query: &nativeSelectorsInput{Version: 1, Action: "prepare", Request: nativeFeedRequest(digest, "generic-At-C", 0), Mode: graphReadAt, Cut: result.Snapshot}})
	if cause == nil {
		var observed nativeSelectorsReply
		if e := nativeFeedDecodeJSON(probe.Body, &observed); e != nil {
			t.Fatal(e)
		}
		cut, e := nativeTransferCutDecode(observed.Cut)
		if e != nil || !feedCutEqual(cut, old) || observed.Handle == 0 {
			t.Fatal("generic At(C) substituted a cut", e)
		}
		client.query(nativeSelectorsInput{Action: "publish", Handle: observed.Handle})
		nativeTransferObserve(client, spec, observed.Handle, false)
		client.query(nativeSelectorsInput{Action: "release", Handle: observed.Handle})
		result.GenericAtAfterRelease = "available-actual-At-C-complete-typed-index-read"
	} else if errors.Is(cause, errGraphHistoryExpired) {
		result.GenericAtAfterRelease = "expired-actual-native-history-floor"
	} else {
		t.Fatal("generic At(C) failed with an unclassified source cause", cause)
	}
	business = time.Now()
	phase(5, half, half+1, true)
	result.BusinessPhases[3] = nativeCreateUpdatePhaseStop("checkpoint-update", business, started)
	checkpoint := nativeScalarWorkloadCut(t, ctx, n, graphReadFresh, graphCut{}, rangeMovePooledSemanticContractID())
	result.Checkpoint = wire(checkpoint)
	result.CheckpointFirst = half + 1
	client.member, client.stage = 2, "unread-post-gc"
	nativeTransferDeliver(client, mid, digest, &pageSequence, &result, false, true, processes)
	if len(result.PendingPage) == 0 {
		t.Fatal("actual unread pending business page was not replaced")
	}
	business = time.Now()
	phase(5, half+1, spec.Facts, true)
	result.BusinessPhases[4] = nativeCreateUpdatePhaseStop("remaining-update", business, started)
	final := nativeScalarWorkloadCut(t, ctx, n, graphReadFresh, graphCut{}, rangeMovePooledSemanticContractID())
	result.Final = wire(final)
	client.member, client.stage = 2, "final-complete-feed"
	nativeTransferDrain(client, final, digest, &pageSequence, &result)
	client.feed(nativeFeedInput{Action: "ack", Request: result.PendingRequest, Digest: result.PendingDigest})
	if spec.Layout == "columns" {
		pair[1] = nativeFeedRetainedPublishColumns(t, ctx, n, spec.cost(), checkpoint, final, nativeFeedRequest(digest, "checkpoint-columns", 0), "checkpoint-seal", &result.ColumnFailed[1], columns, result.CheckpointFirst)
	}
	client.stage = "final-current"
	query, actual := nativeTransferQueryOpen(client, nativeSelectorsInput{Action: "prepare", Request: nativeFeedRequest(digest, "current-query", 0), Mode: graphReadAt, Cut: wire(final)})
	if !feedCutEqual(actual, final) {
		t.Fatal("current query changed requested final cut")
	}
	nativeTransferObserve(client, spec, query.Handle, true)
	client.query(nativeSelectorsInput{Action: "close", Handle: query.Handle})
	client.feed(nativeFeedInput{Action: "close"})
	result.Transport = append(result.Transport, nativeScalarWorkloadRestart(t, ctx, n, processes, func() ([]string, error) { return nativeTransferChildEnv(spec, a, true) }, func() { nativeTransferAttach(t, n, a) }))
	result.ProcessInstances += 6
	result.Reaped += 6
	client.member, client.stage = 2, "reopened"
	client.feed(nativeFeedInput{Action: "open", Token: result.Token})
	retained, actual := nativeTransferQueryOpen(client, nativeSelectorsInput{Action: "reopen", Request: nativeFeedRequest(digest, "current-query", 1), Token: query.Token})
	if !feedCutEqual(actual, final) {
		t.Fatal("reopen substituted a current cut")
	}
	nativeTransferObserve(client, spec, retained.Handle, true)
	client.query(nativeSelectorsInput{Action: "release", Handle: retained.Handle})
	client.stage = "snapshot-refused-after-reopen"
	if _, e := client.exchange(nativeTransferInput{Action: "feed", Feed: &nativeFeedInput{Version: 1, Action: "snapshot"}}); !errors.Is(e, errGraphHistoryExpired) {
		t.Fatal("released C snapshot reopened", e)
	}
	client.member, client.stage = 2, "K-after-physical-GC"
	r, e = nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCoordinate, data: k.wire})
	if e != nil {
		t.Fatal(e)
	}
	againK, e = decodeGraphProcessCoordinatorResult(r)
	if e != nil || againK.decision != kResult.decision {
		t.Fatal("K result changed after real collection/reopen", e)
	}
	result.KReopened = mustNativeTransferResponse(t, r)
	lookupK("K-original-after-physical-GC-reopen")
	if spec.Layout == "columns" {
		for _, cached := range []bool{false, true} {
			clients := nativeGraphEdgeUniqueColumnClients(t, ctx, n, &pair[1], fmt.Sprintf("checkpoint-reopened-%t", cached), false, &result.ColumnFailed[1], columns)
			nativeFeedRetainedReadColumns(t, clients, &pair[1], spec.cost(), final, cached, false, result.CheckpointFirst)
			nativeGraphEdgeUniqueColumnCloseClients(clients)
		}
		nativeFeedRetainedRetireColumns(t, ctx, n, &pair[1], "retire-checkpoint-columns", &result.ColumnFailed[1], columns)
		for i := range pair {
			result.ColumnBindings[i], result.ColumnManifests[i] = pair[i].Bindings, pair[i].Manifests
		}
	}
	client.member, client.stage = 2, "terminal"
	client.feed(nativeFeedInput{Action: "release"})
	client.feed(nativeFeedInput{Action: "close"})
	client.stage = "terminal-token-refusal"
	if _, e := client.exchange(nativeTransferInput{Action: "feed", Feed: &nativeFeedInput{Version: 1, Action: "open", Token: result.Token}}); !errors.Is(e, errGraphPinReleased) {
		t.Fatal("native terminal feed token did not refuse", e)
	}
	result.SourceRows[2] = nativeTransferSources(t, ctx, n, result.SparseFrom, final, result.Original, "post-gc-final-suffix", sources, client)
	result.ControlExtraInvocations, result.OriginalCaptures, result.ExtraInvocations = extra, driver.originals, driver.extra
	for _, p := range result.BusinessPhases {
		result.BusinessNanos += p.WallNanos
	}
	result.Reaped += nativeGraphCostStop(t, n, 2, processes)
	result.Transport = append(result.Transport, n.all[1].mux.hub.workload.receipt(n.all[1].mux.hub))
	for member := uint64(1); member <= 6; member++ {
		logical, allocated, e := nativeGraphCostFileBytes(filepath.Join(n.dir, fmt.Sprint(member)))
		if e != nil || !nativeColumnAdd(result.PersistedFileBytes, logical) || !nativeColumnAdd(result.AllocatedFileBytes, allocated) {
			t.Fatal("actual all-volume census", e)
		}
		result.PersistedFileBytes += logical
		result.AllocatedFileBytes += allocated
	}
	result.TotalNanos = time.Since(started).Nanoseconds()
	result.DiagnosticNanos = result.TotalNanos - result.SetupNanos - result.BusinessNanos - result.TransferNanos
	if ctx.Err() != nil || result.DiagnosticNanos < 0 || result.Reaped != result.ProcessInstances {
		t.Fatal("incomplete actual process/timing lifecycle", ctx.Err())
	}
	result.Scope = "Actual native Recipient.Acquire/Cursor.Next, all HOME publication moves, complete original K/ABORT/GML/old-current/type-index/CDC/column/cache/reopen and real held-C plus rolling-ACK collection; actual postmove business is partition8 local while original logical roles remain unchanged. Recovery4 independent retained byte pools/hard ceilings remain authoritative; no execution, capacity, runtime or paired acceptance."
	if e := open("result.json").Encode(result); e != nil {
		t.Fatal(e)
	}
}

func mustNativeTransferResponse(t *testing.T, r processResponse) []byte {
	t.Helper()
	wire, e := encodeProcessResponse(r)
	if e != nil {
		t.Fatal(e)
	}
	return wire
}
