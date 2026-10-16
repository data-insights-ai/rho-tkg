package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Static source-policy admission only. It allocates no required-profile data,
// executes no workload, and makes no runtime/RSS/capacity qualification claim.
func TestNativeContentionRequiredProfileAndWholeGenerationGrantAreFinite(t *testing.T) {
	spec := nativeContentionSpec{Version: 1, Family: nativeContentionFamily, Facts: 1_000_000, CrossPercent: 100, Writers: 32, Layout: "columns"}
	init := declaredInitCommand{}
	l, application := raftlog.DefaultLimits(), raftlog.DefaultApplicationPolicy(1)
	application.MaxImageBytes, application.MaxInstallWrites = 172, 256
	generation := raftlog.ApplicationGenerationLimits{}
	if err := nativeContentionProfile(&spec, &init, &l, &application, &generation); err != nil {
		t.Fatal("required native profile refused before effects", err)
	}
	if err := errors.Join(l.Validate(), application.Validate(l)); err != nil || generation.MaxBytes > 1<<40 || generation.MaxRecords > 1<<40 {
		t.Fatal("actual native profile contract admission", err)
	}
	frames, err := spec.retainedFrames()
	if err != nil {
		t.Fatal(err)
	}
	_, records, _, err := spec.applicationAdmission()
	rootBytes, recoveryRecords, recoveryErr := raftlog.ApplicationRecoveryRootOverhead(0)
	// Derive the extra row from the actual native recovery API, including its
	// empty-credit envelope. A graph-only259-row ceiling must fail this guard.
	if err != nil || recoveryErr != nil || rootBytes == 0 || recoveryRecords != 1 || records != frames*(uint64(application.MaxInstallWrites)+3+recoveryRecords)+65536 || application.RetainedApplicationRecords != records {
		t.Fatal("native recovery record omitted from whole-run admission", records, recoveryRecords, err, recoveryErr)
	}
	p, err := nativeContentionColumnAllowance(application, defaultMaterializerLimits(), frames, nativeContentionWholeSeconds)
	if err != nil || p.Version != 1 || p.ChunkRecords != 16 || p.ChunkRows != 16 || p.Lifetime != time.Minute || p.MaxWorkBytes == ^uint64(0) || p.MaxRetainedBytes > 1<<62 || p.CacheBytes != 2<<20 || p.MemTableBytes != 1<<20 {
		t.Fatal("required additive whole-generation policy", p, err)
	}
	opening, page, err := nativeColumnLimitAllowance(application, defaultMaterializerLimits(), p)
	if err != nil || !opening.admit(page, p) || p.MaxChunks < frames {
		t.Fatal("authentic capture/first page not prepaid", err)
	}
	legacy := nativeColumnGuardPolicy()
	wire, err := json.Marshal(legacy)
	if err != nil || legacy.Version != 0 || bytes.Contains(wire, []byte(`"Version"`)) {
		t.Fatal("fresh policy changed legacy zero-version bytes", err)
	}
	bad := spec
	bad.Facts++
	if _, _, _, err := bad.applicationAdmission(); !errors.Is(err, errInvalid) {
		t.Fatal("unsupported profile admitted", err)
	}
	// No quota wraps into a small accepted grant, including persisted cursor
	// products and the monotone representation-work additions.
	if _, err := page.scaled(^uint64(0)); !errors.Is(err, errLimit) || nativeColumnAdd(^uint64(0), 1) {
		t.Fatal("whole-query accounting overflow accepted", err)
	}
	transport := nativeWorkloadTransportSpec{Kind: "injected-rtt", TargetRTTMillis: 100, Seed: 1}
	floor, err := nativeContentionCausalMinimum(spec, transport)
	if err != nil || floor != 200_000_300 {
		t.Fatal("required causal RTT lower bound", floor, err)
	}
	a := nativeContentionAdmission{Version: 1, Spec: spec, Run: [16]byte{1}, Transport: transport, BudgetSeconds: nativeContentionWholeSeconds, CausalMinimumMillis: floor, DeadlineUnixNanos: time.Now().Add(nativeContentionWholeSeconds * time.Second).UnixNano()}
	digest, err := a.digest()
	if err != nil || digest == ([32]byte{}) {
		t.Fatal("required fixed budget identity", err)
	}
	short := a
	short.BudgetSeconds = 1800
	if short.validate() == nil {
		t.Fatal("guaranteed-impossible RTT budget admitted")
	}
	a.BudgetSeconds--
	changedDigest, err := a.digest()
	if err != nil || changedDigest == digest {
		t.Fatal("chosen budget absent from source-policy identity", err)
	}
	metrics := nativeContentionRunMetrics{CallNanos: 1<<63 - 1}
	if err := metrics.addDurations(nativeContentionTrace{ElapsedNanos: 1}); !errors.Is(err, errLimit) || metrics.CallNanos != 1<<63-1 {
		t.Fatal("caller timing overflow changed ledger", err)
	}
	failure := &nativeColumnCreationFailure{Failure: &nativeColumnSourceFailure{Cause: context.DeadlineExceeded, Charged: page, RPCs: 2, RequestBytes: 3, ReplyBytes: 4}, FullTTL: true, Expired: true}
	if !errors.Is(failure, context.DeadlineExceeded) {
		t.Fatal("internal capture sentinel lost")
	}
	wire, err = json.Marshal(failure)
	var diagnostic nativeColumnCreationFailure
	if err != nil || json.Unmarshal(wire, &diagnostic) != nil || diagnostic.Failure == nil || diagnostic.Failure.RPCs != 2 || diagnostic.Failure.RequestBytes != 3 || diagnostic.Failure.ReplyBytes != 4 || diagnostic.Failure.Charged != page {
		t.Fatal("actual failure diagnostics cannot cross native child JSON", err)
	}
	// The required fresh maximum is a checked ledger policy, not a buffer or a
	// disk-capacity assertion. Legacy admission remains exactly one TiB.
	cacheFS := vfs.NewMem()
	if p.MaxRetainedBytes <= nativeColumnsCacheBytes {
		t.Fatal("required profile no longer exercises fresh cache admission")
	}
	if _, err := openNativeColumnCache("legacy", cacheFS, true, p.MaxRetainedBytes); !errors.Is(err, errInvalid) {
		t.Fatal("legacy cache cap changed", err)
	}
	cache, err := openNativeColumnCacheForPolicy("fresh", cacheFS, true, p)
	if err != nil {
		t.Fatal("finite fresh cache refused", err)
	}
	if cache.ledger.Maximum != p.MaxRetainedBytes || cache.ledger.Charged != 2*nativeColumnMetadataBytes {
		t.Fatal("cache quota allocated or identity lost", cache.ledger)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	cache, err = openNativeColumnCacheForPolicy("fresh", cacheFS, false, p)
	if err != nil {
		t.Fatal("fresh cache ledger could not reopen", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	changed := p
	changed.MaxRetainedBytes--
	if _, err := openNativeColumnCacheForPolicy("fresh", cacheFS, false, changed); !errors.Is(err, errCorrupt) {
		t.Fatal("foreign fresh cache maximum adopted", err)
	}
	if _, err := openNativeColumnCacheForPolicy("old-policy", cacheFS, true, legacy); !errors.Is(err, errInvalid) {
		t.Fatal("legacy policy entered fresh cache constructor", err)
	}
}

// A compact real dependency prefix with an earlier-original replay. The short
// PRIVATE test lifetime exercises actual owner expiry; ordinary source leases
// and required workload policies retain their one-minute cap. Source authored
// only: this guard has not been executed during implementation.
func TestNativeContentionColumnDependencyPrepassSurvivesExpiryAndReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := collectedGraphNetworkWithAgreement(t, nil, durableColumnGraphSemanticContractID())
	ops, claims := crossEdgeFixture(t, n)
	remote := remoteCertifiedFixture(t, n)
	c := &crossProcessCoordinator{host: n.hosts[0][0], transports: remote.transports, streams: remote.streams}
	publisher, err := newRetainedPinPublisher(c)
	if err != nil {
		t.Fatal(err)
	}
	cut := func() graphCut {
		t.Helper()
		read, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
		if err != nil {
			t.Fatal(err)
		}
		at, err := read.Cut()
		err = errors.Join(err, read.Close())
		if err != nil {
			t.Fatal(err)
		}
		return at
	}
	set := func(value int64, id uint64) allocationProposal {
		t.Helper()
		p, err := c.host.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "p", Value: graphstate.I64(value), ValueID: graphstate.ValueID(id)}}, id+40, []freshBinding{{role: valueBinding, id: id, grant: claims[0].grant}})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	original := set(5, 5)
	first := n.submitPartitionGraph(0, original, reasonNone)
	initial := cut()
	replay := n.submitPartitionGraph(0, original, reasonNone)
	if replay.index != first.index || replay.disposition != requestReplay {
		t.Fatal("actual earlier original lost")
	}
	n.submitPartitionGraph(0, set(6, 6), reasonNone)
	final := cut()
	policy := nativeColumnGuardPolicy()
	policy.Version, policy.Lifetime = 1, 5*time.Second
	owner, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	stop := context.AfterFunc(t.Context(), cancel)
	defer func() { stop(); cancel() }()
	source, err := newPartitionNativeColumnSource(owner, c, initial, final, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.retainedBinding(); err != nil {
		t.Fatal(err)
	}
	pinRequest, err := publisher.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadAt, cut: initial}, 20)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := pinRequest.Original()
	if err != nil {
		t.Fatal(err)
	}
	dir, id := filepath.Join(t.TempDir(), "dependency-prefix"), [16]byte{211}
	store, err := openNativeColumnStore(dir, vfs.Default, true, source)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.prepareRetention(id, source, seed); err != nil {
		t.Fatal(err)
	}
	var saved nativeColumnRetentionRecord
	for page := 0; page < 8; page++ {
		_, complete, err := store.retentionDependencyPage(t.Context(), id, source)
		if err != nil || complete {
			t.Fatal("replay should precede final source EOF", complete, err)
		}
		saved, err = store.retentionRecord(id)
		if err != nil {
			t.Fatal(err)
		}
		if saved.Dependencies.Feed[0] == first.index {
			break
		}
	}
	if saved.Dependencies.Feed[0] != first.index || saved.Dependencies.Positions[0] <= source.initial[0].sourceIndex || saved.Dependencies.Positions[0] >= source.final[0].sourceIndex || saved.Dependencies.EOF || len(saved.Original)+len(saved.Token) != 0 {
		t.Fatal("not a genuine partial earlier-original dependency prefix", saved.Dependencies)
	}
	before, err := store.progress(id)
	if err != nil {
		t.Fatal(err)
	}
	failed, stopFailed := context.WithCancel(t.Context())
	stopFailed()
	if _, _, err := store.retentionDependencyPage(failed, id, source); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled prepass", err)
	}
	after, err := store.progress(id)
	if err != nil || after.Work.Bytes <= before.Work.Bytes {
		t.Fatal("failed page grant refunded", err)
	}
	unchanged, err := store.retentionRecord(id)
	if err != nil || unchanged.Dependencies.AttemptedPages != saved.Dependencies.AttemptedPages+1 {
		t.Fatal("attempt not retained", err)
	}
	saved.Dependencies.AttemptedPages++
	if unchanged.Dependencies != saved.Dependencies {
		t.Fatal("failed hydration advanced cursor/minimum/EOF")
	}
	// An explicit short CALL deadline is not evidence that the actual owner TTL
	// elapsed and cannot authorize another opening grant. FullTTL still describes
	// the genuine source constructor; only its own Expired can retire that TTL.
	short, stopShort := context.WithTimeout(t.Context(), time.Millisecond)
	classified := nativeColumnCreationInterruption(short, source)
	stopShort()
	if classified == nil || !classified.FullTTL || classified.Expired {
		t.Fatal("call deadline masqueraded as actual TTL", classified)
	}
	bad := saved
	bad.Dependencies.Seed[0] ^= 1
	if err := store.saveRetention(bad, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.retentionDependencyPage(t.Context(), id, source); !errors.Is(err, errCorrupt) {
		t.Fatal("foreign checksummed progress admitted", err)
	}
	if err := store.saveRetention(saved, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-source.ctx.Done():
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	classified = nativeColumnCreationInterruption(owner, source)
	if classified == nil || !classified.FullTTL || !classified.Expired || classified.DeadlineUnixNanos-classified.IssuedUnixNanos != int64(policy.Lifetime) {
		t.Fatal("actual immutable TTL diagnostic", classified)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, source, err = reopenDurablePartitionNativeColumns(owner, dir, vfs.Default, c, id, policy)
	if err != nil {
		t.Fatal("same prepass continuation", err)
	}
	defer source.Close()
	defer store.Close()
	done, err := store.retentionRecord(id)
	if err != nil || !done.Dependencies.EOF || done.Dependencies.CompletedPages <= saved.Dependencies.CompletedPages || done.Dependencies.AttemptedPages <= saved.Dependencies.AttemptedPages || done.Dependencies.Feed[0] != first.index || done.Feed != done.Dependencies.Feed || done.State != nativeColumnPinLive || !bytes.Equal(done.Seed, seed) {
		t.Fatal("prepass restart/substitution or lost original minimum", err, done.Dependencies)
	}
	expectedFrames := source.finalPositions()[0] - source.initial[0].sourceIndex
	if done.Dependencies.Frames != expectedFrames || done.Dependencies.CompletedPages != expectedFrames+1 || done.Dependencies.AttemptedPages != expectedFrames+2 {
		t.Fatal("completed prefix was replayed or failed work was forgotten", done.Dependencies, expectedFrames)
	}
	for g := range 2 {
		if done.Dependencies.Positions[g] != source.finalPositions()[g] {
			t.Fatal("omitted retained source prefix")
		}
	}
	token, err := decodeRetainedPinToken(done.Token)
	if err != nil {
		t.Fatal(err)
	}
	held, err := publisher.Open(t.Context(), token)
	if err != nil {
		t.Fatal("journal replaced real native pin authority", err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.publish(t.Context(), id, source)
	if err != nil || manifest.Chunks < 2 {
		t.Fatal("genuine small publication", err)
	}
	query, stopQuery := context.WithTimeout(owner, time.Minute)
	defer stopQuery()
	columnOwner := &nativeGraphCostColumnOwner{ctx: query, coordinator: c, gate: newGraphRequestGate(), source: source, store: store, generation: id, cookie: nativeGraphCostColumnCookie{1, source.owner}, started: time.Now(), cancel: stopQuery, closeDone: make(chan struct{}), starts: 1, startsLimit: uint64(time.Minute/policy.Lifetime) + 13}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(t.Context()), 3*time.Second)
		defer stop()
		if err := columnOwner.close(cleanup); err != nil {
			t.Error(err)
		}
	}()
	if _, err := columnOwner.next(t.Context(), nativeGraphCostColumnRequest{Action: nativeColumnsRead, Cookie: columnOwner.cookie, Sequence: 1, At: nativeGraphCostColumnCutInput(t, final)}); err != nil {
		t.Fatal(err)
	}
	request := nativeGraphCostColumnRequest{Action: nativeColumnsNext, Cookie: columnOwner.cookie, Sequence: 2}
	accepted, err := columnOwner.next(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	positions, chain := columnOwner.cursor.positions, columnOwner.cursor.chain
	if columnOwner.cursor.ordinal != 1 {
		t.Fatal("first whole chunk not committed")
	}
	select {
	case <-source.ctx.Done():
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	// Last-genuine replay survives transient source expiry under the SAME live
	// query deadline; it does not capture a fresh At or advance its private cursor.
	replayed, err := columnOwner.next(t.Context(), request)
	if err != nil || !bytes.Equal(replayed, accepted) || columnOwner.starts != 1 || columnOwner.cursor.positions != positions || columnOwner.cursor.chain != chain {
		t.Fatal("lost genuine chunk replay/cursor", err)
	}
	request.Sequence++
	renewed, err := columnOwner.next(t.Context(), request)
	if err != nil {
		t.Fatal("same-query authenticated source renewal", err)
	}
	receipt, err := nativeGraphCostColumnDecodeReceipt(renewed)
	if err != nil || receipt.Cookie != request.Cookie || receipt.Ordinal != 1 || receipt.SourceStarts != 2 || receipt.SourceExpirations != 1 || columnOwner.cursor.ordinal != 2 || columnOwner.cursor.source == source || columnOwner.cursor.at != [2]graphCertificate{final.certificates[0], final.certificates[1]} {
		t.Fatal("renewal restarted/imported/replaced actual read", err)
	}
	for !receipt.EOF {
		request.Sequence++
		body, err := columnOwner.next(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		receipt, err = nativeGraphCostColumnDecodeReceipt(body)
		if err != nil || request.Sequence > manifest.Chunks+3 {
			t.Fatal("read EOF/cumulative bound", err)
		}
	}
	if !nativeColumnSame(columnOwner.cursor.manifest, manifest) {
		t.Fatal("immutable generation changed across source expiry")
	}
	if err := columnOwner.source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Retire(t.Context(), id, publisher); err != nil {
		t.Fatal(err)
	}
}
