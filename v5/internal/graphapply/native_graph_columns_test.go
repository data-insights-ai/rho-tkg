package graphapply

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func nativeColumnGuardPolicy() nativeColumnPolicy {
	return nativeColumnPolicy{ChunkRecords: 1, ChunkRows: 8, ChunkBytes: 64 << 10, FrameBytes: 32 << 10, MaxGenerations: 4, MaxChunks: 512, MaxRetainedBytes: 8 << 20, MaxWorkRecords: 1 << 32, MaxWorkBytes: 1 << 44, MaxDecodedRows: 1 << 34, MaxOutputBytes: 1 << 38, MaxRPCs: 1 << 20, MaxWireBytes: 1 << 40, CacheBytes: 1 << 20, MemTableBytes: 64 << 10, Lifetime: time.Minute}
}

// One real retained graph history, then a mutation. Publication never accepts
// runner-produced JSON, caller counts or a copied certificate as completeness.
func TestNativeColumnsPublishResumeCacheAndRetainedCut(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	c, _ := certifiedCoordinator(n)
	cut := func() graphCut {
		r := certifiedOpen(t, c, graphReadRequest{mode: graphReadFresh}, nil)
		at, err := r.Cut()
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		return at
	}
	initial := cut()
	ops, claims := crossEdgeFixture(t, n)
	tx, plan := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, plan)
	n.crossResolve(decision)
	set := func(value int64, id uint64) {
		p, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "p", Value: graphstate.I64(value), ValueID: graphstate.ValueID(id)}}, id+30, []freshBinding{{role: valueBinding, id: id, grant: claims[0].grant}})
		if err != nil {
			t.Fatal(err)
		}
		original := n.submitPartitionGraph(0, p, reasonNone)
		replayed := n.submitPartitionGraph(0, p, reasonNone)
		if replayed.disposition != requestReplay || replayed.index != original.index || replayed.identity != original.identity || replayed.hash != original.hash {
			t.Fatal("original proposal replay lost lineage", original, replayed)
		}
	}
	set(5, 5)
	old := cut()
	hosts := [2]*allocationHost{n.hosts[0][0], n.hosts[1][0]}
	policy := nativeColumnGuardPolicy()
	// A shape-valid but unauthenticated certificate would fail At. Tiny whole
	// query admission must win BEFORE any native certificate/root inspection.
	foreign := old
	foreign.certificates = append([]graphCertificate(nil), old.certificates...)
	foreign.certificates[0].image[0]++
	tiny := policy
	tiny.MaxWorkBytes = uint64(tiny.ChunkBytes)
	if source, err := newNativeColumnSource(t.Context(), hosts, initial, foreign, tiny); source != nil || !errors.Is(err, errLimit) {
		t.Fatal("capture ran before admission", source, err)
	}
	allowance, _, err := nativeColumnSourceAllowance(hosts, policy)
	if err != nil {
		t.Fatal(err)
	}
	if source, err := newNativeColumnSource(t.Context(), hosts, initial, foreign, policy); source != nil || !errors.Is(err, errInvalid) {
		t.Fatal(source, err)
	} else if receipt, ok := errors.AsType[*nativeColumnSourceFailure](err); !ok || receipt.Charged != allowance {
		t.Fatal("failed capture refunded", err)
	}
	source, err := newNativeColumnSource(t.Context(), hosts, initial, old, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	dir := filepath.Join(t.TempDir(), "columns")
	store, err := openNativeColumnStore(dir, vfs.Default, true, source)
	if err != nil {
		t.Fatal(err)
	}
	id := [16]byte{1}
	// An actual persisted single-page prefix survives a close/reopen. It is not
	// a readable manifest, and the retry retains its cumulative attempted work.
	if _, err := store.publishPage(t.Context(), id, source); err != nil {
		t.Fatal(err)
	}
	if _, err := store.manifest(id); !errors.Is(err, errNativeColumnIncomplete) {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openNativeColumnStore(dir, vfs.Default, false, source)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	manifest, err := store.publish(t.Context(), id, source)
	if err != nil || manifest.Frames == 0 || manifest.Rows != 4 {
		t.Fatal(manifest, err)
	}
	cache, err := openNativeColumnCache(filepath.Join(t.TempDir(), "cache"), vfs.Default, true, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	if err := store.cache(t.Context(), id, source, cache); err != nil {
		t.Fatal(err)
	}
	assert := func(src *nativeColumnSource, generation [16]byte, at graphCut, cached *nativeColumnCache, value int64, rows int) {
		t.Helper()
		count, property := 0, 0
		end, err := store.walk(t.Context(), generation, src, at, cached, func(row nativeColumnRow) error {
			count++
			if row.Component == byte(graphstate.ScalarProperty) {
				property++
				if row.Owner != 1 || row.Life != 2 || row.After.Value != value || !row.After.Present || row.After.Revision == 0 || row.After.Provenance == 0 {
					t.Fatal(row)
				}
			}
			if row.Owner == 999 {
				t.Fatal("phantom")
			}
			if err := store.Close(); !errors.Is(err, errLimit) {
				t.Fatal("callback Close must refuse safely", err)
			}
			return nil
		})
		if err != nil || end.Generation.ID != generation || count != rows || property != rows-3 {
			t.Fatal(end, count, property, err)
		}
	}
	assert(source, id, old, cache, 5, 4)
	rejected := errors.New("consumer refuses column prefix")
	if end, err := store.walk(t.Context(), id, source, old, nil, func(nativeColumnRow) error { return rejected }); !errors.Is(err, rejected) || end.Frames != 0 {
		t.Fatal("callback supplied complete EOF", end, err)
	}
	// Cache bytes are real owned files. A corrupt cached frame is refused even
	// while native source and immutable manifest remain genuine.
	cacheName := nativeColumnCacheChunkName(manifest.Generation, 0)
	original, err := cache.get(cacheName, policy.ChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), original...)
	bad[len(bad)-1] ^= 1
	if err := cache.atomic(cacheName, bad); err != nil {
		t.Fatal(err)
	}
	if end, err := store.walk(t.Context(), id, source, old, cache, func(nativeColumnRow) error { return nil }); !errors.Is(err, errCorrupt) || end.Frames != 0 {
		t.Fatal("corrupt cache supplied EOF", end, err)
	}
	if err := cache.atomic(cacheName, original); err != nil {
		t.Fatal(err)
	}
	exhausted, err := openNativeColumnCache(filepath.Join(t.TempDir(), "exhausted"), vfs.Default, true, 2*nativeColumnMetadataBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.cache(t.Context(), id, source, exhausted); !errors.Is(err, errLimit) {
		t.Fatal("exhausted cache grew", err)
	}
	if err := exhausted.Close(); err != nil {
		t.Fatal(err)
	}
	set(6, 6)
	closeTx, closePlan := n.crossPlan([]graphstate.Operation{{Kind: graphstate.Close, Owner: 3, Life: 4, Scope: ops[0].Scope}}, nil, 38)
	closeDecision := n.crossCommit(closeTx, closePlan)
	n.crossResolve(closeDecision)
	current := cut()
	assert(source, id, old, cache, 5, 4)
	if _, err := store.publish(t.Context(), [16]byte{9}, source); !errors.Is(err, errNativeColumnChanged) {
		t.Fatal("old source sealed after mutation", err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	newSource, err := newNativeColumnSource(t.Context(), hosts, initial, current, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer newSource.Close()
	if _, err := store.publish(t.Context(), [16]byte{2}, newSource); err != nil {
		t.Fatal(err)
	}
	// The generation is an ordered change column; an old At omits the later
	// replacement, while the full stream carries both original values/revisions.
	assert(newSource, [16]byte{2}, old, nil, 5, 4)
	seen := []int64{}
	ended := false
	if _, err := store.walk(t.Context(), [16]byte{2}, newSource, current, nil, func(row nativeColumnRow) error {
		if row.Component == byte(graphstate.ScalarProperty) {
			seen = append(seen, row.After.Value)
		}
		if row.Owner == 3 && row.Component == byte(graphstate.Presence) && !row.After.Present {
			ended = row.Before.Present && row.Before.Handle == 4 && row.After.Revision > row.Before.Revision && row.SourceLife == 2 && row.TargetLife == 18
		}
		return nil
	}); err != nil || len(seen) != 2 || seen[0] != 5 || seen[1] != 6 || !ended {
		t.Fatal(seen, ended, err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if end, err := store.walk(cancelled, id, newSource, old, nil, func(nativeColumnRow) error { return nil }); !errors.Is(err, context.Canceled) || end.Frames != 0 {
		t.Fatal(end, err)
	}
	if _, err := store.walk(t.Context(), id, newSource, old, nil, func(nativeColumnRow) error { return nil }); !errors.Is(err, errInvalid) {
		t.Fatal("foreign generation source", err)
	}
	if err := newSource.Close(); err != nil {
		t.Fatal(err)
	}
	opening, pageGrant, err := nativeColumnSourceAllowance(hosts, policy)
	if err != nil {
		t.Fatal(err)
	}
	limitedPolicy := policy
	limitedPolicy.MaxWorkBytes = opening.Bytes + pageGrant.Bytes
	limited, err := newNativeColumnSource(t.Context(), hosts, initial, current, limitedPolicy)
	if err != nil {
		t.Fatal(err)
	}
	positions := [2]uint64{limited.initial[0].sourceIndex, limited.initial[1].sourceIndex}
	if frames, next, err := limited.page(t.Context(), positions); err != nil || len(frames) != 1 || next == positions {
		t.Fatal("funded genuine prefix", frames, next, err)
	}
	charged := limited.charged
	if frames, _, err := limited.page(t.Context(), positions); !errors.Is(err, errLimit) || frames != nil || limited.charged != charged {
		t.Fatal("exhausted source dispatched/refunded", frames, err)
	}
	if err := limited.Close(); err != nil {
		t.Fatal(err)
	}
	ownerCtx, cancelOwner := context.WithCancel(t.Context())
	expiring, err := newNativeColumnSource(ownerCtx, hosts, initial, current, policy)
	if err != nil {
		t.Fatal(err)
	}
	cancelOwner()
	// Explicit Close joins the same expiry-triggered teardown, without sleeps
	// or racing constructor view publication. Every native view is released.
	if err := expiring.Close(); err != nil {
		t.Fatal(err)
	}
	for _, host := range hosts {
		usage, err := host.machine.store.ApplicationUsage()
		if err != nil || usage.Views != 0 || usage.ViewBytes != 0 {
			t.Fatal("cancelled owner pinned native generation", usage, err)
		}
	}
}

func TestNativePartitionColumnsBindDisjointGenuinePrefixesAndRetainedReadCursor(t *testing.T) {
	n := newCertifiedGraphNetwork(t)
	remote := remoteCertifiedFixture(t, n)
	cut := func() graphCut {
		read, err := remote.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
		if err != nil {
			t.Fatal(err)
		}
		result, err := read.Cut()
		err = errors.Join(err, read.Close())
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	initial := cut()
	ops, claims := crossEdgeFixture(t, n)
	tx, plan := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, plan)
	n.crossResolve(decision)
	set := func(value int64, id uint64) {
		p, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "p", Value: graphstate.I64(value), ValueID: graphstate.ValueID(id)}}, id+30, []freshBinding{{role: valueBinding, id: id, grant: claims[0].grant}})
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(0, p, reasonNone)
	}
	set(5, 5)
	old := cut()
	policy := nativeColumnGuardPolicy()
	id := [16]byte{7}
	var sources [2]*nativeColumnSource
	var stores [2]*nativeColumnStore
	var bindings [2]nativeColumnBinding
	var manifests [2]nativeColumnManifest
	for g := range 2 {
		c := &crossProcessCoordinator{host: n.hosts[g][0], transports: remote.transports, streams: remote.streams}
		source, err := newPartitionNativeColumnSource(t.Context(), c, initial, old, policy)
		if err != nil {
			t.Fatal(err)
		}
		sources[g] = source
		defer source.Close()
		store, err := openNativeColumnStore(filepath.Join(t.TempDir(), "local-columns"), vfs.Default, true, source)
		if err != nil {
			t.Fatal(err)
		}
		stores[g] = store
		defer store.Close()
		if source.binding.Coverage != 1<<g || source.views[1-g] != nil || source.hosts[1-g] != nil {
			t.Fatal("invented omitted native Host/source")
		}
		manifests[g], err = store.publish(t.Context(), id, source)
		if err != nil {
			t.Fatal(err)
		}
		bindings[g] = source.binding
		if manifests[g].Positions[1-g] != source.initial[1-g].sourceIndex || manifests[g].Frames != source.final[g].sourceIndex-source.initial[g].sourceIndex {
			t.Fatal("omitted source claimed EOF", manifests[g])
		}
	}
	if err := nativeColumnJoinBoundaries(bindings, manifests); err != nil {
		t.Fatal(err)
	}
	bad := manifests
	bad[1].Generation.Sources[0].Generation++
	if err := nativeColumnJoinBoundaries(bindings, bad); !errors.Is(err, errInvalid) {
		t.Fatal("foreign source generation joined", err)
	}
	// The actual cost-owner dispatch retains cookie/sequence, exact last-page
	// replay and one fixed source deadline; no fake native frame is bootstrapped.
	spec := nativeGraphCostSpec{Version: 1, Facts: 2, CrossPercent: 100, Writers: 1}
	local := &crossProcessCoordinator{host: n.hosts[0][0], transports: remote.transports, streams: remote.streams}
	sessions := &nativeGraphCostColumnSessions{ctx: t.Context(), coordinator: local, spec: &spec, dir: t.TempDir()}
	defer sessions.close()
	request := nativeGraphCostColumnRequest{Action: nativeColumnsOpen, Initial: nativeGraphCostColumnCutInput(t, initial), Final: nativeGraphCostColumnCutInput(t, old), Generation: [16]byte{9}, Create: true}
	invoke := func(q nativeGraphCostColumnRequest) ([]byte, nativeGraphCostColumnReceipt, error) {
		t.Helper()
		wire, err := nativeGraphCostColumnInput(q)
		if err != nil {
			t.Fatal(err)
		}
		body, err := sessions.serve(t.Context(), processRequest{op: processCostObserve, data: wire})
		if err != nil {
			return body, nativeGraphCostColumnReceipt{}, err
		}
		receipt, err := nativeGraphCostColumnDecodeReceipt(body)
		return body, receipt, err
	}
	_, opened, err := invoke(request)
	if err != nil {
		t.Fatal(err)
	}
	request = nativeGraphCostColumnRequest{Action: nativeColumnsPublish, Cookie: opened.Cookie}
	for {
		request.Sequence++
		_, receipt, err := invoke(request)
		if err != nil {
			t.Fatal(err)
		}
		if receipt.EOF {
			break
		}
	}
	request.Action, request.At = nativeColumnsRead, nativeGraphCostColumnCutInput(t, old)
	request.Sequence++
	if _, _, err := invoke(request); err != nil {
		t.Fatal(err)
	}
	request.Action, request.At = nativeColumnsNext, nil
	request.Sequence++
	first, receipt, err := invoke(request)
	if err != nil || receipt.EOF || len(receipt.Body) == 0 {
		t.Fatal(receipt, err)
	}
	owner := sessions.owner
	beforeOwner, beforeSource := owner.lastSequence, owner.source.charged
	replayed, _, err := invoke(request)
	if err != nil || !bytes.Equal(first, replayed) || owner.lastSequence != beforeOwner || owner.source.charged != beforeSource {
		t.Fatal("last genuine chunk replay reset work", err)
	}
	if _, _, err := invoke(request); !errors.Is(err, errLimit) {
		t.Fatal("multiple replay grants", err)
	}
	foreignRequest := request
	foreignRequest.Cookie.Request[0]++
	if _, _, err := invoke(foreignRequest); !errors.Is(err, errInvalid) {
		t.Fatal("foreign owner cookie", err)
	}
	set(6, 6)
	_ = cut()
	cursor, err := stores[0].openReadCursor(t.Context(), id, sources[0], old, nil)
	if err != nil {
		t.Fatal(err)
	}
	before := *cursor
	rejected := errors.New("column page rejected")
	if complete, err := stores[0].nextReadChunk(t.Context(), cursor, func(uint64, []byte, []nativeColumnFrame, bool) error { return rejected }); complete || !errors.Is(err, rejected) || cursor.ordinal != before.ordinal || cursor.positions != before.positions || cursor.complete {
		t.Fatal("failed callback advanced original read cursor", complete, err)
	}
	rows, properties := 0, 0
	for {
		complete, err := stores[0].nextReadChunk(t.Context(), cursor, func(_ uint64, _ []byte, frames []nativeColumnFrame, eof bool) error {
			if err := stores[0].Close(); !errors.Is(err, errLimit) {
				t.Fatal("callback Close deadlocked/closed active store", err)
			}
			for _, frame := range frames {
				for _, row := range frame.Rows {
					rows++
					if row.Component == byte(graphstate.ScalarProperty) {
						properties++
						if row.After.Value != 5 {
							t.Fatal("retained cursor substituted current p6", row)
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			break
		}
	}
	if rows != 3 || properties != 1 || cursor.manifest.Rows != 3 {
		t.Fatal(rows, properties, cursor.manifest)
	}
	if _, err := stores[0].publish(t.Context(), [16]byte{8}, sources[0]); !errors.Is(err, errNativeColumnChanged) {
		t.Fatal("old local source sealed after mutation", err)
	}
	// Consumer cancellation during a complete hydrated chunk cannot commit its
	// cursor or advertise EOF. Source Close is safe outside source/store locks.
	prior := *owner.cursor
	if !owner.gate.tryLock() {
		t.Fatal("owner unexpectedly in flight")
	}
	complete, callbackErr := owner.store.nextReadChunk(t.Context(), owner.cursor, func(uint64, []byte, []nativeColumnFrame, bool) error {
		if _, err := owner.receipt(0); err != nil {
			t.Fatal("callback accounting deadlocked", err)
		}
		return owner.source.Close()
	})
	owner.gate.unlock()
	if complete || !errors.Is(callbackErr, context.Canceled) || owner.cursor.ordinal != prior.ordinal || owner.cursor.positions != prior.positions || owner.cursor.complete {
		t.Fatal("callback cancellation committed continuation/EOF", complete, callbackErr)
	}
	if err := sessions.close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := invoke(nativeGraphCostColumnRequest{Action: nativeColumnsNext, Cookie: opened.Cookie, Sequence: owner.lastSequence + 1}); !errors.Is(err, errInvalid) {
		t.Fatal("late owner revived", err)
	}
	if owner.source.views[0] != nil || !owner.source.closed {
		t.Fatal("canceled owner retained native view")
	}
}

// Pure static required-profile arithmetic. No dataset, process, probe, compiler
// or runtime measurement is constructed by this guard.
func TestNativeColumnRequiredCostProfilesPrepayEveryFinitePass(t *testing.T) {
	limits := defaultMaterializerLimits()
	for _, facts := range []uint64{10_000, 100_000, 1_000_000} {
		application := raftlog.DefaultApplicationPolicy(1)
		application.MaxImageBytes = 172
		spec := nativeGraphCostSpec{Version: 1, Facts: facts, CrossPercent: 100, Writers: 32}
		init := declaredInitCommand{}
		log := raftlog.DefaultLimits()
		generations := raftlog.ApplicationGenerationLimits{}
		if err := nativeGraphCostProfile(&spec, &init, &log, &application, &generations); err != nil {
			t.Fatal(err)
		}
		policy, err := nativeGraphCostColumnsPolicy(application, limits)
		if err != nil {
			t.Fatal("required profile cannot prepay", facts, err)
		}
		opening, grant, err := nativeColumnLimitAllowance(application, limits, policy)
		if err != nil {
			t.Fatal(err)
		}
		opening, err = opening.scaled(2)
		if err != nil {
			t.Fatal(err)
		}
		opening, err = nativeColumnPeerOpening(opening, limits)
		if err != nil {
			t.Fatal(err)
		}
		pages, err := grant.scaled((application.RetainedApplicationRecords/3 + 1) * nativeColumnsPasses * nativeColumnsInvocationAttempts)
		if err != nil || !opening.admit(pages, policy) || policy.MaxOutputBytes <= 1<<40 || policy.MaxDecodedRows <= 1<<40 {
			t.Fatal(facts, policy, err)
		}
	}
	if _, err := (nativeColumnWork{Bytes: ^uint64(0)}).plus(nativeColumnWork{Bytes: 1}); !errors.Is(err, errLimit) {
		t.Fatal("counter sum overflow", err)
	}
	if _, err := (nativeColumnWork{OutputBytes: ^uint64(0)}).scaled(2); !errors.Is(err, errLimit) {
		t.Fatal("pass product overflow", err)
	}
}
