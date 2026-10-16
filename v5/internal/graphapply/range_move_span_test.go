package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func moveSpanOutput(t *testing.T) *graphstate.OutputBudget {
	t.Helper()
	b, err := graphstate.NewOutputBudget(64 << 20)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Authored source guards only: execution/qualification is deliberately separate.
func TestRangeMoveSpanBatchFramingAndFragments(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := namespace{graph: [16]byte{1}, partition: 3}
	before := rangeMoveReplayStart()
	page := func(c rangeMoveReplayCursor, id byte, value []byte) rangeMoveReplayPage {
		key := rangeMoveReplayNativeKey(n, 4, [16]byte{id})
		next := rangeMoveReplayClone(c)
		next.sequence++
		next.rows++
		next.bytes += uint64(len(key) + len(value) + 17)
		next.after = key
		next.digest = rangeMoveReplayDigest(c.digest, false, key, value)
		return rangeMoveReplayPage{before: c, next: next, key: key, value: value, recordBytes: uint32(len(value)), recordDone: true} // #nosec G115 -- fixed small test values.
	}
	first := page(before, 1, bytes.Repeat([]byte{7}, 2048))
	second := page(first.next, 2, []byte{8})
	batch := rangeMoveReplayBatch{pages: []rangeMoveReplayPage{first, second}}
	one, err := encodeRangeMoveReplayPage(first, moveSpanOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	two, err := encodeRangeMoveReplayPage(second, moveSpanOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	exact := 4 + 4 + sha256.Size + 4 + len(one) + 4 + len(two)
	wire, err := encodeMoveReplayBatch(batch, moveSpanOutput(t), exact)
	if err != nil || len(wire) != exact || rangeMoveBatchFramingBytes != 40 {
		t.Fatal("exact outer seal admission", len(wire), exact, err)
	}
	decoded, err := decodeMoveReplayBatch(wire, moveSpanOutput(t), exact)
	if err != nil || len(decoded.pages) != 2 || !rangeMoveReplaySame(decoded.pages[1].next, second.next) {
		t.Fatal("complete canonical originals", err)
	}
	if _, err := encodeMoveReplayBatch(batch, moveSpanOutput(t), exact-1); !errors.Is(err, errLimit) {
		t.Fatal("omitted outer-seal byte admitted", err)
	}
	for _, prefix := range [][]byte{wire[:len(wire)-1], wire[:len(wire)-32]} {
		if _, err := decodeMoveReplayBatch(prefix, moveSpanOutput(t), exact); !errors.Is(err, errCorrupt) {
			t.Fatal("partial outer EOF accepted", err)
		}
	}
	for _, bad := range []rangeMoveReplayBatch{{pages: []rangeMoveReplayPage{second, first}}, {pages: []rangeMoveReplayPage{first, first}}} {
		if _, err := encodeMoveReplayBatch(bad, moveSpanOutput(t), exact); !errors.Is(err, errInvalid) {
			t.Fatal("noncontiguous original admitted", err)
		}
	}
	partial := first
	partial.value = first.value[:1024]
	partial.recordDone = false
	partial.next = rangeMoveReplayClone(before)
	partial.next.sequence++
	partial.next.key = first.key
	partial.next.offset = 1024
	partial.next.recordBytes = 2048
	partial.next.startSequence = partial.next.sequence
	if _, err := encodeMoveReplayBatch(rangeMoveReplayBatch{pages: []rangeMoveReplayPage{partial}}, moveSpanOutput(t), exact); err != nil {
		t.Fatal("terminal fragment refused", err)
	}
	finish := first
	finish.before = partial.next
	finish.offset = 1024
	finish.value = first.value[1024:]
	finish.next.sequence = partial.next.sequence + 1
	if !rangeMoveReplayPageValid(finish) {
		t.Fatal("bad authored continuation")
	}
	if _, err := encodeMoveReplayBatch(rangeMoveReplayBatch{pages: []rangeMoveReplayPage{partial, finish}}, moveSpanOutput(t), exact); !errors.Is(err, errInvalid) {
		t.Fatal("sibling uncommitted fragment read admitted", err)
	}
	// Recomputed outer seal must not bypass the decoder's independent rule.
	partialWire, err := encodeRangeMoveReplayPage(partial, moveSpanOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	finishWire, err := encodeRangeMoveReplayPage(finish, moveSpanOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	forged, err := boundedEncoding(1<<20, func(w *boundedWriter) { w.add([]byte("GEB\x01")); w.u32(2); w.field(partialWire); w.field(finishWire) })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeMoveReplayBatch(forged, moveSpanOutput(t), 1<<20); !errors.Is(err, errCorrupt) {
		t.Fatal("sealed fragment siblings accepted", err)
	}
	q := &reader{}
	if _, err := stageMoveReplayBatch(q, n, namespace{graph: n.graph, partition: 8}, [16]byte{3}, rangeMoveReplayBatch{pages: []rangeMoveReplayPage{partial, finish}}, before); !errors.Is(err, errInvalid) || len(q.writes) != 0 {
		t.Fatal("stage accepted sibling fragment read", err)
	}
	q.limits.stageRows = 2
	q.stageBytes = 123
	q.writes = []raftlog.KV{{Key: []byte{1}, Value: []byte{2}}, {Key: []byte{1}, Value: []byte{2}}}
	if err := coalesceMoveReplayWrites(q, 1); err != nil || len(q.writes) != 1 || q.stageBytes != 123 {
		t.Fatal("identical original alias changed charges", err)
	}
	q.writes = append(q.writes, raftlog.KV{Key: []byte{1}, Value: []byte{3}})
	if err := coalesceMoveReplayWrites(q, 1); !errors.Is(err, errCorrupt) {
		t.Fatal("differing native output alias", err)
	}
	if _, err := encodeMoveReplayBatch(rangeMoveReplayBatch{pages: []rangeMoveReplayPage{finish}}, moveSpanOutput(t), exact); err != nil {
		t.Fatal("next native batch continuation", err)
	}
	if _, err := exportMoveReplayBatch(nil, n, before, exact, 2); !errors.Is(err, errInvalid) {
		t.Fatal("nil native source", err)
	}
}

func moveSpanNetwork(t *testing.T) *allocationTestNetwork {
	t.Helper()
	return collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes = 256 << 20
		p.RetainedApplicationRecords = 131072
	}, rangeMoveSpanSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 3})
}
func moveSpanRead(t *testing.T, n *allocationTestNetwork, group int, move [16]byte, lineage bool) rangeMoveAuthority {
	t.Helper()
	h := n.hosts[group][0]
	pump := nativeGraphTestPump{n: n, group: group, host: h}
	q := rangeMoveQuery{spans: true, lineage: lineage, move: move, nonce: [16]byte(n.requestID()), sourceRows: 4096, sourceBytes: 16 << 20, outputBytes: 32 << 20}
	reply, err := pump.Move(t.Context(), q)
	if err != nil || reply.err != nil || reply.question != q {
		t.Fatal("actual fresh move read", err, reply.err)
	}
	a, err := h.exportRangeMoveAuthority(reply.proof)
	if err != nil || !a.validSeal() {
		t.Fatal("actual bound authority", err)
	}
	return a
}
func moveSpanManifest(t *testing.T, h *allocationHost) raftlog.ApplicationSnapshotManifest {
	t.Helper()
	if err := h.machine.store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := h.machine.store.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	x, err := h.machine.store.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := x.Close(); err != nil {
			t.Error(err)
		}
	}()
	for range 256 {
		done, err := x.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			m, err := x.Manifest()
			if err != nil || m.Version != 8 {
				t.Fatal("actual AS8", m.Version, err)
			}
			return m
		}
	}
	t.Fatal("finite AS8 build did not reach EOF")
	return raftlog.ApplicationSnapshotManifest{}
}
func moveSpanAudit(t *testing.T, n *allocationTestNetwork, group int) {
	t.Helper()
	h := n.hosts[group][0]
	receiver, prepared, manifest, c, _ := preparedDeclaredAuditFixture(t, h.machine.store, n.configs[group][2])
	if manifest.Version != 8 {
		t.Fatal("fresh export was not AS8", manifest.Version)
	}
	a, err := newDeclaredSnapshotAuditor(receiver, h.machine.ns, n.d, h.machine.limits, h.machine.SemanticContractID())
	if err != nil {
		t.Fatal(err)
	}
	finishDeclaredTestAudit(t, beginDeclaredTestAudit(t, a, prepared, c))
}

// Exercise the private batching seam with genuine exported manifest fragments
// and genuine installed native puts. This is not a completed handoff authority.
func moveSpanInstalledFragments(t *testing.T, n *allocationTestNetwork, tx [16]byte) {
	t.Helper()
	source, target := n.hosts[0][0], n.hosts[1][0]
	q := rangeMoveReplayNativeReader(t, source, 64<<20)
	key := crossControlKey(source.machine.ns, tx, 'R')
	original, found, err := crossControl(q, key, 1<<20)
	if err != nil || !found || len(original.Value) < 1024 {
		t.Fatal("actual original manifest", found, err)
	}
	cursor := rangeMoveReplayStart()
	cursor.family = 9
	workRows, workBytes := q.rows, q.bytes
	if _, err := exportMoveReplayBatch(q, source.machine.ns, cursor, 2048, 2); !errors.Is(err, errLimit) || q.rows != workRows || q.bytes != workBytes {
		t.Fatal("framing refusal consumed another native subcall", err)
	}
	whole, err := exportRangeMoveReplayPage(q, source.machine.ns, cursor, raftlog.ReadBudget{Rows: 1, Bytes: 1 << 20})
	if err != nil || !whole.recordDone || !bytes.Equal(whole.key, key) {
		t.Fatal("actual whole manifest", err)
	}
	transition := rangeMoveReplayPage{before: whole.next, next: rangeMoveReplayClone(whole.next)}
	transition.next.sequence++
	transition.next.family++
	transition.next.after = nil
	inherited := whole
	inherited.before = transition.next
	inherited.next = rangeMoveReplayClone(transition.next)
	inherited.next.sequence++
	inherited.next.rows++
	inherited.next.bytes += uint64(len(key) + len(original.Value) + 17)
	inherited.next.after = rangeMoveReplayKey(source.machine.ns, source.machine.ns.partition, tx, 'R')
	inherited.next.digest = rangeMoveReplayDigest(inherited.before.digest, true, key, original.Value)
	// Inert representation guard: distinct source families can alias one
	// qualified native target key. No fabricated page becomes source authority.
	aliases := rangeMoveReplayBatch{pages: []rangeMoveReplayPage{whole, transition, inherited}}
	r := rangeMoveReplayNativeReader(t, target, 64<<20)
	if _, err := stageMoveReplayBatch(r, source.machine.ns, target.machine.ns, [16]byte{214}, aliases, cursor); err != nil || len(r.writes) != 1 {
		t.Fatal("identical source-qualified alias did not coalesce", len(r.writes), err)
	}
	bad := inherited
	bad.value = bytes.Clone(inherited.value)
	bad.value[0] ^= 1
	bad.next.digest = rangeMoveReplayDigest(bad.before.digest, true, key, bad.value)
	r = rangeMoveReplayNativeReader(t, target, 64<<20)
	if _, err := stageMoveReplayBatch(r, source.machine.ns, target.machine.ns, [16]byte{215}, rangeMoveReplayBatch{pages: []rangeMoveReplayPage{whole, transition, bad}}, cursor); !errors.Is(err, errCorrupt) {
		t.Fatal("differing original alias staged", err)
	}
	move := [16]byte{213}
	payload := (len(original.Value) + 2) / 3
	pages := 0
	for range 8 {
		p, err := exportRangeMoveReplayPage(q, source.machine.ns, cursor, raftlog.ReadBudget{Rows: 1, Bytes: 1024 + payload})
		if err != nil || !bytes.Equal(p.key, key) {
			t.Fatal("actual native fragment export", err)
		}
		wire, err := encodeMoveReplayBatch(rangeMoveReplayBatch{pages: []rangeMoveReplayPage{p}}, moveSpanOutput(t), 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := decodeMoveReplayBatch(wire, moveSpanOutput(t), 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		index, _, err := target.machine.store.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		view, err := target.machine.store.ApplicationView(index)
		if err != nil {
			t.Fatal(err)
		}
		base, err := view.RootBounded(t.Context(), 172)
		if err != nil {
			t.Fatal(err)
		}
		limits := target.machine.limits.allocation
		limits.readRows, limits.readBytes = 16384, 64<<20
		r := &reader{ctx: t.Context(), view: view, ns: target.machine.ns, base: base, limits: limits, arena: moveSpanOutput(t)}
		next, err := stageMoveReplayBatch(r, source.machine.ns, target.machine.ns, move, batch, cursor)
		err = errors.Join(err, view.Close())
		if err != nil || len(r.writes) != 1 {
			t.Fatal("installed predecessor fragment not visible", len(r.writes), err)
		}
		// Existing privileged native fixture installs exactly this actual put;
		// unlike a cursor-only test, the next batch reads committed bytes.
		corruptDeclaredKV(t, target.machine.store, r.writes[0])
		pages++
		cursor = next
		if p.recordDone {
			break
		}
	}
	if pages < 2 || cursor.rows != 1 {
		t.Fatal("fragmented native original did not complete", pages, cursor.rows)
	}
	index, _, err := target.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	view, err := target.machine.store.ApplicationView(index)
	if err != nil {
		t.Fatal(err)
	}
	row, found, err := view.Get(t.Context(), rangeMoveReplayKey(target.machine.ns, source.machine.ns.partition, tx, 'R'), 1<<20)
	err = errors.Join(err, view.Close())
	if err != nil || !found || !bytes.Equal(row.Value, original.Value) {
		t.Fatal("fragment join changed original bytes", found, err)
	}
}

func TestRangeMoveSpanNativeOriginalsReferencesAndUndo(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	t.Run("cross COMMIT ABORT and unused prepare", func(t *testing.T) {
		n := moveSpanNetwork(t)
		ops, claims := crossEdgeFixture(t, n)
		ids := crossRequestsWithLoser(n, 1)
		winner, wreg := crossPlanAt(n, 0, ids[0], ops, claims, 24)
		loser, lreg := crossPlanAt(n, 1, ids[1], ops, claims, 25)
		ws, ls := n.crossSubmit(0, wreg), n.crossSubmit(1, lreg)
		wr, lr := n.crossReadPhase(0, ws.tx, crossRegister), n.crossReadPhase(1, ls.tx, crossRegister)
		prepare, err := n.hosts[0][0].PrepareCrossParticipant(winner, 3, wr, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !n.crossSubmit(0, prepare).yes {
			t.Fatal("winner did not acquire real lock")
		}
		blocked, err := n.hosts[1][0].PrepareCrossParticipant(loser, 3, lr, nil)
		if err != nil {
			t.Fatal(err)
		}
		proof := n.capacityRead(0, blocked)
		abort, existing, err := n.hosts[1][0].AbortCrossTransactionForCapacity(loser, proof)
		if err != nil || existing != (crossStatus{}) {
			t.Fatal("actual conflict authority", existing, err)
		}
		if got := n.crossSubmit(1, abort); got.commit || got.phase != crossDecide {
			t.Fatal(got)
		}
		reserved := moveSpanManifest(t, n.hosts[1][0]).RecoveryReservation
		if len(reserved.Spans) != 2 || reserved.Spans[0].Next != uint32(crossPrepare) || reserved.Spans[1].Next != uint32(crossResolve) {
			t.Fatal("Decide retired unused prepare", reserved)
		}
		crossReopenAll(t, n)
		decision := n.crossReadPhase(1, ls.tx, crossDecide)
		crossResolveAt(n, 1, decision, 1)
		if owner := rangeMoveAdmissionOwner(t, n.hosts[1][0]); owner != (raftlog.ApplicationRecoveryOwner{}) {
			t.Fatal("actual final Resolve did not retire suffix", owner)
		}
		// A local Prepare was never installed at the losing coordinator.
		q, err := graphRetentionReader(n.hosts[1][0], t.Context())
		if err != nil {
			t.Fatal(err)
		}
		_, found, err := crossControl(q, crossControlKey(q.ns, ls.tx, 'P'), 256)
		if err = errors.Join(err, q.view.Close()); err != nil || found {
			t.Fatal("missing prepare was fabricated", found, err)
		}
		// Recover the unchanged winning original after restart, then COMMIT.
		wr = n.crossReadPhase(0, ws.tx, crossRegister)
		winner, err = n.hosts[0][0].RecoverCrossTransaction(wr)
		if err != nil {
			t.Fatal(err)
		}
		vote := n.crossReadPhase(0, ws.tx, crossPrepare)
		prepare, err = n.hosts[0][0].PrepareCrossParticipant(winner, 8, wr, []crossProof{vote})
		if err != nil {
			t.Fatal(err)
		}
		if !n.crossSubmit(1, prepare).yes {
			t.Fatal("winner remote prepare refused")
		}
		commit, err := n.hosts[0][0].DecideCrossTransaction(winner, []crossProof{vote, n.crossReadPhase(1, ws.tx, crossPrepare)})
		if err != nil {
			t.Fatal(err)
		}
		if !n.crossSubmit(0, commit).commit {
			t.Fatal("genuine COMMIT refused")
		}
		n.crossResolve(n.crossReadPhase(0, ws.tx, crossDecide))
		for group := range 2 {
			moveSpanAudit(t, n, group)
		}
		moveSpanInstalledFragments(t, n, ws.tx)
	})
	t.Run("feed original plus reference hydration partial undo and reopen", func(t *testing.T) {
		n, p, ops, _ := retainedPublisherFromNetwork(t, moveSpanNetwork(t))
		owner, err := beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, retainedFeedGuardPolicy(128))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = owner.Publish(t.Context()); err != nil {
			t.Fatal(err)
		}
		local, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "after-feed-C", Scope: ops[0].Scope}}, 50, nil)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(0, local, reasonNone)
		current, err := p.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
		if err != nil {
			t.Fatal(err)
		}
		boundary, err := current.Cut()
		if err = errors.Join(err, current.Close()); err != nil {
			t.Fatal(err)
		}
		seen, complete := 0, false
		for range 64 {
			id := [16]byte(n.requestID())
			d, err := owner.Page(t.Context(), id, boundary)
			if err != nil {
				t.Fatal(err)
			}
			if d.page.kind == feedLocalEnvelope {
				seen++
				if len(d.resolved.entities) == 0 {
					t.Fatal("empty original")
				}
			}
			if _, err = owner.Ack(t.Context(), id, d.digest); err != nil {
				t.Fatal(err)
			}
			if d.page.eof {
				complete = true
				break
			}
		}
		if !complete || seen != 1 {
			t.Fatal("fresh feed lost/duplicated original", complete, seen)
		}
		if err = owner.Release(t.Context()); err != nil {
			t.Fatal(err)
		}
		intent, err := n.hosts[0][0].PrepareRangeMove(t.Context(), n.requestID(), 1, 8, 64, 32, 8<<10)
		if err != nil {
			t.Fatal(err)
		}
		wide := intent
		wide.maxPages = 2048
		wideWire, err := encodeRangeMoveIntent(wide)
		if err != nil || !bytes.HasPrefix(wideWire, []byte("GMI\x02")) {
			t.Fatal("fresh format retained old page ceiling", err)
		}
		mixed := bytes.Clone(wideWire[:len(wideWire)-sha256.Size])
		mixed[3] = 1
		if _, err := decodeRangeMoveIntent(seal(mixed)); !errors.Is(err, errCorrupt) {
			t.Fatal("mixed-version intent upgraded", err)
		}
		wide.source.semantic, wide.target.semantic = rangeMoveSemanticContractID(), rangeMoveSemanticContractID()
		if _, err := encodeRangeMoveIntent(wide); !errors.Is(err, errInvalid) {
			t.Fatal("legacy page ceiling widened", err)
		}
		// Both page count and all four growth counters are funded explicitly.
		reservation, err := n.hosts[1][0].machine.spanMoveReservation(intent)
		if err != nil || !n.hosts[1][0].machine.spanMoveFits(reservation) {
			t.Fatal("fresh recipe not admitted", err)
		}
		bad := reservation
		bad.Spans = append([]raftlog.ApplicationRecoverySpan(nil), reservation.Spans...)
		bad.Spans[1].RetainedBytes = math.MaxUint64
		if n.hosts[1][0].machine.spanMoveFits(bad) {
			t.Fatal("aggregate multiplication overflow accepted")
		}
		coordinator, err := newRangeMoveCoordinatorOn(p.coordinator)
		if err != nil {
			t.Fatal(err)
		}
		gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: p.coordinator.transports[8], sequence: 3}
		p.coordinator.transports[8] = gate
		pending, err := runRangeMoveToCompletion(t, coordinator, intent)
		if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) {
			t.Fatal("actual partial import not reached", pending, err)
		}
		endpoints, err := p.coordinator.endpoints(newGraphRemoteBudget(n.hosts[0][0].machine.limits, moveSpanOutput(t)))
		if err != nil {
			t.Fatal(err)
		}
		lease, err := endpoints[3].openMoveSource(t.Context(), pending.resume.move)
		if err != nil || lease.move.scope.semantic != rangeMoveSpanSemanticContractID() {
			t.Fatal("authenticated fresh sourceMoveOpen", err)
		}
		page, err := lease.moveFrozenPage(t.Context(), 1)
		if err = errors.Join(err, lease.Close()); err != nil || len(page) < 2 || page[0] != 1 {
			t.Fatal("actual frozen source lease/page", err)
		}
		source := moveSpanRead(t, n, 0, pending.resume.move, true)
		hydrated, err := decodeRangeMoveProgress(source.observation.progress, moveSpanOutput(t))
		if err != nil || !hydrated.inlineRetention || len(hydrated.retention) == 0 || !hydrated.reference.valid(hydrated.descriptor) {
			t.Fatal("actual source hydration", err)
		}
		target := moveSpanRead(t, n, 1, pending.resume.move, false)
		progress, err := decodeRangeMoveProgress(target.observation.progress, moveSpanOutput(t))
		if err != nil || progress.inlineRetention || len(progress.retention) != 0 || progress.reference.packet != hydrated.reference.packet || len(target.observation.progress) > rangeMoveSpanProgressBytes {
			t.Fatal("stored progress repeated/substituted lineage", err)
		}
		for _, edit := range []func(*rangeMoveProgress){func(p *rangeMoveProgress) { p.reference.header[0] ^= 1 }, func(p *rangeMoveProgress) { p.reference.key[28] ^= 1 }, func(p *rangeMoveProgress) { p.reference.bytes++ }} {
			forged := hydrated
			forged.reference.key = bytes.Clone(hydrated.reference.key)
			edit(&forged)
			if _, err := encodeRangeMoveProgress(forged, moveSpanOutput(t)); !errors.Is(err, errCorrupt) {
				t.Fatal("forged native hydration accepted", err)
			}
		}
		moveSpanAudit(t, n, 1)
		wire, err := encodeRangeMoveIntent(intent)
		if err != nil {
			t.Fatal(err)
		}
		status := rangeMoveAdmissionSubmit(t, n, 0, wire, rangeMoveAbort, 0, nil, nil)
		if status.result != rangeMoveApplied {
			t.Fatal(status)
		}
		sourceAbort := moveSpanRead(t, n, 0, pending.resume.move, false)
		total := target.observation.status.sequence
		if total == 0 || total > 2 {
			t.Fatal("unexpected actual partial undo extent", total)
		}
		for sequence := uint64(1); sequence <= total; sequence++ {
			status = rangeMoveAdmissionSubmit(t, n, 1, wire, rangeMoveAbort, sequence, nil, []rangeMoveAuthority{sourceAbort})
			if status.result != rangeMoveApplied || status.sequence != sequence {
				t.Fatal("exact-next undo", status)
			}
			if sequence == 1 {
				crossReopenAll(t, n)
				sourceAbort = moveSpanRead(t, n, 0, pending.resume.move, false)
			}
			reservation := moveSpanManifest(t, n.hosts[1][0]).RecoveryReservation
			found := false
			for _, span := range reservation.Spans {
				if span.First == moveSpanUndo {
					found = true
					if span.Next != moveSpanUndo+uint32(sequence) {
						t.Fatal("descending/refilled undo band", span)
					}
				}
			} // #nosec G115 -- actual extent<=2.
			if !found || reservation.ID == ([32]byte{}) {
				t.Fatal("partial undo retired remaining phase bank")
			}
		}
		status = rangeMoveAdmissionSubmit(t, n, 1, wire, rangeMoveAbort, 0, nil, []rangeMoveAuthority{sourceAbort})
		if status.result != rangeMoveApplied || rangeMoveAdmissionOwner(t, n.hosts[1][0]) != (raftlog.ApplicationRecoveryOwner{}) {
			t.Fatal("final terminal did not release", status)
		}
		crossReopenAll(t, n)
		for member := range 3 {
			view, close := crossView(t, n, member, [2]uint64{})
			assertCertifiedProjection(t, view, 1, true, []string{"after-feed-C"})
			assertCertifiedProjection(t, view, 17, true, nil)
			assertCertifiedProjection(t, view, 999, false, nil)
			close()
		}
	})
}
