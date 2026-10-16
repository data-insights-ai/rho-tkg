package graphapply

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// This RED uses only existing native APIs. It fails on actual missing original
// copy after a completed handoff, never on a missing replay helper symbol.
func TestRangeMoveReplayHandoffPreservesSourceOriginalWithCollidingTargetRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, coordinator, intent, _, _ := rangeMoveCoordinatorFixture(t, 48)
	intent.pageBytes = 8 << 10
	id := requestID{79}
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	var originals [2][]byte
	for group, owner := range []graphstate.EntityID{1, 17} {
		h := n.hosts[group][0]
		name := []string{"source-original", "target-original"}[group]
		proposal, err := h.PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: owner, Life: graphstate.LifeID(owner + 1), Scope: window, Name: name}}, uint64(79+group), nil)
		if err != nil {
			t.Fatal("genuine namespace-local original proposal", err)
		}
		n.submitPartitionGraph(group, proposal, reasonNone)
		view, err := h.machine.store.ApplicationView(h.driver.Applied())
		if err != nil {
			t.Fatal(err)
		}
		row, found, err := view.Get(t.Context(), declaredRequestKey(h.machine.ns, graphOperations, [16]byte(id)), 4096)
		if closeErr := view.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if err != nil || !found || row.Deleted {
			t.Fatal("actual stored namespace-local original", group, found, err)
		}
		originals[group] = row.Value
	}
	if bytes.Equal(originals[0], originals[1]) {
		t.Fatal("colliding request fixture did not produce distinct source/target originals")
	}
	done, err := runRangeMoveToCompletion(t, coordinator, intent)
	if err != nil || !done.committed || done.source.phase != rangeMoveFinish || done.target.phase != rangeMoveFinish {
		t.Fatal("handoff must actually complete before missing-copy assertion", done, err)
	}
	for _, target := range n.hosts[1] {
		view, err := target.machine.store.ApplicationView(target.driver.Applied())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := view.Close(); err != nil {
				t.Error(err)
			}
		}()
		base, err := view.RootBounded(t.Context(), 172)
		if err != nil {
			t.Fatal(err)
		}
		output, err := graphstate.NewOutputBudget(1 << 20)
		if err != nil {
			t.Fatal(err)
		}
		q := reader{ctx: t.Context(), view: view, ns: target.machine.ns, base: base, limits: target.machine.limits.allocation, arena: output}
		// Family4 is the existing source-qualified shadow door for exact native
		// ga4 mapped outcomes. Raw source bytes retain their original coordinates.
		sourceOriginal, found, err := q.getBounded(rangeMoveReplayKey(target.machine.ns, intent.source.partition, [16]byte(id), 4), 4096)
		if err != nil || !found || !bytes.Equal(sourceOriginal, originals[0]) {
			t.Fatal("completed handoff omitted exact source-qualified original outcome", found, err)
		}
		localOriginal, found, err := view.Get(t.Context(), declaredRequestKey(target.machine.ns, graphOperations, [16]byte(id)), 4096)
		if err != nil || !found || localOriginal.Deleted || !bytes.Equal(localOriginal.Value, originals[1]) {
			t.Fatal("source copy overwrote independent target original", found, err)
		}
	}
}

func rangeMoveReplayNativeReader(t *testing.T, h *allocationHost, allowance int) *reader {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	})
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	output, err := graphstate.NewOutputBudget(allowance)
	if err != nil {
		t.Fatal(err)
	}
	limits := h.machine.limits.allocation
	limits.readRows, limits.readBytes = 16384, 64<<20
	return &reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: limits, arena: output}
}

func rangeMoveReplayTwoHopFixture(t *testing.T) (*allocationTestNetwork, map[uint64]graphPeerWireTransport) {
	t.Helper()
	// Credits for32 bounded graph+replay steps fit this explicit native storage
	// profile; client source/output budgets keep their original finite caps.
	n := newCrossPartitionNetworkAgreement(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes = 128 << 20
		p.RetainedApplicationRecords = 65536
	}, rangeMoveSemanticContractID())
	_, _ = crossEdgeFixture(t, n)
	transports := make(map[uint64]graphPeerWireTransport, 2)
	for group, partition := range []uint64{3, 8} {
		h := n.hosts[group][0]
		server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
		if err != nil {
			t.Fatal(err)
		}
		service, err := newGraphPeerService(h, server, &nativeGraphTestPump{n: n, group: group, host: h})
		if err != nil {
			t.Fatal(err)
		}
		transports[partition] = &nativeGraphTestTransport{service: service}
	}
	return n, transports
}

func rangeMoveReplayIntentAt(t *testing.T, n *allocationTestNetwork, group int, targetPartition uint64) rangeMoveIntent {
	t.Helper()
	h := n.hosts[group][0]
	configuration := n.read(group, allocationQuery{kind: observeConfiguration}).observation.configuration
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	publication, owner, _, err := graphstore.LookupPublishedRange(t.Context(), view, graphstore.Namespace{Graph: graphstate.GraphID(h.machine.ns.graph), Partition: h.machine.ns.partition}, 1, configuration, graphstore.OwnershipBudget{SourceRows: 64, SourceBytes: 1 << 20, OutputBytes: 1 << 20})
	if closeErr := view.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	target, err := h.target(targetPartition)
	if err != nil {
		t.Fatal(err)
	}
	return rangeMoveIntent{id: n.requestID(), source: h.machine.scope(), target: target, publication: publication, epoch: owner.Epoch, maxPages: 96, pageRows: 64, pageBytes: 8 << 10}
}

func TestRangeMoveReplaySecondHandoffKeepsFirstOriginWithCollidingRequest(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, transports := rangeMoveReplayTwoHopFixture(t)
	id := requestID{85}
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	var originals [2][]byte
	var origins [2]namespace
	for group, owner := range []graphstate.EntityID{1, 17} {
		h := n.hosts[group][0]
		origins[group] = h.machine.ns
		name := []string{"first-origin-A", "independent-origin-B"}[group]
		proposal, err := h.PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: owner, Life: graphstate.LifeID(owner + 1), Scope: window, Name: name}}, uint64(85+group), nil)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(group, proposal, reasonNone)
		q := rangeMoveReplayNativeReader(t, h, 64<<20)
		originals[group], _, err = q.getBounded(declaredRequestKey(q.ns, graphOperations, [16]byte(id)), 4096)
		if err != nil || len(originals[group]) == 0 {
			t.Fatal("genuine original", err)
		}
	}
	if bytes.Equal(originals[0], originals[1]) {
		t.Fatal("same-key originals were not independent")
	}
	for _, hop := range []struct {
		group  int
		target uint64
	}{{0, 8}, {1, 3}} {
		coordinator, err := newRangeMoveCoordinator(n.hosts[hop.group][0], transports)
		if err != nil {
			t.Fatal(err)
		}
		// Reconnect through the native service constructor because replacement
		// coordinators own new authenticated connection streams.
		if hop.group == 1 {
			for _, transport := range transports {
				wire := transport.(*nativeGraphTestTransport)
				old := wire.service
				service, err := newGraphPeerService(old.host, old.sources, old.pump)
				if err != nil {
					t.Fatal(err)
				}
				wire.service = service
			}
		}
		done, err := runRangeMoveToCompletion(t, coordinator, rangeMoveReplayIntentAt(t, n, hop.group, hop.target))
		if err != nil || !done.committed {
			t.Fatal("genuine hop did not finish", hop, done, err)
		}
	}
	// A→B→A is a second handoff onto an independently active namespace. The
	// inherited A row must keep A; B's sameID row must remain separately B.
	for _, target := range n.hosts[0] {
		q := rangeMoveReplayNativeReader(t, target, 64<<20)
		for group, origin := range origins {
			key := declaredRequestKey(origin, graphOperations, [16]byte(id))
			value, found, err := lookupRangeMoveReplay(q, origin, false, key)
			if err != nil || !found || !bytes.Equal(value, originals[group]) {
				t.Fatal("second handoff lost/requalified first original", group, found, err)
			}
		}
	}
}

func rangeMoveReplayNativeCrossRecord(t *testing.T) (*allocationTestNetwork, namespace, []byte, []byte) {
	t.Helper()
	n := newCrossPartitionNetworkAgreement(t, nil, rangeMoveSemanticContractID())
	ops, claims := crossEdgeFixture(t, n)
	tx, proposal := n.crossPlan(ops, claims, 81)
	decision := n.crossCommit(tx, proposal)
	n.crossResolve(decision)
	h := n.hosts[0][0]
	q := rangeMoveReplayNativeReader(t, h, 64<<20)
	key := crossControlKey(h.machine.ns, decision.status.tx, 'R')
	row, found, err := crossControl(q, key, 1<<20)
	if err != nil || !found || len(row.Value) < 1024 {
		t.Fatal("actual native original manifest fixture", found, err)
	}
	return n, h.machine.ns, key, row.Value
}

func TestRangeMoveReplayFragmentsPreserveNativeManifestAndCanonicalRowDigest(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, source, originalKey, originalValue := rangeMoveReplayNativeCrossRecord(t)
	initial := rangeMoveReplayStart()
	initial.family = 9 // native GP R, discovered from actual ga11 inventory.
	wholeReader := rangeMoveReplayNativeReader(t, n.hosts[0][0], 64<<20)
	whole, err := exportRangeMoveReplayPage(wholeReader, source, initial, raftlog.ReadBudget{Rows: 4, Bytes: 1 << 20})
	if err != nil || !whole.control || !whole.recordDone || !bytes.Equal(whole.key, originalKey) || !bytes.Equal(whole.value, originalValue) {
		t.Fatal("actual whole native original", err)
	}
	fragmentReader := rangeMoveReplayNativeReader(t, n.hosts[0][0], 64<<20)
	cursor := initial
	joined := make([]byte, 0, len(originalValue))
	var first, last rangeMoveReplayPage
	pages := 0
	payload := (len(originalValue) + 2) / 3
	for range 32 {
		page, err := exportRangeMoveReplayPage(fragmentReader, source, cursor, raftlog.ReadBudget{Rows: 4, Bytes: 1024 + payload})
		if err != nil {
			t.Fatal("bounded genuine manifest fragment", err)
		}
		if len(page.key) == 0 || page.complete || !page.control || !bytes.Equal(page.key, originalKey) || int(page.offset) != len(joined) || len(page.value) > payload {
			t.Fatal("fragment changed original identity or byte coordinate", page.offset)
		}
		if pages == 0 {
			first = page
		}
		pages++
		joined = append(joined, page.value...)
		if !page.recordDone && (page.next.rows != initial.rows || page.next.bytes != initial.bytes || page.next.digest != initial.digest) {
			t.Fatal("partial fragment changed logical totals")
		}
		cursor, last = page.next, page
		if page.recordDone {
			break
		}
	}
	if pages < 2 || !last.recordDone || !bytes.Equal(joined, originalValue) || last.next.rows != whole.next.rows || last.next.bytes != whole.next.bytes || last.next.digest != whole.next.digest {
		t.Fatal("fragment borders changed complete original digest", pages)
	}
	if fragmentReader.rows <= wholeReader.rows || fragmentReader.bytes <= wholeReader.bytes {
		t.Fatal("repeated actual source reads were not charged cumulatively")
	}
	output, err := graphstate.NewOutputBudget(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := encodeRangeMoveReplayPage(first, output)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeRangeMoveReplayPage(wire, output)
	if err != nil || !reflect.DeepEqual(decoded, first) {
		t.Fatal("fragment codec lost frozen continuation", err)
	}
	clear(wire)
	if !reflect.DeepEqual(decoded, first) {
		t.Fatal("decoded fragment borrowed caller frame")
	}
	target := rangeMoveReplayNativeReader(t, n.hosts[1][0], 64<<20)
	if err := stageRangeMoveReplayPage(target, source, target.ns, [16]byte{82}, first); err != nil || len(target.writes) != 1 || !bytes.Equal(target.writes[0].Value, first.value) {
		t.Fatal("bounded private fragment staging", err)
	}
	if !bytes.Equal(target.writes[0].Key, rangeMoveReplayFragmentKey(target.ns, [16]byte{82}, first.next.sequence)) {
		t.Fatal("fragment became an original/native control row")
	}
	// The earlier private staging was never committed. A genuine final source
	// fragment must not turn absent durable target fragments into an original.
	missing := rangeMoveReplayNativeReader(t, n.hosts[1][0], 64<<20)
	if err := stageRangeMoveReplayPage(missing, source, missing.ns, [16]byte{82}, last); !errors.Is(err, errCorrupt) || len(missing.writes) != 0 {
		t.Fatal("missing durable fragments created a phantom original", err)
	}
	if value, found, err := lookupRangeMoveReplay(missing, source, true, originalKey); value != nil || found || err != nil {
		t.Fatal("refused final fragment published an original", found, err)
	}
}

func TestRangeMoveReplayNativeFutureKeyHolesAdvanceWithoutPhantomOriginal(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, _, _, _ := rangeMoveCoordinatorFixture(t, 8)
	h := n.hosts[0][0]
	frozen := rangeMoveReplayNativeReader(t, h, 64<<20)
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	ids := []requestID{{101}, {102}}
	for _, id := range ids {
		proposal, err := h.PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "future"}}, 83, nil)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(0, proposal, reasonNone)
	}
	initial := rangeMoveReplayStart()
	initial.after = declaredRequestKey(frozen.ns, graphOperations, [16]byte{100})
	first, err := exportRangeMoveReplayPage(frozen, frozen.ns, initial, raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || len(first.key) != 0 || first.complete || !bytes.Equal(first.next.after, declaredRequestKey(frozen.ns, graphOperations, [16]byte(ids[0]))) {
		t.Fatal("first actual future hole failed to advance", err)
	}
	second, err := exportRangeMoveReplayPage(frozen, frozen.ns, first.next, raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || len(second.key) != 0 || second.next.rows != initial.rows || second.next.bytes != initial.bytes || second.next.digest != initial.digest || bytes.Equal(second.next.after, first.next.after) && second.next.family == first.next.family {
		t.Fatal("future holes became originals or stalled continuation", err)
	}
	if frozen.rows == 0 || frozen.bytes == 0 {
		t.Fatal("native skipped-key work was omitted")
	}
	current := rangeMoveReplayNativeReader(t, h, 64<<20)
	page, err := exportRangeMoveReplayPage(current, current.ns, initial, raftlog.ReadBudget{Rows: 1, Bytes: 4096})
	if err != nil || !page.recordDone || !bytes.Equal(page.key, declaredRequestKey(current.ns, graphOperations, [16]byte(ids[0]))) {
		t.Fatal("current funded counterpart did not see actual future original", err)
	}
}

func TestRangeMoveReplayScopesInertStagesAndPhantomLookup(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, _, _, _ := rangeMoveCoordinatorFixture(t, 8)
	source := rangeMoveReplayNativeReader(t, n.hosts[0][0], 64<<20)
	page, err := exportRangeMoveReplayPage(source, source.ns, rangeMoveReplayCursor{}, raftlog.ReadBudget{Rows: 4, Bytes: 8192})
	if err != nil || !page.recordDone {
		t.Fatal("actual mapped-outcome fixture", err)
	}
	target := rangeMoveReplayNativeReader(t, n.hosts[1][0], 64<<20)
	if err := stageRangeMoveReplayPage(target, source.ns, target.ns, [16]byte{84}, page); err != nil || len(target.writes) != 1 {
		t.Fatal("inert native original staging", err)
	}
	namespace, kind, id, valid := rangeMoveReplayOriginal(page.key, false)
	if !valid || !bytes.Equal(target.writes[0].Key, rangeMoveReplayKey(target.ns, namespace.partition, id, kind)) || !bytes.Equal(target.writes[0].Value, page.value) {
		t.Fatal("qualification rewrote original key/value")
	}
	if value, found, err := lookupRangeMoveReplay(target, source.ns, false, page.key); err != nil || found || value != nil {
		t.Fatal("uninstalled private effect minted visible original", found, err)
	}
	foreign := source.ns
	foreign.graph[0] ^= 1
	if err := stageRangeMoveReplayPage(target, foreign, target.ns, [16]byte{84}, page); !errors.Is(err, errInvalid) {
		t.Fatal("foreign graph staged original", err)
	}
	unknown := source.ns
	unknown.partition = 999
	key := append(recordKey(unknown, 4), id[:]...)
	if _, _, err := lookupRangeMoveReplay(target, unknown, false, key); !errors.Is(err, errInvalid) {
		t.Fatal("undeclared original namespace was accepted", err)
	}
	tight := rangeMoveReplayNativeReader(t, n.hosts[0][0], 0)
	if got, err := exportRangeMoveReplayPage(tight, tight.ns, rangeMoveReplayCursor{}, raftlog.ReadBudget{Rows: 4, Bytes: 8192}); !reflect.DeepEqual(got, rangeMoveReplayPage{}) || !errors.Is(err, graphstate.ErrResourceLimit) || tight.rows != 0 || tight.bytes != 0 {
		t.Fatal("exhausted replay ownership admitted native work", err)
	}
}

func TestRangeMoveReplayCodecsRejectCorruptionTruncationAndNilBudgets(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, _, _, _ := rangeMoveCoordinatorFixture(t, 8)
	q := rangeMoveReplayNativeReader(t, n.hosts[0][0], 64<<20)
	page, err := exportRangeMoveReplayPage(q, q.ns, rangeMoveReplayCursor{}, raftlog.ReadBudget{Rows: 4, Bytes: 8192})
	if err != nil {
		t.Fatal(err)
	}
	output, err := graphstate.NewOutputBudget(64 << 20)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := encodeRangeMoveReplayPage(page, output)
	if err != nil {
		t.Fatal(err)
	}
	for size := range len(wire) {
		got, err := decodeRangeMoveReplayPage(wire[:size], output)
		if !reflect.DeepEqual(got, rangeMoveReplayPage{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("truncated page returned partial original", size, err)
		}
	}
	bad := bytes.Clone(wire)
	bad[len(bad)-1] ^= 1
	if got, err := decodeRangeMoveReplayPage(bad, output); !reflect.DeepEqual(got, rangeMoveReplayPage{}) || !errors.Is(err, errCorrupt) {
		t.Fatal("corrupt page admitted original", err)
	}
	if _, err := encodeRangeMoveReplayPage(page, nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil page ownership", err)
	}
	if _, err := decodeRangeMoveReplayPage(wire, nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil decoder ownership", err)
	}
	if _, err := encodeRangeMoveReplayCursor(page.next, nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil cursor ownership", err)
	}
	if _, err := decodeRangeMoveReplayCursor(nil, output); !errors.Is(err, errCorrupt) {
		t.Fatal("nil cursor frame", err)
	}
	if _, err := decodeRangeMoveReplayCursor(nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil cursor decoder ownership", err)
	}
	cursorWire, err := encodeRangeMoveReplayCursor(page.next, output)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeRangeMoveReplayCursor(cursorWire, output)
	if err != nil || !reflect.DeepEqual(got, page.next) {
		t.Fatal("portable cursor roundtrip", err)
	}
}

func TestRangeMoveReplayFirstOriginKeyBijectionAndInheritedInertStage(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, _, _, _ := rangeMoveCoordinatorFixture(t, 8)
	a, b := n.hosts[0][0].machine.ns, n.hosts[1][0].machine.ns
	id := [16]byte{87}
	// These are inert representation values. They construct no completed read,
	// source seal, handoff, or recovery outcome; staged writes stay private.
	for _, tag := range []byte{4, 5, 11, 16, 'A', 'D', 'K', 'P', 'R'} {
		t.Run(strconv.Itoa(int(tag)), func(t *testing.T) {
			control := tag >= 'A'
			key := append(recordKey(a, tag), id[:]...)
			if control {
				key = crossControlKey(a, id, tag)
			}
			qualified := rangeMoveReplayKey(b, a.partition, id, tag)
			original, gotControl, err := rangeMoveReplayUnwrap(b, qualified)
			if err != nil || gotControl != control || !bytes.Equal(original, key) {
				t.Fatal("first-origin full key was not reconstructed bijectively", tag, err)
			}
			origin, family, actualID, valid := rangeMoveReplayOriginal(original, control)
			if !valid || origin != a || family != tag || actualID != id {
				t.Fatal("original key lost namespace/family/identity", tag)
			}
			bad := bytes.Clone(qualified)
			bad[35] = 99
			if original, _, err := rangeMoveReplayUnwrap(b, bad); original != nil || !errors.Is(err, errCorrupt) {
				t.Fatal("unknown inherited native family admitted", err)
			}
			if original, _, err := rangeMoveReplayUnwrap(a, qualified); original != nil || !errors.Is(err, errCorrupt) {
				t.Fatal("another exporter path was requalified", err)
			}
			before := rangeMoveReplayStart()
			before.family = rangeMoveReplayInherited
			value := []byte{tag, 88}
			next := before
			next.sequence, next.rows, next.bytes = 1, 1, uint64(len(key)+len(value)+17)
			next.after, next.digest = qualified, rangeMoveReplayDigest(before.digest, control, key, value)
			page := rangeMoveReplayPage{before: before, next: next, key: key, value: value, control: control, recordDone: true, recordBytes: uint32(len(value))}
			output, err := graphstate.NewOutputBudget(8 << 20)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := encodeRangeMoveReplayPage(page, output)
			if err != nil {
				t.Fatal("inherited codec counterpart", err)
			}
			decoded, err := decodeRangeMoveReplayPage(wire, output)
			if err != nil || !reflect.DeepEqual(decoded, page) {
				t.Fatal("inherited cursor/key codec changed origin", err)
			}
			q := rangeMoveReplayNativeReader(t, n.hosts[0][0], 64<<20)
			if err := stageRangeMoveReplayPage(q, b, a, [16]byte{89}, decoded); err != nil || len(q.writes) != 1 || !bytes.Equal(q.writes[0].Key, rangeMoveReplayKey(a, a.partition, id, tag)) || !bytes.Equal(q.writes[0].Value, value) {
				t.Fatal("B exporter rewrote inherited A original as B", tag, err)
			}
			index, _, err := n.hosts[0][0].machine.store.Checkpoint()
			if err != nil || index != q.base.Index {
				t.Fatal("inert lineage stage installed private effects", err)
			}
		})
	}
}

func TestRangeMoveReplayMissingNativePhaseAndFamilyEOFKeepExactTotals(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, source, _, _ := rangeMoveReplayNativeCrossRecord(t)
	q := rangeMoveReplayNativeReader(t, n.hosts[1][0], 64<<20)
	// Participants own actual ga11 statuses but only the coordinator records D.
	// Exact frozen missing D advances the actual status key without a phantom row.
	start := rangeMoveReplayStart()
	start.family = 6
	page, err := exportRangeMoveReplayPage(q, q.ns, start, raftlog.ReadBudget{Rows: 4, Bytes: 4096})
	if err != nil || len(page.key) != 0 || page.next.rows != 0 || page.next.bytes != 0 || page.next.digest != start.digest || len(page.next.after) == 0 {
		t.Fatal("missing native coordinator phase did not advance without row", err)
	}
	done, err := exportRangeMoveReplayPage(q, q.ns, page.next, raftlog.ReadBudget{Rows: 4, Bytes: 4096})
	if err != nil || done.next.family != 7 || done.next.digest != start.digest {
		t.Fatal("actual family EOF changed original totals", err)
	}
	q = rangeMoveReplayNativeReader(t, n.hosts[0][0], 64<<20)
	start = rangeMoveReplayStart()
	start.family = rangeMoveReplayInherited
	eof, err := exportRangeMoveReplayPage(q, source, start, raftlog.ReadBudget{Rows: 4, Bytes: 4096})
	if err != nil || !eof.complete || len(eof.key) != 0 || eof.next.rows != 0 || eof.next.bytes != 0 || eof.next.digest != start.digest {
		t.Fatal("empty actual inherited inventory was not complete", err)
	}
	if got, err := exportRangeMoveReplayPage(q, source, eof.next, raftlog.ReadBudget{Rows: 4, Bytes: 4096}); !reflect.DeepEqual(got, rangeMoveReplayPage{}) || !errors.Is(err, errInvalid) {
		t.Fatal("EOF cursor restarted original stream", err)
	}
}

func TestRangeMoveReplayDirectRefusalsPreserveZeroOutputs(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	if got, err := exportRangeMoveReplayPage(nil, namespace{}, rangeMoveReplayCursor{}, raftlog.ReadBudget{Rows: 1, Bytes: 4096}); !reflect.DeepEqual(got, rangeMoveReplayPage{}) || !errors.Is(err, errInvalid) {
		t.Fatal("nil reader export", err)
	}
	if err := stageRangeMoveReplayPage(nil, namespace{}, namespace{}, [16]byte{}, rangeMoveReplayPage{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil reader stage", err)
	}
	if value, found, err := lookupRangeMoveReplay(nil, namespace{}, false, nil); value != nil || found || !errors.Is(err, errInvalid) {
		t.Fatal("nil original lookup", err)
	}
	output, err := graphstate.NewOutputBudget(0)
	if err != nil {
		t.Fatal(err)
	}
	if wire, err := encodeRangeMoveReplayCursor(rangeMoveReplayStart(), output); wire != nil || !errors.Is(err, graphstate.ErrResourceLimit) {
		t.Fatal("unfunded cursor allocated output", err)
	}
	for _, bad := range []rangeMoveReplayCursor{{family: 12}, {family: 1}, {family: 0, sequence: 1}, {family: 0, key: []byte{1}}, {family: 1, complete: true, digest: [32]byte{1}}} {
		if wire, err := encodeRangeMoveReplayCursor(bad, output); wire != nil || !errors.Is(err, errInvalid) {
			t.Fatal("invalid cursor accepted", bad, err)
		}
	}
	n, _, _, _, _ := rangeMoveCoordinatorFixture(t, 8)
	q := rangeMoveReplayNativeReader(t, n.hosts[0][0], 64<<20)
	q.limits.readRows = 0
	if got, err := exportRangeMoveReplayPage(q, q.ns, rangeMoveReplayCursor{}, raftlog.ReadBudget{Rows: 1, Bytes: 4096}); !reflect.DeepEqual(got, rangeMoveReplayPage{}) || !errors.Is(err, errLimit) || q.rows != 0 {
		t.Fatal("exhausted physical work read metadata", err)
	}
}
