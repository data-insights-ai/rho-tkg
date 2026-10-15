package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Authored only: authentic stored tickets survive normal deadline/caller loss;
// this is API composition, not an actual OS process-replacement qualification.
func TestGraphOwnerActualFundedCleanupAfterNormalDeadline(t *testing.T) {
	h, _, catalog, ca, cb, a, b := ownerActualTransferFixture(t, 5)
	if _, err := ca.ReserveIDs(t.Context(), "remote-live", [32]byte{10}, 2, 1); err != nil {
		t.Fatal(err)
	}
	o, err := cb.AllocationSession(t.Context(), "remote-live", "session", [32]byte{10}, [32]byte{11})
	if err != nil {
		t.Fatal(err)
	}
	if cursor, _, err := cb.InstallIDs(t.Context(), o); err != nil || cursor == nil {
		t.Fatal(err)
	}
	if _, err := ca.ReserveIDs(t.Context(), "remote-reserved", [32]byte{12}, 2, 1); err != nil {
		t.Fatal(err)
	}
	at := mustCut(t, h, []uint8{0, 1}, nil)
	move, err := catalog.ReserveMove(t.Context(), a, ca.routes[0], "funded-move", [32]byte{13}, 0, b.ID, at)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Freeze(t.Context(), a, move); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(time.Until(time.Unix(0, catalog.binding.Transfer.DeadlineUnixNanos)))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	if _, err := ca.ReserveIDs(t.Context(), "late-new", [32]byte{14}, 2, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("new authority after expiry", err)
	}
	lost, cancel := context.WithCancel(t.Context())
	cancel()
	if err := catalog.Cancel(lost, a, move); err != nil {
		t.Fatal("funded owner cleanup stranded", err)
	}
	r, err := cb.CloseAllocationSession(lost, o)
	if err != nil || r.Slot.State != 2 || r.Slot.Consumed != 0 {
		t.Fatal("same lost session did not burn/fence", r, err)
	}
	if _, err := cb.CloseReservedIDs(lost, "remote-reserved", [32]byte{12}); err != nil {
		t.Fatal("reserved terminal credit lost", err)
	}
	_, _, err = graphCostParallelRead(t.Context(), h.db, func(tr fdb.ReadTransaction) (bool, error) {
		for _, request := range []string{"move/funded-move", "grant/remote-live", "grant/remote-reserved"} {
			ticket, found, err := get[graphOwnerCleanup](tr, catalog.key("cleanup", request))
			if err != nil {
				return false, err
			}
			if !found || ticket.Calls != 1 || ticket.DeadlineUnixNanos <= catalog.binding.Transfer.DeadlineUnixNanos || ticket.DeadlineUnixNanos > catalog.binding.Transfer.DeadlineUnixNanos+int64(30*time.Second) {
				return false, errMismatch
			}
		}
		owner, found, err := (&graphHistoryRetention{h: h, binding: catalog.binding.Source}).owner(tr, move.Owner.Request)
		if err != nil {
			return false, err
		}
		if !found || owner.State != historyReleased {
			return false, errMismatch
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if cursor, _, err := cb.InstallIDs(t.Context(), o); cursor != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cleanup minted cursor", err)
	}
}

func TestGraphOwnerCatalogAdmissionGuards(t *testing.T) {
	for _, p := range []graphOwnerPolicy{{}, {4097, 16 << 20}, {8, 256<<20 + 1}} {
		if err := p.validate(); !errors.Is(err, errInvalid) {
			t.Fatal("unbounded controls", p, err)
		}
	}
	if err := (graphOwnerPolicy{64, 16 << 20}).validate(); err != nil {
		t.Fatal(err)
	}
	if err := (graphOwnerMove{Bucket: 255}).validate(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	var catalog *graphOwnerCatalog
	if _, err := catalog.Client(t.Context(), graphOwnerAuthority{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, _, err := catalog.Observe(t.Context()); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := catalog.ReserveMove(t.Context(), graphOwnerAuthority{}, graphOwnerRoute{}, "", [32]byte{}, 0, "", cut{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := catalog.Freeze(t.Context(), graphOwnerAuthority{}, graphOwnerMove{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := catalog.Activate(t.Context(), graphOwnerAuthority{}, graphOwnerMove{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := catalog.Cancel(t.Context(), graphOwnerAuthority{}, graphOwnerMove{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := newGraphOwnerCatalog(ctx, &graphHistoryRetention{h: &harness{}}, [2]graphOwnerAuthority{}, graphOwnerPolicy{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled install entered SDK", err)
	}
	bad := graphOwnerMove{Before: graphOwnerState{Version: math.MaxUint64}}
	if err := bad.validate(); !errors.Is(err, errInvalid) {
		t.Fatal("epoch/version wrap", err)
	}
}

// Direct real-SDK API composition, AUTHORED ONLY. This is not a physical target
// process replacement, transfer workload acceptance or an allocation proof.
func TestGraphOwnerActualAtomicBucketTransferAndOriginalReplay(t *testing.T) {
	p := graphCostProfile{1, 2, 100, 1}
	h := fixture(t, graphCostConfig(graphOwnerNamespace+rand.Text(), p))
	t.Cleanup(func() {
		_, err := h.db.Transact(func(tr fdb.Transaction) (any, error) {
			for g := uint8(0); g < 2; g++ {
				tr.ClearRange(prefixRange(h.prefix(g)))
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
		}
	})
	initial := mustCut(t, h, []uint8{0, 1}, nil)
	source, err := newGraphHistoryRetentionContext(t.Context(), h, initial, graphSelectorsPolicy(h.config).History)
	if err != nil {
		t.Fatal(err)
	}
	a := graphOwnerAuthority{"source-A", [32]byte{1}}
	b := graphOwnerAuthority{"target-B", [32]byte{2}}
	policy := graphOwnerPolicy{MaxRecords: 128, MaxControlBytes: 16 << 20}
	catalog, err := newGraphOwnerCatalog(t.Context(), source, [2]graphOwnerAuthority{a, b}, policy)
	if err != nil {
		t.Fatal(err)
	}
	clientA, err := catalog.Client(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	clientB, err := catalog.Client(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	prepare := func(id string, command graphCommand, provenance uint64, client *graphOwnerClient) graphOwnerIntent {
		t.Helper()
		before, _, err := h.graphCostPointSnapshot(command)
		if err != nil {
			t.Fatal(err)
		}
		coordinator := uint8(0)
		if command.Kind == "node-create" {
			coordinator = command.Node.Owner
		}
		x := h.graphTransaction(id, coordinator, command, before)
		x.Lifecycle = &lifecycleMetadata{Provenance: provenance}
		if command.Kind != "node-create" {
			x.Lifecycle.EdgeLife = command.Edge.ID + 1
			x.Lifecycle.Definition = &lifecycleDefinition{ID: uint64(command.Edge.Value), Kind: "i64", I64: command.Edge.Value} // #nosec G115 -- fixed fixture values5/6.
		}
		body, err := wire(x)
		if err != nil {
			t.Fatal(err)
		}
		intent, err := client.Prepare(t.Context(), body)
		if err != nil {
			t.Fatal(err)
		}
		body[0] = 0
		if len(intent.Body) == 0 || intent.Body[0] != '{' {
			t.Fatal("intent aliases caller bytes")
		}
		return intent
	}
	commit := func(client *graphOwnerClient, intent graphOwnerIntent) outcome {
		t.Helper()
		o, callbacks, err := client.Submit(t.Context(), intent)
		if err != nil || !o.Commit || callbacks < 1 {
			t.Fatal(o, callbacks, err)
		}
		return o
	}
	for i, node := range p.nodes() {
		client := clientA
		if node.Owner == 1 {
			client = clientB
		}
		commit(client, prepare(fmt.Sprintf("owned-node-%d", i), graphCommand{Kind: "node-create", Node: node}, uint64(i+1), client))
	}
	k := prepare("original-K", graphCommand{Kind: "edge-create", Edge: p.edge(0, 5)}, 4, clientA)
	kResult := commit(clientA, k)
	commit(clientA, prepare("other-edge", graphCommand{Kind: "edge-create", Edge: p.edge(1, 5)}, 5, clientA))
	old := mustCut(t, h, []uint8{0, 1}, nil)
	absent := prepare("old-capture-without-original", graphCommand{Kind: "edge-update", Edge: p.edge(0, 6)}, 4+p.Facts, clientA)
	// A structurally valid stale current-generation capture produces a genuine
	// retained ABORT pair, never business CDC. Preserve that original too.
	abortTx, err := decode[transaction](absent.Body)
	if err != nil {
		t.Fatal(err)
	}
	abortTx.ID, abortTx.Request = "retained-abort", "request/retained-abort"
	abortTx.Participants[0].Generation--
	abortBody, err := wire(abortTx)
	if err != nil {
		t.Fatal(err)
	}
	aborted, err := clientA.Prepare(t.Context(), abortBody)
	if err != nil {
		t.Fatal(err)
	}
	abortResult, _, err := clientA.Submit(t.Context(), aborted)
	if err != nil || abortResult.Commit || abortResult.Reason != "generation" {
		t.Fatal("no authentic ABORT", abortResult, err)
	}
	x, err := decode[transaction](k.Body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.submit(x); !errors.Is(err, errStale) {
		t.Fatal("ordinary submit bypass", err)
	}
	if _, _, err := h.graphCostSubmit(x); !errors.Is(err, errStale) {
		t.Fatal("ordinary cost submit bypass", err)
	}
	move, err := catalog.ReserveMove(t.Context(), a, clientA.routes[0], "move-0-A-B", [32]byte{3}, 0, b.ID, old)
	if err != nil {
		t.Fatal(err)
	}
	moveAgain, err := catalog.ReserveMove(t.Context(), a, clientA.routes[0], move.Request, move.Owner.Secret, 0, b.ID, old)
	if err != nil || !graphColumnSame(move, moveAgain) {
		t.Fatal("factory intent changed", err)
	}
	frozen, err := catalog.Freeze(t.Context(), a, move)
	if err != nil || frozen.Phase != 1 {
		t.Fatal(frozen, err)
	}
	// No other source readers/pins exist here: THIS move owner must exclude GC.
	if err := source.begin("gc-during-move", cut{frozen.Cut}); !errors.Is(err, errHistoryInUse) {
		t.Fatal("frozen handoff lost physical retained-prefix protection", err)
	}
	frozenAgain, err := catalog.Freeze(t.Context(), a, move)
	if err != nil || !graphColumnSame(frozen, frozenAgain) {
		t.Fatal("freeze replay", err)
	}
	if _, _, err := clientA.Submit(t.Context(), absent); !errors.Is(err, errOwnerBusy) {
		t.Fatal("frozen NEW write", err)
	}
	if o, _, err := clientA.Submit(t.Context(), k); err != nil || o != kResult {
		t.Fatal("original changed under freeze", o, err)
	}
	if _, err := catalog.Activate(t.Context(), a, move); !errors.Is(err, errStale) {
		t.Fatal("source activated as target", err)
	}
	state, err := catalog.Activate(t.Context(), b, move)
	if err != nil || state.Routes != [2]graphOwnerRoute{{b.ID, 2}, {b.ID, 1}} || state.Moving != "" {
		t.Fatal(state, err)
	}
	if again, err := catalog.Activate(t.Context(), b, move); err != nil || again != state {
		t.Fatal("activation replay", again, err)
	}
	view, _, err := catalog.Observe(t.Context())
	if err != nil || view.State != state {
		t.Fatal(view, err)
	}
	stateBody, err := wire(state)
	if err != nil || !bytes.Equal(view.Records[1].Value, stateBody) {
		t.Fatal("routing was not native observation", err)
	}
	if _, _, err := clientA.Submit(t.Context(), k); !errors.Is(err, errStale) {
		t.Fatal("old owner ACKed K", err)
	}
	if _, _, err := clientA.Original(t.Context(), k); !errors.Is(err, errStale) {
		t.Fatal("old owner recovered K", err)
	}
	if _, _, err := clientB.Submit(t.Context(), k); !errors.Is(err, errStale) {
		t.Fatal("pre-activation target client acquired new epoch by label", err)
	}
	clientB, err = catalog.Client(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	proof, _, err := clientB.Original(t.Context(), k)
	if err != nil || proof.Outcome != kResult {
		t.Fatal("successor original", proof.Outcome, err)
	}
	if o, _, err := clientB.Submit(t.Context(), k); err != nil || o != kResult {
		t.Fatal("SAME old body replay", o, err)
	}
	if proof, _, err := clientB.Original(t.Context(), aborted); err != nil || proof.Outcome != abortResult || len(proof.Original.Value) != 0 {
		t.Fatal("successor ABORT/no CDC authority", proof, err)
	}
	if o, _, err := clientB.Submit(t.Context(), aborted); err != nil || o != abortResult {
		t.Fatal("ABORT body changed", o, err)
	}
	if _, _, err := clientB.Original(t.Context(), absent); !errors.Is(err, errUnknown) {
		t.Fatal("missing original invented decision", err)
	}
	if _, _, err := clientB.Submit(t.Context(), absent); !errors.Is(err, errStale) {
		t.Fatal("old missing original executed after move", err)
	}
	wrongTx, err := decode[transaction](k.Body)
	if err != nil {
		t.Fatal(err)
	}
	wrongTx.Dependency++ // Still structurally valid, genuinely different body K.
	wrongBody, err := wire(wrongTx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clientB.Prepare(t.Context(), wrongBody); !errors.Is(err, errMismatch) {
		t.Fatal("different valid body under K", err)
	}
	// Actual new work uses a NEW immutable factory request/current routing.
	read, err := source.OpenGraphSnapshot(t.Context(), graphSnapshotRequest{Mode: "At", Scope: []uint8{0, 1}, At: old.record}, graphSelectorsPolicy(h.config).Read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := read.Close(); err != nil {
			t.Error(err)
		}
	})
	updated := prepare("successor-new-update", graphCommand{Kind: "edge-update", Edge: p.edge(0, 6)}, 4+p.Facts, clientB)
	commit(clientB, updated)
	points, err := read.Points(t.Context(), []graphSnapshotKey{{Group: 0, Key: edgeKey("edge", p.edge(0, 5))}, {Group: 0, Key: "phantom"}})
	if err != nil || len(points) != 2 || !points[0].Found || points[0].Value.Value != 5 || points[1].Found {
		t.Fatal("old5/phantom after successor6", points, err)
	}
	current, _, err := h.graphCostPointSnapshot(graphCommand{Kind: "edge-update", Edge: p.edge(0, 6)})
	if err != nil || current.Values[0][edgeKey("edge", p.edge(0, 6))].Value != 6 {
		t.Fatal("successor did not perform real write", err)
	}
	check := func(reader *graphSnapshot, corrected bool) {
		t.Helper()
		want := map[string]int64{}
		for _, node := range p.nodes() {
			want[fmt.Sprintf("%d/%s", node.Owner, lifeKey(node))] = 1
		}
		for fact := uint64(0); fact < 2; fact++ {
			v := int64(5)
			if corrected && fact == 0 {
				v = 6
			}
			edge := p.edge(fact, v)
			for _, kind := range []string{"edge", "out", "in"} {
				group := uint8(0)
				if kind == "in" {
					group = edge.Target.Owner
				}
				want[fmt.Sprintf("%d/%s", group, edgeKey(kind, edge))] = v
			}
		}
		end, err := reader.Scan(t.Context(), func(row graphSnapshotRow) error {
			key := fmt.Sprintf("%d/%s", row.Group, row.Key)
			v, exists := want[key]
			if !exists || row.Value.Value != v {
				return errMismatch
			}
			delete(want, key)
			return nil
		})
		if err != nil || len(want) != 0 || end.Rows[0]+end.Rows[1] != 9 {
			t.Fatal("whole old/current exact set", end, want, err)
		}
	}
	check(read, false)
	freshReader, err := source.OpenGraphSnapshot(t.Context(), graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}}, graphSelectorsPolicy(h.config).Read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := freshReader.Close(); err != nil {
			t.Error(err)
		}
	})
	check(freshReader, true)
	if err := freshReader.Close(); err != nil {
		t.Fatal(err)
	}
	// Reconstruct catalog/client from retained native binding, not process labels.
	reopened, err := newGraphOwnerCatalog(t.Context(), source, [2]graphOwnerAuthority{a, b}, policy)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := reopened.Client(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	if proof, _, err := recovered.Original(t.Context(), k); err != nil || proof.Outcome != kResult {
		t.Fatal("reconstructed genuine authority", err)
	}
	final := mustCut(t, h, []uint8{0, 1}, nil)
	reverse, err := catalog.ReserveMove(t.Context(), b, clientB.routes[0], "cancel-B-A", [32]byte{4}, 0, a.ID, final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Freeze(t.Context(), b, reverse); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Cancel(t.Context(), b, reverse); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Cancel(t.Context(), b, reverse); err != nil {
		t.Fatal("exact cleanup replay", err)
	}
	if _, err := catalog.Freeze(t.Context(), b, reverse); !errors.Is(err, errStale) {
		t.Fatal("canceled move reused", err)
	}
	back, err := catalog.ReserveMove(t.Context(), b, clientB.routes[0], "actual-B-A", [32]byte{5}, 0, a.ID, final)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Freeze(t.Context(), b, back); err != nil {
		t.Fatal(err)
	}
	if state, err := catalog.Activate(t.Context(), a, back); err != nil || state.Routes[0] != (graphOwnerRoute{a.ID, 3}) {
		t.Fatal("real logical owner ABA", state, err)
	}
	if _, _, err := clientA.Submit(t.Context(), k); !errors.Is(err, errStale) {
		t.Fatal("A1 client ACKed K after A3", err)
	}
	if _, err := catalog.ReserveMove(t.Context(), a, clientA.routes[0], move.Request, move.Owner.Secret, 0, b.ID, old); !errors.Is(err, errStale) {
		t.Fatal("A1 move factory revived after A3", err)
	}
	if _, err := catalog.ReserveMove(t.Context(), a, clientA.routes[0], "never-installed-A1", [32]byte{6}, 0, b.ID, old); !errors.Is(err, errStale) {
		t.Fatal("delayed A1 reservation minted A3 authority", err)
	}
	currentA, err := catalog.Client(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	if o, _, err := currentA.Submit(t.Context(), k); err != nil || o != kResult {
		t.Fatal("current A3 lost immutable K", o, err)
	}
}
