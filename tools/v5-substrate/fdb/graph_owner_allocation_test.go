package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func TestGraphOwnerLocalIssuanceAndBurnedLifetimeGuards(t *testing.T) {
	cursor := &graphOwnerAllocationCursor{grant: graphOwnerAllocationGrant{First: math.MaxUint64, Last: math.MaxUint64, Count: 1, Request: "grant"}, session: graphOwnerAllocationSession{Request: "session"}}
	if id, err := cursor.Next(); err != nil || id.ID != math.MaxUint64 {
		t.Fatal(id, err)
	}
	if _, err := cursor.Next(); !errors.Is(err, errLimit) {
		t.Fatal("uint64 wrap", err)
	}
	cursor.closed = true
	if _, err := cursor.Next(); !errors.Is(err, errStale) {
		t.Fatal("closed local owner issued", err)
	}
	if _, err := (*graphOwnerAllocationCursor)(nil).Next(); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := graphOwnerAllocatedInput([]graphOwnerAllocatedID{{ID: 1, Grant: "g", Session: "s"}, {ID: 1, Grant: "g", Session: "s"}}); !errors.Is(err, errInvalid) {
		t.Fatal("duplicate issuance proof", err)
	}
	input := []graphOwnerAllocatedID{{ID: 1, Grant: "g", Session: "s"}}
	owned, err := graphOwnerAllocatedInput(input)
	input[0].ID = 2
	if err != nil || owned[0].ID != 1 {
		t.Fatal("caller-owned provenance", owned, err)
	}
}

func ownerActualTransferFixture(t *testing.T, budget ...uint64) (*harness, *graphHistoryRetention, *graphOwnerCatalog, *graphOwnerClient, *graphOwnerClient, graphOwnerAuthority, graphOwnerAuthority) {
	t.Helper()
	var run [16]byte
	if _, err := rand.Read(run[:]); err != nil {
		t.Fatal(err)
	}
	spec := graphFeedWorkloadSpec{Version: 1, Family: graphOwnerTransferFamily, Facts: 20, CrossPercent: 100, Writers: 1, Layout: "kv"}
	cfg := graphOwnerTransferConfig(spec, hex.EncodeToString(run[:]))
	h := fixture(t, cfg)
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
	source, err := newGraphHistoryRetentionContext(t.Context(), h, initial, graphSelectorsPolicy(cfg).History)
	if err != nil {
		t.Fatal(err)
	}
	a := graphOwnerAuthority{"A", [32]byte{1}}
	b := graphOwnerAuthority{"B", [32]byte{2}}
	policy := ownerDirectTransferPolicy(cfg, run)
	if len(budget) > 0 {
		policy.BudgetSeconds = budget[0]
		policy.DeadlineUnixNanos = policy.AdmittedUnixNanos + int64(budget[0])*int64(time.Second)
	}
	catalog, err := newGraphOwnerCatalogTransfer(t.Context(), source, [2]graphOwnerAuthority{a, b}, policy)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := catalog.Client(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := catalog.Client(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	return h, source, catalog, ca, cb, a, b
}

// Genuine SDK source, AUTHORED ONLY. Lost installation is a native SAME receipt
// guard here; physical SDK child replacement belongs to the full driver.
func TestGraphOwnerActualAllocationFactoryAndFencedAbort(t *testing.T) {
	t.Run("LostInstallBurnsInsteadOfReissuing", func(t *testing.T) {
		h, _, _, a, _, _, _ := ownerActualTransferFixture(t)
		grant, err := a.ReserveIDs(t.Context(), "lost", [32]byte{3}, 2, 0)
		if err != nil {
			t.Fatal(err)
		}
		original, err := a.AllocationSession(t.Context(), "lost", "lost-session", [32]byte{3}, [32]byte{4})
		if err != nil {
			t.Fatal(err)
		}
		cursor, receipt, err := a.InstallIDs(t.Context(), original)
		if err != nil || cursor == nil {
			t.Fatal(receipt, err)
		}
		node, ids, err := cursor.Node()
		if err != nil {
			t.Fatal(err)
		}
		// Drop the original returned local object/ACK; recovery may observe its
		// installation but must never construct another iterator at the range base.
		if again, stored, err := a.InstallIDs(t.Context(), original); again != nil || !errors.Is(err, errAllocationConsumed) || !bytes.Equal(stored.Records[0].Value, receipt.Records[0].Value) {
			t.Fatal("SAME install reset cursor", stored, err)
		}
		closed, err := a.CloseAllocationSession(t.Context(), original)
		if err != nil || closed.Slot.State != 2 || closed.Slot.Consumed != 0 {
			t.Fatal("unknown suffix not burned", closed, err)
		}
		command := graphCommand{Kind: "node-create", Node: node}
		before, _, err := h.graphCostPointSnapshot(command)
		if err != nil {
			t.Fatal(err)
		}
		x := h.graphTransaction("burned-factory", 0, command, before)
		x.Lifecycle = &lifecycleMetadata{Provenance: 1}
		body, err := wire(x)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.PrepareAllocated(t.Context(), body, ids[:]); !errors.Is(err, errAllocationConsumed) {
			t.Fatal("old local candidate authorized NEW intent", err)
		}
		next, err := a.ReserveIDs(t.Context(), "after-loss", [32]byte{5}, 2, 0)
		if err != nil || next.Grant.First != grant.Grant.Last+1 {
			t.Fatal("burned range recycled", next, err)
		}
	})
	t.Run("MultipleGrantFactorySuccessorAndNativeAbort", func(t *testing.T) {
		h, source, catalog, ca, cb, a, b := ownerActualTransferFixture(t)
		reserves := []struct {
			name   string
			count  uint64
			bucket uint8
			secret [32]byte
		}{{"home-0", 32, 0, [32]byte{10}}, {"home-1", 14, 0, [32]byte{11}}, {"remote", 2, 1, [32]byte{12}}}
		var cursors [3]*graphOwnerAllocationCursor
		var originals [3]graphOwnerAllocationSession
		for i, spec := range reserves {
			g, err := ca.ReserveIDs(t.Context(), spec.name, spec.secret, spec.count, spec.bucket)
			if err != nil {
				t.Fatal(err)
			}
			again, err := ca.ReserveIDs(t.Context(), spec.name, spec.secret, spec.count, spec.bucket)
			if err != nil || !graphColumnSame(g, again) {
				t.Fatal("reservation changed", err)
			}
			client := ca
			if spec.bucket == 1 {
				client = cb
			}
			originals[i], err = client.AllocationSession(t.Context(), spec.name, "session-"+spec.name, spec.secret, [32]byte{byte(20 + i)})
			if err != nil {
				t.Fatal(err)
			}
			cursors[i], _, err = client.InstallIDs(t.Context(), originals[i])
			if err != nil {
				t.Fatal(err)
			}
		}
		claims := map[uint64]graphOwnerAllocatedID{}
		add := func(pair [2]graphOwnerAllocatedID) {
			for _, id := range pair {
				if _, exists := claims[id.ID]; exists {
					t.Fatal("duplicate local issuance", id)
				}
				claims[id.ID] = id
			}
		}
		n0, ids, err := cursors[0].Node()
		if err != nil {
			t.Fatal(err)
		}
		add(ids)
		n1, ids, err := cursors[0].Node()
		if err != nil {
			t.Fatal(err)
		}
		add(ids)
		five, err := cursors[0].Next()
		if err != nil {
			t.Fatal(err)
		}
		claims[five.ID] = five
		six, err := cursors[0].Next()
		if err != nil {
			t.Fatal(err)
		}
		claims[six.ID] = six
		n2, ids, err := cursors[2].Node()
		if err != nil {
			t.Fatal(err)
		}
		add(ids)
		nodes := [3]endpoint{n0, n1, n2}
		if nodes != h.config.Cost.nodes() || five.ID != 5 || six.ID != 6 {
			t.Fatal("actual grants differ from fresh fixture declaration", nodes, five, six)
		}
		factory := func(id string, command graphCommand, life, provenance uint64, client *graphOwnerClient) graphOwnerIntent {
			t.Helper()
			before, _, err := h.graphCostPointSnapshot(command)
			if err != nil {
				t.Fatal(err)
			}
			coord := uint8(0)
			if command.Kind == "node-create" {
				coord = command.Node.Owner
			}
			x := h.graphTransaction(id, coord, command, before)
			x.Lifecycle = &lifecycleMetadata{Provenance: provenance}
			if command.Kind != "node-create" {
				def := five
				if command.Edge.Value == 6 {
					def = six
				}
				x.Lifecycle.EdgeLife = life
				x.Lifecycle.Definition = &lifecycleDefinition{ID: def.ID, Kind: "i64", I64: command.Edge.Value}
			}
			ids, err := ownerCommandIDs(x)
			if err != nil {
				t.Fatal(err)
			}
			proofs := make([]graphOwnerAllocatedID, 0, len(ids))
			for _, id := range ids {
				proof, exists := claims[id]
				if !exists {
					t.Fatal("factory used non-Next ID", id)
				}
				proofs = append(proofs, proof)
			}
			body, err := wire(x)
			if err != nil {
				t.Fatal(err)
			}
			intent, err := client.PrepareAllocated(t.Context(), body, proofs)
			if err != nil {
				t.Fatal(err)
			}
			return intent
		}
		commit := func(client *graphOwnerClient, intent graphOwnerIntent) outcome {
			t.Helper()
			o, _, err := client.Submit(t.Context(), intent)
			if err != nil || !o.Commit {
				t.Fatal(o, err)
			}
			return o
		}
		for i, node := range nodes {
			client := ca
			if node.Owner == 1 {
				client = cb
			}
			commit(client, factory(fmt.Sprintf("node-%d", i), graphCommand{Kind: "node-create", Node: node}, 0, uint64(i+1), client))
		}
		var first graphOwnerIntent
		var firstResult outcome
		for fact := uint64(0); fact < h.config.Cost.Facts; fact++ {
			cursor := cursors[0]
			if fact >= 13 {
				cursor = cursors[1]
			}
			edge, life, ids, err := cursor.Edge(n0, n2, 5)
			if err != nil {
				t.Fatal(err)
			}
			add(ids)
			intent := factory(fmt.Sprintf("create-%d", fact), graphCommand{Kind: "edge-create", Edge: edge}, life, 4+fact, ca)
			o := commit(ca, intent)
			if fact == 0 {
				first, firstResult = intent, o
			}
		}
		old := mustCut(t, h, []uint8{0, 1}, nil)
		update := factory("before-move", graphCommand{Kind: "edge-update", Edge: graphEdge{ID: 7, Source: n0, Target: n2, Value: 6}}, 8, 4+h.config.Cost.Facts, ca)
		move, err := catalog.ReserveMove(t.Context(), a, ca.routes[0], "move", [32]byte{40}, 0, b.ID, old)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := catalog.Freeze(t.Context(), a, move); !errors.Is(err, errHistoryInUse) {
			t.Fatal("live native allocation session ignored", err)
		}
		for _, cursor := range cursors {
			if _, err := cursor.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := cursors[0].Next(); !errors.Is(err, errStale) {
			t.Fatal("closed local session", err)
		}
		if _, err := catalog.Freeze(t.Context(), a, move); err != nil {
			t.Fatal(err)
		}
		if err := source.begin("gc-frozen", old); !errors.Is(err, errHistoryInUse) {
			t.Fatal(err)
		}
		if _, err := catalog.Activate(t.Context(), b, move); err != nil {
			t.Fatal(err)
		}
		if _, err := ca.ReserveIDs(t.Context(), "home-0", reserves[0].secret, 32, 0); !errors.Is(err, errStale) {
			t.Fatal("old HOME recovered authority", err)
		}
		if _, _, err := ca.InstallIDs(t.Context(), originals[0]); !errors.Is(err, errStale) {
			t.Fatal("old install authority", err)
		}
		if _, _, err := ca.ResolveFenced(t.Context(), update); !errors.Is(err, errStale) {
			t.Fatal("source invented successor ABORT", err)
		}
		target, err := catalog.Client(t.Context(), b)
		if err != nil {
			t.Fatal(err)
		}
		// If COMMIT won before fencing, resolution recovers it unchanged.
		if o, _, err := target.ResolveFenced(t.Context(), first); err != nil || o != firstResult {
			t.Fatal("overwrote existing COMMIT", o, err)
		}
		aborted, _, err := target.ResolveFenced(t.Context(), update)
		if err != nil || aborted.Commit || aborted.Reason != "owner-epoch" {
			t.Fatal("no genuine fenced ABORT", aborted, err)
		}
		if o, _, err := target.ResolveFenced(t.Context(), update); err != nil || o != aborted {
			t.Fatal("ABORT replay changed", o, err)
		}
		proof, _, err := target.Original(t.Context(), update)
		if err != nil || proof.Outcome != aborted || len(proof.Original.Value) != 0 {
			t.Fatal("ABORT emitted CDC", proof, err)
		}
		fresh := factory("successor-update", graphCommand{Kind: "edge-update", Edge: graphEdge{ID: 7, Source: n0, Target: n2, Value: 6}}, 8, 4+h.config.Cost.Facts, target)
		commit(target, fresh)
		// Old issuance provenance still authenticates existing IDs; no old
		// processing epoch is substituted into actual certificate/storage epochs.
		stored, err := target.AllocatedID(t.Context(), 7)
		if err != nil || stored != claims[7] {
			t.Fatal("lost original grant lineage", stored, err)
		}
		next, err := target.ReserveIDs(t.Context(), "target-new", [32]byte{41}, 2, 0)
		if err != nil || next.Grant.First != 49 || next.Grant.Recipient != (graphOwnerRoute{b.ID, 2}) {
			t.Fatal("target high-water/owner", next, err)
		}
		if _, err := target.CloseReservedIDs(t.Context(), "target-new", [32]byte{41}); err != nil {
			t.Fatal(err)
		}
		if _, err := target.Prepare(t.Context(), fresh.Body); !errors.Is(err, errInvalid) {
			t.Fatal("fresh factory omitted allocation proof", err)
		}
		bad := fresh
		bad.Allocated = slices.Clone(fresh.Allocated)
		bad.Allocated[0].Offset++
		if _, _, err := target.Submit(t.Context(), bad); !errors.Is(err, errMismatch) {
			t.Fatal("substituted issuance proof", err)
		}
		if !bytes.Equal(old.record.Scope, []byte{0, 1}) || old.record.Epochs != h.config.Epochs {
			t.Fatal("processing epoch relabelled source cut")
		}
	})
}
