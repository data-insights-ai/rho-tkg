package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Authored only. Retained-feed identity is distinct from physical-source owner
// authority, and a failed funded Close preserves its exact recoverable identity.
func TestGraphRetainedFeedIdentityAndLifetimeGuards(t *testing.T) {
	var store *graphFeedStore
	if _, err := store.OpenRetained(t.Context(), graphFeedToken{}, "reader"); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := store.ToRetainedOnly(t.Context(), graphFeedToken{}, "release"); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := store.CloseRetained(t.Context(), graphFeedToken{}, "reader"); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	token := graphFeedToken{Request: "original", Secret: [32]byte{1}, Original: [32]byte{2}}
	id := retainedFeedIdentity(token, "reader")
	if id == retainedFeedIdentity(token, "other") || id.Original != token.Original {
		t.Fatal("reader identity aliases", id)
	}
	run := [16]byte{1}
	cfg := graphOwnerTransferConfig(graphFeedWorkloadSpec{Version: 1, Family: graphOwnerTransferFamily, Facts: 20, CrossPercent: 100, Writers: 1, Layout: "kv"}, fmt.Sprintf("%x", run))
	p := ownerDirectTransferPolicy(cfg, run)
	p.AdmittedUnixNanos -= int64(240 * time.Second)
	p.DeadlineUnixNanos = p.AdmittedUnixNanos + int64(180*time.Second)
	token.DeadlineNanos = p.DeadlineUnixNanos
	s := &graphFeedStore{binding: graphFeedBinding{Retention: &graphOwnerBinding{Transfer: &p}}}
	r := &graphFeedRetainedReader{s: s, token: token, id: retainedFeedIdentity(token, "reader")}
	err := r.closeWith(t.Context())
	pending, ok := errors.AsType[*graphFeedRetainedPending](err)
	if !ok || !errors.Is(err, context.DeadlineExceeded) || pending.Identity() != r.id {
		t.Fatal("lost same cleanup identity", err)
	}
	if err := pending.Close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("replacement reset lifetime", err)
	}
	if r.work() != (graphSnapshotWork{}) {
		t.Fatal("pre-admission failure claimed durable credit")
	}
	if err := r.enter(); !errors.Is(err, errHistoryExpired) {
		t.Fatal("failed close renewed authority", err)
	}
}

// Direct genuine SDK composition, AUTHORED ONLY: mutation5->6, old exact set,
// physical-owner refusal, authenticated mode transition, actual physical GC,
// complete original/page replay+ACK and cleanup-only replacement. No runtime,
// actual child-loss, matched native retention or transfer-matrix claim.
func TestGraphRetainedFeedActualPhysicalGCAndSameOriginalReplay(t *testing.T) {
	h, source, catalog, ca, cb, _, _ := ownerActualTransferFixture(t)
	grants := []struct {
		name   string
		count  uint64
		bucket uint8
	}{{"feed-home", 32, 0}, {"feed-home-tail", 14, 0}, {"feed-remote", 2, 1}}
	var cursors [3]*graphOwnerAllocationCursor
	for i, g := range grants {
		secret := [32]byte{byte(10 + i)}
		if _, err := ca.ReserveIDs(t.Context(), g.name, secret, g.count, g.bucket); err != nil {
			t.Fatal(err)
		}
		client := ca
		if g.bucket == 1 {
			client = cb
		}
		o, err := client.AllocationSession(t.Context(), g.name, "session-"+g.name, secret, [32]byte{byte(20 + i)})
		if err != nil {
			t.Fatal(err)
		}
		cursors[i], _, err = client.InstallIDs(t.Context(), o)
		if err != nil {
			t.Fatal(err)
		}
	}
	claims := map[uint64]graphOwnerAllocatedID{} // Fixed ten-ID direct fixture only.
	add := func(ids [2]graphOwnerAllocatedID) {
		for _, id := range ids {
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
	edge, life, ids, err := cursors[0].Edge(n0, n2, 5)
	if err != nil {
		t.Fatal(err)
	}
	add(ids)
	apply := func(id string, command graphCommand, provenance uint64, client *graphOwnerClient) graphOwnerIntent {
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
			def := five
			if command.Edge.Value == 6 {
				def = six
			}
			x.Lifecycle.EdgeLife = life
			x.Lifecycle.Definition = &lifecycleDefinition{ID: def.ID, Kind: "i64", I64: command.Edge.Value}
		}
		needed, err := ownerCommandIDs(x)
		if err != nil {
			t.Fatal(err)
		}
		proof := make([]graphOwnerAllocatedID, 0, len(needed))
		for _, id := range needed {
			claim, found := claims[id]
			if !found {
				t.Fatal("factory without actual Next", id)
			}
			proof = append(proof, claim)
		}
		body, err := wire(x)
		if err != nil {
			t.Fatal(err)
		}
		intent, err := client.PrepareAllocated(t.Context(), body, proof)
		if err != nil {
			t.Fatal(err)
		}
		o, _, err := client.Submit(t.Context(), intent)
		if err != nil || !o.Commit {
			t.Fatal(o, err)
		}
		return intent
	}
	for i, node := range []endpoint{n0, n1, n2} {
		client := ca
		if node.Owner == 1 {
			client = cb
		}
		apply(fmt.Sprintf("node-%d", i), graphCommand{Kind: "node-create", Node: node}, uint64(i+1), client)
	}
	created := apply("create", graphCommand{Kind: "edge-create", Edge: edge}, 4, ca)
	s, err := newGraphRetainedFeedStore(t.Context(), catalog, graphFeedPolicyFor(h.config.Cost))
	if err != nil {
		t.Fatal(err)
	}
	defer s.stopNormal()
	original, err := s.Begin(t.Context(), "retained", [32]byte{40})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, token, err := s.Publish(t.Context(), original)
	if err != nil {
		t.Fatal(err)
	}
	physical, err := s.Open(t.Context(), token, "physical")
	if err != nil {
		t.Fatal(err)
	}
	updated := edge
	updated.Value = 6
	apply("update", graphCommand{Kind: "edge-update", Edge: updated}, 4+h.config.Cost.Facts, ca)
	final := mustCut(t, h, []uint8{0, 1}, nil)
	seen := map[graphSnapshotKey]value{}
	expected := map[graphSnapshotKey]int64{}
	for _, n := range []endpoint{n0, n1, n2} {
		expected[graphSnapshotKey{Group: n.Owner, Key: lifeKey(n)}] = 1
	}
	for _, kind := range []string{"edge", "out", "in"} {
		g := uint8(0)
		if kind == "in" {
			g = n2.Owner
		}
		expected[graphSnapshotKey{Group: g, Key: edgeKey(kind, edge)}] = 5
	}
	end, err := snapshot.Scan(t.Context(), func(row graphSnapshotRow) error {
		key := graphSnapshotKey{Group: row.Group, Key: row.Key}
		if _, duplicate := seen[key]; duplicate {
			return errMismatch
		}
		seen[key] = row.Value
		want, found := expected[key]
		if !found || row.Value.Value != want {
			return errMismatch
		}
		return nil
	})
	if err != nil || len(seen) != 6 || end.Rows[0]+end.Rows[1] != 6 {
		t.Fatal("old became current/partial", end, err)
	}
	points, err := snapshot.Points(t.Context(), []graphSnapshotKey{{Group: 0, Key: "node/999999"}})
	if err != nil || len(points) != 1 || points[0].Found {
		t.Fatal("phantom", points, err)
	}
	if _, err := s.ToRetainedOnly(t.Context(), token, "release"); !errors.Is(err, errHistoryInUse) {
		t.Fatal("unclosed snapshot released", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ToRetainedOnly(t.Context(), token, "release"); !errors.Is(err, errHistoryInUse) {
		t.Fatal("held physical reader lost", err)
	}
	if err := source.begin("blocked", final); !errors.Is(err, errHistoryInUse) {
		t.Fatal("old physical pin did not block GC", err)
	}
	if err := physical.Close(); err != nil {
		t.Fatal(err)
	}
	record, err := s.ToRetainedOnly(t.Context(), token, "release")
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.ToRetainedOnly(t.Context(), token, "release")
	if err != nil || !bytes.Equal(record.Value, again.Value) {
		t.Fatal("transition original changed", err)
	}
	if _, err := s.ToRetainedOnly(t.Context(), token, "foreign"); !errors.Is(err, errMismatch) {
		t.Fatal("transition identity changed", err)
	}
	if _, _, err := s.Publish(t.Context(), original); !errors.Is(err, errHistoryExpired) {
		t.Fatal("snapshot reopened after relinquishment", err)
	}
	if _, err := s.Open(t.Context(), token, "late-physical"); !errors.Is(err, errHistoryExpired) {
		t.Fatal("physical pin reacquired", err)
	}
	f, err := s.OpenRetained(t.Context(), token, "first")
	if err != nil {
		t.Fatal(err)
	}
	if err := source.begin("retire", final); err != nil {
		t.Fatal(err)
	}
	var page graphHistoryPage
	for range 64 {
		page, err = source.collect(t.Context(), "retire", graphHistoryBudget{Rows: 2, Bytes: 128 << 10, OutputBytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if page.Done {
			break
		}
	}
	if !page.Done || page.Seal.Deleted[0]+page.Seal.Deleted[1] != 3 || page.Seal.Anchors[0]+page.Seal.Anchors[1] != 6 {
		t.Fatal("not actual physical history collection", page)
	}
	if _, err := h.at(cut{original.Cut}, 2, nil); !errors.Is(err, errHistoryExpired) {
		t.Fatal("deleted old cut silently reopened", err)
	}
	current, err := h.at(final, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Values[0])+len(current.Values[1]) != len(expected) {
		t.Fatal("post-GC current partial/phantom", current)
	}
	for key, v := range seen {
		want := int64(6)
		if expected[key] == 1 {
			want = 1
		}
		got, found := current.Values[key.Group][key.Key]
		if !found || got.Value != want || got.Version < v.Version {
			t.Fatal("current anchor differs from original lineage", key, got)
		}
	}
	delivery, err := f.Page(t.Context(), "update-page", final.record)
	if err != nil || !delivery.Page.EOF || len(delivery.Page.Rows) != 1 {
		t.Fatal("CDC gap after physical GC", delivery, err)
	}
	group, err := decode[changeGroup](delivery.Page.Rows[0].Value)
	if err != nil || len(group.Writes) != 3 {
		t.Fatal("partial transaction group", group, err)
	}
	for _, write := range group.Writes {
		if write.Before == nil || write.Before.Value != 5 || write.After.Value != 6 {
			t.Fatal("lost exact before/after", write)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := s.OpenRetained(t.Context(), token, "replacement")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := reopened.Page(t.Context(), "update-page", final.record)
	if err != nil || !replay.Replay || !bytes.Equal(replay.Record.Value, delivery.Record.Value) {
		t.Fatal("same pending page changed", err)
	}
	if _, err := reopened.Ack(t.Context(), "update-page", sha256.Sum256([]byte("foreign"))); !errors.Is(err, errMismatch) {
		t.Fatal("foreign ACK", err)
	}
	ack, err := reopened.Ack(t.Context(), "update-page", sha256.Sum256(delivery.Record.Value))
	if err != nil || ack.Cursor.Positions != final.record.Groups {
		t.Fatal("wrong complete boundary", ack, err)
	}
	if _, _, err := ca.Submit(t.Context(), created); err != nil {
		t.Fatal("original COMMIT lost to GC", err)
	}
	if err := s.Finish(t.Context(), token, "expire", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OpenRetained(t.Context(), token, "after-expiry"); !errors.Is(err, errHistoryExpired) {
		t.Fatal("expired feed renewed", err)
	}
	work, err := s.CloseRetained(t.Context(), token, "replacement")
	if err != nil || work.CleanupRows != 1024 {
		t.Fatal("same cleanup recovery", work, err)
	}
	if err := reopened.Close(); err != nil || reopened.Work() != work {
		t.Fatal("cleanup counters reset", reopened.Work(), err)
	}
	_, _, err = graphCostParallelRead(t.Context(), h.db, func(tr fdb.ReadTransaction) (bool, error) {
		for _, request := range []string{"first", "replacement"} {
			r, err := s.retainedReaderIn(tr, token, retainedFeedIdentity(token, request))
			if err != nil {
				return false, err
			}
			if r.State != 2 || r.Cleanup.Calls != 1 {
				return false, errMismatch
			}
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
