package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Authored guards only: none of these rows is SDK/runtime evidence.
func TestGraphFeedAdmissionAndImmutableOriginalGuards(t *testing.T) {
	p := graphFeedPolicyFor(graphCostProfile{Version: 5, Facts: 40, CrossPercent: 50, Writers: 8})
	if err := p.validate(); err != nil {
		t.Fatal(err)
	}
	bad := p
	bad.PageRows = 129
	if err := bad.validate(); !errors.Is(err, errInvalid) {
		t.Fatal("unbounded native page", err)
	}
	bad = p
	bad.PageBytes = 100001
	if err := bad.validate(); !errors.Is(err, errInvalid) {
		t.Fatal("native value quota hidden by page policy", err)
	}
	original := graphFeedOriginal{Version: 1, Request: "fixture", Secret: [32]byte{1}, Policy: p}
	if err := original.validate(); !errors.Is(err, errInvalid) {
		t.Fatal("encoded identity without genuine cut/owners admitted", err)
	}
	var absent *graphFeed
	if _, err := absent.Page(t.Context(), "one", certificate{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil feed published empty EOF", err)
	}
	if err := absent.Close(); !errors.Is(err, errInvalid) {
		t.Fatal("nil feed close", err)
	}
	if absent.Work() != (graphSnapshotWork{}) {
		t.Fatal("nil feed invented work")
	}
}

// Direct real-SDK API composition, authored only. A historical snapshot must
// remain5 after mutation6; the complete committed group replays byte-for-byte
// until authenticated ACK, including after a local handle Close/reopen.
func TestGraphFeedActualSnapshotPageAckAndRetainedOwner(t *testing.T) {
	p := graphCostProfile{Version: 1, Facts: 20, CrossPercent: 50, Writers: 1}
	h := fixture(t, graphCostConfig("feed-direct-"+rand.Text(), p))
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
	policy := graphSelectorsPolicy(h.config)
	source, err := newGraphHistoryRetention(h, initial, policy.History)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newGraphFeedStore(source, graphFeedPolicyFor(p))
	if err != nil {
		t.Fatal(err)
	}
	apply := func(phase string, fact uint64) {
		t.Helper()
		err := h.graphSelectorsApply(t.Context(), phase, fact, 0, func(e graphCostParallelEvent) {
			if e.Attempt != nil && e.Attempt.Classification != "committed" {
				t.Fatal("tiny direct attempt", e.Attempt)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := uint64(0); i < 3; i++ {
		apply("setup", i)
	}
	apply("create", 0)
	secret, err := newGraphFeedSecret()
	if err != nil {
		t.Fatal(err)
	}
	original, err := store.Begin(t.Context(), "direct", secret)
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.Begin(t.Context(), "direct", secret)
	if err != nil || !graphColumnSame(original, again) {
		t.Fatal("Begin selected a different original", err)
	}
	snapshot, token, err := store.Publish(t.Context(), original)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := snapshot.Close(); err != nil {
			t.Error(err)
		}
	})
	apply("update", 0)
	final := mustCut(t, h, []uint8{0, 1}, nil)
	rows := 0
	end, err := snapshot.Scan(t.Context(), func(row graphSnapshotRow) error {
		want := int64(5)
		if bytes.HasPrefix([]byte(row.Key), []byte("node/")) {
			want = 1
		}
		if row.Value.Value != want {
			return errMismatch
		}
		rows++
		return nil
	})
	if err != nil || rows != 6 || end.Rows[0]+end.Rows[1] != 6 {
		t.Fatal("snapshot became current/phantom", end, rows, err)
	}
	f, err := store.Open(t.Context(), token, "reader-one")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	})
	page, err := f.Page(t.Context(), "page-one", final.record)
	if err != nil || !page.Page.EOF || len(page.Page.Rows) != 1 {
		t.Fatal("missing complete update", page, err)
	}
	if page.Page.From != original.Cut.Groups || page.Page.Next != final.record.Groups {
		t.Fatal("wrong native boundary", page)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(t.Context(), token, "reader-two")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	replay, err := reopened.Page(t.Context(), "page-one", final.record)
	if err != nil || !replay.Replay || !bytes.Equal(page.Record.Value, replay.Record.Value) {
		t.Fatal("pending-page replay changed", err)
	}
	if _, err := reopened.Ack(t.Context(), "wrong-page", sha256.Sum256([]byte("foreign"))); !errors.Is(err, errMismatch) {
		t.Fatal("wrong ACK authority", err)
	}
	committedAck, err := reopened.Ack(t.Context(), "page-one", sha256.Sum256(page.Record.Value))
	if err != nil || !sameCertificate(committedAck.Ack.Completed, final.record) {
		t.Fatal("complete update ACK", err)
	}
	// An independent cursor at the final cut exercises genuine exact-empty.
	secondSecret, err := newGraphFeedSecret()
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Begin(t.Context(), "second", secondSecret)
	if err != nil {
		t.Fatal(err)
	}
	secondSnapshot, secondToken, err := store.Publish(t.Context(), second)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	secondFeed, err := store.Open(t.Context(), secondToken, "second-reader")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := secondFeed.Close(); err != nil {
			t.Error(err)
		}
	})
	empty, err := secondFeed.Page(t.Context(), "empty", second.Cut)
	if err != nil || !empty.Page.EOF || len(empty.Page.Rows) != 0 {
		t.Fatal("exact empty confused with unavailable", err)
	}
	ack, err := secondFeed.Ack(t.Context(), "empty", sha256.Sum256(empty.Record.Value))
	if err != nil || !sameCertificate(ack.Ack.Completed, second.Cut) {
		t.Fatal("ACK did not advance exact completed vector", err)
	}
	duplicate, err := secondFeed.Ack(t.Context(), "empty", sha256.Sum256(empty.Record.Value))
	if err != nil || !bytes.Equal(ack.Record.Value, duplicate.Record.Value) {
		t.Fatal("ACK replay mutated original", err)
	}
	// Ordinary quota exhaustion is an authored admission guard, not a measured
	// capacity result. It cannot consume either already-funded terminal lane.
	_, err = h.db.Transact(func(tr fdb.Transaction) (any, error) {
		c, err := store.cursorIn(tr, token)
		if err != nil {
			return nil, err
		}
		c.Calls = store.binding.Policy.MaxCalls
		c.WorkRows = store.binding.Policy.MaxWorkRows
		c.WorkBytes = store.binding.Policy.MaxWorkBytes
		return nil, put(tr, store.key("cursor", token.Request), c)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Page(t.Context(), "quota-refused", final.record); !errors.Is(err, errLimit) {
		t.Fatal("ordinary quota not explicit", err)
	}
	if err := store.Finish(t.Context(), token, "expire", true); err != nil {
		t.Fatal(err)
	}
	// Cleanup-only recovery is authorized by the same durable factory ticket,
	// even when the caller lost its local handle and the pin is expired. It does
	// not reacquire or create a replacement identity, and spent credit persists.
	before, _, err := graphCostParallelRead(t.Context(), h.db, func(tr fdb.ReadTransaction) (graphFeedCleanup, error) {
		return store.cleanupIn(tr, token, reopened.reader.id.Request)
	})
	if err != nil {
		t.Fatal(err)
	}
	work, err := store.CloseOriginal(t.Context(), token, reopened.reader.id.Request)
	if err != nil || work.CleanupBytes == 0 {
		t.Fatal("exhausted normal traffic stranded original reader", work, err)
	}
	againWork, err := store.CloseOriginal(t.Context(), token, reopened.reader.id.Request)
	if err != nil || againWork.CleanupRows != work.CleanupRows+1024 || againWork.CleanupBytes != work.CleanupBytes+(64<<20) {
		t.Fatal("cleanup replay reset spent ticket", err)
	}
	lostHandleWork, err := store.CloseReader(t.Context(), token, "reader-two")
	if err != nil || lostHandleWork.CleanupRows != againWork.CleanupRows+1024 {
		t.Fatal("expired-pin cleanup required a returned/acquired handle", err)
	}
	if _, err := store.CloseReader(t.Context(), token, "foreign-reader"); !errors.Is(err, errUnavailable) {
		t.Fatal("missing original became new owner authority", err)
	}
	after, _, err := graphCostParallelRead(t.Context(), h.db, func(tr fdb.ReadTransaction) (graphFeedCleanup, error) {
		return store.cleanupIn(tr, token, reopened.reader.id.Request)
	})
	if err != nil || !after.Closed || after.Calls != before.Calls+3 || after.Owner.Request != before.Owner.Request || after.DeadlineNanos <= 0 || after.DeadlineNanos > after.OuterDeadlineNanos {
		t.Fatal("lost immutable recovery ticket/deadline", after, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if got := reopened.Work(); got.CleanupRows != lostHandleWork.CleanupRows+1024 {
		t.Fatal("local cleanup handle reset durable work", got, lostHandleWork)
	}
	if _, err := snapshot.Points(t.Context(), []graphSnapshotKey{{0, edgeKey("edge", p.edge(0, 5))}, {0, edgeKey("edge", p.edge(19, 5))}}); err != nil {
		t.Fatal("expiry invalidated held snapshot", err)
	}
	if err := source.begin("blocked-"+fmt.Sprint(final.record.Round), final); !errors.Is(err, errHistoryInUse) {
		t.Fatal("held owner failed to protect physical history", err)
	}
	if err := store.Finish(t.Context(), secondToken, "release", false); err != nil {
		t.Fatal(err)
	}
	if absent, err := store.Open(t.Context(), secondToken, "released-new"); absent != nil || !errors.Is(err, errHistoryExpired) {
		t.Fatal("released pin created new authority", err)
	}
}
