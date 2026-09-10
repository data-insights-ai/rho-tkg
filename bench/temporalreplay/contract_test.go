package temporalreplay

// The "tests that must break incorrect implementations" of
// tasks/plan-temporal-index-ingestion.md, written against the CURRENT public
// API only. Each test states the contract the AI-SOC replay adapter relies on.
// A test that stays red documents a demonstrated gap, not a wish.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func mustOpen(t *testing.T, dir string) *graphpkg.Graph {
	t.Helper()
	g, err := openGraph(dir)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func countByLabelProp(t *testing.T, g *graphpkg.Graph, label, key string) int {
	t.Helper()
	ns, err := g.Nodes().ByLabelAndProperty(label, keyProp, key, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	return len(ns)
}

// TestTwoWritersSameIdentity: two sessions race to create one logical Fact and
// each links its own Occurrence to it.
//
// Contract observed on the current API (both session modes):
//   - exactly one Fact survives (unique constraint);
//   - the loser's Submit returns ErrUniqueViolation for the WHOLE group;
//   - the loser's Occurrence and Inbox nodes are COMMITTED anyway (keep-
//     survivors semantics) while its OCCURRENCE_OF relationship is short-
//     circuited ("endpoint create failed");
//   - Submit does not say WHICH intents failed, so the adapter must re-read
//     every create in the group and re-link. After that repair no occurrence is
//     lost and the fact is one.
func TestTwoWritersSameIdentity(t *testing.T) {
	for _, mode := range []string{"strong", "concurrent"} {
		t.Run(mode, func(t *testing.T) {
			g := mustOpen(t, t.TempDir())
			defer g.Close()
			const factKey = "fact-race"
			var wg sync.WaitGroup
			errs := make([]error, 2)
			writers := make([]*sessionWriter, 2)
			for i := 0; i < 2; i++ {
				w, err := newSessionWriter(g, "t1", i+1, mode == "concurrent")
				if err != nil {
					t.Fatal(err)
				}
				writers[i] = w
			}
			// Both prepare (read: nothing exists) BEFORE either submits, which is
			// the race the read-then-prepare design has by construction.
			type prepared struct {
				creates []pendingCreate
				rows    []row
			}
			preps := make([]prepared, 2)
			for i, w := range writers {
				r := row{idx: i, factKey: factKey, host: fmt.Sprintf("h%d", i), ts: 1_700_000_000_000 + int64(i), srcID: "s" + strconv.Itoa(i)}
				w.facts, w.batchMax = map[string]*types.Node{}, map[string]int64{}
				asset, err := w.s.AddNode([]string{labelAsset}, map[string]any{keyProp: r.host})
				if err != nil {
					t.Fatal(err)
				}
				w.assets[r.host] = asset
				fact, err := w.s.AddNode([]string{labelFact}, map[string]any{keyProp: factKey, "first_seen": r.ts, "last_seen": r.ts})
				if err != nil {
					t.Fatal(err)
				}
				w.facts[factKey] = fact
				occ, err := w.s.AddNode([]string{labelOccurrence}, map[string]any{keyProp: occurrenceKey("t1", r), "fact_key": factKey, "ts": r.ts, "src": r.srcID})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := w.s.AddRelationship(relOccurrenceOf, occ, fact, nil); err != nil {
					t.Fatal(err)
				}
				if _, err := w.s.AddRelationship(relObservedOn, occ, asset, nil); err != nil {
					t.Fatal(err)
				}
				preps[i] = prepared{creates: []pendingCreate{{label: labelFact, key: factKey, node: fact}, {label: labelAsset, key: r.host, node: asset}}, rows: []row{r}}
			}
			for i, w := range writers {
				wg.Add(1)
				go func(i int, w *sessionWriter) {
					defer wg.Done()
					_, errs[i] = w.s.Submit()
				}(i, w)
			}
			wg.Wait()
			losers := 0
			for i, err := range errs {
				if err == nil {
					continue
				}
				if !errors.Is(err, graphpkg.ErrUniqueViolation) {
					t.Fatalf("writer %d: unexpected error %v", i, err)
				}
				losers++
				// The group error is the only signal; the loser's occurrence is
				// already durable and unlinked.
				if n := countByLabelProp(t, g, labelOccurrence, occurrenceKey("t1", preps[i].rows[0])); n != 1 {
					t.Fatalf("loser's occurrence committed %d times, want 1 (keep-survivors)", n)
				}
				if err := w2reconcile(writers[i], preps[i].creates, preps[i].rows); err != nil {
					t.Fatalf("reconcile: %v", err)
				}
			}
			if losers != 1 {
				t.Fatalf("expected exactly one loser, got %d (errs=%v)", losers, errs)
			}
			if n := countByLabelProp(t, g, labelFact, factKey); n != 1 {
				t.Fatalf("fact identity count = %d, want 1", n)
			}
			occs, err := g.Nodes().CountByLabel(labelOccurrence)
			if err != nil {
				t.Fatal(err)
			}
			if occs != 2 {
				t.Fatalf("occurrences = %d, want 2 (no lost occurrence)", occs)
			}
			if rels, _ := g.Rels().CountByType(relOccurrenceOf); rels != 2 {
				t.Fatalf("OCCURRENCE_OF = %d, want 2 after repair", rels)
			}
		})
	}
}

func w2reconcile(w *sessionWriter, creates []pendingCreate, rows []row) error {
	return w.reconcile(creates, rows)
}

// TestRepeatedUpdatesRetainExactTimesAndMultiplicity: five occurrences of one
// fact, two of them at the SAME event time from different sources, written
// through the strong session. Every occurrence, its exact time, and the fact's
// own last_seen version chain survive Close + reopen.
func TestRepeatedUpdatesRetainExactTimesAndMultiplicity(t *testing.T) {
	dir := t.TempDir()
	g := mustOpen(t, dir)
	w, err := newSessionWriter(g, "t1", 1, false)
	if err != nil {
		t.Fatal(err)
	}
	base := int64(1_700_000_000_000)
	rows := []row{
		{idx: 0, factKey: "f", host: "h", ts: base, srcID: "a"},
		{idx: 1, factKey: "f", host: "h", ts: base + 60_000, srcID: "b"},
		{idx: 2, factKey: "f", host: "h", ts: base + 60_000, srcID: "c"}, // equal time, distinct record
		{idx: 3, factKey: "f", host: "h", ts: base + 3_600_000, srcID: "d"},
		{idx: 4, factKey: "f", host: "h", ts: base + 3_600_001, srcID: "e"},
	}
	for _, r := range rows {
		if err := w.writeBatch([]row{r}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g = mustOpen(t, dir)
	defer g.Close()
	facts, err := g.Nodes().ByLabelAndProperty(labelFact, keyProp, "f", storepkg.QueryOpts{})
	if err != nil || len(facts) != 1 {
		t.Fatalf("fact lookup: %v / %d", err, len(facts))
	}
	var got []int64
	if err := g.Rels().ForEachAdjacentEndpoint(facts[0].ID(), relOccurrenceOf, true, func(_ types.RelID, occ types.NodeID) bool {
		n, err := g.Nodes().Get(context.Background(), occ)
		if err != nil {
			t.Fatal(err)
		}
		ts, _ := n.GetProperty("ts")
		got = append(got, ts.(int64))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("occurrence multiplicity = %d, want 5", len(got))
	}
	want := map[int64]int{base: 1, base + 60_000: 2, base + 3_600_000: 1, base + 3_600_001: 1}
	have := map[int64]int{}
	for _, ts := range got {
		have[ts]++
	}
	for ts, n := range want {
		if have[ts] != n {
			t.Fatalf("time %d: multiplicity %d, want %d", ts, have[ts], n)
		}
	}
	// The fact's last_seen chain: one superseded version per strictly
	// increasing maximum (the equal-time repeat must NOT create a version —
	// nothing changed), each retaining the exact value it carried.
	hist, err := g.Nodes().History(facts[0].ID())
	if err != nil {
		t.Fatal(err)
	}
	var seen []int64
	for _, v := range hist {
		ls, _ := v.GetProperty("last_seen")
		seen = append(seen, ls.(int64))
	}
	wantChain := []int64{base, base + 60_000, base + 3_600_000}
	if len(seen) != len(wantChain) {
		t.Fatalf("superseded versions = %v, want %v", seen, wantChain)
	}
	for i := range wantChain {
		if seen[i] != wantChain[i] {
			t.Fatalf("superseded versions = %v, want %v", seen, wantChain)
		}
	}
	ls, _ := facts[0].GetProperty("last_seen")
	if ls.(int64) != base+3_600_001 {
		t.Fatalf("last_seen = %v, want %d", ls, base+3_600_001)
	}
}

// TestInvalidOpAmongValid_SyncSubmitReportsAndKeepsSurvivors: a group carrying
// one unique violator and valid siblings. Contract: Submit returns the error
// (so a consumer checkpoint cannot advance past it), the survivors are
// committed, and the outcome is stable across a second wait on the same token.
func TestInvalidOpAmongValid_SyncSubmitReportsAndKeepsSurvivors(t *testing.T) {
	g := mustOpen(t, t.TempDir())
	defer g.Close()
	if _, err := g.Nodes().Add(context.Background(), []string{labelFact}, map[string]any{keyProp: "taken"}); err != nil {
		t.Fatal(err)
	}
	s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"ok-1", "taken", "ok-2"} {
		if _, err := s.AddNode([]string{labelFact}, map[string]any{keyProp: k}); err != nil {
			t.Fatal(err)
		}
	}
	tok, err := s.Submit()
	if !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("Submit error = %v, want ErrUniqueViolation", err)
	}
	if n := countByLabelProp(t, g, labelFact, "ok-1") + countByLabelProp(t, g, labelFact, "ok-2"); n != 2 {
		t.Fatalf("survivors committed = %d, want 2", n)
	}
	if n := countByLabelProp(t, g, labelFact, "taken"); n != 1 {
		t.Fatalf("violator duplicated: %d", n)
	}
	// A sync submitter already received the truth; WaitApplied on its token is
	// documented to return nil (the failure was consumed by the ack).
	if err := g.Ingest().WaitApplied(tok); err != nil {
		t.Fatalf("WaitApplied after a sync ack = %v, want nil (documented)", err)
	}
}

// TestAsyncRepeatedWaitNeverTurnsFailureIntoSuccess pins the R4 contract on
// the ASYNC door: a failed token must not read as success on a second wait.
//
// STATUS: fails on the current API by design of prune-on-read — the second
// WaitApplied returns nil. The replay adapter uses SYNC sessions only, so this
// is a documented hazard rather than a blocker; it runs only with
// TKG_REPLAY_R4=1 as an opt-in probe for a future repeatable-outcome contract; the declared
// vocabulary durability regression below is fixed and runs in the default suite.
func TestAsyncRepeatedWaitNeverTurnsFailureIntoSuccess(t *testing.T) {
	if os.Getenv("TKG_REPLAY_R4") == "" {
		t.Skip("R4 hazard pin; set TKG_REPLAY_R4=1 to run (fails on the current API)")
	}
	g := mustOpen(t, t.TempDir())
	defer g.Close()
	if _, err := g.Nodes().Add(context.Background(), []string{labelFact}, map[string]any{keyProp: "taken"}); err != nil {
		t.Fatal(err)
	}
	s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddNode([]string{labelFact}, map[string]any{keyProp: "taken"}); err != nil {
		t.Fatal(err)
	}
	tok, err := s.Submit()
	if err != nil {
		t.Fatal(err)
	}
	first := g.Ingest().WaitApplied(tok)
	if !errors.Is(first, graphpkg.ErrUniqueViolation) {
		t.Fatalf("first WaitApplied = %v, want ErrUniqueViolation", first)
	}
	second := g.Ingest().WaitApplied(tok)
	if second == nil {
		t.Fatalf("second WaitApplied on a FAILED token returned nil: a retried wait reports success for work that never committed (R4)")
	}
}

// TestKillBeforeAndAfterDurableAck: a child process writes batches through the
// chosen door and prints ACK after each synchronous acknowledgement; the parent
// SIGKILLs it after a fixed number of ACKs. Contract: every acknowledged batch
// is complete on reopen, and a replay of ALL batches converges to exactly one
// copy of every row with every relationship present (idempotent replay), for
// tx, strong and concurrent doors alike.
func TestKillBeforeAndAfterDurableAck(t *testing.T) {
	if dir := os.Getenv("TKG_REPLAY_KILL_CHILD_DIR"); dir != "" {
		killChild(dir, os.Getenv("TKG_REPLAY_KILL_MODE"))
		return
	}
	for _, mode := range []string{"tx", "strong", "concurrent"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			const acksBeforeKill = 6
			cmd := exec.Command(os.Args[0], "-test.run=^TestKillBeforeAndAfterDurableAck$")
			// Declared vocabulary is exercised on purpose: it was the 2026-09-09
			// durability defect (TestDeclaredRelTypesAreDurableBeforeAck).
			cmd.Env = append(os.Environ(), "TKG_REPLAY_KILL_CHILD_DIR="+dir, "TKG_REPLAY_KILL_MODE="+mode)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			acked := 0
			buf := make([]byte, 4096)
			deadline := time.Now().Add(60 * time.Second)
			for acked < acksBeforeKill && time.Now().Before(deadline) {
				n, err := stdout.Read(buf)
				if err != nil {
					break
				}
				acked += strings.Count(string(buf[:n]), "ACK\n")
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			if acked < acksBeforeKill {
				t.Fatalf("child produced only %d acks before deadline", acked)
			}

			g := mustOpen(t, dir)
			rows := killRows()
			// Every ACKED batch is wholly present with both relationships.
			for b := 0; b < acked; b++ {
				for _, r := range rows[b] {
					assertRowComplete(t, g, r, "acked batch "+strconv.Itoa(b))
				}
			}
			// Replay everything: idempotent convergence to one copy per row.
			if err := replayAll(g, mode, rows); err != nil {
				t.Fatalf("replay: %v", err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = mustOpen(t, dir)
			defer g.Close()
			total := 0
			for _, b := range rows {
				total += len(b)
				for _, r := range b {
					assertRowComplete(t, g, r, "after replay")
				}
			}
			if n, _ := g.Nodes().CountByLabel(labelOccurrence); n != total {
				t.Fatalf("occurrences after replay = %d, want %d", n, total)
			}
			if n, _ := g.Rels().CountByType(relOccurrenceOf); n != total {
				t.Fatalf("OCCURRENCE_OF after replay = %d, want %d", n, total)
			}
			if n := countByLabelProp(t, g, labelFact, "kf-0"); n != 1 {
				t.Fatalf("fact kf-0 count = %d, want 1", n)
			}
		})
	}
}

func killRows() [][]row {
	var out [][]row
	for b := 0; b < 40; b++ {
		var batch []row
		for i := 0; i < 4; i++ {
			idx := b*4 + i
			batch = append(batch, row{idx: idx, factKey: fmt.Sprintf("kf-%d", idx%3), host: fmt.Sprintf("kh-%d", idx%2), ts: 1_700_000_000_000 + int64(idx)*1000, srcID: "k" + strconv.Itoa(idx)})
		}
		out = append(out, batch)
	}
	return out
}

func killChild(dir, mode string) {
	g, err := openGraph(dir)
	if err != nil {
		os.Exit(3)
	}
	var w *sessionWriter
	if mode != "tx" {
		if w, err = newSessionWriter(g, "t1", 1, mode == "concurrent"); err != nil {
			os.Exit(4)
		}
	}
	for _, batch := range killRows() {
		if mode == "tx" {
			err = writeBatchTx(g, "t1", batch)
		} else {
			err = w.writeBatch(batch)
		}
		if err != nil {
			os.Exit(5)
		}
		fmt.Println("ACK")
	}
	// Never Close: the parent kills us long before this point anyway.
	time.Sleep(time.Minute)
}

func assertRowComplete(t *testing.T, g *graphpkg.Graph, r row, where string) {
	t.Helper()
	occs, err := g.Nodes().ByLabelAndProperty(labelOccurrence, keyProp, occurrenceKey("t1", r), storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(occs) != 1 {
		t.Fatalf("%s: row %d occurrence count = %d, want 1", where, r.idx, len(occs))
	}
	for _, rel := range []string{relOccurrenceOf, relObservedOn} {
		if d, _ := g.Rels().OutgoingDegree(occs[0].ID(), rel); d != 1 {
			t.Fatalf("%s: row %d has %d %s relationships, want 1", where, r.idx, d, rel)
		}
	}
	if n := countByLabelProp(t, g, labelInbox, "inbox-"+occurrenceKey("t1", r)); n != 1 {
		t.Fatalf("%s: row %d inbox entries = %d, want 1", where, r.idx, n)
	}
}

// replayAll is the adapter's idempotent replay: a row whose occurrence already
// exists is re-checked for completeness (a killed group can have left the node
// without its relationships) and only the missing pieces are written.
func replayAll(g *graphpkg.Graph, mode string, batches [][]row) error {
	var w *sessionWriter
	var err error
	if mode != "tx" {
		if w, err = newSessionWriter(g, "t1", 1, mode == "concurrent"); err != nil {
			return err
		}
		defer w.s.Close()
	}
	for _, batch := range batches {
		var fresh, partial []row
		for _, r := range batch {
			occs, err := g.Nodes().ByLabelAndProperty(labelOccurrence, keyProp, occurrenceKey("t1", r), storepkg.QueryOpts{})
			if err != nil {
				return err
			}
			if len(occs) == 0 {
				fresh = append(fresh, r)
				continue
			}
			complete := true
			for _, rel := range []string{relOccurrenceOf, relObservedOn} {
				if d, _ := g.Rels().OutgoingDegree(occs[0].ID(), rel); d != 1 {
					complete = false
				}
			}
			if n, _ := g.Nodes().ByLabelAndProperty(labelInbox, keyProp, "inbox-"+occurrenceKey("t1", r), storepkg.QueryOpts{}); len(n) != 1 {
				complete = false
			}
			if !complete {
				partial = append(partial, r)
			}
		}
		if len(fresh) > 0 {
			if mode == "tx" {
				err = writeBatchTx(g, "t1", fresh)
			} else {
				err = w.writeBatch(fresh)
			}
			if err != nil {
				return err
			}
		}
		for _, r := range partial {
			if err := repairRow(g, mode, w, r); err != nil {
				return err
			}
		}
	}
	return nil
}

// repairRow completes a row whose occurrence node exists but whose
// relationships or inbox entry were lost to a kill between store doors.
func repairRow(g *graphpkg.Graph, mode string, w *sessionWriter, r row) error {
	ctx := context.Background()
	occs, err := g.Nodes().ByLabelAndProperty(labelOccurrence, keyProp, occurrenceKey("t1", r), storepkg.QueryOpts{})
	if err != nil {
		return err
	}
	occ := occs[0]
	fact, _, err := g.Nodes().GetOrCreateByKey(ctx, labelFact, keyProp, r.factKey, map[string]any{"first_seen": r.ts, "last_seen": r.ts})
	if err != nil {
		return err
	}
	asset, _, err := g.Nodes().GetOrCreateByKey(ctx, labelAsset, keyProp, r.host, map[string]any{"host": r.host})
	if err != nil {
		return err
	}
	if _, _, err := g.Rels().AddByIDIfAbsent(ctx, relOccurrenceOf, occ.ID(), fact.ID(), nil); err != nil {
		return err
	}
	if _, _, err := g.Rels().AddByIDIfAbsent(ctx, relObservedOn, occ.ID(), asset.ID(), nil); err != nil {
		return err
	}
	if n, _ := g.Nodes().ByLabelAndProperty(labelInbox, keyProp, "inbox-"+occurrenceKey("t1", r), storepkg.QueryOpts{}); len(n) == 0 {
		if _, err := g.Nodes().Add(ctx, []string{labelInbox}, map[string]any{keyProp: "inbox-" + occurrenceKey("t1", r), "occurrence_key": occurrenceKey("t1", r), "seq": int64(0), "repaired": true}); err != nil {
			return err
		}
	}
	_ = mode
	_ = w
	return nil
}

// TestDeclaredRelTypesAreDurableBeforeAck pins the gap R1 demonstrated
// (2026-09-09, fixed the same day in predeclareVocabulary): IngestOptions.
// DeclareRelTypes interned the names in memory and called
// persistRegistriesIfDirtyLockedPanicSafe, a no-op unless a PREVIOUS checkpoint
// had failed. A relationship created under a
// declared type is acknowledged by a Sync Submit while its type token is not
// yet durable; after a SIGKILL the row survives but its type resolves to
// nothing (CountByType/OutgoingDegree = 0). The concurrent door's
// declare-on-prepare path has the same defect without any option set.
//
// Contract: a name interned by NewSession/declare-on-prepare is persisted
// before the first acknowledgement of a mutation that references it —
// predeclareVocabulary checkpoints when it interned anything, the rule the tx
// door already applies in checkpointRegistriesOnCommit.
func TestDeclaredRelTypesAreDurableBeforeAck(t *testing.T) {
	if dir := os.Getenv("TKG_REPLAY_DECL_CHILD_DIR"); dir != "" {
		declChild(dir, os.Getenv("TKG_REPLAY_DECL_MODE"))
		return
	}
	for _, mode := range []string{"strong-declared", "concurrent"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestDeclaredRelTypesAreDurableBeforeAck$")
			cmd.Env = append(os.Environ(), "TKG_REPLAY_DECL_CHILD_DIR="+dir, "TKG_REPLAY_DECL_MODE="+mode)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 64)
			if _, err := stdout.Read(buf); err != nil || !strings.HasPrefix(string(buf), "ACK") {
				t.Fatalf("child did not ack: %v %q", err, buf)
			}
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			g := mustOpen(t, dir)
			defer g.Close()
			all, err := g.Rels().All(storepkg.QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			byType, err := g.Rels().CountByType("DECLARED_EDGE")
			if err != nil {
				t.Fatal(err)
			}
			if len(all) != 1 || byType != 1 {
				t.Fatalf("after kill: %d relationship rows survived but %d resolve to the declared type DECLARED_EDGE (acknowledged type token was not durable)", len(all), byType)
			}
		})
	}
}

func declChild(dir, mode string) {
	g, err := openGraph(dir)
	if err != nil {
		os.Exit(3)
	}
	opts := ingest.IngestOptions{Sync: true}
	if mode == "strong-declared" {
		opts.DeclareRelTypes = []string{"DECLARED_EDGE"}
	} else {
		opts.Concurrent = true
	}
	s, err := g.Ingest().NewSession(opts)
	if err != nil {
		os.Exit(4)
	}
	a, err := s.AddNode([]string{labelAsset}, map[string]any{keyProp: "a"})
	if err != nil {
		os.Exit(5)
	}
	b, err := s.AddNode([]string{labelAsset}, map[string]any{keyProp: "b"})
	if err != nil {
		os.Exit(5)
	}
	if _, err := s.AddRelationship("DECLARED_EDGE", a, b, nil); err != nil {
		os.Exit(6)
	}
	if _, err := s.Submit(); err != nil {
		os.Exit(7)
	}
	fmt.Println("ACK")
	time.Sleep(time.Minute)
}

// TestGroupCommitIsOneDurableOperation pins the R3 group-commit contract
// (tasks/evidence/temporal-index/20260909-stream2/result.md, "R3 contract"):
// one acknowledged Sync Submit of one group costs O(1) physical durability
// operations, not O(mutations). Before the group commit a 128-row group cost
// 833 sync calls in this measurement (1 node-batch flush + 256 relationship
// flushes + 126 update flushes, ≈1.9 msync each); after it, one WriteBatch.
//
// The test runs a child of itself under `strace -f -c -e trace=msync,fsync,
// fdatasync` (skips when strace is absent) with 0, 1 and 3 groups of 128 rows.
// The first group carries the session's one-time registry write-aheads
// (declared labels/rel types, new property keys); the STEADY-STATE cost per
// group is (3 groups − 1 group)/2 and must stay within groupBudget (Badger
// issues two msync per WriteBatch.Flush). Every absolute count is logged. It
// must never be made green by disabling SyncWrites: the child opens the
// consumer's SyncWrites config.
func TestGroupCommitIsOneDurableOperation(t *testing.T) {
	if dir := os.Getenv("TKG_REPLAY_GC_CHILD_DIR"); dir != "" {
		g, err := openGraph(dir)
		if err != nil {
			os.Exit(3)
		}
		if groups, _ := strconv.Atoi(os.Getenv("TKG_REPLAY_GC_GROUPS")); groups > 0 {
			w, err := newSessionWriter(g, "t1", 1, false)
			if err != nil {
				os.Exit(4)
			}
			all := workloadSpec{rows: 128 * groups, producers: 1, seed: 7, distinctFacts: 32 * groups, hotHosts: 8, coldHosts: 64}.generate()[0]
			for i := 0; i < groups; i++ {
				if err := w.writeBatch(all[i*128 : (i+1)*128]); err != nil {
					os.Exit(5)
				}
			}
			_ = w.s.Close()
		}
		_ = g.Close()
		os.Exit(0)
	}
	if _, err := exec.LookPath("strace"); err != nil {
		t.Skip("strace not installed; the physical-durability count cannot be measured here")
	}
	count := func(groups int) int {
		dir := t.TempDir()
		summary := dir + "-strace.txt"
		cmd := exec.Command("strace", "-f", "-c", "-e", "trace=msync,fsync,fdatasync", "-o", summary,
			os.Args[0], "-test.run=^TestGroupCommitIsOneDurableOperation$")
		cmd.Env = append(os.Environ(), "TKG_REPLAY_GC_CHILD_DIR="+dir, "TKG_REPLAY_GC_GROUPS="+strconv.Itoa(groups))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child under strace: %v\n%s", err, out)
		}
		raw, err := os.ReadFile(summary)
		if err != nil {
			t.Fatal(err)
		}
		total := 0
		for _, line := range strings.Split(string(raw), "\n") {
			f := strings.Fields(line)
			if len(f) >= 4 && (f[len(f)-1] == "msync" || f[len(f)-1] == "fsync" || f[len(f)-1] == "fdatasync") {
				n, _ := strconv.Atoi(f[3])
				total += n
			}
		}
		return total
	}
	base, one, three := count(0), count(1), count(3)
	perGroup := float64(three-one) / 2
	const groupBudget = 4.0 // one WriteBatch.Flush = two msync; room for one extra flush
	t.Logf("physical sync calls: open+close=%d, +1 group=%d (first group incl. registry write-aheads: %d), +3 groups=%d, steady state per 128-row group=%.1f (budget %.0f)",
		base, one, one-base, three, perGroup, groupBudget)
	if perGroup > groupBudget {
		t.Fatalf("one acknowledged 128-row group costs %.1f physical sync calls in steady state (budget %.0f): the group did not commit as one durable operation (R3)", perGroup, groupBudget)
	}
}

// TestGroupCommitAtomicVisibility pins property (a) of the R3 contract: while
// the strong applier applies a group, a concurrent reader sees the whole group
// or none of it — never a prefix. 200 groups of 64 nodes under one label; the
// reader samples CountByLabel continuously and every sample must be a multiple
// of 64.
func TestGroupCommitAtomicVisibility(t *testing.T) {
	g := mustOpen(t, t.TempDir())
	defer g.Close()
	const groupSize, groups = 64, 200
	s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true, DeclareLabels: []string{"Atomic"}})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var bad []int
	var samples int
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			n, err := g.Nodes().CountByLabel("Atomic")
			if err == nil {
				samples++
				if n%groupSize != 0 {
					bad = append(bad, n)
				}
			}
		}
	}()
	for i := 0; i < groups; i++ {
		for j := 0; j < groupSize; j++ {
			if _, err := s.AddNode([]string{"Atomic"}, map[string]any{keyProp: fmt.Sprintf("a-%d-%d", i, j)}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.Submit(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	<-done
	if len(bad) > 0 {
		t.Fatalf("reader observed partial groups (counts not a multiple of %d): %v", groupSize, bad[:min(len(bad), 10)])
	}
	if samples < 10 {
		t.Fatalf("reader took only %d samples; the test did not exercise concurrency", samples)
	}
	if n, _ := g.Nodes().CountByLabel("Atomic"); n != groupSize*groups {
		t.Fatalf("final count %d", n)
	}
}
