package graph_test

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

type publicCASProofObservation struct {
	publicCASObservation
	proofProbes, proofDecodes, proofCopies int64
	historyCopies, visits, pending         int64
}

func (f *publicCASFixture) proofMismatch(t *testing.T, budget store.ReadBudget, targets []publicCASTarget) publicCASProofObservation {
	t.Helper()
	return f.observeProofComparison(t, budget, targets, bytes.Repeat([]byte{9}, 512), []byte("replacement"), false)
}

func (f *publicCASFixture) observeProofComparison(t *testing.T, budget store.ReadBudget, targets []publicCASTarget, expected, replacement any, wantMatched bool) publicCASProofObservation {
	t.Helper()
	probes, stopProbes := badger.CountAsOfIndexProbesForTest()
	defer stopProbes()
	decodes, stopDecodes := badger.CountAsOfIndexDecodesForTest()
	defer stopDecodes()
	copies, stopCopies := badger.CountAsOfIndexBytesCopiedForTest()
	defer stopCopies()
	history, stopHistory := badger.CountHistoryBytesCopiedForTest()
	defer stopHistory()
	visits, stopVisits := badger.CountIndexEntriesVisitedForTest()
	defer stopVisits()
	pending, stopPending := badger.CountPendingKeysVisitedForTest()
	defer stopPending()
	got := publicCASProofObservation{publicCASObservation: f.observeComparison(t, budget, targets, expected, replacement, wantMatched)}
	got.proofProbes, got.proofDecodes, got.proofCopies = probes(), decodes(), copies()
	got.historyCopies, got.visits, got.pending = history(), visits(), pending()
	return got
}

func (f *publicCASFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.g.Close(); err != nil {
		t.Fatal(err)
	}
	f.open(t)
	f.owners = []*badger.Store{f.st.RefShardForTest(), f.st.HotShardForTest().Store()}
	// Neither clock operation reads an entity or its history. The first
	// comparison below must prove the WholeAxis shape from reopened storage.
	if _, err := f.g.Temporal().AdvanceClock(types.Instant(time.Now().Add(time.Hour).UnixMilli())); err != nil {
		t.Fatal(err)
	}
}

// A real same-row history rewrite invalidates the append-only completeness
// proof without changing the logical entity. The budgeted comparison must then
// admit its metadata fallback; a wrapper hiding capabilities would not exercise
// the native refusal to trust an invalid durable proof.
func (f *publicCASFixture) invalidateHistoryProof(t *testing.T) {
	t.Helper()
	before := make([]publicCASState, len(f.targets))
	for i, target := range f.targets {
		before[i] = target.state(t, f.g)
		if target.node != 0 {
			rows, err := f.g.Nodes().History(target.node)
			if err != nil || len(rows) == 0 {
				t.Fatalf("history rewrite setup: %d rows/error %v", len(rows), err)
			}
			if err := f.st.PutNodeVersion(target.node, rows[0].Version(), rows[0]); err != nil {
				t.Fatal(err)
			}
		} else {
			rows, err := f.g.Rels().History(target.rel)
			if err != nil || len(rows) == 0 {
				t.Fatalf("history rewrite setup: %d rows/error %v", len(rows), err)
			}
			if err := f.st.PutRelVersion(target.rel, rows[0].Version(), rows[0]); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := f.st.DurableFlush(); err != nil {
		t.Fatal(err)
	}
	for i, target := range f.targets {
		if !reflect.DeepEqual(before[i], target.state(t, f.g)) {
			t.Fatal("same-row history rewrite changed logical state")
		}
	}
}

// ValueSize provides the persisted wire length independently of the tally and
// the entity decoder, so a missing proof byte charge cannot hide in a control
// that sets its own later allowance. This neither decodes nor caches a row.
func (f *publicCASFixture) currentRowBytes(t *testing.T) uint64 {
	t.Helper()
	var total uint64
	for i, target := range f.targets {
		var key []byte
		if target.node != 0 {
			key = storeutil.NodeKey(target.node.SnowflakeID())
		} else {
			key = storeutil.RelKey(target.rel.SnowflakeID())
		}
		if err := f.owners[i].DBForTest().View(func(txn *badgerv4.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			size := item.ValueSize()
			if size <= 0 {
				return fmt.Errorf("current wire has invalid size %d", size)
			}
			total += uint64(size)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return total
}

func TestReadBudgetPublicCASProofCumulative(t *testing.T) {
	testtier.Slow(t)
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicCASFixture(t, kind)
			before := []publicCASState{f.targets[0].state(t, f.g), f.targets[1].state(t, f.g)}
			clock := publicCASClock(t, f.g)
			control := f.proofMismatch(t, store.ReadBudget{}, f.targets)
			w := control.work
			// Per owner the node comparison decodes its head and two liveness
			// probes; relationships also perform the endpoint-lock reread.
			// Each proof is 41 bytes; the zero-generation key is absent, so
			// its admitted native probe adds no copy or scalar decode.
			wantDecoded := int64(6)
			wantCandidates := uint64(14)
			if kind == "rel" {
				wantDecoded = 8
				wantCandidates = 13
			}
			// Nodes pay seven candidates per owner: the initial core/route
			// reads, proof route, two liveness probes and proof/generation.
			// Relationships pay six on reference and seven on event (ref miss).
			wantBytes := f.currentRowBytes(t)*uint64(wantDecoded/2) + 82
			if control.err != nil || control.calls != 2 || control.decoded != wantDecoded || control.meta != [2]int64{} || control.proofProbes != 4 || control.proofDecodes != 2 || control.proofCopies != 82 || control.historyCopies != 0 || control.visits != 0 || control.pending != 0 || w.HistoryExamined != 0 || w.Candidates != wantCandidates || w.DecodedBytes != wantBytes || w.BufferBytes != 82 {
				t.Fatalf("nonempty two-owner proof control: %+v", control)
			}
			exact := store.ReadBudget{Candidates: w.Candidates, DecodedBytes: w.DecodedBytes, BufferBytes: w.BufferBytes, HistoryExamined: 2}
			got := f.proofMismatch(t, exact, f.targets)
			if got != control {
				t.Fatalf("exact proof allowance: %+v; want %+v; error %v", got, control, got.err)
			}
			for _, dim := range []struct {
				name                  string
				budget                store.ReadBudget
				probes, decodes, copy int64
			}{
				{"candidates", store.ReadBudget{Candidates: w.Candidates - 1}, 4, 2, 82},
				{"decoded bytes", store.ReadBudget{DecodedBytes: w.DecodedBytes - 1}, 4, 2, 82},
				{"buffer bytes", store.ReadBudget{BufferBytes: w.BufferBytes - 1}, 3, 1, 41},
			} {
				got = f.proofMismatch(t, dim.budget, f.targets)
				var refusal *store.ReadBudgetError
				// The final liveness row cannot be decoded after refusal. A
				// buffer refusal occurs earlier, before copying the second proof.
				if !errors.As(got.err, &refusal) || refusal.Limit != dim.name || refusal.Budget != dim.budget || refusal.Work != got.work || got.calls != 1 || got.decoded != wantDecoded-1 || got.meta != [2]int64{} || got.proofProbes != dim.probes || got.proofDecodes != dim.decodes || got.proofCopies != dim.copy || got.historyCopies != 0 || got.visits != 0 || got.pending != 0 {
					t.Fatalf("one under %s: %+v; error %v", dim.name, got, got.err)
				}
				t.Logf("one under %s: %+v", dim.name, got)
			}
			first := f.proofMismatch(t, store.ReadBudget{}, f.targets[:1])
			second := f.proofMismatch(t, store.ReadBudget{}, f.targets[1:])
			shared := store.ReadBudget{DecodedBytes: max(first.work.DecodedBytes, second.work.DecodedBytes), HistoryExamined: 2}
			for i, target := range f.targets {
				got = f.proofMismatch(t, shared, []publicCASTarget{target})
				if got.err != nil || got.calls != 1 || got.decoded == 0 || got.proofDecodes == 0 {
					t.Fatalf("single owner cannot fit shared allowance: %+v; error %v", got, got.err)
				}
				single := []publicCASProofObservation{first, second}[i]
				sw := single.work
				budget := store.ReadBudget{Candidates: sw.Candidates, DecodedBytes: sw.DecodedBytes, BufferBytes: sw.BufferBytes, HistoryExamined: 2}
				got = f.observeProofComparison(t, budget, []publicCASTarget{target}, bytes.Repeat([]byte{3}, 512), bytes.Repeat([]byte{3}, 512), true)
				if got != single {
					t.Fatalf("owner %d matching no-op skipped or changed proof work: %+v; want %+v; error %v", i, got, single, got.err)
				}
			}
			got = f.proofMismatch(t, shared, f.targets)
			var refusal *store.ReadBudgetError
			if !errors.As(got.err, &refusal) || refusal.Limit != "decoded bytes" || refusal.Budget != shared || refusal.Work != got.work || got.calls != 1 || got.decoded >= control.decoded || got.proofDecodes != first.proofDecodes {
				t.Fatalf("proof allowance reset across owners: %+v; error %v", got, got.err)
			}
			for i, target := range f.targets {
				if !reflect.DeepEqual(before[i], target.state(t, f.g)) || publicCASClock(t, f.g) != clock {
					t.Fatal("mismatch or refusal changed head, properties, history, integrity or commit clock")
				}
			}
			t.Logf("proof control: %+v; shared refusal: %+v", control, got)
		})
	}
}

// Publications grow the same persisted heads from two cuts to eight. The
// nonempty two-cut comparison fixes every allowance except HistoryExamined:
// that retains the host's finite allowance of two even when the compact proof
// needs no history rows. A clean reopen cannot depend on a warmed row or proof.
func TestReadBudgetPublicCASHistoryGrowth(t *testing.T) {
	testtier.Slow(t)
	for _, kind := range []string{"node", "rel"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicCASFixtureCuts(t, kind, 2)
			var controls [2]publicCASProofObservation
			var budgets [2]store.ReadBudget
			for i, target := range f.targets {
				controls[i] = f.proofMismatch(t, store.ReadBudget{}, []publicCASTarget{target})
				c := controls[i]
				if c.err != nil || c.calls != 1 || c.decoded == 0 || c.work.Candidates == 0 || c.work.DecodedBytes == 0 || c.work.BufferBytes == 0 {
					t.Fatalf("owner %d nonempty two-cut control: %+v", i, c)
				}
				w := c.work
				budgets[i] = store.ReadBudget{Candidates: w.Candidates, DecodedBytes: w.DecodedBytes, BufferBytes: w.BufferBytes, HistoryExamined: 2, Segments: max(w.Segments, 1), ShardOpens: max(w.ShardOpens, 1)}
			}
			for _, cuts := range []byte{2, 8} {
				if cuts == 8 {
					for revision := byte(3); revision <= cuts; revision++ {
						for _, target := range f.targets {
							matched, err := target.compare(t.Context(), f.g, bytes.Repeat([]byte{revision - 1}, 512), bytes.Repeat([]byte{revision}, 512))
							if err != nil || !matched {
								t.Fatalf("publication %d: matched %v/error %v", revision, matched, err)
							}
						}
						if err := f.st.DurableFlush(); err != nil {
							t.Fatal(err)
						}
					}
				}
				before := []publicCASState{f.targets[0].state(t, f.g), f.targets[1].state(t, f.g)}
				for i, state := range before {
					if state.head.version != uint32(cuts) || len(state.history) != int(cuts) || !types.PropertyEqual(state.head.properties["state"], bytes.Repeat([]byte{cuts}, 512)) || state.head.properties["guard"] != "unchanged" {
						t.Fatalf("owner %d missing exact %d-cut publication state: version %d/history %d", i, cuts, state.head.version, len(state.history))
					}
				}
				for _, phase := range []string{"persisted", "reopened"} {
					if phase == "reopened" {
						f.reopen(t)
					}
					clock := publicCASClock(t, f.g)
					for i, target := range f.targets {
						t.Run(fmt.Sprintf("%d/%s/owner%d", cuts, phase, i), func(t *testing.T) {
							// Run the finite comparison first after reopen, before a
							// control or state assertion can read this owner's head.
							got := f.proofMismatch(t, budgets[i], []publicCASTarget{target})
							wide := f.proofMismatch(t, store.ReadBudget{}, []publicCASTarget{target})
							control := controls[i]
							t.Logf("two-cut control %+v; %d-cut %s finite %+v; unlimited %+v", control, cuts, phase, got, wide)
							if got.err != nil || got.calls != 1 || wide.err != nil || wide.calls != 1 || got != control || wide != control {
								t.Errorf("same-head comparison cost grew: finite %+v/unlimited %+v; want %+v; error %v", got, wide, control, got.err)
							}
							if wide.work.HistoryExamined != 0 || wide.meta != [2]int64{} || wide.historyCopies != 0 || wide.visits != 0 || wide.pending != 0 || wide.proofProbes == 0 || wide.proofDecodes == 0 || wide.proofCopies == 0 {
								t.Errorf("ordinary persisted head scanned history instead of its admitted proof: %+v", wide)
							}
							if after := target.state(t, f.g); !reflect.DeepEqual(before[i], after) || publicCASClock(t, f.g) != clock {
								t.Fatal("mismatch or refusal changed head, properties, history, integrity or commit clock")
							}
						})
					}
				}
			}
		})
	}
}
