package main

import (
	"errors"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"os"
	"testing"
)

// These authored guards are not evidence of a real child interruption.
func TestGraphColumnPublicationObserverCannotGrantManifestAuthority(t *testing.T) {
	var missing *graphColumnStore
	if m, err := missing.publishObserved(t.Context(), "g", cut{}, cut{}, nil); !errors.Is(err, errInvalid) || !graphColumnSame(m, graphColumnManifest{}) {
		t.Fatal("nil publication", m, err)
	}
	state := graphColumnProgress{Rows: 1, Sequences: [2]uint64{1, 0}, Generation: graphColumnGeneration{Cut: certificate{Round: 1, Groups: [2]uint64{1, 0}}}}
	state.Metadata = [2]metadata{{Floor: 1, Groups: 1, Versions: 2}, {Floor: 1}}
	state.History = [2]uint64{1, 0}
	if graphColumnComplete(state) {
		t.Fatal("observer/caller count blessed missing authentic history")
	}
	if err := columnFaultCase("helper-reconstruction"); !errors.Is(err, errInvalid) {
		t.Fatal("fake fault mode accepted", err)
	}
}

// Authored real SDK composition, never a substitute for external child kills.
func TestGraphColumnActualInterruptedUploadAndSharedManifestReducer(t *testing.T) {
	db, err := openDB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	c := graphCostConfig("column-publication-v1-"+t.Name(), graphCostProfile{Version: 1, Facts: 1, CrossPercent: 100, Writers: 1})
	h := &harness{db: db, config: c}
	reset := func() {
		_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
			for g := uint8(0); g < 2; g++ {
				tr.ClearRange(prefixRange(h.prefix(g)))
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	reset()
	t.Cleanup(reset)
	h, err = newHarness(db, c)
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	p, pp := columnFaultPolicies()
	r := columnFaultState{Config: c, Columns: p, Pins: pp, Initial: mustCut(t, h, []uint8{0, 1}, nil).record}
	for i := range 3 {
		if err := columnFaultApply(root, h, &r, "setup", uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := columnFaultApply(root, h, &r, "create", 0); err != nil {
		t.Fatal(err)
	}
	r.Old = mustCut(t, h, []uint8{0, 1}, nil).record
	s, pins, err := columnFaultStores(h, r)
	if err != nil {
		t.Fatal(err)
	}
	callbacks := 0
	if m, err := s.publishObserved(t.Context(), "old", cut{r.Initial}, cut{r.Old}, func(e graphColumnPublicationEvent) error { callbacks++; return errColumnFaultSeam }); !errors.Is(err, errColumnFaultSeam) || !graphColumnSame(m, graphColumnManifest{}) || callbacks != 1 {
		t.Fatal("partial upload gained EOF", m, callbacks, err)
	}
	if _, err := s.manifest("old"); !errors.Is(err, errUnavailable) {
		t.Fatal("observer granted manifest", err)
	}
	r.OldManifest, err = s.publish(t.Context(), "old", cut{r.Initial}, cut{r.Old})
	if err != nil {
		t.Fatal(err)
	}
	r.Token, err = pins.pin(t.Context(), "old", "old", cut{r.Old})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := newGraphColumnRead(s, "old", &r.Token)
	if err != nil {
		t.Fatal(err)
	}
	r.Owner = owner.id
	if err = owner.open(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	if err = columnFaultApply(root, h, &r, "update", 0); err != nil {
		t.Fatal(err)
	}
	r.Final = mustCut(t, h, []uint8{0, 1}, nil).record
	if m, err := s.publishObserved(t.Context(), "final", cut{r.Initial}, cut{r.Final}, func(e graphColumnPublicationEvent) error {
		if e.Stage == "source-eof" {
			return errColumnFaultSeam
		}
		return nil
	}); !errors.Is(err, errColumnFaultSeam) || !graphColumnSame(m, graphColumnManifest{}) {
		t.Fatal("source EOF published despite observer failure", err)
	}
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Cancel()
	m, err := s.sealIn(tr, graphColumnGeneration{Version: 1, ID: "final", Binding: s.digest, Initial: r.Initial, Cut: r.Final})
	if err != nil {
		t.Fatal(err)
	}
	if err = tr.Commit().Get(); err != nil {
		t.Fatal(err)
	}
	if again, err := s.publish(t.Context(), "final", cut{r.Initial}, cut{r.Final}); err != nil || !graphColumnSame(m, again) {
		t.Fatal("same reducer retry changed manifest", err)
	}
	if err = pins.finish(t.Context(), "expired", r.Token, true); err != nil {
		t.Fatal(err)
	}
	if _, err = pins.open(t.Context(), r.Token); !errors.Is(err, errColumnExpired) {
		t.Fatal("expired token admitted NEW reader", err)
	}
	recovered, err := columnFaultOwner(s, r)
	if err != nil {
		t.Fatal(err)
	}
	if err = recovered.open(t.Context()); err != nil {
		t.Fatal("exact active durable identity lost", err)
	}
	if _, err = recovered.walk(t.Context(), cut{r.Old}, nil, func(row graphColumnRow) error {
		if row.Owner == 7 && row.Value != 5 {
			t.Fatal("held old became current")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = recovered.Close(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []certificate{r.Initial, r.Old, r.Final} {
		v, err := columnFaultSourceView(h, c)
		if err != nil {
			t.Fatal(err)
		}
		r.Views = append(r.Views, v)
	}
	if err = columnFaultCheckSource(h, r); err != nil {
		t.Fatal(err)
	}
}
