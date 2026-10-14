package main

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Source guards only until the separately coordinated real platform campaign.
// A marker/caller file never substitutes for native stored outcome authority.
func TestGraphPlatformImmutableBodyAndUnavailableAuthorityGuards(t *testing.T) {
	h := &harness{config: lifecycleConfig("platform-guard")}
	r := graphPlatformRecord{Version: 1, Format: graphPlatformFormat, Config: h.config, Case: "storage", Coordinator: 0}
	s := snapshot{Values: literalRows([]outcome{{Round: 1}, {Round: 2}, {Round: 3}})}
	x := h.graphTransaction("edge-update", 0, scenarioCommand(4), s)
	x.Dependency = 3
	x.Lifecycle = &lifecycleMetadata{Provenance: 4, EdgeLife: 4, Definition: new(h.config.Lifecycle.Values[1])}
	b, err := wire(x)
	if err != nil {
		t.Fatal(err)
	}
	r.Originals = []pairedOriginal{{Operation: x.ID, Bytes: b}}
	// An update cannot bypass its captured complete predecessor prefix.
	if _, err := h.platformOriginal(r, 0); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if err := graphPlatformCase("missing"); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if !bytes.Equal(b, r.Originals[0].Bytes) {
		t.Fatal("original changed")
	}
	row := graphPlatformAttempt{Classification: "native-error", Error: "deadline", Retryable: true}
	if row.unavailable() {
		t.Fatal("non-SDK deadline granted native unavailable authority")
	}
	row.NativeCode = new(1031)
	if !row.unavailable() {
		t.Fatal("actual native transaction timeout not recorded")
	}
	row.Outcome.Commit = true
	if row.unavailable() {
		t.Fatal("committed operation classified unavailable")
	}
}

// A real SDK composition guard, not evidence of an injected platform fault.
func TestGraphPlatformActualOriginalsAndTwoPhaseProjection(t *testing.T) {
	for coord := uint8(0); coord < 2; coord++ {
		t.Run(fmt.Sprintf("coordinator%d", coord), func(t *testing.T) {
			db, err := openDB()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(db.Close)
			c := lifecycleConfig("platform-v1-" + t.Name())
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
			r := graphPlatformRecord{Version: 1, Format: graphPlatformFormat, Config: h.config, Case: "storage", Coordinator: coord, Initial: mustCut(t, h, []uint8{0, 1}, nil).record}
			for i := range 4 {
				if err := h.platformCapture(&r, i); err != nil {
					t.Fatal(err)
				}
				x, err := h.platformOriginal(r, i)
				if err != nil {
					t.Fatal(err)
				}
				if err = h.platformAppend(&r, i, mustSubmit(t, h, x)); err != nil {
					t.Fatal(err)
				}
			}
			trace, err := h.platformCheck(r)
			if err != nil {
				t.Fatal(err)
			}
			if trace.OldReads[2].View.Entities[2].Property.I64 != 5 || trace.Steps[3].After.Entities[2].Property.I64 != 6 {
				t.Fatal("old/current substitution")
			}
			x, err := h.platformOriginal(r, 3)
			if err != nil {
				t.Fatal(err)
			}
			proof, _, err := h.graphCostReadParallelOriginal(t.Context(), x, r.Originals[3].Bytes)
			if err != nil {
				t.Fatal(err)
			}
			if replay := mustSubmit(t, h, x); replay != proof.Outcome {
				t.Fatal("original changed")
			}
			changed := x
			changed.Request = "different"
			if _, err = h.submit(changed); !errors.Is(err, errMismatch) {
				t.Fatal("changed request accepted", err)
			}
			after, err := h.platformCheck(r)
			if err != nil || !reflect.DeepEqual(after, trace) {
				t.Fatal("replay changed graph/history/CDC", err)
			}
			if _, _, err = h.graphCostReadParallelOriginal(t.Context(), changed, r.Originals[3].Bytes); !errors.Is(err, errMismatch) {
				t.Fatal("one-sided/foreign original authority", err)
			}
		})
	}
}
