package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Authored guards only. No child/workload/measurement acceptance is implied.
func TestNativeGraphGuardedScalarProfilesAndCleanup(t *testing.T) {
	for _, version := range []int{13, 14} {
		for _, facts := range []uint64{40, 10_000, 100_000, 1_000_000} {
			for _, writers := range []int{1, 8, 32} {
				for _, cross := range []uint64{0, 10, 50, 100} {
					s := nativeGraphCostSpec{version, facts, cross, writers}
					if s.validate() != nil || nativeFeedValidate(s) == nil || nativeSelectorsValidate(s) == nil {
						t.Fatal("fresh profile changed old validation")
					}
					frames, err := nativeGuardedScalarFrames(s)
					if err != nil || frames*260+65536 > 1<<39 || s.deliveries() == 0 {
						t.Fatal("finite unchanged native record envelope", err)
					}
					block, err := nativeGraphCostBlockSize(s)
					if err != nil {
						t.Fatal(err)
					}
					var total uint64
					for sequence := uint64(1); total < s.homeCount(); sequence++ {
						first, count, err := nativeGraphCostGrantPlan(s, block, 3, sequence)
						if err != nil || first != total+1 || count == 0 {
							t.Fatal("genuine grant recipe correspondence", err)
						}
						total += count
					}
					if total != s.homeCount() || nativeGraphCostOperationID(s, 5, 0).Dataset != nativeGraphCostOperationID(nativeGraphCostSpec{1, facts, cross, writers}, 5, 0).Dataset {
						t.Fatal("profile relabelled logical dataset or IDs")
					}
				}
			}
		}
	}
	for _, family := range []string{nativeFeedFamily, nativeSelectorsFamily} {
		spec := nativeFeedSpec{2, family, 40, 50, 32, "columns"}
		body, err := json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("RHO_GRAPH_WORKLOAD_SPEC", string(body))
		if family == nativeFeedFamily {
			if _, err := nativeGuardedFeedSpecFromEnv(); err != nil {
				t.Fatal(err)
			}
			if _, err := nativeFeedSpecFromEnv(); !errors.Is(err, errInvalid) {
				t.Fatal("fresh spec entered old direct feed", err)
			}
		} else {
			if _, err := nativeGuardedSelectorsSpecFromEnv(); err != nil {
				t.Fatal(err)
			}
			if _, err := nativeSelectorsSpecFromEnv(); !errors.Is(err, errInvalid) {
				t.Fatal("fresh spec entered old direct selectors", err)
			}
		}
		for _, changed := range [][]byte{bytes.Replace(body, []byte(`"writers":32`), []byte(`"writers":true`), 1), append(body, 'x')} {
			t.Setenv("RHO_GRAPH_WORKLOAD_SPEC", string(changed))
			if family == nativeFeedFamily {
				if _, err := nativeGuardedFeedSpecFromEnv(); !errors.Is(err, errInvalid) {
					t.Fatal(err)
				}
			} else if _, err := nativeGuardedSelectorsSpecFromEnv(); !errors.Is(err, errInvalid) {
				t.Fatal(err)
			}
		}
	}
	for _, version := range []int{13, 14} {
		s := nativeGraphCostSpec{version, 40, 0, 32}
		row := nativeColumnRow{Owner: 7, Component: 1, Kind: 2, Mode: 1, Type: "edge", Source: 1, Target: 3, SourceLife: 2, TargetLife: 4, Axis: []byte{1}, Scope: []byte{1}, Before: nativeColumnCell{Present: true, Handle: 8, Revision: 10, Provenance: 4}, After: nativeColumnCell{Present: true, Handle: 8, Revision: 11, Provenance: 44}}
		if class, err := nativeGraphCostColumnRow(s, row); err != nil || class != 0 {
			t.Fatal("funded extra presence revision", err)
		}
		for _, mutate := range []func(*nativeColumnRow){func(r *nativeColumnRow) { r.Axis = nil }, func(r *nativeColumnRow) { r.Scope = nil }, func(r *nativeColumnRow) { r.After.Present = false }, func(r *nativeColumnRow) { r.After.Null = true }, func(r *nativeColumnRow) { r.After.Provenance = 4 }, func(r *nativeColumnRow) { r.Before.Handle++ }} {
			bad := row
			mutate(&bad)
			if _, err := nativeGraphCostColumnRow(s, bad); !errors.Is(err, errCorrupt) {
				t.Fatal("malformed guarded row", err)
			}
		}
	}
	if _, err := newNativeGuardedFeedSessions(context.Background(), nil, &nativeGraphCostSpec{13, 40, 0, 1}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := newNativeGuardedSelectorsSessions(context.Background(), nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := nativeGuardedFeedColumnPolicy(nil, nativeFeedSpec{}, nativeFeedAdmission{}, [32]byte{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := nativeGuardedSelectorsColumnPolicy(nil, nativeSelectorsSpec{}, nativeSelectorsAdmission{}, [32]byte{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	t.Run("actual-closed-endpoint-deleted-edge-and-held-past", nativeCreateUpdateGuard)
	t.Run("fixed-cleanup-lane-after-normal-cancel", nativeGuardedFeedCleanupGuard)
}

func nativeGuardedFeedCleanupGuard(t *testing.T) {
	n, p, _, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithAgreement(t, nil, retainedFeedSemanticContractID()))
	normal, cancel := context.WithCancel(t.Context())
	cancel()
	cleanup, stop := context.WithDeadline(context.WithoutCancel(normal), time.Now().Add(time.Minute))
	defer stop()
	s := &nativeFeedSessions{ctx: normal, coordinator: p.coordinator, spec: nativeFeedSpec{2, nativeFeedFamily, 40, 0, 1, "kv"}, admission: nativeFeedAdmission{BudgetSeconds: 60}, cleanup: cleanup}
	before := n.hosts[0][0].driver.Applied()
	call := func(q nativeFeedInput) error {
		wire, err := nativeFeedInputWire(q)
		if err != nil {
			return err
		}
		_, err = s.serve(normal, processRequest{op: processCostObserve, data: wire})
		return err
	}
	if err := call(nativeFeedInput{Version: 1, Action: "publish"}); !errors.Is(err, context.Canceled) || s.cleanupCalls != 0 {
		t.Fatal("normal cancellation admitted work", err)
	}
	for _, action := range []string{"close", "snapshot-close", "release", "expire"} {
		wire, _ := nativeFeedInputWire(nativeFeedInput{Version: 1, Action: action})
		if !nativeGuardedFeedTerminalControl(processRequest{op: processCostObserve, data: wire}) || nativeGuardedFeedTerminalControl(processRequest{op: processCostObserve, data: append(wire, 'x')}) {
			t.Fatal("outer/inner terminal classification differs")
		}
	}
	bad, _ := nativeFeedInputWire(nativeFeedInput{Version: 1, Action: "close", From: 1})
	if nativeGuardedFeedTerminalControl(processRequest{op: processCostObserve, data: bad}) {
		t.Fatal("malformed action acquired cleanup lane")
	}
	for i := 0; i < nativeGuardedFeedCleanupCalls; i++ {
		if err := call(nativeFeedInput{Version: 1, Action: "snapshot-close"}); err != nil {
			t.Fatal("funded Close blocked by canceled normal lifetime", err)
		}
	}
	if err := call(nativeFeedInput{Version: 1, Action: "snapshot-close"}); !errors.Is(err, errLimit) {
		t.Fatal("cleanup credit renewed", err)
	}
	if n.hosts[0][0].driver.Applied() != before {
		t.Fatal("empty terminal cleanup invented native effects")
	}
	past, pastCancel := context.WithDeadline(context.WithoutCancel(t.Context()), time.Now().Add(-time.Second))
	defer pastCancel()
	deadlineSession := &nativeFeedSessions{ctx: past, coordinator: p.coordinator, spec: s.spec, admission: s.admission, cleanup: cleanup}
	publish, _ := nativeFeedInputWire(nativeFeedInput{Version: 1, Action: "publish"})
	if _, err := deadlineSession.serve(t.Context(), processRequest{op: processCostObserve, data: publish}); !errors.Is(err, context.DeadlineExceeded) || deadlineSession.cleanupCalls != 0 {
		t.Fatal("saved normal deadline admitted work", err)
	}
	closeWire, _ := nativeFeedInputWire(nativeFeedInput{Version: 1, Action: "snapshot-close"})
	if _, err := deadlineSession.serve(t.Context(), processRequest{op: processCostObserve, data: closeWire}); err != nil || deadlineSession.cleanupCalls != 1 {
		t.Fatal("original funded cleanup blocked by normal deadline", err)
	}
	deadlineSession.cleanup = past
	if _, err := deadlineSession.serve(t.Context(), processRequest{op: processCostObserve, data: closeWire}); !errors.Is(err, context.DeadlineExceeded) || deadlineSession.cleanupCalls != 2 {
		t.Fatal("cleanup deadline renewed or unknown exchange refunded", err)
	}
	if n.hosts[0][0].driver.Applied() != before {
		t.Fatal("deadline refusal invented native effects")
	}
	legacy := &nativeFeedSessions{ctx: normal, coordinator: p.coordinator, spec: nativeFeedSpec{1, nativeFeedFamily, 40, 0, 1, "kv"}}
	wire, _ := nativeFeedInputWire(nativeFeedInput{Version: 1, Action: "snapshot-close"})
	if _, err := legacy.serve(normal, processRequest{op: processCostObserve, data: wire}); !errors.Is(err, context.Canceled) {
		t.Fatal("old5 cancellation behavior changed", err)
	}
}
