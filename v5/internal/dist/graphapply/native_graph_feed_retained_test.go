package graphapply

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Authored guards, unexecuted during implementation-first source integration.
func TestNativeFeedRetainedAdmissionWireAndCleanupStayProfileBound(t *testing.T) {
	spec := nativeFeedRetainedSpec{Version: 3, Family: nativeFeedFamily, Facts: 40, CrossPercent: 50, Writers: 32, Layout: "columns"}
	if err := nativeFeedRetainedValidate(spec.cost()); err != nil {
		t.Fatal(err)
	}
	if _, err := newNativeFeedRetainedSessions(context.Background(), nil, &nativeGraphCostSpec{Version: 7, Facts: 40, CrossPercent: 50, Writers: 32}); !errors.Is(err, errInvalid) {
		t.Fatal("nil source coordinator", err)
	}
	if _, err := newNativeFeedRetainedGC(context.Background(), nil, 1, spec, nativeFeedRetainedAdmission{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil collector Host", err)
	}
	for _, facts := range []uint64{10_000, 100_000, 1_000_000} {
		cost := spec.cost()
		cost.Facts = facts
		frames, err := nativeFeedRetainedFrames(cost)
		if err != nil || frames < 2*facts || frames > (1<<41)/1024 || 260*frames+65536 > 1<<39 {
			t.Fatal("finite admitted complete schedule", err)
		}
		var total uint64
		for sequence := uint64(1); ; sequence++ {
			_, count, err := nativeFeedRetainedGrantPlan(cost, 1<<20, 3, sequence)
			if errors.Is(err, errInvalid) {
				break
			}
			if err != nil || count == 0 {
				t.Fatal(err)
			}
			total += count
		}
		if total != cost.homeCount() {
			t.Fatal("real complete installed-grant geometry")
		}
	}
	// Guarded updates still pass through the shared axis/scope and current-cell
	// checks before the fresh presence reducer accepts their lifecycle shape.
	row := nativeColumnRow{Kind: 2, Component: 1, Owner: 7, Type: "edge", Mode: 1, Source: 1, Target: 3, SourceLife: 2, TargetLife: 4, Axis: []byte{1}, Scope: []byte{1}, Before: nativeColumnCell{Present: true, Handle: 8, Revision: 1, Provenance: 4}, After: nativeColumnCell{Present: true, Handle: 8, Revision: 2, Provenance: 44}}
	if class, err := nativeGraphCostColumnRow(spec.cost(), row); err != nil || class != 0 {
		t.Fatal("valid guarded presence", err)
	}
	for _, mutate := range []func(*nativeColumnRow){
		func(r *nativeColumnRow) { r.Axis = nil },
		func(r *nativeColumnRow) { r.Scope = nil },
		func(r *nativeColumnRow) { r.After.Present = false },
		func(r *nativeColumnRow) { r.After.Null = true },
		func(r *nativeColumnRow) { r.After.Revision = 0 },
	} {
		bad := row
		mutate(&bad)
		if _, err := nativeGraphCostColumnRow(spec.cost(), bad); !errors.Is(err, errCorrupt) {
			t.Fatal("malformed guarded presence bypassed common validation", err)
		}
	}

	q := nativeFeedInput{Version: 1, Action: "retain", Request: [16]byte{1}}
	wire, err := nativeFeedRetainedInputWire(q)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := nativeFeedRetainedInputDecode(processRequest{op: processCostObserve, data: wire}); err != nil || got.Request != q.Request {
		t.Fatal("fresh mode transition framing", err)
	}
	if _, err := nativeFeedInputDecode(processRequest{op: processCostObserve, data: wire}); !errors.Is(err, errInvalid) {
		t.Fatal("Cost5 accepted new marker", err)
	}
	old, err := nativeFeedInputWire(q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nativeFeedInputDecode(processRequest{op: processCostObserve, data: old}); !errors.Is(err, errInvalid) {
		t.Fatal("Cost5 accepted new action", err)
	}
	for _, bad := range []nativeFeedInput{{Version: 1, Action: "retain"}, {Version: 1, Action: "retain", Request: [16]byte{1}, From: 1}} {
		body, err := nativeFeedRetainedInputWire(bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := nativeFeedRetainedInputDecode(processRequest{op: processCostObserve, data: body}); !errors.Is(err, errInvalid) {
			t.Fatal("malformed retained-only command", err)
		}
	}
	for _, action := range []string{"close", "release", "snapshot-close", "snapshot", "retain"} {
		q.Action = action
		q.Request = [16]byte{}
		if action == "retain" {
			q.Request = [16]byte{1}
		}
		body, err := nativeFeedRetainedInputWire(q)
		if err != nil {
			t.Fatal(err)
		}
		actual := processRequest{op: processCostObserve, data: body}
		want := action == "close" || action == "release" || action == "snapshot-close"
		if nativeFeedRetainedTerminalControl(actual) != want {
			t.Fatal("terminal slot classification", action)
		}
		actual.data = append(body, 'x')
		if nativeFeedRetainedTerminalControl(actual) {
			t.Fatal("noncanonical terminal slot")
		}
	}
	cancelled, finish := context.WithCancel(context.Background())
	finish()
	sessions := &nativeFeedRetainedSessions{ctx: cancelled, spec: spec, core: &nativeFeedSessions{}, admit: nativeFeedRetainedAdmission{BudgetSeconds: 1}}
	q = nativeFeedInput{Version: 1, Action: "begin", Request: [16]byte{1}}
	wire, err = nativeFeedRetainedInputWire(q)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.serve(cancelled, processRequest{op: processCostObserve, data: wire}); !errors.Is(err, context.Canceled) || sessions.core.owner != nil {
		t.Fatal("cancelled read created source work", err)
	}
	// Exhausted terminal admission retains the actual owner; it cannot be used
	// as a normal source or silently renew the immutable deadline.
	cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sessions.cleanup, sessions.cleanupCalls = cleanup, 32
	wire, err = nativeFeedRetainedInputWire(nativeFeedInput{Version: 1, Action: "close"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.serve(context.Background(), processRequest{op: processCostObserve, data: wire}); !errors.Is(err, errLimit) {
		t.Fatal("terminal allowance renewed", err)
	}
	body, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var again nativeFeedRetainedSpec
	if err := nativeFeedDecodeJSON(body, &again); err != nil || again != spec {
		t.Fatal("fresh spec binding", err)
	}
}
