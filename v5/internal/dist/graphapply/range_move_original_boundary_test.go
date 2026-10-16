package graphapply

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

func TestRangeMoveLookupOriginalBoundaryRefusalsDoNotDispatchOrReturnPartialData(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, intent, wires, _ := rangeMoveCoordinatorFixture(t, 64)
	encoded, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	resume := rangeMoveResume{scope: intent.source, request: intent.id, move: rangeMoveID(n.hosts[0][0].machine.ns, intent.id), intent: sha256.Sum256(encoded)}
	origin := n.hosts[0][0].machine.ns
	key := rangeMoveReplayNativeKey(origin, 4, [16]byte{201})
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	cases := []struct {
		name   string
		c      *rangeMoveCoordinator
		ctx    context.Context
		resume rangeMoveResume
		origin namespace
		key    []byte
		cause  error
	}{
		{"nil receiver", nil, t.Context(), resume, origin, key, errInvalid},
		{"nil context", c, nil, resume, origin, key, errInvalid},
		{"cancelled", c, cancelled, resume, origin, key, context.Canceled},
		{"unbound resume", c, t.Context(), rangeMoveResume{}, origin, key, errInvalid},
		{"foreign original", c, t.Context(), resume, namespace{origin.graph, 8}, key, errInvalid},
		{"malformed key", c, t.Context(), resume, origin, []byte{1}, errInvalid},
		{"undeclared origin", c, t.Context(), resume, namespace{origin.graph, 999}, rangeMoveReplayNativeKey(namespace{origin.graph, 999}, 4, [16]byte{201}), errInvalid},
	}
	before := wires[0].calls + wires[1].calls
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.c.LookupOriginal(tc.ctx, tc.resume, tc.origin, false, tc.key)
			if !reflect.DeepEqual(result, rangeMoveOriginal{}) || !errors.Is(err, tc.cause) {
				t.Fatal("original refusal returned partial data or lost sentinel", result, err)
			}
		})
	}
	if wires[0].calls+wires[1].calls != before {
		t.Fatal("boundary refusal dispatched native work")
	}
	c.limits.outputBytes = 4096 + 4*len(key) - 1
	if result, err := c.LookupOriginal(t.Context(), resume, origin, false, key); !reflect.DeepEqual(result, rangeMoveOriginal{}) || !errors.Is(err, errLimit) || !errors.Is(err, graphstate.ErrResourceLimit) {
		t.Fatal("one-short outer admission returned data/dispatched", result, err)
	}
	if wires[0].calls+wires[1].calls != before {
		t.Fatal("outer admission reset source allowance")
	}
	c.limits = n.hosts[0][0].machine.limits
	// A completed native destination observation of no handoff is pending, not
	// evidence that this original was absent at its keeper or at the target.
	if result, err := c.LookupOriginal(t.Context(), resume, origin, false, key); !reflect.DeepEqual(result, rangeMoveOriginal{}) || !errors.Is(err, errRangeMovePending) {
		t.Fatal("uncommitted handoff produced completed absence", result, err)
	}
	if wires[1].calls == 0 {
		t.Fatal("pending discovery skipped genuine destination ReadIndex")
	}
}
