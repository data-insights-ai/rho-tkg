package graphapply

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func TestNativeScopeReadDoorPreservesLegacyBytesAndRejectsForeignScopeShapes(t *testing.T) {
	legacy := graphE15ReadRequest{mode: graphReadFresh}
	wire, err := encodeGraphE15ReadRequest(legacy)
	expected, oldErr := boundedEncoding(16<<10, func(w *boundedWriter) {
		w.add([]byte{'G', 'E', 'Q', 1})
		w.tag(byte(graphReadFresh))
		w.add(make([]byte, 16))
		w.u64(0)
		w.u32(0)
	})
	if err != nil || oldErr != nil || !bytes.Equal(wire, expected) {
		t.Fatal("ordinary GEQ1 byte contract", err, oldErr)
	}
	decoded, err := decodeGraphE15ReadRequest(wire)
	if err != nil || decoded.partitions != nil || decoded.mode != legacy.mode {
		t.Fatal("ordinary nil-scope decode", err)
	}
	for _, scope := range [][]uint64{{3}, {8}, {3, 8}} {
		first := scope[0]
		request := graphE15ReadRequest{mode: graphReadFresh, partitions: scope}
		wire, err := encodeGraphE15ReadRequest(request)
		decoded, decodeErr := decodeGraphE15ReadRequest(wire)
		if err != nil || decodeErr != nil || wire[3] != 2 || !slices.Equal(decoded.partitions, scope) {
			t.Fatal("exact explicit scope round trip", err, decodeErr)
		}
		decoded.partitions[0]++
		if request.partitions[0] != first || wire[3] != 2 {
			t.Fatal("scope decode aliased original input")
		}
	}
	for _, scope := range [][]uint64{{0}, {3, 3}, {8, 3}, make([]uint64, 17)} {
		if _, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadFresh, partitions: scope}); !errors.Is(err, errInvalid) {
			t.Fatal("invalid scope admitted", scope, err)
		}
	}
	malformed, err := boundedEncoding(16<<10, func(w *boundedWriter) {
		w.add([]byte{'G', 'E', 'Q', 2})
		w.tag(byte(graphReadFresh))
		w.add(make([]byte, 16))
		w.u64(0)
		w.u32(0)
		w.u32(0)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeGraphE15ReadRequest(malformed); !errors.Is(err, errInvalid) {
		t.Fatal("GEQ2 cannot silently mean unrestricted scope", err)
	}
}

func TestNativeP01CurrentDiagnosticKeepsFollowerHistoricalAndLeaderFreshRoles(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedGraphNetwork(t)
	ops, _ := crossEdgeFixture(t, n)
	live := remoteCertifiedFixture(t, n)
	old, err := live.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := old.Close(); err != nil {
			t.Error(err)
		}
	})
	cut, err := old.Cut()
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "after-old-cut", Scope: ops[0].Scope}}, 31, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, mutation, reasonNone)

	// Exactly the restored target's historical routing: remote A follower2,
	// own B follower3 (process member6). These are actual six-voter services.
	transports := make(map[uint64]graphPeerWireTransport, 2)
	for group, member := range []int{1, 2} {
		h := n.hosts[group][member]
		server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := server.close(); err != nil {
				t.Error(err)
			}
		})
		service, err := newGraphPeerService(h, server, &nativeGraphTestPump{n: n, group: group, host: h})
		if err != nil {
			t.Fatal(err)
		}
		transports[h.machine.ns.partition] = &nativeGraphTestTransport{service: service}
	}
	historical, err := newCrossProcessCoordinator(n.hosts[1][2], transports)
	if err != nil {
		t.Fatal(err)
	}
	held, err := historical.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil)
	if err != nil {
		t.Fatal("follower lost genuine old certificate", err)
	}
	t.Cleanup(func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
	})
	actual, err := held.Cut()
	if err != nil || !slices.Equal(actual.certificates, cut.certificates) {
		t.Fatal("historical cut changed", err)
	}
	assertCertifiedProjection(t, certifiedView(t, held), 1, true, nil)
	assertCertifiedProjection(t, certifiedView(t, held), 7, false, nil)
	before := n.hosts[1][2].driver.Applied()
	if read, err := historical.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil); read != nil || !errors.Is(err, replica.ErrUnavailable) || graphErrorMask(err) != 1<<3 || n.hosts[1][2].driver.Applied() != before {
		t.Fatal("historical follower invented current quorum authority", read, err)
	}
	actor, err := nativeTransitionScopeCurrentCaller(2, 6)
	if err != nil {
		t.Fatal(err)
	}
	readers := map[uint64]*crossProcessCoordinator{2: live, 6: historical}
	current, err := readers[actor].OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal("current diagnostic reused historical-only follower route", actor, err)
	}
	t.Cleanup(func() {
		if err := current.Close(); err != nil {
			t.Error(err)
		}
	})
	assertCertifiedProjection(t, certifiedView(t, current), 1, true, []string{"after-old-cut"})
	assertCertifiedProjection(t, certifiedView(t, current), 7, false, nil)
	assertCertifiedProjection(t, certifiedView(t, held), 1, true, nil)
	assertCertifiedProjection(t, certifiedView(t, old), 1, true, nil)
	for _, tc := range []struct{ reader, selected, want uint64 }{{2, 0, 6}, {3, 0, 6}, {2, 3, 6}, {3, 1, 6}, {3, 4, 6}, {2, 6, 2}, {3, 6, 3}} {
		if got, err := nativeTransitionScopeCurrentCaller(tc.reader, tc.selected); err != nil || got != tc.want || got == tc.selected {
			t.Fatal("current diagnostic selected retired caller", tc, got, err)
		}
	}
	for _, tc := range [][2]uint64{{0, 6}, {1, 6}, {4, 6}, {2, 2}, {3, 3}, {2, 5}, {2, 7}} {
		if got, err := nativeTransitionScopeCurrentCaller(tc[0], tc[1]); got != 0 || !errors.Is(err, errInvalid) {
			t.Fatal("unbound current caller roles admitted", tc, got, err)
		}
	}
}

func TestNativeP01OutsideDataScopeFreshAndAfterResolveOrRefuseActualCoordinator(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, mode := range []graphReadMode{graphReadFresh, graphReadAfter} {
		name := "Fresh"
		if mode == graphReadAfter {
			name = "After"
		}
		t.Run(name, func(t *testing.T) { nativeTransitionScopeCase(t, mode) })
	}
}

func TestNativeP01ScopedCallerLeavesHeldSourceWorkerAndOriginalReadersIntact(t *testing.T) {
	for _, tc := range []struct {
		campaign bool
		program  *nativeProtocolProgramSpec
		source   uint64
		want     uint64
	}{
		{false, nil, 1, 3},
		{true, nil, 1, 2},
		{true, new(nativeProtocolProgramSpec{1, "original", "literal"}), 1, 2},
		{true, new(nativeProtocolProgramSpec{1, "rotated", "reorder"}), 2, 3},
	} {
		caller, err := nativeTransitionScopeCaller(tc.campaign, tc.program, tc.source)
		if err != nil || caller != tc.want || caller == tc.source {
			t.Fatal("blocked fresh caller consumes held-source worker", caller, tc.want, err)
		}
	}
	for _, layout := range []string{"original", "rotated"} {
		for _, message := range []string{"literal", "delay", "reorder", "duplicate"} {
			program := nativeProtocolProgramSpec{1, layout, message}
			source := uint64(1)
			if layout == "rotated" {
				source = 2
			}
			caller, err := nativeTransitionScopeCaller(true, &program, source)
			if err != nil || caller == source || caller < 2 || caller > 3 || processSourceWorkers != 2 {
				t.Fatal("actual two-worker P01 topology", caller, source, err)
			}
		}
	}
	for _, tc := range []struct {
		campaign bool
		program  *nativeProtocolProgramSpec
		source   uint64
	}{
		{true, nil, 0},
		{true, nil, 4},
		{false, new(nativeProtocolProgramSpec{1, "rotated", "literal"}), 2},
		{true, new(nativeProtocolProgramSpec{1, "renumber-only", "literal"}), 2},
	} {
		if caller, err := nativeTransitionScopeCaller(tc.campaign, tc.program, tc.source); caller != 0 || !errors.Is(err, errInvalid) {
			t.Fatal("unbound caller/source profile admitted", caller, err)
		}
	}
}
