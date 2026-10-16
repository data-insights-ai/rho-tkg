package graphapply

import (
	"bytes"
	"errors"
	"slices"
	"testing"
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

func TestNativeP01OutsideDataScopeFreshAndAfterResolveOrRefuseActualCoordinator(t *testing.T) {
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
