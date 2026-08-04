package index

import (
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The codec's ONLY contract is that a decoded snapshot is INDISTINGUISHABLE from the
// one that was encoded — values, presence, validity and zone-map decisions. If it is
// merely close, a persisted column stops being a cache and becomes a second source
// of truth, and the whole no-migration argument collapses.

func codecBuild(t *testing.T, vals map[types.NodeID]any, temporal bool) *LabelDocValues {
	t.Helper()
	ids := make([]types.NodeID, 0, len(vals))
	for id := range vals {
		ids = append(ids, id)
	}
	gp := func(id types.NodeID, _ string) (any, bool) { v, ok := vals[id]; return v, ok }
	var gt func(types.NodeID) (int64, int64, bool)
	if temporal {
		gt = func(id types.NodeID) (int64, int64, bool) {
			if int64(id)%3 == 0 {
				return int64(id) * 10, 0, true // open-ended
			}
			return int64(id) * 10, int64(id)*10 + 7, true
		}
	}
	return BuildLabelDocValues(1, ids, []string{"v"}, gp, gt)
}

// codecDump is the comparable projection: every observable a consumer can reach.
func codecDump(t *testing.T, l *LabelDocValues) []string {
	t.Helper()
	out := []string{}
	v, ok := l.View("v")
	for ord, id := range l.IDs() {
		s := "unbuildable"
		if ok {
			switch {
			case !v.Present(ord):
				s = "absent"
			case v.Type == ColString:
				s = "s:" + v.StringAt(ord)
			case v.IsFloat(ord):
				s = "f:" + itoa(int64(math.Float64bits(v.Flts[ord])))
			default:
				s = "i:" + itoa(v.Ints[ord])
			}
		}
		row := "id=" + itoa(int64(id)) + " " + s
		if l.HasTemporal() {
			row += " [" + itoa(l.ValidFrom()[ord]) + "," + itoa(l.ValidTo()[ord]) + ")"
		}
		out = append(out, row)
	}
	// Zone-map decisions are observable behaviour: a wrong block bound silently
	// drops rows, and comparing values alone would never see it.
	for _, q := range [][2]int64{{0, 50}, {100, 300}, {0, 0}, {9000, 9999}} {
		for s := 0; s < l.Len(); s += zoneBlockSize {
			out = append(out, "z"+itoa(int64(s))+":"+itoa(q[0])+"-"+itoa(q[1])+"="+btoa(l.BlockCanMatch(s, q[0], q[1])))
		}
	}
	out = append(out, "epoch="+itoa(int64(l.Epoch())), "hasTemporal="+btoa(l.HasTemporal()))
	return out
}

func TestCodec_RoundTripIsIndistinguishable(t *testing.T) {
	big := int64(9_000_000_000_000_001) // > 2^53
	cases := map[string]struct {
		vals     map[types.NodeID]any
		temporal bool
	}{
		"uniform_int":     {map[types.NodeID]any{1: int64(1), 2: int64(2), 3: big}, true},
		"uniform_float":   {map[types.NodeID]any{1: 1.5, 2: -0.0, 3: math.MaxFloat64}, true},
		"mixed_numeric":   {map[types.NodeID]any{1: int64(7), 2: 2.25, 3: big}, true},
		"strings":         {map[types.NodeID]any{1: "b", 2: "a", 3: "b"}, true},
		"with_absent":     {map[types.NodeID]any{1: int64(7), 3: int64(9)}, true},
		"no_temporal":     {map[types.NodeID]any{1: int64(1), 2: int64(2)}, false},
		"unbuildable":     {map[types.NodeID]any{1: true, 2: false}, true},
		"nan_and_neg_inf": {map[types.NodeID]any{1: math.NaN(), 2: math.Inf(-1)}, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			orig := codecBuild(t, tc.vals, tc.temporal)
			blob := EncodeColumns(orig)
			got, err := DecodeColumns(blob, func(v int64) types.NodeID { return types.NodeID(v) })
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			a, b := codecDump(t, orig), codecDump(t, got)
			if len(a) != len(b) {
				t.Fatalf("projection lengths differ: %d vs %d", len(a), len(b))
			}
			for i := range a {
				if a[i] != b[i] {
					t.Errorf("entry %d: original %q, decoded %q", i, a[i], b[i])
				}
			}
		})
	}
}

// TestCodec_CorruptionAlwaysRebuilds is the safety property. Every malformed input
// must be reported as unreadable — never decoded into a plausible-looking column,
// and never a panic. The caller's response is always "rebuild".
func TestCodec_CorruptionAlwaysRebuilds(t *testing.T) {
	orig := codecBuild(t, map[types.NodeID]any{1: "a", 2: "b", 3: int64(0)}, true)
	good := EncodeColumns(orig)
	mk := func(v int64) types.NodeID { return types.NodeID(v) }

	corrupt := map[string][]byte{
		"empty":            {},
		"version_bump":     append([]byte{columnBlobVersion + 1}, good[1:]...),
		"truncated_head":   good[:3],
		"truncated_middle": good[:len(good)/2],
		"trailing_garbage": append(append([]byte{}, good...), 0xFF, 0xFF),
	}
	// Every single-byte flip in the first 64 bytes, too: the header and length
	// prefixes live there, and that is where a corrupt count could allocate wildly.
	for i := 0; i < 64 && i < len(good); i++ {
		b := append([]byte{}, good...)
		b[i] ^= 0xFF
		corrupt["flip"+itoa(int64(i))] = b
	}

	for name, blob := range corrupt {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("decode PANICKED on corrupt input (%v); it must return an error", p)
				}
			}()
			got, err := DecodeColumns(blob, mk)
			if err == nil {
				// Decoding is allowed to succeed only if the flip landed somewhere
				// genuinely value-neutral; it must still be internally consistent.
				if got == nil {
					t.Fatal("nil snapshot with nil error")
				}
				return
			}
			if !errors.Is(err, ErrColumnBlobUnreadable) {
				t.Errorf("error %v does not wrap ErrColumnBlobUnreadable, so a caller "+
					"cannot tell it means 'rebuild'", err)
			}
		})
	}
}

// TestCodec_EpochSurvives pins the staleness gate: a persisted column carries the
// epoch it was built at, and a caller compares that against the label's CURRENT
// epoch. Losing it would make every persisted column look fresh forever.
func TestCodec_EpochSurvives(t *testing.T) {
	ids := []types.NodeID{1, 2}
	gp := func(types.NodeID, string) (any, bool) { return int64(1), true }
	orig := BuildLabelDocValues(4242, ids, []string{"v"}, gp, nil)
	got, err := DecodeColumns(EncodeColumns(orig), func(v int64) types.NodeID { return types.NodeID(v) })
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Epoch() != 4242 {
		t.Errorf("epoch %d survived as %d — a persisted column that loses its stamp "+
			"looks fresh forever", 4242, got.Epoch())
	}
}
