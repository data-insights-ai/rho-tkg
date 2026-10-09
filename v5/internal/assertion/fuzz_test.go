package assertion

import (
	"bytes"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func FuzzRecordCanonicalRoundTrip(f *testing.F) {
	axis := testAxis(f, temporal.ProfileRationalQ, 1, "fuzz")
	for _, p := range []Placement{
		{Kind: NoAssociation},
		{Kind: NativePlacement, Native: testPoint(f, axis, 12)},
		{Kind: SymbolicPlacement, Symbolic: testDescriptor(f, 1, []byte("preserve"))},
	} {
		f.Add(recordWire(f, mustRecord(f, testSpec(f, 1, p))))
	}
	point := testPoint(f, axis, 12)
	hard, err := temporal.HardPointKnowledge(point, temporal.Limits{})
	if err != nil {
		f.Fatal(err)
	}
	opaque, err := temporal.OpaquePointKnowledge(axis, testDescriptor(f, 2, []byte("shared-x")), temporal.Limits{})
	if err != nil {
		f.Fatal(err)
	}
	for _, k := range []temporal.PointKnowledge{hard, opaque} {
		s := testSpec(f, 2, Placement{Kind: NativePlacement, Native: point})
		s.Knowledge = Knowledge{Kind: PointKnowledge, Point: k}
		f.Add(recordWire(f, mustRecord(f, s)))
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 4096 {
			return
		}
		before := bytes.Clone(input)
		for _, expected := range []temporal.Axis{axis, {}} {
			r, err := DecodeRecord(input, expected, Limits{MaxRecordBytes: 4096})
			if !bytes.Equal(before, input) {
				t.Fatal("decode changed source bytes")
			}
			if err != nil {
				if r.Spec().Ref != (Ref{}) {
					t.Fatal("decode refusal returned partial record")
				}
				continue
			}
			wire, err := AppendRecord(nil, r, Limits{MaxRecordBytes: 4096})
			if err != nil || !bytes.Equal(input, wire) {
				t.Fatal("accepted noncanonical input", err)
			}
			x, err := Apply(r, r.Spec(), Limits{MaxRecordBytes: 4096})
			if err != nil || !x.Replay() || !bytes.Equal(wire, recordWire(t, x.After())) {
				t.Fatal("accepted record did not exactly replay", err)
			}
		}
	})
}
