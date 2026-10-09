package state

import (
	"bytes"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func FuzzStateCodec(f *testing.F) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(f, profile)
		s, err := New(a, Limits{})
		if err != nil {
			f.Fatal(err)
		}
		empty, err := AppendState(nil, s, CodecLimits{})
		if err != nil {
			f.Fatal(err)
		}
		f.Add(empty, byte(profile), false)
		empty, err = AppendChanges(nil, a, nil, CodecLimits{})
		if err != nil {
			f.Fatal(err)
		}
		f.Add(empty, byte(profile), true)
		current, changes, l := codecFixture(f, profile, 3)
		wire, err := AppendState(nil, current, l)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(wire, byte(profile), false)
		wire, err = AppendChanges(nil, a, changes, l)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(wire, byte(profile), true)
	}
	f.Add([]byte{}, byte(1), false)
	f.Fuzz(func(t *testing.T, wire []byte, p byte, changes bool) {
		profile := temporal.Profile(p%3 + 1)
		// Seed values use 1/2/3 directly, preserving their declared axes.
		if p >= 1 && p <= 3 {
			profile = temporal.Profile(p)
		}
		a := axis(t, profile)
		l := CodecLimits{State: Limits{MaxPieces: 64, MaxChangePieces: 64, MaxMetadataBytes: 16384, MaxChangeMetadataBytes: 16384, MaxReferencedBytes: 4096}, MaxEncodedBytes: 16384}
		var canonical []byte
		var err error
		if changes {
			decoded, usage, e := DecodeChanges(wire, a, l)
			if e != nil {
				return
			}
			canonical, err = AppendChanges(nil, a, decoded, l)
			if err != nil || !bytes.Equal(wire, canonical) {
				t.Fatal("accepted changes not canonical", err)
			}
			expected := initialUsage(a)
			for _, c := range decoded {
				expected.pieces++
				expected.metadataBytes += c.scopeBytes + 2*cellMetadataBytes
				expected.references += c.before.references() + c.after.references()
			}
			if usage != expected {
				t.Fatal("change ledger drift", usage, expected)
			}
			clear(wire)
			owned, err := AppendChanges(nil, a, decoded, l)
			if err != nil || !bytes.Equal(owned, canonical) {
				t.Fatal("changes alias input", err)
			}
		} else {
			decoded, e := DecodeState(wire, a, l)
			if e != nil {
				return
			}
			canonical, err = AppendState(nil, decoded, l)
			if err != nil || !bytes.Equal(wire, canonical) {
				t.Fatal("accepted state not canonical", err)
			}
			clear(wire)
			owned, err := AppendState(nil, decoded, l)
			if err != nil || !bytes.Equal(owned, canonical) {
				t.Fatal("state aliases input", err)
			}
		}
	})
}
