package graphstore

import (
	"bytes"
	"context"
	"errors"
	"math"
	"math/big"
	"testing"
	"unsafe"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func cpTestAxis(t *testing.T, profile temporal.Profile, tag byte) (temporal.Axis, currentPresenceAxis) {
	t.Helper()
	id := temporal.AxisID{tag}
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: id, Profile: profile, Version: 1, Reference: "test-reference", CanonicalUnit: "step"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return a, currentPresenceAxis{id, a.DefinitionHash(), profile}
}
func cpTestPosition(t *testing.T, a temporal.Axis, text string, micro int64) temporal.Position {
	t.Helper()
	var p temporal.Position
	var err error
	if a.Descriptor().Profile == temporal.ProfileIntegerZ {
		n, e := temporal.ParseInteger(text, temporal.Limits{})
		if e != nil {
			t.Fatal(e)
		}
		p, err = temporal.IntegerPosition(a, n)
	} else {
		q, e := temporal.ParseRational(text, temporal.Limits{})
		if e != nil {
			t.Fatal(e)
		}
		if a.Descriptor().Profile == temporal.ProfileRationalQ {
			p, err = temporal.RationalPosition(a, q)
		} else {
			p, err = temporal.LexPosition(a, q, temporal.Int64(micro))
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func cpTestBody(t *testing.T, p temporal.Position, l temporal.Limits) []byte {
	t.Helper()
	wire, err := temporal.AppendPosition(nil, p, l)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(wire[52:])
}
func cpTestPointPage(t *testing.T, profile temporal.Profile) currentPresencePage {
	t.Helper()
	a, ref := cpTestAxis(t, profile, 1)
	body := cpTestBody(t, cpTestPosition(t, a, "75", 0), temporal.Limits{})
	return currentPresencePage{id: 7, axes: []currentPresenceAxis{ref}, groups: []currentPresenceGroup{{endpoint: 1, bound: 11, axis: 0, mode: graphstate.LifeBound}}, rows: []currentPresenceRow{{relationship: 3, life: 31, flags: cpPoint | cpSource}}, wire: body}
}
func cpTestNamespace() Namespace { return Namespace{Graph: graphstate.GraphID{1}, Partition: 1} }

func TestCurrentPresenceConcreteLedger(t *testing.T) {
	for _, v := range []struct {
		name   string
		size   uintptr
		charge int
	}{
		{"page", unsafe.Sizeof(currentPresencePage{}), cpPageOwned}, {"axis", unsafe.Sizeof(currentPresenceAxis{}), cpAxisOwned}, {"group", unsafe.Sizeof(currentPresenceGroup{}), cpGroupOwned}, {"row", unsafe.Sizeof(currentPresenceRow{}), cpRowOwned}, {"child", unsafe.Sizeof(currentPresenceChild{}), cpChildOwned},
	} {
		if int(v.size) > v.charge {
			t.Fatalf("%s size%d charge%d", v.name, v.size, v.charge)
		}
	}
	p := cpTestPointPage(t, temporal.ProfileIntegerZ)
	p.rows = make([]currentPresenceRow, 1, 128)
	p.rows[0] = currentPresenceRow{relationship: 3, life: 31, flags: cpPoint | cpSource}
	p.wire = append(make([]byte, 0, 4096), p.wire...)
	l := defaultCurrentPresenceLimits()
	u, err := cpValidatePage(t.Context(), p, l)
	if err != nil {
		t.Fatal(err)
	}
	want := cpPageOwned + 4096 + cpAxisOwned + cpGroupOwned + 128*cpRowOwned
	if u.OwnedBytes != want {
		t.Fatalf("capacity charge %d want%d", u.OwnedBytes, want)
	}
	l.maxOwnedBytes = want - 1
	if _, err := cpValidatePage(t.Context(), p, l); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	l.maxOwnedBytes = want
	if _, err := cpValidatePage(t.Context(), p, l); err != nil {
		t.Fatal(err)
	}
	l.maxScratchBytes = u.ScratchBytes - 1
	if _, err := cpValidatePage(t.Context(), p, l); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	l.maxScratchBytes = u.ScratchBytes
	if _, err := cpValidatePage(t.Context(), p, l); err != nil {
		t.Fatal(err)
	}
	var nilCtx context.Context
	if err := cpCheckContext(nilCtx); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
func TestCurrentPresenceExactCoordinateOrdering(t *testing.T) {
	cases := []struct {
		profile temporal.Profile
		values  []string
		micro   []int64
	}{
		{temporal.ProfileIntegerZ, []string{"-340282366920938463463374607431768211457", "-100", "-1", "0", "1", "100", "340282366920938463463374607431768211457"}, nil},
		{temporal.ProfileRationalQ, []string{"-340282366920938463463374607431768211457/3", "-2/3", "-1/3", "0", "1/3", "2/3", "340282366920938463463374607431768211457/3"}, nil},
		{temporal.ProfileLexicographicQN, []string{"-1/3", "0", "0", "0", "1/3"}, []int64{0, 0, 1, math.MaxInt64, 0}},
	}
	for _, tc := range cases {
		t.Run(cpProfileName(tc.profile), func(t *testing.T) {
			a, _ := cpTestAxis(t, tc.profile, 1)
			var bodies [][]byte
			var positions []temporal.Position
			for i, text := range tc.values {
				micro := int64(0)
				if tc.micro != nil {
					micro = tc.micro[i]
				}
				p := cpTestPosition(t, a, text, micro)
				positions = append(positions, p)
				bodies = append(bodies, cpTestBody(t, p, temporal.Limits{}))
			}
			wrongByteOrder := false
			for i := range positions {
				for j := range positions {
					left, e := cpReadCoordinate(bodies[i], 0, tc.profile, temporal.Limits{})
					if e != nil {
						t.Fatal(e)
					}
					right, e := cpReadCoordinate(bodies[j], 0, tc.profile, temporal.Limits{})
					if e != nil {
						t.Fatal(e)
					}
					want, e := temporal.ComparePositions(positions[i], positions[j], temporal.Limits{})
					if e != nil {
						t.Fatal(e)
					}
					got := cpCompareCoordinate(left, right, tc.profile)
					if (got < 0) != (want < 0) || (got > 0) != (want > 0) {
						t.Fatalf("%s vs%s got%d want%d", tc.values[i], tc.values[j], got, want)
					}
					if (bytes.Compare(bodies[i], bodies[j]) < 0) != (want < 0) {
						wrongByteOrder = true
					}
				}
			}
			if tc.profile != temporal.ProfileLexicographicQN && !wrongByteOrder {
				t.Fatal("control did not distinguish byte ordering")
			}
		})
	}
}

// Profiles are private codec enum data, not a dynamic comparison dispatch.
func cpProfileName(p temporal.Profile) string {
	switch p {
	case temporal.ProfileIntegerZ:
		return "Z"
	case temporal.ProfileRationalQ:
		return "Q"
	default:
		return "QxN"
	}
}
func TestCurrentPresenceNumericWidthsAndCanonicality(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		t.Run(cpProfileName(profile), func(t *testing.T) {
			a, _ := cpTestAxis(t, profile, 1)
			limits := temporal.DefaultLimits()
			limits.MaxMagnitudeBits = 65536
			limits.MaxInputBytes = 1 << 20
			limits.MaxValueBytes = 1 << 20
			wideText := new(big.Int).Lsh(big.NewInt(1), 65535).String()
			wide, e := temporal.ParseInteger(wideText, limits)
			if e != nil {
				t.Fatal(e)
			}
			var p temporal.Position
			if profile == temporal.ProfileIntegerZ {
				p, e = temporal.IntegerPosition(a, wide)
			} else {
				q, err := temporal.Fraction(wide, temporal.Int64(3), limits)
				if err != nil {
					t.Fatal(err)
				}
				if profile == temporal.ProfileRationalQ {
					p, e = temporal.RationalPosition(a, q)
				} else {
					p, e = temporal.LexPosition(a, q, wide)
				}
			}
			if e != nil {
				t.Fatal(e)
			}
			body := cpTestBody(t, p, limits)
			got, err := cpReadCoordinate(body, 0, profile, limits)
			if err != nil || got.end != len(body) {
				t.Fatalf("max width %v", err)
			}
			limits.MaxMagnitudeBits = 65535
			if _, err := cpReadCoordinate(body, 0, profile, limits); !errors.Is(err, ErrResourceLimit) || !errors.Is(err, temporal.ErrResourceLimit) {
				t.Fatal(err)
			}
			limits.MaxMagnitudeBits = 65536
			limits.MaxValueBytes = len(body) + 51
			if _, err := cpReadCoordinate(body, 0, profile, limits); !errors.Is(err, ErrResourceLimit) {
				t.Fatal(err)
			}
			limits.MaxValueBytes++
			if _, err := cpReadCoordinate(body, 0, profile, limits); err != nil {
				t.Fatal(err)
			}
		})
	}
	malformed := [][]byte{{3, 0, 0}, {1, 0, 0}, {0, 0, 1, 1}, {1, 0, 1, 0}, {1, 0, 2, 1}, {1}}
	for _, body := range malformed {
		if _, err := cpReadCoordinate(body, 0, temporal.ProfileIntegerZ, temporal.Limits{}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%x: %v", body, err)
		}
	}
	for _, body := range [][]byte{{0, 0, 0, 1, 0, 1, 2}, {1, 0, 1, 2, 1, 0, 1, 4}, {1, 0, 1, 1, 2, 0, 1, 1}, {1, 0, 1, 1, 0, 0, 0}} {
		if _, err := cpReadCoordinate(body, 0, temporal.ProfileRationalQ, temporal.Limits{}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Q %x: %v", body, err)
		}
	}
	if _, err := cpReadCoordinate([]byte{0, 0, 0, 1, 0, 1, 1, 2, 0, 1, 1}, 0, temporal.ProfileLexicographicQN, temporal.Limits{}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := cpReadCoordinate(nil, 1, temporal.ProfileIntegerZ, temporal.Limits{}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := cpReadCoordinate([]byte{0, 0, 0}, 0, 0, temporal.Limits{}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestCurrentPresenceCoordinateValueCapPrecedesReduction(t *testing.T) {
	// 2/4 is noncanonical, but an insufficient input/value allowance must refuse
	// before the big.Int GCD/reduction path. Full allowance then reports corruption.
	body := []byte{1, 0, 1, 2, 1, 0, 1, 4}
	l := temporal.DefaultLimits()
	l.MaxValueBytes = len(body) + 51
	if _, err := cpReadCoordinate(body, 0, temporal.ProfileRationalQ, l); !errors.Is(err, ErrResourceLimit) || !errors.Is(err, temporal.ErrResourceLimit) || errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	l.MaxValueBytes++
	if _, err := cpReadCoordinate(body, 0, temporal.ProfileRationalQ, l); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}
