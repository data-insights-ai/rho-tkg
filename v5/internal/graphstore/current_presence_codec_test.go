package graphstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func cpRoundTrip(t *testing.T, p currentPresencePage) (currentPresencePage, []byte, currentPresenceUsage) {
	t.Helper()
	l := defaultCurrentPresenceLimits()
	wire, _, err := encodeCurrentPresencePage(t.Context(), cpTestNamespace(), p, l)
	if err != nil {
		t.Fatal(err)
	}
	got, u, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), p.id, currentPresenceDigest(wire), wire, l)
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := encodeCurrentPresencePage(t.Context(), cpTestNamespace(), got, l)
	if err != nil || !bytes.Equal(wire, again) {
		t.Fatalf("noncanonical roundtrip %v", err)
	}
	return got, wire, u
}
func cpAddBody(t *testing.T, p *currentPresencePage, axis temporal.Axis, text string, micro int64) uint32 {
	t.Helper()
	offset := uint32(len(p.wire))
	p.wire = append(p.wire, cpTestBody(t, cpTestPosition(t, axis, text, micro), temporal.Limits{})...)
	return offset
}
func TestCurrentPresenceCompactRoundtripAndOwnership(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		t.Run(cpProfileName(profile), func(t *testing.T) {
			p := cpTestPointPage(t, profile)
			p.rows[0].flags |= cpTarget
			got, wire, u := cpRoundTrip(t, p)
			if len(got.rows) != 1 || got.rows[0].relationship != 3 || got.rows[0].life != 31 || got.rows[0].flags != cpPoint|cpSource|cpTarget || len(got.axes) != 1 || len(got.groups) != 1 {
				t.Fatal("wrong exact posting")
			}
			expected := 43 + 49 + 12 + 10 + 17 + len(p.wire)
			if len(wire) != expected {
				t.Fatalf("wire%d want%d", len(wire), expected)
			}
			if u.OwnedBytes != cpOwnedBytes(len(wire), 1, 1, 1, 0) {
				t.Fatal(u)
			}
			before := bytes.Clone(got.wire)
			clear(wire)
			if !bytes.Equal(got.wire, before) {
				t.Fatal("borrowed page alias")
			}
			empty := currentPresencePage{id: 1}
			read, _, _ := cpRoundTrip(t, empty)
			s, err := read.summary(defaultCurrentPresenceLimits())
			if err != nil || s.count != 0 || !s.mixed {
				t.Fatal(s, err)
			}
		})
	}
	// A full small-Z point page has128 rows and shares axis/group metadata once.
	p := cpTestPointPage(t, temporal.ProfileIntegerZ)
	p.rows = make([]currentPresenceRow, 128)
	for i := range p.rows {
		p.rows[i] = currentPresenceRow{relationship: graphstate.EntityID(i + 3), life: 31, flags: cpPoint | cpSource}
	}
	got, wire, u := cpRoundTrip(t, p)
	if len(wire) != 2802 || len(got.rows) != 128 || u.OwnedBytes > 7500 {
		t.Fatalf("compact page wire%d ledger%+v", len(wire), u)
	}
	for i, r := range got.rows {
		if r.relationship != graphstate.EntityID(i+3) || r.life != 31 {
			t.Fatal("omission/phantom")
		}
	}
}
func TestCurrentPresenceSharedGroupsAndMixedAxes(t *testing.T) {
	a, ar := cpTestAxis(t, temporal.ProfileIntegerZ, 1)
	b, br := cpTestAxis(t, temporal.ProfileRationalQ, 2)
	p := currentPresencePage{id: 7, axes: []currentPresenceAxis{ar, br}, groups: []currentPresenceGroup{
		{1, 11, 0, graphstate.LifeBound}, {1, 12, 0, graphstate.LifeBound}, {1, 13, 0, graphstate.LifeBound}, {1, 14, 0, graphstate.LifeBound}, {1, 0, 0, graphstate.IdentityReference}, {1, 11, 1, graphstate.LifeBound}, {2, 11, 0, graphstate.LifeBound},
	}}
	for i, g := range p.groups {
		axis := a
		if g.axis == 1 {
			axis = b
		}
		lo := cpAddBody(t, &p, axis, "-1", 0)
		hi := cpAddBody(t, &p, axis, "2", 0)
		p.rows = append(p.rows, currentPresenceRow{relationship: graphstate.EntityID(i + 3), life: 31, lower: lo, upper: hi, group: uint16(i), flags: cpLowerClosed | cpTarget})
	}
	got, _, _ := cpRoundTrip(t, p)
	if !slices.Equal(p.groups, got.groups) {
		t.Fatal("group binding changed")
	}
	s, err := got.summary(defaultCurrentPresenceLimits())
	if err != nil || !s.mixed || s.min != (currentPresenceExtent{}) || s.max != (currentPresenceExtent{}) {
		t.Fatal(s, err)
	}
	// Mixed axes must decline pruning even when a queried coordinate is beyond
	// every bound on one axis; it must never compare the other reference system.
	other := cpTestBody(t, cpTestPosition(t, a, "999", 0), temporal.Limits{})
	ok, err := cpSummaryMayContain(got, s, ar, other, defaultCurrentPresenceLimits())
	if err != nil || !ok {
		t.Fatal("unsafe mixed-axis prune", err)
	}
}
func TestCurrentPresenceExtremaAcrossLivesAndOpenBounds(t *testing.T) {
	a, ref := cpTestAxis(t, temporal.ProfileRationalQ, 1)
	p := currentPresencePage{id: 7, axes: []currentPresenceAxis{ref}, groups: []currentPresenceGroup{{1, 11, 0, graphstate.LifeBound}, {1, 12, 0, graphstate.LifeBound}}}
	// First key belongs to life11 at100; later life12 begins at-100. Key fences
	// therefore cannot stand in for all-life temporal extrema.
	for i, v := range []struct {
		lo, hi string
		flags  byte
	}{{"100", "200", cpLowerClosed | cpSource}, {"-100", "-50", cpUpperClosed | cpTarget}} {
		p.rows = append(p.rows, currentPresenceRow{relationship: graphstate.EntityID(i + 3), life: 31, lower: cpAddBody(t, &p, a, v.lo, 0), upper: cpAddBody(t, &p, a, v.hi, 0), group: uint16(i), flags: v.flags})
	}
	got, _, _ := cpRoundTrip(t, p)
	s, err := got.summary(defaultCurrentPresenceLimits())
	if err != nil || s.mixed {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		at   string
		want bool
	}{{"-101", false}, {"-100", false}, {"-99", true}, {"0", true}, {"100", true}, {"199", true}, {"200", false}, {"201", false}} {
		at := cpTestBody(t, cpTestPosition(t, a, tc.at, 0), temporal.Limits{})
		ok, err := cpSummaryMayContain(got, s, ref, at, defaultCurrentPresenceLimits())
		if err != nil || ok != tc.want {
			t.Fatalf("at%s got%v want%v %v", tc.at, ok, tc.want, err)
		}
	}
	// Sound bounding boxes may include gaps (0 above); this is a candidate
	// summary, not a certificate that a relationship is present/effective.
	p.rows[0].flags = cpLowerInfinite | cpUpperInfinite | cpSource
	p.rows[0].lower, p.rows[0].upper = 0, 0
	p.rows = p.rows[:1]
	p.groups = p.groups[:1]
	got, _, _ = cpRoundTrip(t, p)
	s, err = got.summary(defaultCurrentPresenceLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []string{"-1000", "0", "1000"} {
		ok, err := cpSummaryMayContain(got, s, ref, cpTestBody(t, cpTestPosition(t, a, at, 0), temporal.Limits{}), defaultCurrentPresenceLimits())
		if err != nil || !ok {
			t.Fatal(err)
		}
	}
}
func cpTestParent(t *testing.T) (currentPresencePage, currentPresencePage, currentPresencePage) {
	t.Helper()
	left := cpTestPointPage(t, temporal.ProfileIntegerZ)
	right := cpTestPointPage(t, temporal.ProfileIntegerZ)
	right.id = 8
	right.rows[0].relationship = 4
	left, _, _ = cpRoundTrip(t, left)
	right, _, _ = cpRoundTrip(t, right)
	first, err := left.summary(defaultCurrentPresenceLimits())
	if err != nil {
		t.Fatal(err)
	}
	second, err := right.summary(defaultCurrentPresenceLimits())
	if err != nil {
		t.Fatal(err)
	}
	// For equal coordinate bodies, the leaf references are identical offsets.
	parent := currentPresencePage{id: 9, level: 1, axes: slices.Clone(left.axes), groups: slices.Clone(left.groups), children: []currentPresenceChild{first, second}, wire: bytes.Clone(left.wire)}
	parent, _, _ = cpRoundTrip(t, parent)
	return parent, left, right
}
func TestCurrentPresenceParentAuthenticatedSummaries(t *testing.T) {
	p, left, right := cpTestParent(t)
	l := defaultCurrentPresenceLimits()
	for i, ch := range []currentPresencePage{left, right} {
		if err := verifyCurrentPresenceChild(p, p.children[i], ch, l); err != nil {
			t.Fatal(err)
		}
	}
	s, err := p.summary(l)
	if err != nil || s.count != 2 || s.mixed {
		t.Fatal(s, err)
	}
	cases := []func(*currentPresenceChild){func(c *currentPresenceChild) { c.count++ }, func(c *currentPresenceChild) { c.id++ }, func(c *currentPresenceChild) { c.digest[0] ^= 1 }, func(c *currentPresenceChild) { c.first.relationship++ }, func(c *currentPresenceChild) { c.max.flags = 0 }, func(c *currentPresenceChild) { c.mixed = true }}
	for _, mutate := range cases {
		expected := p.children[0]
		mutate(&expected)
		if err := verifyCurrentPresenceChild(p, expected, left, l); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	// A changed leaf with a newly valid digest still fails the original parent.
	changed := cpTestPointPage(t, temporal.ProfileIntegerZ)
	changed.rows[0].flags |= cpTarget
	changed, _, _ = cpRoundTrip(t, changed)
	if err := verifyCurrentPresenceChild(p, p.children[0], changed, l); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	// A mixed summary has no extrema bytes; it never certifies endpoint validity.
	p.children[0].mixed = true
	p.children[0].axis = 0
	p.children[0].min = currentPresenceExtent{}
	p.children[0].max = currentPresenceExtent{}
	mixed, _, _ := cpRoundTrip(t, p)
	s, err = mixed.summary(l)
	if err != nil || !s.mixed {
		t.Fatal(err)
	}
	p.children[0].count = math.MaxUint64
	p.children[1].count = 1
	if _, err := cpValidatePage(t.Context(), p, l); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}
func TestCurrentPresenceCodecAdversarialFraming(t *testing.T) {
	p := cpTestPointPage(t, temporal.ProfileIntegerZ)
	_, wire, _ := cpRoundTrip(t, p)
	l := defaultCurrentPresenceLimits()
	decode := func(src []byte) error {
		_, _, e := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), 7, currentPresenceDigest(src), src, l)
		return e
	}
	for size := range len(wire) {
		if err := decode(wire[:size]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncation%d %v", size, err)
		}
	}
	mutations := []struct {
		name string
		fn   func([]byte) []byte
	}{
		{"family", func(b []byte) []byte { b[3] = byte(declaredIncidentRecord); return b }},
		{"namespace", func(b []byte) []byte { b[4] ^= 1; return b }},
		{"self", func(b []byte) []byte { b[35]++; return b }},
		{"version", func(b []byte) []byte { b[36]++; return b }},
		{"level", func(b []byte) []byte { b[37] = 8; return b }},
		{"count", func(b []byte) []byte { binary.BigEndian.PutUint16(b[38:40], 129); return b }},
		{"axes", func(b []byte) []byte { b[40] = 129; return b }},
		{"groups", func(b []byte) []byte { binary.BigEndian.PutUint16(b[41:43], 129); return b }},
		{"profile", func(b []byte) []byte { b[91] = 4; return b }},
		{"axisID", func(b []byte) []byte { clear(b[43:59]); return b }},
		{"axisHash", func(b []byte) []byte { clear(b[59:91]); return b }},
		{"emptyRun", func(b []byte) []byte { clear(b[102:104]); return b }},
		{"axisSlot", func(b []byte) []byte { b[100] = 128; return b }},
		{"mode", func(b []byte) []byte { b[101] = 0; return b }},
		{"boundLife", func(b []byte) []byte { clear(b[104:112]); return b }},
		{"rowsMismatch", func(b []byte) []byte { clear(b[112:114]); return b }},
		{"emptyEndpoint", func(b []byte) []byte { clear(b[92:100]); return b }},
		{"emptyRel", func(b []byte) []byte { clear(b[114:122]); return b }},
		{"emptyLife", func(b []byte) []byte { clear(b[122:130]); return b }},
		{"role", func(b []byte) []byte { b[130] = cpPoint; return b }},
		{"pointExtraFlags", func(b []byte) []byte { b[130] |= cpUpperClosed; return b }},
		{"unknownFlags", func(b []byte) []byte { b[130] |= 128; return b }},
		{"noncanonicalInteger", func(b []byte) []byte { b[len(b)-1] = 0; return b }},
		{"trailing", func(b []byte) []byte { return append(b, 0) }},
	}
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			if err := decode(m.fn(bytes.Clone(wire))); !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
		})
	}
	if _, _, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), 7, [32]byte{}, wire, l); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	stale := currentPresenceDigest(wire)
	changed := bytes.Clone(wire)
	changed[len(changed)-1]++
	if _, _, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), 7, stale, changed, l); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

type cpCancelAfter struct {
	context.Context
	calls, after int
}

func (c *cpCancelAfter) Err() error {
	c.calls++
	if c.calls >= c.after {
		return context.Canceled
	}
	return nil
}
func TestCurrentPresenceCodecBudgetsAndCancellation(t *testing.T) {
	p := cpTestPointPage(t, temporal.ProfileIntegerZ)
	_, wire, u := cpRoundTrip(t, p)
	l := defaultCurrentPresenceLimits()
	for _, delta := range []int{-1, 0, 1} {
		limit := l
		limit.maxPageBytes = len(wire) + delta
		out, _, err := encodeCurrentPresencePage(t.Context(), cpTestNamespace(), p, limit)
		if delta < 0 {
			if !errors.Is(err, ErrResourceLimit) || out != nil {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		got, usage, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), 7, currentPresenceDigest(wire), wire, limit)
		if delta < 0 {
			if !errors.Is(err, ErrResourceLimit) || got.id != 0 || usage != (currentPresenceUsage{}) {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	for _, delta := range []int{-1, 0, 1} {
		limit := l
		limit.maxOwnedBytes = u.OwnedBytes + delta
		got, usage, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), 7, currentPresenceDigest(wire), wire, limit)
		if delta < 0 {
			if !errors.Is(err, ErrResourceLimit) || got.id != 0 || usage != (currentPresenceUsage{}) {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	before := bytes.Clone(p.wire)
	ctx := &cpCancelAfter{Context: t.Context(), after: 3}
	out, usage, err := encodeCurrentPresencePage(ctx, cpTestNamespace(), p, l)
	if !errors.Is(err, context.Canceled) || out != nil || usage != (currentPresenceUsage{}) || !bytes.Equal(before, p.wire) {
		t.Fatal("partial cancelled encode", err)
	}
	ctx = &cpCancelAfter{Context: t.Context(), after: 4}
	got, usage, err := decodeCurrentPresencePage(ctx, cpTestNamespace(), 7, currentPresenceDigest(wire), wire, l)
	if !errors.Is(err, context.Canceled) || got.id != 0 || usage != (currentPresenceUsage{}) {
		t.Fatal("partial cancelled decode", err)
	}
	var nilCtx context.Context
	if _, _, err := decodeCurrentPresencePage(nilCtx, cpTestNamespace(), 7, currentPresenceDigest(wire), wire, l); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	l.maxOwnedBytes = 0
	if _, _, err := encodeCurrentPresencePage(t.Context(), cpTestNamespace(), p, l); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestCurrentPresenceOldImageAfterOwnEnding(t *testing.T) {
	a, ref := cpTestAxis(t, temporal.ProfileIntegerZ, 1)
	p := currentPresencePage{id: 7, axes: []currentPresenceAxis{ref}, groups: []currentPresenceGroup{{1, 11, 0, graphstate.LifeBound}}}
	lo := cpAddBody(t, &p, a, "0", 0)
	hi := cpAddBody(t, &p, a, "100", 0)
	p.rows = []currentPresenceRow{{relationship: 3, life: 31, lower: lo, upper: hi, flags: cpLowerClosed | cpSource | cpTarget}}
	old, oldWire, _ := cpRoundTrip(t, p)
	oldBytes := bytes.Clone(oldWire)
	p.rows[0].upper = cpAddBody(t, &p, a, "50", 0)
	current, _, _ := cpRoundTrip(t, p)
	l := defaultCurrentPresenceLimits()
	query := cpTestBody(t, cpTestPosition(t, a, "75", 0), temporal.Limits{})
	for _, tc := range []struct {
		page currentPresencePage
		want bool
	}{{old, true}, {current, false}} {
		sum, err := tc.page.summary(l)
		if err != nil {
			t.Fatal(err)
		}
		ok, err := cpSummaryMayContain(tc.page, sum, ref, query, l)
		if err != nil || ok != tc.want {
			t.Fatalf("got%v want%v %v", ok, tc.want, err)
		}
	}
	reopened, _, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), 7, currentPresenceDigest(oldBytes), oldBytes, l)
	if err != nil {
		t.Fatal(err)
	}
	s, err := reopened.summary(l)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := cpSummaryMayContain(reopened, s, ref, query, l)
	if err != nil || !ok || !bytes.Equal(oldBytes, oldWire) {
		t.Fatal("old image substituted", err)
	}
}
func TestCurrentPresenceRejectsInvalidNativePages(t *testing.T) {
	p, left, _ := cpTestParent(t)
	l := defaultCurrentPresenceLimits()
	cases := []struct {
		name   string
		mutate func(*currentPresencePage)
	}{
		{"cycle", func(p *currentPresencePage) { p.children[0].id = p.id }},
		{"repeatedChild", func(p *currentPresencePage) { p.children[1].id = p.children[0].id }},
		{"missingDigest", func(p *currentPresencePage) { p.children[0].digest = [32]byte{} }},
		{"emptyCount", func(p *currentPresencePage) { p.children[0].count = 0 }},
		{"fenceOrder", func(p *currentPresencePage) { p.children[0].first.relationship = 100 }},
		{"childOrder", func(p *currentPresencePage) { p.children[1].first.relationship = 2 }},
		{"fenceGroup", func(p *currentPresencePage) { p.children[0].first.group = 128 }},
		{"fenceInfinityClosed", func(p *currentPresencePage) { p.children[0].first.flags = cpLowerInfinite | cpLowerClosed }},
		{"badAxis", func(p *currentPresencePage) { p.children[0].axis = 128 }},
		{"badMinimum", func(p *currentPresencePage) { p.children[0].min.flags = cpUpperInfinite }},
		{"badMaximum", func(p *currentPresencePage) { p.children[0].max.flags = cpLowerInfinite }},
		{"mixedExtrema", func(p *currentPresencePage) { p.children[0].mixed = true }},
		{"emptyExtentPoint", func(p *currentPresencePage) { p.children[0].max.flags = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			copy := p
			copy.children = slices.Clone(p.children)
			tc.mutate(&copy)
			wire, usage, err := encodeCurrentPresencePage(t.Context(), cpTestNamespace(), copy, l)
			if !errors.Is(err, ErrCorrupt) || wire != nil || usage != (currentPresenceUsage{}) {
				t.Fatal(err)
			}
		})
	}
	// Lower inclusivity belongs in the semantic fence order before identity IDs.
	leaf := cpTestPointPage(t, temporal.ProfileRationalQ)
	leaf.rows[0].flags = cpLowerClosed | cpSource
	leaf.rows[0].upper = uint32(len(leaf.wire))
	qAxis, _ := cpTestAxis(t, temporal.ProfileRationalQ, 1)
	leaf.wire = append(leaf.wire, cpTestBody(t, cpTestPosition(t, qAxis, "100", 0), temporal.Limits{})...)
	leaf.rows = append(leaf.rows, currentPresenceRow{relationship: 2, life: 31, lower: 0, upper: leaf.rows[0].upper, flags: cpSource})
	got, _, _ := cpRoundTrip(t, leaf)
	if got.rows[0].relationship != 3 || got.rows[1].relationship != 2 {
		t.Fatal("closed/open order uses identity first")
	}
	for _, mutate := range []func(*currentPresencePage){func(p *currentPresencePage) { p.rows[1] = p.rows[0] }, func(p *currentPresencePage) { p.rows[0].upper = p.rows[0].lower }, func(p *currentPresencePage) { p.groups[0].mode = graphstate.IdentityReference }, func(p *currentPresencePage) { p.axes = append(p.axes, p.axes[0]) }} {
		copy := leaf
		copy.rows = slices.Clone(leaf.rows)
		copy.groups = slices.Clone(leaf.groups)
		copy.axes = slices.Clone(leaf.axes)
		mutate(&copy)
		if _, _, err := encodeCurrentPresencePage(t.Context(), cpTestNamespace(), copy, l); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	// Tighter constructor bounds reject before returning decoded table ownership.
	for _, change := range []func(*currentPresenceLimits){func(l *currentPresenceLimits) { l.maxLevels = 1 }, func(l *currentPresenceLimits) {
		l.maxChildren = 2
		l.maxGroups = 1
		l.maxAxes = 1
		l.maxOwnedBytes = 400
	}} {
		limits := l
		change(&limits)
		wire, _, err := encodeCurrentPresencePage(t.Context(), cpTestNamespace(), p, l)
		if err != nil {
			t.Fatal(err)
		}
		got, usage, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), p.id, currentPresenceDigest(wire), wire, limits)
		if !errors.Is(err, ErrResourceLimit) || got.id != 0 || usage != (currentPresenceUsage{}) {
			t.Fatal(err)
		}
	}
	if err := verifyCurrentPresenceChild(p, p.children[0], left, l); err != nil {
		t.Fatal(err)
	}
}
func TestCurrentPresenceSummaryQueryLimitsAndAxes(t *testing.T) {
	p := cpTestPointPage(t, temporal.ProfileIntegerZ)
	p, _, _ = cpRoundTrip(t, p)
	l := defaultCurrentPresenceLimits()
	s, err := p.summary(l)
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Clone(p.wire[p.rows[0].lower:])
	axis := p.axes[0]
	_, other := cpTestAxis(t, temporal.ProfileIntegerZ, 2)
	ok, err := cpSummaryMayContain(p, s, other, body, l)
	if err != nil || ok {
		t.Fatal("cross-axis phantom", err)
	}
	for _, delta := range []int{-1, 0, 1} {
		limit := l
		pageScratch, e := cpScratchBytes(p, l)
		if e != nil {
			t.Fatal(e)
		}
		limit.maxScratchBytes = max(pageScratch, 2048+64*(len(body)+32)) + delta
		ok, err := cpSummaryMayContain(p, s, axis, body, limit)
		if delta < 0 {
			if !errors.Is(err, ErrResourceLimit) || ok {
				t.Fatal(err)
			}
		} else if err != nil || !ok {
			t.Fatal(err)
		}
	}
	for _, bad := range [][]byte{{1}, append(bytes.Clone(body), 0)} {
		ok, err := cpSummaryMayContain(p, s, axis, bad, l)
		if !errors.Is(err, ErrCorrupt) || ok {
			t.Fatal(err)
		}
	}
	limit := l
	limit.temporal.MaxValueBytes = len(body) + 51
	if ok, err := cpSummaryMayContain(p, s, axis, body, limit); !errors.Is(err, ErrResourceLimit) || ok {
		t.Fatal(err)
	}
	limit = l
	limit.temporal.MaxMagnitudeBits = -1
	if _, err := cpValidatePage(t.Context(), p, limit); !errors.Is(err, ErrInvalid) || !errors.Is(err, temporal.ErrInvalidLimits) {
		t.Fatal(err)
	}
}

func TestCurrentPresenceAuthenticatedInternalWireCorruption(t *testing.T) {
	p, _, _ := cpTestParent(t)
	_, wire, _ := cpRoundTrip(t, p)
	l := defaultCurrentPresenceLimits()
	// Offsets are independent golden framing: header43+axis49+group22=114,
	// then child(id8,count8,digest32,first23,last23,uniform1,axis1,min5,max5).
	if len(wire) != 326 {
		t.Fatalf("internal golden framing %d", len(wire))
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"cycle", func(b []byte) { binary.BigEndian.PutUint64(b[114:122], 9) }},
		{"missingDigest", func(b []byte) { clear(b[130:162]) }},
		{"zeroCount", func(b []byte) { clear(b[122:130]) }},
		{"overflow", func(b []byte) { binary.BigEndian.PutUint64(b[122:130], math.MaxUint64) }},
		{"repeatChild", func(b []byte) { binary.BigEndian.PutUint64(b[220:228], 7) }},
		{"minimumBeyondFence", func(b []byte) { b[214] = 76 }},
		{"maximumBeforeFence", func(b []byte) { b[219] = 74 }},
		{"unknownSummary", func(b []byte) { b[208] = 2 }},
		{"wrongSummaryAxis", func(b []byte) { b[209] = 1 }},
		{"fenceFlags", func(b []byte) { b[164] = cpLowerInfinite | cpLowerClosed }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := bytes.Clone(wire)
			tc.mutate(bad)
			got, usage, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), p.id, currentPresenceDigest(bad), bad, l)
			if !errors.Is(err, ErrCorrupt) || got.id != 0 || usage != (currentPresenceUsage{}) {
				t.Fatal(err)
			}
		})
	}
}

func TestCurrentPresenceMaximumWidthPageBudgets(t *testing.T) {
	axis, ref := cpTestAxis(t, temporal.ProfileIntegerZ, 1)
	temporalLimits := temporal.DefaultLimits()
	temporalLimits.MaxMagnitudeBits = 65536
	temporalLimits.MaxInputBytes = 1 << 20
	temporalLimits.MaxValueBytes = 1 << 20
	magnitude := new(big.Int).Lsh(big.NewInt(1), 65535)
	number, err := temporal.ParseInteger(magnitude.String(), temporalLimits)
	if err != nil {
		t.Fatal(err)
	}
	position, err := temporal.IntegerPosition(axis, number)
	if err != nil {
		t.Fatal(err)
	}
	p := currentPresencePage{id: 7, axes: []currentPresenceAxis{ref}, groups: []currentPresenceGroup{{1, 11, 0, graphstate.LifeBound}}, rows: []currentPresenceRow{{relationship: 3, life: 31, flags: cpPoint | cpSource}}, wire: cpTestBody(t, position, temporalLimits)}
	l := defaultCurrentPresenceLimits()
	l.temporal = temporalLimits
	wire, _, err := encodeCurrentPresencePage(t.Context(), cpTestNamespace(), p, l)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) <= 4096 {
		t.Fatal("did not exercise legal oversized atom")
	}
	got, u, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), 7, currentPresenceDigest(wire), wire, l)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.rows) != 1 || got.rows[0].relationship != 3 || u.ScratchBytes != 2048+64*(8192+32) {
		t.Fatal(u)
	}
	for _, delta := range []int{-1, 0, 1} {
		limits := l
		limits.maxScratchBytes = u.ScratchBytes + delta
		page, usage, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), 7, currentPresenceDigest(wire), wire, limits)
		if delta < 0 {
			if !errors.Is(err, ErrResourceLimit) || page.id != 0 || usage != (currentPresenceUsage{}) {
				t.Fatal(err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	l.temporal.MaxMagnitudeBits--
	if _, _, err := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), 7, currentPresenceDigest(wire), wire, l); !errors.Is(err, ErrResourceLimit) || !errors.Is(err, temporal.ErrResourceLimit) {
		t.Fatal(err)
	}
}

func TestCurrentPresenceDiscreteAtomDecoderMatchesScopeOracle(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileLexicographicQN} {
		t.Run(cpProfileName(profile), func(t *testing.T) {
			axis, ref := cpTestAxis(t, profile, 1)
			at := func(n int64) temporal.Position {
				if profile == temporal.ProfileIntegerZ {
					return cpTestPosition(t, axis, fmt.Sprint(n), 0)
				}
				return cpTestPosition(t, axis, "1/3", n)
			}
			zero, one, two := at(0), at(1), at(2)
			point, err := temporal.Point(zero)
			if err != nil || point.Kind() != temporal.ScopePoint {
				t.Fatal(err)
			}
			cases := []struct {
				name      string
				lo, hi    temporal.Position
				flags     byte
				kind      temporal.ScopeKind
				canonical bool
			}{
				{"open-adjacent-empty", zero, one, cpSource, temporal.ScopeEmpty, false},
				{"closed-open-adjacent-point", zero, one, cpLowerClosed | cpSource, temporal.ScopePoint, false},
				{"closed-closed-normalizes-upper", zero, two, cpLowerClosed | cpUpperClosed | cpSource, temporal.ScopeSpan, false},
				{"open-lower-normalizes", zero, two, cpSource, temporal.ScopePoint, false},
				{"canonical-span", zero, two, cpLowerClosed | cpSource, temporal.ScopeSpan, true},
				{"open-half-unbounded", zero, temporal.Position{}, cpUpperInfinite | cpSource, temporal.ScopeSpan, false},
				{"closed-half-unbounded", zero, temporal.Position{}, cpLowerClosed | cpUpperInfinite | cpSource, temporal.ScopeSpan, true},
				{"closed-upper-half-unbounded", temporal.Position{}, zero, cpLowerInfinite | cpUpperClosed | cpSource, temporal.ScopeSpan, false},
				{"open-upper-half-unbounded", temporal.Position{}, zero, cpLowerInfinite | cpSource, temporal.ScopeSpan, true},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					lo, hi := temporal.NegativeInfinity(), temporal.PositiveInfinity()
					if tc.flags&cpLowerInfinite == 0 {
						var e error
						lo, e = temporal.FiniteBound(tc.lo, tc.flags&cpLowerClosed != 0)
						if e != nil {
							t.Fatal(e)
						}
					}
					if tc.flags&cpUpperInfinite == 0 {
						var e error
						hi, e = temporal.FiniteBound(tc.hi, tc.flags&cpUpperClosed != 0)
						if e != nil {
							t.Fatal(e)
						}
					}
					scope, e := temporal.Span(axis, lo, hi, temporal.Limits{})
					if e != nil || scope.Kind() != tc.kind {
						t.Fatalf("oracle kind%d want%d %v", scope.Kind(), tc.kind, e)
					}
					// Independently encode the nonnormalized atom; bypass the production
					// encoder so a writer-side rejection cannot mask decoder acceptance.
					b := recordHeader(cpTestNamespace(), currentPresenceRecord)
					b = binary.BigEndian.AppendUint64(b, 7)
					b = append(b, 1, 0, 0, 1, 1, 0, 1)
					b = append(b, ref.id[:]...)
					b = append(b, ref.hash[:]...)
					b = append(b, byte(ref.profile))
					b = binary.BigEndian.AppendUint64(b, 1)
					b = append(b, 0, byte(graphstate.LifeBound), 0, 1)
					b = binary.BigEndian.AppendUint64(b, 11)
					b = append(b, 0, 1)
					b = binary.BigEndian.AppendUint64(b, 3)
					b = binary.BigEndian.AppendUint64(b, 31)
					b = append(b, tc.flags)
					if tc.flags&cpLowerInfinite == 0 {
						b = append(b, cpTestBody(t, tc.lo, temporal.Limits{})...)
					}
					if tc.flags&cpUpperInfinite == 0 {
						b = append(b, cpTestBody(t, tc.hi, temporal.Limits{})...)
					}
					p, u, e := decodeCurrentPresencePage(t.Context(), cpTestNamespace(), 7, currentPresenceDigest(b), b, defaultCurrentPresenceLimits())
					if tc.canonical {
						if e != nil || len(p.rows) != 1 {
							t.Fatal(e)
						}
					} else if !errors.Is(e, ErrCorrupt) || p.id != 0 || u != (currentPresenceUsage{}) {
						t.Fatalf("accepted noncanonical support oracle%d: %v", scope.Kind(), e)
					}
				})
			}
		})
	}
}

func TestCurrentPresenceLexSpanAcrossRationalModels(t *testing.T) {
	axis, ref := cpTestAxis(t, temporal.ProfileLexicographicQN, 1)
	p := currentPresencePage{id: 7, axes: []currentPresenceAxis{ref}, groups: []currentPresenceGroup{{1, 11, 0, graphstate.LifeBound}}}
	lo := cpAddBody(t, &p, axis, "0", 0)
	hi := cpAddBody(t, &p, axis, "1/3", 1)
	lowPosition := cpTestPosition(t, axis, "0", 0)
	highPosition := cpTestPosition(t, axis, "1/3", 1)
	low, err := temporal.FiniteBound(lowPosition, true)
	if err != nil {
		t.Fatal(err)
	}
	high, err := temporal.FiniteBound(highPosition, false)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := temporal.Span(axis, low, high, temporal.Limits{})
	if err != nil || scope.Kind() != temporal.ScopeSpan {
		t.Fatal(err)
	}
	p.rows = []currentPresenceRow{{relationship: 3, life: 31, lower: lo, upper: hi, flags: cpLowerClosed | cpSource}}
	got, _, _ := cpRoundTrip(t, p)
	sum, err := got.summary(defaultCurrentPresenceLimits())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		model string
		micro int64
		want  bool
	}{{"0", 0, true}, {"0", math.MaxInt64, true}, {"1/3", 0, true}, {"1/3", 1, false}} {
		ok, err := cpSummaryMayContain(got, sum, ref, cpTestBody(t, cpTestPosition(t, axis, tc.model, tc.micro), temporal.Limits{}), defaultCurrentPresenceLimits())
		if err != nil || ok != tc.want {
			t.Fatal(tc, err)
		}
	}
}
