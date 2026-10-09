package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// rawCodec is deliberately unchecked, so adversarial tests can encode corrupt
// ordering/cells without making the public encoder normalize them first.
func rawCodec(t testing.TB, a temporal.Axis, scopes []temporal.Scope, first, second []Cell, changes bool) []byte {
	t.Helper()
	out := appendCodecHeader(nil, a, len(scopes), changes)
	for i, scope := range scopes {
		var err error
		out, err = appendCodecScope(out, scope, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		out = appendCodecCell(out, first[i])
		if changes {
			out = appendCodecCell(out, second[i])
		}
	}
	return out
}
func codecEqualState(t testing.TB, want, got State) {
	t.Helper()
	if want.Axis() != got.Axis() || want.Usage() != got.Usage() || len(want.pieces) != len(got.pieces) {
		t.Fatalf("state ledger/axis/count differ: %+v %+v", want.usage, got.usage)
	}
	for i, p := range want.pieces {
		same, err := p.scope.SameSupport(got.pieces[i].scope, temporal.Limits{})
		if err != nil || !same || p.cell != got.pieces[i].cell || p.scope.Kind() != got.pieces[i].scope.Kind() || p.scopeBytes != got.pieces[i].scopeBytes {
			t.Fatalf("piece %d differs: %+v %+v (%v)", i, p, got.pieces[i], err)
		}
	}
}
func codecEqualChanges(t testing.TB, want, got []Change) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("change count %d != %d", len(want), len(got))
	}
	for i, c := range want {
		same, err := c.scope.SameSupport(got[i].scope, temporal.Limits{})
		if err != nil || !same || c.before != got[i].before || c.after != got[i].after || c.scopeBytes != got[i].scopeBytes {
			t.Fatalf("change %d differs", i)
		}
	}
}
func codecRoundtrip(t testing.TB, result Result) {
	t.Helper()
	s := result.State()
	wire, err := AppendState(nil, s, CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeState(wire, s.Axis(), CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	codecEqualState(t, s, got)
	rewritten, err := AppendState(nil, got, CodecLimits{})
	if err != nil || !bytes.Equal(wire, rewritten) {
		t.Fatal("noncanonical state roundtrip", err)
	}
	changes := result.Changes()
	cwire, err := AppendChanges(nil, s.Axis(), changes, CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	cgot, usage, err := DecodeChanges(cwire, s.Axis(), CodecLimits{})
	if err != nil || usage != result.ChangeUsage() {
		t.Fatal("change ledger", usage, result.ChangeUsage(), err)
	}
	codecEqualChanges(t, changes, cgot)
	rewritten, err = AppendChanges(nil, s.Axis(), cgot, CodecLimits{})
	if err != nil || !bytes.Equal(cwire, rewritten) {
		t.Fatal("noncanonical change roundtrip", err)
	}
	clear(wire)
	clear(cwire)
	codecEqualState(t, s, got)
	codecEqualChanges(t, changes, cgot)
}
func TestCodecHistoricalCorrectionsAndFragmentedChanges(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		s, err := New(a, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		// Diverging metadata/lifecycles: null, payload (including 0 bytes), gaps,
		// explicit unset, equal value/new provenance, and unbounded state tails.
		all, _ := temporal.All(a)
		first, err := s.Set(all, value(t, math.MaxUint64, 0), revision(t, 1), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		codecRoundtrip(t, first)
		second, err := first.State().Set(span(t, a, 0, 4, true, true), Null(), revision(t, 2), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		codecRoundtrip(t, second)
		third, err := second.State().Unset(span(t, a, 1, 3, true, false), revision(t, 3), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		codecRoundtrip(t, third)
		fourth, err := third.State().Set(span(t, a, 2, 5, true, false), value(t, 9, 32), revision(t, 4), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if len(fourth.Changes()) < 2 {
			t.Fatal("fixture does not cross old-cell boundaries")
		}
		codecRoundtrip(t, fourth)
		historicalWire, err := AppendState(nil, second.State(), CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		historical, err := DecodeState(historicalWire, a, CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		p := position(t, a, 2, 1, 0)
		if old := at(t, historical, p); !old.Present() || !old.Value().IsNull() || old.Revision() != revision(t, 2) {
			t.Fatal("historical provenance lost", old)
		}
		if current := at(t, fourth.State(), p); current.Value().ID() != 9 || current.Revision() != revision(t, 4) {
			t.Fatal(current)
		}
		empty, err := AppendState(nil, s, CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeState(empty, a, CodecLimits{})
		if err != nil || at(t, decoded, p) != (Cell{}) {
			t.Fatal("phantom value", err)
		}
		// Real reducer Region mutation spans asserted support and never-asserted gaps.
		p0, _ := temporal.Point(position(t, a, 0, 1, 0))
		p2, _ := temporal.Point(position(t, a, 2, 1, 0))
		p4, _ := temporal.Point(position(t, a, 4, 1, 0))
		reg, err := temporal.Region(a, []temporal.Scope{p0, p2, p4}, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		seeded, err := s.Set(p2, Null(), revision(t, 8), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		changed, err := seeded.State().Unset(reg, revision(t, 9), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if len(changed.Changes()) != 3 || changed.Changes()[0].Before() != (Cell{}) || changed.Changes()[1].Before().Revision() != revision(t, 8) {
			t.Fatal("before images", changed.Changes())
		}
		codecRoundtrip(t, changed)
	}
}
func TestCodecScopeSemanticsAndWideOwnership(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		s, err := New(a, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		singleton := span(t, a, 0, 0, true, true)
		if singleton.Kind() != temporal.ScopePoint {
			t.Fatal("closed singleton became duration")
		}
		result, err := s.Set(singleton, Null(), revision(t, 1), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		codecRoundtrip(t, result)
		empty := span(t, a, 0, 0, false, true)
		result, err = s.Unset(empty, revision(t, 2), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		codecRoundtrip(t, result)
		if len(result.State().pieces) != 0 || len(result.Changes()) != 0 {
			t.Fatal("empty became point")
		}
		// Exact coordinates wider than int64 are owned by DecodeScope; both rational
		// denominator and natural microstep survive without any int64 coercion.
		n, err := temporal.ParseInteger("184467440737095516170000000001", temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		q, err := temporal.Fraction(n, temporal.Int64(3), temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		var p temporal.Position
		switch profile {
		case temporal.ProfileIntegerZ:
			p, err = temporal.IntegerPosition(a, n)
		case temporal.ProfileRationalQ:
			p, err = temporal.RationalPosition(a, q)
		case temporal.ProfileLexicographicQN:
			p, err = temporal.LexPosition(a, q, n)
		}
		if err != nil {
			t.Fatal(err)
		}
		point, err := temporal.Point(p)
		if err != nil {
			t.Fatal(err)
		}
		maxRev, err := NewRevision(math.MaxUint64, math.MaxUint64)
		if err != nil {
			t.Fatal(err)
		}
		result, err = s.Set(point, value(t, math.MaxUint64, 16), maxRev, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		codecRoundtrip(t, result)
		encoded, err := AppendState(nil, result.State(), CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeState(encoded, a, CodecLimits{})
		if err != nil {
			t.Fatal(err)
		}
		clear(encoded)
		if cell := at(t, decoded, p); cell.Revision() != maxRev || cell.Value().ID() != math.MaxUint64 {
			t.Fatal("wide state aliases input", cell)
		}
	}
}
func TestCodecIndependentLimitsAndAxis(t *testing.T) {
	a := axis(t, temporal.ProfileIntegerZ)
	s, err := New(a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := DefaultCodecLimits().Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (CodecLimits{}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, l := range []CodecLimits{{MaxEncodedBytes: -1}, {MaxEncodedBytes: hardMetadataBytes + 1}, {State: Limits{MaxPieces: -1}}, {State: Limits{MaxMetadataBytes: -1}}, {State: Limits{Temporal: temporal.Limits{MaxValueBytes: -1}}}} {
		want := ErrInvalidLimits
		if l.State.Temporal.MaxValueBytes < 0 {
			want = temporal.ErrInvalidLimits
		}
		if err := l.Validate(); !errors.Is(err, want) {
			t.Fatal(err)
		}
		if _, err := AppendState(nil, s, l); !errors.Is(err, want) {
			t.Fatal(err)
		}
		if _, err := DecodeState(nil, a, l); !errors.Is(err, want) {
			t.Fatal(err)
		}
		if _, err := AppendChanges(nil, a, nil, l); !errors.Is(err, want) {
			t.Fatal(err)
		}
		if _, _, err := DecodeChanges(nil, a, l); !errors.Is(err, want) {
			t.Fatal(err)
		}
	}
	if _, err := AppendState(nil, State{}, CodecLimits{}); !errors.Is(err, ErrInvalidState) {
		t.Fatal(err)
	}
	for _, changes := range []bool{false, true} {
		var wire []byte
		if changes {
			wire, err = AppendChanges(nil, a, nil, CodecLimits{})
		} else {
			wire, err = AppendState(nil, s, CodecLimits{})
		}
		if err != nil || len(wire) != codecHeaderBytes {
			t.Fatal(wire, err)
		}
		d := a.Descriptor()
		d.Reference += "-changed"
		changed, err := temporal.NewAxis(d, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		for _, ax := range []temporal.Axis{changed, {}} {
			want := temporal.ErrAxisMismatch
			if ax == (temporal.Axis{}) {
				want = temporal.ErrInvalidAxis
			}
			if changes {
				_, _, err = DecodeChanges(wire, ax, CodecLimits{})
			} else {
				_, err = DecodeState(wire, ax, CodecLimits{})
			}
			if !errors.Is(err, want) {
				t.Fatal("axis", changes, err)
			}
		}
		for _, l := range []CodecLimits{{MaxEncodedBytes: 55}, {State: Limits{MaxMetadataBytes: 1, MaxChangeMetadataBytes: 1}}, {State: Limits{Temporal: temporal.Limits{MaxDescriptorBytes: 1}}}} {
			want := ErrResourceLimit
			if l.State.Temporal.MaxDescriptorBytes == 1 {
				want = temporal.ErrResourceLimit
			}
			if changes {
				_, _, err = DecodeChanges(wire, a, l)
			} else {
				_, err = DecodeState(wire, a, l)
			}
			if !errors.Is(err, want) {
				t.Fatal("empty limit", changes, err)
			}
		}
	}
	// Change ledger is independent from a hypothetical snapshot ledger.
	if _, err := AppendChanges(nil, a, nil, CodecLimits{State: Limits{MaxMetadataBytes: 1}}); err != nil {
		t.Fatal(err)
	}
	point, _ := temporal.Point(position(t, a, 4, 1, 0))
	result, err := s.Set(point, value(t, 1, 128), revision(t, 1), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := AppendState(nil, result.State(), CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	tight := CodecLimits{State: Limits{MaxMetadataBytes: result.State().Usage().MetadataBytes(), MaxReferencedBytes: 128}, MaxEncodedBytes: len(wire)}
	if _, err := DecodeState(wire, a, tight); err != nil {
		t.Fatal(err)
	}
	limits := []CodecLimits{
		{MaxEncodedBytes: len(wire) - 1},
		{State: Limits{MaxMetadataBytes: result.State().Usage().MetadataBytes() - 1}},
		{State: Limits{MaxReferencedBytes: 127}},
		{State: Limits{Temporal: temporal.Limits{MaxInputBytes: 55}}},
		{State: Limits{Temporal: temporal.Limits{MaxValueBytes: 55}}},
		{State: Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 2}}},
	}
	for i, l := range limits {
		want := ErrResourceLimit
		if i >= 3 {
			want = temporal.ErrResourceLimit
		}
		if _, err := DecodeState(wire, a, l); !errors.Is(err, want) {
			t.Fatalf("cap %d: %v", i, err)
		}
		// Encode doesn't consume MaxInputBytes; it still applies every value cap.
		if i != 3 {
			if _, err := AppendState(nil, result.State(), l); !errors.Is(err, want) {
				t.Fatalf("encode cap %d: %v", i, err)
			}
		}
	}
	// A tighter active policy accepts fitting actual data from a wider policy.
	if _, err := DecodeState(wire, a, CodecLimits{State: Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 3}}}); err != nil {
		t.Fatal(err)
	}
	long := a.Descriptor()
	long.Reference = strings.Repeat("r", 1000)
	large, err := temporal.NewAxis(long, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AppendChanges(nil, large, nil, CodecLimits{State: Limits{MaxChangeMetadataBytes: 1000}}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
}

func TestCodecMalformedFramingAndCells(t *testing.T) {
	a := axis(t, temporal.ProfileIntegerZ)
	s, _ := New(a, Limits{})
	p, _ := temporal.Point(position(t, a, 0, 1, 0))
	r, err := s.Set(p, value(t, 1, 8), revision(t, 1), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, changes := range []bool{false, true} {
		wire, err := AppendState(nil, r.State(), CodecLimits{})
		if changes {
			wire, err = AppendChanges(nil, a, r.Changes(), CodecLimits{})
		}
		if err != nil {
			t.Fatal(err)
		}
		decode := func(src []byte) error {
			if changes {
				_, _, err := DecodeChanges(src, a, CodecLimits{})
				return err
			}
			_, err := DecodeState(src, a, CodecLimits{})
			return err
		}
		for n := range len(wire) {
			if err := decode(wire[:n]); !errors.Is(err, ErrInvalidEncoding) {
				t.Fatalf("trunc %d changes=%v: %v", n, changes, err)
			}
		}
		scopeLen := int(binary.BigEndian.Uint32(wire[56:60]))
		cellAt := 60 + scopeLen
		if changes {
			cellAt += cellMetadataBytes
		}
		cases := []struct {
			name string
			mut  func([]byte)
			want error
		}{
			{"magic", func(b []byte) { b[0] = '?' }, ErrInvalidEncoding},
			{"version", func(b []byte) { b[2] = 255 }, ErrUnknownVersion},
			{"profile", func(b []byte) { b[3] = 255 }, temporal.ErrUnknownProfile},
			{"axis", func(b []byte) { b[4] ^= 1 }, temporal.ErrAxisMismatch},
			{"definition", func(b []byte) { b[20] ^= 1 }, temporal.ErrAxisMismatch},
			{"count overflow", func(b []byte) { binary.BigEndian.PutUint32(b[52:56], math.MaxUint32) }, ErrResourceLimit},
			{"count short", func(b []byte) { binary.BigEndian.PutUint32(b[52:56], 2) }, ErrInvalidEncoding},
			{"length overflow", func(b []byte) { binary.BigEndian.PutUint32(b[56:60], math.MaxUint32) }, ErrInvalidEncoding},
			{"length short", func(b []byte) { binary.BigEndian.PutUint32(b[56:60], 52) }, ErrInvalidEncoding},
			{"presence", func(b []byte) { b[cellAt] = 2 }, ErrInvalidEncoding},
			{"max presence", func(b []byte) { b[cellAt] = 255 }, ErrInvalidEncoding},
			{"max scope kind", func(b []byte) { b[112] = 255 }, temporal.ErrInvalidEncoding},
			{"unknown kind", func(b []byte) { b[cellAt+1] = 255 }, ErrInvalidValueRef},
			{"absent payload", func(b []byte) { b[cellAt] = 0 }, ErrInvalidValueRef},
			{"bad null", func(b []byte) { b[cellAt+1] = byte(valueNull) }, ErrInvalidValueRef},
			{"zero payload ID", func(b []byte) { clear(b[cellAt+2 : cellAt+10]) }, ErrInvalidValueRef},
			{"uint64 size", func(b []byte) { binary.BigEndian.PutUint64(b[cellAt+10:cellAt+18], math.MaxUint64) }, ErrResourceLimit},
			{"zero revision", func(b []byte) { clear(b[cellAt+18 : cellAt+26]) }, ErrInvalidRevision},
			{"nested version", func(b []byte) { b[62] = 255 }, temporal.ErrUnknownVersion},
			{"nested definition", func(b []byte) { b[80] ^= 1 }, temporal.ErrAxisMismatch},
			{"coordinate sign", func(b []byte) { b[113] = 3 }, temporal.ErrInvalidEncoding},
		}
		for _, c := range cases {
			t.Run(c.name+map[bool]string{false: "/state", true: "/changes"}[changes], func(t *testing.T) {
				b := bytes.Clone(wire)
				c.mut(b)
				if err := decode(b); !errors.Is(err, c.want) {
					t.Fatal(err)
				}
			})
		}
		if err := decode(append(bytes.Clone(wire), 0)); !errors.Is(err, ErrInvalidEncoding) {
			t.Fatal(err)
		}
		if changes {
			b := bytes.Clone(wire)
			copy(b[cellAt-cellMetadataBytes:cellAt], b[cellAt:cellAt+cellMetadataBytes])
			if err := decode(b); !errors.Is(err, ErrInvalidEncoding) {
				t.Fatal("unchanged image accepted", err)
			}
			b = bytes.Clone(wire)
			b[60+scopeLen+26] = 1
			if err := decode(b); !errors.Is(err, ErrInvalidRevision) {
				t.Fatal("inauthentic zero before", err)
			}
		}
	}
}

func TestCodecRejectsNoncanonicalStreamsAtomically(t *testing.T) {
	a := axis(t, temporal.ProfileIntegerZ)
	c := Cell{present: true, value: Null(), revision: revision(t, 1)}
	other := Cell{revision: revision(t, 2)}
	p0, _ := temporal.Point(position(t, a, 0, 1, 0))
	p1, _ := temporal.Point(position(t, a, 1, 1, 0))
	p2, _ := temporal.Point(position(t, a, 2, 1, 0))
	all, _ := temporal.All(a)
	empty, _ := temporal.Empty(a)
	unplaced, _ := temporal.Unplaced(a)
	region, _ := temporal.Region(a, []temporal.Scope{p0, p2}, temporal.Limits{})
	foreign, _ := temporal.Point(position(t, axis(t, temporal.ProfileRationalQ), 0, 1, 0))
	for _, test := range []struct {
		name   string
		scopes []temporal.Scope
		cells  []Cell
		want   error
	}{
		{"unsorted", []temporal.Scope{p2, p0}, []Cell{c, other}, ErrInvalidEncoding},
		{"overlap", []temporal.Scope{p0, p0}, []Cell{c, other}, ErrInvalidEncoding},
		{"all overlaps", []temporal.Scope{all, p0}, []Cell{c, other}, ErrInvalidEncoding},
		{"mergeable", []temporal.Scope{p0, p1}, []Cell{c, c}, ErrInvalidEncoding},
		{"empty", []temporal.Scope{empty}, []Cell{c}, ErrInvalidEncoding},
		{"unplaced", []temporal.Scope{unplaced}, []Cell{c}, ErrInvalidEncoding},
		{"region", []temporal.Scope{region}, []Cell{c}, ErrInvalidEncoding},
		{"zero cell", []temporal.Scope{p0}, []Cell{{}}, ErrInvalidRevision},
		{"foreign", []temporal.Scope{foreign}, []Cell{c}, temporal.ErrAxisMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			pieces := make([]Piece, len(test.scopes))
			changes := make([]Change, len(test.scopes))
			befores := make([]Cell, len(test.scopes))
			for i, scope := range test.scopes {
				pieces[i] = Piece{scope: scope, cell: test.cells[i]}
				changes[i] = Change{scope: scope, after: test.cells[i]}
			}
			fabricated := State{axis: a, pieces: pieces, usage: initialUsage(a), valid: true}
			backing := bytes.Repeat([]byte{0xa5}, 4096)
			prefix := backing[:8]
			before := bytes.Clone(backing)
			if out, err := AppendState(prefix, fabricated, CodecLimits{}); !errors.Is(err, test.want) || len(out) != len(prefix) || !bytes.Equal(backing, before) {
				t.Fatal("state append nonatomic", err)
			}
			if out, err := AppendChanges(prefix, a, changes, CodecLimits{}); !errors.Is(err, test.want) || len(out) != len(prefix) || !bytes.Equal(backing, before) {
				t.Fatal("change append nonatomic", err)
			}
			wire := rawCodec(t, a, test.scopes, test.cells, nil, false)
			if out, err := DecodeState(wire, a, CodecLimits{}); !errors.Is(err, test.want) || out.valid {
				t.Fatal("bad state accepted", err)
			}
			wire = rawCodec(t, a, test.scopes, befores, test.cells, true)
			if out, usage, err := DecodeChanges(wire, a, CodecLimits{}); !errors.Is(err, test.want) || out != nil || usage != (Usage{}) {
				t.Fatal("bad changes accepted", err)
			}
		})
	}
	// Non-touching equal cells are canonical. Touching different cells must remain
	// distinct even if both are null, preserving revisions/provenance exactly.
	for _, cells := range [][]Cell{{c, c}, {c, Cell{present: true, value: Null(), revision: revision(t, 2)}}} {
		scopes := []temporal.Scope{p0, p2}
		if cells[0] != cells[1] {
			scopes[1] = p1
		}
		wire := rawCodec(t, a, scopes, cells, nil, false)
		got, err := DecodeState(wire, a, CodecLimits{})
		if err != nil || len(got.pieces) != 2 {
			t.Fatal(err)
		}
	}
	// Failures on the last entry's budgets cannot modify any spare backing byte.
	s, _ := New(a, Limits{})
	r, err := s.Set(p0, value(t, 1, 8), revision(t, 1), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range []CodecLimits{{MaxEncodedBytes: 56}, {State: Limits{MaxReferencedBytes: 7}}, {State: Limits{MaxMetadataBytes: initialUsage(a).MetadataBytes() + 87}}, {State: Limits{MaxChangeMetadataBytes: initialUsage(a).MetadataBytes() + 121}}, {State: Limits{Temporal: temporal.Limits{MaxValueBytes: 54}}}} {
		for _, changes := range []bool{false, true} {
			b := bytes.Repeat([]byte{0xa5}, 4096)
			before := bytes.Clone(b)
			prefix := b[:8]
			var out []byte
			if changes {
				out, err = AppendChanges(prefix, a, r.Changes(), l)
			} else {
				out, err = AppendState(prefix, r.State(), l)
			}
			// A limit of the opposite ledger may legitimately permit this envelope.
			if err != nil && (len(out) != 8 || !bytes.Equal(b, before)) {
				t.Fatal("failed append wrote backing", err)
			}
			if err == nil && (!bytes.Equal(out[:8], before[:8]) || len(out) <= 8) {
				t.Fatal("successful append corrupted prefix")
			}
		}
	}
}

func TestCodecChangeCapsAndReferenceArithmetic(t *testing.T) {
	a := axis(t, temporal.ProfileIntegerZ)
	c := Cell{present: true, value: value(t, 1, 64), revision: revision(t, 1)}
	d := Cell{present: true, value: value(t, 2, 128), revision: revision(t, 2)}
	p0, _ := temporal.Point(position(t, a, 0, 1, 0))
	p2, _ := temporal.Point(position(t, a, 2, 1, 0))
	scopes := []temporal.Scope{p0, p2}
	wire := rawCodec(t, a, scopes, []Cell{c, c}, []Cell{d, d}, true)
	parts, usage, err := DecodeChanges(wire, a, CodecLimits{})
	if err != nil {
		t.Fatal(err)
	}
	exact := CodecLimits{State: Limits{MaxChangePieces: 2, MaxChangeMetadataBytes: usage.MetadataBytes(), MaxReferencedBytes: 384}, MaxEncodedBytes: len(wire)}
	if out, u, err := DecodeChanges(wire, a, exact); err != nil || len(out) != 2 || u != usage {
		t.Fatal(out, u, err)
	}
	for _, l := range []CodecLimits{
		{State: Limits{MaxChangePieces: 1}},
		{State: Limits{MaxChangeMetadataBytes: usage.MetadataBytes() - 1}},
		{State: Limits{MaxReferencedBytes: 383}},
		{MaxEncodedBytes: len(wire) - 1},
		{State: Limits{Temporal: temporal.Limits{MaxMagnitudeBits: 1}}},
	} {
		want := ErrResourceLimit
		if l.State.Temporal.MaxMagnitudeBits == 1 {
			want = temporal.ErrResourceLimit
		}
		if _, _, err := DecodeChanges(wire, a, l); !errors.Is(err, want) {
			t.Fatal("decode change cap", err)
		}
		backing := bytes.Repeat([]byte{0xa5}, len(wire)+32)
		original := bytes.Clone(backing)
		if out, err := AppendChanges(backing[:8], a, parts, l); !errors.Is(err, want) || len(out) != 8 || !bytes.Equal(backing, original) {
			t.Fatal("append change cap/atomicity", err)
		}
	}
	// Maximum admitted size remains metadata only: no 1 GiB payload is present.
	big := Cell{present: true, value: value(t, math.MaxUint64, hardReferenceBytes), revision: revision(t, 3)}
	one := rawCodec(t, a, []temporal.Scope{p0}, []Cell{big}, nil, false)
	l := CodecLimits{State: Limits{MaxReferencedBytes: hardReferenceBytes}}
	s, err := DecodeState(one, a, l)
	if err != nil || s.Usage().DeclaredReferenceBytes() != hardReferenceBytes {
		t.Fatal(s.Usage(), err)
	}
	if _, err := AppendState(nil, s, l); err != nil {
		t.Fatal(err)
	}
	two := rawCodec(t, a, []temporal.Scope{p0}, []Cell{big}, []Cell{bigWithDifferentRevision(big)}, true)
	if _, _, err := DecodeChanges(two, a, l); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("before + after overflow", err)
	}
	// A count alone cannot allocate a count-sized piece array. With no entries,
	// both tiny and hard-max admitted counts fail during framing preflight.
	high := CodecLimits{State: Limits{MaxPieces: hardPieces, MaxChangePieces: hardPieces, MaxMetadataBytes: hardMetadataBytes, MaxChangeMetadataBytes: hardMetadataBytes}, MaxEncodedBytes: hardMetadataBytes}
	for _, changes := range []bool{false, true} {
		small := appendCodecHeader(nil, a, 1, changes)
		huge := appendCodecHeader(nil, a, hardPieces, changes)
		decode := func(src []byte) {
			var e error
			if changes {
				_, _, e = DecodeChanges(src, a, high)
			} else {
				_, e = DecodeState(src, a, high)
			}
			if !errors.Is(e, ErrInvalidEncoding) {
				t.Fatal(e)
			}
		}
		fewer := testing.AllocsPerRun(20, func() { decode(small) })
		more := testing.AllocsPerRun(20, func() { decode(huge) })
		if more > fewer+1 {
			t.Fatalf("untrusted count allocated output: tiny=%v huge=%v", fewer, more)
		}
	}
}
func bigWithDifferentRevision(c Cell) Cell { c.revision.id++; return c }

func TestCodecNoncanonicalCoordinatesDecline(t *testing.T) {
	c := Cell{present: true, value: Null(), revision: revision(t, 1)}
	// Full scope codecs remain the sole authority for exact scalar syntax.
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ, temporal.ProfileLexicographicQN} {
		a := axis(t, profile)
		p, _ := temporal.Point(position(t, a, 0, 1, 0))
		for _, changes := range []bool{false, true} {
			wire := rawCodec(t, a, []temporal.Scope{p}, []Cell{c}, []Cell{{revision: revision(t, 2)}}, changes)
			// Point's zero integer numerator cannot use positive sign with 0 length.
			wire[60+codecScopeMinimumBytes] = 1
			var err error
			if changes {
				_, _, err = DecodeChanges(wire, a, CodecLimits{})
			} else {
				_, err = DecodeState(wire, a, CodecLimits{})
			}
			if !errors.Is(err, ErrInvalidEncoding) || !errors.Is(err, temporal.ErrInvalidEncoding) {
				t.Fatal("coordinate accepted", profile, changes, err)
			}
		}
	}
	// Z [0,1) is a point: an alternate Span encoding is rejected, not normalized.
	a := axis(t, temporal.ProfileIntegerZ)
	p, _ := temporal.Point(position(t, a, 0, 1, 0))
	scope, err := temporal.AppendScope(nil, p, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	scope = append(scope[:52], byte(temporal.ScopeSpan), byte(temporal.BoundFinite)|0x80, 0, 0, 0, byte(temporal.BoundFinite), 1, 0, 1, 1)
	wire := appendCodecHeader(nil, a, 1, false)
	wire = binary.BigEndian.AppendUint32(wire, uint32(len(scope)))
	wire = append(wire, scope...)
	wire = appendCodecCell(wire, c)
	if _, err := DecodeState(wire, a, CodecLimits{}); !errors.Is(err, temporal.ErrInvalidEncoding) {
		t.Fatal("alternate singleton normalized", err)
	}
}
