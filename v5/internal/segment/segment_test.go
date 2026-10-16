package segment

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Ported from v4 pkg/graph/internal/segment/segment_test.go (rows are Row
// values, hashes are recomputed with RowHash, coordinates are Coords).

const testType = "HOP"

// testSchema declares columns of every supported kind (two each of I64, F64).
func testSchema() Schema {
	return Schema{TypeName: testType, Columns: []Column{
		{"s", KindString}, {"b", KindBool}, {"i", KindI64}, {"i64", KindI64},
		{"f", KindF64}, {"f64", KindF64},
	}}
}

// mkRow builds a row with finite coordinates 10/20/30/+inf and props.
func mkRow(id, rev uint32, start, end int64, props ...Property) Row {
	return Row{
		ID: types.RelID(id), Start: types.NodeID(start), End: types.NodeID(end), Revision: rev,
		ValidFrom: Finite(10), ValidTo: PosInf(), TxFrom: Finite(30), TxTo: PosInf(), Props: props,
	}
}

// corpus builds n adversarial rows: every declared kind with extreme values,
// absent declared properties, kind mismatches (fallback), undeclared keys,
// several revisions of one ID, zero/negative/extreme and infinite
// coordinates in every allowed combination.
func corpus(tb testing.TB, n int, seed uint64) []Row {
	tb.Helper()
	rng := rand.New(rand.NewPCG(seed, 99)) // #nosec G404 -- test data
	nodes := make([]int64, 37)
	for i := range nodes {
		nodes[i] = 1_000_000 + int64(i)*977 + int64(rng.IntN(50))
	}
	nodes[0], nodes[1] = 1, math.MaxInt64
	extremesI := []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 1 << 40, 1<<53 + 1}
	extremesF := []float64{0, math.Copysign(0, -1), 1.5, -3.25e300, math.MaxFloat64, math.SmallestNonzeroFloat64, 1 << 53, 3}
	rows := make([]Row, 0, n)
	id := int64(5_000_000_000)
	for i := 0; i < n; i++ {
		rev := uint32(0)
		if i > 0 && rng.IntN(7) == 0 {
			rev = rows[i-1].Revision + 1 // another revision of the previous ID
		} else {
			id += 1 + int64(rng.IntN(1<<20))
		}
		rowID := id
		if i == 3 {
			rowID = math.MaxInt64 - 1
		}
		s, e := rng.IntN(len(nodes)), rng.IntN(len(nodes))
		var props []Property
		put := func(k string, v graphstate.Scalar) {
			if rng.IntN(9) != 0 { // sometimes absent
				props = append(props, Property{k, v})
			}
		}
		set := func(k string, v graphstate.Scalar) {
			for j := range props {
				if props[j].Key == k {
					props[j].Value = v
					return
				}
			}
			props = append(props, Property{k, v})
		}
		strs := []string{"", "a", "actor-1", "ünïcode", string([]byte{0, 255, 7})}
		put("s", sS(strs[rng.IntN(len(strs))]))
		put("b", sB(rng.IntN(2) == 0))
		put("i", sI(extremesI[rng.IntN(len(extremesI))]))
		put("i64", sI(extremesI[rng.IntN(len(extremesI))]))
		put("f", sF(tb, extremesF[rng.IntN(len(extremesF))]))
		put("f64", sF(tb, extremesF[rng.IntN(len(extremesF))]))
		switch rng.IntN(7) { // kind mismatches and undeclared properties
		case 0:
			set("i64", sS("not an int"))
		case 1:
			set("s", sI(42))
		case 2:
			set("extra", sI(9))
			set("tags", sS("a,b"))
		case 3:
			set("when", sF(tb, 2.5))
			set("zero", sB(false))
		case 4: // I64/F64 swapped into the other kind's column
			set("i64", sF(tb, 3))
			set("f64", sI(3))
		}
		rng.Shuffle(len(props), func(a, b int) { props[a], props[b] = props[b], props[a] })
		vf := Finite(int64(1_787_443_200_000) + rng.Int64N(86_400_000))
		switch rng.IntN(12) {
		case 0:
			vf = Finite(0) // zero is a coordinate
		case 1:
			vf = Finite(-rng.Int64N(1 << 50))
		case 2:
			vf = NegInf()
		case 3:
			vf = Finite(math.MinInt64)
		}
		v, _ := vf.Value()
		vt := PosInf()
		switch rng.IntN(6) {
		case 0, 1, 2:
			vt = Finite(v + 1 + rng.Int64N(5000))
		case 3:
			vt = Finite(0)
		case 4:
			vt = Finite(math.MaxInt64)
		}
		tx := Finite(v + 30_000)
		if vf.Kind() == CoordNegInf || rng.IntN(15) == 0 {
			tx = NegInf()
		}
		txTo := PosInf()
		if rng.IntN(8) == 0 {
			txTo = Finite(v + 60_000)
		}
		rows = append(rows, Row{
			ID: types.RelID(rowID), Start: types.NodeID(nodes[s]), End: types.NodeID(nodes[e]), Revision: rev,
			ValidFrom: vf, ValidTo: vt, TxFrom: tx, TxTo: txTo, Props: props,
		})
	}
	return rows
}

// canon returns r with Props sorted by key (what the reader returns).
func canon(r Row) Row {
	r.Props = slices.Clone(r.Props)
	slices.SortFunc(r.Props, func(a, b Property) int {
		switch {
		case a.Key < b.Key:
			return -1
		case a.Key > b.Key:
			return 1
		}
		return 0
	})
	return r
}

// assertSameRow is exact equality of kind and canonical value.
func assertSameRow(tb testing.TB, want, got Row) {
	tb.Helper()
	want = canon(want)
	if d := rowDiff(&want, &got); d != "" {
		tb.Fatalf("row %d r%d: %s\n got %+v\nwant %+v", want.ID, want.Revision, d, got, want)
	}
	// Independent of rowDiff: the Scalar kinds survive.
	for k := range want.Props {
		if want.Props[k].Value.Kind() != got.Props[k].Value.Kind() {
			tb.Fatalf("row %d: property %q kind %d, want %d", want.ID, want.Props[k].Key, got.Props[k].Value.Kind(), want.Props[k].Value.Kind())
		}
	}
}

type rowKey struct {
	id  types.RelID
	rev uint32
}

func byKey(rows []Row) map[rowKey]Row {
	m := make(map[rowKey]Row, len(rows))
	for _, r := range rows {
		m[rowKey{r.ID, r.Revision}] = r
	}
	return m
}

func mustEncode(tb testing.TB, rows []Row, opts Options, pageRows int) []byte {
	tb.Helper()
	data, err := encode(testSchema(), rows, opts, pageRows)
	if err != nil {
		tb.Fatalf("encode: %v", err)
	}
	return data
}

func mustOpen(tb testing.TB, data []byte) *Segment {
	tb.Helper()
	seg, err := Open(data)
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	return seg
}

// scanAll decodes every row and checks segment order.
func scanAll(tb testing.TB, seg *Segment) []Row {
	tb.Helper()
	var out []Row
	if err := seg.Scan(func(i int, r *Row) bool {
		if i != len(out) {
			tb.Fatalf("scan index %d, want %d", i, len(out))
		}
		out = append(out, *r)
		return true
	}); err != nil {
		tb.Fatalf("scan: %v", err)
	}
	for i := 1; i < len(out); i++ {
		if !lessRow(&out[i-1], &out[i]) {
			tb.Fatalf("rows %d,%d out of segment order", i-1, i)
		}
	}
	return out
}

func TestRoundTrip_BitExactIncludingGoKinds(t *testing.T) {
	for _, pageRows := range []int{PageRows, 64, 1} {
		t.Run(fmt.Sprintf("pageRows=%d", pageRows), func(t *testing.T) {
			rows := corpus(t, 1500, 1)
			seg := mustOpen(t, mustEncode(t, rows, Options{}, pageRows))
			if seg.Len() != len(rows) {
				t.Fatalf("Len %d, want %d", seg.Len(), len(rows))
			}
			want := byKey(rows)
			got := scanAll(t, seg)
			if len(got) != len(rows) {
				t.Fatalf("scanned %d rows, want %d", len(got), len(rows))
			}
			for i, g := range got {
				w, ok := want[rowKey{g.ID, g.Revision}]
				if !ok {
					t.Fatalf("phantom row %d r%d", g.ID, g.Revision)
				}
				assertSameRow(t, w, g)
				r, err := seg.Row(i)
				if err != nil {
					t.Fatalf("Row(%d): %v", i, err)
				}
				assertSameRow(t, w, *r)
			}
		})
	}
}

// TestCoords_ZeroIsACoordinateInfinitiesAreExplicit: Finite(0) must never
// decode as an infinity (v4 stored 0 = open), and every allowed infinity
// round-trips, in a column page that also holds extreme finite values.
func TestCoords_ZeroIsACoordinateInfinitiesAreExplicit(t *testing.T) {
	mk := func(id uint32, vf, vt, tf, tt Coord) Row {
		r := mkRow(id, 0, 1, 2)
		r.ValidFrom, r.ValidTo, r.TxFrom, r.TxTo = vf, vt, tf, tt
		return r
	}
	rows := []Row{
		mk(1, Finite(0), Finite(0), Finite(0), Finite(0)),
		mk(2, NegInf(), PosInf(), NegInf(), PosInf()),
		mk(3, Finite(math.MinInt64), Finite(math.MaxInt64), Finite(math.MinInt64), Finite(math.MaxInt64)),
		mk(4, Finite(-5), Finite(0), Finite(-1), PosInf()),
		mk(5, NegInf(), Finite(0), Finite(0), Finite(1)),
	}
	seg := mustOpen(t, mustEncode(t, rows, Options{}, PageRows))
	want := byKey(rows)
	for _, g := range scanAll(t, seg) {
		assertSameRow(t, want[rowKey{g.ID, g.Revision}], g)
	}
	// All-finite input needs no kind columns at all.
	fin := []Row{mk(1, Finite(0), Finite(0), Finite(0), Finite(0)), mk(2, Finite(1), Finite(2), Finite(3), Finite(4))}
	s := mustOpen(t, mustEncode(t, fin, Options{}, PageRows))
	for _, n := range []string{colValidFromKind, colValidToKind, colTxFromKind, colTxToKind} {
		if s.sectionLen(n) != 0 {
			t.Fatalf("all-finite segment stores %s (%d bytes)", n, s.sectionLen(n))
		}
	}
}

func TestCoords_BoundConversionKeepsInfinitiesAndZero(t *testing.T) {
	axis, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: temporal.ProfileIntegerZ, Version: 1, Reference: "test-v1", CanonicalUnit: "unit"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []Coord{NegInf(), PosInf(), Finite(0), Finite(-7), Finite(math.MaxInt64), Finite(math.MinInt64)} {
		for _, incl := range []bool{false, true} {
			b, err := c.Bound(axis, incl)
			if err != nil {
				t.Fatalf("%v.Bound: %v", c, err)
			}
			got, gi, err := CoordFromBound(b)
			if err != nil || got != c {
				t.Fatalf("round trip %v -> %v, %v", c, got, err)
			}
			if c.Kind() == CoordFinite && gi != incl {
				t.Fatalf("%v: inclusive %v, want %v", c, gi, incl)
			}
		}
	}
	if _, _, err := CoordFromBound(temporal.Bound{}); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("invalid bound: %v", err)
	}
	if _, err := (Coord{}).Bound(axis, true); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("invalid Coord.Bound: %v", err)
	}
}

func TestCoord_CompareOrdersInfinitiesAroundEveryFiniteValue(t *testing.T) {
	order := []Coord{NegInf(), Finite(math.MinInt64), Finite(-1), Finite(0), Finite(1), Finite(math.MaxInt64), PosInf()}
	for i := range order {
		for j := range order {
			want := 0
			if i < j {
				want = -1
			} else if i > j {
				want = 1
			}
			if got := order[i].Compare(order[j]); got != want {
				t.Fatalf("%v vs %v = %d, want %d", order[i], order[j], got, want)
			}
		}
	}
	if (Coord{}).Compare(NegInf()) >= 0 {
		t.Fatal("the zero Coord must sort below every valid kind")
	}
}

func TestRoundTrip_SchemaAndTypeNameSurvive(t *testing.T) {
	seg := mustOpen(t, mustEncode(t, corpus(t, 10, 2), Options{}, PageRows))
	if !reflect.DeepEqual(seg.Schema(), testSchema()) {
		t.Fatalf("schema %+v, want %+v", seg.Schema(), testSchema())
	}
}

func TestHashes_RecomputedRowHashRootsVerifyAtEveryBlockSize(t *testing.T) {
	rows := corpus(t, 5000, 3)
	for _, b := range []int{1, 64, 4096} {
		t.Run(fmt.Sprintf("IntegrityBlockRows=%d", b), func(t *testing.T) {
			seg := mustOpen(t, mustEncode(t, rows, Options{IntegrityBlockRows: b}, PageRows))
			if seg.IntegrityBlockRows() != b {
				t.Fatalf("footer block size %d, want %d", seg.IntegrityBlockRows(), b)
			}
			groups := (len(rows) + b - 1) / b
			for gi := 0; gi < groups; gi++ {
				if err := seg.VerifyGroup(gi); err != nil {
					t.Fatalf("VerifyGroup(%d): %v", gi, err)
				}
			}
			if err := seg.VerifyGroup(groups); !errors.Is(err, ErrOutOfRange) {
				t.Fatalf("VerifyGroup past the end: %v, want ErrOutOfRange", err)
			}
			if err := seg.Verify(); err != nil {
				t.Fatalf("Verify: %v", err)
			}
		})
	}
}

func TestHashes_DefaultBlockSizeIs64(t *testing.T) {
	seg := mustOpen(t, mustEncode(t, corpus(t, 10, 4), Options{}, PageRows))
	if seg.IntegrityBlockRows() != DefaultIntegrityBlockRows || DefaultIntegrityBlockRows != 64 {
		t.Fatalf("default block size %d", seg.IntegrityBlockRows())
	}
}

func TestRowHash_CoversEveryField(t *testing.T) {
	base := mkRow(5, 1, 2, 3, Property{"a", sI(1)}, Property{"b", sS("x")})
	h0, err := RowHash(testType, &base)
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := RowHash(testType, &base); again != h0 {
		t.Fatal("RowHash is not deterministic")
	}
	shuffled := base
	shuffled.Props = []Property{base.Props[1], base.Props[0]}
	if h, _ := RowHash(testType, &shuffled); h != h0 {
		t.Fatal("property order changes the hash")
	}
	muts := map[string]func(r *Row){
		"id":       func(r *Row) { r.ID++ },
		"start":    func(r *Row) { r.Start++ },
		"end":      func(r *Row) { r.End++ },
		"revision": func(r *Row) { r.Revision++ },
		"vf":       func(r *Row) { r.ValidFrom = Finite(11) },
		"vf inf":   func(r *Row) { r.ValidFrom = NegInf() },
		"vt zero":  func(r *Row) { r.ValidTo = Finite(0) },
		"tx":       func(r *Row) { r.TxFrom = Finite(31) },
		"txto":     func(r *Row) { r.TxTo = Finite(0) },
		"prop val": func(r *Row) { r.Props = []Property{{"a", sI(2)}, {"b", sS("x")}} },
		"prop kind": func(r *Row) {
			r.Props = []Property{{"a", sF(t, 1)}, {"b", sS("x")}}
		},
		"prop key": func(r *Row) { r.Props = []Property{{"a2", sI(1)}, {"b", sS("x")}} },
		"prop add": func(r *Row) { r.Props = append(slices.Clone(r.Props), Property{"c", sB(true)}) },
	}
	seen := map[[32]byte]string{h0: "base"}
	for name, mut := range muts {
		r := base
		r.Props = slices.Clone(base.Props)
		mut(&r)
		h, err := RowHash(testType, &r)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if prev, dup := seen[h]; dup {
			t.Fatalf("mutation %q hashes like %q", name, prev)
		}
		seen[h] = name
	}
	// F64 -0 is canonicalised to +0 by the engine scalar: one value, one hash.
	z, nz := base, base
	z.Props = []Property{{"a", sF(t, 0)}}
	nz.Props = []Property{{"a", sF(t, math.Copysign(0, -1))}}
	if a, _ := RowHash(testType, &z); a != mustHash(t, &nz) {
		t.Fatal("F64 -0 and +0 hash differently")
	}
	lo, hi := base, base
	lo.ValidTo, hi.ValidTo = NegInf(), PosInf() // same slot, different infinity
	if a, _ := RowHash(testType, &lo); a == h0 {
		t.Fatal("-inf and +inf valid_to hash alike")
	}
	if h, _ := RowHash("OTHER", &base); h == h0 {
		t.Fatal("type name is not hashed")
	}
	bad := base
	bad.Props = []Property{{"a", graphstate.Null()}}
	if _, err := RowHash(testType, &bad); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("non-scalar property: %v", err)
	}
}

func TestOptions_RejectInvalidBlockSizes(t *testing.T) {
	rows := corpus(t, 5, 5)
	for _, b := range []int{-1, 3, 12, 8192, 4097} {
		if _, err := Encode(testSchema(), rows, Options{IntegrityBlockRows: b}); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("block size %d: %v, want ErrInvalidOptions", b, err)
		}
	}
	for _, b := range []int{1, 2, 1024, 4096} {
		if _, err := Encode(testSchema(), rows, Options{IntegrityBlockRows: b}); err != nil {
			t.Fatalf("block size %d rejected: %v", b, err)
		}
	}
}

func TestIntegrity_TamperedColumnValueFailsItsGroupOnly(t *testing.T) {
	// A value changed inside a column with a matching CRC: the row decodes,
	// but its group's root no longer matches. The roots, not the CRCs, are
	// the content witness.
	rows := make([]Row, 0, 300)
	for i := 0; i < 300; i++ {
		rows = append(rows, mkRow(uint32(10_000+i), 0, 1, int64(2+i), Property{"i64", sI(int64(1000 + i))}))
	}
	data := mustEncode(t, rows, Options{IntegrityBlockRows: 64}, PageRows)
	secs, err := sections(data)
	if err != nil {
		t.Fatal(err)
	}
	var target sectionInfo
	for _, s := range secs {
		if s.name == "prop:i64" {
			target = s
		}
	}
	if target.length == 0 {
		t.Fatalf("no prop:i64 section in %v", secs)
	}
	tampered := append([]byte(nil), data...)
	tampered[target.offset+target.length-1] ^= 0x01
	resealSection(t, tampered, target)
	seg := mustOpen(t, tampered)
	bad := 0
	for gi := 0; gi < (300+63)/64; gi++ {
		if err := seg.VerifyGroup(gi); err != nil {
			if !errors.Is(err, ErrIntegrity) {
				t.Fatalf("group %d: %v, want ErrIntegrity", gi, err)
			}
			bad++
		}
	}
	if bad != 1 {
		t.Fatalf("%d groups failed, want exactly 1", bad)
	}
	if err := seg.Verify(); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("Verify: %v, want ErrIntegrity", err)
	}
}

func TestFallback_MixedKindColumnKeepsKinds(t *testing.T) {
	values := []graphstate.Scalar{sI(7), sF(t, 7), sS("7"), sB(true), sI(1 << 53), sF(t, 1<<53)}
	rows := make([]Row, 0, len(values))
	for i, v := range values {
		rows = append(rows, mkRow(uint32(100+i), 0, 1, 2, Property{"i64", v}))
	}
	seg := mustOpen(t, mustEncode(t, rows, Options{}, PageRows))
	if seg.sectionLen("fallback") == 0 {
		t.Fatal("mixed-kind values did not reach the fallback column")
	}
	want := byKey(rows)
	for _, g := range scanAll(t, seg) {
		assertSameRow(t, want[rowKey{g.ID, g.Revision}], g)
	}
	if err := seg.Verify(); err != nil {
		t.Fatal(err)
	}
	// A column whose every value has the declared kind needs no fallback.
	clean := []Row{mkRow(1, 0, 1, 2, Property{"i64", sI(1)})}
	if n := mustOpen(t, mustEncode(t, clean, Options{}, PageRows)).sectionLen("fallback"); n != 0 {
		t.Fatalf("fallback section of %d bytes for a clean column", n)
	}
}

func TestFallback_UndeclaredScalarsOfEveryKind(t *testing.T) {
	props := []Property{
		{"x.bool", sB(true)}, {"x.i64", sI(math.MinInt64)}, {"x.i64max", sI(math.MaxInt64)},
		{"x.f64", sF(t, -3.25e300)}, {"x.f64min", sF(t, math.SmallestNonzeroFloat64)},
		{"x.s", sS("ü\x00z")}, {"x.empty", sS("")},
	}
	rows := []Row{mkRow(1, 0, 1, 2, props...)}
	for _, g := range scanAll(t, mustOpen(t, mustEncode(t, rows, Options{}, PageRows))) {
		assertSameRow(t, rows[0], g)
	}
}

// TestSeal_RowThatDoesNotRoundTripIsRefused pins the seal's refusal rule: a
// decoded row that differs from its source fails the seal with ErrInvalidRow.
func TestSeal_RowThatDoesNotRoundTripIsRefused(t *testing.T) {
	sealed := []Row{mkRow(1, 0, 1, 2, Property{"i64", sI(2)})}
	data := mustEncode(t, sealed, Options{}, PageRows)
	source := []Row{mkRow(1, 0, 1, 2, Property{"i64", sI(3)})}
	if err := verifySealed(data, source, []int{0}); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("verifySealed on a differing row: %v, want ErrInvalidRow", err)
	}
	// Same content but a different hash cannot happen without a different
	// row; a row with another type name is the hash-only difference.
	if err := verifySealedHash(data, "OTHER", sealed, []int{0}); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("verifySealedHash with another type name: %v, want ErrHashMismatch", err)
	}
}

func TestEdge_EmptySegment(t *testing.T) {
	seg := mustOpen(t, mustEncode(t, nil, Options{}, PageRows))
	if seg.Len() != 0 {
		t.Fatalf("Len %d", seg.Len())
	}
	if got := scanAll(t, seg); len(got) != 0 {
		t.Fatalf("scanned %d rows", len(got))
	}
	if _, err := seg.Row(0); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("Row(0) on empty: %v", err)
	}
	if ids, err := seg.Lookup(1); err != nil || len(ids) != 0 {
		t.Fatalf("Lookup: %v %v", ids, err)
	}
	if lo, hi, err := seg.OutRows(1); err != nil || lo != hi {
		t.Fatalf("OutRows: %d %d %v", lo, hi, err)
	}
	if err := seg.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	// An empty segment from an empty (non-nil) slice is identical.
	other, err := encode(testSchema(), []Row{}, Options{}, PageRows)
	if err != nil || !bytes.Equal(other, mustEncode(t, nil, Options{}, PageRows)) {
		t.Fatalf("nil and empty slices encode differently: %v", err)
	}
}

func TestEdge_OneRow(t *testing.T) {
	r := mkRow(42, 3, 5, 5, Property{"s", sS("only")})
	r.ValidFrom, r.TxFrom = Finite(0), Finite(0)
	seg := mustOpen(t, mustEncode(t, []Row{r}, Options{IntegrityBlockRows: 4096}, PageRows))
	got := scanAll(t, seg)
	if len(got) != 1 {
		t.Fatalf("%d rows", len(got))
	}
	assertSameRow(t, r, got[0])
	if err := seg.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestEdge_MaxIDsAndNodes(t *testing.T) {
	r := mkRow(1, math.MaxUint32, 1, 2)
	r.ID, r.Start, r.End = math.MaxInt64, math.MaxInt64, math.MaxInt64
	r2 := r
	r2.ID, r2.Start, r2.End, r2.Revision = 1, 1, math.MaxInt64, 0
	rows := []Row{r, r2}
	seg := mustOpen(t, mustEncode(t, rows, Options{}, PageRows))
	want := byKey(rows)
	for _, g := range scanAll(t, seg) {
		assertSameRow(t, want[rowKey{g.ID, g.Revision}], g)
	}
	if lo, hi := seg.IDRange(); lo != 1 || hi != math.MaxInt64 {
		t.Fatalf("IDRange %d..%d", lo, hi)
	}
	if ix, err := seg.Lookup(math.MaxInt64); err != nil || len(ix) != 1 {
		t.Fatalf("Lookup(MaxInt64) = %v, %v", ix, err)
	}
	if lo, hi, _ := seg.OutRows(math.MaxInt64); hi-lo != 1 {
		t.Fatalf("OutRows(MaxInt64) = [%d,%d)", lo, hi)
	}
	if in, _ := seg.InRows(math.MaxInt64); len(in) != 2 {
		t.Fatalf("InRows(MaxInt64) = %v", in)
	}
}

func TestEdge_MaxBlockSizeWithPartialLastGroup(t *testing.T) {
	rows := corpus(t, 4096+17, 6)
	seg := mustOpen(t, mustEncode(t, rows, Options{IntegrityBlockRows: MaxIntegrityBlockRows}, PageRows))
	for _, gi := range []int{0, 1} {
		if err := seg.VerifyGroup(gi); err != nil {
			t.Fatalf("group %d: %v", gi, err)
		}
	}
	if err := seg.VerifyGroup(2); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("group 2: %v", err)
	}
}

func TestEncode_UnsortedInputYieldsTheSameBytesAsSortedInput(t *testing.T) {
	rows := corpus(t, 700, 21)
	want := mustEncode(t, rows, Options{}, 64)
	for seed := uint64(0); seed < 3; seed++ {
		shuffled := slices.Clone(rows)
		rng := rand.New(rand.NewPCG(seed, 1)) // #nosec G404 -- test data
		rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
		if got := mustEncode(t, shuffled, Options{}, 64); !bytes.Equal(got, want) {
			t.Fatalf("shuffle %d changes the bytes", seed)
		}
	}
	rev := slices.Clone(rows)
	slices.Reverse(rev)
	if !bytes.Equal(mustEncode(t, rev, Options{}, 64), want) {
		t.Fatal("reversed input changes the bytes")
	}
	// The input slice is not modified.
	before := slices.Clone(rows)
	_ = mustEncode(t, rows, Options{}, 64)
	if !reflect.DeepEqual(before, rows) {
		t.Fatal("Encode modified its input")
	}
}

func TestEncode_RejectsInvalidInput(t *testing.T) {
	good := corpus(t, 3, 7)
	with := func(f func(r *Row)) []Row {
		r := mkRow(1, 0, 1, 2)
		f(&r)
		return []Row{r}
	}
	cases := map[string]struct {
		schema Schema
		rows   []Row
		want   error
	}{
		"duplicate id+revision": {testSchema(), []Row{mkRow(1, 0, 1, 2), mkRow(1, 0, 3, 4)}, ErrInvalidRow},
		"duplicate far apart":   {testSchema(), []Row{mkRow(1, 0, 1, 2), mkRow(2, 0, 1, 2), mkRow(3, 0, 5, 6), mkRow(1, 0, 9, 9)}, ErrInvalidRow},
		"id zero":               {testSchema(), with(func(r *Row) { r.ID = 0 }), ErrInvalidRow},
		"id negative":           {testSchema(), with(func(r *Row) { r.ID = -1 }), ErrInvalidRow},
		"id min int64":          {testSchema(), with(func(r *Row) { r.ID = math.MinInt64 }), ErrInvalidRow},
		"start zero":            {testSchema(), with(func(r *Row) { r.Start = 0 }), ErrInvalidRow},
		"end negative":          {testSchema(), with(func(r *Row) { r.End = -4 }), ErrInvalidRow},
		"zero valid_from":       {testSchema(), with(func(r *Row) { r.ValidFrom = Coord{} }), ErrInvalidRow},
		"zero valid_to":         {testSchema(), with(func(r *Row) { r.ValidTo = Coord{} }), ErrInvalidRow},
		"zero tx_from":          {testSchema(), with(func(r *Row) { r.TxFrom = Coord{} }), ErrInvalidRow},
		"zero tx_to":            {testSchema(), with(func(r *Row) { r.TxTo = Coord{} }), ErrInvalidRow},
		"valid_from +inf":       {testSchema(), with(func(r *Row) { r.ValidFrom = PosInf() }), ErrInvalidRow},
		"valid_to -inf":         {testSchema(), with(func(r *Row) { r.ValidTo = NegInf() }), ErrInvalidRow},
		"tx_from +inf":          {testSchema(), with(func(r *Row) { r.TxFrom = PosInf() }), ErrInvalidRow},
		"tx_to -inf":            {testSchema(), with(func(r *Row) { r.TxTo = NegInf() }), ErrInvalidRow},
		"unknown coord kind":    {testSchema(), with(func(r *Row) { r.ValidTo = Coord{kind: 9} }), ErrInvalidRow},
		"nonscalar property":    {testSchema(), with(func(r *Row) { r.Props = []Property{{"a", graphstate.Null()}} }), ErrInvalidRow},
		"nil property value":    {testSchema(), with(func(r *Row) { r.Props = []Property{{"a", graphstate.Scalar{}}} }), ErrInvalidRow},
		"duplicate property":    {testSchema(), with(func(r *Row) { r.Props = []Property{{"a", sI(1)}, {"a", sI(2)}} }), ErrInvalidRow},
		"empty property key":    {testSchema(), with(func(r *Row) { r.Props = []Property{{"", sI(1)}} }), ErrInvalidRow},
		"oversize property key": {testSchema(), with(func(r *Row) { r.Props = []Property{{string(make([]byte, 1<<16)), sI(1)}} }), ErrInvalidRow},
		"empty type name":       {Schema{}, good, ErrInvalidSchema},
		"oversize type name":    {Schema{TypeName: string(make([]byte, 1<<16))}, good, ErrInvalidSchema},
		"duplicate column":      {Schema{TypeName: testType, Columns: []Column{{"a", KindI64}, {"a", KindString}}}, good, ErrInvalidSchema},
		"empty column name":     {Schema{TypeName: testType, Columns: []Column{{"", KindI64}}}, good, ErrInvalidSchema},
		"unsupported kind":      {Schema{TypeName: testType, Columns: []Column{{"a", Kind(15)}}}, good, ErrInvalidSchema},
		"kind zero":             {Schema{TypeName: testType, Columns: []Column{{"a", 0}}}, good, ErrInvalidSchema},
		"too many columns":      {Schema{TypeName: testType, Columns: manyColumns(MaxColumns + 1)}, good, ErrInvalidSchema},
	}
	for name, tc := range cases {
		if _, err := Encode(tc.schema, tc.rows, Options{}); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, want %v", name, err, tc.want)
		}
	}
	// A reserved-looking name is an ordinary column in v5 (no tkg_ system keys).
	if _, err := Encode(Schema{TypeName: testType, Columns: []Column{{"tkg_x", KindI64}}}, good, Options{}); err != nil {
		t.Errorf("tkg_ column refused: %v", err)
	}
	// Many max-size columns still fit.
	if _, err := Encode(Schema{TypeName: testType, Columns: manyColumns(MaxColumns)}, good, Options{}); err != nil {
		t.Errorf("MaxColumns refused: %v", err)
	}
}

func TestEncode_OversizeRowCountIsRefused(t *testing.T) {
	old := maxRows
	maxRows = 3
	defer func() { maxRows = old }()
	rows := []Row{mkRow(1, 0, 1, 2), mkRow(2, 0, 1, 2), mkRow(3, 0, 1, 2)}
	if _, err := Encode(testSchema(), rows, Options{}); err != nil {
		t.Fatalf("exactly maxRows rows: %v", err)
	}
	if _, err := Encode(testSchema(), append(rows, mkRow(4, 0, 1, 2)), Options{}); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("maxRows+1 rows: %v, want ErrInvalidRow", err)
	}
	data, err := encode(testSchema(), rows, Options{}, PageRows)
	if err != nil {
		t.Fatal(err)
	}
	maxRows = 2
	if _, err := Open(data); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open of a segment above maxRows: %v, want ErrCorrupt", err)
	}
}

func TestRowError_NamesTheRefusedRow(t *testing.T) {
	good := mkRow(10, 0, 1, 2)
	bad := mkRow(11, 3, 1, 2)
	bad.ValidFrom = PosInf()
	_, err := Encode(testSchema(), []Row{good, bad}, Options{})
	var re *RowError
	if !errors.As(err, &re) || re.ID != 11 || re.Revision != 3 || !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("Encode with a bad row = %v, want a RowError for id 11 r3 wrapping ErrInvalidRow", err)
	}
	if re.Error() == "" || !errors.Is(re.Unwrap(), ErrInvalidRow) {
		t.Fatalf("RowError text/unwrap: %q %v", re.Error(), re.Unwrap())
	}
}

func manyColumns(n int) []Column {
	cols := make([]Column, n)
	for i := range cols {
		cols[i] = Column{Name: fmt.Sprintf("c%d", i), Kind: KindI64}
	}
	return cols
}

func TestLookupAndAdjacency(t *testing.T) {
	rows := corpus(t, 3000, 8)
	seg := mustOpen(t, mustEncode(t, rows, Options{}, 128))
	got := scanAll(t, seg)
	wantRows := map[types.RelID][]int{}
	for i, r := range got {
		wantRows[r.ID] = append(wantRows[r.ID], i)
	}
	for id, want := range wantRows {
		ix, err := seg.Lookup(id)
		if err != nil {
			t.Fatal(err)
		}
		sort.Ints(ix)
		if !reflect.DeepEqual(ix, want) {
			t.Fatalf("Lookup(%d) = %v, want %v", id, ix, want)
		}
	}
	for _, phantom := range []types.RelID{0, -1, 1, got[0].ID - 1, got[len(got)-1].ID + 1, math.MaxInt64} {
		if _, ok := wantRows[phantom]; ok {
			continue
		}
		if ix, err := seg.Lookup(phantom); err != nil || len(ix) != 0 {
			t.Fatalf("Lookup(phantom %d) = %v, %v", phantom, ix, err)
		}
	}
	outs, ins := map[types.NodeID][]int{}, map[types.NodeID][]int{}
	for i, r := range got {
		outs[r.Start] = append(outs[r.Start], i)
		ins[r.End] = append(ins[r.End], i)
	}
	nodes := map[types.NodeID]struct{}{0: {}, -1: {}, 2: {}}
	for n := range outs {
		nodes[n] = struct{}{}
	}
	for n := range ins {
		nodes[n] = struct{}{}
	}
	for n := range nodes {
		lo, hi, err := seg.OutRows(n)
		if err != nil {
			t.Fatal(err)
		}
		var run []int
		for i := lo; i < hi; i++ {
			run = append(run, i)
		}
		if len(run) != len(outs[n]) || (len(run) > 0 && !reflect.DeepEqual(run, outs[n])) {
			t.Fatalf("OutRows(%d) = [%d,%d), want %v", n, lo, hi, outs[n])
		}
		in, err := seg.InRows(n)
		if err != nil {
			t.Fatal(err)
		}
		sort.Ints(in)
		if len(in) == 0 && len(ins[n]) == 0 {
			continue
		}
		if !reflect.DeepEqual(in, ins[n]) {
			t.Fatalf("InRows(%d) = %v, want %v", n, in, ins[n])
		}
	}
}

// --- corruption ---

func TestCorrupt_EverySectionFlipIsRejectedWithItsName(t *testing.T) {
	data := mustEncode(t, corpus(t, 2000, 9), Options{}, 256)
	secs, err := sections(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(secs) < 10 {
		t.Fatalf("only %d sections", len(secs))
	}
	for _, s := range secs {
		for _, off := range []int{0, s.length / 2, s.length - 1} {
			bad := append([]byte(nil), data...)
			bad[s.offset+off] ^= 0x40
			_, err := Open(bad)
			var ce *CorruptError
			if !errors.Is(err, ErrCorrupt) || !errors.As(err, &ce) || ce.Section != s.name {
				t.Fatalf("flip in %s at +%d: %v, want CorruptError{Section:%q}", s.name, off, err, s.name)
			}
		}
	}
	for _, where := range []struct {
		name string
		off  int
	}{{"header", 3}, {"header", 20}, {"trailer", len(data) - 1}, {"footer", len(data) - trailerSize - 2}} {
		bad := append([]byte(nil), data...)
		bad[where.off] ^= 0x01
		_, err := Open(bad)
		var ce *CorruptError
		if !errors.As(err, &ce) || ce.Section != where.name {
			t.Fatalf("flip in %s: %v", where.name, err)
		}
	}
}

func TestCorrupt_TruncationAtAnyOffsetIsRejected(t *testing.T) {
	data := mustEncode(t, corpus(t, 300, 10), Options{}, 64)
	for n := 0; n < len(data); n += 1 + n/40 {
		if _, err := Open(data[:n]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncated to %d of %d: %v, want ErrCorrupt", n, len(data), err)
		}
	}
	if _, err := Open(data[:len(data)-1]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("one byte missing accepted: %v", err)
	}
	if _, err := Open(append(append([]byte(nil), data...), 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("one trailing byte accepted: %v", err)
	}
	for _, in := range [][]byte{nil, {}, {0}, bytes.Repeat([]byte{0xff}, 4096)} {
		if _, err := Open(in); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("garbage %d bytes: %v", len(in), err)
		}
	}
}

func TestCorrupt_V4SegmentIsNotAV5Segment(t *testing.T) {
	data := mustEncode(t, corpus(t, 5, 1), Options{}, PageRows)
	v4 := append([]byte(nil), data...)
	copy(v4, "TKGSEG\x00\x01")
	if _, err := Open(v4); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("v4 magic accepted: %v", err)
	}
}

func TestVersion_UnknownFormatAndFooterVersionsFailClosed(t *testing.T) {
	data := mustEncode(t, corpus(t, 50, 11), Options{}, PageRows)

	hdr := append([]byte(nil), data...)
	binary.LittleEndian.PutUint16(hdr[8:], CurrentFormatVersion+1)
	binary.LittleEndian.PutUint32(hdr[56:], crc32.Checksum(hdr[:56], castagnoli))
	if _, err := Open(hdr); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("format %d: %v, want ErrUnsupportedVersion", CurrentFormatVersion+1, err)
	}

	ftr := append([]byte(nil), data...)
	fstart, _ := footerBounds(t, ftr)
	binary.LittleEndian.PutUint16(ftr[fstart:], CurrentFooterVersion+1)
	resealFooter(t, ftr)
	if _, err := Open(ftr); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("footer %d: %v, want ErrUnsupportedVersion", CurrentFooterVersion+1, err)
	}
}

func TestAlloc_LyingCountsDoNotAllocate(t *testing.T) {
	data := mustEncode(t, corpus(t, 40, 12), Options{}, PageRows)
	for _, patch := range []struct {
		name string
		off  int
	}{{"rowCount", 16}, {"nodeCount", 20}} {
		bad := append([]byte(nil), data...)
		binary.LittleEndian.PutUint32(bad[patch.off:], math.MaxUint32)
		binary.LittleEndian.PutUint32(bad[56:], crc32.Checksum(bad[:56], castagnoli))
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		seg, err := Open(bad)
		if err == nil {
			_ = seg.Scan(func(int, *Row) bool { return true })
		}
		runtime.ReadMemStats(&after)
		if err == nil {
			t.Fatalf("%s = 2^32-1 accepted", patch.name)
		}
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("%s: %v, want ErrCorrupt", patch.name, err)
		}
		if d := after.TotalAlloc - before.TotalAlloc; d > 1<<20 {
			t.Fatalf("%s: lying count allocated %d bytes", patch.name, d)
		}
	}
}

// TestCorrupt_DecodedValuesThatCannotBeRowsAreRefused: values the CRC layer
// accepts (resealed) but no valid row can hold are refused on read, never
// returned as rows.
func TestCorrupt_DecodedValuesThatCannotBeRowsAreRefused(t *testing.T) {
	rows := []Row{mkRow(1, 0, 1, 2, Property{"f", sF(t, 5)}), mkRow(2, 0, 1, 3)}
	data := mustEncode(t, rows, Options{}, PageRows)
	secs, _ := sections(data)
	for _, name := range []string{colRelID, secNodes, "prop:f"} {
		var target sectionInfo
		for _, s := range secs {
			if s.name == name {
				target = s
			}
		}
		if target.length == 0 {
			t.Fatalf("no section %s", name)
		}
		for off := 0; off < target.length; off++ {
			for _, x := range []byte{0x80, 0xff, 0x01} {
				bad := append([]byte(nil), data...)
				bad[target.offset+off] ^= x
				resealSection(t, bad, target)
				seg, err := Open(bad)
				if err != nil {
					continue
				}
				_ = seg.Scan(func(_ int, r *Row) bool {
					if r.ID < 1 || r.Start < 1 || r.End < 1 {
						t.Fatalf("%s+%d^%#x: row with ids %d %d %d returned", name, off, x, r.ID, r.Start, r.End)
					}
					return true
				})
			}
		}
	}
}

// --- test helpers over the byte layout ---

func footerBounds(tb testing.TB, data []byte) (start, end int) {
	tb.Helper()
	n := len(data)
	flen := int(binary.LittleEndian.Uint32(data[n-trailerSize:]))
	return n - trailerSize - flen, n - trailerSize
}

func resealFooter(tb testing.TB, data []byte) {
	tb.Helper()
	s, e := footerBounds(tb, data)
	binary.LittleEndian.PutUint32(data[len(data)-trailerSize+4:], crc32.Checksum(data[s:e], castagnoli))
}

// resealSection recomputes one section's CRC in the footer's directory and
// then the footer CRC, so a deliberate content change passes the CRC layer.
func resealSection(tb testing.TB, data []byte, s sectionInfo) {
	tb.Helper()
	binary.LittleEndian.PutUint32(data[s.crcAt:], crc32.Checksum(data[s.offset:s.offset+s.length], castagnoli))
	resealFooter(tb, data)
}

// TestRoot_IsSHA256OverGroupRootsOfRowHashes recomputes the witness
// independently of the writer: group roots over RowHash in segment order, the
// segment root over the group roots.
func TestRoot_IsSHA256OverGroupRootsOfRowHashes(t *testing.T) {
	rows := corpus(t, 300, 13)
	seg := mustOpen(t, mustEncode(t, rows, Options{IntegrityBlockRows: 16}, PageRows))
	var roots, leaf []byte
	for i, g := range scanAll(t, seg) {
		h, err := RowHash(testType, &g)
		if err != nil {
			t.Fatal(err)
		}
		leaf = append(leaf, h[:]...)
		if (i+1)%16 == 0 || i == len(rows)-1 {
			sum := sha256.Sum256(leaf)
			roots, leaf = append(roots, sum[:]...), leaf[:0]
		}
	}
	if got, exp := seg.Root(), sha256.Sum256(roots); got != exp {
		t.Fatalf("Root %x, want %x", got, exp)
	}
}

func TestScan_StopsWhenTheCallbackSaysSo(t *testing.T) {
	seg := mustOpen(t, mustEncode(t, corpus(t, 200, 14), Options{}, 16))
	n := 0
	if err := seg.Scan(func(int, *Row) bool { n++; return n < 5 }); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("callback ran %d times, want 5", n)
	}
	if _, err := seg.Row(-1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("Row(-1): %v", err)
	}
	if err := seg.VerifyGroup(-1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("VerifyGroup(-1): %v", err)
	}
}

func TestCorruptError_NamesSectionAndReason(t *testing.T) {
	err := error(&CorruptError{Section: "col:end", Reason: "page 3 truncated"})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatal("CorruptError does not unwrap to ErrCorrupt")
	}
	if msg := err.Error(); msg != "segment: corrupt col:end: page 3 truncated" {
		t.Fatalf("message %q", msg)
	}
}
