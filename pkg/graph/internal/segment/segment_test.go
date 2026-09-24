package segment

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const (
	testType  = "HOP"
	testToken = uint16(7)
)

// testSchema declares one column of every supported kind.
func testSchema() Schema {
	return Schema{TypeName: testType, TypeToken: testToken, Columns: []Column{
		{"s", KindString}, {"b", KindBool}, {"i", KindInt}, {"i8", KindInt8},
		{"i16", KindInt16}, {"i32", KindInt32}, {"i64", KindInt64}, {"u", KindUint},
		{"u8", KindUint8}, {"u16", KindUint16}, {"u32", KindUint32}, {"u64", KindUint64},
		{"f32", KindFloat32}, {"f64", KindFloat64},
	}}
}

// mkRow builds a relationship the way the store holds it: properties,
// version, temporal metadata, and integrity whose Hash is the real content
// hash (unless ig already names one).
func mkRow(tb testing.TB, id int64, version uint32, start, end int64, props map[string]any,
	tm *types.TemporalMetadata, ig *types.RelIntegrity,
) *types.Relationship {
	tb.Helper()
	r := types.NewRelationship(types.RelID(id), testToken, types.NodeID(start), types.NodeID(end))
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		tb.Fatalf("props: %v", err)
	}
	if err := r.SetProperties(ps); err != nil {
		tb.Fatalf("set props: %v", err)
	}
	r.SetVersion(version)
	if tm != nil {
		r.SetTemporal(tm)
	}
	if ig == nil {
		ig = &types.RelIntegrity{}
	}
	if ig.Hash == "" {
		ig.Hash = integrity.ComputeRelHash(r, testType)
	}
	r.SetIntegrity(ig)
	return r
}

func hexHash(rng *rand.Rand) string {
	var b [32]byte
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return fmt.Sprintf("%x", b[:])
}

// corpus builds n adversarial rows: every declared kind with extreme values,
// absent declared properties, kind mismatches (fallback), undeclared complex
// properties, several versions of one ID, nil and fully populated temporal
// metadata, every sparse integrity field, and endpoint hashes that differ
// from the node's usual hash.
func corpus(tb testing.TB, n int, seed uint64) []*types.Relationship {
	tb.Helper()
	rng := rand.New(rand.NewPCG(seed, 99)) // #nosec G404 -- test data
	nodes := make([]int64, 37)
	nodeHash := make([]string, len(nodes))
	for i := range nodes {
		nodes[i] = 1_000_000 + int64(i)*977 + int64(rng.IntN(50))
		nodeHash[i] = hexHash(rng)
	}
	extremesI := []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 1 << 40}
	extremesU := []uint64{0, 1, math.MaxUint64, 1 << 63}
	extremesF := []float64{0, math.Copysign(0, -1), math.NaN(), math.Inf(1), math.Inf(-1), 1.5, -3.25e300, math.Float64frombits(0x7ff8000000000123)}
	rows := make([]*types.Relationship, 0, n)
	id := int64(5_000_000_000)
	for i := 0; i < n; i++ {
		version := uint32(0)
		if i > 0 && rng.IntN(7) == 0 {
			version = rows[i-1].Version() + 1 // another version of the previous ID
		} else {
			id += 1 + int64(rng.IntN(1<<20))
		}
		s, e := rng.IntN(len(nodes)), rng.IntN(len(nodes))
		props := map[string]any{}
		put := func(k string, v any) {
			if rng.IntN(9) != 0 { // sometimes absent
				props[k] = v
			}
		}
		strs := []string{"", "a", "actor-1", "ünïcode", string([]byte{0, 255, 7})}
		put("s", strs[rng.IntN(len(strs))])
		put("b", rng.IntN(2) == 0)
		put("i", int(extremesI[rng.IntN(len(extremesI))]))
		put("i8", int8(rng.IntN(256)-128))
		put("i16", int16(rng.IntN(65536)-32768))
		put("i32", int32(rng.Uint32()))
		put("i64", extremesI[rng.IntN(len(extremesI))])
		put("u", uint(extremesU[rng.IntN(len(extremesU))]))
		put("u8", uint8(rng.IntN(256)))
		put("u16", uint16(rng.IntN(65536)))
		put("u32", rng.Uint32())
		put("u64", extremesU[rng.IntN(len(extremesU))])
		put("f32", float32(extremesF[rng.IntN(len(extremesF))]))
		put("f64", extremesF[rng.IntN(len(extremesF))])
		switch rng.IntN(6) { // kind mismatches and undeclared properties
		case 0:
			props["i64"] = "not an int"
		case 1:
			props["s"] = int64(42)
		case 2:
			props["extra"] = []any{"x", int64(3), map[string]any{"k": 1.5, "u": uint64(9)}}
			props["tags"] = []string{"a", "b"}
		case 3:
			props["when"] = types.TemporalValue{Kind: types.TemporalDate, Value: "2026-09-24"}
			props["vec"] = []float32{1, 2.5}
		}
		var tm *types.TemporalMetadata
		if rng.IntN(10) != 0 {
			vf := int64(1_787_443_200_000) + rng.Int64N(86_400_000)
			tm = &types.TemporalMetadata{ValidFrom: types.Instant(vf), TxFrom: types.Instant(vf + 30_000)}
			if rng.IntN(5) != 0 {
				tm.ValidTo = types.Instant(vf + 1 + rng.Int64N(5000))
			}
			if rng.IntN(8) == 0 {
				tm.TxTo = types.Instant(vf + 60_000)
				tm.CreatedAt = types.Instant(vf - 5)
				tm.UpdatedAt = types.Instant(vf + 7)
				tm.DeletedAt = types.Instant(vf + 9)
				tm.CreatedBy = "creator"
				tm.UpdatedBy = "updater-" + fmt.Sprint(rng.IntN(3))
				tm.SetBaseEntityID(types.EntityID(id - 1))
			}
		}
		ig := &types.RelIntegrity{FromNodeHash: nodeHash[s], ToNodeHash: nodeHash[e]}
		switch rng.IntN(10) {
		case 0:
			ig.PrevHash = hexHash(rng)
			ig.AuthorID = "author"
			ig.AuthorizedBy = "boss"
			ig.AuthorizationLevel = uint8(1 + rng.IntN(255))
			ig.Signature = []byte{1, 2, 3}
		case 1:
			ig.Signature = []byte{}        // empty but non-nil
			ig.FromNodeHash = hexHash(rng) // endpoint changed since
		case 2:
			ig.ToNodeHash = "not-hex"
		}
		rows = append(rows, mkRow(tb, id, version, nodes[s], nodes[e], props, tm, ig))
	}
	return rows
}

// assertSameRow is bit-exact equality: identity, endpoints, version,
// every temporal and integrity field, and every property's key, Go type and
// hash bytes (which carry the type tag and the exact bit pattern, so NaN
// payloads and -0 are compared exactly).
func assertSameRow(tb testing.TB, want, got *types.Relationship) {
	tb.Helper()
	if got == nil {
		tb.Fatalf("row %d: nil", want.ID())
	}
	if got.ID() != want.ID() || got.TypeToken() != want.TypeToken() || got.StartNodeID() != want.StartNodeID() ||
		got.EndNodeID() != want.EndNodeID() || got.Version() != want.Version() {
		tb.Fatalf("identity: got (%d,%d,%d->%d,v%d) want (%d,%d,%d->%d,v%d)", got.ID(), got.TypeToken().Value(),
			got.StartNodeID(), got.EndNodeID(), got.Version(), want.ID(), want.TypeToken().Value(),
			want.StartNodeID(), want.EndNodeID(), want.Version())
	}
	wt, gt := want.Temporal(), got.Temporal()
	if (wt == nil) != (gt == nil) {
		tb.Fatalf("row %d: temporal nil-ness got %v want %v", want.ID(), gt == nil, wt == nil)
	}
	if wt != nil && (*wt != *gt || wt.BaseEntityID() != gt.BaseEntityID()) {
		tb.Fatalf("row %d: temporal got %+v want %+v", want.ID(), *gt, *wt)
	}
	wi, gi := want.Integrity(), got.Integrity()
	if !reflect.DeepEqual(wi, gi) {
		tb.Fatalf("row %d: integrity got %+v want %+v", want.ID(), gi, wi)
	}
	wp, gp := want.Properties(), got.Properties()
	if len(wp) != len(gp) {
		tb.Fatalf("row %d: %d properties, want %d (%v vs %v)", want.ID(), len(gp), len(wp), gp, wp)
	}
	for k := range wp {
		if wp[k].Key != gp[k].Key || reflect.TypeOf(wp[k].Value) != reflect.TypeOf(gp[k].Value) ||
			!bytes.Equal(types.AppendPropertyValueHashBytes(nil, wp[k].Value), types.AppendPropertyValueHashBytes(nil, gp[k].Value)) {
			tb.Fatalf("row %d: property %q got %T(%v) want %q %T(%v)", want.ID(), gp[k].Key, gp[k].Value, gp[k].Value,
				wp[k].Key, wp[k].Value, wp[k].Value)
		}
	}
}

type rowKey struct {
	id      types.RelID
	version uint32
}

func byKey(rows []*types.Relationship) map[rowKey]*types.Relationship {
	m := make(map[rowKey]*types.Relationship, len(rows))
	for _, r := range rows {
		m[rowKey{r.ID(), r.Version()}] = r
	}
	return m
}

func mustEncode(tb testing.TB, rows []*types.Relationship, opts Options, pageRows int) []byte {
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

// scanAll decodes every row and checks segment order: (start, end,
// valid_from, id, version) ascending.
func scanAll(tb testing.TB, seg *Segment) []*types.Relationship {
	tb.Helper()
	var out []*types.Relationship
	if err := seg.Scan(func(i int, r *types.Relationship) bool {
		if i != len(out) {
			tb.Fatalf("scan index %d, want %d", i, len(out))
		}
		out = append(out, r)
		return true
	}); err != nil {
		tb.Fatalf("scan: %v", err)
	}
	for i := 1; i < len(out); i++ {
		if !rowLess(out[i-1], out[i]) {
			tb.Fatalf("rows %d,%d out of segment order", i-1, i)
		}
	}
	return out
}

func vfOf(r *types.Relationship) types.Instant {
	if tm := r.Temporal(); tm != nil {
		return tm.ValidFrom
	}
	return 0
}

func rowLess(a, b *types.Relationship) bool {
	if a.StartNodeID() != b.StartNodeID() {
		return a.StartNodeID() < b.StartNodeID()
	}
	if a.EndNodeID() != b.EndNodeID() {
		return a.EndNodeID() < b.EndNodeID()
	}
	if vfOf(a) != vfOf(b) {
		return vfOf(a) < vfOf(b)
	}
	if a.ID() != b.ID() {
		return a.ID() < b.ID()
	}
	return a.Version() < b.Version()
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
				w := want[rowKey{g.ID(), g.Version()}]
				if w == nil {
					t.Fatalf("phantom row %d v%d", g.ID(), g.Version())
				}
				assertSameRow(t, w, g)
				r, err := seg.Row(i)
				if err != nil {
					t.Fatalf("Row(%d): %v", i, err)
				}
				assertSameRow(t, w, r)
			}
		})
	}
}

func TestRoundTrip_SchemaAndTypeNameSurvive(t *testing.T) {
	seg := mustOpen(t, mustEncode(t, corpus(t, 10, 2), Options{}, PageRows))
	if !reflect.DeepEqual(seg.Schema(), testSchema()) {
		t.Fatalf("schema %+v, want %+v", seg.Schema(), testSchema())
	}
}

func TestHashes_RecomputedRowHashEqualsStoredHashAtEveryBlockSize(t *testing.T) {
	rows := corpus(t, 5000, 3)
	want := byKey(rows)
	for _, b := range []int{1, 64, 4096} {
		t.Run(fmt.Sprintf("IntegrityBlockRows=%d", b), func(t *testing.T) {
			seg := mustOpen(t, mustEncode(t, rows, Options{IntegrityBlockRows: b}, PageRows))
			if seg.IntegrityBlockRows() != b {
				t.Fatalf("footer block size %d, want %d", seg.IntegrityBlockRows(), b)
			}
			for _, g := range scanAll(t, seg) {
				stored := want[rowKey{g.ID(), g.Version()}].Integrity().Hash
				if h := integrity.ComputeRelHash(g, testType); h != stored {
					t.Fatalf("row %d v%d: recomputed %s, stored %s", g.ID(), g.Version(), h, stored)
				}
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
	// A value changed inside a column with a matching CRC (an encoder bug or
	// a flip the CRC cannot see): the row decodes, but its group's root no
	// longer matches. The hash roots, not the CRCs, are the content witness.
	rows := make([]*types.Relationship, 0, 300)
	for i := 0; i < 300; i++ {
		rows = append(rows, mkRow(t, int64(10_000+i), 0, 1, int64(2+i), map[string]any{"i64": int64(1000 + i)}, nil, nil))
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
	// Flip the last byte of the packed values (belongs to the last rows), then
	// re-seal the section CRC and the footer CRC.
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

func TestFallback_MixedKindColumnKeepsKindsAndHashes(t *testing.T) {
	values := []any{int64(7), int(7), float64(7), "7", int32(7), uint64(7), []any{int64(7)}}
	rows := make([]*types.Relationship, 0, len(values))
	for i, v := range values {
		rows = append(rows, mkRow(t, int64(100+i), 0, 1, 2, map[string]any{"i64": v}, nil, nil))
	}
	data := mustEncode(t, rows, Options{}, PageRows)
	seg := mustOpen(t, data)
	if seg.sectionLen("fallback") == 0 {
		t.Fatal("mixed-kind values did not reach the fallback column")
	}
	want := byKey(rows)
	for _, g := range scanAll(t, seg) {
		w := want[rowKey{g.ID(), g.Version()}]
		assertSameRow(t, w, g)
		if h := integrity.ComputeRelHash(g, testType); h != w.Integrity().Hash {
			t.Fatalf("row %d: hash %s, stored %s", g.ID(), h, w.Integrity().Hash)
		}
	}
	// A column whose every value has the declared kind needs no fallback.
	clean := []*types.Relationship{mkRow(t, 1, 0, 1, 2, map[string]any{"i64": int64(1)}, nil, nil)}
	if n := mustOpen(t, mustEncode(t, clean, Options{}, PageRows)).sectionLen("fallback"); n != 0 {
		t.Fatalf("fallback section of %d bytes for a clean column", n)
	}
}

// TestFallback_NestedKindsRoundTrip: the fallback column uses the entity
// wire, which before backlog item 3 widened nested values ([]any{int16(2)}
// read back as int64), so Encode refused such rows. The wire now keeps every
// nested kind, so the rows seal, decode field for field and recompute their
// stored hash.
func TestFallback_NestedKindsRoundTrip(t *testing.T) {
	values := []any{
		[]any{int16(2)}, []any{int8(-1)}, []any{int32(3)}, []any{int(4)},
		[]any{uint8(5)}, []any{uint16(6)}, []any{uint32(7)}, []any{uint(8)},
		[]any{float32(1.5)}, []any{[]string{"a"}}, []any{[]int{1}}, []any{[]string(nil)},
		map[string]any{"k": int16(2)}, map[string]any{"k": map[string]string{"a": "b"}},
		map[string]any{"k": []any{uint8(9), map[string]any{"j": int8(1)}}},
	}
	rows := make([]*types.Relationship, 0, len(values))
	for i, v := range values {
		rows = append(rows, mkRow(t, int64(200+i), 0, 1, 2, map[string]any{"nested": v}, nil, nil))
	}
	data, err := Encode(testSchema(), rows, Options{})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	seg := mustOpen(t, data)
	want := byKey(rows)
	got := scanAll(t, seg)
	if len(got) != len(rows) {
		t.Fatalf("%d rows, want %d", len(got), len(rows))
	}
	for _, g := range got {
		w := want[rowKey{g.ID(), g.Version()}]
		assertSameRow(t, w, g)
		if h := integrity.ComputeRelHash(g, testType); h != w.Integrity().Hash {
			t.Fatalf("row %d: hash %s, stored %s", g.ID(), h, w.Integrity().Hash)
		}
	}
	if err := seg.Verify(); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// TestSeal_RowThatDoesNotRoundTripIsRefused pins the seal's refusal rule
// directly (it held before backlog item 3 too; the nested-int16 row that used
// to exercise it now round-trips): a decoded row that differs from its source
// in a nested value's kind fails the seal with ErrInvalidRow.
func TestSeal_RowThatDoesNotRoundTripIsRefused(t *testing.T) {
	sealed := []*types.Relationship{mkRow(t, 1, 0, 1, 2, map[string]any{"nested": []any{int64(2)}}, nil, nil)}
	data := mustEncode(t, sealed, Options{}, PageRows)
	source := []*types.Relationship{mkRow(t, 1, 0, 1, 2, map[string]any{"nested": []any{int16(2)}}, nil, nil)}
	if err := verifySealed(data, source, []int{0}); !errors.Is(err, ErrInvalidRow) {
		t.Fatalf("verifySealed on a row whose nested kind differs: %v, want ErrInvalidRow", err)
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
}

func TestEdge_OneRow(t *testing.T) {
	r := mkRow(t, 42, 3, 5, 5, map[string]any{"s": "only"}, &types.TemporalMetadata{ValidFrom: 10, TxFrom: 20}, nil)
	seg := mustOpen(t, mustEncode(t, []*types.Relationship{r}, Options{IntegrityBlockRows: 4096}, PageRows))
	got := scanAll(t, seg)
	if len(got) != 1 {
		t.Fatalf("%d rows", len(got))
	}
	assertSameRow(t, r, got[0])
	if err := seg.Verify(); err != nil {
		t.Fatal(err)
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

func TestEncode_RejectsInvalidInput(t *testing.T) {
	good := corpus(t, 3, 7)
	cases := map[string]struct {
		schema Schema
		rows   []*types.Relationship
		want   error
	}{
		"nil row":          {testSchema(), []*types.Relationship{good[0], nil}, ErrInvalidRow},
		"wrong type token": {testSchema(), []*types.Relationship{types.NewRelationship(1, 9, 1, 2)}, ErrInvalidRow},
		"duplicate id+version": {testSchema(), []*types.Relationship{
			mkRow(t, 1, 0, 1, 2, nil, nil, nil), mkRow(t, 1, 0, 3, 4, nil, nil, nil)}, ErrInvalidRow},
		"no stored hash":     {testSchema(), []*types.Relationship{noIntegrity(t)}, ErrHashMismatch},
		"stored hash wrong":  {testSchema(), []*types.Relationship{mkRow(t, 1, 0, 1, 2, nil, nil, &types.RelIntegrity{Hash: hexHash(rand.New(rand.NewPCG(1, 1)))})}, ErrHashMismatch}, // #nosec G404
		"stored hash no hex": {testSchema(), []*types.Relationship{mkRow(t, 1, 0, 1, 2, nil, nil, &types.RelIntegrity{Hash: "zz"})}, ErrHashMismatch},
		"empty type name":    {Schema{TypeToken: testToken}, good, ErrInvalidSchema},
		"token zero":         {Schema{TypeName: testType}, good, ErrInvalidSchema},
		"duplicate column":   {Schema{TypeName: testType, TypeToken: testToken, Columns: []Column{{"a", KindInt64}, {"a", KindString}}}, good, ErrInvalidSchema},
		"reserved column":    {Schema{TypeName: testType, TypeToken: testToken, Columns: []Column{{"tkg_x", KindInt64}}}, good, ErrInvalidSchema},
		"empty column name":  {Schema{TypeName: testType, TypeToken: testToken, Columns: []Column{{"", KindInt64}}}, good, ErrInvalidSchema},
		"unsupported kind":   {Schema{TypeName: testType, TypeToken: testToken, Columns: []Column{{"a", Kind(15)}}}, good, ErrInvalidSchema},
		"kind zero":          {Schema{TypeName: testType, TypeToken: testToken, Columns: []Column{{"a", 0}}}, good, ErrInvalidSchema},
		"too many columns":   {Schema{TypeName: testType, TypeToken: testToken, Columns: manyColumns(MaxColumns + 1)}, good, ErrInvalidSchema},
	}
	for name, tc := range cases {
		if _, err := Encode(tc.schema, tc.rows, Options{}); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v, want %v", name, err, tc.want)
		}
	}
}

func manyColumns(n int) []Column {
	cols := make([]Column, n)
	for i := range cols {
		cols[i] = Column{Name: fmt.Sprintf("c%d", i), Kind: KindInt64}
	}
	return cols
}

func noIntegrity(tb testing.TB) *types.Relationship {
	tb.Helper()
	return types.NewRelationship(1, testToken, 1, 2)
}

func TestLookupAndAdjacency(t *testing.T) {
	rows := corpus(t, 3000, 8)
	seg := mustOpen(t, mustEncode(t, rows, Options{}, 128))
	got := scanAll(t, seg)
	// Lookup: every ID finds exactly its versions, and nothing else.
	wantRows := map[types.RelID][]int{}
	for i, r := range got {
		wantRows[r.ID()] = append(wantRows[r.ID()], i)
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
	for _, phantom := range []types.RelID{0, 1, got[0].ID() - 1, got[len(got)-1].ID() + 1, math.MaxInt64} {
		if _, ok := wantRows[phantom]; ok {
			continue
		}
		if ix, err := seg.Lookup(phantom); err != nil || len(ix) != 0 {
			t.Fatalf("Lookup(phantom %d) = %v, %v", phantom, ix, err)
		}
	}
	// Adjacency: out = a contiguous run, in = exactly the rows ending there.
	outs, ins := map[types.NodeID][]int{}, map[types.NodeID][]int{}
	for i, r := range got {
		outs[r.StartNodeID()] = append(outs[r.StartNodeID()], i)
		ins[r.EndNodeID()] = append(ins[r.EndNodeID()], i)
	}
	nodes := map[types.NodeID]struct{}{0: {}, math.MaxInt64: {}}
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
		if !reflect.DeepEqual(run, outs[n]) {
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
	if _, err := Open(append(append([]byte(nil), data...), 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("one trailing byte accepted: %v", err)
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
			_ = seg.Scan(func(int, *types.Relationship) bool { return true })
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

// TestRoot_IsSHA256OverGroupRootsOfStoredHashes recomputes the witness
// independently of the writer: group roots over the stored hashes in segment
// order, the segment root over the group roots.
func TestRoot_IsSHA256OverGroupRootsOfStoredHashes(t *testing.T) {
	rows := corpus(t, 300, 13)
	want := byKey(rows)
	seg := mustOpen(t, mustEncode(t, rows, Options{IntegrityBlockRows: 16}, PageRows))
	var roots, leaf []byte
	for i, g := range scanAll(t, seg) {
		h, err := hex.DecodeString(want[rowKey{g.ID(), g.Version()}].Integrity().Hash)
		if err != nil {
			t.Fatal(err)
		}
		leaf = append(leaf, h...)
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
	if err := seg.Scan(func(int, *types.Relationship) bool { n++; return n < 5 }); err != nil {
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
