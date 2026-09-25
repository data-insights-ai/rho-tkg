package types

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

const (
	testHashA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testHashB = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	testHashC = "00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff00ff"
)

// compactRelCase is one relationship shape and whether the compact form must
// hold it (a first version with a canonical hash) or leave it in the ordinary
// frozen form.
type compactRelCase struct {
	name    string
	compact bool
	tm      *TemporalMetadata
	ig      *RelIntegrity
}

func firstVersionTM() *TemporalMetadata {
	return &TemporalMetadata{ValidFrom: 1_787_443_200_000, ValidTo: 1_787_443_200_900, TxFrom: 1_787_443_230_000}
}

func compactRelCases() []compactRelCase {
	base := func() *RelIntegrity {
		return &RelIntegrity{Hash: testHashA, FromNodeHash: testHashB, ToNodeHash: testHashC}
	}
	withTM := func(f func(*TemporalMetadata)) *TemporalMetadata {
		tm := firstVersionTM()
		f(tm)
		return tm
	}
	withIG := func(f func(*RelIntegrity)) *RelIntegrity {
		ig := base()
		f(ig)
		return ig
	}
	based := withTM(func(tm *TemporalMetadata) {})
	based.SetBaseEntityID(7)
	return []compactRelCase{
		{name: "first version", compact: true, tm: firstVersionTM(), ig: base()},
		{name: "zero temporal", compact: true, tm: &TemporalMetadata{}, ig: base()},
		{name: "self loop shares one endpoint hash", compact: true, tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.ToNodeHash = ig.FromNodeHash })},
		{name: "no endpoint hashes", compact: true, tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.FromNodeHash, ig.ToNodeHash = "", "" })},
		{name: "tx to (history version)", tm: withTM(func(tm *TemporalMetadata) { tm.TxTo = 5 }), ig: base()},
		{name: "created at", tm: withTM(func(tm *TemporalMetadata) { tm.CreatedAt = 5 }), ig: base()},
		{name: "updated at", tm: withTM(func(tm *TemporalMetadata) { tm.UpdatedAt = 5 }), ig: base()},
		{name: "deleted at", tm: withTM(func(tm *TemporalMetadata) { tm.DeletedAt = 5 }), ig: base()},
		{name: "created by", tm: withTM(func(tm *TemporalMetadata) { tm.CreatedBy = "u" }), ig: base()},
		{name: "updated by", tm: withTM(func(tm *TemporalMetadata) { tm.UpdatedBy = "u" }), ig: base()},
		{name: "base entity", tm: based, ig: base()},
		{name: "prev hash", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.PrevHash = testHashB })},
		{name: "author", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.AuthorID = "a" })},
		{name: "signature", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.Signature = []byte{1, 2} })},
		{name: "empty non-nil signature", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.Signature = []byte{} })},
		{name: "authorized by", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.AuthorizedBy = "b" })},
		{name: "authorization level", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.AuthorizationLevel = 2 })},
		{name: "uppercase hash", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.Hash = strings.ToUpper(testHashA) })},
		{name: "short hash", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.Hash = testHashA[:63] })},
		{name: "long hash", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.Hash = testHashA + "0" })},
		{name: "non-hex hash", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.Hash = "g" + testHashA[1:] })},
		{name: "empty hash", tm: firstVersionTM(), ig: withIG(func(ig *RelIntegrity) { ig.Hash = "" })},
		{name: "no temporal", ig: base()},
		{name: "no integrity", tm: firstVersionTM()},
		{name: "no metadata"},
	}
}

func buildCompactTestRel(t *testing.T, c compactRelCase) *Relationship {
	t.Helper()
	r := NewRelationship(RelID(11), 3, NodeID(21), NodeID(22))
	r.SetVersion(0)
	for k, v := range map[string]any{"actor": "corp\\u1", "obs": int64(4), "tags": []string{"x", "y"}, "m": map[string]any{"k": []any{int64(1)}}} {
		if err := r.SetProperty(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if c.tm != nil {
		tm := *c.tm
		r.SetTemporal(&tm)
	}
	if c.ig != nil {
		r.SetIntegrity(c.ig.DeepCopy())
	}
	return r
}

// assertRelViewsEqual checks every read accessor of got against want.
func assertRelViewsEqual(t *testing.T, got, want *Relationship) {
	t.Helper()
	if got.ID() != want.ID() || got.StartNodeID() != want.StartNodeID() || got.EndNodeID() != want.EndNodeID() ||
		got.TypeToken() != want.TypeToken() || got.Version() != want.Version() || got.IsFrozen() != want.IsFrozen() {
		t.Fatalf("identity fields differ: got %v/%v/%v/%v/%v/%v", got.ID(), got.StartNodeID(), got.EndNodeID(), got.TypeToken(), got.Version(), got.IsFrozen())
	}
	if !reflect.DeepEqual(got.Temporal(), want.Temporal()) {
		t.Fatalf("Temporal() = %+v, want %+v", got.Temporal(), want.Temporal())
	}
	gf, gt, gok := got.ValidRange()
	wf, wt, wok := want.ValidRange()
	if gf != wf || gt != wt || gok != wok {
		t.Fatalf("ValidRange() = %v %v %v, want %v %v %v", gf, gt, gok, wf, wt, wok)
	}
	if !reflect.DeepEqual(got.Integrity(), want.Integrity()) {
		t.Fatalf("Integrity() = %+v, want %+v", got.Integrity(), want.Integrity())
	}
	if !reflect.DeepEqual(got.Properties(), want.Properties()) || !reflect.DeepEqual(got.PropertiesMap(), want.PropertiesMap()) {
		t.Fatalf("properties differ: %v vs %v", got.Properties(), want.Properties())
	}
	if string(got.AppendPropertyHashBytes(nil)) != string(want.AppendPropertyHashBytes(nil)) {
		t.Fatal("property hash bytes differ")
	}
	// The thawed form is the ordinary representation, field for field.
	if !reflect.DeepEqual(got.DeepCopy(), want.DeepCopy()) {
		t.Fatalf("DeepCopy() = %+v, want %+v", got.DeepCopy(), want.DeepCopy())
	}
}

// TestRelationshipCompactFrozenCopyAnswersLikeAFrozenDeepCopy: for every
// metadata shape, the compact copy answers each accessor exactly like
// DeepCopy()+Freeze(), and it uses the compact form only for first versions
// with a canonical hash.
func TestRelationshipCompactFrozenCopyAnswersLikeAFrozenDeepCopy(t *testing.T) {
	for _, c := range compactRelCases() {
		t.Run(c.name, func(t *testing.T) {
			r := buildCompactTestRel(t, c)
			want := r.DeepCopy()
			want.Freeze()
			got := r.CompactFrozenCopy()
			if (got.meta != nil) != c.compact {
				t.Fatalf("compact form used = %t, want %t", got.meta != nil, c.compact)
			}
			if got.meta != nil && (got.temporal != nil || got.integrity != nil) {
				t.Fatal("compact row also carries the ordinary metadata objects")
			}
			assertRelViewsEqual(t, got, want)
			// Re-compacting a row (a store copying its own cached row) is stable.
			again := got.CompactFrozenCopy()
			if c.compact && again.meta != got.meta {
				t.Fatal("re-compacting rebuilt the immutable metadata")
			}
			assertRelViewsEqual(t, again, want)
			if got.ApproxHeapBytes() <= 0 || (c.compact && got.ApproxHeapBytes() >= want.ApproxHeapBytes()) {
				t.Fatalf("ApproxHeapBytes compact %d, frozen deep copy %d", got.ApproxHeapBytes(), want.ApproxHeapBytes())
			}
		})
	}
}

// TestRelationshipCompactFrozenCopyIsIndependent: writes through the source
// or through the copies the accessors hand out never reach the compact row.
func TestRelationshipCompactFrozenCopyIsIndependent(t *testing.T) {
	r := buildCompactTestRel(t, compactRelCases()[0])
	cp := r.CompactFrozenCopy()
	want := cp.DeepCopy()

	r.Temporal().ValidFrom = 99
	r.Integrity().FromNodeHash = "changed"
	if err := r.SetProperty("actor", "other"); err != nil {
		t.Fatal(err)
	}
	cp.Temporal().TxFrom = 99
	cp.Integrity().Hash = "changed"
	if tags, _ := cp.GetProperty("tags"); tags != nil {
		tags.([]string)[0] = "changed"
	}
	if !reflect.DeepEqual(cp.DeepCopy(), want) {
		t.Fatalf("compact row changed: %+v, want %+v", cp.DeepCopy(), want)
	}
}

// TestRelationshipCompactFrozenCopyRejectsMutation: a compact row is frozen.
func TestRelationshipCompactFrozenCopyRejectsMutation(t *testing.T) {
	cp := buildCompactTestRel(t, compactRelCases()[0]).CompactFrozenCopy()
	if err := cp.SetProperty("k", "v"); !errors.Is(err, ErrFrozenRelationship) {
		t.Fatalf("SetProperty on a compact row: %v, want ErrFrozenRelationship", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("SetTemporal on a compact row did not panic")
		}
	}()
	cp.SetTemporal(&TemporalMetadata{})
}

func TestRelationshipCompactFrozenCopyNil(t *testing.T) {
	var r *Relationship
	if r.CompactFrozenCopy() != nil {
		t.Fatal("nil relationship compacted to non-nil")
	}
}

type compactNodeCase struct {
	name    string
	compact bool
	tm      *TemporalMetadata
	ig      *NodeIntegrity
}

func compactNodeCases() []compactNodeCase {
	base := func() *NodeIntegrity { return &NodeIntegrity{Hash: testHashA} }
	withTM := func(f func(*TemporalMetadata)) *TemporalMetadata {
		tm := firstVersionTM()
		f(tm)
		return tm
	}
	withIG := func(f func(*NodeIntegrity)) *NodeIntegrity {
		ig := base()
		f(ig)
		return ig
	}
	based := firstVersionTM()
	based.SetBaseEntityID(7)
	return []compactNodeCase{
		{name: "first version", compact: true, tm: firstVersionTM(), ig: base()},
		{name: "non-hex hash string kept verbatim", compact: true, tm: firstVersionTM(), ig: withIG(func(ig *NodeIntegrity) { ig.Hash = "not-hex" })},
		{name: "zero temporal", compact: true, tm: &TemporalMetadata{}, ig: base()},
		{name: "tx to (history version)", tm: withTM(func(tm *TemporalMetadata) { tm.TxTo = 5 }), ig: base()},
		{name: "created at", tm: withTM(func(tm *TemporalMetadata) { tm.CreatedAt = 5 }), ig: base()},
		{name: "updated at", tm: withTM(func(tm *TemporalMetadata) { tm.UpdatedAt = 5 }), ig: base()},
		{name: "deleted at", tm: withTM(func(tm *TemporalMetadata) { tm.DeletedAt = 5 }), ig: base()},
		{name: "created by", tm: withTM(func(tm *TemporalMetadata) { tm.CreatedBy = "u" }), ig: base()},
		{name: "updated by", tm: withTM(func(tm *TemporalMetadata) { tm.UpdatedBy = "u" }), ig: base()},
		{name: "base entity", tm: based, ig: base()},
		{name: "prev hash", tm: firstVersionTM(), ig: withIG(func(ig *NodeIntegrity) { ig.PrevHash = testHashB })},
		{name: "author", tm: firstVersionTM(), ig: withIG(func(ig *NodeIntegrity) { ig.AuthorID = "a" })},
		{name: "signature", tm: firstVersionTM(), ig: withIG(func(ig *NodeIntegrity) { ig.Signature = []byte{1} })},
		{name: "empty non-nil signature", tm: firstVersionTM(), ig: withIG(func(ig *NodeIntegrity) { ig.Signature = []byte{} })},
		{name: "authorized by", tm: firstVersionTM(), ig: withIG(func(ig *NodeIntegrity) { ig.AuthorizedBy = "b" })},
		{name: "authorization level", tm: firstVersionTM(), ig: withIG(func(ig *NodeIntegrity) { ig.AuthorizationLevel = 1 })},
		{name: "empty hash", tm: firstVersionTM(), ig: withIG(func(ig *NodeIntegrity) { ig.Hash = "" })},
		{name: "no temporal", ig: base()},
		{name: "no integrity", tm: firstVersionTM()},
		{name: "no metadata"},
	}
}

func buildCompactTestNode(t *testing.T, c compactNodeCase) *Node {
	t.Helper()
	n := NewNode(NodeID(31), 1, []uint16{2, 3})
	for k, v := range map[string]any{"name": "10.0.0.1", "tags": []string{"x"}} {
		if err := n.SetProperty(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if c.tm != nil {
		tm := *c.tm
		n.SetTemporal(&tm)
	}
	if c.ig != nil {
		n.SetIntegrity(c.ig.DeepCopy())
	}
	return n
}

func assertNodeViewsEqual(t *testing.T, got, want *Node) {
	t.Helper()
	if got.ID() != want.ID() || got.PrimaryLabelToken() != want.PrimaryLabelToken() || got.Version() != want.Version() || got.IsFrozen() != want.IsFrozen() {
		t.Fatalf("identity fields differ")
	}
	if !reflect.DeepEqual(got.AllLabelTokens(), want.AllLabelTokens()) {
		t.Fatalf("labels %v, want %v", got.AllLabelTokens(), want.AllLabelTokens())
	}
	if !reflect.DeepEqual(got.Temporal(), want.Temporal()) {
		t.Fatalf("Temporal() = %+v, want %+v", got.Temporal(), want.Temporal())
	}
	gf, gt, gok := got.ValidRange()
	wf, wt, wok := want.ValidRange()
	if gf != wf || gt != wt || gok != wok {
		t.Fatalf("ValidRange() = %v %v %v, want %v %v %v", gf, gt, gok, wf, wt, wok)
	}
	if !reflect.DeepEqual(got.Integrity(), want.Integrity()) {
		t.Fatalf("Integrity() = %+v, want %+v", got.Integrity(), want.Integrity())
	}
	if !reflect.DeepEqual(got.Properties(), want.Properties()) {
		t.Fatalf("properties differ")
	}
	if !reflect.DeepEqual(got.DeepCopy(), want.DeepCopy()) {
		t.Fatalf("DeepCopy() = %+v, want %+v", got.DeepCopy(), want.DeepCopy())
	}
}

// TestNodeCompactFrozenCopyAnswersLikeAFrozenDeepCopy is the node mirror
// (testing rule 2).
func TestNodeCompactFrozenCopyAnswersLikeAFrozenDeepCopy(t *testing.T) {
	for _, c := range compactNodeCases() {
		t.Run(c.name, func(t *testing.T) {
			n := buildCompactTestNode(t, c)
			want := n.DeepCopy()
			want.Freeze()
			got := n.CompactFrozenCopy()
			if (got.meta != nil) != c.compact {
				t.Fatalf("compact form used = %t, want %t", got.meta != nil, c.compact)
			}
			if got.meta != nil && (got.temporal != nil || got.integrity != nil) {
				t.Fatal("compact row also carries the ordinary metadata objects")
			}
			assertNodeViewsEqual(t, got, want)
			again := got.CompactFrozenCopy()
			if c.compact && again.meta != got.meta {
				t.Fatal("re-compacting rebuilt the immutable metadata")
			}
			assertNodeViewsEqual(t, again, want)
			if got.ApproxHeapBytes() <= 0 || (c.compact && got.ApproxHeapBytes() >= want.ApproxHeapBytes()) {
				t.Fatalf("ApproxHeapBytes compact %d, frozen deep copy %d", got.ApproxHeapBytes(), want.ApproxHeapBytes())
			}
		})
	}
}

func TestNodeCompactFrozenCopyIsIndependent(t *testing.T) {
	n := buildCompactTestNode(t, compactNodeCases()[0])
	cp := n.CompactFrozenCopy()
	want := cp.DeepCopy()
	n.Temporal().ValidFrom = 99
	n.Integrity().Hash = "changed"
	if !n.AddLabelTokenRaw(9) {
		t.Fatal("add label")
	}
	cp.Temporal().TxFrom = 99
	cp.Integrity().Hash = "changed"
	if !reflect.DeepEqual(cp.DeepCopy(), want) {
		t.Fatalf("compact node changed: %+v, want %+v", cp.DeepCopy(), want)
	}
}

func TestNodeCompactFrozenCopyRejectsMutation(t *testing.T) {
	cp := buildCompactTestNode(t, compactNodeCases()[0]).CompactFrozenCopy()
	if err := cp.SetProperty("k", "v"); !errors.Is(err, ErrFrozenNode) {
		t.Fatalf("SetProperty on a compact node: %v, want ErrFrozenNode", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("SetIntegrity on a compact node did not panic")
		}
	}()
	cp.SetIntegrity(&NodeIntegrity{})
}

func TestNodeCompactFrozenCopyNil(t *testing.T) {
	var n *Node
	if n.CompactFrozenCopy() != nil {
		t.Fatal("nil node compacted to non-nil")
	}
}

func TestDecodeCanonicalHash(t *testing.T) {
	for _, c := range []struct {
		in string
		ok bool
	}{
		{testHashA, true},
		{testHashC, true},
		{strings.Repeat("0", 64), true},
		{strings.Repeat("f", 64), true},
		{strings.ToUpper(testHashA), false},
		{testHashA[:63], false},
		{testHashA + "a", false},
		{"", false},
		{strings.Repeat("0", 63) + "g", false},
		{strings.Repeat("0", 63) + "/", false},
		{strings.Repeat("0", 63) + ":", false},
		{strings.Repeat("0", 63) + "`", false},
	} {
		var dst [32]byte
		if got := decodeCanonicalHash(c.in, &dst); got != c.ok {
			t.Fatalf("decodeCanonicalHash(%q) = %t, want %t", c.in, got, c.ok)
		}
		if c.ok {
			m := relMeta{hash: dst}
			if s := m.integrity().Hash; s != c.in {
				t.Fatalf("round trip %q -> %q", c.in, s)
			}
		}
	}
}
