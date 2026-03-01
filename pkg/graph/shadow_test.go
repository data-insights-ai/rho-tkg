package graph

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// helper: create a graph with a node that has temporal + integrity metadata.
func makeNodeWithMeta(t *testing.T) (*Graph, *types.Node) {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	n, err := g.AddNode([]string{"Person", "Actor"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	tm := &types.TemporalMetadata{
		ValidFrom: 1000,
		ValidTo:   2000,
		TxFrom:    3000,
		TxTo:      4000,
		CreatedAt: 5000,
		UpdatedAt: 6000,
		DeletedAt: 7000,
		CreatedBy: "alice",
		UpdatedBy: "bob",
	}
	tm.SetBaseEntityID(snowflake.ID(42))
	n.SetTemporal(tm)
	n.SetIntegrity(&types.NodeIntegrity{Hash: "abc123", PrevHash: "def456"})
	n.SetVersion(3)
	return g, n
}

// helper: create a graph with a relationship that has temporal + integrity metadata.
func makeRelWithMeta(t *testing.T) (*Graph, *types.Relationship) {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, err := g.AddRelationship("KNOWS", nA, nB, map[string]any{"weight": 1.5})
	if err != nil {
		t.Fatal(err)
	}
	tm := &types.TemporalMetadata{
		ValidFrom: 1000,
		ValidTo:   2000,
		TxFrom:    3000,
		TxTo:      4000,
		CreatedAt: 5000,
		UpdatedAt: 6000,
		DeletedAt: 7000,
		CreatedBy: "alice",
		UpdatedBy: "bob",
	}
	tm.SetBaseEntityID(snowflake.ID(99))
	r.SetTemporal(tm)
	r.SetIntegrity(&types.RelIntegrity{Hash: "xyz789", PrevHash: "uvw012"})
	r.SetVersion(7)
	return g, r
}

// ─── Node shadow resolution ─────────────────────────────────────────────────

func TestResolveNodePropertyUserKey(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)
	val, ok := g.ResolveNodeProperty(n, "name")
	if !ok || val != "Alice" {
		t.Errorf("ResolveNodeProperty(\"name\") = (%v, %v), want (\"Alice\", true)", val, ok)
	}

	// Missing user key.
	_, ok = g.ResolveNodeProperty(n, "missing")
	if ok {
		t.Error("ResolveNodeProperty(\"missing\") should return false")
	}
}

func TestResolveNodePropertyLabels(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)
	val, ok := g.ResolveNodeProperty(n, types.ShadowLabels)
	if !ok {
		t.Fatal("ResolveNodeProperty(tkg_labels) should return true")
	}
	labels, isSlice := val.([]string)
	if !isSlice {
		t.Fatalf("tkg_labels should be []string, got %T", val)
	}
	if len(labels) != 2 || labels[0] != "Person" || labels[1] != "Actor" {
		t.Errorf("tkg_labels = %v, want [Person Actor]", labels)
	}
}

func TestResolveNodePropertyType(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)
	val, ok := g.ResolveNodeProperty(n, types.ShadowType)
	if ok || val != nil {
		t.Errorf("tkg_type on node: got (%v, %v), want (nil, false)", val, ok)
	}
}

func TestResolveNodePropertyTemporal(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)

	cases := []struct {
		key  string
		want any
	}{
		{types.ShadowValidFrom, types.Instant(1000)},
		{types.ShadowValidTo, types.Instant(2000)},
		{types.ShadowTxFrom, types.Instant(3000)},
		{types.ShadowTxTo, types.Instant(4000)},
		{types.ShadowCreatedAt, types.Instant(5000)},
		{types.ShadowUpdatedAt, types.Instant(6000)},
		{types.ShadowDeletedAt, types.Instant(7000)},
	}
	for _, tc := range cases {
		val, ok := g.ResolveNodeProperty(n, tc.key)
		if !ok {
			t.Errorf("ResolveNodeProperty(%q) returned false", tc.key)
			continue
		}
		if val != tc.want {
			t.Errorf("ResolveNodeProperty(%q) = %v, want %v", tc.key, val, tc.want)
		}
	}
}

func TestResolveNodePropertyProvenance(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)

	val, ok := g.ResolveNodeProperty(n, types.ShadowCreatedBy)
	if !ok || val != "alice" {
		t.Errorf("tkg_created_by = (%v, %v), want (\"alice\", true)", val, ok)
	}

	val, ok = g.ResolveNodeProperty(n, types.ShadowUpdatedBy)
	if !ok || val != "bob" {
		t.Errorf("tkg_updated_by = (%v, %v), want (\"bob\", true)", val, ok)
	}

	val, ok = g.ResolveNodeProperty(n, types.ShadowVersion)
	if !ok || val != uint32(3) {
		t.Errorf("tkg_version = (%v, %v), want (3, true)", val, ok)
	}
}

func TestResolveNodePropertyIntegrity(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)

	val, ok := g.ResolveNodeProperty(n, types.ShadowHash)
	if !ok || val != "abc123" {
		t.Errorf("tkg_hash = (%v, %v), want (\"abc123\", true)", val, ok)
	}

	val, ok = g.ResolveNodeProperty(n, types.ShadowPrevHash)
	if !ok || val != "def456" {
		t.Errorf("tkg_prev_hash = (%v, %v), want (\"def456\", true)", val, ok)
	}
}

func TestResolveNodePropertyBaseEntity(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)
	val, ok := g.ResolveNodeProperty(n, types.ShadowBaseEntity)
	if !ok {
		t.Fatal("tkg_base_entity should return true")
	}
	if val != snowflake.ID(42) {
		t.Errorf("tkg_base_entity = %v, want 42", val)
	}
}

func TestResolveNodePropertyNilTemporal(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	n, _ := g.AddNode([]string{"X"}, nil)
	// No Temporal set — most temporal shadow keys should return (nil, false).
	// Exception: tkg_created_at derives from snowflake ID.

	nilKeys := []string{
		types.ShadowValidFrom, types.ShadowValidTo,
		types.ShadowTxFrom, types.ShadowTxTo,
		types.ShadowUpdatedAt, types.ShadowDeletedAt,
		types.ShadowCreatedBy, types.ShadowUpdatedBy,
		types.ShadowBaseEntity,
	}
	for _, key := range nilKeys {
		val, ok := g.ResolveNodeProperty(n, key)
		if ok || val != nil {
			t.Errorf("ResolveNodeProperty(%q) with nil temporal: got (%v, %v), want (nil, false)", key, val, ok)
		}
	}

	// tkg_created_at should derive from snowflake ID even without temporal metadata.
	val, ok := g.ResolveNodeProperty(n, types.ShadowCreatedAt)
	if !ok {
		t.Fatal("tkg_created_at should return true even without temporal metadata")
	}
	derived, isInstant := val.(types.Instant)
	if !isInstant {
		t.Fatalf("tkg_created_at should be Instant, got %T", val)
	}
	if derived <= 0 {
		t.Errorf("derived tkg_created_at = %d, want positive Unix ms", derived)
	}
}

func TestResolveNodePropertyNilIntegrity(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	// Construct a node directly (bypassing AddNode) to simulate a legacy
	// entity loaded from disk without integrity metadata.
	tok, _ := g.GetOrCreateLabel("X")
	n := types.NewNode(g.NextNodeID(), tok, nil)

	for _, key := range []string{types.ShadowHash, types.ShadowPrevHash} {
		val, ok := g.ResolveNodeProperty(n, key)
		if ok || val != nil {
			t.Errorf("ResolveNodeProperty(%q) with nil integrity: got (%v, %v), want (nil, false)", key, val, ok)
		}
	}
}

// ─── Relationship shadow resolution ─────────────────────────────────────────

func TestResolveRelPropertyType(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	val, ok := g.ResolveRelProperty(r, types.ShadowType)
	if !ok {
		t.Fatal("tkg_type on rel should return true")
	}
	if val != "KNOWS" {
		t.Errorf("tkg_type = %v, want \"KNOWS\"", val)
	}
}

func TestResolveRelPropertyLabels(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	val, ok := g.ResolveRelProperty(r, types.ShadowLabels)
	if ok || val != nil {
		t.Errorf("tkg_labels on rel: got (%v, %v), want (nil, false)", val, ok)
	}
}

func TestResolveRelPropertyTemporal(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)

	cases := []struct {
		key  string
		want any
	}{
		{types.ShadowValidFrom, types.Instant(1000)},
		{types.ShadowValidTo, types.Instant(2000)},
		{types.ShadowTxFrom, types.Instant(3000)},
		{types.ShadowTxTo, types.Instant(4000)},
		{types.ShadowCreatedAt, types.Instant(5000)},
		{types.ShadowUpdatedAt, types.Instant(6000)},
		{types.ShadowDeletedAt, types.Instant(7000)},
	}
	for _, tc := range cases {
		val, ok := g.ResolveRelProperty(r, tc.key)
		if !ok {
			t.Errorf("ResolveRelProperty(%q) returned false", tc.key)
			continue
		}
		if val != tc.want {
			t.Errorf("ResolveRelProperty(%q) = %v, want %v", tc.key, val, tc.want)
		}
	}
}

func TestResolveRelPropertyProvenance(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)

	val, ok := g.ResolveRelProperty(r, types.ShadowCreatedBy)
	if !ok || val != "alice" {
		t.Errorf("tkg_created_by = (%v, %v), want (\"alice\", true)", val, ok)
	}

	val, ok = g.ResolveRelProperty(r, types.ShadowUpdatedBy)
	if !ok || val != "bob" {
		t.Errorf("tkg_updated_by = (%v, %v), want (\"bob\", true)", val, ok)
	}

	val, ok = g.ResolveRelProperty(r, types.ShadowVersion)
	if !ok || val != uint32(7) {
		t.Errorf("tkg_version = (%v, %v), want (7, true)", val, ok)
	}
}

func TestResolveRelPropertyIntegrity(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)

	val, ok := g.ResolveRelProperty(r, types.ShadowHash)
	if !ok || val != "xyz789" {
		t.Errorf("tkg_hash = (%v, %v), want (\"xyz789\", true)", val, ok)
	}

	val, ok = g.ResolveRelProperty(r, types.ShadowPrevHash)
	if !ok || val != "uvw012" {
		t.Errorf("tkg_prev_hash = (%v, %v), want (\"uvw012\", true)", val, ok)
	}
}

func TestResolveRelPropertyBaseEntity(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	val, ok := g.ResolveRelProperty(r, types.ShadowBaseEntity)
	if !ok {
		t.Fatal("tkg_base_entity on rel should return true")
	}
	if val != snowflake.ID(99) {
		t.Errorf("tkg_base_entity = %v, want 99", val)
	}
}

func TestResolveRelPropertyUserKey(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	val, ok := g.ResolveRelProperty(r, "weight")
	if !ok || val != 1.5 {
		t.Errorf("ResolveRelProperty(\"weight\") = (%v, %v), want (1.5, true)", val, ok)
	}
}

func TestResolveRelPropertyNilTemporal(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("R", nA, nB, nil)

	// Most temporal keys return (nil, false) without temporal metadata.
	// Exception: tkg_created_at derives from snowflake ID.
	nilKeys := []string{
		types.ShadowValidFrom, types.ShadowValidTo,
		types.ShadowTxFrom, types.ShadowTxTo,
		types.ShadowUpdatedAt, types.ShadowDeletedAt,
		types.ShadowCreatedBy, types.ShadowUpdatedBy,
		types.ShadowBaseEntity,
	}
	for _, key := range nilKeys {
		val, ok := g.ResolveRelProperty(r, key)
		if ok || val != nil {
			t.Errorf("ResolveRelProperty(%q) with nil temporal: got (%v, %v), want (nil, false)", key, val, ok)
		}
	}

	// tkg_created_at should derive from snowflake ID even without temporal metadata.
	val, ok := g.ResolveRelProperty(r, types.ShadowCreatedAt)
	if !ok {
		t.Fatal("tkg_created_at on rel should return true even without temporal metadata")
	}
	derived, isInstant := val.(types.Instant)
	if !isInstant {
		t.Fatalf("tkg_created_at should be Instant, got %T", val)
	}
	if derived <= 0 {
		t.Errorf("derived tkg_created_at = %d, want positive Unix ms", derived)
	}
}

func TestResolveRelPropertyNilIntegrity(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	// Construct a relationship directly (bypassing AddRelationship) to simulate
	// a legacy entity loaded from disk without integrity metadata.
	tok, _ := g.GetOrCreateRelType("R")
	r := types.NewRelationship(g.NextRelID(), tok, g.NextNodeID(), g.NextNodeID())

	for _, key := range []string{types.ShadowHash, types.ShadowPrevHash} {
		val, ok := g.ResolveRelProperty(r, key)
		if ok || val != nil {
			t.Errorf("ResolveRelProperty(%q) with nil integrity: got (%v, %v), want (nil, false)", key, val, ok)
		}
	}
}

// ─── CreatedAt derivation from snowflake ID ─────────────────────────────────

func TestResolveNodeCreatedAtExplicitPriority(t *testing.T) {
	t.Parallel()

	// When temporal metadata has an explicit CreatedAt, it takes priority
	// over the snowflake-derived timestamp.
	g, n := makeNodeWithMeta(t)
	val, ok := g.ResolveNodeProperty(n, types.ShadowCreatedAt)
	if !ok {
		t.Fatal("tkg_created_at should return true")
	}
	if val != types.Instant(5000) {
		t.Errorf("tkg_created_at = %v, want 5000 (explicit)", val)
	}
}

func TestResolveNodeCreatedAtZeroFallback(t *testing.T) {
	t.Parallel()

	// When temporal metadata exists but CreatedAt is zero, derive from ID.
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	n, _ := g.AddNode([]string{"X"}, nil)
	n.SetTemporal(&types.TemporalMetadata{CreatedAt: 0, CreatedBy: "test"})

	val, ok := g.ResolveNodeProperty(n, types.ShadowCreatedAt)
	if !ok {
		t.Fatal("tkg_created_at should return true even with zero CreatedAt")
	}
	derived, isInstant := val.(types.Instant)
	if !isInstant {
		t.Fatalf("tkg_created_at should be Instant, got %T", val)
	}
	if derived <= 0 {
		t.Errorf("derived tkg_created_at = %d, want positive Unix ms", derived)
	}
}

func TestResolveRelCreatedAtExplicitPriority(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	val, ok := g.ResolveRelProperty(r, types.ShadowCreatedAt)
	if !ok {
		t.Fatal("tkg_created_at should return true")
	}
	if val != types.Instant(5000) {
		t.Errorf("tkg_created_at = %v, want 5000 (explicit)", val)
	}
}

func TestResolveRelCreatedAtZeroFallback(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	nA, _ := g.AddNode([]string{"X"}, nil)
	nB, _ := g.AddNode([]string{"X"}, nil)
	r, _ := g.AddRelationship("R", nA, nB, nil)
	r.SetTemporal(&types.TemporalMetadata{CreatedAt: 0, CreatedBy: "test"})

	val, ok := g.ResolveRelProperty(r, types.ShadowCreatedAt)
	if !ok {
		t.Fatal("tkg_created_at should return true even with zero CreatedAt")
	}
	derived, isInstant := val.(types.Instant)
	if !isInstant {
		t.Fatalf("tkg_created_at should be Instant, got %T", val)
	}
	if derived <= 0 {
		t.Errorf("derived tkg_created_at = %d, want positive Unix ms", derived)
	}
}

func TestResolveNodePropertyUnknownShadow(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)
	val, ok := g.ResolveNodeProperty(n, "tkg_unknown_key")
	if ok || val != nil {
		t.Errorf("unknown tkg_ key: got (%v, %v), want (nil, false)", val, ok)
	}
}

func TestResolveRelPropertyUnknownShadow(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	val, ok := g.ResolveRelProperty(r, "tkg_unknown_key")
	if ok || val != nil {
		t.Errorf("unknown tkg_ key: got (%v, %v), want (nil, false)", val, ok)
	}
}
