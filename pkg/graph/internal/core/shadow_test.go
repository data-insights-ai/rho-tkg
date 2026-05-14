package core

import (
	"context"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// helper: create a graph with a node that has temporal + integrity metadata.
func makeNodeWithMeta(t *testing.T) (*Core, *types.Node) {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	n, err := g.Nodes.Add(context.Background(), []string{"Person", "Actor"}, map[string]any{"name": "Alice"})
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
	tm.SetBaseEntityID(types.EntityID(42))
	n.SetTemporal(tm)
	n.SetIntegrity(&types.NodeIntegrity{Hash: "abc123", PrevHash: "def456"})
	n.SetVersion(3)
	return g, n
}

// helper: create a graph with a relationship that has temporal + integrity metadata.
func makeRelWithMeta(t *testing.T) (*Core, *types.Relationship) {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, err := g.Rels.Add(context.Background(), "KNOWS", nA, nB, map[string]any{"weight": 1.5})
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
	tm.SetBaseEntityID(types.EntityID(99))
	r.SetTemporal(tm)
	r.SetIntegrity(&types.RelIntegrity{Hash: "xyz789", PrevHash: "uvw012"})
	r.SetVersion(7)
	return g, r
}

func TestResolveHelpersNilInputsReturnZeroValues(t *testing.T) {
	t.Parallel()

	g := newTestGraph(t)
	if _, err := g.Resolve.GetOrCreateLabel("Person"); err != nil {
		t.Fatalf("GetOrCreateLabel: %v", err)
	}
	if _, err := g.Resolve.GetOrCreateRelType("KNOWS"); err != nil {
		t.Fatalf("GetOrCreateRelType: %v", err)
	}

	if labels := g.Nodes.Labels(nil); labels != nil {
		t.Fatalf("Nodes.Labels(nil) = %v, want nil", labels)
	}
	if got := g.Nodes.PrimaryLabel(nil); got != "" {
		t.Fatalf("Nodes.PrimaryLabel(nil) = %q, want empty string", got)
	}
	if g.Nodes.HasLabel(nil, "Person") {
		t.Fatal("Nodes.HasLabel(nil, registered label) = true, want false")
	}
	if got := g.Rels.Type(nil); got != "" {
		t.Fatalf("Rels.Type(nil) = %q, want empty string", got)
	}
	if g.Rels.HasType(nil, "KNOWS") {
		t.Fatal("Rels.HasType(nil, registered type) = true, want false")
	}

	for _, key := range []string{"name", types.ShadowLabels, types.ShadowCreatedAt} {
		if val, ok := g.Resolve.NodeProperty(nil, key); ok || val != nil {
			t.Fatalf("Resolve.NodeProperty(nil, %q) = (%v, %v), want (nil, false)", key, val, ok)
		}
	}
	for _, key := range []string{"weight", types.ShadowType, types.ShadowCreatedAt} {
		if val, ok := g.Resolve.RelProperty(nil, key); ok || val != nil {
			t.Fatalf("Resolve.RelProperty(nil, %q) = (%v, %v), want (nil, false)", key, val, ok)
		}
	}
}

// ─── Node shadow resolution ─────────────────────────────────────────────────

func TestResolveNodePropertyUserKey(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)
	val, ok := g.Resolve.NodeProperty(n, "name")
	if !ok || val != "Alice" {
		t.Errorf("ResolveNodeProperty(\"name\") = (%v, %v), want (\"Alice\", true)", val, ok)
	}

	// Missing user key.
	_, ok = g.Resolve.NodeProperty(n, "missing")
	if ok {
		t.Error("ResolveNodeProperty(\"missing\") should return false")
	}
}

func TestResolveNodePropertyLabels(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)
	val, ok := g.Resolve.NodeProperty(n, types.ShadowLabels)
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
	val, ok := g.Resolve.NodeProperty(n, types.ShadowType)
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
		val, ok := g.Resolve.NodeProperty(n, tc.key)
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

	val, ok := g.Resolve.NodeProperty(n, types.ShadowCreatedBy)
	if !ok || val != "alice" {
		t.Errorf("tkg_created_by = (%v, %v), want (\"alice\", true)", val, ok)
	}

	val, ok = g.Resolve.NodeProperty(n, types.ShadowUpdatedBy)
	if !ok || val != "bob" {
		t.Errorf("tkg_updated_by = (%v, %v), want (\"bob\", true)", val, ok)
	}

	val, ok = g.Resolve.NodeProperty(n, types.ShadowVersion)
	if !ok || val != uint32(3) {
		t.Errorf("tkg_version = (%v, %v), want (3, true)", val, ok)
	}
}

func TestResolveNodePropertyIntegrity(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)

	val, ok := g.Resolve.NodeProperty(n, types.ShadowHash)
	if !ok || val != "abc123" {
		t.Errorf("tkg_hash = (%v, %v), want (\"abc123\", true)", val, ok)
	}

	val, ok = g.Resolve.NodeProperty(n, types.ShadowPrevHash)
	if !ok || val != "def456" {
		t.Errorf("tkg_prev_hash = (%v, %v), want (\"def456\", true)", val, ok)
	}
}

func TestResolveNodePropertySignatureReturnsIndependentBytes(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)
	n.SetIntegrity(&types.NodeIntegrity{Signature: []byte{1, 2, 3}})

	val, ok := g.Resolve.NodeProperty(n, types.ShadowSignature)
	if !ok {
		t.Fatal("tkg_signature should return true")
	}
	sig, ok := val.([]byte)
	if !ok {
		t.Fatalf("tkg_signature should be []byte, got %T", val)
	}
	sig[0] = 99
	if got := n.Integrity().Signature[0]; got != 1 {
		t.Fatalf("mutating resolved signature changed node integrity signature to %d", got)
	}
}

func TestResolveNodePropertyBaseEntity(t *testing.T) {
	t.Parallel()

	g, n := makeNodeWithMeta(t)
	val, ok := g.Resolve.NodeProperty(n, types.ShadowBaseEntity)
	if !ok {
		t.Fatal("tkg_base_entity should return true")
	}
	if val != types.EntityID(42) {
		t.Errorf("tkg_base_entity = %v, want 42", val)
	}
}

func TestResolveNodePropertyNilTemporal(t *testing.T) {
	t.Parallel()

	g, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	// Construct node directly (bypassing AddNode) to simulate a legacy entity
	// loaded from disk without temporal metadata. AddNode now always sets TxFrom,
	// so the nil-temporal code path must be tested via direct construction.
	tok, _ := g.Resolve.GetOrCreateLabel("X")
	n := types.NewNode(g.Nodes.NextID(), tok, nil)
	// Temporal is nil — most temporal shadow keys should return (nil, false).
	// Exception: tkg_created_at derives from snowflake ID.

	nilKeys := []string{
		types.ShadowValidFrom, types.ShadowValidTo,
		types.ShadowTxFrom, types.ShadowTxTo,
		types.ShadowUpdatedAt, types.ShadowDeletedAt,
		types.ShadowCreatedBy, types.ShadowUpdatedBy,
		types.ShadowBaseEntity,
	}
	for _, key := range nilKeys {
		val, ok := g.Resolve.NodeProperty(n, key)
		if ok || val != nil {
			t.Errorf("ResolveNodeProperty(%q) with nil temporal: got (%v, %v), want (nil, false)", key, val, ok)
		}
	}

	// tkg_created_at should derive from snowflake ID even without temporal metadata.
	val, ok := g.Resolve.NodeProperty(n, types.ShadowCreatedAt)
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
	tok, _ := g.Resolve.GetOrCreateLabel("X")
	n := types.NewNode(g.Nodes.NextID(), tok, nil)

	for _, key := range []string{types.ShadowHash, types.ShadowPrevHash} {
		val, ok := g.Resolve.NodeProperty(n, key)
		if ok || val != nil {
			t.Errorf("ResolveNodeProperty(%q) with nil integrity: got (%v, %v), want (nil, false)", key, val, ok)
		}
	}
}

// ─── Relationship shadow resolution ─────────────────────────────────────────

func TestResolveRelPropertyType(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	val, ok := g.Resolve.RelProperty(r, types.ShadowType)
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
	val, ok := g.Resolve.RelProperty(r, types.ShadowLabels)
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
		val, ok := g.Resolve.RelProperty(r, tc.key)
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

	val, ok := g.Resolve.RelProperty(r, types.ShadowCreatedBy)
	if !ok || val != "alice" {
		t.Errorf("tkg_created_by = (%v, %v), want (\"alice\", true)", val, ok)
	}

	val, ok = g.Resolve.RelProperty(r, types.ShadowUpdatedBy)
	if !ok || val != "bob" {
		t.Errorf("tkg_updated_by = (%v, %v), want (\"bob\", true)", val, ok)
	}

	val, ok = g.Resolve.RelProperty(r, types.ShadowVersion)
	if !ok || val != uint32(7) {
		t.Errorf("tkg_version = (%v, %v), want (7, true)", val, ok)
	}
}

func TestResolveRelPropertyIntegrity(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)

	val, ok := g.Resolve.RelProperty(r, types.ShadowHash)
	if !ok || val != "xyz789" {
		t.Errorf("tkg_hash = (%v, %v), want (\"xyz789\", true)", val, ok)
	}

	val, ok = g.Resolve.RelProperty(r, types.ShadowPrevHash)
	if !ok || val != "uvw012" {
		t.Errorf("tkg_prev_hash = (%v, %v), want (\"uvw012\", true)", val, ok)
	}
}

func TestResolveRelPropertySignatureReturnsIndependentBytes(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	r.SetIntegrity(&types.RelIntegrity{Signature: []byte{4, 5, 6}})

	val, ok := g.Resolve.RelProperty(r, types.ShadowSignature)
	if !ok {
		t.Fatal("tkg_signature should return true")
	}
	sig, ok := val.([]byte)
	if !ok {
		t.Fatalf("tkg_signature should be []byte, got %T", val)
	}
	sig[0] = 99
	if got := r.Integrity().Signature[0]; got != 4 {
		t.Fatalf("mutating resolved signature changed relationship integrity signature to %d", got)
	}
}

func TestResolveRelPropertyBaseEntity(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	val, ok := g.Resolve.RelProperty(r, types.ShadowBaseEntity)
	if !ok {
		t.Fatal("tkg_base_entity on rel should return true")
	}
	if val != types.EntityID(99) {
		t.Errorf("tkg_base_entity = %v, want 99", val)
	}
}

func TestResolveRelPropertyUserKey(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	val, ok := g.Resolve.RelProperty(r, "weight")
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
	// Construct relationship directly (bypassing AddRelationship) to simulate
	// a legacy entity loaded from disk without temporal metadata. AddRelationship
	// now always sets TxFrom, so the nil-temporal code path must be tested via
	// direct construction.
	tok, _ := g.Resolve.GetOrCreateRelType("R")
	r := types.NewRelationship(g.Rels.NextID(), tok, g.Nodes.NextID(), g.Nodes.NextID())

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
		val, ok := g.Resolve.RelProperty(r, key)
		if ok || val != nil {
			t.Errorf("ResolveRelProperty(%q) with nil temporal: got (%v, %v), want (nil, false)", key, val, ok)
		}
	}

	// tkg_created_at should derive from snowflake ID even without temporal metadata.
	val, ok := g.Resolve.RelProperty(r, types.ShadowCreatedAt)
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
	tok, _ := g.Resolve.GetOrCreateRelType("R")
	r := types.NewRelationship(g.Rels.NextID(), tok, g.Nodes.NextID(), g.Nodes.NextID())

	for _, key := range []string{types.ShadowHash, types.ShadowPrevHash} {
		val, ok := g.Resolve.RelProperty(r, key)
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
	val, ok := g.Resolve.NodeProperty(n, types.ShadowCreatedAt)
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
	n, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	n.SetTemporal(&types.TemporalMetadata{CreatedAt: 0, CreatedBy: "test"})

	val, ok := g.Resolve.NodeProperty(n, types.ShadowCreatedAt)
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
	val, ok := g.Resolve.RelProperty(r, types.ShadowCreatedAt)
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
	nA, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nB, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	r, _ := g.Rels.Add(context.Background(), "R", nA, nB, nil)
	r.SetTemporal(&types.TemporalMetadata{CreatedAt: 0, CreatedBy: "test"})

	val, ok := g.Resolve.RelProperty(r, types.ShadowCreatedAt)
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
	val, ok := g.Resolve.NodeProperty(n, "tkg_unknown_key")
	if ok || val != nil {
		t.Errorf("unknown tkg_ key: got (%v, %v), want (nil, false)", val, ok)
	}
}

func TestResolveRelPropertyUnknownShadow(t *testing.T) {
	t.Parallel()

	g, r := makeRelWithMeta(t)
	val, ok := g.Resolve.RelProperty(r, "tkg_unknown_key")
	if ok || val != nil {
		t.Errorf("unknown tkg_ key: got (%v, %v), want (nil, false)", val, ok)
	}
}
