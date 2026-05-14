package core

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestCAS_Match(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"status": "draft"})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "status", "draft", "published")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should return true on match")
	}

	got, _ := g.Nodes.Get(context.Background(), id)
	v, _ := got.GetProperty("status")
	if v != "published" {
		t.Fatalf("status = %v, want published", v)
	}
}

func TestCAS_Mismatch(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"status": "draft"})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "status", "archived", "published")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("CAS should return false on mismatch")
	}

	got, _ := g.Nodes.Get(context.Background(), id)
	v, _ := got.GetProperty("status")
	if v != "draft" {
		t.Fatalf("status = %v, want draft (unchanged)", v)
	}
}

func TestCAS_NilExpected_Absent(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "status", nil, "active")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should return true when expected=nil and property absent")
	}

	got, _ := g.Nodes.Get(context.Background(), id)
	v, found := got.GetProperty("status")
	if !found || v != "active" {
		t.Fatalf("status = (%v, %v), want (active, true)", v, found)
	}
}

func TestCAS_AddPropertyRejectsFinalPropertyCountOverLimit(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{
		Store:      memory.New(),
		Validation: ValidationLimits{MaxPropertiesPerEntity: 1},
	})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"a": 1})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "b", nil, 2)
	if err == nil {
		t.Fatal("expected property count limit error")
	}
	if ok {
		t.Fatal("CAS should not report success when final property count exceeds limit")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}

	got, getErr := g.Nodes.Get(context.Background(), id)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if got.PropertyCount() != 1 {
		t.Fatalf("property count = %d, want 1", got.PropertyCount())
	}
	if _, found := got.GetProperty("b"); found {
		t.Fatal("overflow property should not be persisted")
	}
}

func TestCAS_NilExpected_Present(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"status": "draft"})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "status", nil, "active")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("CAS should return false when expected=nil but property exists")
	}
}

func TestCAS_DeleteOnMatch(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"status": "draft"})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "status", "draft", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should return true on match+delete")
	}

	got, _ := g.Nodes.Get(context.Background(), id)
	if _, found := got.GetProperty("status"); found {
		t.Fatal("property should be deleted")
	}
}

func TestCAS_NilBoth_Absent(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "status", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should return true for no-op (nil/nil, absent)")
	}
}

func TestCAS_ShadowKey(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	id := n.ID()

	_, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "tkg_labels", nil, "hack")
	if err == nil {
		t.Fatal("CAS should reject tkg_ prefix")
	}
	if !errors.Is(err, types.ErrReservedPrefix) {
		t.Errorf("errors.Is(err, ErrReservedPrefix) = false; err = %v", err)
	}
}

func TestCAS_NodeNotFound(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})

	_, err := g.Nodes.CompareAndSetProperty(context.Background(), types.NodeID(999999), "status", nil, "x")
	if err == nil {
		t.Fatal("CAS should return error for non-existent node")
	}
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Errorf("errors.Is(err, storepkg.ErrNodeNotFound) = false; err = %v", err)
	}
}

func TestCAS_VersionBump(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"v": 1})
	id := n.ID()
	before, _ := g.Nodes.Get(context.Background(), id)
	vBefore := before.Version()

	ok, _ := g.Nodes.CompareAndSetProperty(context.Background(), id, "v", 1, 2)
	if !ok {
		t.Fatal("CAS should succeed")
	}

	after, _ := g.Nodes.Get(context.Background(), id)
	if after.Version() != vBefore+1 {
		t.Fatalf("version = %d, want %d", after.Version(), vBefore+1)
	}
}

func TestCAS_NoVersionBumpOnMismatch(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"v": 1})
	id := n.ID()
	before, _ := g.Nodes.Get(context.Background(), id)
	vBefore := before.Version()

	ok, _ := g.Nodes.CompareAndSetProperty(context.Background(), id, "v", 999, 2)
	if ok {
		t.Fatal("CAS should fail on mismatch")
	}

	after, _ := g.Nodes.Get(context.Background(), id)
	if after.Version() != vBefore {
		t.Fatalf("version = %d, want %d (unchanged)", after.Version(), vBefore)
	}
}

func TestCAS_History(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"v": 1})
	id := n.ID()

	ok, _ := g.Nodes.CompareAndSetProperty(context.Background(), id, "v", 1, 2)
	if !ok {
		t.Fatal("CAS should succeed")
	}

	hist, err := g.Nodes.History(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}
	// History entry should have old value.
	hv, _ := hist[0].GetProperty("v")
	if hv != 1 {
		t.Fatalf("history v = %v, want 1", hv)
	}
}

func TestCAS_TypeMismatch(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"v": int(42)})
	id := n.ID()

	// int64(42) != int(42) — type must match exactly.
	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "v", int64(42), int(99))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("CAS should fail: int64(42) != int(42)")
	}

	got, _ := g.Nodes.Get(context.Background(), id)
	v, _ := got.GetProperty("v")
	if v != int(42) {
		t.Fatalf("v = %v, want 42 (unchanged)", v)
	}
}

func TestCAS_InvalidExpectedValueRejectedBeforeMutation(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"status": "draft"})

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), n.ID(), "status", []any{struct{ X int }{X: 1}}, "published")
	if ok {
		t.Fatal("CAS should not report success for unsupported expected values")
	}
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("CompareAndSetProperty invalid expected = %v, want ErrUnsupportedValueType", err)
	}
	got, err := g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if v, _ := got.GetProperty("status"); v != "draft" {
		t.Fatalf("status after rejected expected = %v, want draft", v)
	}
}

func TestCAS_NaNExpectedMatchesAcceptedPropertyShapes(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{
		"score": float64(math.NaN()),
		"vec":   []float32{1, float32(math.NaN())},
		"meta":  map[string]any{"score": float32(math.NaN())},
	})
	id := n.ID()

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "score", float64(math.NaN()), "matched")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should match scalar NaN properties")
	}

	ok, err = g.Nodes.CompareAndSetProperty(context.Background(), id, "vec", []float32{1, float32(math.NaN())}, "matched")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should match NaN inside []float32 properties")
	}

	ok, err = g.Nodes.CompareAndSetProperty(context.Background(), id, "meta", map[string]any{"score": float32(math.NaN())}, "matched")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS should match NaN inside map[string]any properties")
	}
}

func TestCAS_NaNStillRequiresExactType(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"score": float32(math.NaN())})

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), n.ID(), "score", float64(math.NaN()), "matched")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("CAS should not match float32 NaN with float64 NaN")
	}
}

func TestCAS_RegisteredCustomStructNaNMatches(t *testing.T) {
	if err := types.RegisterPropertyStructType(casNaNProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := casNaNProperty{Score: math.NaN(), Trail: []float32{float32(math.NaN())}}
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"shape": want})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), n.ID(), "shape", casNaNProperty{Score: math.NaN(), Trail: []float32{float32(math.NaN())}}, "matched")
	if err != nil {
		t.Fatalf("node CAS: %v", err)
	}
	if !ok {
		t.Fatal("node CAS should match NaN fields inside registered custom structs")
	}

	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"shape": want})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	ok, err = g.Rels.CompareAndSetProperty(context.Background(), r.ID(), "shape", casNaNProperty{Score: math.NaN(), Trail: []float32{float32(math.NaN())}}, "matched")
	if err != nil {
		t.Fatalf("relationship CAS: %v", err)
	}
	if !ok {
		t.Fatal("relationship CAS should match NaN fields inside registered custom structs")
	}
}

func TestPropertyCASValueEqualReflectBranches(t *testing.T) {
	t.Parallel()

	if types.PropertyValueEqual(int64(1), int32(1)) {
		t.Fatal("different reflect types should not compare equal")
	}
	if !types.PropertyValueEqual(true, true) {
		t.Fatal("equal bools should compare equal")
	}
	if types.PropertyValueEqual(true, false) {
		t.Fatal("different bools should not compare equal")
	}
	if !types.PropertyValueEqual(int64(7), int64(7)) {
		t.Fatal("equal ints should compare equal")
	}
	if types.PropertyValueEqual(int64(7), int64(8)) {
		t.Fatal("different ints should not compare equal")
	}
	if !types.PropertyValueEqual(uint64(7), uint64(7)) {
		t.Fatal("equal uints should compare equal")
	}
	if types.PropertyValueEqual(uint64(7), uint64(8)) {
		t.Fatal("different uints should not compare equal")
	}
	if !types.PropertyValueEqual("same", "same") {
		t.Fatal("equal strings should compare equal")
	}
	if types.PropertyValueEqual("same", "different") {
		t.Fatal("different strings should not compare equal")
	}

	left := &casNaNProperty{Score: math.NaN(), Trail: []float32{float32(math.NaN())}}
	right := &casNaNProperty{Score: math.NaN(), Trail: []float32{float32(math.NaN())}}
	if !types.PropertyValueEqual(left, right) {
		t.Fatal("pointers to structs with NaN fields should compare equal")
	}
	var nilLeft *casNaNProperty
	if !types.PropertyValueEqual(nilLeft, (*casNaNProperty)(nil)) {
		t.Fatal("nil pointers of the same type should compare equal")
	}
	if types.PropertyValueEqual(nilLeft, right) {
		t.Fatal("nil and non-nil pointers should not compare equal")
	}

	var ifaceLeft any = float64(math.NaN())
	var ifaceRight any = float64(math.NaN())
	if !types.PropertyValueEqual(ifaceLeft, ifaceRight) {
		t.Fatal("interfaces wrapping NaN should compare equal")
	}

	if types.PropertyValueEqual(casNaNProperty{Score: 1}, casNaNProperty{Score: 2}) {
		t.Fatal("different struct fields should not compare equal")
	}
	if types.PropertyValueEqual([]float64{1}, []float64{1, 2}) {
		t.Fatal("different slice lengths should not compare equal")
	}
	var nilSlice []float64
	if !types.PropertyValueEqual(nilSlice, []float64(nil)) {
		t.Fatal("nil slices of the same type should compare equal")
	}
	if types.PropertyValueEqual(nilSlice, []float64{}) {
		t.Fatal("nil and empty slices should not compare equal")
	}
	if !types.PropertyValueEqual([1]float64{math.NaN()}, [1]float64{math.NaN()}) {
		t.Fatal("arrays containing NaN should compare equal")
	}

	if !types.PropertyValueEqual(map[string]float64{"x": math.NaN()}, map[string]float64{"x": math.NaN()}) {
		t.Fatal("maps containing NaN should compare equal")
	}
	if types.PropertyValueEqual(map[string]float64{"x": 1}, map[string]float64{"y": 1}) {
		t.Fatal("maps missing a key should not compare equal")
	}
	if types.PropertyValueEqual(map[string]float64{"x": 1}, map[string]float64{"x": 2}) {
		t.Fatal("maps with different values should not compare equal")
	}
	var nilMap map[string]float64
	if !types.PropertyValueEqual(nilMap, map[string]float64(nil)) {
		t.Fatal("nil maps of the same type should compare equal")
	}
	if types.PropertyValueEqual(nilMap, map[string]float64{}) {
		t.Fatal("nil and empty maps should not compare equal")
	}

	if types.PropertyValueEqual(make(chan int), make(chan int)) {
		t.Fatal("unsupported reflect kinds should not compare equal")
	}
}

func TestPropertyCASValueEqualFallbackShapes(t *testing.T) {
	t.Parallel()

	if !propertyCASValueEqual(casNaNProperty{
		Score:  math.NaN(),
		Trail:  []float32{float32(math.NaN())},
		Name:   "same",
		Count:  7,
		Active: true,
	}, casNaNProperty{
		Score:  math.NaN(),
		Trail:  []float32{float32(math.NaN())},
		Name:   "same",
		Count:  7,
		Active: true,
	}) {
		t.Fatal("custom structs with NaN and equal primitive fields should compare equal")
	}
	if propertyCASValueEqual(casNaNProperty{
		Score: math.NaN(),
		Name:  "left",
	}, casNaNProperty{
		Score: math.NaN(),
		Name:  "right",
	}) {
		t.Fatal("custom structs with different primitive fields should not compare equal")
	}
	if propertyCASValueEqual(nil, int64(1)) {
		t.Fatal("nil and non-nil values should not compare equal")
	}
	if !propertyCASValueEqual([]float32{float32(math.NaN())}, []float32{float32(math.NaN())}) {
		t.Fatal("[]float32 NaN values should compare equal")
	}
	if propertyCASValueEqual([]float32{1}, []float32{2}) {
		t.Fatal("different []float32 values should not compare equal")
	}
	if !propertyCASValueEqual([]float64{math.NaN()}, []float64{math.NaN()}) {
		t.Fatal("[]float64 NaN values should compare equal")
	}
	if propertyCASValueEqual([]float64{1}, []float64{2}) {
		t.Fatal("different []float64 values should not compare equal")
	}
	var nilAny []any
	if propertyCASValueEqual(nilAny, []any{}) {
		t.Fatal("nil and empty []any values should not compare equal")
	}
	if !propertyCASValueEqual(map[string]string{"x": "y"}, map[string]string{"x": "y"}) {
		t.Fatal("map[string]string equality should fall back to reflect comparison")
	}
	if propertyCASValueEqual(map[string]string{"x": "y"}, map[string]string{"x": "z"}) {
		t.Fatal("different map[string]string values should not compare equal")
	}
	if !propertyCASValueEqual([]any{map[string]any{"score": math.NaN()}}, []any{map[string]any{"score": math.NaN()}}) {
		t.Fatal("nested []any/map NaN values should compare equal")
	}
	if propertyCASValueEqual([]any{float32(math.NaN())}, []any{float64(math.NaN())}) {
		t.Fatal("nested NaN values still require exact type")
	}
	if propertyCASValueEqual([]float64{math.NaN()}, []float64{}) {
		t.Fatal("different []float64 lengths should not compare equal")
	}
	if propertyCASValueEqual(map[string]any{"x": 1}, map[string]any{"y": 1}) {
		t.Fatal("different map[string]any keys should not compare equal")
	}
	if propertyCASValueEqual(map[string]any{"x": 1}, map[string]any{"x": 1, "y": 2}) {
		t.Fatal("different map[string]any lengths should not compare equal")
	}
	if propertyCASValueEqual(map[string]any{"x": 1}, map[string]any{"x": 2}) {
		t.Fatal("different map[string]any values should not compare equal")
	}
}

type casNaNProperty struct {
	Score  float64
	Trail  []float32
	Name   string
	Count  int
	Active bool
}

func (p casNaNProperty) HashBytes() []byte {
	boolByte := byte(0)
	if p.Active {
		boolByte = 1
	}
	buf := make([]byte, 8+4*len(p.Trail)+len(p.Name)+8+1)
	binary.BigEndian.PutUint64(buf[:8], math.Float64bits(p.Score))
	for i, v := range p.Trail {
		binary.BigEndian.PutUint32(buf[8+4*i:12+4*i], math.Float32bits(v))
	}
	pos := 8 + 4*len(p.Trail)
	copy(buf[pos:], p.Name)
	pos += len(p.Name)
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(p.Count))
	buf[pos+8] = boolByte
	return buf
}

func (p casNaNProperty) DeepCopyValue() any {
	cp := p
	cp.Trail = append([]float32(nil), p.Trail...)
	return cp
}

func TestCAS_DeleteMismatch(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"status": "draft"})
	id := n.ID()

	// Try to delete with wrong expected value.
	ok, err := g.Nodes.CompareAndSetProperty(context.Background(), id, "status", "archived", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("CAS delete should fail on mismatch")
	}

	got, _ := g.Nodes.Get(context.Background(), id)
	v, found := got.GetProperty("status")
	if !found || v != "draft" {
		t.Fatalf("status = (%v, %v), want (draft, true) — unchanged", v, found)
	}
}

func TestCAS_WithContextRejectsNilContext(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)

	ok, err := g.Nodes.CompareAndSetProperty(nil, n.ID(), "status", nil, "active") //nolint:staticcheck // intentional nil context boundary test
	if ok {
		t.Fatal("CAS should not report success for nil context")
	}
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("CompareAndSetPropertyWithContext(nil) = %v, want ErrNilContext", err)
	}
}

func TestCAS_WithContextCanceledAfterReadDoesNotPersist(t *testing.T) {
	t.Parallel()
	store := &cancelOnGetNodeStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.cancel = cancel

	ok, err := g.Nodes.CompareAndSetProperty(ctx, n.ID(), "status", "draft", "published")
	if ok {
		t.Fatal("CAS should not report success when context is canceled after the entity read")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CompareAndSetPropertyWithContext = %v, want context.Canceled", err)
	}

	got, err := g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if status, _ := got.GetProperty("status"); status != "draft" {
		t.Fatalf("status = %v, want draft", status)
	}
}

type cancelOnGetNodeStore struct {
	*memory.Store
	cancel   context.CancelFunc
	canceled bool
}

func (s *cancelOnGetNodeStore) GetNode(id types.NodeID) (*types.Node, error) {
	n, err := s.Store.GetNode(id)
	if err == nil && s.cancel != nil && !s.canceled {
		s.canceled = true
		s.cancel()
	}
	return n, err
}

func newRelCASFixture(t *testing.T, props map[string]any) (*Core, types.RelID) {
	t.Helper()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add node a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add node b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, props)
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}
	return g, r.ID()
}

func TestRelCAS_Match(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{"status": "draft"})

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "status", "draft", "published")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("relationship CAS should return true on match")
	}

	got, _ := g.Rels.Get(context.Background(), id)
	v, _ := got.GetProperty("status")
	if v != "published" {
		t.Fatalf("status = %v, want published", v)
	}
}

func TestRelCAS_Mismatch(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{"status": "draft"})

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "status", "archived", "published")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("relationship CAS should return false on mismatch")
	}

	got, _ := g.Rels.Get(context.Background(), id)
	v, _ := got.GetProperty("status")
	if v != "draft" {
		t.Fatalf("status = %v, want draft (unchanged)", v)
	}
}

func TestRelCAS_NilExpectedAbsent(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, nil)

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "status", nil, "active")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("relationship CAS should return true when expected=nil and property absent")
	}

	got, _ := g.Rels.Get(context.Background(), id)
	v, found := got.GetProperty("status")
	if !found || v != "active" {
		t.Fatalf("status = (%v, %v), want (active, true)", v, found)
	}
}

func TestRelCAS_AddPropertyRejectsFinalPropertyCountOverLimit(t *testing.T) {
	t.Parallel()
	g, err := New(Config{
		Store:      memory.New(),
		Validation: ValidationLimits{MaxPropertiesPerEntity: 1},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	r, _ := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"a": 1})

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), r.ID(), "b", nil, 2)
	if err == nil {
		t.Fatal("expected property count limit error")
	}
	if ok {
		t.Fatal("relationship CAS should not report success when final property count exceeds limit")
	}
	if !errors.Is(err, ErrTooManyProperties) {
		t.Fatalf("expected ErrTooManyProperties, got: %v", err)
	}

	got, getErr := g.Rels.Get(context.Background(), r.ID())
	if getErr != nil {
		t.Fatal(getErr)
	}
	if got.PropertyCount() != 1 {
		t.Fatalf("property count = %d, want 1", got.PropertyCount())
	}
	if _, found := got.GetProperty("b"); found {
		t.Fatal("overflow property should not be persisted")
	}
}

func TestRelCAS_NilExpectedPresent(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{"status": "draft"})

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "status", nil, "active")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("relationship CAS should return false when expected=nil but property exists")
	}
}

func TestRelCAS_DeleteOnMatch(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{"status": "draft"})

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "status", "draft", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("relationship CAS should return true on match+delete")
	}

	got, _ := g.Rels.Get(context.Background(), id)
	if _, found := got.GetProperty("status"); found {
		t.Fatal("property should be deleted")
	}
}

func TestRelCAS_NilBothAbsent(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, nil)

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "status", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("relationship CAS should return true for no-op (nil/nil, absent)")
	}
}

func TestRelCAS_ShadowKey(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, nil)

	_, err := g.Rels.CompareAndSetProperty(context.Background(), id, "tkg_type", nil, "hack")
	if err == nil {
		t.Fatal("relationship CAS should reject tkg_ prefix")
	}
	if !errors.Is(err, types.ErrReservedPrefix) {
		t.Errorf("errors.Is(err, ErrReservedPrefix) = false; err = %v", err)
	}
}

func TestRelCAS_InvalidNewValueRejectedBeforeMutation(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, nil)

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "bad", nil, struct{ X int }{X: 1})
	if ok {
		t.Fatal("relationship CAS should not report success for unsupported property values")
	}
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("CompareAndSetProperty invalid value = %v, want ErrUnsupportedValueType", err)
	}
	got, err := g.Rels.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if _, found := got.GetProperty("bad"); found {
		t.Fatal("invalid CAS value should not be persisted")
	}
}

func TestRelCAS_InvalidExpectedValueRejectedBeforeMutation(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{"status": "draft"})

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "status", []any{struct{ X int }{X: 1}}, "published")
	if ok {
		t.Fatal("relationship CAS should not report success for unsupported expected values")
	}
	if !errors.Is(err, types.ErrUnsupportedValueType) {
		t.Fatalf("CompareAndSetProperty invalid expected = %v, want ErrUnsupportedValueType", err)
	}
	got, err := g.Rels.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if v, _ := got.GetProperty("status"); v != "draft" {
		t.Fatalf("relationship status after rejected expected = %v, want draft", v)
	}
}

func TestRelCAS_NotFound(t *testing.T) {
	t.Parallel()
	g, _ := New(Config{Store: memory.New()})

	_, err := g.Rels.CompareAndSetProperty(context.Background(), types.RelID(999999), "status", nil, "x")
	if err == nil {
		t.Fatal("relationship CAS should return error for non-existent relationship")
	}
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Errorf("errors.Is(err, storepkg.ErrRelNotFound) = false; err = %v", err)
	}
}

func TestRelCAS_VersionBump(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{"v": 1})
	before, _ := g.Rels.Get(context.Background(), id)
	vBefore := before.Version()

	ok, _ := g.Rels.CompareAndSetProperty(context.Background(), id, "v", 1, 2)
	if !ok {
		t.Fatal("relationship CAS should succeed")
	}

	after, _ := g.Rels.Get(context.Background(), id)
	if after.Version() != vBefore+1 {
		t.Fatalf("version = %d, want %d", after.Version(), vBefore+1)
	}
}

func TestRelCAS_NoVersionBumpOnMismatch(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{"v": 1})
	before, _ := g.Rels.Get(context.Background(), id)
	vBefore := before.Version()

	ok, _ := g.Rels.CompareAndSetProperty(context.Background(), id, "v", 999, 2)
	if ok {
		t.Fatal("relationship CAS should fail on mismatch")
	}

	after, _ := g.Rels.Get(context.Background(), id)
	if after.Version() != vBefore {
		t.Fatalf("version = %d, want %d (unchanged)", after.Version(), vBefore)
	}
}

func TestRelCAS_History(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{"v": 1})

	ok, _ := g.Rels.CompareAndSetProperty(context.Background(), id, "v", 1, 2)
	if !ok {
		t.Fatal("relationship CAS should succeed")
	}

	hist, err := g.Rels.History(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}
	hv, _ := hist[0].GetProperty("v")
	if hv != 1 {
		t.Fatalf("history v = %v, want 1", hv)
	}
}

func TestRelCAS_TypeMismatch(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{"v": int(42)})

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "v", int64(42), int(99))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("relationship CAS should fail: int64(42) != int(42)")
	}

	got, _ := g.Rels.Get(context.Background(), id)
	v, _ := got.GetProperty("v")
	if v != int(42) {
		t.Fatalf("v = %v, want 42 (unchanged)", v)
	}
}

func TestRelCAS_NaNExpectedMatchesAcceptedPropertyShapes(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{
		"weight": float64(math.NaN()),
		"trail":  []any{float32(math.NaN()), "done"},
	})

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "weight", float64(math.NaN()), "matched")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("relationship CAS should match scalar NaN properties")
	}

	ok, err = g.Rels.CompareAndSetProperty(context.Background(), id, "trail", []any{float32(math.NaN()), "done"}, "matched")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("relationship CAS should match NaN inside []any properties")
	}
}

func TestRelCAS_DeleteMismatch(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, map[string]any{"status": "draft"})

	ok, err := g.Rels.CompareAndSetProperty(context.Background(), id, "status", "archived", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("relationship CAS delete should fail on mismatch")
	}

	got, _ := g.Rels.Get(context.Background(), id)
	v, found := got.GetProperty("status")
	if !found || v != "draft" {
		t.Fatalf("status = (%v, %v), want (draft, true) — unchanged", v, found)
	}
}

func TestRelCAS_WithContextRejectsNilContext(t *testing.T) {
	t.Parallel()
	g, id := newRelCASFixture(t, nil)

	ok, err := g.Rels.CompareAndSetProperty(nil, id, "status", nil, "active") //nolint:staticcheck // intentional nil context boundary test
	if ok {
		t.Fatal("relationship CAS should not report success for nil context")
	}
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("CompareAndSetPropertyWithContext(nil) = %v, want ErrNilContext", err)
	}
}

func TestRelCAS_WithContextCanceledAfterReadDoesNotPersist(t *testing.T) {
	t.Parallel()
	store := &cancelOnSecondGetRelationshipStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.cancel = cancel

	ok, err := g.Rels.CompareAndSetProperty(ctx, r.ID(), "status", "draft", "published")
	if ok {
		t.Fatal("relationship CAS should not report success when context is canceled after the locked entity read")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CompareAndSetPropertyWithContext = %v, want context.Canceled", err)
	}

	got, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if status, _ := got.GetProperty("status"); status != "draft" {
		t.Fatalf("status = %v, want draft", status)
	}
}

func TestRelCAS_WithContextCanceledBeforePersistDoesNotPersist(t *testing.T) {
	t.Parallel()
	store := &cancelOnEndpointNodeHashStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.cancel = cancel

	ok, err := g.Rels.CompareAndSetProperty(ctx, r.ID(), "status", "draft", "published")
	if ok {
		t.Fatal("relationship CAS should not report success when context is canceled before persist")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CompareAndSetPropertyWithContext = %v, want context.Canceled", err)
	}

	got, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if status, _ := got.GetProperty("status"); status != "draft" {
		t.Fatalf("status = %v, want draft", status)
	}
}

type cancelOnSecondGetRelationshipStore struct {
	*memory.Store
	cancel context.CancelFunc
	reads  int
}

func (s *cancelOnSecondGetRelationshipStore) GetRelationship(id types.RelID) (*types.Relationship, error) {
	r, err := s.Store.GetRelationship(id)
	if err == nil && s.cancel != nil {
		s.reads++
		if s.reads == 2 {
			s.cancel()
		}
	}
	return r, err
}

type cancelOnEndpointNodeHashStore struct {
	*memory.Store
	cancel   context.CancelFunc
	canceled bool
}

func (s *cancelOnEndpointNodeHashStore) GetNode(id types.NodeID) (*types.Node, error) {
	n, err := s.Store.GetNode(id)
	if err == nil && s.cancel != nil && !s.canceled {
		s.canceled = true
		s.cancel()
	}
	return n, err
}

// ─── OutgoingRelationshipsForNodes ───────────────────────────────────────────
