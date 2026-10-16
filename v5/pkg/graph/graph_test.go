package graph

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func binding(t testing.TB, g types.GraphID) types.DefaultAxisBinding {
	t.Helper()
	b, err := types.NewDefaultAxisBinding(g, temporal.AxisID{19}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func goodConfig(t testing.TB) Config {
	g := types.GraphID{3}
	return Config{Dir: t.TempDir(), GraphID: g, DefaultAxis: binding(t, g), Durability: DurabilityCommit}
}

func axis(t testing.TB) temporal.Axis { return binding(t, types.GraphID{3}).Axis() }

func scopeAll(t testing.TB) temporal.Scope {
	s, err := temporal.All(axis(t))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func scopeEmpty(t testing.TB) temporal.Scope {
	s, err := temporal.Empty(axis(t))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// zero is a coordinate: a point at 0 is a valid scope.
func scopeZero(t testing.TB) temporal.Scope {
	p, err := types.InstantPosition(axis(t), 0, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := temporal.Point(p)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func otherAxisScope(t testing.TB) temporal.Scope {
	b, err := types.NewDefaultAxisBinding(types.GraphID{3}, temporal.AxisID{77}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := temporal.All(b.Axis())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSentinelsAreDistinctAndMatchable(t *testing.T) {
	all := []error{ErrNotImplemented, ErrInvalidConfig, ErrInvalidScope, ErrInvalidID, ErrInvalidBound,
		ErrInvalidOp, ErrInvalidSelector, ErrInvalidSnapshotSpec, ErrInvalidCursor, ErrInvalidRequest,
		ErrClosed, ErrTxDone, ErrNotFound, ErrConflict, ErrUniqueViolation, ErrCommitNotDurable, ErrSnapshotExpired}
	for i, a := range all {
		if a == nil || a.Error() == "" {
			t.Fatalf("sentinel %d empty", i)
		}
		if !errors.Is(a, a) {
			t.Fatalf("sentinel %d not Is itself", i)
		}
		for j, b := range all {
			if i != j && errors.Is(a, b) {
				t.Fatalf("sentinels %d and %d alias", i, j)
			}
		}
	}
	if !errors.Is(ErrInvalidID, types.ErrInvalidID) {
		t.Fatal("ErrInvalidID must be the types sentinel")
	}
}

func TestOpenWithoutEngineReturnsNotImplemented(t *testing.T) {
	g, err := Open(context.Background(), goodConfig(t))
	if !errors.Is(err, ErrNotImplemented) || g != nil {
		t.Fatalf("got %v %v", g, err)
	}
}

func TestOpenValidatesBeforeEngineSentinel(t *testing.T) {
	cfg := goodConfig(t)
	cfg.Dir = ""
	if _, err := Open(context.Background(), cfg); !errors.Is(err, ErrInvalidConfig) || errors.Is(err, ErrNotImplemented) {
		t.Fatalf("got %v", err)
	}
	if _, err := Open(nil, goodConfig(t)); !errors.Is(err, ErrInvalidConfig) { //nolint:staticcheck // nil ctx is the case
		t.Fatalf("nil ctx: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(ctx, goodConfig(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: %v", err)
	}
}

func TestConfigValidate(t *testing.T) {
	g := types.GraphID{3}
	mem := Config{Memory: true, GraphID: g, DefaultAxis: binding(t, g), Durability: DurabilityBuffered}
	if err := mem.Validate(); err != nil {
		t.Fatalf("memory config: %v", err)
	}
	if err := goodConfig(t).Validate(); err != nil {
		t.Fatalf("dir config: %v", err)
	}
	cases := map[string]func(c *Config){
		"empty":               func(c *Config) { *c = Config{} },
		"blank dir":           func(c *Config) { c.Dir = "  \t" },
		"nul in dir":          func(c *Config) { c.Dir = "a\x00b" },
		"dir and memory":      func(c *Config) { c.Memory = true },
		"neither":             func(c *Config) { c.Dir = "" },
		"zero graph":          func(c *Config) { c.GraphID = types.GraphID{} },
		"zero binding":        func(c *Config) { c.DefaultAxis = types.DefaultAxisBinding{} },
		"binding other graph": func(c *Config) { c.GraphID = types.GraphID{4} },
		"zero durability":     func(c *Config) { c.Durability = 0 },
		"unknown durability":  func(c *Config) { c.Durability = 99 },
		"bad limits":          func(c *Config) { c.Limits = temporal.Limits{MaxInputBytes: -1} },
		"memory commit":       func(c *Config) { c.Dir, c.Memory, c.Durability = "", true, DurabilityCommit },
	}
	for name, mut := range cases {
		c := goodConfig(t)
		mut(&c)
		if err := c.Validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func nid(v uint64) types.NodeID { return types.NodeID(v) }
func rid(v uint64) types.RelID  { return types.RelID(v) }

func TestRefRefusesZeroIDs(t *testing.T) {
	if _, err := NodeRef(0); !errors.Is(err, ErrInvalidID) {
		t.Fatal(err)
	}
	if _, err := RelRef(0); !errors.Is(err, ErrInvalidID) {
		t.Fatal(err)
	}
	if err := (Ref{kind: refNode, node: types.NodeID(-1)}).Validate(); !errors.Is(err, ErrInvalidID) {
		t.Fatal("negative ID must be refused", err)
	}
	r, err := NodeRef(nid(5))
	if err != nil || r.Node() != nid(5) || r.IsRel() {
		t.Fatalf("%v %v", r, err)
	}
	r, err = RelRef(rid(6))
	if err != nil || r.Rel() != rid(6) || !r.IsRel() {
		t.Fatalf("%v %v", r, err)
	}
	if err := (Ref{}).Validate(); !errors.Is(err, ErrInvalidID) {
		t.Fatal("zero Ref must be invalid", err)
	}
}

func TestOpsValidate(t *testing.T) {
	l := temporal.Limits{}
	n5, _ := NodeRef(5)
	good := []Op{
		CreateNode{Labels: []string{"A"}, Scope: scopeZero(t)},
		CreateNode{Scope: scopeAll(t)},
		CreateRel{Type: "R", From: nid(1), To: nid(2), Scope: scopeAll(t)},
		Correct{Target: n5, Set: map[string]Value{"k": 1}, Scope: scopeAll(t)},
		Close{Target: n5, End: temporal.PositiveInfinity(), Scope: scopeAll(t)},
		AddLabel{Target: nid(1), Label: "L", Scope: scopeAll(t)},
		RemoveLabel{Target: nid(1), Label: "L", Scope: scopeAll(t)},
		Set{Target: n5, Key: "k", Value: 0, Scope: scopeAll(t)},
		Unset{Target: n5, Key: "k", Scope: scopeAll(t)},
		Retract{Target: n5, Scope: scopeAll(t)},
		GetOrCreate{Label: "L", Key: "k", Value: "v", Scope: scopeAll(t)},
	}
	for i, op := range good {
		if err := op.Validate(l); err != nil {
			t.Errorf("good %d (%T): %v", i, op, err)
		}
	}
	bad := map[string]struct {
		op   Op
		want error
	}{
		"zero scope":       {CreateNode{Labels: []string{"A"}}, ErrInvalidScope},
		"empty scope":      {CreateNode{Scope: scopeEmpty(t)}, ErrInvalidScope},
		"empty label":      {CreateNode{Labels: []string{""}, Scope: scopeAll(t)}, ErrInvalidOp},
		"rel no type":      {CreateRel{From: nid(1), To: nid(2), Scope: scopeAll(t)}, ErrInvalidOp},
		"rel zero from":    {CreateRel{Type: "R", To: nid(2), Scope: scopeAll(t)}, ErrInvalidID},
		"rel zero to":      {CreateRel{Type: "R", From: nid(1), Scope: scopeAll(t)}, ErrInvalidID},
		"correct zero ref": {Correct{Set: map[string]Value{"k": 1}, Scope: scopeAll(t)}, ErrInvalidID},
		"correct no set":   {Correct{Target: n5, Scope: scopeAll(t)}, ErrInvalidOp},
		"close zero bound": {Close{Target: n5, Scope: scopeAll(t)}, ErrInvalidBound},
		"close -inf end":   {Close{Target: n5, End: temporal.NegativeInfinity(), Scope: scopeAll(t)}, ErrInvalidBound},
		"close zero scope": {Close{Target: n5, End: temporal.PositiveInfinity()}, ErrInvalidScope},
		"label zero id":    {AddLabel{Label: "L", Scope: scopeAll(t)}, ErrInvalidID},
		"label empty":      {RemoveLabel{Target: nid(1), Scope: scopeAll(t)}, ErrInvalidOp},
		"set empty key":    {Set{Target: n5, Scope: scopeAll(t)}, ErrInvalidOp},
		"unset zero ref":   {Unset{Key: "k", Scope: scopeAll(t)}, ErrInvalidID},
		"retract zero":     {Retract{Scope: scopeAll(t)}, ErrInvalidID},
		"getorcreate key":  {GetOrCreate{Label: "L", Scope: scopeAll(t)}, ErrInvalidOp},
	}
	for name, c := range bad {
		err := c.op.Validate(l)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v want %v", name, err, c.want)
		}
	}
}

func TestCheckOpRequiresDefaultAxis(t *testing.T) {
	b := binding(t, types.GraphID{3})
	n5, _ := NodeRef(5)
	if err := CheckOp(b, temporal.Limits{}, Set{Target: n5, Key: "k", Scope: scopeAll(t)}); err != nil {
		t.Fatal(err)
	}
	if err := CheckOp(b, temporal.Limits{}, Set{Target: n5, Key: "k", Scope: otherAxisScope(t)}); !errors.Is(err, ErrInvalidScope) {
		t.Fatal("axis mismatch", err)
	}
	if err := CheckOp(b, temporal.Limits{}, nil); !errors.Is(err, ErrInvalidOp) {
		t.Fatal(err)
	}
	if err := CheckOp(b, temporal.Limits{}, Set{Target: n5, Key: "k"}); !errors.Is(err, ErrInvalidScope) {
		t.Fatal(err)
	}
}

func TestSelectorValidateNamesViewAndBothScopes(t *testing.T) {
	ok := Selector{View: ViewEffective, Valid: scopeAll(t), Known: scopeZero(t)}
	if err := ok.Validate(temporal.Limits{}); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(s *Selector){
		"no view":      func(s *Selector) { s.View = 0 },
		"unknown view": func(s *Selector) { s.View = 9 },
		"no valid":     func(s *Selector) { s.Valid = temporal.Scope{} },
		"no known":     func(s *Selector) { s.Known = temporal.Scope{} },
		"empty label":  func(s *Selector) { s.Labels = []string{""} },
		"empty eq key": func(s *Selector) { s.Equals = []PropertyEq{{Key: ""}} },
		"label and type": func(s *Selector) {
			s.Labels, s.RelType = []string{"A"}, "R"
		},
	}
	for name, mut := range cases {
		s := ok
		mut(&s)
		if err := s.Validate(temporal.Limits{}); !errors.Is(err, ErrInvalidSelector) && !errors.Is(err, ErrInvalidScope) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestSnapshotSpec(t *testing.T) {
	var cut Cut
	var rc Receipt
	for name, s := range map[string]SnapshotSpec{"zero": {}, "at zero cut": At(cut), "after zero receipt": After(rc)} {
		if err := s.Validate(); !errors.Is(err, ErrInvalidSnapshotSpec) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, s := range map[string]SnapshotSpec{"fresh": Fresh(), "stable": Stable()} {
		if err := s.Validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := At(testCut()).Validate(); err != nil {
		t.Error(err)
	}
	if err := After(testReceipt()).Validate(); err != nil {
		t.Error(err)
	}
}

func TestOpenSnapshotWithoutEngine(t *testing.T) {
	var g Graph
	if _, err := g.OpenSnapshot(context.Background(), SnapshotSpec{}); !errors.Is(err, ErrInvalidSnapshotSpec) {
		t.Fatal(err)
	}
	if _, err := g.OpenSnapshot(context.Background(), Fresh()); !errors.Is(err, ErrNotImplemented) {
		t.Fatal(err)
	}
	var nilG *Graph
	if _, err := nilG.OpenSnapshot(context.Background(), Fresh()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := nilG.Update(context.Background(), func(Tx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := nilG.Close(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestUpdateValidatesArguments(t *testing.T) {
	var g Graph
	if _, err := g.Update(context.Background(), nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal("nil fn", err)
	}
	if _, err := g.Update(nil, func(Tx) error { return nil }); !errors.Is(err, ErrInvalidRequest) { //nolint:staticcheck
		t.Fatal("nil ctx", err)
	}
	if _, err := g.Update(context.Background(), func(Tx) error { return nil }); !errors.Is(err, ErrNotImplemented) {
		t.Fatal(err)
	}
}

func TestCursorOpaqueAndRefusesEmpty(t *testing.T) {
	if _, err := ParseCursor(nil); !errors.Is(err, ErrInvalidCursor) {
		t.Fatal(err)
	}
	if _, err := ParseCursor([]byte{}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatal(err)
	}
	if _, err := ParseCursor(make([]byte, MaxCursorBytes+1)); !errors.Is(err, ErrInvalidCursor) {
		t.Fatal(err)
	}
	c, err := ParseCursor([]byte("abc"))
	if err != nil || string(c.Bytes()) != "abc" {
		t.Fatal(c, err)
	}
	b := c.Bytes()
	b[0] = 'x'
	if string(c.Bytes()) != "abc" {
		t.Fatal("Bytes must copy")
	}
	if err := (Cursor{}).Validate(); !errors.Is(err, ErrInvalidCursor) {
		t.Fatal("zero cursor", err)
	}
}

func goodSel(t testing.TB) Selector {
	return Selector{View: ViewDeclared, Valid: scopeAll(t), Known: scopeAll(t)}
}

func TestScanRequestValidate(t *testing.T) {
	ok := ScanRequest{Selector: goodSel(t), Limit: 100, MaxBytes: 1 << 20}
	if err := ok.Validate(temporal.Limits{}); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(r *ScanRequest){
		"zero limit":   func(r *ScanRequest) { r.Limit = 0 },
		"neg limit":    func(r *ScanRequest) { r.Limit = -1 },
		"over ceiling": func(r *ScanRequest) { r.Limit = MaxPageRows + 1 },
		"no bytes":     func(r *ScanRequest) { r.MaxBytes = 0 },
		"no view":      func(r *ScanRequest) { r.Selector.View = 0 },
		"empty prop":   func(r *ScanRequest) { r.Projection.Properties = []string{""} },
	}
	for name, mut := range cases {
		r := ok
		mut(&r)
		if err := r.Validate(temporal.Limits{}); err == nil || !(errors.Is(err, ErrInvalidRequest) || errors.Is(err, ErrInvalidSelector)) {
			t.Errorf("%s: got %v", name, err)
		}
	}
	r := ok
	r.Limit = MaxPageRows
	if err := r.Validate(temporal.Limits{}); err != nil {
		t.Fatal("ceiling itself is allowed", err)
	}
}

func TestSnapshotReadsValidateThenNotImplemented(t *testing.T) {
	var s Snapshot
	ctx := context.Background()
	if _, err := s.Scan(ctx, ScanRequest{}); !errors.Is(err, ErrInvalidRequest) && !errors.Is(err, ErrInvalidSelector) {
		t.Fatal(err)
	}
	if _, err := s.Scan(ctx, ScanRequest{Selector: goodSel(t), Limit: 1, MaxBytes: 1}); !errors.Is(err, ErrNotImplemented) {
		t.Fatal(err)
	}
	if _, err := s.Resume(ctx, Cursor{}, 10, 1024); !errors.Is(err, ErrInvalidCursor) {
		t.Fatal("zero cursor", err)
	}
	c, _ := ParseCursor([]byte("x"))
	if _, err := s.Resume(ctx, c, 0, 1024); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal("zero limit", err)
	}
	if _, err := s.Resume(ctx, c, 10, 1024); !errors.Is(err, ErrNotImplemented) {
		t.Fatal(err)
	}
	var nilS *Snapshot
	if _, err := nilS.Scan(ctx, ScanRequest{Selector: goodSel(t), Limit: 1, MaxBytes: 1}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestLookupRequestValidate(t *testing.T) {
	s := goodSel(t)
	ok := LookupRequest{Selector: s, Nodes: []types.NodeID{1, 2}}
	if err := ok.Validate(temporal.Limits{}); err != nil {
		t.Fatal(err)
	}
	cases := map[string]LookupRequest{
		"none":      {Selector: s},
		"zero node": {Selector: s, Nodes: []types.NodeID{1, 0}},
		"zero rel":  {Selector: s, Rels: []types.RelID{0}},
		"too many":  {Selector: s, Nodes: make([]types.NodeID, MaxLookupIDs+1)},
		"no view":   {Selector: Selector{Valid: scopeAll(t), Known: scopeAll(t)}, Nodes: []types.NodeID{1}},
	}
	for name, r := range cases {
		if err := r.Validate(temporal.Limits{}); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	var snap Snapshot
	if _, err := snap.Lookup(context.Background(), LookupRequest{Selector: s, Nodes: []types.NodeID{0}}); !errors.Is(err, ErrInvalidID) {
		t.Fatal(err)
	}
	if _, err := snap.Lookup(context.Background(), ok); !errors.Is(err, ErrNotImplemented) {
		t.Fatal(err)
	}
}

func TestTimelineValidate(t *testing.T) {
	var snap Snapshot
	ctx := context.Background()
	if _, err := snap.Timeline(ctx, Ref{}, goodSel(t)); !errors.Is(err, ErrInvalidID) {
		t.Fatal(err)
	}
	n, _ := NodeRef(1)
	bad := goodSel(t)
	bad.View = 0
	if _, err := snap.Timeline(ctx, n, bad); !errors.Is(err, ErrInvalidSelector) {
		t.Fatal(err)
	}
	if _, err := snap.Timeline(ctx, n, goodSel(t)); !errors.Is(err, ErrNotImplemented) {
		t.Fatal(err)
	}
}

func TestErrorTextDoesNotLeakDir(t *testing.T) {
	c := goodConfig(t)
	c.Durability = 0
	if err := c.Validate(); err == nil || strings.Contains(err.Error(), c.Dir) {
		t.Fatal("error must not echo the directory", err)
	}
}
