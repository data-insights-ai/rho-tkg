package core

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 14c: propertyIndexCap/relPropertyIndexCap (DDL: CreatePropertyIndex/
// CreateRelPropertyIndex) type-asserted the SAME bundled interface
// (PropertyIndexCapability/RelPropertyIndexCapability) that
// propertyQueryCapability/relPropertyQueryCapability (query acceleration,
// core.go) already guard against wrapper-promotion — but the DDL accessors
// had no such guard. A wrapper that merely inherits the interface via Go
// embedding (never declaring the query method itself) could still
// successfully CreatePropertyIndex (promoted straight through to the
// embedded native store) while the graph's OWN query path refuses to trust
// that same inherited capability and falls back to a scan — a permanently
// inert index maintained forever but never consulted.

// concreteRelPropertyScanFaultStore is the relationship-side mirror of
// concretePropertyQueryFaultStore: RelPropertyIndexCapability is not part of
// the storepkg.Store interface (unlike PropertyIndexCapability), so a
// wrapper must embed the concrete *memory.Store to promote it. Overrides
// only RelationshipsByType, never RelationshipsByTypeAndProperty.
type concreteRelPropertyScanFaultStore struct {
	*memory.Store
	err  error
	fail bool
}

func (s *concreteRelPropertyScanFaultStore) RelationshipsByType(token uint16, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	if s.fail {
		return nil, s.err
	}
	return s.Store.RelationshipsByType(token, opts)
}

// concreteDirectRelPropertyQueryFaultStore declares RelationshipsByTypeAndProperty
// directly (mirrors a genuine out-of-tree implementation), so the
// wrapper-promotion guard must NOT suppress it.
type concreteDirectRelPropertyQueryFaultStore struct {
	*memory.Store
	queryErr error
	scanErr  error
}

func (s *concreteDirectRelPropertyQueryFaultStore) RelationshipsByType(token uint16, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	return nil, s.scanErr
}

func (s *concreteDirectRelPropertyQueryFaultStore) RelationshipsByTypeAndProperty(uint16, string, any, storepkg.QueryOpts) ([]*types.Relationship, error) {
	return nil, s.queryErr
}

func TestPropertyIndexDDL_IgnoresInterfaceEmbeddedNativeStore(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic interface-embedded NodesByLabel fault")
	ms := memory.New()
	fs := &interfaceStorePropertyScanFaultStore{Store: ms, err: injected}
	if _, ok := any(fs).(storepkg.PropertyIndexCapability); !ok {
		t.Fatal("test wrapper must inherit PropertyIndexCapability from store.Store")
	}

	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Query side already refuses the inherited capability (existing guard).
	if g.propertyQuery != nil {
		t.Fatal("interface-embedded native store must not enable the property-query fast path")
	}
	// DDL side must now refuse it too — else it would build a real index on
	// ms that the query side above can never use (BACKLOG 14c).
	if err := g.Index.CreateProperty("Doc", "status"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("CreateProperty with interface-embedded native store = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestPropertyIndexDDL_AllowsInterfaceEmbeddedDirectCapability(t *testing.T) {
	t.Parallel()
	queryErr := errors.New("synthetic direct interface property query fault")
	scanErr := errors.New("synthetic fallback label scan fault")
	ms := memory.New()
	fs := &interfaceStoreDirectPropertyQueryFaultStore{
		Store:    ms,
		queryErr: queryErr,
		scanErr:  scanErr,
	}
	if _, ok := any(fs).(storepkg.PropertyIndexCapability); !ok {
		t.Fatal("test wrapper must satisfy PropertyIndexCapability")
	}

	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if g.propertyQuery == nil {
		t.Fatal("interface-embedded direct property query capability must remain enabled")
	}
	// A wrapper that genuinely declares the query method itself must still be
	// trusted for DDL — the guard must not over-block real implementations.
	if err := g.Index.CreateProperty("Doc", "status"); err != nil {
		t.Fatalf("CreateProperty with direct interface store: %v", err)
	}
}

func TestRelPropertyIndexDDL_IgnoresConcreteEmbeddedNativeStore(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic concrete-embedded RelationshipsByType fault")
	ms := memory.New()
	fs := &concreteRelPropertyScanFaultStore{Store: ms, err: injected}
	if _, ok := any(fs).(storepkg.RelPropertyIndexCapability); !ok {
		t.Fatal("test wrapper must inherit RelPropertyIndexCapability from *memory.Store")
	}

	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if g.relPropertyQuery != nil {
		t.Fatal("interface-embedded native store must not enable the rel-property-query fast path")
	}
	if err := g.Index.CreateRelProperty("KNOWS", "status"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("CreateRelProperty with interface-embedded native store = %v, want ErrCapabilityNotSupported", err)
	}
}

func TestRelPropertyIndexDDL_AllowsConcreteEmbeddedDirectCapability(t *testing.T) {
	t.Parallel()
	queryErr := errors.New("synthetic direct rel-property query fault")
	scanErr := errors.New("synthetic fallback type scan fault")
	ms := memory.New()
	fs := &concreteDirectRelPropertyQueryFaultStore{
		Store:    ms,
		queryErr: queryErr,
		scanErr:  scanErr,
	}
	if _, ok := any(fs).(storepkg.RelPropertyIndexCapability); !ok {
		t.Fatal("test wrapper must satisfy RelPropertyIndexCapability")
	}

	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if g.relPropertyQuery == nil {
		t.Fatal("interface-embedded direct rel-property query capability must remain enabled")
	}
	if err := g.Index.CreateRelProperty("KNOWS", "status"); err != nil {
		t.Fatalf("CreateRelProperty with direct interface store: %v", err)
	}
}
