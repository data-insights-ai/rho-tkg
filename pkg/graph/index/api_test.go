package index

import (
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestAPINilReceiversReturnErrNilGraphOrNil(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "CreateProperty", run: func() error { return nilAPI.CreateProperty("Node", "name") }},
		{name: "DropProperty", run: func() error { return nilAPI.DropProperty("Node", "name") }},
		{name: "CreateHighFrequency", run: func() error { return nilAPI.CreateHighFrequency("Node", time.Second) }},
		{name: "DropHighFrequency", run: func() error { return nilAPI.DropHighFrequency("Node") }},
		{name: "CreateTemporal", run: func() error { return nilAPI.CreateTemporal("Node") }},
		{name: "DropTemporal", run: func() error { return nilAPI.DropTemporal("Node") }},
		{name: "CreateVector", run: func() error { return nilAPI.CreateVector("Node", "embedding", 3, storepkg.DistanceCosine) }},
		{name: "DropVector", run: func() error { return nilAPI.DropVector("Node", "embedding") }},
		{name: "RegisterProvider", run: func() error { return nilAPI.RegisterProvider(testIndexProvider{}) }},
		{name: "RegisterLegacyProvider", run: func() error { return nilAPI.RegisterLegacyProvider(testLegacyIndexProvider{}) }},
		{name: "UnregisterProvider", run: func() error { return nilAPI.UnregisterProvider("geo") }},
	} {
		if err := tc.run(); !errors.Is(err, grapherr.ErrNilGraph) {
			t.Fatalf("%s = %v, want ErrNilGraph", tc.name, err)
		}
	}
	if _, err := nilAPI.SearchNearest("Node", "embedding", []float32{1, 2, 3}, 2, storepkg.QueryOpts{}); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("SearchNearest = %v, want ErrNilGraph", err)
	}
	if got := nilAPI.Providers(); got != nil {
		t.Fatalf("nil Providers = %v, want nil", got)
	}

	api := New((*indexOpsSpy)(nil))
	if err := api.CreateTemporal("Node"); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil CreateTemporal = %v, want ErrNilGraph", err)
	}
	if got := api.Providers(); got != nil {
		t.Fatalf("typed-nil Providers = %v, want nil", got)
	}
}

func TestAPIForwardsMethodsAndErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("index op failed")
	wantProviders := []string{"geo", "text"}
	ops := &indexOpsSpy{
		err:       wantErr,
		providers: wantProviders,
	}
	api := New(ops)
	provider := testIndexProvider{name: "geo"}
	legacyProvider := testLegacyIndexProvider{name: "legacy"}
	query := []float32{0.25, 0.5, 0.75}
	opts := storepkg.QueryOpts{Limit: 5}

	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{name: "CreateProperty", run: func() error { return api.CreateProperty("Node", "name") }},
		{name: "DropProperty", run: func() error { return api.DropProperty("Node", "name") }},
		{name: "CreateHighFrequency", run: func() error { return api.CreateHighFrequency("Node", time.Minute) }},
		{name: "DropHighFrequency", run: func() error { return api.DropHighFrequency("Node") }},
		{name: "CreateTemporal", run: func() error { return api.CreateTemporal("Node") }},
		{name: "DropTemporal", run: func() error { return api.DropTemporal("Node") }},
		{name: "CreateVector", run: func() error { return api.CreateVector("Node", "embedding", 3, storepkg.DistanceEuclidean) }},
		{name: "DropVector", run: func() error { return api.DropVector("Node", "embedding") }},
		{name: "RegisterProvider", run: func() error { return api.RegisterProvider(provider) }},
		{name: "RegisterLegacyProvider", run: func() error { return api.RegisterLegacyProvider(legacyProvider) }},
		{name: "UnregisterProvider", run: func() error { return api.UnregisterProvider("geo") }},
	} {
		if err := tc.run(); !errors.Is(err, wantErr) {
			t.Fatalf("%s = %v, want %v", tc.name, err, wantErr)
		}
	}
	if _, err := api.SearchNearest("Node", "embedding", query, 4, opts); !errors.Is(err, wantErr) {
		t.Fatalf("SearchNearest = %v, want %v", err, wantErr)
	}
	if got := api.Providers(); len(got) != len(wantProviders) || got[0] != wantProviders[0] || got[1] != wantProviders[1] {
		t.Fatalf("Providers = %v, want %v", got, wantProviders)
	}
	providers := api.Providers()
	providers[0] = "mutated"
	if ops.providers[0] != "geo" {
		t.Fatalf("mutating Providers result changed ops providers: %v", ops.providers)
	}

	wantCalls := []string{
		"CreateProperty", "DropProperty", "CreateHighFrequency", "DropHighFrequency",
		"CreateTemporal", "DropTemporal", "CreateVector", "DropVector",
		"RegisterProvider", "RegisterLegacyProvider", "UnregisterProvider",
		"SearchNearest", "Providers", "Providers",
	}
	if len(ops.calls) != len(wantCalls) {
		t.Fatalf("calls = %v, want %v", ops.calls, wantCalls)
	}
	for i, want := range wantCalls {
		if ops.calls[i] != want {
			t.Fatalf("call[%d] = %s, want %s; all calls %v", i, ops.calls[i], want, ops.calls)
		}
	}
	if ops.vectorMetric != storepkg.DistanceEuclidean || ops.vectorDims != 3 {
		t.Fatalf("CreateVector forwarded dims/metric = %d/%d", ops.vectorDims, ops.vectorMetric)
	}
	if len(ops.query) != len(query) || ops.k != 4 || ops.opts != opts {
		t.Fatalf("SearchNearest forwarded query/k/opts = %v/%d/%+v", ops.query, ops.k, ops.opts)
	}
	if ops.providerName != provider.Name() || ops.legacyProviderName != legacyProvider.Name() || ops.unregisterName != "geo" {
		t.Fatalf("provider forwarding = %q/%q/%q", ops.providerName, ops.legacyProviderName, ops.unregisterName)
	}
}

type indexOpsSpy struct {
	err       error
	providers []string

	calls              []string
	vectorDims         int
	vectorMetric       storepkg.DistanceMetric
	query              []float32
	k                  int
	opts               storepkg.QueryOpts
	providerName       string
	legacyProviderName string
	unregisterName     string
}

func (s *indexOpsSpy) record(name string) { s.calls = append(s.calls, name) }

func (s *indexOpsSpy) CreateProperty(label, propertyKey string) error {
	s.record("CreateProperty")
	return s.err
}

func (s *indexOpsSpy) DropProperty(label, propertyKey string) error {
	s.record("DropProperty")
	return s.err
}

func (s *indexOpsSpy) CreateHighFrequency(label string, bucketSize time.Duration) error {
	s.record("CreateHighFrequency")
	return s.err
}

func (s *indexOpsSpy) DropHighFrequency(label string) error {
	s.record("DropHighFrequency")
	return s.err
}

func (s *indexOpsSpy) CreateTemporal(label string) error {
	s.record("CreateTemporal")
	return s.err
}

func (s *indexOpsSpy) DropTemporal(label string) error {
	s.record("DropTemporal")
	return s.err
}

func (s *indexOpsSpy) CreateVector(label, propertyKey string, dims int, metric storepkg.DistanceMetric) error {
	s.record("CreateVector")
	s.vectorDims = dims
	s.vectorMetric = metric
	return s.err
}

func (s *indexOpsSpy) DropVector(label, propertyKey string) error {
	s.record("DropVector")
	return s.err
}

func (s *indexOpsSpy) SearchNearest(label, propertyKey string, query []float32, k int, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.record("SearchNearest")
	s.query = query
	s.k = k
	s.opts = opts
	return nil, s.err
}

func (s *indexOpsSpy) RegisterProvider(p IndexProvider) error {
	s.record("RegisterProvider")
	s.providerName = p.Name()
	return s.err
}

func (s *indexOpsSpy) RegisterLegacyProvider(p LegacyIndexProvider) error {
	s.record("RegisterLegacyProvider")
	s.legacyProviderName = p.Name()
	return s.err
}

func (s *indexOpsSpy) UnregisterProvider(name string) error {
	s.record("UnregisterProvider")
	s.unregisterName = name
	return s.err
}

func (s *indexOpsSpy) Providers() []string {
	s.record("Providers")
	return s.providers
}

type testIndexProvider struct{ name string }

func (p testIndexProvider) Name() string {
	if p.name == "" {
		return "provider"
	}
	return p.name
}

func (testIndexProvider) OnEvent(events.Event) error { return nil }

func (testIndexProvider) Close() error { return nil }

type testLegacyIndexProvider struct{ name string }

func (p testLegacyIndexProvider) Name() string {
	if p.name == "" {
		return "legacy"
	}
	return p.name
}

func (testLegacyIndexProvider) OnEvent(events.Event, GraphReader) {}

func (testLegacyIndexProvider) Close() error { return nil }
