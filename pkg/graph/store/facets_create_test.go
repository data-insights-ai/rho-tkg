package store_test

import (
	"errors"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// TestCapabilitiesOf_MatchesCreate (WP-31, sigma row 59): a consumer asks
// CapabilitiesOf instead of probing a Create. So on every in-tree backend each
// index flag must equal "a plain Create of that kind succeeds" — a flag that
// reports true while the Create declines (tiered's relationship property
// index before v4.58.0, its event-label node index) is a lie. Labels: on
// tiered both a reference label and an event label, since the node property
// flag covers every label.
func TestCapabilitiesOf_MatchesCreate(t *testing.T) {
	type backend struct {
		name string
		open func(t *testing.T) storecontract.Store
		// labels the node-index kinds are created on (token per attempt)
		labels []uint16
	}
	backends := []backend{
		{name: "memory", labels: []uint16{1, 2}, open: func(t *testing.T) storecontract.Store { return memory.New() }},
		{name: "badger", labels: []uint16{1, 2}, open: func(t *testing.T) storecontract.Store {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return bs
		}},
		{name: "sharded", labels: []uint16{1, 2}, open: func(t *testing.T) storecontract.Store {
			st, err := sharded.New(sharded.Config{InMemory: true, SlotCount: 2})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return st
		}},
		{name: "tiered", open: func(t *testing.T) storecontract.Store {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)
			for _, name := range []string{"Case", "Signal"} { // tokens 1 (reference), 2 (event)
				if _, err := reg.GetOrCreate(name); err != nil {
					t.Fatal(err)
				}
			}
			return ts
		}, labels: []uint16{1, 2}},
	}
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			s := b.open(t)
			t.Cleanup(func() { _ = s.Close() })
			r := storecontract.CapabilitiesOf(s).IndexAcceleration
			type attempt struct {
				kind     string
				reported bool
				create   func(label uint16) (bool, error) // (implemented, err)
			}
			attempts := []attempt{
				{"PropertyIndex", r.PropertyIndex, func(l uint16) (bool, error) {
					c, ok := s.(storecontract.PropertyIndexCapability)
					if !ok {
						return false, nil
					}
					return true, c.CreatePropertyIndex(l, "k")
				}},
				{"RelPropertyIndex", r.RelPropertyIndex, func(l uint16) (bool, error) {
					c, ok := s.(storecontract.RelPropertyIndexCapability)
					if !ok {
						return false, nil
					}
					return true, c.CreateRelPropertyIndex(l, "k")
				}},
				{"CompositePropertyIndex", r.CompositePropertyIndex, func(l uint16) (bool, error) {
					c, ok := s.(storecontract.CompositePropertyIndexCapability)
					if !ok {
						return false, nil
					}
					return true, c.CreateCompositePropertyIndex(l, []string{"a", "b"})
				}},
				{"TemporalIndex", r.TemporalIndex, func(l uint16) (bool, error) {
					c, ok := s.(storecontract.TemporalIndexCapability)
					if !ok {
						return false, nil
					}
					return true, c.CreateTemporalIndex(l)
				}},
				{"VectorIndex", r.VectorIndex, func(l uint16) (bool, error) {
					c, ok := s.(storecontract.VectorIndexCapability)
					if !ok {
						return false, nil
					}
					return true, c.CreateVectorIndex(l, "v", 4, storecontract.DistanceCosine)
				}},
				{"VectorIndexOptions", r.VectorIndexOptions, func(l uint16) (bool, error) {
					c, ok := s.(storecontract.VectorIndexOptionsCapability)
					if !ok {
						return false, nil
					}
					return true, c.CreateVectorIndexWithOptions(l, "w", 4, storecontract.DistanceCosine, storecontract.VectorIndexOptions{})
				}},
				{"HighFrequencyIndex", r.HighFrequencyIndex, func(l uint16) (bool, error) {
					c, ok := s.(storecontract.HighFrequencyIndexCapability)
					if !ok {
						return false, nil
					}
					// a label of its own: one temporal index kind per label
					return true, c.CreateHighFrequencyIndex(l+100, time.Hour)
				}},
			}
			for _, a := range attempts {
				for _, label := range b.labels {
					implemented, err := a.create(label)
					succeeds := implemented && err == nil
					if a.reported != succeeds {
						t.Errorf("%s label %d: CapabilitiesOf reports %v, Create implemented=%v err=%v", a.kind, label, a.reported, implemented, err)
					}
					if implemented && err != nil && !errors.Is(err, storecontract.ErrCapabilityNotSupported) && !errors.Is(err, storecontract.ErrRelPropertyIndexUnsupported) {
						t.Errorf("%s label %d: Create failed with %v, want success or a capability sentinel", a.kind, label, err)
					}
				}
			}
		})
	}
}
