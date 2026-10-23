package store_test

import (
	"errors"
	"math"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestCapabilitiesOf_MatchesCreate (WP-31, sigma row 59): a consumer asks
// CapabilitiesOf instead of probing a Create. So on every in-tree backend each
// index flag must equal "a plain Create of that kind succeeds" — a flag that
// reports true while the Create declines (tiered's relationship property
// index before v4.58.0, its event-label node index) is a lie. Labels: on
// tiered both a reference label and an event label, since the node property
// flag covers every label.
func TestCapabilitiesOf_MatchesCreate(t *testing.T) {
	t.Parallel()
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

// TestCapabilitiesOf_MatchesServedCalls (sigma-tkgd round 10, pattern 5): the
// flags that say what a declared index serves equal what the call does on
// every in-tree backend: after a plain create of the node and relationship
// property index, the ordered range and prefix walks answer without error,
// the bounds are stated (ok) and the relationship class counts answer,
// exactly where the report says so.
func TestCapabilitiesOf_MatchesServedCalls(t *testing.T) {
	open := map[string]func(t *testing.T) storecontract.Store{
		"memory": func(t *testing.T) storecontract.Store { return memory.New() },
		"badger": func(t *testing.T) storecontract.Store {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatal(err)
			}
			return bs
		},
		"sharded": func(t *testing.T) storecontract.Store {
			st, err := sharded.New(sharded.Config{InMemory: true, SlotCount: 2})
			if err != nil {
				t.Fatal(err)
			}
			return st
		},
		"tiered": func(t *testing.T) storecontract.Store {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
			if err != nil {
				t.Fatal(err)
			}
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)
			if _, err := reg.GetOrCreate("Case"); err != nil {
				t.Fatal(err)
			}
			return ts
		},
	}
	for name, mk := range open {
		t.Run(name, func(t *testing.T) {
			s := mk(t)
			t.Cleanup(func() { _ = s.Close() })
			r := storecontract.CapabilitiesOf(s).IndexAcceleration
			if err := s.(storecontract.PropertyIndexCapability).CreatePropertyIndex(1, "k"); err != nil {
				t.Fatal(err)
			}
			if err := s.(storecontract.RelPropertyIndexCapability).CreateRelPropertyIndex(1, "k"); err != nil {
				t.Fatal(err)
			}
			served := func(err error) bool { return err == nil }
			keepNode := func(*types.Node) bool { return true }
			keepRel := func(*types.Relationship) bool { return true }
			nodeWalk := false
			if c, ok := s.(storecontract.NodeOrderedRangeScanCapability); ok {
				p, pok := s.(storecontract.NodePrefixScanCapability)
				nodeWalk = served(c.ForEachNodeByLabelPropertyRangeOrdered(1, "k", math.Inf(-1), math.Inf(1), true, true, true, keepNode)) &&
					pok && served(p.ForEachNodeByLabelPropertyPrefix(1, "k", "", false, keepNode))
			}
			relWalk := false
			if c, ok := s.(storecontract.RelOrderedRangeScanCapability); ok {
				p, pok := s.(storecontract.RelPrefixScanCapability)
				relWalk = served(c.ForEachRelByTypePropertyRangeOrdered(1, "k", math.Inf(-1), math.Inf(1), true, true, false, keepRel)) &&
					pok && served(p.ForEachRelByTypePropertyPrefix(1, "k", "", true, keepRel))
			}
			nodeBounds, relBounds, relClasses := false, false, false
			if c, ok := s.(storecontract.NodePropertyBoundsCapability); ok {
				_, nodeBounds, _ = c.NodePropertyBounds(1, "k")
			}
			if c, ok := s.(storecontract.RelPropertyBoundsCapability); ok {
				_, relBounds, _ = c.RelPropertyBounds(1, "k")
			}
			if c, ok := s.(storecontract.RelPropertyTypeClassCountsCapability); ok {
				_, err := c.RelPropertyTypeClassCounts(1, "k")
				relClasses = err == nil
			}
			for _, c := range []struct {
				flag   string
				report bool
				call   bool
			}{
				{"OrderedPropertyScan", r.OrderedPropertyScan, nodeWalk},
				{"OrderedRelPropertyScan", r.OrderedRelPropertyScan, relWalk},
				{"PropertyBounds", r.PropertyBounds, nodeBounds},
				{"RelPropertyBounds", r.RelPropertyBounds, relBounds},
				{"RelPropertyTypeClassCounts", r.RelPropertyTypeClassCounts, relClasses},
			} {
				if c.report != c.call {
					t.Errorf("%s: CapabilitiesOf reports %v, the call serves %v", c.flag, c.report, c.call)
				}
				if !c.call {
					t.Errorf("%s: not served on %s (every in-tree backend serves it since sigma round 10)", c.flag, name)
				}
			}
		})
	}
}

// TestCapabilitiesOf_BoundsFollowConfiguration: a store whose configuration
// switches the stated bounds off reports so (PropertyBoundsStatedCapability),
// and the flags equal the calls: badger, sharded and tiered with
// DisablePlannerStats state none; badger with PropertyIndexOnDisk reports the
// relationship bounds only (its node bounds door declines on the disk index).
func TestCapabilitiesOf_BoundsFollowConfiguration(t *testing.T) {
	stores := map[string]func(t *testing.T) storecontract.Store{
		"badger-no-stats": func(t *testing.T) storecontract.Store {
			bs, err := badger.New(badger.Config{InMemory: true, DisablePlannerStats: true})
			if err != nil {
				t.Fatal(err)
			}
			return bs
		},
		"sharded-no-stats": func(t *testing.T) storecontract.Store {
			st, err := sharded.New(sharded.Config{InMemory: true, SlotCount: 2, DisablePlannerStats: true})
			if err != nil {
				t.Fatal(err)
			}
			return st
		},
		"tiered-no-stats": func(t *testing.T) storecontract.Store {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1, DisablePlannerStats: true})
			if err != nil {
				t.Fatal(err)
			}
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)
			if _, err := reg.GetOrCreate("Case"); err != nil {
				t.Fatal(err)
			}
			return ts
		},
	}
	disk, err := badger.New(badger.Config{Dir: t.TempDir(), PropertyIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	if r := storecontract.CapabilitiesOf(disk).IndexAcceleration; r.PropertyBounds || !r.RelPropertyBounds {
		t.Fatalf("PropertyIndexOnDisk: flags %v/%v, want false/true", r.PropertyBounds, r.RelPropertyBounds)
	}
	_ = disk.Close()
	for name, mk := range stores {
		t.Run(name, func(t *testing.T) {
			s := mk(t)
			t.Cleanup(func() { _ = s.Close() })
			r := storecontract.CapabilitiesOf(s).IndexAcceleration
			if err := s.(storecontract.PropertyIndexCapability).CreatePropertyIndex(1, "k"); err != nil {
				t.Fatal(err)
			}
			if err := s.(storecontract.RelPropertyIndexCapability).CreateRelPropertyIndex(1, "k"); err != nil {
				t.Fatal(err)
			}
			_, nodeOK, err := s.(storecontract.NodePropertyBoundsCapability).NodePropertyBounds(1, "k")
			if err != nil {
				t.Fatal(err)
			}
			_, relOK, err := s.(storecontract.RelPropertyBoundsCapability).RelPropertyBounds(1, "k")
			if err != nil {
				t.Fatal(err)
			}
			if r.PropertyBounds != nodeOK || r.RelPropertyBounds != relOK {
				t.Fatalf("flags %v/%v, calls %v/%v", r.PropertyBounds, r.RelPropertyBounds, nodeOK, relOK)
			}
			if nodeOK {
				t.Fatalf("node bounds stated under %s", name)
			}
		})
	}
}
