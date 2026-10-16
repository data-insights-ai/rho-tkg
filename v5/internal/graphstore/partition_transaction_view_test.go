package graphstore

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func transactionViewFixture(t *testing.T) ([]*raftlog.ApplicationView, OwnershipDeclaration, types.DefaultAxisBinding, [32]byte, []*raftlog.Store) {
	t.Helper()
	d, err := NewOwnershipDeclaration(graphstate.GraphID{1}, 9, []PartitionOwnership{{Partition: 3, OwnershipEpoch: 2, Group: [16]byte{3}}, {Partition: 8, OwnershipEpoch: 3, Group: [16]byte{8}}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := types.NewDefaultAxisBinding(types.GraphID(d.Graph()), temporal.AxisID{31}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := [32]byte{1}
	routing, err := NewPartitionRouting(d, 1, 0, []BucketOwnership{{Kind: IdentityRoutingBucket, Partition: 3, Epoch: 1}, {Kind: UniqueRoutingBucket, Partition: 3, Epoch: 1}, {Kind: AxisRoutingBucket, Partition: 3, Epoch: 1}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	definition, err := EncodePartitionRoutingBinding(routing, d, binding, cfg, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	sources := make([]*raftlog.ApplicationView, d.Len())
	stores := make([]*raftlog.Store, d.Len())
	for index, entry := range d.entries {
		n := Namespace{d.Graph(), entry.Partition}
		config := ownershipConfig(vfs.NewMem(), n, entry.Group, 1)
		config.Controls = raftlog.ApplicationControlConfig{Version: 1}
		config.Transfer.Contract, err = raftlog.ApplicationContractForPolicyWithControls(config.Application, config.Controls)
		if err != nil {
			t.Fatal(err)
		}
		store, err := raftlog.Open(config)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.Close(); err != nil {
				t.Error(err)
			}
		})
		stores[index] = store
		seedRoot, err := NewOwnershipRoot(n, d)
		if err != nil {
			t.Fatal(err)
		}
		seedImage, err := EncodeRoot(seedRoot)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Initialize([]uint64{1, 2, 3}, seedImage); err != nil {
			t.Fatal(err)
		}
		seed := ownershipView(t, store, 1)
		effects, _, err := InitializeDeclaredPartitionWithDefaultAxis(t.Context(), seed, d, binding, []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}, {Name: "p", Owner: graphstate.Relationship, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality}}, Limits{}, GraphLimits{}, ownershipBudget())
		if err != nil {
			t.Fatal(err)
		}
		root, err := effects.Root.AdvanceEffects([32]byte{byte(entry.Partition)})
		if err != nil {
			t.Fatal(err)
		}
		image, err := EncodeRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		ownershipInstallRaw(t, store, image, effects.Writes)
		rows := []raftlog.KV{{Key: RoutingDefinitionKey(n), Value: definition}}
		for j, owner := range d.entries {
			first := uint64(1 + 16*j)
			publication := RangePublication{Graph: d.Graph(), RangeID: first, First: first, Last: first + 15, InitialPartition: owner.Partition, InitialEpoch: owner.OwnershipEpoch, Configuration: cfg, SourcePartition: 3, SourceIndex: 1}
			pub, err := EncodeRangePublication(publication)
			if err != nil {
				t.Fatal(err)
			}
			placement, err := EncodeRangeOwnership(RangeOwnership{Graph: d.Graph(), RangeID: first, Partition: owner.Partition, Epoch: owner.OwnershipEpoch, Configuration: cfg})
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, raftlog.KV{Key: RangePublicationKey(n, first), Value: pub}, raftlog.KV{Key: RangeOwnershipKey(n, first), Value: placement}, raftlog.KV{Key: RangeEndKey(n, first+15), Value: pub})
		}
		slices.SortFunc(rows, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
		at := ownershipInstallRaw(t, store, image, rows)
		sources[index] = ownershipView(t, store, at)
	}
	return sources, d, binding, cfg, stores
}

// GUARD: incomplete, duplicate, cancelled and foreign source sets never confer
// complete-source authority; every refusal preserves all borrowed handles.
func TestPartitionTransactionViewDirectInputBudgetAndLifetimeRefusals(t *testing.T) {
	sources, d, b, cfg, _ := transactionViewFixture(t)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name    string
		ctx     context.Context
		sources []*raftlog.ApplicationView
		cfg     [32]byte
		budget  OwnershipBudget
		output  int
		want    error
	}{
		{"nil-context", nil, sources, cfg, transactionTestBudget(), 16 << 20, ErrInvalid},
		{"nil-source", t.Context(), []*raftlog.ApplicationView{sources[0], nil}, cfg, transactionTestBudget(), 16 << 20, ErrInvalid},
		{"missing-source", t.Context(), sources[:1], cfg, transactionTestBudget(), 16 << 20, ErrInvalid},
		{"duplicate-source", t.Context(), []*raftlog.ApplicationView{sources[0], sources[0]}, cfg, transactionTestBudget(), 16 << 20, ErrInvalid},
		{"cancelled", cancelled, sources, cfg, transactionTestBudget(), 16 << 20, context.Canceled},
		{"missing-configuration", t.Context(), sources, [32]byte{}, transactionTestBudget(), 16 << 20, ErrInvalid},
		{"rows", t.Context(), sources, cfg, OwnershipBudget{1, 16 << 20, 16 << 20}, 16 << 20, ErrResourceLimit},
		{"bytes", t.Context(), sources, cfg, OwnershipBudget{4096, 1, 16 << 20}, 16 << 20, ErrResourceLimit},
		{"output", t.Context(), sources, cfg, transactionTestBudget(), 0, ErrResourceLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := graphstate.NewOutputBudget(tc.output)
			if err != nil {
				t.Fatal(err)
			}
			v, _, err := OpenPartitionTransactionView(tc.ctx, tc.sources, d, b, tc.cfg, Limits{}, GraphLimits{}, tc.budget, output)
			if v != nil || !errors.Is(err, tc.want) {
				t.Fatal(v, err)
			}
			for _, source := range sources {
				if _, err := source.Root(); err != nil {
					t.Fatal("refusal closed borrow", err)
				}
			}
		})
	}
	var nilView *PartitionTransactionView
	if nilView.Graph() != (graphstate.GraphID{}) || nilView.Identity() != (graphstate.ViewID{}) || nilView.Work() != (PageWork{}) || !errors.Is(nilView.Close(), ErrInvalid) {
		t.Fatal("nil fixed accessor")
	}
	for _, call := range []func() error{func() error { _, err := nilView.Entity(t.Context(), 1); return err }, func() error { _, err := nilView.Life(t.Context(), 1, 1); return err }, func() error { _, err := nilView.Property(t.Context(), graphstate.Node, "p"); return err }, func() error { _, err := nilView.Value(t.Context(), 1); return err }, func() error { _, err := nilView.ValueIdentity(t.Context(), graphstate.I64(1)); return err }, func() error {
		_, err := nilView.ComponentPage(t.Context(), graphstate.ComponentQuery{}, 0, raftlog.ReadBudget{})
		return err
	}, func() error {
		_, err := nilView.ComponentKeys(t.Context(), graphstate.KeyPredicate{}, 0, raftlog.ReadBudget{})
		return err
	}, func() error {
		_, err := nilView.UniqueCandidates(t.Context(), graphstate.UniquePredicate{}, 0, raftlog.ReadBudget{})
		return err
	}, func() error {
		_, err := nilView.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{}, 0, raftlog.ReadBudget{})
		return err
	}} {
		if err := call(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, b, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	all, err := temporal.All(b.Axis())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Entity(t.Context(), 0); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := v.Entity(t.Context(), 33); !errors.Is(err, ErrRoutingUnknown) {
		t.Fatal("unpublished range proved absence", err)
	}
	if _, err := v.ValueIdentity(t.Context(), graphstate.Scalar{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := PlanPartitionTransaction(t.Context(), v, nil, state.Revision{}); !errors.Is(err, state.ErrInvalidRevision) {
		t.Fatal(err)
	}
	if _, _, err := PlanPartitionTransaction(t.Context(), nil, nil, state.Revision{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	for _, call := range []func() error{func() error { _, err := v.Entity(t.Context(), 1); return err }, func() error { _, err := v.Life(t.Context(), 1, 1); return err }, func() error { _, err := v.Property(t.Context(), graphstate.Node, "p"); return err }, func() error { _, err := v.Value(t.Context(), 1); return err }, func() error { _, err := v.ValueIdentity(t.Context(), graphstate.I64(1)); return err }, func() error {
		_, err := v.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}, Window: all}, 0, raftlog.ReadBudget{Rows: 100, Bytes: 1 << 20})
		return err
	}, func() error {
		_, err := v.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 1}, 0, raftlog.ReadBudget{Rows: 100, Bytes: 1 << 20})
		return err
	}, func() error {
		_, err := v.UniqueCandidates(t.Context(), graphstate.UniquePredicate{}, 0, raftlog.ReadBudget{Rows: 100, Bytes: 1 << 20})
		return err
	}, func() error {
		_, err := v.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{Endpoint: 1, Window: all}, 0, raftlog.ReadBudget{Rows: 100, Bytes: 1 << 20})
		return err
	}} {
		if err := call(); !errors.Is(err, ErrClosed) {
			t.Fatal(err)
		}
	}
	if v.Graph() != (graphstate.GraphID{}) || v.Identity() != (graphstate.ViewID{}) {
		t.Fatal("closed accessor retained identity")
	}
}

// A local scope cannot answer a routed remote absence even when the
// complete declared source set was supplied. Guards elsewhere retain local refusal.
func TestPartitionTransactionViewMalformedCurrentRoutingAuthorityNeverFallsBackToAbsence(t *testing.T) {
	for _, kind := range []string{"fenced", "regressed-epoch", "conflicting-range", "conflicting-table"} {
		t.Run(kind, func(t *testing.T) {
			sources, d, binding, cfg, stores := transactionViewFixture(t)
			n := Namespace{d.Graph(), 8}
			base, err := sources[1].Root()
			if err != nil {
				t.Fatal(err)
			}
			var rows []raftlog.KV
			want := ErrStaleOwner
			if kind == "conflicting-table" {
				routing, err := NewPartitionRouting(d, 1, 0, []BucketOwnership{{Kind: IdentityRoutingBucket, Partition: 8, Epoch: 1}, {Kind: UniqueRoutingBucket, Partition: 3, Epoch: 1}, {Kind: AxisRoutingBucket, Partition: 3, Epoch: 1}}, Limits{})
				if err != nil {
					t.Fatal(err)
				}
				wire, err := EncodePartitionRoutingBinding(routing, d, binding, cfg, Limits{})
				if err != nil {
					t.Fatal(err)
				}
				rows = []raftlog.KV{{Key: RoutingDefinitionKey(n), Value: wire}}
				want = ErrRebinding
			} else {
				owner := RangeOwnership{Graph: d.Graph(), RangeID: 17, Partition: 8, Epoch: 3, Configuration: cfg}
				switch kind {
				case "fenced":
					owner.Fenced = true
				case "regressed-epoch":
					owner.Epoch = 2
					want = ErrCorrupt
				case "conflicting-range":
					owner.Partition = 3
					want = ErrRebinding
				}
				wire, err := EncodeRangeOwnership(owner)
				if err != nil {
					t.Fatal(err)
				}
				rows = []raftlog.KV{{Key: RangeOwnershipKey(n, 17), Value: wire}}
			}
			changed := ownershipView(t, stores[1], ownershipInstallRaw(t, stores[1], base.Image, rows))
			fresh := []*raftlog.ApplicationView{sources[0], changed}
			view, _, err := OpenPartitionTransactionView(t.Context(), fresh, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if kind == "conflicting-table" {
				if view != nil || !errors.Is(err, want) {
					t.Fatal(view, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer view.Close()
			read, err := view.Entity(t.Context(), 17)
			if !errors.Is(err, want) || read.Found || read.View != (graphstate.ViewID{}) {
				t.Fatal("malformed current authority fell through to local/global absence", read, err)
			}
			retained, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			defer retained.Close()
			old, err := retained.Entity(t.Context(), 17)
			if err != nil || old.Found {
				t.Fatal("current authority corruption rewrote retained routing", old, err)
			}
		})
	}
}

func TestPartitionTransactionViewRoutesRemoteNegativeEntityAndValueReads(t *testing.T) {
	sources, d, b, cfg, _ := transactionViewFixture(t)
	output, err := graphstate.NewOutputBudget(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, b, cfg, Limits{}, GraphLimits{}, ownershipBudget(), output)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	entity, err := v.Entity(t.Context(), 31)
	if err != nil || entity.Found || entity.View != v.Identity() {
		t.Fatalf("remote entity absence needs actual owner: %+v %v", entity, err)
	}
	value, err := v.Value(t.Context(), 32)
	if err != nil || value.Found || value.View != v.Identity() {
		t.Fatalf("remote value absence needs actual owner: %+v %v", value, err)
	}
}
