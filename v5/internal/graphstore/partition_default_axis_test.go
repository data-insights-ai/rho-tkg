package graphstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func TestPartitionDefaultAxisAtomicInitializeRetainedAndRebinding(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 1}
	s, _, seed := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
	old := ownershipView(t, s, 1)
	binding, err := types.NewDefaultAxisBinding(types.GraphID(d.Graph()), temporal.AxisID{31}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	effects, work, err := InitializeDeclaredPartitionWithDefaultAxis(t.Context(), old, d, binding, nil, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil || work.Records < 3 || effects.Work != work || len(effects.Writes) != 8 {
		t.Fatal(effects.Root, work, len(effects.Writes), err)
	}
	after, err := s.ApplicationUsage()
	if err != nil || before != after {
		t.Fatal("staging published", after, err)
	}
	root, err := effects.Root.AdvanceEffects(sha256.Sum256([]byte("typed default-axis genesis")))
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	at := ownershipInstallRaw(t, s, image, effects.Writes)
	current := ownershipView(t, s, at)
	open := func(v *raftlog.ApplicationView, b types.DefaultAxisBinding, budget OwnershipBudget) (*Catalog, PageWork, error) {
		return OpenPartitionCatalogWithDefaultAxis(t.Context(), v, local, seed.owner, b, Limits{}, GraphLimits{}, budget)
	}
	c, opened, err := open(current, binding, ownershipBudget())
	if err != nil || opened.Records != 8 {
		t.Fatal(opened, err)
	}
	axis, found, err := c.Axis(t.Context(), binding.Axis().Descriptor().ID)
	if err != nil || !found || binding.Check(binding.Graph(), axis, temporal.Limits{}) != nil {
		t.Fatal(axis, found, err)
	}
	if _, _, err := open(old, binding, ownershipBudget()); !errors.Is(err, ErrTopologyUnsupported) {
		t.Fatal("old view aliased current", err)
	}
	absent, err := types.NewDefaultAxisBinding(binding.Graph(), temporal.AxisID{32}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if c, w, err := open(current, absent, ownershipBudget()); c != nil || !errors.Is(err, ErrCorrupt) || w.Records != opened.Records || w.Bytes <= 0 {
		t.Fatal("missing axis/work", c, w, err)
	}
}

func TestPartitionDefaultAxisInitializerDirectRefusalsAndNoEffects(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 1}
	s, _, _ := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
	view := ownershipView(t, s, 1)
	binding, err := types.NewDefaultAxisBinding(types.GraphID(d.Graph()), temporal.AxisID{31}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := types.NewDefaultAxisBinding(types.GraphID{99}, temporal.AxisID{31}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	before, err := s.ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		ctx    context.Context
		view   *raftlog.ApplicationView
		b      types.DefaultAxisBinding
		budget OwnershipBudget
		want   error
	}{
		{"nil-view", t.Context(), nil, binding, ownershipBudget(), ErrInvalid},
		{"nil-context", nil, view, binding, ownershipBudget(), ErrInvalid},
		{"cancelled", cancelled, view, binding, ownershipBudget(), context.Canceled},
		{"wrong-graph", t.Context(), view, wrong, ownershipBudget(), types.ErrDefaultAxisMismatch},
		{"missing-binding", t.Context(), view, types.DefaultAxisBinding{}, ownershipBudget(), types.ErrInvalidGraphIdentity},
		{"source-row-exhaustion", t.Context(), view, binding, OwnershipBudget{1, 1 << 20, 1 << 20}, ErrResourceLimit},
		{"output-headroom", t.Context(), view, binding, OwnershipBudget{512, 1 << 20, 1}, ErrResourceLimit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, w, err := InitializeDeclaredPartitionWithDefaultAxis(tc.ctx, tc.view, d, tc.b, nil, Limits{}, GraphLimits{}, tc.budget)
			if !errors.Is(err, tc.want) || len(e.Writes) != 0 || e.Root != (Root{}) || e.Base.Index != 0 {
				t.Fatal(e, w, err)
			}
			if tc.name == "source-row-exhaustion" && w.Records != 1 {
				t.Fatal(w)
			}
			after, err := s.ApplicationUsage()
			if err != nil || after != before {
				t.Fatal("refusal changed ledgers", after, err)
			}
		})
	}
	if _, err := view.Root(); err != nil {
		t.Fatal("refusal closed borrow", err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := InitializeDeclaredPartitionWithDefaultAxis(t.Context(), view, d, binding, nil, Limits{}, GraphLimits{}, ownershipBudget()); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal(err)
	}
}

func TestPartitionDefaultAxisOpenerSameViewCompleteDescriptorAndBudget(t *testing.T) {
	for _, kind := range []string{"nil-view", "nil-context", "closed-view", "wrong-graph", "absent-axis", "changed-descriptor", "long-descriptor", "source-boundary", "output-boundary"} {
		t.Run(kind, func(t *testing.T) {
			d := partitionInitDeclaration(t)
			local := Namespace{d.Graph(), 1}
			s, _, seed := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
			view := ownershipView(t, s, 1)
			binding, err := types.NewDefaultAxisBinding(types.GraphID(d.Graph()), temporal.AxisID{31}, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			effects, _, err := InitializeDeclaredPartitionWithDefaultAxis(t.Context(), view, d, binding, nil, Limits{}, GraphLimits{}, ownershipBudget())
			if err != nil {
				t.Fatal(err)
			}
			root, err := effects.Root.AdvanceEffects(sha256.Sum256([]byte("default-axis")))
			if err != nil {
				t.Fatal(err)
			}
			image, err := EncodeRoot(root)
			if err != nil {
				t.Fatal(err)
			}
			at := ownershipInstallRaw(t, s, image, effects.Writes)
			old := ownershipView(t, s, at)
			current := old
			ctx := t.Context()
			b := binding
			budget := ownershipBudget()
			want := ErrInvalid
			switch kind {
			case "nil-view":
				current = nil
			case "nil-context":
				ctx = nil
			case "closed-view":
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
				want = raftlog.ErrClosed
			case "wrong-graph":
				b, err = types.NewDefaultAxisBinding(types.GraphID{99}, temporal.AxisID{31}, temporal.Limits{})
				want = types.ErrDefaultAxisMismatch
			case "absent-axis":
				b, err = types.NewDefaultAxisBinding(binding.Graph(), temporal.AxisID{32}, temporal.Limits{})
				want = ErrCorrupt
			case "changed-descriptor", "long-descriptor":
				desc := binding.Axis().Descriptor()
				desc.Reference = "another-origin:v1"
				if kind == "long-descriptor" {
					desc.Reference = strings.Repeat("x", 4096)
				}
				axis, e := temporal.NewAxis(desc, temporal.Limits{})
				if e != nil {
					t.Fatal(e)
				}
				wire, e := encodeAxis(local, axis, DefaultLimits())
				if e != nil {
					t.Fatal(e)
				}
				at = ownershipInstallRaw(t, s, image, []raftlog.KV{{Key: axisKey(local, desc.ID), Value: wire}})
				current = ownershipView(t, s, at)
				want = ErrCorrupt
				if kind == "long-descriptor" {
					want = ErrResourceLimit
				}
			case "source-boundary":
				budget.SourceRows = 7
				want = ErrResourceLimit
			case "output-boundary":
				budget.OutputBytes = 256 + axisVariableBytes(binding.Axis())
				want = ErrResourceLimit
			}
			if err != nil {
				t.Fatal(err)
			}
			c, work, err := OpenPartitionCatalogWithDefaultAxis(ctx, current, local, seed.owner, b, Limits{}, GraphLimits{}, budget)
			if c != nil || !errors.Is(err, want) {
				t.Fatal(c, work, err)
			}
			if kind == "changed-descriptor" || kind == "absent-axis" || kind == "long-descriptor" {
				if work.Records != 8 || work.Bytes <= 0 {
					t.Fatal("attempted read not charged", work)
				}
			}
			if kind == "source-boundary" {
				if work.Records != 7 {
					t.Fatal(work)
				}
				budget.SourceRows++
				if c, _, err := OpenPartitionCatalogWithDefaultAxis(t.Context(), current, local, seed.owner, b, Limits{}, GraphLimits{}, budget); c == nil || err != nil {
					t.Fatal("exact row relief", err)
				}
			}
			if kind == "changed-descriptor" {
				if c, _, err := OpenPartitionCatalogWithDefaultAxis(t.Context(), old, local, seed.owner, binding, Limits{}, GraphLimits{}, ownershipBudget()); c == nil || err != nil {
					t.Fatal("retained descriptor changed", err)
				}
			}
			if kind != "closed-view" {
				if _, err := old.Root(); err != nil {
					t.Fatal("refusal consumed borrow", err)
				}
			}
		})
	}
}

func TestPartitionDefaultAxisOpenerExactByteAndOutputCliffs(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 1}
	s, _, seed := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
	view := ownershipView(t, s, 1)
	binding, err := types.NewDefaultAxisBinding(types.GraphID(d.Graph()), temporal.AxisID{31}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	effects, _, err := InitializeDeclaredPartitionWithDefaultAxis(t.Context(), view, d, binding, nil, Limits{}, GraphLimits{}, ownershipBudget())
	if err != nil {
		t.Fatal(err)
	}
	root, err := effects.Root.AdvanceEffects(sha256.Sum256([]byte("default-axis")))
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	current := ownershipView(t, s, ownershipInstallRaw(t, s, image, effects.Writes))
	open := func(budget OwnershipBudget) (*Catalog, PageWork, error) {
		return OpenPartitionCatalogWithDefaultAxis(t.Context(), current, local, seed.owner, binding, Limits{}, GraphLimits{}, budget)
	}
	c, used, err := open(ownershipBudget())
	if err != nil || c == nil {
		t.Fatal(used, err)
	}
	minimumOutput := 256 + axisVariableBytes(binding.Axis()) + ownershipMetadataBytes + fullStageMetadataBytes
	for _, kind := range []string{"bytes", "output"} {
		for _, delta := range []int{-1, 0, 1} {
			b := ownershipBudget()
			if kind == "bytes" {
				b.SourceBytes = used.Bytes + delta
			} else {
				b.OutputBytes = minimumOutput + delta
			}
			got, work, err := open(b)
			if delta < 0 {
				if got != nil || !errors.Is(err, ErrResourceLimit) || work.Bytes == 0 {
					t.Fatal(kind, delta, work, err)
				}
			} else if got == nil || err != nil || work != used {
				t.Fatal(kind, delta, work, used, err)
			}
		}
	}
}

func TestPartitionDefaultAxisTightDescriptorPolicyPreservesResourceCause(t *testing.T) {
	d := partitionInitDeclaration(t)
	local := Namespace{d.Graph(), 1}
	s, _, seed := ownershipStore(t, vfs.NewMem(), local, d, [16]byte{9})
	view := ownershipView(t, s, 1)
	binding, err := types.NewDefaultAxisBinding(types.GraphID(d.Graph()), temporal.AxisID{31}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	narrow := DefaultLimits()
	narrow.Temporal.MaxDescriptorBytes = 77
	effects, work, err := InitializeDeclaredPartitionWithDefaultAxis(t.Context(), view, d, binding, nil, narrow, GraphLimits{}, ownershipBudget())
	if !errors.Is(err, ErrResourceLimit) || !errors.Is(err, temporal.ErrResourceLimit) || len(effects.Writes) != 0 || work != (PageWork{}) {
		t.Fatal(effects, work, err)
	}
	catalog, work, err := OpenPartitionCatalogWithDefaultAxis(t.Context(), view, local, seed.owner, binding, narrow, GraphLimits{}, ownershipBudget())
	if !errors.Is(err, ErrResourceLimit) || !errors.Is(err, temporal.ErrResourceLimit) || catalog != nil || work != (PageWork{}) {
		t.Fatal(catalog, work, err)
	}
	if _, err := view.Root(); err != nil {
		t.Fatal("refusal consumed borrow", err)
	}
}
