package graphstore

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func transactionTestBudget() OwnershipBudget {
	return OwnershipBudget{SourceRows: 4096, SourceBytes: 16 << 20, OutputBytes: 16 << 20}
}
func transactionTestOutput(t *testing.T) *graphstate.OutputBudget {
	t.Helper()
	b, err := graphstate.NewOutputBudget(16 << 20)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func transactionInstallControls(t *testing.T, s *raftlog.Store, image []byte, controls []raftlog.ApplicationControlPut) uint64 {
	t.Helper()
	index, previous, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	next := index + 1
	if err := s.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(next)}, Entries: []*pb.Entry{{Index: new(next), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("private checked prepare controls")}}}); err != nil {
		t.Fatal(err)
	}
	batch := raftlog.ApplicationBatch{BaseGeneration: s.ApplicationGeneration(), BaseIndex: index, BaseImageHash: sha256.Sum256(previous), Image: image, ControlPuts: controls}
	if err := s.InstallApplication(next, batch); err != nil {
		t.Fatal(err)
	}
	return next
}
func transactionInstallTemplate(t *testing.T, sources []*raftlog.ApplicationView, stores []*raftlog.Store, template *PartitionTransactionTemplate, revision state.Revision) []*raftlog.ApplicationView {
	t.Helper()
	current := make([]*raftlog.ApplicationView, len(stores))
	for n, part := range template.participants {
		kernel, err := template.ParticipantKernel(part.partition, [16]byte{byte(n + 1)}, transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		effects, _, err := StagePreparedPartitionKernel(t.Context(), sources[n], kernel, revision, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		root, err := effects.Root.AdvanceEffects(kernel.Digest())
		if err != nil {
			t.Fatal(err)
		}
		image, err := EncodeRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		current[n] = ownershipView(t, stores[n], ownershipInstallRaw(t, stores[n], image, effects.Writes))
	}
	return current
}
func TestPartitionTransactionStageCrossEdgeBothEndpointEffectsAndRetainedView(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{
		{Kind: graphstate.CreateNode, Owner: 1, Life: 101, Scope: all},
		{Kind: graphstate.CreateNode, Owner: 17, Life: 202, Scope: all},
		{Kind: graphstate.CreateRelationship, Owner: 2, Life: 303, Scope: all, Record: graphstate.EntityRecord{Type: "CROSS", Source: 1, Target: 17, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 101, TargetLife: 202}},
		{Kind: graphstate.Set, Owner: 17, Life: 202, Scope: all, Name: "p", Value: graphstate.I64(5), ValueID: 18},
		{Kind: graphstate.Set, Owner: 1, Life: 101, Scope: all, Name: "p", Value: graphstate.I64(5), ValueID: 19},
	}
	provisional, _ := state.NewRevision(100, 9)
	template, work, err := PlanPartitionTransaction(t.Context(), v, ops, provisional)
	if err != nil {
		t.Fatal(work, err)
	}
	participants, err := template.Participants(transactionTestOutput(t))
	if err != nil || !slices.Equal(participants, []uint64{3, 8}) {
		t.Fatal(participants, err)
	}
	deps, err := template.Dependencies(transactionTestOutput(t))
	if err != nil || len(deps) == 0 {
		t.Fatal(deps, err)
	}
	bases, err := template.Bases(transactionTestOutput(t))
	if err != nil || len(bases) != 2 {
		t.Fatal(bases, err)
	}
	if template.Digest() == ([32]byte{}) {
		t.Fatal("missing complete template identity")
	}
	current := make([]*raftlog.ApplicationView, 2)
	decided, _ := state.NewRevision(200, 9)
	for n, partition := range participants {
		kernel, err := template.ParticipantKernel(partition, [16]byte{7}, transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		if kernel.Digest() == ([32]byte{}) || kernel.Digest() == template.Digest() {
			t.Fatal("participant and transaction digests conflated")
		}
		before, err := stores[n].ApplicationUsage()
		if err != nil {
			t.Fatal(err)
		}
		effects, _, err := StagePreparedPartitionKernel(t.Context(), sources[n], kernel, decided, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
		if err != nil {
			t.Fatal(partition, err)
		}
		after, err := stores[n].ApplicationUsage()
		if err != nil || after != before {
			t.Fatal("private staging installed effects", after, err)
		}
		root, err := effects.Root.AdvanceEffects(kernel.Digest())
		if err != nil {
			t.Fatal(err)
		}
		image, err := EncodeRoot(root)
		if err != nil {
			t.Fatal(err)
		}
		at := ownershipInstallRaw(t, stores[n], image, effects.Writes)
		current[n] = ownershipView(t, stores[n], at)
	}
	newView, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer newView.Close()
	if newView.Graph() != d.Graph() || newView.Identity() == v.Identity() || newView.Work().Records == 0 {
		t.Fatal("composite reader identity/work")
	}
	for _, entity := range []struct {
		id   graphstate.EntityID
		life graphstate.LifeID
		kind graphstate.EntityKind
	}{{1, 101, graphstate.Node}, {17, 202, graphstate.Node}, {2, 303, graphstate.Relationship}} {
		read, err := newView.Entity(t.Context(), entity.id)
		if err != nil || !read.Found || read.Record.Kind != entity.kind {
			t.Fatal(read, err)
		}
		life, err := newView.Life(t.Context(), entity.id, entity.life)
		if err != nil || !life.Found || life.Record.Owner != entity.id {
			t.Fatal(life, err)
		}
		page, err := newView.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: entity.id, Kind: graphstate.Presence}, Window: all}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
		if err != nil || !page.Complete || page.Data.Usage().Pieces() != 1 || page.Data.Pieces()[0].Cell().Revision().ID() != 200 {
			t.Fatal(page, err)
		}
		keys, err := newView.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: entity.id}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
		want := 1
		if entity.kind == graphstate.Node {
			want = 2
		}
		if err != nil || !keys.Complete || len(keys.Keys) != want {
			t.Fatal("primary component exact set", keys, err)
		}
	}
	property, err := newView.Property(t.Context(), graphstate.Node, "p")
	if err != nil || !property.Found || property.Record.Type != graphstate.ScalarI64 {
		t.Fatal(property, err)
	}
	for _, endpoint := range []struct {
		id   graphstate.EntityID
		life graphstate.LifeID
	}{{1, 101}, {17, 202}} {
		page, err := newView.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{Endpoint: endpoint.id, Life: endpoint.life, Window: all}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
		if err != nil || !page.Complete || !slices.Equal(page.Entities, []graphstate.EntityID{2}) {
			t.Fatal(endpoint, page, err)
		}
		old, err := v.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{Endpoint: endpoint.id, Life: endpoint.life, Window: all}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
		if err != nil || !old.Complete || len(old.Entities) != 0 {
			t.Fatal("retained posting state became current", old, err)
		}
	}
	for _, source := range newView.views {
		if source.descriptor.canonical.count != 1 || source.descriptor.declared.count != 1 || source.descriptor.own.count != 1 {
			t.Fatal("endpoint posting footprint", source.c.root.namespace, source.descriptor)
		}
	}
	identity, err := newView.ValueIdentity(t.Context(), graphstate.I64(5))
	if err != nil || !identity.Found || identity.ID != 18 {
		t.Fatal("global exact identity and remote ValueID", identity, err)
	}
	for _, id := range []graphstate.ValueID{19, 20} {
		missing, err := newView.Value(t.Context(), id)
		if err != nil || missing.Found {
			t.Fatal("unused alias or phantom value appeared", id, missing, err)
		}
	}
	oldEntity, err := v.Entity(t.Context(), 2)
	if err != nil || oldEntity.Found {
		t.Fatal("retained entity became current", oldEntity, err)
	}
}

func TestPartitionTransactionViewSharedCursorCountAndBytesRefusalPreservesPriorCursor(t *testing.T) {
	for _, kind := range []string{"count", "bytes"} {
		t.Run(kind, func(t *testing.T) {
			sources, d, binding, cfg, stores := transactionViewFixture(t)
			all, err := temporal.All(binding.Axis())
			if err != nil {
				t.Fatal(err)
			}
			v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 1, Scope: all}, {Kind: graphstate.CreateNode, Owner: 17, Life: 1, Scope: all}}
			for _, owner := range []graphstate.EntityID{1, 17} {
				for _, name := range []string{"A", "B"} {
					ops = append(ops, graphstate.Operation{Kind: graphstate.AddLabel, Owner: owner, Life: 1, Scope: all, Name: name})
				}
			}
			revision, _ := state.NewRevision(10, 0)
			template, _, err := PlanPartitionTransaction(t.Context(), v, ops, revision)
			if err != nil {
				t.Fatal(err)
			}
			_ = v.Close()
			current := make([]*raftlog.ApplicationView, 2)
			for n, partition := range []uint64{3, 8} {
				kernel, err := template.ParticipantKernel(partition, [16]byte{1}, transactionTestOutput(t))
				if err != nil {
					t.Fatal(err)
				}
				effects, _, err := StagePreparedPartitionKernel(t.Context(), sources[n], kernel, revision, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
				if err != nil {
					t.Fatal(err)
				}
				root, err := effects.Root.AdvanceEffects(kernel.Digest())
				if err != nil {
					t.Fatal(err)
				}
				image, err := EncodeRoot(root)
				if err != nil {
					t.Fatal(err)
				}
				current[n] = ownershipView(t, stores[n], ownershipInstallRaw(t, stores[n], image, effects.Writes))
			}
			g := DefaultGraphLimits()
			if kind == "count" {
				g.Pages.MaxCursors = 1
			} else {
				g.Pages.MaxCursors = 2
				g.Pages.MaxCursorBytes = 257
			}
			reader, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			predicate := graphstate.KeyPredicate{Owner: 1, Life: 1, Kind: graphstate.Label}
			budget := graphstate.ReadBudget{Rows: 12, Bytes: 1 << 20}
			first, err := reader.ComponentKeys(t.Context(), predicate, 0, budget)
			if err != nil || first.Complete || first.Next == 0 || len(first.Keys) != 1 || first.Keys[0].Name != "A" {
				t.Fatal(first, err)
			}
			second, err := reader.ComponentKeys(t.Context(), graphstate.KeyPredicate{Owner: 17, Life: 1, Kind: graphstate.Label}, 0, budget)
			if !errors.Is(err, ErrResourceLimit) || second.Next != 0 || len(second.Keys) != 0 {
				t.Fatal("source count multiplied continuation allowance", second, err)
			}
			last, err := reader.ComponentKeys(t.Context(), predicate, first.Next, graphstate.ReadBudget{Rows: 512, Bytes: 1 << 20})
			if err != nil || !last.Complete || last.Next != 0 || len(last.Keys) != 1 || last.Keys[0].Name != "B" {
				t.Fatal("refusal consumed prior continuation", last, err)
			}
		})
	}
}

func TestPartitionTransactionViewComponentCursorCountAndBytesAreSharedAcrossSources(t *testing.T) {
	for _, kind := range []string{"count", "bytes"} {
		t.Run(kind, func(t *testing.T) {
			sources, d, binding, cfg, stores := transactionViewFixture(t)
			all, err := temporal.All(binding.Axis())
			if err != nil {
				t.Fatal(err)
			}
			span := func(lo, hi int64) temporal.Scope {
				lower, err := temporal.RationalPosition(binding.Axis(), temporal.RationalInt64(lo))
				if err != nil {
					t.Fatal(err)
				}
				upper, err := temporal.RationalPosition(binding.Axis(), temporal.RationalInt64(hi))
				if err != nil {
					t.Fatal(err)
				}
				a, err := temporal.FiniteBound(lower, true)
				if err != nil {
					t.Fatal(err)
				}
				b, err := temporal.FiniteBound(upper, false)
				if err != nil {
					t.Fatal(err)
				}
				out, err := temporal.Span(binding.Axis(), a, b, temporal.Limits{})
				if err != nil {
					t.Fatal(err)
				}
				return out
			}
			g := DefaultGraphLimits()
			g.Pages.MaxCells = 1
			g.Pages.MaxTailRecords = 1
			v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 1, Scope: all}, {Kind: graphstate.CreateNode, Owner: 17, Life: 1, Scope: all}}
			for _, owner := range []graphstate.EntityID{1, 17} {
				ops = append(ops, graphstate.Operation{Kind: graphstate.AddLabel, Owner: owner, Life: 1, Name: "A", Scope: span(0, 5)}, graphstate.Operation{Kind: graphstate.RemoveLabel, Owner: owner, Life: 1, Name: "A", Scope: span(5, 10)})
			}
			revision, _ := state.NewRevision(10, 0)
			template, _, err := PlanPartitionTransaction(t.Context(), v, ops, revision)
			if err != nil {
				t.Fatal(err)
			}
			_ = v.Close()
			current := transactionInstallTemplate(t, sources, stores, template, revision)
			if kind == "count" {
				g.Pages.MaxCursors = 1
			} else {
				g.Pages.MaxCursors = 2
				g.Pages.MaxCursorBytes = 2048
			}
			reader, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			query := graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: 1, Life: 1, Kind: graphstate.Label, Name: "A"}, Window: all}
			budget := graphstate.ReadBudget{Rows: 512, Bytes: 1 << 20}
			first, err := reader.ComponentPage(t.Context(), query, 0, budget)
			if err != nil || first.Complete || first.Next == 0 || first.Data.Usage().Pieces() != 1 {
				t.Fatal(first, err)
			}
			other := query
			other.Key.Owner = 17
			refused, err := reader.ComponentPage(t.Context(), other, 0, budget)
			if !errors.Is(err, ErrResourceLimit) || refused.Next != 0 {
				t.Fatal("nested page pool multiplied allowance", refused, err)
			}
			last, err := reader.ComponentPage(t.Context(), query, first.Next, budget)
			if err != nil || !last.Complete || last.Next != 0 || last.Data.Usage().Pieces() != 1 || last.Data.Pieces()[0].Cell().Present() {
				t.Fatal("refusal consumed prior component continuation", last, err)
			}
		})
	}
}

func TestPartitionTransactionKernelSequentialCDCBeforeTagsAndLocalDurableRestore(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	provisional, _ := state.NewRevision(100, 77)
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 17, Life: 1, Scope: all}, {Kind: graphstate.Set, Owner: 17, Life: 1, Scope: all, Name: "p", Value: graphstate.I64(1), ValueID: 18}, {Kind: graphstate.Set, Owner: 17, Life: 1, Scope: all, Name: "p", Value: graphstate.I64(2), ValueID: 19}}
	template, _, err := PlanPartitionTransaction(t.Context(), v, ops, provisional)
	if err != nil {
		t.Fatal(err)
	}
	kernel, err := template.ParticipantKernel(8, [16]byte{11}, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := kernel.Binding(transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := kernel.Bytes(transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	inspected, round, err := InspectPreparedPartitionKernel(wire, Limits{}, GraphLimits{}, transactionTestOutput(t))
	if err != nil || !sameKernelBinding(inspected, expected) || round != provisional {
		t.Fatal(inspected, round, err)
	}
	fresh, err := InspectPreparedKernelIdentities(wire, Limits{}, GraphLimits{}, transactionTestOutput(t))
	if err != nil || !slices.Equal(fresh.Entities, []graphstate.EntityID{17}) || !slices.Equal(fresh.Values, []graphstate.ValueID{18, 19}) || len(fresh.Lives) != 1 || fresh.Lives[0].Owner != 17 || fresh.Lives[0].Life != 1 {
		t.Fatal("fresh grant metadata exact set", fresh, err)
	}
	broken := exactCopy(wire)
	broken[len(broken)-1] ^= 1
	if binding, rev, err := InspectPreparedPartitionKernel(broken, Limits{}, GraphLimits{}, transactionTestOutput(t)); !errors.Is(err, ErrCorrupt) || binding.TxID != ([16]byte{}) || rev.ID() != 0 {
		t.Fatal("malformed inspector gave metadata authority", binding, rev, err)
	}
	base, err := sources[1].Root()
	if err != nil {
		t.Fatal(err)
	}
	key, err := PreparedKernelControlKey(expected, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	prepareKey, err := PreparedKernelPrepareControlKey(expected, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	prepareValue := []byte("graphapply fixture: checked Tx11/participant8/locks/template/effect")
	at := transactionInstallControls(t, stores[1], base.Image, []raftlog.ApplicationControlPut{{Key: key, Value: wire}, {Key: prepareKey, Value: prepareValue}})
	proof := PreparedKernelRestoreProof{PrepareKey: prepareKey, PrepareDigest: sha256.Sum256(prepareValue), PrepareIndex: at}
	local := ownershipView(t, stores[1], at)
	// Remote group lifetime is irrelevant after a durable local prepare.
	if err := sources[0].Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	restored, work, err := ReadPreparedPartitionKernel(t.Context(), local, proof, expected, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(work, err)
	}
	decided, _ := state.NewRevision(300, 77)
	effects, _, err := StagePreparedPartitionKernel(t.Context(), local, restored, decided, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	if effects.Base.Index != at {
		t.Fatal("protocol advance rejected or stale installation base", effects.Base.Index, at)
	}
	if len(effects.Groups) != 3 {
		t.Fatal("ordered CDC omitted", len(effects.Groups))
	}
	first := effects.Groups[1].Changes[0]
	second := effects.Groups[2].Changes[0]
	if first.Before().Revision().ID() != 0 || first.After().Revision().ID() != 300 || second.Before().Revision().ID() != 300 || second.After().Revision().ID() != 300 || second.Before().Value() == second.After().Value() {
		t.Fatal("new/retained sequential CDC tags", first, second)
	}
	for _, patch := range effects.Delta.Patches {
		for _, piece := range patch.State.Pieces() {
			if piece.Cell().Revision().ID() != 300 || piece.Cell().Revision().Provenance() != 77 {
				t.Fatal("installed state revision tags", piece)
			}
		}
	}
	wrong := expected
	wrong.TxID[0]++
	if k, _, err := ReadPreparedPartitionKernel(t.Context(), local, proof, wrong, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t)); k != nil || !errors.Is(err, ErrRebinding) {
		t.Fatal("inspection bytes granted unrelated transaction authority", k, err)
	}
	if e, _, err := StagePreparedPartitionKernel(t.Context(), local, restored, state.Revision{}, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t)); len(e.Writes) != 0 || !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid revision returned partial effects", e, err)
	}
}

func TestPartitionTransactionKernelRetainedNodeRelationshipBeforeAndCheckpointCellsNeverRetag(t *testing.T) {
	for _, kind := range []graphstate.EntityKind{graphstate.Node, graphstate.Relationship} {
		t.Run(map[graphstate.EntityKind]string{graphstate.Node: "node", graphstate.Relationship: "relationship"}[kind], func(t *testing.T) {
			sources, d, binding, cfg, stores := transactionViewFixture(t)
			all, err := temporal.All(binding.Axis())
			if err != nil {
				t.Fatal(err)
			}
			v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			owner, life := graphstate.EntityID(17), graphstate.LifeID(202)
			if kind == graphstate.Relationship {
				owner, life = 2, 303
			}
			initial := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 101, Scope: all}, {Kind: graphstate.CreateNode, Owner: 17, Life: 202, Scope: all}, {Kind: graphstate.CreateRelationship, Owner: 2, Life: 303, Scope: all, Record: graphstate.EntityRecord{Type: "CROSS", Source: 1, Target: 17, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 101, TargetLife: 202}}, {Kind: graphstate.Set, Owner: owner, Life: life, Scope: all, Name: "p", Value: graphstate.I64(1), ValueID: 18}}
			original, _ := state.NewRevision(100, 9)
			template, _, err := PlanPartitionTransaction(t.Context(), v, initial, original)
			if err != nil {
				t.Fatal(err)
			}
			_ = v.Close()
			current := transactionInstallTemplate(t, sources, stores, template, original)
			g := DefaultGraphLimits()
			g.Pages.MaxTailRecords = 1
			old, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			lo, err := temporal.RationalPosition(binding.Axis(), temporal.RationalInt64(0))
			if err != nil {
				t.Fatal(err)
			}
			hi, err := temporal.RationalPosition(binding.Axis(), temporal.RationalInt64(5))
			if err != nil {
				t.Fatal(err)
			}
			lower, err := temporal.FiniteBound(lo, true)
			if err != nil {
				t.Fatal(err)
			}
			upper, err := temporal.FiniteBound(hi, false)
			if err != nil {
				t.Fatal(err)
			}
			window, err := temporal.Span(binding.Axis(), lower, upper, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			provisional, _ := state.NewRevision(200, 77)
			next, _, err := PlanPartitionTransaction(t.Context(), old, []graphstate.Operation{{Kind: graphstate.Set, Owner: owner, Life: life, Scope: window, Name: "p", Value: graphstate.I64(2), ValueID: 19}}, provisional)
			if err != nil {
				t.Fatal(err)
			}
			decided, _ := state.NewRevision(300, 77)
			partition := uint64(8)
			if kind == graphstate.Relationship {
				partition = 3
			}
			kernel, err := next.ParticipantKernel(partition, [16]byte{11}, transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			index := 1
			if partition == 3 {
				index = 0
			}
			effects, _, err := StagePreparedPartitionKernel(t.Context(), current[index], kernel, decided, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			change := effects.Groups[0].Changes[0]
			if change.Before().Revision() != original || change.After().Revision() != decided {
				t.Fatal("retained before image rewritten", change)
			}
			installed := transactionInstallTemplate(t, current, stores, next, decided)
			reader, _, err := OpenPartitionTransactionView(t.Context(), installed, d, binding, cfg, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			query := graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: owner, Life: life, Kind: graphstate.ScalarProperty, Name: "p"}, Window: all}
			page, err := reader.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 512, Bytes: 1 << 20})
			if err != nil || !page.Complete {
				t.Fatal(page, err)
			}
			pieces := page.Data.Pieces()
			if len(pieces) != 3 || pieces[0].Cell().Revision() != original || pieces[1].Cell().Revision() != decided || pieces[2].Cell().Revision() != original {
				t.Fatal("retained checkpoint cells numerically retagged", pieces)
			}
			historical, err := old.ComponentPage(t.Context(), query, 0, graphstate.ReadBudget{Rows: 512, Bytes: 1 << 20})
			if err != nil || historical.Data.Usage().Pieces() != 1 || historical.Data.Pieces()[0].Cell().Revision() != original {
				t.Fatal("retained two-phase result changed", historical, err)
			}
			if _, _, err := PlanPartitionTransaction(t.Context(), old, []graphstate.Operation{{Kind: graphstate.Set, Owner: owner, Life: life, Scope: window, Name: "p", Value: graphstate.I64(3), ValueID: 20}}, original); !errors.Is(err, state.ErrInvalidRevision) {
				t.Fatal("equal provisional revision suppressed imported state conflict", err)
			}
		})
	}
}

// GUARD: inspection or ordinary graph KV never becomes a durable prepare.
func TestPartitionTransactionKernelRequiresExactCoInstalledImmutableControls(t *testing.T) {
	for _, kind := range []string{"inspection-only", "ordinary-kv", "prepare-missing", "kernel-missing", "split-entry", "wrong-type", "wrong-index", "wrong-digest", "wrong-binding", "corrupt-kernel", "changed-root", "budget"} {
		t.Run(kind, func(t *testing.T) {
			sources, d, binding, cfg, stores := transactionViewFixture(t)
			all, err := temporal.All(binding.Axis())
			if err != nil {
				t.Fatal(err)
			}
			v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			revision, _ := state.NewRevision(100, 0)
			template, _, err := PlanPartitionTransaction(t.Context(), v, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 17, Life: 1, Scope: all}}, revision)
			if err != nil {
				t.Fatal(err)
			}
			kernel, err := template.ParticipantKernel(8, [16]byte{11}, transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			expected, err := kernel.Binding(transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			wire, err := kernel.Bytes(transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			kernelKey, err := PreparedKernelControlKey(expected, transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			prepareKey, err := PreparedKernelPrepareControlKey(expected, transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			value := []byte("typed-prepare fixture checked by graphapply")
			base, err := sources[1].Root()
			if err != nil {
				t.Fatal(err)
			}
			at := base.Index
			want := ErrCorrupt
			prepareIndex := base.Index + 1
			switch kind {
			case "inspection-only":
			case "ordinary-kv":
				at = ownershipInstallRaw(t, stores[1], base.Image, []raftlog.KV{{Key: kernelKey, Value: wire}, {Key: prepareKey, Value: value}})
			case "prepare-missing":
				at = transactionInstallControls(t, stores[1], base.Image, []raftlog.ApplicationControlPut{{Key: kernelKey, Value: wire}})
			case "kernel-missing":
				at = transactionInstallControls(t, stores[1], base.Image, []raftlog.ApplicationControlPut{{Key: prepareKey, Value: value}})
			case "split-entry":
				prepareIndex = transactionInstallControls(t, stores[1], base.Image, []raftlog.ApplicationControlPut{{Key: prepareKey, Value: value}})
				at = transactionInstallControls(t, stores[1], base.Image, []raftlog.ApplicationControlPut{{Key: kernelKey, Value: wire}})
				want = ErrRebinding
			default:
				if kind == "corrupt-kernel" {
					wire[len(wire)-1] ^= 1
				}
				at = transactionInstallControls(t, stores[1], base.Image, []raftlog.ApplicationControlPut{{Key: kernelKey, Value: wire}, {Key: prepareKey, Value: value}})
			}
			proof := PreparedKernelRestoreProof{PrepareKey: prepareKey, PrepareDigest: sha256.Sum256(value), PrepareIndex: prepareIndex}
			output := transactionTestOutput(t)
			switch kind {
			case "wrong-type":
				proof.PrepareKey = exactCopy(proof.PrepareKey)
				proof.PrepareKey[2] = 'D'
				want = ErrRebinding
			case "wrong-index":
				proof.PrepareIndex++
				want = ErrRebinding
			case "wrong-digest":
				proof.PrepareDigest[0] ^= 1
				want = ErrRebinding
			case "wrong-binding":
				expected.Configuration[0]++
				want = ErrRebinding
			case "changed-root":
				root, err := DecodeRoot(base.Image)
				if err != nil {
					t.Fatal(err)
				}
				root, err = root.AdvanceEffects([32]byte{99})
				if err != nil {
					t.Fatal(err)
				}
				image, err := EncodeRoot(root)
				if err != nil {
					t.Fatal(err)
				}
				at = ownershipInstallRaw(t, stores[1], image, nil)
				want = ErrPatchConflict
			case "budget":
				output, err = graphstate.NewOutputBudget(0)
				if err != nil {
					t.Fatal(err)
				}
				want = ErrResourceLimit
			}
			local := ownershipView(t, stores[1], at)
			before, err := stores[1].ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			restored, _, err := ReadPreparedPartitionKernel(t.Context(), local, proof, expected, Limits{}, GraphLimits{}, transactionTestBudget(), output)
			if restored != nil || !errors.Is(err, want) {
				t.Fatal("invalid prepare conferred stageability", kind, restored, err)
			}
			after, err := stores[1].ApplicationUsage()
			if err != nil || after != before {
				t.Fatal("failed restoration changed application", after, err)
			}
		})
	}
}

func TestPartitionTransactionKernelPublicNilAndBudgetGuards(t *testing.T) {
	var template *PartitionTransactionTemplate
	var kernel *PreparedPartitionKernel
	if template.Digest() != ([32]byte{}) || kernel.Digest() != ([32]byte{}) {
		t.Fatal("nil digest")
	}
	for _, call := range []func() error{func() error { _, err := template.Participants(transactionTestOutput(t)); return err }, func() error { _, err := template.Dependencies(transactionTestOutput(t)); return err }, func() error { _, err := template.Bases(transactionTestOutput(t)); return err }, func() error {
		_, err := template.ParticipantKernel(8, [16]byte{1}, transactionTestOutput(t))
		return err
	}, func() error { _, err := kernel.Binding(transactionTestOutput(t)); return err }, func() error { _, err := kernel.Bytes(transactionTestOutput(t)); return err }, func() error {
		_, _, err := InspectPreparedPartitionKernel(nil, Limits{}, GraphLimits{}, nil)
		return err
	}, func() error { _, err := InspectPreparedKernelIdentities(nil, Limits{}, GraphLimits{}, nil); return err }, func() error {
		_, err := PreparedKernelControlKey(PreparedKernelBinding{}, transactionTestOutput(t))
		return err
	}, func() error {
		_, err := PreparedKernelPrepareControlKey(PreparedKernelBinding{}, transactionTestOutput(t))
		return err
	}} {
		if err := call(); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if b, r, err := InspectPreparedPartitionKernel(nil, Limits{}, GraphLimits{}, transactionTestOutput(t)); !errors.Is(err, ErrCorrupt) || b.TxID != ([16]byte{}) || r.ID() != 0 {
		t.Fatal(b, r, err)
	}
	if ids, err := InspectPreparedKernelIdentities(nil, Limits{}, GraphLimits{}, transactionTestOutput(t)); !errors.Is(err, ErrCorrupt) || len(ids.Entities) != 0 {
		t.Fatal(ids, err)
	}
}

func TestPartitionTransactionKernelCanonicalByteMutationsAndConsumptiveAdmission(t *testing.T) {
	sources, d, binding, cfg, _ := transactionViewFixture(t)
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	view, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	revision, _ := state.NewRevision(100, 7)
	template, _, err := PlanPartitionTransaction(t.Context(), view, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 17, Life: 1, Scope: all}, {Kind: graphstate.Set, Owner: 17, Life: 1, Scope: all, Name: "p", Value: graphstate.I64(1), ValueID: 18}}, revision)
	if err != nil {
		t.Fatal(err)
	}
	kernel, err := template.ParticipantKernel(8, [16]byte{1}, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := kernel.Binding(transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := kernel.Bytes(transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	// Every delivered byte is perturbed through real canonical inspection. A
	// still-valid metadata-only envelope must expose its changed binding; it
	// cannot silently retain the original body/digest/revision interpretation.
	for offset := range len(wire) {
		changed := exactCopy(wire)
		changed[offset] ^= 1
		if offset < len(changed)-32 {
			changed = sealRecord(changed[:len(changed)-32])
		}
		read, round, err := InspectPreparedPartitionKernel(changed, Limits{}, GraphLimits{}, transactionTestOutput(t))
		if err == nil {
			if sameKernelBinding(read, expected) && round == revision {
				t.Fatal("changed canonical bytes retained original interpretation", offset)
			}
		} else {
			if read.TxID != ([16]byte{}) || round.ID() != 0 {
				t.Fatal("malformed inspection returned partial metadata", offset, read, round, err)
			}
			if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrResourceLimit) {
				t.Fatal("malformed bytes lost sentinel identity", offset, err)
			}
		}
	}
	for _, cap := range []int{0, 128, 1024, 4096, 16384, 65536, 1 << 20} {
		output, err := graphstate.NewOutputBudget(cap)
		if err != nil {
			t.Fatal(err)
		}
		read, round, err := InspectPreparedPartitionKernel(wire, Limits{}, GraphLimits{}, output)
		if output.Used() > cap {
			t.Fatal("ancestor budget exceeded", cap, output.Used())
		}
		if err != nil {
			if !errors.Is(err, ErrResourceLimit) || read.TxID != ([16]byte{}) || round.ID() != 0 {
				t.Fatal(cap, read, round, err)
			}
		} else if !sameKernelBinding(read, expected) || round != revision {
			t.Fatal("admitted inspector changed binding", cap, read, round)
		}
	}
	for _, invoke := range []func(*graphstate.OutputBudget) error{func(b *graphstate.OutputBudget) error { _, err := template.Participants(b); return err }, func(b *graphstate.OutputBudget) error { _, err := template.Dependencies(b); return err }, func(b *graphstate.OutputBudget) error { _, err := template.Bases(b); return err }, func(b *graphstate.OutputBudget) error {
		_, err := template.ParticipantKernel(8, [16]byte{1}, b)
		return err
	}, func(b *graphstate.OutputBudget) error { _, err := kernel.Binding(b); return err }, func(b *graphstate.OutputBudget) error { _, err := kernel.Bytes(b); return err }, func(b *graphstate.OutputBudget) error {
		_, err := InspectPreparedKernelIdentities(wire, Limits{}, GraphLimits{}, b)
		return err
	}} {
		empty, err := graphstate.NewOutputBudget(0)
		if err != nil {
			t.Fatal(err)
		}
		if err := invoke(empty); !errors.Is(err, ErrResourceLimit) {
			t.Fatal("zero allowance widened", err)
		}
		if err := invoke(nil); !errors.Is(err, ErrInvalid) {
			t.Fatal("nil allowance silently opted out", err)
		}
	}
	if _, err := template.ParticipantKernel(99, [16]byte{1}, transactionTestOutput(t)); !errors.Is(err, ErrNamespace) {
		t.Fatal(err)
	}
	if _, err := template.ParticipantKernel(8, [16]byte{}, transactionTestOutput(t)); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestPartitionTransactionStageGlobalUniqueAuthorityChecksRemoteNodeAndRelationshipClaims(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	definitions := []graphstate.PropertyDefinition{{Name: "p", Owner: graphstate.Node, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}, {Name: "p", Owner: graphstate.Relationship, Type: graphstate.ScalarI64, Cardinality: graphstate.ScalarCardinality, Unique: graphstate.UniqueScalar}}
	routing, err := NewPartitionRouting(d, 1, 0, []BucketOwnership{{Kind: IdentityRoutingBucket, Partition: 3, Epoch: 1}, {Kind: UniqueRoutingBucket, Partition: 8, Epoch: 1}, {Kind: AxisRoutingBucket, Partition: 3, Epoch: 1}}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := EncodePartitionRoutingBinding(routing, d, binding, cfg, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for n, source := range sources {
		base, err := source.Root()
		if err != nil {
			t.Fatal(err)
		}
		namespace := Namespace{d.Graph(), d.entries[n].Partition}
		rows := []raftlog.KV{{Key: RoutingDefinitionKey(namespace), Value: wire}}
		for _, definition := range definitions {
			body, err := encodeProperty(namespace, definition, DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, raftlog.KV{Key: schemaKey(namespace, definition.Owner, definition.Name), Value: body})
		}
		slices.SortFunc(rows, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
		sources[n] = ownershipView(t, stores[n], ownershipInstallRaw(t, stores[n], base.Image, rows))
	}
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	view, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 1, Scope: all}, {Kind: graphstate.CreateNode, Owner: 17, Life: 1, Scope: all}, {Kind: graphstate.CreateRelationship, Owner: 2, Life: 1, Scope: all, Record: graphstate.EntityRecord{Type: "CROSS", Source: 1, Target: 17, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 1, TargetLife: 1}}, {Kind: graphstate.Set, Owner: 17, Life: 1, Scope: all, Name: "p", Value: graphstate.I64(42), ValueID: 18}, {Kind: graphstate.Set, Owner: 2, Life: 1, Scope: all, Name: "p", Value: graphstate.I64(42), ValueID: 19}}
	revision, _ := state.NewRevision(100, 0)
	template, _, err := PlanPartitionTransaction(t.Context(), view, ops, revision)
	if err != nil {
		t.Fatal(err)
	}
	_ = view.Close()
	current := transactionInstallTemplate(t, sources, stores, template, revision)
	reader, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for n, definition := range definitions {
		page, err := reader.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: definition, Value: graphstate.I64(42), Window: all}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 1 << 20})
		owner := graphstate.EntityID(17)
		if n == 1 {
			owner = 2
		}
		if err != nil || !page.Complete || len(page.Claims) != 1 || page.Claims[0].Owner != owner {
			t.Fatal("owner-kind exact claim set", page, err)
		}
		empty, err := reader.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: definition, Value: graphstate.I64(43), Window: all}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 1 << 20})
		if err != nil || !empty.Complete || len(empty.Claims) != 0 {
			t.Fatal("phantom typed value has a claim", empty, err)
		}
	}
	before, err := stores[0].ApplicationUsage()
	if err != nil {
		t.Fatal(err)
	}
	next, _ := state.NewRevision(200, 0)
	failed, _, err := PlanPartitionTransaction(t.Context(), reader, []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 1, Scope: all, Name: "p", Value: graphstate.I64(42), ValueID: 20}}, next)
	if failed != nil || !errors.Is(err, graphstate.ErrUniqueOverlap) {
		t.Fatal("remote current claim silently omitted", failed, err)
	}
	after, err := stores[0].ApplicationUsage()
	if err != nil || after != before {
		t.Fatal("failed uniqueness staging changed store", after, err)
	}
	// Closing a remote endpoint releases both its own and the relation's
	// effective claims, while retaining declared/raw history postings.
	fresh, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	closed, _, err := PlanPartitionTransaction(t.Context(), fresh, []graphstate.Operation{{Kind: graphstate.Close, Owner: 17, Life: 1, Scope: all}, {Kind: graphstate.Set, Owner: 1, Life: 1, Scope: all, Name: "p", Value: graphstate.I64(42), ValueID: 20}}, next)
	if err != nil {
		t.Fatal(err)
	}
	_ = closed
	deps, err := template.Dependencies(transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[graphstate.DependencyKind]bool{}
	for _, dep := range deps {
		kinds[dep.Kind] = true
	}
	for _, kind := range []graphstate.DependencyKind{graphstate.EntityDependency, graphstate.LifeDependency, graphstate.SchemaDependency, graphstate.ValueDependency, graphstate.ValueIdentityDependency, graphstate.ComponentDependency, graphstate.PrefixDependency, graphstate.UniquenessDependency, graphstate.IncidentDependency} {
		if !kinds[kind] {
			t.Fatal("negative/lifecycle authority omitted from dependency footprint", kind)
		}
	}
}
