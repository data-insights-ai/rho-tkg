package graphstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func rangeMoveTestCommit(t *testing.T, store *raftlog.Store, e RangeMovePageEffects) *raftlog.ApplicationView {
	t.Helper()
	wire, err := EncodeRangeMoveImportState(e.Next, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	next := e.Base.Index + 1
	if err = store.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(next)}, Entries: []*pb.Entry{{Index: new(next), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("checked range move page fixture")}}}); err != nil {
		t.Fatal(err)
	}
	batch := raftlog.ApplicationBatch{BaseGeneration: e.Base.Generation, BaseIndex: e.Base.Index, BaseImageHash: e.Base.ImageHash, Image: e.Base.Image, Writes: e.Writes, ControlPuts: []raftlog.ApplicationControlPut{{Key: binary.BigEndian.AppendUint64([]byte("range-move-progress"), next), Value: wire}}}
	if err = store.InstallApplication(next, batch); err != nil {
		t.Fatal(err)
	}
	return ownershipView(t, store, next)
}

func rangeMoveTestPlan(t *testing.T, sources []*raftlog.ApplicationView, d OwnershipDeclaration, binding types.DefaultAxisBinding, cfg [32]byte) RangeMoveDescriptor {
	t.Helper()
	publication, from, _, err := LookupPublishedRange(t.Context(), sources[0], Namespace{d.Graph(), 3}, 1, cfg, transactionTestBudget())
	if err != nil {
		t.Fatal(err)
	}
	to := from
	to.Partition, to.Epoch = 8, from.Epoch+1
	descriptor, _, err := PlanRangeMove(t.Context(), sources[0], sources[1], d, binding, cfg, RangeMoveSpec{publication, from, to}, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}
func rangeMoveTestSeal(t *testing.T, source ApplicationReadSource, d RangeMoveDescriptor) RangeMoveDescriptor {
	t.Helper()
	var cursor RangeMoveCursor
	for step := 0; step < 4096; step++ {
		page, _, err := ExportRangeMovePage(t.Context(), source, d, cursor, raftlog.ReadBudget{Rows: 1, Bytes: 4160}, Limits{}, GraphLimits{}, transactionTestOutput(t))
		if err != nil {
			t.Fatal("manifest", step, err)
		}
		wire, err := EncodeRangeMoveCursor(page.Next, transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		cursor, err = DecodeRangeMoveCursor(wire, transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		if page.Complete {
			sealed, err := SealRangeMoveDescriptor(d, cursor, transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			return sealed
		}
	}
	t.Fatal("manifest failed to terminate")
	return RangeMoveDescriptor{}
}

func TestRangeMoveStreamingDurableResumePreservesValuesAbsenceAndTarget(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all}, {Kind: graphstate.CreateNode, Owner: 17, Life: 18, Scope: all}, {Kind: graphstate.CreateRelationship, Owner: 3, Life: 4, Scope: all, Record: graphstate.EntityRecord{Type: "CROSS", Source: 1, Target: 17, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 18}}, {Kind: graphstate.Set, Owner: 1, Life: 2, Scope: all, Name: "p", Value: graphstate.I64(5), ValueID: 5}, {Kind: graphstate.Set, Owner: 17, Life: 18, Scope: all, Name: "p", Value: graphstate.I64(9), ValueID: 19}}
	revision, _ := state.NewRevision(10, 1)
	template, _, err := PlanPartitionTransaction(t.Context(), v, ops, revision)
	if err != nil {
		t.Fatal(err)
	}
	_ = v.Close()
	initial := transactionInstallTemplate(t, sources, stores, template, revision)
	v, _, err = OpenPartitionTransactionView(t.Context(), initial, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	unsetRevision, _ := state.NewRevision(20, 1)
	template, _, err = PlanPartitionTransaction(t.Context(), v, []graphstate.Operation{{Kind: graphstate.Unset, Owner: 1, Life: 2, Scope: all, Name: "p"}}, unsetRevision)
	if err != nil {
		t.Fatal(err)
	}
	_ = v.Close()
	current := transactionInstallTemplate(t, initial, stores, template, unsetRevision)
	if current[1] == nil {
		current[1] = initial[1]
	}
	descriptor := rangeMoveTestSeal(t, current[0], rangeMoveTestPlan(t, current, d, binding, cfg))
	if descriptor.Rows < 10 || descriptor.Bytes <= 64 {
		t.Fatal("range was not larger than one fragment", descriptor.Rows, descriptor.Bytes)
	}
	target := current[1]
	progress, _, err := BeginRangeMoveImport(t.Context(), target, descriptor, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	steps := 0
	for !progress.PreparationComplete {
		effects, _, e := AdvanceRangeMoveImportPreparation(t.Context(), target, descriptor, progress, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
		if e != nil {
			t.Fatal(e)
		}
		next := rangeMoveTestCommit(t, stores[1], effects)
		if target != current[1] {
			_ = target.Close()
		}
		target = next
		progress = effects.Next
		steps++
		if steps > 64 {
			t.Fatal("preparation did not finish")
		}
	}
	var cursor RangeMoveCursor
	fragments := 0
	for step := 0; step < 4096; step++ {
		page, _, e := ExportRangeMovePage(t.Context(), current[0], descriptor, cursor, raftlog.ReadBudget{Rows: 1, Bytes: 4160}, Limits{}, GraphLimits{}, transactionTestOutput(t))
		if e != nil {
			t.Fatal("export", step, e)
		}
		if len(page.Payload) > 64 {
			t.Fatal("fragment exceeded page allowance")
		}
		if !page.RecordDone && !page.Complete {
			fragments++
		}
		if step == 0 {
			bad := page
			bad.Next.Digest[0] ^= 1
			effects, _, e := StageRangeMovePage(t.Context(), target, descriptor, progress, bad, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if !errors.Is(e, ErrInvalid) || effects.Writes != nil {
				t.Fatal("damaged page admitted", e)
			}
		}
		effects, _, e := StageRangeMovePage(t.Context(), target, descriptor, progress, page, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
		if e != nil {
			t.Fatal("stage", step, page.Kind, e)
		}
		next := rangeMoveTestCommit(t, stores[1], effects)
		_ = target.Close()
		target = next
		control, found, _, e := target.GetControl(t.Context(), binary.BigEndian.AppendUint64([]byte("range-move-progress"), effects.Base.Index+1), raftlog.ReadBudget{Rows: 1, Bytes: 2 << 20})
		if e != nil || !found {
			t.Fatal("missing co-committed progress", e)
		}
		progress, e = DecodeRangeMoveImportState(control.Value, transactionTestOutput(t))
		if e != nil {
			t.Fatal(e)
		}
		duplicate, _, e := StageRangeMovePage(t.Context(), target, descriptor, progress, page, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
		if !errors.Is(e, ErrInvalid) || duplicate.Writes != nil {
			t.Fatal("duplicate page admitted", e)
		}
		root, e := target.Root()
		if e != nil || !bytes.Equal(root.Image, descriptor.Target.Image[:]) {
			t.Fatal("staging published root", e)
		}
		cursor = page.Next
		if page.Complete {
			break
		}
		if step == 4095 {
			t.Fatal("transfer did not terminate")
		}
	}
	if fragments == 0 {
		t.Fatal("record fragmentation was not exercised")
	}
	sealed, _, err := FinishRangeMoveImport(t.Context(), target, descriptor, progress, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	effects, _, err := ActivateRangeMoveImport(t.Context(), target, descriptor, sealed, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil || len(effects.Groups) != 0 || len(effects.Delta.Patches) != 0 {
		t.Fatal("activation invented business CDC", err)
	}
	image, err := EncodeRoot(effects.Root)
	if err != nil {
		t.Fatal(err)
	}
	activated := ownershipView(t, stores[1], ownershipInstallRaw(t, stores[1], image, effects.Writes))
	cat := &Catalog{view: activated, root: effects.Root, rootImageBytes: 172, limits: DefaultLimits(), hash: equalityDigest}
	reader, err := cat.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	index, found, err := reader.fullDescriptor(effects.Root)
	if err != nil || !found {
		t.Fatal(err)
	}
	reader.fullView = &index
	pages := &pageReader{q: reader, limits: DefaultPageLimits()}
	for _, id := range []graphstate.EntityID{1, 3, 17} {
		entity, found, e := reader.entity(EntityRef{d.Graph(), id})
		if e != nil || !found || entity.ID != id {
			t.Fatal("lost moved or unrelated identity", id, e)
		}
	}
	for _, test := range []struct {
		id    graphstate.ValueID
		value int64
	}{{5, 5}, {19, 9}} {
		entry, _, found, e := reader.value(ValueRef{d.Graph(), test.id})
		if e != nil || !found || entry.Value.Render() != graphstate.I64(test.value).Render() {
			t.Fatal("lost unreferenced or unrelated value", test.id, e)
		}
	}
	key := graphstate.ComponentKey{Owner: 1, Life: 2, Kind: graphstate.ScalarProperty, Name: "p"}
	meta, found, err := pages.readMeta(key)
	if err != nil || !found {
		t.Fatal(err)
	}
	leaf, err := pages.directory(meta.Root, key, binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := rangeMoveState(pages, leaf)
	if err != nil {
		t.Fatal(err)
	}
	pieces := snapshot.Pieces()
	if len(pieces) != 1 || pieces[0].Cell().Present() || pieces[0].Cell().Revision() != unsetRevision {
		t.Fatal("absence revision was replaced", pieces)
	}
	for _, endpoint := range []graphstate.EntityID{1, 17} {
		wanted := canonicalIncidentKeys(graphstate.EntityRecord{ID: 3, Kind: graphstate.Relationship, Source: 1, Target: 17, Mode: graphstate.LifeBound})
		for _, k := range wanted {
			if k.endpoint != endpoint {
				continue
			}
			found, e := pages.hasPostingKey(index.canonical, k, keyTreeLimits(pages.limits))
			if e != nil || !found {
				t.Fatal("endpoint index was lost", endpoint, e)
			}
		}
	}
	old := initial[0]
	oldRoot, _ := old.Root()
	oldDecoded, _ := DecodeRoot(oldRoot.Image)
	oldCat := &Catalog{view: old, root: oldDecoded, rootImageBytes: 172, limits: DefaultLimits(), hash: equalityDigest}
	oldReader, _ := oldCat.reader(t.Context())
	oldIndex, _, e := oldReader.fullDescriptor(oldDecoded)
	if e != nil {
		t.Fatal(e)
	}
	oldReader.fullView = &oldIndex
	oldPages := &pageReader{q: oldReader, limits: DefaultPageLimits()}
	oldMeta, _, e := oldPages.readMeta(key)
	if e != nil {
		t.Fatal(e)
	}
	oldLeaf, e := oldPages.directory(oldMeta.Root, key, binding.Axis())
	if e != nil {
		t.Fatal(e)
	}
	oldState, e := rangeMoveState(oldPages, oldLeaf)
	if e != nil || len(oldState.Pieces()) != 1 || !oldState.Pieces()[0].Cell().Present() || oldState.Pieces()[0].Cell().Revision() != revision {
		t.Fatal("old native view substituted moved state", e)
	}
	if len(effects.Dependencies) != 0 {
		t.Fatal("move invented business dependencies")
	}
	_ = target.Close()
}
