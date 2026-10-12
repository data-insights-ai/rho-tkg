package graphstore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Storage-boundary guards complement the actual graphapply abort/new-handoff
// RED. This setup uses genuine native prepared kernels, committed point rows,
// retained roots, and actual native tombstones; no ApplicationView is fabricated.
func rangeMoveMaintenanceFixture(t *testing.T) (RangeMoveDescriptor, *raftlog.ApplicationView, *raftlog.Store) {
	t.Helper()
	sources, declaration, binding, configuration, stores := transactionViewFixture(t)
	view, _, err := OpenPartitionTransactionView(t.Context(), sources, declaration, binding, configuration, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	all, err := temporal.All(binding.Axis())
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.NewRevision(10, 23)
	if err != nil {
		t.Fatal(err)
	}
	template, _, err := PlanPartitionTransaction(t.Context(), view, []graphstate.Operation{
		{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all},
		{Kind: graphstate.CreateNode, Owner: 17, Life: 18, Scope: all},
		{Kind: graphstate.Set, Owner: 17, Life: 18, Scope: all, Name: "p", Value: graphstate.I64(13), ValueID: 19},
	}, revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	current := transactionInstallTemplate(t, sources, stores, template, revision)
	descriptor := rangeMoveTestSeal(t, current[0], rangeMoveTestPlan(t, current, declaration, binding, configuration))
	return descriptor, current[1], stores[1]
}

func TestRangeMoveMaintenanceNativeTombstoneRetainsChargedBacking(t *testing.T) {
	descriptor, target, store := rangeMoveMaintenanceFixture(t)
	root, err := target.Root()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRoot(root.Image)
	if err != nil {
		t.Fatal(err)
	}
	key := entityKey(decoded.namespace, 1)
	index := ownershipInstallRaw(t, store, root.Image, []raftlog.KV{{Key: key, Deleted: true}})
	deleted := ownershipView(t, store, index)
	native, found, err := deleted.Get(t.Context(), key, 1<<20)
	if err != nil || !found || !native.Deleted || len(native.Key) == 0 {
		t.Fatal("actual native tombstone", native, found, err)
	}
	source := rangeMoveMaintenanceSource{deleted}
	row, present, err := source.Get(t.Context(), key, 1<<20)
	if err != nil || present || !row.Deleted || !bytes.Equal(row.Key, native.Key) || !bytes.Equal(row.Value, native.Value) || cap(row.Key) != cap(native.Key) || cap(row.Value) != cap(native.Value) {
		t.Fatal("maintenance absence discarded actual owned tombstone backing", row, present, err)
	}
	output := transactionTestOutput(t)
	catalog := &Catalog{view: deleted, data: source, root: decoded, rootImageBytes: len(root.Image), limits: DefaultLimits(), hash: equalityDigest}
	reader, err := catalog.readerWithOutputBudget(t.Context(), output)
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, beforeRows, beforeOutput := reader.bytes, reader.rows, output.Remaining()
	value, present, err := reader.get(key)
	cost := cap(native.Key) + cap(native.Value) + 64
	if err != nil || present || value != nil || reader.rows-beforeRows != 1 || reader.bytes-beforeBytes != max(len(key), cap(native.Key))+cap(native.Value)+64 || beforeOutput-output.Remaining() != cost {
		t.Fatal("native absence escaped source/output accounting", err, present, reader.rows-beforeRows, reader.bytes-beforeBytes, beforeOutput-output.Remaining(), cost)
	}
	// The importer, not the ordinary catalog, uses absence to reinstall an
	// optional entity point row after a genuine native delete/rollback.
	pageReader, _, err := rangeMoveTargetReader(t.Context(), deleted, descriptor, nil, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	entity := graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: descriptor.DefaultAxis.Axis()}
	if err := pageReader.q.stageEntity(EntityRef{decoded.namespace.Graph, 1}, entity); err != nil {
		t.Fatal("optional native entity tombstone blocked reinstall", err)
	}
	staged, ok := pageReader.q.pending[string(key)]
	if !ok || staged.Deleted || len(staged.Value) == 0 {
		t.Fatal("optional reinstall produced no inert write")
	}
	ordinary := &Catalog{view: deleted, root: decoded, rootImageBytes: len(root.Image), limits: DefaultLimits(), hash: equalityDigest}
	strict, err := ordinary.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := strict.get(key); !errors.Is(err, ErrCorrupt) {
		t.Fatal("maintenance changed ordinary catalog semantics", err)
	}
	for _, tight := range []string{"source-bytes", "output-bytes"} {
		t.Run(tight, func(t *testing.T) {
			arena := transactionTestOutput(t)
			if tight == "output-bytes" {
				var err error
				arena, err = graphstate.NewOutputBudget(cost - 1)
				if err != nil {
					t.Fatal(err)
				}
			}
			q, err := catalog.readerWithOutputBudget(t.Context(), arena)
			if err != nil {
				t.Fatal(err)
			}
			if tight == "source-bytes" {
				q.maxBytes = q.bytes + cost - 1
			}
			value, found, err := q.get(key)
			if !errors.Is(err, ErrResourceLimit) || found || value != nil {
				t.Fatal("tombstone exceeded original grant", tight, err, found, value)
			}
		})
	}
	if _, err := deleted.Root(); err != nil {
		t.Fatal("inert reader closed native borrow", err)
	}
}

func TestRangeMoveMaintenanceRequiredNativeTombstonesRemainCorrupt(t *testing.T) {
	for _, kind := range []string{"descriptor", "tree-root", "axis", "directory", "bucket-member"} {
		t.Run(kind, func(t *testing.T) {
			descriptor, target, store := rangeMoveMaintenanceFixture(t)
			p, base, err := rangeMoveTargetReader(t.Context(), target, descriptor, nil, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			key := componentIndexDescriptorKey(p.root.namespace)
			component := graphstate.ComponentKey{Owner: 17, Kind: graphstate.Presence}
			meta, found, err := p.readMeta(component)
			if err != nil || !found {
				t.Fatal("actual target component", found, err)
			}
			valueKey, err := graphstate.I64(13).EqualityKey(p.q.c.limits.valueLimits())
			if err != nil {
				t.Fatal(err)
			}
			hash := equalityDigest(valueKey)
			if member, err := p.q.member(hash, 0); err != nil || member != 19 {
				t.Fatal("actual target bucket member", member, err)
			}
			switch kind {
			case "tree-root":
				key = physicalKey(p.root.namespace, componentKeyTreeRecord, p.q.full.descriptor.keys.id)
			case "axis":
				key = axisKey(p.root.namespace, descriptor.DefaultAxis.Axis().Descriptor().ID)
			case "directory":
				key = physicalKey(p.root.namespace, directoryRecord, meta.Root)
			case "bucket-member":
				key = memberKey(p.root.namespace, hash, 0)
			}
			original, existed, err := target.Get(t.Context(), key, 1<<20)
			if err != nil || !existed || original.Deleted || len(original.Value) == 0 {
				t.Fatal("required row was not genuinely present", kind, err)
			}
			index := ownershipInstallRaw(t, store, base.Image, []raftlog.KV{{Key: key, Deleted: true}})
			deleted := ownershipView(t, store, index)
			native, present, err := deleted.Get(t.Context(), key, 1<<20)
			if err != nil || !present || !native.Deleted {
				t.Fatal("actual required-row native tombstone", kind, err)
			}
			p, _, err = rangeMoveTargetReader(t.Context(), deleted, descriptor, nil, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if kind != "descriptor" && kind != "tree-root" {
				if err != nil {
					t.Fatal("native setup stopped before required consumer", kind, err)
				}
				switch kind {
				case "axis":
					err = p.q.checkAxis(descriptor.DefaultAxis.Axis())
				case "directory":
					_, err = p.directory(meta.Root, component, descriptor.DefaultAxis.Axis())
				case "bucket-member":
					_, err = p.q.member(hash, 0)
				}
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal("required native row became successful absence", kind, err)
			}
			if _, err := target.Root(); err != nil {
				t.Fatal("required-row refusal closed retained borrow", err)
			}
		})
	}
}
