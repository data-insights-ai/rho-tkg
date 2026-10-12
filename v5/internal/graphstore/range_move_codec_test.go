package graphstore

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestRangeMoveCodecsOwnBuffersAndRefuseMalformedBudget(t *testing.T) {
	sources, d, binding, cfg, _ := transactionViewFixture(t)
	descriptor := rangeMoveTestPlan(t, sources, d, binding, cfg)
	dw, err := EncodeRangeMoveDescriptor(descriptor, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRangeMoveDescriptor(dw, transactionTestOutput(t))
	if err != nil || decoded.binding() != descriptor.binding() {
		t.Fatal(err)
	}
	start := rangeMoveStart(descriptor)
	start.After = []byte("a")
	cw, err := EncodeRangeMoveCursor(start, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	owned, err := DecodeRangeMoveCursor(cw, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	copy(cw, bytes.Repeat([]byte{0xff}, len(cw)))
	if string(owned.After) != "a" {
		t.Fatal("cursor retained decoder input")
	}
	next := start
	next.Sequence = 1
	next.Key = []byte("key")
	next.Offset = 1
	page := RangeMovePage{Before: start, Next: next, Kind: rangeMoveValues, Key: []byte("key"), Payload: []byte("x"), RecordBytes: 2}
	pw, err := EncodeRangeMovePage(page, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	p, err := DecodeRangeMovePage(pw, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	copy(pw, bytes.Repeat([]byte{0xee}, len(pw)))
	if string(p.Payload) != "x" || string(p.Next.Key) != "key" {
		t.Fatal("page retained decoder input")
	}
	for _, tc := range []struct {
		wire   []byte
		decode func([]byte, *graphstate.OutputBudget) error
	}{
		{dw, func(w []byte, b *graphstate.OutputBudget) error { _, e := DecodeRangeMoveDescriptor(w, b); return e }},
		{func() []byte { w, _ := EncodeRangeMoveCursor(start, transactionTestOutput(t)); return w }(), func(w []byte, b *graphstate.OutputBudget) error { _, e := DecodeRangeMoveCursor(w, b); return e }},
		{func() []byte { w, _ := EncodeRangeMovePage(page, transactionTestOutput(t)); return w }(), func(w []byte, b *graphstate.OutputBudget) error { _, e := DecodeRangeMovePage(w, b); return e }},
	} {
		for cut := 0; cut < len(tc.wire); cut++ {
			if e := tc.decode(tc.wire[:cut], transactionTestOutput(t)); !errors.Is(e, ErrCorrupt) {
				t.Fatal("truncation accepted", cut, e)
			}
		}
		for cut := 4; cut < len(tc.wire)-32; cut++ {
			damaged := sealRecord(bytes.Clone(tc.wire[:cut]))
			if e := tc.decode(damaged, transactionTestOutput(t)); !errors.Is(e, ErrCorrupt) {
				t.Fatal("resealed truncation admitted", cut, e)
			}
		}
		bad := bytes.Clone(tc.wire)
		bad[len(bad)-1] ^= 1
		if e := tc.decode(bad, transactionTestOutput(t)); !errors.Is(e, ErrCorrupt) {
			t.Fatal("damaged seal accepted", e)
		}
		if e := tc.decode(tc.wire, nil); !errors.Is(e, ErrInvalid) {
			t.Fatal("nil output", e)
		}
	}
	exhausted, _ := graphstate.NewOutputBudget(1)
	if err = exhausted.Reserve(1); err != nil {
		t.Fatal(err)
	}
	large := page
	large.Payload = make([]byte, 8<<20)
	large.RecordBytes = uint32(len(large.Payload) + 1)
	large.Next.Offset = uint32(len(large.Payload))
	wire, err := EncodeRangeMovePage(large, exhausted)
	if !errors.Is(err, ErrResourceLimit) || wire != nil || exhausted.Remaining() != 0 {
		t.Fatal("large caller payload allocated before admission", err, exhausted.Remaining())
	}
	foreign := descriptor
	foreign.Source.Binding.Identity.Group[1] = 1
	_, _, err = ExportRangeMovePage(t.Context(), sources[0], foreign, rangeMoveStart(descriptor), raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}, Limits{}, GraphLimits{}, transactionTestOutput(t))
	if !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign capture reused cursor", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err = ExportRangeMovePage(canceled, sources[0], descriptor, RangeMoveCursor{}, raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}, Limits{}, GraphLimits{}, transactionTestOutput(t))
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// Genuine same-group native snapshot preparation changes only local physical
// generation. Its verified data is inspection, never range-move authority.
func TestRangeMoveFrozenCaptureSurvivesNativeGenerationChange(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	all, e := temporal.All(binding.Axis())
	if e != nil {
		t.Fatal(e)
	}
	composite, _, e := OpenPartitionTransactionView(t.Context(), sources, d, binding, cfg, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if e != nil {
		t.Fatal(e)
	}
	revision, _ := state.NewRevision(10, 1)
	template, _, e := PlanPartitionTransaction(t.Context(), composite, []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all}, {Kind: graphstate.Set, Owner: 1, Life: 2, Scope: all, Name: "p", Value: graphstate.I64(7), ValueID: 5}}, revision)
	if e != nil {
		t.Fatal(e)
	}
	_ = composite.Close()
	populated := transactionInstallTemplate(t, sources, stores, template, revision)
	if populated[1] == nil {
		populated[1] = sources[1]
	}
	sources = populated
	descriptor := rangeMoveTestPlan(t, sources, d, binding, cfg)
	if err := stores[0].PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := stores[0].PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := stores[0].BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	for step := 0; ; step++ {
		if step == 1024 {
			t.Fatal("snapshot manifest cap")
		}
		done, e := export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 16, Bytes: 64 << 10})
		if e != nil {
			t.Fatal(e)
		}
		if done {
			break
		}
	}
	manifest, err := export.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	config := ownershipConfig(vfs.NewMem(), Namespace{d.Graph(), 3}, [16]byte{3}, 2)
	config.Controls = raftlog.ApplicationControlConfig{Version: 1}
	config.Transfer.Contract, err = raftlog.ApplicationContractForPolicyWithControls(config.Application, config.Controls)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := raftlog.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	seed, err := NewOwnershipRoot(Namespace{d.Graph(), 3}, d)
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(seed)
	if err != nil {
		t.Fatal(err)
	}
	if err = receiver.Initialize([]uint64{1, 2, 3}, image); err != nil {
		t.Fatal(err)
	}
	incoming, err := receiver.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	for step := 0; ; step++ {
		if step == 1024 {
			t.Fatal("snapshot chunk cap")
		}
		chunk, e := export.Next(t.Context(), raftlog.ReadBudget{Rows: 16, Bytes: 64 << 10})
		if e != nil {
			t.Fatal(e)
		}
		if e = incoming.Append(t.Context(), chunk); e != nil {
			t.Fatal(e)
		}
		if chunk.Final {
			break
		}
	}
	if err = incoming.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := incoming.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	view, err := prepared.OpenReadView(t.Context(), manifest.Index, 172)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	root, err := view.Root()
	if err != nil || root.Generation == descriptor.Source.Generation || root.Index != descriptor.Source.Index || !bytes.Equal(root.Image, descriptor.Source.Image[:]) {
		t.Fatal("did not create alternate genuine generation", root, err)
	}
	page, _, err := ExportRangeMovePage(t.Context(), view, descriptor, RangeMoveCursor{}, raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}, Limits{}, GraphLimits{}, transactionTestOutput(t))
	if err != nil || page.Complete || page.Kind != rangeMoveValues {
		t.Fatal("same frozen populated source refused only for local generation", err)
	}
	cursor := page.Next
	for step := 0; !page.Complete; step++ {
		if step == 64 {
			t.Fatal("generation continuation failed")
		}
		page, _, err = ExportRangeMovePage(t.Context(), view, descriptor, cursor, raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}, Limits{}, GraphLimits{}, transactionTestOutput(t))
		if err != nil {
			t.Fatal("genuine generation continuation", step, err)
		}
		cursor = page.Next
	}
	if cursor.Rows < 4 {
		t.Fatal("lost native values/components", cursor.Rows)
	}
}

func TestRangeMovePageExhaustedBudgetPrecedesBackingAllocation(t *testing.T) {
	cursor := RangeMoveCursor{Binding: [32]byte{1}, Family: rangeMoveValues, Digest: [32]byte{1}}
	next := cursor
	next.Sequence = 1
	next.Key = []byte("key")
	payload := make([]byte, 8<<20)
	next.Offset = uint32(len(payload))
	page := RangeMovePage{Before: cursor, Next: next, Kind: rangeMoveValues, Key: []byte("key"), Payload: payload, RecordBytes: uint32(len(payload) + 1)}
	budget, _ := graphstate.NewOutputBudget(0)
	var err error
	var wire []byte
	allocations := testing.AllocsPerRun(3, func() { wire, err = EncodeRangeMovePage(page, budget) })
	if !errors.Is(err, ErrResourceLimit) || wire != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	wire, err = EncodeRangeMovePage(page, budget)
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(payload)
	if !errors.Is(err, ErrResourceLimit) || wire != nil || after.TotalAlloc-before.TotalAlloc >= 64<<10 {
		t.Fatal("large backing allocated before exhausted-budget refusal", after.TotalAlloc-before.TotalAlloc, allocations, err)
	}
}

func TestRangeMoveNativeProgressCodecAndPublicRefusals(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	descriptor := rangeMoveTestSeal(t, sources[0], rangeMoveTestPlan(t, sources, d, binding, cfg))
	initial, _, err := BeginRangeMoveImport(t.Context(), sources[1], descriptor, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	wire, err := EncodeRangeMoveImportState(initial, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	for cut := 4; cut < len(wire)-32; cut++ {
		bad := sealRecord(bytes.Clone(wire[:cut]))
		got, e := DecodeRangeMoveImportState(bad, transactionTestOutput(t))
		if !errors.Is(e, ErrCorrupt) || got.Sequence != 0 || got.FutureRoot != (Root{}) {
			t.Fatal("resealed progress truncation admitted", cut, e)
		}
	}
	if got, e := DecodeRangeMoveImportState(wire, nil); !errors.Is(e, ErrInvalid) || got.FutureRoot != (Root{}) {
		t.Fatal(e)
	}
	zero, _ := graphstate.NewOutputBudget(0)
	if got, e := DecodeRangeMoveImportState(wire, zero); !errors.Is(e, ErrResourceLimit) || got.FutureRoot != (Root{}) {
		t.Fatal(e)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		ctx    context.Context
		target *raftlog.ApplicationView
		limits Limits
		g      GraphLimits
		budget OwnershipBudget
		output *graphstate.OutputBudget
		want   error
	}{
		{nil, sources[1], Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t), ErrInvalid},
		{canceled, sources[1], Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t), context.Canceled},
		{t.Context(), sources[0], Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t), ErrRebinding},
		{t.Context(), sources[1], Limits{MaxNameBytes: -1}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t), ErrInvalid},
		{t.Context(), sources[1], Limits{}, GraphLimits{MaxSourceRows: -1}, transactionTestBudget(), transactionTestOutput(t), ErrInvalid},
		{t.Context(), sources[1], Limits{}, GraphLimits{}, OwnershipBudget{SourceRows: 1 << 21, SourceBytes: 1, OutputBytes: 1}, transactionTestOutput(t), ErrInvalid},
		{t.Context(), sources[1], Limits{}, GraphLimits{}, OwnershipBudget{SourceRows: 1, SourceBytes: 16 << 20, OutputBytes: 16 << 20}, transactionTestOutput(t), ErrResourceLimit},
		{t.Context(), sources[1], Limits{}, GraphLimits{}, transactionTestBudget(), zero, ErrResourceLimit},
	} {
		result, _, err := BeginRangeMoveImport(tc.ctx, tc.target, descriptor, tc.limits, tc.g, tc.budget, tc.output)
		if !errors.Is(err, tc.want) || result.FutureRoot != (Root{}) {
			t.Fatal("public capture refusal", tc.want, err)
		}
	}
	prepared, _, e := AdvanceRangeMoveImportPreparation(t.Context(), sources[1], descriptor, initial, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if e != nil {
		t.Fatal(e)
	}
	native := rangeMoveTestCommit(t, stores[1], prepared)
	page, _, e := ExportRangeMovePage(t.Context(), sources[0], descriptor, RangeMoveCursor{}, raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}, Limits{}, GraphLimits{}, transactionTestOutput(t))
	if e != nil {
		t.Fatal(e)
	}
	altered := prepared.Next
	altered.FutureRoot, e = altered.FutureRoot.AdvanceEffects([32]byte{9})
	if e != nil {
		t.Fatal(e)
	}
	if result, _, e := StageRangeMovePage(t.Context(), native, descriptor, altered, page, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t)); !errors.Is(e, ErrInvalid) || result.Writes != nil {
		t.Fatal("inert progress changed captured logical root", e)
	}
	if result, _, e := FinishRangeMoveImport(t.Context(), sources[1], descriptor, initial, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t)); !errors.Is(e, ErrInvalid) || result.Sealed {
		t.Fatal("incomplete stream sealed", e)
	}
	if result, _, e := ActivateRangeMoveImport(t.Context(), sources[1], descriptor, initial, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t)); !errors.Is(e, ErrInvalid) || result.Writes != nil {
		t.Fatal("unsealed progress activated", e)
	}
}
