package graphstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/big"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func rangeMoveInstallSmall(t *testing.T, sources []*raftlog.ApplicationView, stores []*raftlog.Store, template *PartitionTransactionTemplate, revision state.Revision, g GraphLimits) []*raftlog.ApplicationView {
	t.Helper()
	current := append([]*raftlog.ApplicationView(nil), sources...)
	for _, part := range template.participants {
		n := 0
		if part.partition == 8 {
			n = 1
		}
		kernel, e := template.ParticipantKernel(part.partition, [16]byte{byte(n + 1)}, transactionTestOutput(t))
		if e != nil {
			t.Fatal(e)
		}
		effects, _, e := StagePreparedPartitionKernel(t.Context(), sources[n], kernel, revision, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
		if e != nil {
			t.Fatal(e)
		}
		root, e := effects.Root.AdvanceEffects(kernel.Digest())
		if e != nil {
			t.Fatal(e)
		}
		image, e := EncodeRoot(root)
		if e != nil {
			t.Fatal(e)
		}
		current[n] = ownershipView(t, stores[n], ownershipInstallRaw(t, stores[n], image, effects.Writes))
	}
	return current
}
func rangeMoveWideSpan(t *testing.T, axis temporal.Axis, offset int64) temporal.Scope {
	t.Helper()
	position := func(n int64) temporal.Position {
		value := new(big.Int).Lsh(big.NewInt(1), 3072)
		value.Add(value, big.NewInt(n))
		integer, e := temporal.ParseRational(value.String()+"/1", temporal.Limits{})
		if e != nil {
			t.Fatal(e)
		}
		p, e := temporal.RationalPosition(axis, integer)
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	lower, e := temporal.FiniteBound(position(offset), true)
	if e != nil {
		t.Fatal(e)
	}
	upper, e := temporal.FiniteBound(position(offset+2), false)
	if e != nil {
		t.Fatal(e)
	}
	span, e := temporal.Span(axis, lower, upper, temporal.Limits{})
	if e != nil {
		t.Fatal(e)
	}
	return span
}

func TestRangeMovePortableSplitTreesAndFragmentedPresenceResume(t *testing.T) {
	sources, d, binding, cfg, stores := transactionViewFixture(t)
	g := DefaultGraphLimits()
	g.Pages.MaxChildren, g.Pages.MaxCells = 3, 3
	all, e := temporal.All(binding.Axis())
	if e != nil {
		t.Fatal(e)
	}
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all}, {Kind: graphstate.CreateNode, Owner: 17, Life: 18, Scope: all}}
	for i := uint64(3); i <= 15; i += 2 {
		ops = append(ops, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: graphstate.EntityID(i), Life: graphstate.LifeID(i + 100), Scope: rangeMoveWideSpan(t, binding.Axis(), int64(i*4)), Record: graphstate.EntityRecord{Type: "CROSS", Source: 1, Target: 17, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 18}})
	}
	current := sources
	batches := [][]graphstate.Operation{ops[:2]}
	for _, op := range ops[2:] {
		batches = append(batches, []graphstate.Operation{op})
	}
	for number, batch := range batches {
		rev, _ := state.NewRevision(uint64(10+number), 1)
		view, _, err := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		template, _, err := PlanPartitionTransaction(t.Context(), view, batch, rev)
		if err != nil {
			t.Fatal("bounded fixture transaction", err)
		}
		_ = view.Close()
		current = rangeMoveInstallSmall(t, current, stores, template, rev, g)
	}
	view, _, e := OpenPartitionTransactionView(t.Context(), current, d, binding, cfg, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
	if e != nil {
		t.Fatal(e)
	}

	ops = nil
	for i := int64(0); i < 5; i++ {
		integer, _ := temporal.ParseRational(big.NewInt(i*4).String()+"/1", temporal.Limits{})
		p, _ := temporal.RationalPosition(binding.Axis(), integer)
		point, _ := temporal.Point(p)
		ops = append(ops, graphstate.Operation{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: point})
	}
	rev, _ := state.NewRevision(20, 1)
	template, _, e := PlanPartitionTransaction(t.Context(), view, ops, rev)
	if e != nil {
		t.Fatal(e)
	}
	_ = view.Close()
	current = rangeMoveInstallSmall(t, current, stores, template, rev, g)
	descriptor := rangeMoveTestPlan(t, current, d, binding, cfg)
	q, e := rangeMoveExportReader(t.Context(), current[0], descriptor, Limits{}, g, transactionTestOutput(t))
	if e != nil {
		t.Fatal(e)
	}
	index, e := q.fullDescriptor()
	if e != nil || index.canonical.level == 0 || index.declared.level == 0 || index.own.level == 0 {
		t.Fatal("fixture lacks actual split posting/presence trees", index, e)
	}
	meta, found, e := q.readMeta(graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence})
	if e != nil || !found {
		t.Fatal(e)
	}
	directory, e := q.directory(meta.Root, meta.Key, binding.Axis())
	if e != nil || directory.Level == 0 {
		t.Fatal("fixture lacks actual split component directory", directory.Level, e)
	}
	var cursor RangeMoveCursor
	counts := [8]int{}
	var row []byte
	var key []byte
	hadDepth, hadFragment := false, false
	for step := 0; step < 4096; step++ {
		page, _, err := ExportRangeMovePage(t.Context(), current[0], descriptor, cursor, raftlog.ReadBudget{Rows: 1, Bytes: 4160}, Limits{}, g, transactionTestOutput(t))
		if err != nil {
			t.Fatal("split continuation", step, cursor.Family, cursor.Depth, err)
		}
		hadDepth = hadDepth || cursor.Depth > 1
		if page.Complete {
			if len(row) > 0 {
				t.Fatal("EOF lost a fragment")
			}
			cursor = page.Next
			break
		}
		if page.Offset == 0 {
			key = bytes.Clone(page.Key)
			row = nil
		} else if !bytes.Equal(key, page.Key) || int(page.Offset) != len(row) {
			t.Fatal("fragment skipped its original record", step)
		}
		row = append(row, page.Payload...)
		if page.RecordDone {
			counts[page.Kind]++
			if page.Kind == rangeMovePresence {
				atom, e := rangeMoveReadAtom(row)
				if e != nil || atom.endpoint != 1 || atom.relationship < 3 || atom.relationship > 15 || uint64(atom.relationship)%2 != 1 {
					t.Fatal("presence emitted phantom/foreign atom", atom, e)
				}
			}
			row = nil
		}
		hadFragment = hadFragment || page.Kind == rangeMovePresence && page.Offset > 0
		encoded, e := EncodeRangeMoveCursor(page.Next, transactionTestOutput(t))
		if e != nil {
			t.Fatal(e)
		}
		cursor, e = DecodeRangeMoveCursor(encoded, transactionTestOutput(t))
		if e != nil {
			t.Fatal(e)
		}
		// Acquire a new genuine native handle at the original immutable coordinate.
		replacement := ownershipView(t, stores[0], descriptor.Source.Index)
		old := current[0]
		current[0] = replacement
		_ = old.Close()
		if step == 4095 {
			t.Fatal("bounded continuation failed to finish")
		}
	}
	if !hadDepth || !hadFragment || counts[rangeMoveCanonical] != 7 || counts[rangeMoveDeclared] != 7 || counts[rangeMovePresence] != 7 || counts[rangeMoveComponents] < 14 {
		t.Fatal("incomplete/duplicate split stream", counts, hadDepth, hadFragment)
	}
	sealedDescriptor, err := SealRangeMoveDescriptor(descriptor, cursor, transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	target := current[1]
	progress, _, err := BeginRangeMoveImport(t.Context(), target, sealedDescriptor, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	for !progress.PreparationComplete {
		effects, _, err := AdvanceRangeMoveImportPreparation(t.Context(), target, sealedDescriptor, progress, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		replacement := rangeMoveTestCommit(t, stores[1], effects)
		_ = target.Close()
		target = replacement
		progress = effects.Next
	}
	var receive RangeMoveCursor
	for step := 0; step < 4096; step++ {
		page, _, err := ExportRangeMovePage(t.Context(), current[0], sealedDescriptor, receive, raftlog.ReadBudget{Rows: 1, Bytes: 4160}, Limits{}, g, transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		if page.Kind == rangeMoveComponents && page.Offset == 0 {
			whole, _, err := ExportRangeMovePage(t.Context(), current[0], sealedDescriptor, page.Before, raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}, Limits{}, g, transactionTestOutput(t))
			if err != nil || !whole.RecordDone {
				t.Fatal(err)
			}
			rangeMoveRejectDamagedComponent(t, target, sealedDescriptor, progress, whole, g)
		}
		effects, _, err := StageRangeMovePage(t.Context(), target, sealedDescriptor, progress, page, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
		if err != nil {
			t.Fatal("split import", step, page.Kind, err)
		}
		encoded, err := EncodeRangeMoveImportState(effects.Next, transactionTestOutput(t))
		if err != nil || len(encoded) > 16<<10 {
			t.Fatal("durable progress retained fragment backing", len(encoded), err)
		}
		replacement := rangeMoveTestCommit(t, stores[1], effects)
		_ = target.Close()
		target = replacement
		progress, err = DecodeRangeMoveImportState(encoded, transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		receive = page.Next
		if page.Complete {
			break
		}
		if step == 4095 {
			t.Fatal("split import did not finish")
		}
	}
	finished, _, err := FinishRangeMoveImport(t.Context(), target, sealedDescriptor, progress, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
	if err != nil || finished.index.own.count != 14 || finished.index.canonical.count != 14 || finished.index.declared.count != 14 {
		t.Fatal("target failed exact merge of both endpoint ranges", finished.index, err)
	}
	rootPages, _, err := rangeMoveTargetReader(t.Context(), target, sealedDescriptor, &finished, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	m, found, err := rootPages.readMeta(graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence})
	if err != nil || !found {
		t.Fatal(err)
	}
	rootDirectory, err := rootPages.directory(m.Root, m.Key, binding.Axis())
	if err != nil || rootDirectory.Level == 0 {
		t.Fatal("target did not reconstruct split component index", err)
	}
	effects, _, err := ActivateRangeMoveImport(t.Context(), target, sealedDescriptor, finished, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	image, err := EncodeRoot(effects.Root)
	if err != nil {
		t.Fatal(err)
	}
	activated := ownershipView(t, stores[1], ownershipInstallRaw(t, stores[1], image, effects.Writes))
	catalog := &Catalog{view: activated, root: effects.Root, rootImageBytes: 172, limits: DefaultLimits(), hash: equalityDigest}
	reader, err := catalog.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	activatedIndex, found, err := reader.fullDescriptor(effects.Root)
	if err != nil || !found {
		t.Fatal(err)
	}
	reader.fullView = &activatedIndex
	activePages := &pageReader{q: reader, limits: g.Pages}
	activeMeta, found, err := activePages.readMeta(graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence})
	if err != nil || !found {
		t.Fatal(err)
	}
	for _, number := range []int64{-1, 0, 1, 4, 5, 8, 9, 12, 13, 16, 17} {
		r, e := temporal.ParseRational(big.NewInt(number).String()+"/1", temporal.Limits{})
		if e != nil {
			t.Fatal(e)
		}
		position, e := temporal.RationalPosition(binding.Axis(), r)
		if e != nil {
			t.Fatal(e)
		}
		cell := rangeMoveCellAt(t, activePages, activeMeta, position)
		absent := number >= 0 && number <= 16 && number%4 == 0
		expected, _ := state.NewRevision(10, 1)
		if absent {
			expected, _ = state.NewRevision(20, 1)
		}
		if cell.Present() == absent || cell.Revision() != expected || !absent && cell.Value().ID() != 2 {
			t.Fatal("split activation replaced exact source cell/revision", number, cell)
		}
	}
	if activatedIndex.own.count != 14 || activatedIndex.canonical.count != 14 || activatedIndex.declared.count != 14 || len(effects.Groups) != 0 || len(effects.Delta.Patches) != 0 {
		t.Fatal("split activation lost endpoint facts or invented CDC")
	}
	_ = target.Close()

	for _, bad := range []RangeMoveCursor{{Binding: descriptor.binding(), Family: rangeMoveComponents, Digest: [32]byte{1}, Key: []byte{1}}, {Binding: descriptor.binding(), Family: rangeMoveComponents, Digest: [32]byte{1}, Key: binary.BigEndian.AppendUint64(bytes.Repeat([]byte{1}, 58), 1)}} {
		page, _, err := ExportRangeMovePage(t.Context(), current[0], descriptor, bad, raftlog.ReadBudget{Rows: 1, Bytes: 4160}, Limits{}, g, transactionTestOutput(t))
		if !errors.Is(err, ErrInvalid) || page.Payload != nil {
			t.Fatal("malformed component key admitted", err)
		}
	}
}

// These are damaged copies of genuine completed source rows, not fabricated
// successful native proof. Refusal must expose no target writes or partial state.
func rangeMoveRejectDamagedComponent(t *testing.T, target *raftlog.ApplicationView, d RangeMoveDescriptor, prior RangeMoveImportState, page RangeMovePage, g GraphLimits) {
	t.Helper()
	for _, edit := range []func(*RangeMovePage){
		func(p *RangeMovePage) { p.Key = bytes.Clone(p.Key); p.Key[1] ^= 1 },
		func(p *RangeMovePage) { p.Key = bytes.Clone(p.Key); binary.BigEndian.PutUint64(p.Key[25:33], 0) },
		func(p *RangeMovePage) {
			c := cursor{src: p.Payload}
			axis, _ := c.field(2048)
			owned, _ := c.field(1 << 20)
			snapshot, _ := c.field(1 << 20)
			axis = bytes.Clone(axis)
			axis[16] = 0
			p.Payload, _ = rangeMoveBuild(transactionTestOutput(t), rangeMoveMaxLogicalBytes, func(w *rangeMoveWriter) { w.field(axis); w.field(owned); w.field(snapshot) })
		},
		func(p *RangeMovePage) {
			c := cursor{src: p.Payload}
			axis, _ := c.field(2048)
			_, _ = c.field(1 << 20)
			snapshot, _ := c.field(1 << 20)
			p.Payload, _ = rangeMoveBuild(transactionTestOutput(t), rangeMoveMaxLogicalBytes, func(w *rangeMoveWriter) { w.field(axis); w.field([]byte{1}); w.field(snapshot) })
		},
		func(p *RangeMovePage) {
			c := cursor{src: p.Payload}
			axis, _ := c.field(2048)
			owned, _ := c.field(1 << 20)
			p.Payload, _ = rangeMoveBuild(transactionTestOutput(t), rangeMoveMaxLogicalBytes, func(w *rangeMoveWriter) { w.field(axis); w.field(owned); w.add([]byte{0, 0, 0}) })
		},
		func(p *RangeMovePage) {
			c := cursor{src: p.Payload}
			axis, _ := c.field(2048)
			_, _ = c.field(1 << 20)
			snapshot, _ := c.field(1 << 20)
			ac := cursor{src: axis}
			a, _ := decodeAxis(&ac, DefaultLimits())
			empty, _ := temporal.Empty(a)
			owned, _ := temporal.AppendScope(nil, empty, temporal.Limits{})
			p.Payload, _ = rangeMoveBuild(transactionTestOutput(t), rangeMoveMaxLogicalBytes, func(w *rangeMoveWriter) { w.field(axis); w.field(owned); w.field(snapshot) })
		},
		func(p *RangeMovePage) {
			c := cursor{src: p.Payload}
			axis, _ := c.field(2048)
			_, _ = c.field(1 << 20)
			snapshot, _ := c.field(1 << 20)
			ac := cursor{src: axis}
			a, _ := decodeAxis(&ac, DefaultLimits())
			r, _ := temporal.ParseRational("-12345/1", temporal.Limits{})
			position, _ := temporal.RationalPosition(a, r)
			point, _ := temporal.Point(position)
			owned, _ := temporal.AppendScope(nil, point, temporal.Limits{})
			p.Payload, _ = rangeMoveBuild(transactionTestOutput(t), rangeMoveMaxLogicalBytes, func(w *rangeMoveWriter) { w.field(axis); w.field(owned); w.field(snapshot) })
		},

		func(p *RangeMovePage) { p.Key = bytes.Clone(p.Key); binary.BigEndian.PutUint64(p.Key[25:33], 16) },
		func(p *RangeMovePage) { p.Payload = bytes.Clone(p.Payload); p.Payload = p.Payload[:3] },
		func(p *RangeMovePage) {
			c := cursor{src: p.Payload}
			axis, _ := c.field(2048)
			owned, _ := c.field(1 << 20)
			snapshot, _ := c.field(1 << 20)
			snapshot = bytes.Clone(snapshot)
			snapshot[0] ^= 1
			p.Payload, _ = rangeMoveBuild(transactionTestOutput(t), rangeMoveMaxLogicalBytes, func(w *rangeMoveWriter) { w.field(axis); w.field(owned); w.field(snapshot) })
		},
	} {
		bad := page
		edit(&bad)
		bad.RecordBytes = uint32(len(bad.Payload))
		bad.Next.Digest = rangeMoveRowDigest(bad.Before.Digest, bad.Kind, bad.Key, bad.Payload)
		bad.Next.Bytes = bad.Before.Bytes + uint64(len(bad.Key)+len(bad.Payload)+17)
		result, _, err := StageRangeMovePage(t.Context(), target, d, prior, bad, Limits{}, g, transactionTestBudget(), transactionTestOutput(t))
		if !errors.Is(err, ErrInvalid) && !errors.Is(err, ErrCorrupt) && !errors.Is(err, state.ErrInvalidEncoding) {
			t.Fatal("damaged component was admitted", err)
		}
		if result.Writes != nil || result.Next.Sequence != 0 {
			t.Fatal("damaged component exposed partial install")
		}
	}

	cases := []Limits{{MaxStageRecords: 1}}
	body := cursor{src: page.Payload}
	_, _ = body.field(2048)
	owned, _ := body.field(1 << 20)
	if len(owned) > 128 {
		cases = append(cases, Limits{Temporal: temporal.Limits{MaxValueBytes: 128}})
	}
	for _, limits := range cases {
		result, _, err := StageRangeMovePage(t.Context(), target, d, prior, page, limits, g, transactionTestBudget(), transactionTestOutput(t))
		if !errors.Is(err, ErrResourceLimit) || result.Writes != nil {
			t.Fatal("bounded component refusal exposed partial writes", err)
		}
	}
}

func rangeMoveCellAt(t *testing.T, p *pageReader, m componentMeta, position temporal.Position) state.Cell {
	t.Helper()
	id := m.Root
	for depth := 0; depth < 8; depth++ {
		directory, err := p.directory(id, m.Key, m.Axis)
		if err != nil {
			t.Fatal(err)
		}
		if directory.Level == 0 {
			snapshot, err := rangeMoveState(p, directory)
			if err != nil {
				t.Fatal(err)
			}
			cell, err := snapshot.At(position, p.limits.stateLimits(p.q.c))
			if err != nil {
				t.Fatal(err)
			}
			return cell
		}
		selected := uint64(0)
		for _, child := range directory.Children {
			inside, err := child.Owned.Contains(position, p.q.c.limits.Temporal)
			if err != nil {
				t.Fatal(err)
			}
			if inside {
				if selected != 0 {
					t.Fatal("overlapping staged directory ownership")
				}
				selected = child.ID
			}
		}
		if selected == 0 {
			t.Fatal("staged directory omitted point")
		}
		id = selected
	}
	t.Fatal("staged directory exceeded depth")
	return state.Cell{}
}
