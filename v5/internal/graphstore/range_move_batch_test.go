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
)

// Authored direct guards; these source fixtures are not executed qualification.
func TestRangeMoveBatchOwnWritesFragmentsAndRetainedCut(t *testing.T) {
	sources, declaration, axis, configuration, stores := transactionViewFixture(t)
	all, err := temporal.All(axis.Axis())
	if err != nil {
		t.Fatal(err)
	}
	view, _, err := OpenPartitionTransactionView(t.Context(), sources, declaration, axis, configuration, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	ops := []graphstate.Operation{{Kind: graphstate.CreateNode, Owner: 1, Life: 2, Scope: all}, {Kind: graphstate.CreateNode, Owner: 17, Life: 18, Scope: all}, {Kind: graphstate.CreateRelationship, Owner: 3, Life: 4, Scope: all, Record: graphstate.EntityRecord{Type: "CROSS", Source: 1, Target: 17, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 18}}, {Kind: graphstate.Set, Owner: 3, Life: 4, Name: "p", Value: graphstate.I64(5), ValueID: 5, Scope: all}}
	revision, _ := state.NewRevision(10, 1)
	template, _, err := PlanPartitionTransaction(t.Context(), view, ops, revision)
	if err = errors.Join(err, view.Close()); err != nil {
		t.Fatal(err)
	}
	old := transactionInstallTemplate(t, sources, stores, template, revision)
	view, _, err = OpenPartitionTransactionView(t.Context(), old, declaration, axis, configuration, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	changed, _ := state.NewRevision(20, 1)
	template, _, err = PlanPartitionTransaction(t.Context(), view, []graphstate.Operation{{Kind: graphstate.Set, Owner: 3, Life: 4, Name: "p", Value: graphstate.I64(6), ValueID: 6, Scope: all}}, changed)
	if err = errors.Join(err, view.Close()); err != nil {
		t.Fatal(err)
	}
	current := transactionInstallTemplate(t, old, stores, template, changed)
	if current[1] == nil {
		current[1] = old[1]
	}
	// Query/export C5 after genuine C6 mutation; never substitute current data.
	d := rangeMoveTestSeal(t, old[0], rangeMoveTestPlan(t, []*raftlog.ApplicationView{old[0], current[1]}, declaration, axis, configuration))

	// Direct public exporter: genuine source, complete framing, shared work.
	packed, work, err := ExportRangeMoveBatch(t.Context(), old[0], d, RangeMoveCursor{}, raftlog.ReadBudget{Rows: 32, Bytes: 16 << 10}, Limits{}, GraphLimits{}, transactionTestOutput(t))
	if err != nil || len(packed.Pages) == 0 || work.Records == 0 || work.Bytes == 0 {
		t.Fatal("direct bounded exporter", len(packed.Pages), work, err)
	}
	for step := 0; len(packed.Pages) < 2 && step < 8; step++ {
		cursor := packed.Pages[len(packed.Pages)-1].Next
		packed, work, err = ExportRangeMoveBatch(t.Context(), old[0], d, cursor, raftlog.ReadBudget{Rows: 32, Bytes: 16 << 10}, Limits{}, GraphLimits{}, transactionTestOutput(t))
		if err != nil || len(packed.Pages) == 0 {
			t.Fatal("next family export", work, err)
		}
	}
	if len(packed.Pages) < 2 {
		t.Fatal("actual byte packing never grouped rows")
	}
	wire, err := EncodeRangeMoveBatch(packed, transactionTestOutput(t), 16<<10)
	if err != nil || len(wire) > 16<<10 {
		t.Fatal("actual packed wire", len(wire), err)
	}
	_, firstWork, err := ExportRangeMovePage(t.Context(), old[0], d, RangeMoveCursor{}, raftlog.ReadBudget{Rows: 1, Bytes: (16 << 10) - rangeMoveBatchFraming - 4 - 2048}, Limits{}, GraphLimits{}, transactionTestOutput(t))
	if err != nil || firstWork.Records < 1 {
		t.Fatal("first actual source grant", firstWork, err)
	}
	for _, pageOnly := range []bool{false, true} {
		limited := DefaultGraphLimits()
		if pageOnly {
			limited.Pages.MaxWorkRecords = firstWork.Records
			limited.MaxSourceRows = 16384
		} else {
			limited.MaxSourceRows = firstWork.Records
			limited.Pages.MaxWorkRecords = 16384
		}
		refused, work, err := ExportRangeMoveBatch(t.Context(), old[0], d, RangeMoveCursor{}, raftlog.ReadBudget{Rows: 32, Bytes: 16 << 10}, Limits{}, limited, transactionTestOutput(t))
		if !errors.Is(err, ErrResourceLimit) || refused.Pages != nil || work.Records != firstWork.Records {
			t.Fatal("aggregate exhausted remainder reset", pageOnly, firstWork, work, err)
		}
	}

	target := current[1]
	progress, _, err := BeginRangeMoveImport(t.Context(), target, d, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	for step := 0; !progress.PreparationComplete; step++ {
		if step >= 8 {
			t.Fatal("preparation EOF")
		}
		e, _, err := AdvanceRangeMoveImportPreparation(t.Context(), target, d, progress, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		target = rangeMoveTestCommit(t, stores[1], e)
		progress = e.Next
	}
	var cursor RangeMoveCursor
	multi, siblingFragments := false, false
	for step := 0; !progress.Complete; step++ {
		if step >= 64 {
			t.Fatal("bounded graph EOF")
		}
		// Small genuine source pages force fragments, including sibling reads
		// through the same pending map; later native batches reopen progress.
		batch := RangeMoveBatch{}
		for len(batch.Pages) < 8 {
			page, _, err := ExportRangeMovePage(t.Context(), old[0], d, cursor, raftlog.ReadBudget{Rows: 1, Bytes: 4160}, Limits{}, GraphLimits{}, transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			if len(batch.Pages) > 0 && page.Offset > 0 {
				siblingFragments = true
			}
			batch.Pages = append(batch.Pages, page)
			cursor = page.Next
			if page.Complete {
				break
			}
		}
		multi = multi || len(batch.Pages) > 1
		wire, err := EncodeRangeMoveBatch(batch, transactionTestOutput(t), 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodeRangeMoveBatch(wire, transactionTestOutput(t), 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		if step == 0 {
			bad := RangeMoveBatch{Pages: []RangeMovePage{batch.Pages[0], batch.Pages[0]}}
			if _, err := EncodeRangeMoveBatch(bad, transactionTestOutput(t), 64<<10); !errors.Is(err, ErrInvalid) {
				t.Fatal("duplicate cursor", err)
			}
			first, err := EncodeRangeMovePage(batch.Pages[0], transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			forged, err := rangeMoveEncode(transactionTestOutput(t), 64<<10, func(w *rangeMoveWriter) {
				w.add([]byte("RMB\x01"))
				w.add([]byte{0, 0, 0, 2})
				w.field(first)
				w.field(first)
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeRangeMoveBatch(forged, transactionTestOutput(t), 64<<10); !errors.Is(err, ErrCorrupt) {
				t.Fatal("resealed duplicate frame accepted", err)
			}
			if _, err := DecodeRangeMoveBatch(wire[:len(wire)-1], transactionTestOutput(t), 64<<10); !errors.Is(err, ErrCorrupt) {
				t.Fatal("truncated EOF", err)
			}
			bad = decoded
			bad.Pages[0].Next.Digest[0] ^= 1
			e, _, err := StageRangeMoveBatch(t.Context(), target, d, progress, bad, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if !errors.Is(err, ErrInvalid) || e.Writes != nil {
				t.Fatal("bad predecessor leaked writes", err)
			}
			decoded, err = DecodeRangeMoveBatch(wire, transactionTestOutput(t), 64<<10)
			if err != nil {
				t.Fatal(err)
			}
		}
		e, _, err := StageRangeMoveBatch(t.Context(), target, d, progress, decoded, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
		if err != nil || len(e.Writes) > 128 {
			t.Fatal("shared native stage", err)
		}
		target = rangeMoveTestCommit(t, stores[1], e)
		control, found, _, err := target.GetControl(t.Context(), binary.BigEndian.AppendUint64([]byte("range-move-progress"), e.Base.Index+1), raftlog.ReadBudget{Rows: 1, Bytes: 2 << 20})
		if err != nil || !found {
			t.Fatal("co-committed progress", err)
		}
		progress, err = DecodeRangeMoveImportState(control.Value, transactionTestOutput(t))
		if err != nil {
			t.Fatal(err)
		}
		root, err := target.Root()
		if err != nil || !bytes.Equal(root.Image, d.Target.Image[:]) {
			t.Fatal("early root publication", err)
		}
	}
	if !multi || !siblingFragments {
		t.Fatal("missing genuine batch/fragment coverage")
	}
	sealed, _, err := FinishRangeMoveImport(t.Context(), target, d, progress, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil {
		t.Fatal(err)
	}
	e, _, err := ActivateRangeMoveImport(t.Context(), target, d, sealed, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
	if err != nil || len(e.Groups) != 0 {
		t.Fatal("activation invented CDC", err)
	}
	image, err := EncodeRoot(e.Root)
	if err != nil {
		t.Fatal(err)
	}
	installed := ownershipView(t, stores[1], ownershipInstallRaw(t, stores[1], image, e.Writes))
	readRevision := func(v *raftlog.ApplicationView) state.Revision {
		r, err := v.Root()
		if err != nil {
			t.Fatal(err)
		}
		root, err := DecodeRoot(r.Image)
		if err != nil {
			t.Fatal(err)
		}
		c := &Catalog{view: v, root: root, rootImageBytes: 172, limits: DefaultLimits(), hash: equalityDigest}
		q, err := c.reader(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		index, found, err := q.fullDescriptor(root)
		if err != nil || !found {
			t.Fatal(err)
		}
		q.fullView = &index
		p := &pageReader{q: q, limits: DefaultPageLimits()}
		k := graphstate.ComponentKey{Owner: 3, Life: 4, Kind: graphstate.ScalarProperty, Name: "p"}
		m, found, err := p.readMeta(k)
		if err != nil || !found {
			t.Fatal(err)
		}
		leaf, err := p.directory(m.Root, k, axis.Axis())
		if err != nil {
			t.Fatal(err)
		}
		s, err := rangeMoveState(p, leaf)
		if err != nil || len(s.Pieces()) != 1 {
			t.Fatal(err)
		}
		return s.Pieces()[0].Cell().Revision()
	}
	if readRevision(installed) != revision || readRevision(current[0]) != changed {
		t.Fatal("C5 replaced by C6 or vice versa")
	}
}
