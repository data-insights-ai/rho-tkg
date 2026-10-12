package graphstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// These are real native MVCC catalog records/tombstones. The storage fixture
// grants no graphapply cut, handoff or allocator authority, and no Scan result is
// replaced. Six invisible keys exceed one export call's remaining source work.
func TestRangeMoveNativeScanHolesAdvanceToExactRetainedValues(t *testing.T) {
	for _, kind := range []string{"tombstones", "future-only"} {
		t.Run(kind, func(t *testing.T) {
			sources, d, binding, cfg, stores := transactionViewFixture(t)
			n := Namespace{d.Graph(), 3}
			visible := []graphstate.ValueID{7, 9}
			initial := slices.Clone(visible)
			if kind == "tombstones" {
				initial = []graphstate.ValueID{1, 2, 3, 4, 5, 6, 7, 9}
			}
			base, err := sources[0].Root()
			if err != nil {
				t.Fatal(err)
			}
			writes := rangeMoveScanValueWrites(t, sources[0], initial)
			at := ownershipInstallRaw(t, stores[0], base.Image, writes)
			sources[0] = ownershipView(t, stores[0], at)
			if kind == "tombstones" {
				var deletes []raftlog.KV
				for id := graphstate.ValueID(1); id <= 6; id++ {
					canonical, err := graphstate.I64(int64(id) * 10).EqualityKey(DefaultLimits().valueLimits())
					if err != nil {
						t.Fatal(err)
					}
					hash := equalityDigest(canonical)
					for _, key := range [][]byte{valueKey(n, id), memberKey(n, hash, 0), bucketKey(n, hash)} {
						deletes = append(deletes, raftlog.KV{Key: key, Deleted: true})
					}
				}
				slices.SortFunc(deletes, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
				at = ownershipInstallRaw(t, stores[0], base.Image, deletes)
				sources[0] = ownershipView(t, stores[0], at)
			}
			descriptor := rangeMoveTestPlan(t, sources, d, binding, cfg)
			if kind == "future-only" {
				// A retained native root sees later physical keys but must not emit
				// their values. Acquire the old coordinate again after actual writes.
				future := rangeMoveScanValueWrites(t, sources[0], []graphstate.ValueID{1, 2, 3, 4, 5, 6})
				ownershipInstallRaw(t, stores[0], base.Image, future)
				sources[0] = ownershipView(t, stores[0], descriptor.Source.Index)
			}
			row, found, err := sources[0].Get(t.Context(), valueKey(n, 1), 64<<10)
			if err != nil || kind == "tombstones" && (!found || !row.Deleted) || kind == "future-only" && (found || row.Key != nil) {
				t.Fatal("native hole setup is invalid", kind, row, found, err)
			}
			lower := binary.BigEndian.AppendUint64(keyPrefix(n, valueRecord), 1)
			upper := binary.BigEndian.AppendUint64(keyPrefix(n, valueRecord), 17)
			var after []byte
			for id := graphstate.ValueID(1); id <= 6; id++ {
				page, err := sources[0].Scan(t.Context(), lower, upper, after, raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10})
				if err != nil || len(page.Rows) != 0 || page.Complete || page.Visited != 1 || !bytes.Equal(page.Next, valueKey(n, id)) || bytes.Compare(page.Next, after) <= 0 {
					t.Fatal("real native Scan did not produce advancing empty page", kind, id, page, err)
				}
				after = page.Next
			}
			provider, err := sources[0].Scan(t.Context(), lower, upper, after, raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10})
			if err != nil || len(provider.Rows) != 1 || !bytes.Equal(provider.Rows[0].Key, valueKey(n, 7)) {
				t.Fatal("native provider lost the actual value after the holes", provider, err)
			}
			limits := DefaultGraphLimits()
			limits.MaxSourceRows = 11
			var cursor RangeMoveCursor
			var ids []graphstate.ValueID
			complete := false
			for step := 0; step < 32; step++ {
				page, work, err := ExportRangeMovePage(t.Context(), sources[0], descriptor, cursor, raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}, Limits{}, limits, transactionTestOutput(t))
				if err != nil {
					t.Fatal("rangeMoveCatalogNext repeated invisible native key instead of exporting portable progress", kind, step, cursor.After, err)
				}
				encodedPage, err := EncodeRangeMovePage(page, transactionTestOutput(t))
				if err != nil {
					t.Fatal("actual progress page cannot cross codec", err)
				}
				page, err = DecodeRangeMovePage(encodedPage, transactionTestOutput(t))
				if err != nil {
					t.Fatal(err)
				}
				before := cursor
				if before.Family == 0 {
					before = rangeMoveStart(descriptor)
				}
				if !rangeMoveCursorEqual(page.Before, before) || page.Next.Sequence != before.Sequence+1 || work.Records > limits.MaxSourceRows {
					t.Fatal("export reset or exceeded the original operation allowance", page, work)
				}
				if page.Complete {
					complete = true
					cursor = page.Next
					break
				}
				if page.Kind == 0 {
					for _, edit := range []func(*RangeMovePage){func(p *RangeMovePage) { p.Next.Rows++ }, func(p *RangeMovePage) { p.Next.Digest[0] ^= 1 }, func(p *RangeMovePage) { p.Next.After = p.Before.After }} {
						bad := page
						edit(&bad)
						wire, e := EncodeRangeMovePage(bad, transactionTestOutput(t))
						if !errors.Is(e, ErrInvalid) || wire != nil {
							t.Fatal("scan progress invented data or skipped validation", e)
						}
					}
					// A bounded progress-only result is permitted; it must advance
					// its actual physical continuation without inventing a logical row.
					if page.RecordDone || page.RecordBytes != 0 || len(page.Payload) != 0 || len(page.Key) != 0 || page.Next.Rows != before.Rows || page.Next.Bytes != before.Bytes || page.Next.Digest != before.Digest || bytes.Compare(page.Next.After, before.After) <= 0 {
						t.Fatal("empty scan progress invented data or made no progress", page)
					}
				} else {
					if page.Kind != rangeMoveValues || !page.RecordDone || page.Offset != 0 {
						t.Fatal("unexpected record in value-only retained fixture", page)
					}
					entry, _, _, err := readValue(page.Payload, n, DefaultLimits())
					if err != nil {
						t.Fatal(err)
					}
					value, ok := entry.Value.Int64Value()
					if !ok || value != int64(entry.Ref.ID)*10 {
						t.Fatal("export substituted later value content", entry)
					}
					ids = append(ids, entry.Ref.ID)
				}
				encoded, err := EncodeRangeMoveCursor(page.Next, transactionTestOutput(t))
				if err != nil {
					t.Fatal(err)
				}
				cursor, err = DecodeRangeMoveCursor(encoded, transactionTestOutput(t))
				if err != nil {
					t.Fatal(err)
				}
				// Resume with a newly acquired genuine handle at the same old root.
				old := sources[0]
				sources[0] = ownershipView(t, stores[0], descriptor.Source.Index)
				if err = old.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if !complete || !slices.Equal(ids, visible) || cursor.Rows != 2 {
				t.Fatal("finite export omitted/duplicated records or emitted phantom holes", kind, ids, cursor.Rows, complete)
			}
			sealed, err := SealRangeMoveDescriptor(descriptor, cursor, transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			target := sources[1]
			progress, _, err := BeginRangeMoveImport(t.Context(), target, sealed, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			prepared, _, err := AdvanceRangeMoveImportPreparation(t.Context(), target, sealed, progress, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if err != nil {
				t.Fatal(err)
			}
			target = rangeMoveTestCommit(t, stores[1], prepared)
			progress = prepared.Next
			var receive RangeMoveCursor
			progressPages := 0
			for step := 0; step < 32; step++ {
				page, _, err := ExportRangeMovePage(t.Context(), sources[0], sealed, receive, raftlog.ReadBudget{Rows: 1, Bytes: 64 << 10}, Limits{}, limits, transactionTestOutput(t))
				if err != nil {
					t.Fatal(err)
				}
				wire, err := EncodeRangeMovePage(page, transactionTestOutput(t))
				if err != nil {
					t.Fatal(err)
				}
				page, err = DecodeRangeMovePage(wire, transactionTestOutput(t))
				if err != nil {
					t.Fatal(err)
				}
				effects, _, err := StageRangeMovePage(t.Context(), target, sealed, progress, page, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
				if err != nil {
					t.Fatal("durable target rejected real scan continuation", step, err)
				}
				if page.Kind == 0 && !page.Complete {
					progressPages++
					if len(effects.Writes) != 0 || effects.Next.Rows != progress.Rows || effects.Next.Bytes != progress.Bytes || effects.Next.Digest != progress.Digest || effects.Next.FutureRoot != progress.FutureRoot {
						t.Fatal("physical scan progress fabricated target records/effects")
					}
				}
				replacement := rangeMoveTestCommit(t, stores[1], effects)
				if err = target.Close(); err != nil {
					t.Fatal(err)
				}
				target = replacement
				record, found, _, err := target.GetControl(t.Context(), binary.BigEndian.AppendUint64([]byte("range-move-progress"), effects.Base.Index+1), raftlog.ReadBudget{Rows: 1, Bytes: 2 << 20})
				if err != nil || !found {
					t.Fatal(err)
				}
				progress, err = DecodeRangeMoveImportState(record.Value, transactionTestOutput(t))
				if err != nil {
					t.Fatal(err)
				}
				receive = page.Next
				if page.Complete {
					break
				}
				if step == 31 {
					t.Fatal("durable receive repeated a native hole")
				}
			}
			if progressPages != 6 {
				t.Fatal("did not durably admit all physical-only progress pages", progressPages)
			}
			finished, _, err := FinishRangeMoveImport(t.Context(), target, sealed, progress, Limits{}, GraphLimits{}, transactionTestBudget(), transactionTestOutput(t))
			if err != nil || finished.Rows != 2 {
				t.Fatal("native scan holes changed logical stream completion", err)
			}
			if err = target.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func rangeMoveScanValueWrites(t *testing.T, view *raftlog.ApplicationView, ids []graphstate.ValueID) []raftlog.KV {
	t.Helper()
	base, err := view.Root()
	if err != nil {
		t.Fatal(err)
	}
	root, err := DecodeRoot(base.Image)
	if err != nil {
		t.Fatal(err)
	}
	catalog := &Catalog{view: view, root: root, rootImageBytes: 172, limits: DefaultLimits(), hash: equalityDigest}
	reader, err := catalog.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	index, found, err := reader.fullDescriptor(root)
	if err != nil || !found {
		t.Fatal(err)
	}
	stage, err := allocateFullStage(catalog, fullStageState{root, index, DefaultPageLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	for _, id := range ids {
		if err = stage.Value(t.Context(), ValueRef{root.Namespace().Graph, id}, graphstate.I64(int64(id)*10)); err != nil {
			t.Fatal(err)
		}
	}
	writes, err := stage.Writes()
	if err != nil {
		t.Fatal(err)
	}
	return writes
}
