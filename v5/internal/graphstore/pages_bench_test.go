package graphstore

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func benchInstall(b *testing.B, db *raftlog.Store, root Root, s *Stage) (Root, uint64) {
	b.Helper()
	base, image, err := db.Checkpoint()
	if err != nil {
		b.Fatal(err)
	}
	root, err = root.AdvanceEffects(sha256.Sum256([]byte(fmt.Sprint(base))))
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := EncodeRoot(root)
	if err != nil {
		b.Fatal(err)
	}
	rows, err := s.Writes()
	if err != nil {
		b.Fatal(err)
	}
	index := base + 1
	e := &pb.Entry{Index: new(index), Term: new(uint64(2)), Type: pb.EntryNormal.Enum(), Data: []byte("bounded-benchmark")}
	if err := db.Persist(raft.Ready{HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(index)}, Entries: []*pb.Entry{e}}); err != nil {
		b.Fatal(err)
	}
	batch := raftlog.ApplicationBatch{BaseIndex: base, BaseImageHash: sha256.Sum256(image), Image: encoded, Writes: rows, Changes: []byte("bench-cdc"), Outcome: []byte("bench-ok")}
	if err := db.InstallApplication(index, batch); err != nil {
		b.Fatal(err)
	}
	return root, index
}

type benchmarkHistory struct {
	db           *raftlog.Store
	root         Root
	index        uint64
	current, old *PageReader
	query        graphstate.ComponentQuery
}

func benchmarkPages(b *testing.B, count int) benchmarkHistory {
	b.Helper()
	root, err := NewRoot(testNamespace(), 3)
	if err != nil {
		b.Fatal(err)
	}
	wire, err := EncodeRoot(root)
	if err != nil {
		b.Fatal(err)
	}
	db, err := raftlog.Open(raftlog.Config{Dir: "pages-bench", FS: vfs.NewMem(), Create: true, Application: raftlog.DefaultApplicationPolicy(1)})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	if err := db.Initialize([]uint64{1}, wire); err != nil {
		b.Fatal(err)
	}
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: temporal.ProfileIntegerZ, Version: 1, Reference: "source/clock@v1", CanonicalUnit: "exact-unit"}, temporal.Limits{})
	if err != nil {
		b.Fatal(err)
	}
	index := uint64(1)
	open := func(index uint64) (*Catalog, *Stage) {
		v, err := db.ApplicationView(index)
		if err != nil {
			b.Fatal(err)
		}
		c, err := OpenCatalog(v, testNamespace(), 3, Limits{})
		if err != nil {
			b.Fatal(err)
		}
		s, err := c.NewStage(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		return c, s
	}
	c, s := open(index)
	if err := s.Entity(b.Context(), refEntity(1), graphstate.EntityRecord{ID: 1, Kind: graphstate.Node, Axis: a}); err != nil {
		b.Fatal(err)
	}
	if err := s.Life(b.Context(), refLife(1, 11), graphstate.LifeRecord{Owner: 1, Life: 11}); err != nil {
		b.Fatal(err)
	}
	root, index = benchInstall(b, db, root, s)
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}
	if err := c.view.Close(); err != nil {
		b.Fatal(err)
	}
	key := graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}
	life, _ := state.NewValueRef(11, 0)
	var oldIndex uint64
	for start := 0; start < count; {
		end := min(start+4, count)
		if start == 0 {
			end = 1
		}
		c, s := open(index)
		patches := make([]graphstate.ComponentPatch, 0, end-start)
		for n := start; n < end; n++ {
			empty, err := state.New(a, state.Limits{})
			if err != nil {
				b.Fatal(err)
			}
			pos, err := temporal.IntegerPosition(a, temporal.Int64(int64(n)*3))
			if err != nil {
				b.Fatal(err)
			}
			w, err := temporal.Point(pos)
			if err != nil {
				b.Fatal(err)
			}
			rev, err := state.NewRevision(uint64(n+1), uint64(n+1001))
			if err != nil {
				b.Fatal(err)
			}
			result, err := empty.Set(w, life, rev, state.Limits{})
			if err != nil {
				b.Fatal(err)
			}
			patches = append(patches, graphstate.ComponentPatch{Key: key, Owned: w, State: result.State(), Changes: result.Changes()})
		}
		result, err := StageComponentPatches(b.Context(), s, root, patches, PageLimits{})
		if err != nil {
			b.Fatal(err)
		}
		root, index = benchInstall(b, db, result.Root, s)
		if start == 0 {
			oldIndex = index
		}
		if err := s.Close(); err != nil {
			b.Fatal(err)
		}
		if err := c.view.Close(); err != nil {
			b.Fatal(err)
		}
		start = end
	}
	reader := func(index uint64) *PageReader {
		v, err := db.ApplicationView(index)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if err := v.Close(); err != nil {
				b.Error(err)
			}
		})
		c, err := OpenCatalog(v, testNamespace(), 3, Limits{})
		if err != nil {
			b.Fatal(err)
		}
		p, err := NewPageReader(c, PageLimits{})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() {
			if err := p.Close(); err != nil {
				b.Error(err)
			}
		})
		return p
	}
	pos, _ := temporal.IntegerPosition(a, temporal.Int64(0))
	point, _ := temporal.Point(pos)
	return benchmarkHistory{db, root, index, reader(index), reader(oldIndex), graphstate.ComponentQuery{Key: key, Window: point}}
}
func BenchmarkComponentPointPaged(b *testing.B) {
	for _, count := range []int{64, 4096} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			history := benchmarkPages(b, count)
			current, old, query := history.current, history.old, history.query
			for name, p := range map[string]*PageReader{"current": current, "old": old} {
				b.Run(name, func(b *testing.B) {
					budget := graphstate.ReadBudget{Rows: 2, Bytes: 8192}
					var work PageWork
					for b.Loop() {
						page, err := p.ComponentPage(b.Context(), query, 0, budget)
						if err != nil {
							b.Fatal(err)
						}
						pieces := page.Data.Pieces()
						if !page.Complete || len(pieces) != 1 || pieces[0].Cell().Revision().ID() != 1 {
							b.Fatal("not exact historical point")
						}
						work = p.LastWork()
					}
					if work.DirectoryPages > 3 || work.PatchPages > 32 || work.DecodedCells > 96 {
						b.Fatalf("history-proportional work: %+v", work)
					}
					if name == "old" && (work.DirectoryPages != 1 || work.PatchPages != 1) {
						b.Fatalf("old read scanned future pages: %+v", work)
					}
					b.ReportMetric(float64(work.Records), "records/op")
					b.ReportMetric(float64(work.Bytes), "read-bytes/op")
					b.ReportMetric(float64(work.DirectoryPages), "directory-pages/op")
					b.ReportMetric(float64(work.PatchPages), "patch-pages/op")
					b.ReportMetric(float64(work.DecodedCells), "cells/op")
					b.ReportAllocs()
				})
			}
		})
	}
}

// Every depth has the identical64 native points, values and full revision cells.
// Only physical checkpoint/tail placement differs.32 adds the seal boundary.
func benchmarkTailHistory(b *testing.B, depth int) benchmarkHistory {
	b.Helper()
	h := benchmarkPages(b, 64)
	base, err := h.current.c.reader(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	q := pageReader{q: base, limits: DefaultPageLimits()}
	m, found, err := q.readMeta(h.query.Key)
	if err != nil || !found {
		b.Fatal(err)
	}
	d, err := q.directory(m.Root, m.Key, m.Axis)
	if err != nil {
		b.Fatal(err)
	}
	full, err := q.materialize(d)
	if err != nil {
		b.Fatal(err)
	}
	pieces := full.Pieces()
	checkpoint, err := state.New(m.Axis, state.Limits{})
	if err != nil {
		b.Fatal(err)
	}
	for _, piece := range pieces[:64-depth] {
		result, err := applyPageCell(checkpoint, piece.Scope(), piece.Cell(), state.Limits{})
		if err != nil {
			b.Fatal(err)
		}
		checkpoint = result.State()
	}
	stage, err := h.current.c.NewStage(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	root, checkpointID, err := h.root.ReservePhysical(1)
	if err != nil {
		b.Fatal(err)
	}
	d.Base, d.Head = checkpointID, 0
	d.TailRecords, d.TailBytes, d.TailAtoms = 0, 0, 0
	d.Cells = 64
	err = stage.operation(b.Context(), func(base *reader) error {
		encoded, err := encodeCheckpoint(root.namespace, checkpointID, m.Key, checkpoint, base.c, DefaultPageLimits())
		if err != nil {
			return err
		}
		if err := base.put(physicalKey(root.namespace, checkpointRecord, checkpointID), encoded); err != nil {
			return err
		}
		current := checkpoint
		for _, piece := range pieces[64-depth:] {
			result, err := applyPageCell(current, piece.Scope(), piece.Cell(), state.Limits{})
			if err != nil {
				return err
			}
			next, id, err := root.ReservePhysical(1)
			if err != nil {
				return err
			}
			root = next
			p := patchPage{ID: id, Previous: d.Head, Key: m.Key, Owned: d.Owned, Changes: result.Changes()}
			encoded, err := encodePatch(root.namespace, p, base.c, DefaultPageLimits())
			if err != nil {
				return err
			}
			if err := base.put(physicalKey(root.namespace, patchRecord, id), encoded); err != nil {
				return err
			}
			d.Head = id
			d.TailRecords++
			d.TailBytes += len(encoded)
			d.TailAtoms += len(p.Changes)
			current = result.State()
		}
		same, err := equalState(current, full, base.c, DefaultPageLimits())
		if err != nil {
			return err
		}
		if !same {
			return ErrPatchConflict
		}
		encoded, err = encodeDirectory(root.namespace, d, base.c, DefaultPageLimits())
		if err != nil {
			return err
		}
		return base.put(physicalKey(root.namespace, directoryRecord, d.ID), encoded)
	})
	if err != nil {
		b.Fatal(err)
	}
	h.root, h.index = benchInstall(b, h.db, root, stage)
	if err := stage.Close(); err != nil {
		b.Fatal(err)
	}
	v, err := h.db.ApplicationView(h.index)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := v.Close(); err != nil {
			b.Error(err)
		}
	})
	c, err := OpenCatalog(v, testNamespace(), 3, Limits{})
	if err != nil {
		b.Fatal(err)
	}
	h.current, err = NewPageReader(c, PageLimits{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := h.current.Close(); err != nil {
			b.Error(err)
		}
	})
	return h
}
func BenchmarkComponentTailDepth(b *testing.B) {
	for _, depth := range []int{0, 1, 4, 8, 16, 31, 32} {
		b.Run(fmt.Sprint(depth), func(b *testing.B) {
			h := benchmarkTailHistory(b, depth)
			budget := graphstate.ReadBudget{Rows: 2, Bytes: 8192}
			b.Run("read", func(b *testing.B) {
				var work PageWork
				for b.Loop() {
					page, err := h.current.ComponentPage(b.Context(), h.query, 0, budget)
					if err != nil || !page.Complete || page.Data.Pieces()[0].Cell().Revision().ID() != 1 {
						b.Fatal(err)
					}
					work = h.current.LastWork()
				}
				b.ReportMetric(float64(work.Records), "records/op")
				b.ReportMetric(float64(work.Bytes), "read-bytes/op")
				b.ReportMetric(float64(work.PatchPages), "patch-pages/op")
				b.ReportAllocs()
			})
			page, err := h.current.ComponentPage(b.Context(), h.query, 0, budget)
			if err != nil {
				b.Fatal(err)
			}
			rev, _ := state.NewRevision(999, 1001)
			next, err := page.Data.Set(h.query.Window, page.Data.Pieces()[0].Cell().Value(), rev, state.Limits{})
			if err != nil {
				b.Fatal(err)
			}
			patch := graphstate.ComponentPatch{Key: h.query.Key, Owned: h.query.Window, State: next.State(), Changes: next.Changes()}
			b.Run("one-cell-stage", func(b *testing.B) {
				var work PageWork
				encoded, checkpoints := 0, 0
				for b.Loop() {
					s, err := h.current.c.NewStage(b.Context())
					if err != nil {
						b.Fatal(err)
					}
					result, err := StageComponentPatches(b.Context(), s, h.root, []graphstate.ComponentPatch{patch}, PageLimits{})
					if err != nil {
						b.Fatal(err)
					}
					rows, err := s.Writes()
					if err != nil {
						b.Fatal(err)
					}
					encoded, checkpoints = 0, 0
					for _, row := range rows {
						encoded += len(row.Key) + len(row.Value)
						if row.Key[0] == byte(checkpointRecord) {
							checkpoints++
						}
					}
					work = result.Work
					if err := s.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(work.Records), "records/op")
				b.ReportMetric(float64(work.Bytes), "read-bytes/op")
				b.ReportMetric(float64(encoded), "encoded-write-bytes/op")
				b.ReportMetric(float64(checkpoints), "checkpoints/op")
				b.ReportAllocs()
			})
		})
	}
}
