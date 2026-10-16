package graphapply

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This test-only probe reads actual native store state. Its outputs are inert
// inspection data, never a move/cut/installation authority constructor.
func graphRangeMoveNativeProbe(ctx context.Context, h *allocationHost, r processRequest) (response processResponse, err error) {
	s := h.machine.store
	switch r.op {
	case processRangeProgressProbe:
		response.aux, err = graphE17NativeMoveProgress(ctx, h, r.data)
		return response, err
	case processRangeSnapshotProbe:
		if err := s.PublishSnapshot(); err != nil {
			return response, err
		}
		cut, err := s.PublishedApplicationCut()
		if err != nil {
			return response, err
		}
		export, err := s.BeginPublishedApplicationExport(ctx, cut.ID)
		if err != nil {
			return response, err
		}
		defer func() { err = errors.Join(err, export.Close()) }()
		for pages := 0; pages < 4096; pages++ {
			complete, e := export.BuildManifest(ctx, raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
			if e != nil {
				return response, e
			}
			if complete {
				manifest, e := export.Manifest()
				if e != nil {
					return response, e
				}
				response.aux, err = raftlog.EncodeApplicationSnapshotManifest(manifest)
				return response, err
			}
		}
		return response, errLimit
	case processRangeImportProbe:
		manifest, err := raftlog.DecodeApplicationSnapshotManifest(r.data, s.ApplicationLimits().MaxImageBytes)
		if err != nil {
			return response, err
		}
		imported, err := s.BeginApplicationImport(ctx, manifest)
		if err != nil {
			return response, err
		}
		return response, imported.Abort()
	case processRangeEvidenceProbe:
		view, err := s.ApplicationView(h.driver.Applied())
		if err != nil {
			return response, err
		}
		defer func() { err = errors.Join(err, view.Close()) }()
		cfg, err := h.currentConfiguration()
		if err != nil {
			return response, err
		}
		configuration, err := cfg.digest()
		if err != nil {
			return response, err
		}
		publication, owner, _, err := graphstore.LookupPublishedRange(ctx, view, graphstore.Namespace{Graph: graphstate.GraphID(h.machine.ns.graph), Partition: h.machine.ns.partition}, r.entity, configuration, graphstore.OwnershipBudget{SourceRows: 64, SourceBytes: 1 << 20, OutputBytes: 1 << 20})
		if err != nil {
			return response, err
		}
		p, err := graphstore.EncodeRangePublication(publication)
		if err != nil {
			return response, err
		}
		o, err := graphstore.EncodeRangeOwnership(owner)
		if err != nil {
			return response, err
		}
		response.aux, err = processEncoding(func(w *boundedWriter) { w.field(p); w.field(o) })
		return response, err
	default:
		return response, errInvalid
	}
}

// A logical handoff does not authorize foreign AS5 bank replacement. This
// genuine six-child refusal guard keeps the original immutable identity gate.
func TestGraphRangeMoveSixProcessesForeignSnapshotRefusesCrossIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedProcessNetwork(t)
	oracle := newCertifiedProcessOracle()
	input, err := encodeGraphProcessObserveIDs([]uint64{1, 3, 17})
	if err != nil {
		t.Fatal(err)
	}
	request, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadFresh})
	if err != nil {
		t.Fatal(err)
	}
	old := n.all[2].call(t, processRequest{op: processSourceCertifiedOpen, data: request})
	oldHandle, oldCut, err := decodeGraphE15ReadToken(old.aux)
	if err != nil {
		t.Fatal(err)
	}
	oldClosed := false
	defer func() {
		if !oldClosed {
			n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: oldHandle})
		}
	}()
	before := n.all[4].call(t, processRequest{op: processStateOp})
	source := n.all[1].call(t, processRequest{op: processRangeEvidenceProbe, entity: 1})
	cursor := graphCursor{b: source.aux}
	publication, err := graphstore.DecodeRangePublication(cursor.field(256))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := graphstore.DecodeRangeOwnership(cursor.field(256))
	if err != nil || cursor.err != nil || len(cursor.b) != 0 || publication.RangeID != 1 || publication.First != 1 || publication.Last != 16 || owner.Partition != 3 || owner.Fenced {
		t.Fatal("actual source range", publication, owner, err, cursor.err)
	}
	exported := n.all[1].call(t, processRequest{op: processRangeSnapshotProbe})
	manifest, err := raftlog.DecodeApplicationSnapshotManifest(exported.aux, 172)
	if err != nil || manifest.Version != 5 || manifest.Identity.Partition != 3 || manifest.Identity.Group != oldCut.certificates[0].scope.group {
		t.Fatal("actual source AS5 identity", manifest, err)
	}
	refused, refusal := n.all[4].exchange(t.Context(), processRequest{op: processRangeImportProbe, data: exported.aux})
	if !errors.Is(refusal, raftlog.ErrInvalid) || len(refused.aux) != 0 {
		t.Fatal("same-identity snapshot must refuse another owner", refusal, len(refused.aux))
	}
	after := n.all[4].call(t, processRequest{op: processStateOp})
	if after.applied != before.applied || !bytes.Equal(after.image, before.image) || after.stats != before.stats {
		t.Fatal("foreign snapshot refusal changed target state")
	}
	observed := n.all[2].call(t, processRequest{op: processSourceCertifiedObserve, index: oldHandle, data: input})
	facts, err := decodeGraphProcessObservationBatch(observed.aux)
	if err != nil {
		t.Fatal(err)
	}
	assertGraphE15IDs(t, facts, []uint64{1, 3, 17})
	for _, fact := range facts {
		if err := oracle.compare(fact.id, fact.fact); err != nil {
			t.Fatal("nonempty old cut changed", err)
		}
	}
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: oldHandle})
	oldClosed = true
	n.compare(t, oracle)
}
