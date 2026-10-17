package storeutil

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlogs 4, 7, 25: a delete tombstone that kept the ValidTo believed before
// the delete (types.TemporalMetadata.DeleteKeptValidTo, key "dk") is written
// at row format version KeptValidToWireFormatVersion so a binary that
// predates it (it skips "dk" and would read the kept close or the open end as
// the delete's) fails closed; every codec door carries the marker; a legacy
// tombstone (no marker) encodes and decodes exactly as before.

func keptTombstones(t *testing.T, kept bool) (*types.Node, *types.Relationship) {
	t.Helper()
	tm := func() *types.TemporalMetadata {
		// A scheduled close (900) after the delete (600), kept; the legacy
		// shape has the delete's clamp ValidTo == DeletedAt.
		if kept {
			return &types.TemporalMetadata{ValidFrom: 200, ValidTo: 900, TxFrom: 500, TxTo: 600, DeletedAt: 600, DeleteKeptValidTo: true}
		}
		return &types.TemporalMetadata{ValidFrom: 200, ValidTo: 600, TxFrom: 500, TxTo: 600, DeletedAt: 600}
	}
	n := types.NewNode(types.NodeID(snowflake.ID(6161)), 1, []uint16{2})
	n.SetVersion(2)
	n.SetProperties(mustPropertySlice(t, map[string]any{"s": "v"}))
	n.SetTemporal(tm())
	r := types.NewRelationship(types.RelID(snowflake.ID(6162)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(12)))
	r.SetVersion(2)
	r.SetProperties(mustPropertySlice(t, map[string]any{"s": "v"}))
	r.SetTemporal(tm())
	return n, r
}

// TestKeptValidToRow_FormatVersion catches an encoder that drops "dk" or keeps
// the old fv (an older binary reopens the kept close as delete-written), a
// decoder or scanner that loses the marker, a door that does not fail closed
// for a reader capped below the new version, and a legacy tombstone whose fv
// or reading changed. Not parallel: it moves the package's decode cap.
func TestKeptValidToRow_FormatVersion(t *testing.T) {
	for _, kept := range []bool{true, false} {
		n, r := keptTombstones(t, kept)
		for _, e := range []struct {
			name string
			n    *types.Node
			r    *types.Relationship
		}{{"node", n, nil}, {"rel", nil, r}} {
			for _, p := range markerPaths() {
				raw := p.enc(t, e.n, e.r)
				_, fv := fvOf(t, raw)
				if kept && fv != KeptValidToWireFormatVersion {
					t.Fatalf("kept %s %s: fv %d; want %d", e.name, p.name, fv, KeptValidToWireFormatVersion)
				}
				if !kept && fv != CurrentWireFormatVersion {
					t.Fatalf("legacy %s %s: fv %d; want %d as before", e.name, p.name, fv, CurrentWireFormatVersion)
				}
				got, err := p.dec(e.n != nil, raw)
				if err != nil || got.DeleteKeptValidTo != kept || got.DeletedAt != 600 {
					t.Fatalf("kept=%v %s %s: decoded %+v err=%v", kept, e.name, p.name, got, err)
				}
				// The legacy tombstone reads as delete-written (ValidTo ==
				// DeletedAt), the kept one never (partial slow decodes no ValidTo).
				if got.ValidTo != 0 && DeleteWroteValidTo(got) == kept {
					t.Fatalf("kept=%v %s %s: DeleteWroteValidTo = %v", kept, e.name, p.name, DeleteWroteValidTo(got))
				}
				restore := SetMaxReadableWireFormatVersionForTest(KeptValidToWireFormatVersion - 1)
				_, err = p.dec(e.n != nil, raw)
				restore()
				if kept && !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
					t.Fatalf("%s %s: older reader err = %v; want ErrWireFormatVersionUnsupported", e.name, p.name, err)
				}
				if !kept && err != nil {
					t.Fatalf("legacy %s %s: older reader err = %v", e.name, p.name, err)
				}
			}
		}
	}
}

// TestKeptValidToRow_ValidationRejects: the marker on a row without
// deleted_at is corruption (only a delete writes it), and so is the marker on
// a close before the delete (a delete keeps no such ValidTo marked).
func TestKeptValidToRow_ValidationRejects(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		mut  func(tm *types.TemporalMetadata)
	}{
		{"live", func(tm *types.TemporalMetadata) { tm.DeletedAt, tm.TxTo = 0, 0 }},
		{"close before the delete", func(tm *types.TemporalMetadata) { tm.ValidTo = 500 }},
	} {
		n, r := keptTombstones(t, true)
		tc.mut(n.Temporal())
		tc.mut(r.Temporal())
		nw, _ := NodeToWireChecked(n)
		if err := ValidateNodeWire(nw); err == nil {
			t.Fatalf("node %s: marker accepted", tc.name)
		}
		rw, _ := RelToWireChecked(r)
		if err := ValidateRelWire(rw); err == nil {
			t.Fatalf("rel %s: marker accepted", tc.name)
		}
	}
	n, r := keptTombstones(t, true)
	nw, _ := NodeToWireChecked(n)
	rw, _ := RelToWireChecked(r)
	if err := ValidateNodeWire(nw); err != nil {
		t.Fatalf("kept node tombstone rejected: %v", err)
	}
	if err := ValidateRelWire(rw); err != nil {
		t.Fatalf("kept rel tombstone rejected: %v", err)
	}
}
