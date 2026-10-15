package graphapply

import (
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// Fresh typed-row semantics explicitly include real close tombstones, alternate
// endpoint lives and per-fact canonical definitions. The legacy5/6 row recipe
// is neither widened nor used as acceptance for this different schedule.
func nativeGraphEdgeUniqueColumnRow(spec nativeGraphCostSpec, row nativeColumnRow) (int, error) {
	if nativeGraphEdgeUniqueValidate(spec) != nil || len(row.Axis) == 0 || len(row.Scope) == 0 || row.After.Revision == 0 || row.After.Revision <= row.Before.Revision || row.After.Null {
		return 0, errCorrupt
	}
	n := spec.Facts
	if row.Kind == byte(graphstate.Node) {
		if row.Component != byte(graphstate.Presence) || row.Life != 0 || row.Name != "" || row.Type != "" || row.Source != 0 || row.Target != 0 || row.SourceLife != 0 || row.TargetLife != 0 {
			return 0, errCorrupt
		}
		oldLife, phase := uint64(2), uint64(1)
		switch row.Owner {
		case 1:
		case 3:
			oldLife, phase = 4, 2
		case spec.remoteNode():
			oldLife, phase = spec.remoteNode()+1, 3
		default:
			return 0, errCorrupt
		}
		if row.After.Provenance == phase && row.After.Present && row.After.Handle == oldLife && row.Before == (nativeColumnCell{}) {
			return 0, nil
		}
		if row.Owner == 1 {
			return 0, errCorrupt
		}
		lifePhase, newLife := uint64(4+6*n), 4*n+7
		if row.Owner == spec.remoteNode() {
			lifePhase += 2
			newLife = spec.remoteNode() + 2
		}
		if row.After.Provenance == lifePhase && !row.After.Present && row.After.Handle == 0 && row.Before.Present && row.Before.Handle == oldLife && row.Before.Provenance == phase {
			return 0, nil
		}
		if row.After.Provenance == lifePhase+1 && row.After.Present && row.After.Handle == newLife && !row.Before.Present && row.Before.Handle == 0 && row.Before.Provenance == lifePhase {
			return 0, nil
		}
		return 0, errCorrupt
	}
	if row.Kind != byte(graphstate.Relationship) || row.Owner < 7 || (row.Owner-7)%2 != 0 {
		return 0, errCorrupt
	}
	fact, candidate := (row.Owner-7)/4, ((row.Owner-7)%4)/2
	if fact >= n || row.Type != "edge" || row.Mode != byte(graphstate.LifeBound) || row.Source != 1 || row.SourceLife != 2 {
		return 0, errCorrupt
	}
	target, oldLife, newLife := uint64(3), uint64(4), 4*n+7
	if spec.cross(fact) {
		target, oldLife, newLife = spec.remoteNode(), spec.remoteNode()+1, spec.remoteNode()+2
	}
	if row.Target != target {
		return 0, errCorrupt
	}
	claim, replace, close := 4+2*fact+candidate, 4+3*n+fact, 4+5*n+fact
	if row.After.Provenance == close {
		if row.Component != byte(graphstate.Presence) || row.Life != 0 || row.Name != "" || row.After.Present || row.After.Handle != 0 || !row.Before.Present || row.Before.Handle != row.Owner+1 || row.Before.Provenance != claim || row.TargetLife != oldLife {
			return 0, errCorrupt
		}
		return 1, nil
	}
	if row.After.Provenance != claim && row.After.Provenance != replace || !row.After.Present || row.Before != (nativeColumnCell{}) {
		return 0, errCorrupt
	}
	wantLife := oldLife
	if row.After.Provenance == replace {
		wantLife = newLife
	}
	if row.TargetLife != wantLife {
		return 0, errCorrupt
	}
	if row.Component == byte(graphstate.Presence) {
		if row.Life == 0 && row.Name == "" && row.After.Handle == row.Owner+1 {
			return 1, nil
		}
		return 0, errCorrupt
	}
	if row.Component != byte(graphstate.ScalarProperty) || row.Life != row.Owner+1 || row.Name != "p" || row.After.Handle != 4*n+8+fact || row.After.Value != int64(fact+5) {
		return 0, errCorrupt
	} // #nosec G115 -- fact<1M.
	return 2, nil
}

func nativeGraphEdgeUniqueColumnReadPair(t *testing.T, clients [2]*nativeGraphEdgeUniqueColumnClient, pair *nativeGraphCostColumnPair, spec nativeGraphCostSpec, at graphCut, cached, fill bool) {
	t.Helper()
	var census [3]uint64
	for group, client := range clients {
		r := client.call(nativeGraphCostColumnRequest{Action: nativeColumnsRead, At: nativeGraphCostColumnCutInput(t, at), Cached: cached, Fill: fill})
		if !nativeColumnSame(r.Manifest, pair.Manifests[group]) || r.Binding.Policy.Version != 1 || r.Binding.Policy.Admission == ([32]byte{}) || r.Binding.Version != 2 || r.Retention == nil || r.Retention.State != nativeColumnPinLive || r.EOF || len(r.Body)+len(r.Rows) != 0 {
			t.Fatal("fresh native generation/source admission")
		}
		initial, err := decodeGraphCertificate(r.Manifest.Generation.Sources[group].Initial)
		if err != nil {
			t.Fatal(err)
		}
		var selected graphCertificate
		for _, certificate := range at.certificates {
			if certificate.scope.partition == initial.scope.partition {
				selected = certificate
			}
		}
		position, ordinal := initial.sourceIndex, uint64(0)
		var frames, rows, retained uint64
		var chain [32]byte
		for {
			r = client.call(nativeGraphCostColumnRequest{Action: nativeColumnsNext})
			if !nativeColumnSame(r.Manifest, pair.Manifests[group]) || r.Ordinal != ordinal || r.Cached != cached || r.Fill != fill {
				t.Fatal("fresh column continuation substituted identity")
			}
			if r.EOF {
				m := pair.Manifests[group]
				if len(r.Body)+len(r.Rows) != 0 || ordinal != m.Chunks || frames != m.Frames || rows != m.Rows || retained != m.Bytes || position != m.Positions[group] || chain != m.Chain {
					t.Fatal("fresh native complete EOF")
				}
				break
			}
			decoded, err := decodeNativeColumnChunk(r.Manifest.Generation, ordinal, chain, r.Body, client.binding.Policy)
			if err != nil {
				t.Fatal(err)
			}
			selectedRows := []nativeColumnRow{}
			for _, frame := range decoded {
				if frame.Partition != initial.scope.partition || frame.Index != position+1 || frame.Outcome == nil || frame.CDC == nil {
					t.Fatal("fresh prefix omission")
				}
				position++
				frames++
				rows += uint64(len(frame.Rows))
				if frame.Index <= selected.sourceIndex {
					for _, row := range frame.Rows {
						class, e := nativeGraphEdgeUniqueColumnRow(spec, row)
						if e != nil {
							t.Fatal("fresh alternate-life typed row", row, e)
						}
						census[class]++
						selectedRows = append(selectedRows, row)
					}
				}
			}
			if !reflect.DeepEqual(selectedRows, r.Rows) {
				t.Fatal("native/cache receipt differs from binary typed page")
			}
			retained += uint64(len(nativeColumnChunkKey(pair.ID, ordinal)) + len(r.Body))
			chain = nativeColumnChain(chain, r.Body)
			ordinal++
		}
	}
	want := [3]uint64{3, spec.Facts, spec.Facts}
	if pair.Updated && reflect.DeepEqual(at, pair.Final) {
		want = [3]uint64{7, 3 * spec.Facts, 2 * spec.Facts}
	}
	if census != want {
		t.Fatal("fresh full old/current typed change census", census, want)
	}
}
