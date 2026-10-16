package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Archived-format fixture assembly is deliberately private to this test. It
// models the exact prior four-root image; production exposes no downgrade or
// implicit migration. Logical initialization data remains the original GCD1.
func archivedFormat2Initialization(t *testing.T, initialized raftlog.ApplicationBatch) raftlog.ApplicationBatch {
	t.Helper()
	out := initialized
	out.Image = owned(initialized.Image)
	if len(out.Image) != 140 || binary.BigEndian.Uint64(out.Image[100:108]) != 3 || binary.BigEndian.Uint64(out.Image[44:52]) != 6 {
		t.Fatal("unexpected fresh physical format3 root")
	}
	binary.BigEndian.PutUint64(out.Image[100:108], 2)
	binary.BigEndian.PutUint64(out.Image[44:52], 5)
	checksum := sha256.Sum256(out.Image[:108])
	copy(out.Image[108:], checksum[:])
	out.Writes = make([]raftlog.KV, 0, len(initialized.Writes)-1)
	foundDescriptor, foundOwn := false, false
	for _, row := range initialized.Writes {
		if len(row.Value) >= 4 && bytes.Equal(row.Value[:4], []byte{'G', 'C', 1, 0x15}) {
			foundOwn = true
			continue
		}
		if len(row.Value) == 216 && bytes.Equal(row.Value[:4], []byte{'G', 'C', 1, 0x11}) {
			row.Value = owned(row.Value[:160])
			row.Value[29] = 2
			binary.BigEndian.PutUint64(row.Value[56:64], 2)
			foundDescriptor = true
		}
		out.Writes = append(out.Writes, row)
	}
	if !foundDescriptor || !foundOwn {
		t.Fatal("archived four-root fixture incomplete")
	}
	return out
}

func TestMaterializerPhysicalFormats2And3PreserveLogicalInitializationAndWrites(t *testing.T) {
	var logicalChanges []byte
	var logicalEffect [32]byte
	var mutationChanges []byte
	var mutationEffect [32]byte
	for _, format := range []uint64{2, 3} {
		t.Run(string(rune('0'+format)), func(t *testing.T) {
			f := openGraphFixture(t)
			init := graphInit(t)
			init.schemas = []graphstate.PropertyDefinition{{Name: "answer", Owner: graphstate.Node, Type: graphstate.ScalarScope, Cardinality: graphstate.ScalarCardinality}}
			wire, err := encodeGraphRequest(init, f.m.limits)
			if err != nil {
				t.Fatal(err)
			}
			batch, err := f.stage(t, wire)
			if err != nil {
				t.Fatal(err)
			}
			if format == 2 {
				batch = archivedFormat2Initialization(t, batch)
			}
			initialized := f.install(t, wire, batch)
			root, err := graphstore.DecodeRoot(batch.Image)
			if err != nil {
				t.Fatal(err)
			}
			topology, err := root.SinglePartition()
			if err != nil || topology.IndexVersion != format {
				t.Fatal(topology, err)
			}
			if logicalChanges == nil {
				logicalChanges = owned(batch.Changes)
				logicalEffect = root.EffectDigest()
			} else if !bytes.Equal(logicalChanges, batch.Changes) || logicalEffect != root.EffectDigest() {
				t.Fatal("physical capability changed logical initialization/digest")
			}
			// Restoration validates actual descriptor/root records for both images;
			// the logical semantic identifier alone is not a mixed-version guarantee.
			if _, err := newMaterializer(f.s, f.n, 1, f.m.limits); err != nil {
				t.Fatal("physical image could not restore", format, err)
			}
			f.control(t, codecRequests(t)[2])
			reserve := codecRequests(t)[3]
			reserve.count = 16
			reserve.sequence = 1
			f.control(t, reserve)
			request := graphCodecRequest(t)
			request.operations = append(request.operations, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 14, Life: 15, Scope: request.operations[0].Scope, Record: graphstate.EntityRecord{Type: "R", Source: 11, Target: 11, Mode: graphstate.IdentityReference}})
			request.claims = append(request.claims, freshBinding{role: entityBinding, id: 14, grant: grantReference{session: codecSession(), sequence: 1}}, freshBinding{role: lifeBinding, owner: 14, id: 15, grant: grantReference{session: codecSession(), sequence: 1}})
			_, mutation := f.commit(t, request)
			mutated, err := graphstore.DecodeRoot(mutation.Image)
			if err != nil {
				t.Fatal(err)
			}
			if mutationChanges == nil {
				mutationChanges = owned(mutation.Changes)
				mutationEffect = mutated.EffectDigest()
			} else if !bytes.Equal(mutationChanges, mutation.Changes) || mutationEffect != mutated.EffectDigest() {
				t.Fatal("physical index changed mutation CDC/effect digest")
			}
			topology, err = mutated.SinglePartition()
			if err != nil || topology.IndexVersion != format {
				t.Fatal("write silently changed physical capability", topology, err)
			}
			if format == 2 {
				for _, row := range mutation.Writes {
					if len(row.Value) >= 4 && bytes.Equal(row.Value[:4], []byte{'G', 'C', 1, 0x15}) {
						t.Fatal("legacy write invented own-presence coverage")
					}
				}
			}
			at, err := temporal.RationalPosition(request.operations[0].Scope.Axis(), temporal.RationalInt64(0))
			if err != nil {
				t.Fatal(err)
			}
			before, _, err := f.s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			for _, index := range []uint64{initialized, before} {
				raw, err := f.s.ApplicationView(index)
				if err != nil {
					t.Fatal(err)
				}
				catalog, err := graphstore.OpenCatalog(raw, graphstore.Namespace{Graph: graphstate.GraphID(f.n.graph), Partition: f.n.partition}, 1, f.m.limits.catalog)
				if err != nil {
					t.Fatal(err)
				}
				view, err := graphstore.OpenReadView(t.Context(), catalog, f.m.limits.graph)
				if err != nil {
					t.Fatal(err)
				}
				read, err := view.Entity(t.Context(), 11)
				if err != nil || read.Found != (index == before) {
					t.Fatal("retained image did not preserve historical graph state", index, read, err)
				}
				page, err := view.IncidentAt(t.Context(), graphstore.IncidentAtQuery{Endpoint: 11, At: at, Direction: graphstore.IncidentBoth, Visible: graphstate.Declared}, 0, graphstate.ReadBudget{Rows: 8, Bytes: 4096})
				if format == 2 {
					if !errors.Is(err, graphstore.ErrTopologyUnsupported) || page.Next != 0 || len(page.Candidates) != 0 {
						t.Fatal("legacy absence silently advertised empty current coverage", page, err)
					}
				} else if index == initialized {
					if !errors.Is(err, graphstore.ErrInvalid) {
						t.Fatal("unregistered query axis was accepted", err)
					}
				} else if err != nil || !page.Complete || len(page.Candidates) != 1 || page.Candidates[0] != (graphstore.IncidentAtCandidate{Relationship: 14, Life: 15, Roles: graphstore.IncidentBoth}) {
					t.Fatal("fresh current empty query failed", page, err)
				}
				_ = view.Close()
				_ = raw.Close()
			}
			if _, err := newMaterializer(f.s, f.n, 1, f.m.limits); err != nil {
				t.Fatal("mutated physical image could not restore", format, err)
			}
		})
	}
}
