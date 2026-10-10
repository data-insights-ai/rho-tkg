package graphstore

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestFullIndexRootAndBindingCorruptionNeverReturnsPartialView(t *testing.T) {
	for _, mutation := range []string{"descriptor-missing", "descriptor-owner", "descriptor-format", "descriptor-count", "own-missing", "own-digest", "own-count", "own-id", "own-level", "own-family", "keys-missing", "unique-missing", "canonical-missing", "declared-missing", "canonical-role", "declared-role", "raw-canonical", "raw-owner", "raw-member", "metadata-missing"} {
		t.Run(mutation, func(t *testing.T) {
			f := newFullFixture(t, GraphLimits{})
			scope := f.span(t, 0, 10)
			f.apply(t, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 1, Life: 11, Scope: scope}, graphstate.Operation{Kind: graphstate.CreateNode, Owner: 2, Life: 11, Scope: scope})
			f.apply(t, graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 31, Scope: scope, Record: graphstate.EntityRecord{Type: "R", Source: 1, Target: 2, Mode: graphstate.LifeBound}, Binding: graphstate.LifeRecord{SourceLife: 11, TargetLife: 11}}, graphstate.Operation{Kind: graphstate.Add, Owner: 1, Life: 11, Scope: scope, Name: "set", Value: graphstate.I64(7), ValueID: 99})
			c := f.catalog(t, f.index)
			q, _ := c.reader(t.Context())
			d, _, err := q.fullDescriptor(c.root)
			if err != nil {
				t.Fatal(err)
			}
			p := pageReader{q: q, limits: DefaultPageLimits()}
			row := raftlog.KV{Key: componentIndexDescriptorKey(c.root.namespace)}
			wire, _ := encodeFullDescriptor(d, c.root, c.limits)
			row.Value = wire
			switch mutation {
			case "descriptor-missing":
				row.Deleted = true
				row.Value = nil
			case "descriptor-owner":
				binary.BigEndian.PutUint64(wire[32:40], 99)
			case "descriptor-format":
				binary.BigEndian.PutUint64(wire[56:64], 99)
			case "descriptor-count":
				binary.BigEndian.PutUint64(wire[104:112], d.unique.count+1)
			case "own-missing":
				row = raftlog.KV{Key: physicalKey(c.root.namespace, currentPresenceRecord, d.own.id), Deleted: true}
			case "own-digest":
				wire[184] ^= 1
			case "own-count":
				binary.BigEndian.PutUint64(wire[176:184], d.own.count+1)
			case "own-id":
				binary.BigEndian.PutUint64(wire[168:176], d.canonical.id)
			case "own-level":
				wire[161] = 8
			case "own-family":
				wire[160] = byte(canonicalIncidentRecord)
			case "keys-missing":
				row = raftlog.KV{Key: physicalKey(c.root.namespace, componentKeyTreeRecord, d.keys.id), Deleted: true}
			case "unique-missing":
				row = raftlog.KV{Key: physicalKey(c.root.namespace, uniquePostingRecord, d.unique.id), Deleted: true}
			case "canonical-missing":
				row = raftlog.KV{Key: physicalKey(c.root.namespace, canonicalIncidentRecord, d.canonical.id), Deleted: true}
			case "declared-missing":
				row = raftlog.KV{Key: physicalKey(c.root.namespace, declaredIncidentRecord, d.declared.id), Deleted: true}
			case "canonical-role", "declared-role":
				tree := d.canonical
				if mutation == "declared-role" {
					tree = d.declared
				}
				node, err := p.postingTreeRoot(tree, defaultComponentKeyTreeLimits())
				if err != nil {
					t.Fatal(err)
				}
				node.keys[0].roles = 2
				data, err := encodePostingKeyTreeNode(node, c, defaultComponentKeyTreeLimits())
				if err != nil {
					t.Fatal(err)
				}
				row = raftlog.KV{Key: physicalKey(c.root.namespace, tree.kind, tree.id), Value: data}
			case "raw-canonical", "raw-owner", "raw-member":
				node, err := p.postingTreeRoot(d.unique, defaultComponentKeyTreeLimits())
				if err != nil {
					t.Fatal(err)
				}
				switch mutation {
				case "raw-canonical":
					node.keys[0].value = 999
				case "raw-owner":
					node.keys[0].component.Owner = 99
				case "raw-member":
					node.keys[0].component.Member = 999
				}
				data, err := encodePostingKeyTreeNode(node, c, defaultComponentKeyTreeLimits())
				if err != nil {
					t.Fatal(err)
				}
				row = raftlog.KV{Key: physicalKey(c.root.namespace, d.unique.kind, d.unique.id), Value: data}
			case "metadata-missing":
				row = raftlog.KV{Key: componentKey(c.root.namespace, graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 99}), Deleted: true}
			}
			base, err := c.view.Root()
			if err != nil {
				t.Fatal(err)
			}
			f.install(t, GraphEffects{Base: base, Root: c.root, Writes: []raftlog.KV{row}})
			_ = c.view.Close()
			bad := f.catalog(t, f.index)
			v, err := OpenReadView(t.Context(), bad, GraphLimits{})
			constructorFailure := strings.HasPrefix(mutation, "own-") || mutation == "descriptor-missing" || mutation == "descriptor-owner" || mutation == "descriptor-format" || mutation == "descriptor-count" || mutation == "keys-missing" || mutation == "unique-missing" || mutation == "canonical-missing" || mutation == "declared-missing"
			if constructorFailure {
				if !errors.Is(err, ErrCorrupt) || v != nil || bad.fullViews != 0 || bad.fullViewBytes != 0 {
					t.Fatal("corrupt index advertised Full", v, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			switch mutation {
			case "canonical-role":
				_, err = v.Entity(t.Context(), 3)
			case "declared-role":
				_, err = v.Life(t.Context(), 3, 31)
			case "raw-canonical":
				_, err = v.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 99}, Window: scope}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
			default:
				page, e := v.UniqueCandidates(t.Context(), graphstate.UniquePredicate{Definition: fullSchemas()[1], Value: graphstate.I64(7), Window: scope}, 0, graphstate.ReadBudget{Rows: 512, Bytes: 4 << 20})
				err = e
				if len(page.Claims) != 0 || page.Next != 0 {
					t.Fatal("corrupt candidate returned partial data", page)
				}
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal("stored binding corruption became miss/ordinary input", mutation, err)
			}
			if _, err := bad.Root(); !errors.Is(err, ErrPoisoned) {
				t.Fatal(err)
			}
		})
	}
}
