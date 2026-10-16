package graphstore

import (
	"context"
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// DeclareUniquePartition declares the scalar property def (def.Unique set) on a
// graph that already holds data, in one install. A property never declared is
// declared fresh. A property declared without Unique (identical otherwise) is
// promoted: its existing claims are validated first (a duplicate over
// overlapping effective time is a *graphstate.UniqueConflict, nothing staged)
// and then every existing cell is posted into the unique index, so reads and
// later writes find the index complete. Any other change of an existing
// definition is ErrRebinding; an already unique one is ErrAlreadyUnique.
// unique bounds the validation read; it is the same shape as planning limits.
func DeclareUniquePartition(ctx context.Context, view *raftlog.ApplicationView, d OwnershipDeclaration, binding types.DefaultAxisBinding, configuration [32]byte, def graphstate.PropertyDefinition, l Limits, g GraphLimits, budget OwnershipBudget, unique graphstate.Limits) (GraphEffects, PageWork, error) {
	if def.Unique != graphstate.UniqueScalar || def.Owner != graphstate.Node || def.Cardinality != graphstate.ScalarCardinality {
		return GraphEffects{}, PageWork{}, ErrInvalid
	}
	if err := budget.validate(); err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	g, err := g.resolve()
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	g.MaxOutputBytes = min(g.MaxOutputBytes, budget.OutputBytes)
	arena, err := selectedGraphOutputBudget(g, nil)
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	v, work, err := openPartitionGraphReadViewBudget(ctx, view, d, binding, configuration, l, g, budget, arena)
	if err != nil {
		return GraphEffects{}, work, err
	}
	resolved := v.limits
	resolved.MaxOutputBytes = min(resolved.MaxOutputBytes, budget.OutputBytes)
	return stageDeclareUnique(ctx, v.c, v, def, resolved, unique)
}

// ErrAlreadyUnique reports a declaration of a property that is already unique.
var ErrAlreadyUnique = errors.New("graphstore: property is already unique")

func stageDeclareUnique(ctx context.Context, c *Catalog, v *ReadView, def graphstate.PropertyDefinition, l GraphLimits, unique graphstate.Limits) (effects GraphEffects, work PageWork, err error) {
	defer func() {
		work = addWork(v.Work(), work)
		_ = v.Close()
		if err != nil {
			effects = GraphEffects{}
		}
	}()
	arena := v.arena
	if arena == nil {
		if arena, err = graphstate.NewOutputBudget(l.MaxOutputBytes); err != nil {
			return GraphEffects{}, PageWork{}, callerError(err)
		}
	}
	retained := 512 + 1536
	if v.arena == nil {
		retained += fullViewMetadataBytes + 2*cap(v.base.Image)
	}
	if v.route != nil {
		retained += 64 * len(v.route.routing.owners)
	}
	if err := arena.Reserve(retained); err != nil {
		return GraphEffects{}, PageWork{}, callerError(err)
	}
	v.arena = arena

	old, err := v.Property(ctx, def.Owner, def.Name)
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	var keys []graphstate.ComponentKey
	if old.Found {
		prior := old.Record
		if prior.Unique != graphstate.UniqueNone {
			return GraphEffects{}, PageWork{}, ErrAlreadyUnique
		}
		want := def
		want.Unique = graphstate.UniqueNone
		if prior != want {
			return GraphEffects{}, PageWork{}, ErrRebinding
		}
		if keys, err = v.propertyKeys(ctx, def); err != nil {
			return GraphEffects{}, PageWork{}, err
		}
		if err := graphstate.CheckUniqueClaims(ctx, v, prior, keys, unique); err != nil {
			return GraphEffects{}, PageWork{}, err
		}
	}
	stagePages := l.Pages
	rows, bytes := v.remaining()
	rows = min(stagePages.MaxWorkRecords, rows)
	bytes = min(stagePages.MaxWorkBytes, bytes)
	if rows < 0 || bytes < c.rootImageBytes+fullStageMetadataBytes {
		return GraphEffects{}, PageWork{}, ErrResourceLimit
	}
	s, err := newFullStage(ctx, c, v.descriptor, stagePages)
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	defer func() { _ = s.Close() }()
	s.route = v.route
	s.arena = arena
	var stagedWork PageWork
	err = s.operation(ctx, func(base *reader) error {
		var pages *pageReader
		defer func() {
			if pages != nil {
				stagedWork = pages.work
			}
			stagedWork.Records, stagedWork.Bytes = base.rows, base.bytes
		}()
		base.maxRows, base.maxBytes = rows, bytes
		base.denyReads = rows == 0
		if err := base.materialize(0); err != nil {
			return err
		}
		p := pageStage{&pageReader{q: base, limits: stagePages}, c.root}
		p.allocation = &p.root
		pages = p.pageReader
		if err := p.budget(); err != nil {
			return err
		}
		// Collect first: while the stored definition is still not unique, a page
		// read does not demand a posting for each cell it holds.
		var posts []postingKey
		for _, key := range keys {
			key := key
			p.member = &key
			_, err := p.componentCells(key, func(cell state.Cell) (bool, error) {
				post, present, err := p.uniquePosting(key, cell, def)
				if err == nil && present {
					posts = append(posts, post)
				}
				return false, err
			})
			p.member = nil
			if err != nil {
				return err
			}
			if err := p.budget(); err != nil {
				return err
			}
		}
		encoded, err := encodeProperty(base.c.root.namespace, def, base.c.limits)
		if err != nil {
			return err
		}
		if err := base.put(schemaKey(base.c.root.namespace, def.Owner, def.Name), encoded); err != nil {
			return err
		}
		for _, post := range posts {
			root, err := p.insertPostingKey(base.full.descriptor.unique, post, keyTreeLimits(stagePages))
			if err != nil {
				return err
			}
			base.full.descriptor.unique = root
		}
		base.full.root = p.root
		wire, err := encodeFullDescriptor(base.full.descriptor, p.root, c.limits)
		if err != nil {
			return err
		}
		return base.put(componentIndexDescriptorKey(c.root.namespace), wire)
	})
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	work = addWork(v.Work(), stagedWork)
	if work.Records > l.MaxSourceRows || work.Bytes > l.MaxSourceBytes {
		return GraphEffects{}, PageWork{}, ErrResourceLimit
	}
	effects, err = fullEffects(s, v.base, graphstate.Delta{Graph: v.c.root.namespace.Graph, View: v.id}, nil, work, l)
	return effects, work, err
}

// propertyKeys lists every ScalarProperty component of def's name whose owner
// is of def's owner kind, in key order, by walking the whole key tree.
func (v *ReadView) propertyKeys(ctx context.Context, def graphstate.PropertyDefinition) ([]graphstate.ComponentKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	q, err := v.begin(ctx, raftlog.ReadBudget{})
	if err != nil {
		return nil, err
	}
	var out []graphstate.ComponentKey
	root, err := q.componentKeyTreeRoot(v.descriptor.keys, keyTreeLimits(q.limits))
	if err == nil {
		var walk func(n componentKeyTreeNode) error
		walk = func(n componentKeyTreeNode) error {
			if n.level == 0 {
				for _, key := range n.keys {
					if key.Kind != graphstate.ScalarProperty || key.Name != def.Name {
						continue
					}
					owner, found, err := q.q.entity(EntityRef{v.c.root.namespace.Graph, key.Owner})
					if err != nil {
						return err
					}
					if !found {
						return ErrCorrupt
					}
					if owner.Kind == def.Owner {
						key.Name = def.Name
						out = append(out, key)
					}
				}
				return nil
			}
			for i := range n.children {
				child, err := q.componentKeyTreeChild(n, i, keyTreeLimits(q.limits))
				if err != nil {
					return err
				}
				if err := walk(child); err != nil {
					return err
				}
			}
			return nil
		}
		err = walk(root)
	}
	if err = v.finish(q, err); err != nil {
		return nil, err
	}
	slices.SortFunc(out, compareComponentKeys)
	return out, nil
}

// uniquePosting is the unique-index entry of one cell of a property component
// as def (unique) defines it, for a cell stored while the property was not
// unique: rawKey needs the stored definition to be unique already.
func (q *pageReader) uniquePosting(k graphstate.ComponentKey, cell state.Cell, def graphstate.PropertyDefinition) (postingKey, bool, error) {
	if !cell.Present() || cell.Value().IsNull() || k.Kind != graphstate.ScalarProperty {
		return postingKey{}, false, nil
	}
	value, wire, found, err := q.q.value(ValueRef{q.q.c.root.namespace.Graph, graphstate.ValueID(cell.Value().ID())})
	if err != nil {
		return postingKey{}, false, err
	}
	if !found || value.Value.Kind() != def.Type {
		return postingKey{}, false, ErrCorrupt
	}
	if q.q.route != nil {
		if err := q.q.route.unique(q.q, def, string(wire)); err != nil {
			return postingKey{}, false, err
		}
	}
	if err := q.q.materialize(len(wire)); err != nil {
		return postingKey{}, false, err
	}
	canonical, found, err := q.q.canonicalIdentity(string(wire))
	if err != nil {
		return postingKey{}, false, err
	}
	if !found {
		return postingKey{}, false, ErrCorrupt
	}
	return postingKey{family: uniquePostingRecord, owner: def.Owner, scalar: def.Type, name: k.Name, value: canonical.Ref.ID, component: k}, true, nil
}
