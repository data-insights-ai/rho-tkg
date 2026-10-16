package graphstore

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Preserve cells rather than admitting/replaying business mutations. Validation
// of graph-wide value/unique authority remains with the native frozen source;
// a target-local catalog cannot infer ownership of a third partition's ValueID.
func rangeMoveInstallComponent(p *pageStage, d RangeMoveDescriptor, next *RangeMoveImportState, key, wire []byte) error {
	source := Namespace{d.Spec.Publication.Graph, d.Spec.From.Partition}
	if len(key) < 66 || !bytes.HasPrefix(key, keyPrefix(source, componentRecord)) {
		return ErrInvalid
	}
	kc := cursor{src: key[25 : len(key)-12]}
	component, err := decodeComponent(&kc, p.q.c.limits)
	if err != nil || kc.done() != nil || !d.Spec.Publication.Contains(uint64(component.Owner)) || binary.BigEndian.Uint64(key[len(key)-12:len(key)-4]) == 0 {
		return errors.Join(ErrInvalid, err)
	}
	c := cursor{src: wire}
	axisWire, err := c.field(2048)
	if err != nil {
		return err
	}
	ac := cursor{src: axisWire}
	axis, err := decodeAxis(&ac, p.q.c.limits)
	if err != nil || ac.done() != nil {
		return errors.Join(ErrInvalid, err)
	}
	owned, err := readScopeField(&c, axis, p.q.c.limits)
	if err != nil {
		return err
	}
	stateWire, err := c.field(p.limits.codecLimits(p.q.c).MaxEncodedBytes)
	if err != nil || c.done() != nil {
		return errors.Join(ErrInvalid, err)
	}
	snapshot, err := state.DecodeState(stateWire, axis, p.limits.codecLimits(p.q.c))
	if err != nil {
		return err
	}
	if owned.Kind() == temporal.ScopeEmpty || owned.Kind() == temporal.ScopeUnplaced {
		return ErrInvalid
	}
	entity, found, err := p.q.entity(EntityRef{p.root.namespace.Graph, component.Owner})
	if err != nil {
		return err
	}
	if !found || entity.Axis.Descriptor() != axis.Descriptor() || entity.Axis.DefinitionHash() != axis.DefinitionHash() {
		return ErrInvalid
	}
	for _, piece := range snapshot.Pieces() {
		inside, e := within(piece.Scope(), owned, p.q.c.limits.Temporal)
		if e != nil || !inside {
			return errors.Join(ErrInvalid, e)
		}
	}
	componentWire := key[:len(key)-12]
	fresh := !bytes.Equal(next.component, componentWire)
	var meta componentMeta
	// The first record of a component replaces the target's whole prior copy
	// (a range that left and returns); index rows follow from both copies.
	var replaced []state.Cell
	if fresh {
		if _, e := p.componentCells(component, func(c state.Cell) (bool, error) { replaced = append(replaced, c); return false, nil }); e != nil {
			return e
		}
		id, e := p.reserve()
		if e != nil {
			return e
		}
		all, e := temporal.All(axis)
		if e != nil {
			return e
		}
		meta = componentMeta{Key: component, Axis: axis, Root: id}
		if e = p.putDirectory(directoryPage{ID: id, Key: component, Owned: all}); e != nil {
			return e
		}
		next.component = exactCopy(componentWire)
	} else {
		var e error
		meta, found, e = p.readMeta(component)
		if e != nil || !found {
			return errors.Join(ErrCorrupt, e)
		}
	}
	root, err := p.directory(meta.Root, component, axis)
	if err != nil {
		return err
	}
	children, err := rangeMoveReplaceDirectory(p, root, owned, snapshot)
	if err != nil {
		return err
	}
	if len(children) > 1 {
		if root.Level+1 >= p.limits.MaxLevels {
			return ErrResourceLimit
		}
		id, e := p.reserve()
		if e != nil {
			return e
		}
		root = directoryPage{ID: id, Key: component, Owned: root.Owned, Level: root.Level + 1, Children: children}
		if e = p.putDirectory(root); e != nil {
			return e
		}
		meta.Root = id
	}
	if fresh || meta.Root != root.ID {
		meta.Root = root.ID
	}
	encoded, err := encodeMeta(p.root.namespace, meta, p.q.c.limits)
	if err != nil {
		return err
	}
	if err = p.q.put(componentKey(p.root.namespace, component), encoded); err != nil {
		return err
	}
	tree, err := p.insertComponentKey(p.q.full.descriptor.keys, component, keyTreeLimits(p.limits))
	if err != nil {
		return err
	}
	p.q.full.descriptor.keys = tree
	written := make([]state.Cell, 0, len(snapshot.Pieces()))
	for _, piece := range snapshot.Pieces() {
		written = append(written, piece.Cell())
	}
	if err = p.stageIndexRows(component, replaced, written); err != nil {
		return err
	}
	return p.budget()
}

func rangeMoveReplaceDirectory(p *pageStage, node directoryPage, owned temporal.Scope, incoming state.State) ([]childPage, error) {
	overlap, err := node.Owned.Overlaps(owned, p.q.c.limits.Temporal)
	if err != nil {
		return nil, err
	}
	if !overlap {
		return []childPage{{node.ID, node.Owned}}, nil
	}
	if node.Level == 0 {
		current, err := rangeMoveState(p.pageReader, node)
		if err != nil {
			return nil, err
		}
		outside, err := node.Owned.Difference(owned, p.q.c.limits.Temporal)
		if err != nil {
			return nil, err
		}
		remaining, err := current.Slice(outside, p.limits.stateLimits(p.q.c))
		if err != nil {
			return nil, err
		}
		inside, err := incoming.Slice(node.Owned, p.limits.stateLimits(p.q.c))
		if err != nil {
			return nil, err
		}
		if err = p.q.materialize(4096 + 8*(remaining.Usage().MetadataBytes()+inside.Usage().MetadataBytes())); err != nil {
			return nil, err
		}
		for _, piece := range inside.Pieces() {
			result, e := applyPageCell(remaining, piece.Scope(), piece.Cell(), p.limits.stateLimits(p.q.c))
			if e != nil {
				return nil, e
			}
			remaining = result.State()
		}
		return p.checkpointLeaf(node, remaining)
	}
	if err = p.q.materialize(512 + 256*(len(node.Children)+2)); err != nil {
		return nil, err
	}
	children := make([]childPage, 0, len(node.Children)+2)
	for _, child := range node.Children {
		hits, e := child.Owned.Overlaps(owned, p.q.c.limits.Temporal)
		if e != nil {
			return nil, e
		}
		if !hits {
			children = append(children, child)
			continue
		}
		sub, e := p.directory(child.ID, node.Key, node.Owned.Axis())
		if e != nil {
			return nil, e
		}
		same, e := sameScope(sub.Owned, child.Owned, p.q.c.limits.Temporal)
		if e != nil || !same || sub.Level != node.Level-1 {
			return nil, errors.Join(ErrCorrupt, e)
		}
		replacement, e := rangeMoveReplaceDirectory(p, sub, owned, incoming)
		if e != nil {
			return nil, e
		}
		children = append(children, replacement...)
		if len(children) > 2*p.limits.MaxChildren {
			return nil, ErrResourceLimit
		}
	}
	node.Children = children
	if len(children) <= p.limits.MaxChildren {
		if err = validateChildren(node, p.q.c.limits.Temporal); err != nil {
			return nil, err
		}
		if err = p.putDirectory(node); err != nil {
			return nil, err
		}
		return []childPage{{node.ID, node.Owned}}, nil
	}
	id, err := p.reserve()
	if err != nil {
		return nil, err
	}
	mid := len(children) / 2
	out := make([]childPage, 0, 2)
	for i, group := range [][]childPage{children[:mid], children[mid:]} {
		coverage, e := temporal.Empty(node.Owned.Axis())
		if e != nil {
			return nil, e
		}
		for _, child := range group {
			coverage, e = coverage.Union(child.Owned, p.q.c.limits.Temporal)
			if e != nil {
				return nil, e
			}
		}
		half := node
		half.Children = group
		half.Owned = coverage
		if i == 1 {
			half.ID = id
		}
		if e = validateChildren(half, p.q.c.limits.Temporal); e != nil {
			return nil, e
		}
		if e = p.putDirectory(half); e != nil {
			return nil, e
		}
		out = append(out, childPage{half.ID, coverage})
	}
	return out, nil
}
