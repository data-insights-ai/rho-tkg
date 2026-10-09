package graphstore

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const (
	componentRecord  recordKind = 8
	directoryRecord  recordKind = 9
	checkpointRecord recordKind = 10
	patchRecord      recordKind = 11
)

func validComponent(k graphstate.ComponentKey, l Limits) bool {
	if k.Owner == 0 {
		return false
	}
	switch k.Kind {
	case graphstate.Presence:
		return k.Life == 0 && k.Name == "" && k.Member == 0
	case graphstate.Label, graphstate.ScalarProperty:
		return k.Life != 0 && validName(k.Name, l) && k.Member == 0
	case graphstate.SetMember:
		return k.Life != 0 && validName(k.Name, l) && k.Member != 0
	}
	return false
}
func appendComponent(dst []byte, k graphstate.ComponentKey) []byte {
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Owner))
	dst = binary.BigEndian.AppendUint64(dst, uint64(k.Life))
	dst = append(dst, byte(k.Kind))
	dst = appendField(dst, []byte(k.Name))
	return binary.BigEndian.AppendUint64(dst, uint64(k.Member))
}
func decodeComponent(c *cursor, l Limits) (graphstate.ComponentKey, error) {
	owner, err := c.number()
	if err != nil {
		return graphstate.ComponentKey{}, err
	}
	life, err := c.number()
	if err != nil {
		return graphstate.ComponentKey{}, err
	}
	kind, err := c.tag()
	if err != nil {
		return graphstate.ComponentKey{}, err
	}
	name, err := c.field(l.MaxNameBytes)
	if err != nil {
		return graphstate.ComponentKey{}, err
	}
	member, err := c.number()
	if err != nil {
		return graphstate.ComponentKey{}, err
	}
	k := graphstate.ComponentKey{Owner: graphstate.EntityID(owner), Life: graphstate.LifeID(life), Kind: graphstate.ComponentKind(kind), Name: string(name), Member: graphstate.ValueID(member)}
	if !validComponent(k, l) {
		return graphstate.ComponentKey{}, ErrCorrupt
	}
	return k, nil
}
func componentKey(n Namespace, k graphstate.ComponentKey) []byte {
	return appendComponent(keyPrefix(n, componentRecord), k)
}
func physicalKey(n Namespace, kind recordKind, id uint64) []byte {
	return binary.BigEndian.AppendUint64(keyPrefix(n, kind), id)
}
func encodeMeta(n Namespace, m componentMeta, l Limits) ([]byte, error) {
	if m.Root == 0 || !validComponent(m.Key, l) {
		return nil, ErrInvalid
	}
	b := appendComponent(recordHeader(n, componentRecord), m.Key)
	d := m.Axis.Descriptor()
	hash := m.Axis.DefinitionHash()
	b = append(b, d.ID[:]...)
	b = append(b, hash[:]...)
	return checkRecord(binary.BigEndian.AppendUint64(b, m.Root), l)
}
func (q *pageReader) readMeta(k graphstate.ComponentKey) (componentMeta, bool, error) {
	b, found, err := q.get(componentKey(q.q.c.root.namespace, k))
	if err != nil {
		return componentMeta{}, false, err
	}
	if q.member != nil && *q.member == k {
		if !found {
			return componentMeta{}, false, ErrCorrupt
		}
	} else if q.q.indexes != nil || q.q.full != nil || q.q.fullView != nil || q.q.c.root.topology == keysOnlyTopology || q.q.c.root.topology == fullTopology {
		tree, limits, err := q.membershipRoot()
		if err != nil {
			return componentMeta{}, false, err
		}
		member, err := q.hasComponentKey(tree, k, limits)
		if err != nil {
			return componentMeta{}, false, err
		}
		if member != found {
			return componentMeta{}, false, ErrCorrupt
		}
	}

	if !found {
		return componentMeta{}, false, nil
	}
	c, err := inspectRecord(b, q.q.c.root.namespace, componentRecord, q.q.c.limits)
	if err != nil {
		return componentMeta{}, false, err
	}
	stored, err := decodeComponent(&c, q.q.c.limits)
	if err != nil {
		return componentMeta{}, false, err
	}
	id, err := c.take(16)
	if err != nil {
		return componentMeta{}, false, err
	}
	hash, err := c.take(32)
	if err != nil {
		return componentMeta{}, false, err
	}
	root, err := c.number()
	if err != nil || !q.validPhysical(root) || stored != k || c.done() != nil {
		return componentMeta{}, false, ErrCorrupt
	}
	var axisID temporal.AxisID
	copy(axisID[:], id)
	if axisID == (temporal.AxisID{}) {
		return componentMeta{}, false, ErrCorrupt
	}
	axis, found, err := q.q.axis(axisID)
	if err != nil {
		return componentMeta{}, false, err
	}
	actual := axis.DefinitionHash()
	if !found || !bytes.Equal(hash, actual[:]) {
		return componentMeta{}, false, ErrCorrupt
	}
	if _, err := q.storedBinding(k, axis); err != nil {
		return componentMeta{}, false, err
	}
	return componentMeta{k, axis, root}, true, nil
}
func appendScopeField(dst []byte, s temporal.Scope, l temporal.Limits) ([]byte, error) {
	wire, err := temporal.AppendScope(nil, s, l)
	if err != nil {
		return nil, err
	}
	return appendField(dst, wire), nil
}
func readScopeField(c *cursor, axis temporal.Axis, l Limits) (temporal.Scope, error) {
	b, err := c.field(1 << 20)
	if err != nil {
		return temporal.Scope{}, err
	}
	if len(b) > l.Temporal.MaxValueBytes {
		return temporal.Scope{}, ErrResourceLimit
	}
	s, err := temporal.DecodeScope(b, axis, l.Temporal)
	if err != nil {
		if errors.Is(err, temporal.ErrResourceLimit) {
			return temporal.Scope{}, errors.Join(ErrResourceLimit, err)
		}
		return temporal.Scope{}, errors.Join(ErrCorrupt, err)
	}
	return s, nil
}
func atomOwned(s temporal.Scope) bool {
	return s.Kind() == temporal.ScopeAll || s.Kind() == temporal.ScopeSpan || s.Kind() == temporal.ScopePoint
}
func encodeDirectory(n Namespace, d directoryPage, catalog *Catalog, l PageLimits) ([]byte, error) {
	if d.ID == 0 || !validComponent(d.Key, catalog.limits) || !atomOwned(d.Owned) || d.Level < 0 || d.Level >= l.MaxLevels {
		return nil, ErrInvalid
	}
	b := binary.BigEndian.AppendUint64(recordHeader(n, directoryRecord), d.ID)
	b = appendComponent(b, d.Key)
	var err error
	b, err = appendScopeField(b, d.Owned, catalog.limits.Temporal)
	if err != nil {
		return nil, err
	}
	b = append(b, byte(d.Level))
	if d.Level == 0 {
		for _, v := range []uint64{d.Base, d.Head, uint64(d.TailRecords), uint64(d.TailBytes), uint64(d.TailAtoms), uint64(d.Cells)} {
			b = binary.BigEndian.AppendUint64(b, v)
		}
	} else {
		if len(d.Children) < 2 || len(d.Children) > l.MaxChildren {
			return nil, ErrResourceLimit
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(d.Children)))
		for _, child := range d.Children {
			b = binary.BigEndian.AppendUint64(b, child.ID)
			b, err = appendScopeField(b, child.Owned, catalog.limits.Temporal)
			if err != nil {
				return nil, err
			}
		}
	}
	return checkRecord(b, catalog.limits)
} // #nosec G115 -- validated page fields/counts are positive finite and hard-bounded.
func (q *pageReader) directory(id uint64, k graphstate.ComponentKey, axis temporal.Axis) (directoryPage, error) {
	if !q.validPhysical(id) {
		return directoryPage{}, ErrCorrupt
	}
	b, found, err := q.get(physicalKey(q.q.c.root.namespace, directoryRecord, id))
	if err != nil {
		return directoryPage{}, err
	}
	if !found {
		return directoryPage{}, ErrCorrupt
	}
	q.work.DirectoryPages++
	c, err := inspectRecord(b, q.q.c.root.namespace, directoryRecord, q.q.c.limits)
	if err != nil {
		return directoryPage{}, err
	}
	self, err := c.number()
	if err != nil {
		return directoryPage{}, err
	}
	key, err := decodeComponent(&c, q.q.c.limits)
	if err != nil {
		return directoryPage{}, err
	}
	owned, err := readScopeField(&c, axis, q.q.c.limits)
	if err != nil {
		return directoryPage{}, err
	}
	level, err := c.tag()
	if err != nil || self != id || key != k || !atomOwned(owned) || int(level) >= 8 {
		return directoryPage{}, ErrCorrupt
	}
	if int(level) >= q.limits.MaxLevels {
		return directoryPage{}, ErrResourceLimit
	}
	d := directoryPage{ID: id, Key: k, Owned: owned, Level: int(level)}
	if level == 0 {
		var fields [6]uint64
		for i := range fields {
			fields[i], err = c.number()
			if err != nil {
				return directoryPage{}, err
			}
		}
		if fields[2] > 32 || fields[3] > 1<<20 || fields[4] > 4096 || fields[5] > 4096 || (fields[1] == 0) != (fields[2] == 0) {
			return directoryPage{}, ErrCorrupt
		}
		if fields[2] > uint64(q.limits.MaxTailRecords) || fields[3] > uint64(q.limits.MaxTailBytes) || fields[4] > uint64(q.limits.MaxTailAtoms) || fields[5] > uint64(q.limits.MaxCells) {
			return directoryPage{}, ErrResourceLimit
		}
		if fields[0] != 0 && !q.validPhysical(fields[0]) || fields[1] != 0 && !q.validPhysical(fields[1]) {
			return directoryPage{}, ErrCorrupt
		}
		d.Base, d.Head = fields[0], fields[1]
		d.TailRecords, d.TailBytes, d.TailAtoms, d.Cells = int(fields[2]), int(fields[3]), int(fields[4]), int(fields[5])
	} else {
		count, err := c.take(4)
		if err != nil {
			return directoryPage{}, err
		}
		num := binary.BigEndian.Uint32(count)
		if num < 2 || num > 64 || uint64(num) > uint64(len(c.src)/65) {
			return directoryPage{}, ErrCorrupt
		}
		if num > uint32(q.limits.MaxChildren) {
			return directoryPage{}, ErrResourceLimit
		}
		d.Children = make([]childPage, 0, int(num))
		for range num {
			childID, err := c.number()
			if err != nil || !q.validPhysical(childID) {
				return directoryPage{}, ErrCorrupt
			}
			scope, err := readScopeField(&c, axis, q.q.c.limits)
			if err != nil {
				return directoryPage{}, err
			}
			d.Children = append(d.Children, childPage{childID, scope})
		}
		if err := validateChildren(d, q.q.c.limits.Temporal); err != nil {
			return directoryPage{}, err
		}
	}
	if c.done() != nil {
		return directoryPage{}, ErrCorrupt
	}
	if err := q.q.materialize(len(b)); err != nil {
		return directoryPage{}, err
	}
	if err := q.budget(); err != nil {
		return directoryPage{}, err
	}
	return d, nil
} // #nosec G115 -- fields/counts checked against finite positive limits before allocation/conversion.
func encodeCheckpoint(n Namespace, id uint64, k graphstate.ComponentKey, s state.State, catalog *Catalog, l PageLimits) ([]byte, error) {
	wire, err := state.AppendState(nil, s, l.codecLimits(catalog))
	if err != nil {
		return nil, err
	}
	if len(wire) > l.MaxCheckpointBytes || s.Usage().Pieces() > l.MaxCells {
		return nil, ErrResourceLimit
	}
	b := appendComponent(binary.BigEndian.AppendUint64(recordHeader(n, checkpointRecord), id), k)
	return checkRecord(appendField(b, wire), catalog.limits)
}
func (q *pageReader) checkpoint(id uint64, k graphstate.ComponentKey, axis temporal.Axis) (state.State, error) {
	if id == 0 {
		return state.New(axis, q.limits.stateLimits(q.q.c))
	}
	if !q.validPhysical(id) {
		return state.State{}, ErrCorrupt
	}
	b, found, err := q.get(physicalKey(q.q.c.root.namespace, checkpointRecord, id))
	if err != nil {
		return state.State{}, err
	}
	if !found {
		return state.State{}, ErrCorrupt
	}
	q.work.CheckpointPages++
	c, err := inspectRecord(b, q.q.c.root.namespace, checkpointRecord, q.q.c.limits)
	if err != nil {
		return state.State{}, err
	}
	self, err := c.number()
	if err != nil {
		return state.State{}, err
	}
	key, err := decodeComponent(&c, q.q.c.limits)
	if err != nil {
		return state.State{}, err
	}
	wire, err := c.field(1 << 20)
	if err != nil {
		return state.State{}, err
	}
	if self != id || key != k || c.done() != nil {
		return state.State{}, ErrCorrupt
	}
	if len(wire) > q.limits.MaxCheckpointBytes {
		return state.State{}, ErrResourceLimit
	}
	s, err := state.DecodeState(wire, axis, q.limits.codecLimits(q.q.c))
	if err != nil {
		if errors.Is(err, state.ErrResourceLimit) || errors.Is(err, temporal.ErrResourceLimit) {
			return state.State{}, errors.Join(ErrResourceLimit, err)
		}
		return state.State{}, errors.Join(ErrCorrupt, err)
	}
	if s.Usage().Pieces() > q.limits.MaxCells {
		return state.State{}, ErrResourceLimit
	}
	if _, err := q.storedBinding(k, axis); err != nil {
		return state.State{}, err
	}
	for _, piece := range s.Pieces() {
		if err := q.storedCell(k, axis, piece.Cell()); err != nil {
			return state.State{}, err
		}
	}
	q.work.DecodedCells += s.Usage().Pieces()
	return s, nil
}
func encodePatch(n Namespace, p patchPage, catalog *Catalog, l PageLimits) ([]byte, error) {
	wire, err := state.AppendChanges(nil, p.Owned.Axis(), p.Changes, l.codecLimits(catalog))
	if err != nil {
		return nil, err
	}
	b := appendComponent(binary.BigEndian.AppendUint64(recordHeader(n, patchRecord), p.ID), p.Key)
	b = binary.BigEndian.AppendUint64(b, p.Previous)
	b, err = appendScopeField(b, p.Owned, catalog.limits.Temporal)
	if err != nil {
		return nil, err
	}
	return checkRecord(appendField(b, wire), catalog.limits)
}
func (q *pageReader) patch(id uint64, k graphstate.ComponentKey, axis temporal.Axis) (patchPage, int, error) {
	if !q.validPhysical(id) {
		return patchPage{}, 0, ErrCorrupt
	}
	b, found, err := q.get(physicalKey(q.q.c.root.namespace, patchRecord, id))
	if err != nil {
		return patchPage{}, 0, err
	}
	if !found {
		return patchPage{}, 0, ErrCorrupt
	}
	q.work.PatchPages++
	c, err := inspectRecord(b, q.q.c.root.namespace, patchRecord, q.q.c.limits)
	if err != nil {
		return patchPage{}, 0, err
	}
	self, err := c.number()
	if err != nil {
		return patchPage{}, 0, err
	}
	key, err := decodeComponent(&c, q.q.c.limits)
	if err != nil {
		return patchPage{}, 0, err
	}
	previous, err := c.number()
	if previous != 0 && !q.validPhysical(previous) {
		return patchPage{}, 0, ErrCorrupt
	}
	if err != nil {
		return patchPage{}, 0, err
	}
	owned, err := readScopeField(&c, axis, q.q.c.limits)
	if err != nil {
		return patchPage{}, 0, err
	}
	wire, err := c.field(1 << 20)
	if err != nil {
		return patchPage{}, 0, err
	}
	if self != id || key != k || !atomOwned(owned) || c.done() != nil {
		return patchPage{}, 0, ErrCorrupt
	}
	changes, _, err := state.DecodeChanges(wire, axis, q.limits.codecLimits(q.q.c))
	if err != nil {
		if errors.Is(err, state.ErrResourceLimit) || errors.Is(err, temporal.ErrResourceLimit) {
			return patchPage{}, 0, errors.Join(ErrResourceLimit, err)
		}
		return patchPage{}, 0, errors.Join(ErrCorrupt, err)
	}
	if _, err := q.storedBinding(k, axis); err != nil {
		return patchPage{}, 0, err
	}
	for _, change := range changes {
		if err := q.storedCell(k, axis, change.Before()); err != nil {
			return patchPage{}, 0, err
		}
		if err := q.storedCell(k, axis, change.After()); err != nil {
			return patchPage{}, 0, err
		}
	}
	return patchPage{id, previous, key, owned, changes}, len(b), nil
}
