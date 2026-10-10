package graphstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func validFullDescriptor(d fullIndexDescriptor, r Root) bool {
	if !isFullTopology(r.topology) || d.owner != r.owner || d.topology != r.topology.epoch || d.schema != r.topology.schema || d.format != r.topology.index {
		return false
	}
	roots := []postingTreeRoot{{d.keys.id, d.keys.level, d.keys.count, componentKeyTreeRecord}, d.unique, d.canonical, d.declared}
	kinds := []recordKind{componentKeyTreeRecord, uniquePostingRecord, canonicalIncidentRecord, declaredIncidentRecord}
	for i, t := range roots {
		if t.kind != kinds[i] || t.id == 0 || t.id >= r.next || t.level < 0 || t.level >= 8 || t.level > 0 && t.count < 2 {
			return false
		}
		for _, previous := range roots[:i] {
			if previous.id == t.id {
				return false
			}
		}
	}
	if d.format == 2 {
		return d.own == (currentPresenceTreeRoot{})
	}
	if d.format != 3 || d.own.id == 0 || d.own.id >= r.next || d.own.digest == ([32]byte{}) || d.own.level >= 8 || d.own.level > 0 && d.own.count < 2 {
		return false
	}
	for _, root := range roots {
		if root.id == d.own.id {
			return false
		}
	}
	return true
}
func encodeFullDescriptor(d fullIndexDescriptor, r Root, l Limits) ([]byte, error) {
	if !validFullDescriptor(d, r) {
		return nil, ErrInvalid
	}
	b := append(recordHeader(r.namespace, componentIndexDescriptorRecord), 2, byte(d.format), 0, 0)
	for _, v := range []uint64{d.owner, d.topology, d.schema, d.format} {
		b = binary.BigEndian.AppendUint64(b, v)
	}
	for _, t := range []postingTreeRoot{{d.keys.id, d.keys.level, d.keys.count, componentKeyTreeRecord}, d.unique, d.canonical, d.declared} {
		b = append(b, byte(t.kind), byte(t.level), 0, 0, 0, 0, 0, 0)
		b = binary.BigEndian.AppendUint64(b, t.id)
		b = binary.BigEndian.AppendUint64(b, t.count)
	}
	if d.format == 3 {
		b = append(b, byte(currentPresenceRecord), d.own.level, 0, 0, 0, 0, 0, 0)
		b = binary.BigEndian.AppendUint64(b, d.own.id)
		b = binary.BigEndian.AppendUint64(b, d.own.count)
		b = append(b, d.own.digest[:]...)
	}
	return checkRecord(b, l)
} // #nosec G115 -- four typed families and levels validated against eight.
func (q *reader) fullDescriptor(r Root) (fullIndexDescriptor, bool, error) {
	b, found, err := q.get(componentIndexDescriptorKey(r.namespace))
	if err != nil || !found {
		return fullIndexDescriptor{}, found, err
	}
	c, err := inspectRecord(b, r.namespace, componentIndexDescriptorRecord, q.c.limits)
	if err != nil {
		return fullIndexDescriptor{}, false, err
	}
	tags, err := c.take(4)
	if err != nil || tags[0] != 2 || tags[1] != byte(r.topology.index) || tags[2] != 0 || tags[3] != 0 || tags[1] != 2 && tags[1] != 3 {
		return fullIndexDescriptor{}, false, ErrCorrupt
	}
	var v [4]uint64
	for i := range v {
		v[i], err = c.number()
		if err != nil {
			return fullIndexDescriptor{}, false, err
		}
	}
	var roots [4]postingTreeRoot
	kinds := [4]recordKind{componentKeyTreeRecord, uniquePostingRecord, canonicalIncidentRecord, declaredIncidentRecord}
	for i := range roots {
		flags, err := c.take(8)
		if err != nil || recordKind(flags[0]) != kinds[i] || !bytes.Equal(flags[2:], []byte{0, 0, 0, 0, 0, 0}) {
			return fullIndexDescriptor{}, false, ErrCorrupt
		}
		id, err := c.number()
		if err != nil {
			return fullIndexDescriptor{}, false, err
		}
		count, err := c.number()
		if err != nil {
			return fullIndexDescriptor{}, false, err
		}
		roots[i] = postingTreeRoot{id, int(flags[1]), count, recordKind(flags[0])}
	}
	d := fullIndexDescriptor{owner: v[0], topology: v[1], schema: v[2], format: v[3], keys: componentKeyTreeRoot{roots[0].id, roots[0].level, roots[0].count}, unique: roots[1], canonical: roots[2], declared: roots[3]}
	if tags[1] == 3 {
		flags, err := c.take(8)
		if err != nil || flags[0] != byte(currentPresenceRecord) || !bytes.Equal(flags[2:], []byte{0, 0, 0, 0, 0, 0}) {
			return fullIndexDescriptor{}, false, ErrCorrupt
		}
		d.own.level = flags[1]
		d.own.id, err = c.number()
		if err != nil {
			return fullIndexDescriptor{}, false, err
		}
		d.own.count, err = c.number()
		if err != nil {
			return fullIndexDescriptor{}, false, err
		}
		digest, err := c.take(32)
		if err != nil {
			return fullIndexDescriptor{}, false, err
		}
		copy(d.own.digest[:], digest)
	}
	if c.done() != nil || !validFullDescriptor(d, r) {
		return fullIndexDescriptor{}, false, ErrCorrupt
	}
	if err := q.materialize(256); err != nil {
		return fullIndexDescriptor{}, false, err
	}
	return d, true, nil
}
func (q *pageReader) fullDescriptor() (fullIndexDescriptor, error) {
	if q.q.full != nil {
		return q.q.full.descriptor, nil
	}
	if q.q.fullView != nil {
		return *q.q.fullView, nil
	}
	d, found, err := q.q.fullDescriptor(q.q.c.root)
	if err != nil {
		return fullIndexDescriptor{}, err
	}
	if !found {
		return fullIndexDescriptor{}, ErrCorrupt
	}
	return d, nil
}
func (q *pageReader) membershipRoot() (componentKeyTreeRoot, componentKeyTreeLimits, error) {
	if q.q.indexes != nil {
		return q.q.indexes.descriptor.tree, q.q.indexes.limits, nil
	}
	if q.q.full != nil || q.q.fullView != nil || isFullTopology(q.q.c.root.topology) {
		d, err := q.fullDescriptor()
		return d.keys, keyTreeLimits(q.limits), err
	}
	d, found, err := q.q.componentIndexDescriptor(q.q.c.root)
	if err == nil && !found {
		err = ErrCorrupt
	}
	return d.tree, keyTreeLimits(q.limits), err
}
func allocateFullStage(c *Catalog, state fullStageState) (*Stage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison != nil {
		return nil, errors.Join(ErrPoisoned, c.poison)
	}
	if c.stages >= c.limits.MaxStages || fullStageBaseBytes > c.limits.MaxStageBytes-c.stageBytes {
		return nil, ErrResourceLimit
	}
	c.stages++
	c.stageBytes += fullStageBaseBytes
	return &Stage{c: c, writes: make(map[string]raftlog.KV), bytes: fullStageBaseBytes, full: &state}, nil
}
func (q *pageReader) validateFullRoots(d fullIndexDescriptor) error {
	if _, err := q.componentKeyTreeRoot(d.keys, keyTreeLimits(q.limits)); err != nil {
		return err
	}
	for _, root := range []postingTreeRoot{d.unique, d.canonical, d.declared} {
		if _, err := q.postingTreeRoot(root, keyTreeLimits(q.limits)); err != nil {
			return err
		}
	}
	if d.format == 3 {
		if err := q.q.materialize(cpTreeOperationOwned); err != nil {
			return err
		}
		op := cpTreeOperation{pageStage: &pageStage{pageReader: q, root: q.q.c.root}, limits: fullPresenceLimits(q), cache: make(map[uint64]currentPresencePage), dirty: make(map[uint64][]byte)}
		if _, err := op.load(d.own); err != nil {
			return err
		}
	}
	return nil
}
func newFullStage(ctx context.Context, c *Catalog, d fullIndexDescriptor, l PageLimits) (*Stage, error) {
	if err := c.check(ctx); err != nil {
		return nil, err
	}
	if !validFullDescriptor(d, c.root) {
		return nil, ErrTopologyUnsupported
	}
	return allocateFullStage(c, fullStageState{c.root, d, l})
}
