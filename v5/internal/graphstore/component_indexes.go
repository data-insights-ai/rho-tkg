package graphstore

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// KeysOnly is private and cannot supply a complete graph ReadView. A populated
// KeysOnly catalog cannot broaden coverage without a bounded actual-row rebuild.
const componentIndexDescriptorRecord recordKind = 0x11
const componentIndexKeysOnly byte = 1

// Additional conservative representation allowance: Root (~104B), descriptor
// (~64B) and tree limits (~32B), including their pointer/alignment headroom.
// This is separate from the existing128-byte Stage/map baseline, not an RSS cap.
const indexedStageMetadataBytes = 256
const indexedStageBaseBytes = 128 + indexedStageMetadataBytes

var keysOnlyTopology = topologyDeclaration{epoch: 1, schema: 1, index: 1}

type componentIndexDescriptor struct {
	owner, topology, schema, format uint64
	coverage                        byte
	tree                            componentKeyTreeRoot
}
type indexedStageState struct {
	root       Root
	descriptor componentIndexDescriptor
	limits     componentKeyTreeLimits
}
type stagedIndexes struct {
	stage                     *Stage
	root                      Root
	baseGeneration, baseIndex uint64
	baseImageHash             [32]byte
}

func componentIndexDescriptorKey(n Namespace) []byte {
	return keyPrefix(n, componentIndexDescriptorRecord)
}
func keyTreeLimits(l PageLimits) componentKeyTreeLimits {
	return componentKeyTreeLimits{min(64, max(2, l.MaxCells)), l.MaxChildren, l.MaxLevels, min(64<<10, l.MaxCheckpointBytes)}
}
func validComponentIndexDescriptor(d componentIndexDescriptor, r Root) bool {
	return r.topology == keysOnlyTopology && d.owner == r.owner && d.topology == r.topology.epoch && d.schema == r.topology.schema && d.format == r.topology.index && d.coverage == componentIndexKeysOnly && d.tree.id > 0 && d.tree.id < r.next && d.tree.level >= 0 && d.tree.level < 8 && (d.tree.level == 0 || d.tree.count >= 2)
}
func encodeComponentIndexDescriptor(d componentIndexDescriptor, r Root, l Limits) ([]byte, error) {
	if !validComponentIndexDescriptor(d, r) {
		return nil, ErrInvalid
	}
	b := append(recordHeader(r.namespace, componentIndexDescriptorRecord), 1, d.coverage, byte(d.tree.level), 0)
	for _, v := range []uint64{d.owner, d.topology, d.schema, d.format, d.tree.id, d.tree.count} {
		b = binary.BigEndian.AppendUint64(b, v)
	}
	return checkRecord(b, l)
} // #nosec G115 -- tree level validated in [0,8).
func (q *reader) componentIndexDescriptor(r Root) (componentIndexDescriptor, bool, error) {
	b, found, err := q.get(componentIndexDescriptorKey(r.namespace))
	if err != nil || !found {
		return componentIndexDescriptor{}, found, err
	}
	c, err := inspectRecord(b, r.namespace, componentIndexDescriptorRecord, q.c.limits)
	if err != nil {
		return componentIndexDescriptor{}, false, err
	}
	tags, err := c.take(4)
	if err != nil || tags[0] != 1 || tags[3] != 0 {
		return componentIndexDescriptor{}, false, ErrCorrupt
	}
	var v [6]uint64
	for i := range v {
		v[i], err = c.number()
		if err != nil {
			return componentIndexDescriptor{}, false, err
		}
	}
	d := componentIndexDescriptor{v[0], v[1], v[2], v[3], tags[1], componentKeyTreeRoot{v[4], int(tags[2]), v[5]}}
	if c.done() != nil || !validComponentIndexDescriptor(d, r) {
		return componentIndexDescriptor{}, false, ErrCorrupt
	}
	if err := q.materialize(128); err != nil {
		return componentIndexDescriptor{}, false, err
	}
	return d, true, nil
}
func allocateIndexedStage(c *Catalog, state indexedStageState) (*Stage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poison != nil {
		return nil, errors.Join(ErrPoisoned, c.poison)
	}
	if c.stages >= c.limits.MaxStages || indexedStageBaseBytes > c.limits.MaxStageBytes-c.stageBytes {
		return nil, ErrResourceLimit
	}
	c.stages++
	c.stageBytes += indexedStageBaseBytes
	return &Stage{c: c, writes: make(map[string]raftlog.KV), bytes: indexedStageBaseBytes, indexed: &state}, nil
}
func newIndexedStage(ctx context.Context, c *Catalog, l PageLimits) (*Stage, error) {
	if c == nil {
		return nil, ErrInvalid
	}
	l, err := l.resolve()
	if err != nil {
		return nil, err
	}
	q, err := c.reader(ctx)
	if err != nil {
		return nil, err
	}
	if c.root.topology != keysOnlyTopology {
		return nil, ErrTopologyUnsupported
	}
	q.maxRows, q.maxBytes = l.MaxWorkRecords, l.MaxWorkBytes
	d, found, err := q.componentIndexDescriptor(c.root)
	if err == nil && !found {
		err = ErrCorrupt
	}
	if err != nil {
		return nil, c.failure(err)
	}
	limits := keyTreeLimits(l)
	p := pageReader{q: q, limits: l}
	if _, err := p.componentKeyTreeRoot(d.tree, limits); err != nil {
		return nil, c.failure(err)
	}
	return allocateIndexedStage(c, indexedStageState{c.root, d, limits})
}

// initializeIndexes is empty-store-only; it co-stages schema, a real empty leaf
// and its descriptor. Installation's original base guard is the final authority.
// It cannot initialize unique/adjacency coverage or promote an existing catalog.
func initializeIndexes(ctx context.Context, c *Catalog, schemas []graphstate.PropertyDefinition, l PageLimits) (stagedIndexes, error) {
	if c == nil {
		return stagedIndexes{}, ErrInvalid
	}
	l, err := l.resolve()
	if err != nil {
		return stagedIndexes{}, err
	}
	if err := c.check(ctx); err != nil {
		return stagedIndexes{}, err
	}
	seed, err := NewRoot(c.root.namespace, c.root.owner)
	if err != nil {
		return stagedIndexes{}, err
	}
	if c.root.topology != bootstrapTopology || c.root.epoch != 0 || c.root.next != 1 || c.root.effect != seed.effect {
		return stagedIndexes{}, ErrTopologyUnsupported
	}
	if len(schemas) > c.limits.MaxStageRecords-2 {
		return stagedIndexes{}, ErrResourceLimit
	}
	proof, err := c.view.ProveNoApplicationData(ctx)
	if err != nil {
		return stagedIndexes{}, err
	}
	q, err := c.reader(ctx)
	if err != nil {
		return stagedIndexes{}, err
	}
	if _, found, err := q.get(componentIndexDescriptorKey(c.root.namespace)); err != nil {
		return stagedIndexes{}, c.failure(err)
	} else if found {
		return stagedIndexes{}, c.failure(ErrCorrupt)
	}
	limits := keyTreeLimits(l)
	if err := limits.validate(); err != nil {
		return stagedIndexes{}, err
	}
	s, err := allocateIndexedStage(c, indexedStageState{root: c.root, limits: limits})
	if err != nil {
		return stagedIndexes{}, err
	}
	err = s.operation(ctx, func(base *reader) error {
		base.maxRows, base.maxBytes = l.MaxWorkRecords, l.MaxWorkBytes
		for _, d := range schemas {
			b, err := encodeProperty(c.root.namespace, d, c.limits)
			if err != nil {
				return err
			}
			key := schemaKey(c.root.namespace, d.Owner, d.Name)
			if _, found := base.pending[string(key)]; found {
				return ErrInvalid
			}
			if err := base.put(key, b); err != nil {
				return err
			}
		}
		p := pageStage{&pageReader{q: base, limits: l}, c.root}
		p.allocation = &p.root
		p.root.topology = keysOnlyTopology
		tree, err := p.newComponentKeyTree(limits)
		if err != nil {
			return err
		}
		d := componentIndexDescriptor{p.root.owner, p.root.topology.epoch, p.root.topology.schema, 1, componentIndexKeysOnly, tree}
		b, err := encodeComponentIndexDescriptor(d, p.root, c.limits)
		if err != nil {
			return err
		}
		if err := base.put(componentIndexDescriptorKey(c.root.namespace), b); err != nil {
			return err
		}
		base.indexes = &indexedStageState{p.root, d, limits}
		return p.budget()
	})
	if err != nil {
		_ = s.Close()
		return stagedIndexes{}, err
	}
	return stagedIndexes{s, s.indexed.root, proof.Generation, proof.Index, proof.ImageHash}, nil
}
