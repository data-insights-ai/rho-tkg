package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
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
	native, err := c.initializationView()
	if err != nil {
		return stagedIndexes{}, err
	}
	proof, err := native.ProveNoApplicationData(ctx)
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

// Index pages. Two plain-KV row families, kept beside the component trees by
// installFullPlan (stageIndexRows) and read by ReadView.LabelMembers and
// EqualityMembers through the immutable source Scan, so every page and cursor
// is bound to one cut. A row exists exactly while its component holds at least
// one present piece, in any scope: a candidate set, never a temporal answer.
//
//	label:    prefix | field(label) | owner | life
//	equality: prefix | ownerKind | field(name) | sha256(EqualityKey) | owner | life | member
//
// Equality rows key by Scalar.EqualityKey (I64(3) and F64(3.0) meet); the
// value dictionary and unique buckets stay on IdentityKey. Rows sort by owner
// ID, so pages are ID-ordered; an owner with several lives or members is
// listed once. Stores written before these rows existed have no rows.
const (
	labelPostingRecord    recordKind = 0x18
	equalityPostingRecord recordKind = 0x19
)
const indexEntityBytes = 16
const indexScanBatch = 64

func indexRowValue(n Namespace, kind recordKind) []byte { return append(recordHeader(n, kind), 1) }
func labelPostingPrefix(n Namespace, label string) []byte {
	return appendField(keyPrefix(n, labelPostingRecord), []byte(label))
}

// equalityPostingPrefix keys the value by the SHA-256 of its EqualityKey, so a
// wide value never makes a wide row key.
func equalityPostingPrefix(n Namespace, owner graphstate.EntityKind, name, equality string) []byte {
	b := append(keyPrefix(n, equalityPostingRecord), byte(owner))
	digest := sha256.Sum256([]byte(equality))
	return append(appendField(b, []byte(name)), digest[:]...)
}
func appendIndexRowTail(b []byte, owner graphstate.EntityID, life graphstate.LifeID) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(b, uint64(owner)), uint64(life))
}
func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil
}

// retract stages a tombstone; only rows this stage knows exist are retracted.
func (q *reader) retract(key []byte) error {
	if err := q.put(key, nil); err != nil {
		return err
	}
	q.pending[string(key)] = raftlog.KV{Key: exactCopy(key), Deleted: true}
	return nil
}

// componentCells calls visit with every piece cell of a component, stopping when
// visit reports true. A missing component has none.
func (q *pageReader) componentCells(k graphstate.ComponentKey, visit func(state.Cell) (bool, error)) (bool, error) {
	m, found, err := q.readMeta(k)
	if err != nil || !found {
		return false, err
	}
	var walk func(id uint64, depth int) (bool, error)
	walk = func(id uint64, depth int) (bool, error) {
		if depth >= q.limits.MaxLevels {
			return false, ErrCorrupt
		}
		d, err := q.directory(id, m.Key, m.Axis)
		if err != nil {
			return false, err
		}
		if d.Level == 0 {
			s, err := q.materialize(d)
			if err != nil {
				return false, err
			}
			for _, piece := range s.Pieces() {
				if stop, err := visit(piece.Cell()); err != nil || stop {
					return stop, err
				}
			}
			return false, nil
		}
		for _, child := range d.Children {
			if stop, err := walk(child.ID, depth+1); err != nil || stop {
				return stop, err
			}
		}
		return false, nil
	}
	return walk(m.Root, 0)
}

// indexValueKey is the canonical equality key a present cell contributes to a
// property component; ok is false for absent cells and Null.
func (q *pageReader) indexValueKey(k graphstate.ComponentKey, cell state.Cell) (string, bool, error) {
	if !cell.Present() {
		return "", false, nil
	}
	id := k.Member
	if k.Kind == graphstate.ScalarProperty {
		if cell.Value().IsNull() {
			return "", false, nil
		}
		id = graphstate.ValueID(cell.Value().ID())
	}
	value, known := q.q.hints.value(id)
	if !known {
		entry, _, found, err := q.q.value(ValueRef{q.q.c.root.namespace.Graph, id})
		if err != nil {
			return "", false, err
		}
		if !found {
			return "", false, ErrCorrupt
		}
		value = entry.Value
	}
	key, err := value.EqualityKey(graphstate.Limits{})
	if err != nil {
		return "", false, ErrCorrupt
	}
	return key, true, q.budget()
}

// indexHints are the entities and values the plan itself creates: the stager
// already holds them, so index maintenance reads neither again.
type indexHints struct {
	kinds  map[graphstate.EntityID]graphstate.EntityKind
	values map[graphstate.ValueID]graphstate.Scalar
}

func newIndexHints(planned graphstate.Delta) *indexHints {
	h := &indexHints{kinds: make(map[graphstate.EntityID]graphstate.EntityKind, len(planned.Entities)), values: make(map[graphstate.ValueID]graphstate.Scalar, len(planned.Values))}
	for _, e := range planned.Entities {
		h.kinds[e.ID] = e.Kind
	}
	for _, w := range planned.Values {
		h.values[w.ID] = w.Value
	}
	return h
}
func (h *indexHints) kind(id graphstate.EntityID) (graphstate.EntityKind, bool) {
	if h == nil {
		return 0, false
	}
	k, ok := h.kinds[id]
	return k, ok
}
func (h *indexHints) value(id graphstate.ValueID) (graphstate.Scalar, bool) {
	if h == nil {
		return graphstate.Scalar{}, false
	}
	v, ok := h.values[id]
	return v, ok
}

// stageIndexRows runs after the patch is installed, so scans see the new state.
func (q *pageStage) stageIndexRows(patch graphstate.ComponentPatch) error {
	k := patch.Key
	if k.Kind != graphstate.Label && k.Kind != graphstate.ScalarProperty && k.Kind != graphstate.SetMember {
		return nil
	}
	n := q.q.c.root.namespace
	if k.Kind == graphstate.Label {
		var before, after bool
		for _, change := range patch.Changes {
			before = before || change.Before().Present()
			after = after || change.After().Present()
		}
		row := appendIndexRowTail(labelPostingPrefix(n, k.Name), k.Owner, k.Life)
		if after {
			return q.q.put(row, indexRowValue(n, labelPostingRecord))
		}
		if !before {
			return nil
		}
		present, err := q.componentCells(k, func(c state.Cell) (bool, error) { return c.Present(), nil })
		if err != nil || present {
			return err
		}
		return q.q.retract(row)
	}
	ownerKind, known := q.q.hints.kind(k.Owner)
	if !known {
		owner, found, err := q.q.entity(EntityRef{n.Graph, k.Owner})
		if err != nil {
			return err
		}
		if !found {
			return ErrCorrupt
		}
		ownerKind = owner.Kind
	}
	row := func(equality string) []byte {
		b := appendIndexRowTail(equalityPostingPrefix(n, ownerKind, k.Name, equality), k.Owner, k.Life)
		return binary.BigEndian.AppendUint64(b, uint64(k.Member))
	}
	added := make(map[string]struct{})
	var removed []string
	for _, change := range patch.Changes {
		if equality, ok, err := q.indexValueKey(k, change.After()); err != nil {
			return err
		} else if ok {
			if _, dup := added[equality]; !dup {
				added[equality] = struct{}{}
				if err := q.q.put(row(equality), indexRowValue(n, equalityPostingRecord)); err != nil {
					return err
				}
			}
		}
		if equality, ok, err := q.indexValueKey(k, change.Before()); err != nil {
			return err
		} else if _, kept := added[equality]; ok && !kept {
			removed = append(removed, equality)
		}
	}
	if len(removed) == 0 {
		return nil
	}
	held := make(map[string]struct{})
	if _, err := q.componentCells(k, func(c state.Cell) (bool, error) {
		equality, ok, err := q.indexValueKey(k, c)
		if ok {
			held[equality] = struct{}{}
		}
		return false, err
	}); err != nil {
		return err
	}
	for _, equality := range removed {
		if _, kept := held[equality]; !kept {
			if err := q.q.retract(row(equality)); err != nil {
				return err
			}
		}
	}
	return nil
}

// relationshipEffective reports whether both endpoints of a relationship life
// still have a present Presence piece (the bound life for LifeBound).
func (q *pageReader) relationshipEffective(id graphstate.EntityID, life graphstate.LifeID) (bool, error) {
	n := q.q.c.root.namespace
	entity, found, err := q.q.entity(EntityRef{n.Graph, id})
	if err != nil || !found {
		if err == nil {
			err = ErrCorrupt
		}
		return false, err
	}
	if entity.Kind != graphstate.Relationship {
		return true, nil
	}
	var bound [2]graphstate.LifeID
	if entity.Mode == graphstate.LifeBound {
		binding, found, err := q.q.life(LifeRef{n.Graph, id, life})
		if err != nil || !found {
			if err == nil {
				err = ErrCorrupt
			}
			return false, err
		}
		bound = [2]graphstate.LifeID{binding.SourceLife, binding.TargetLife}
	}
	for i, endpoint := range [2]graphstate.EntityID{entity.Source, entity.Target} {
		want := bound[i]
		live, err := q.componentCells(graphstate.ComponentKey{Owner: endpoint, Kind: graphstate.Presence}, func(c state.Cell) (bool, error) {
			return c.Present() && (want == 0 || graphstate.LifeID(c.Value().ID()) == want), nil
		})
		if err != nil || !live {
			return false, err
		}
	}
	return true, nil
}

// LabelMembers lists nodes holding label in any life, ascending, each once.
func (v *ReadView) LabelMembers(ctx context.Context, label string, token graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.EntityPage, error) {
	if v == nil {
		return graphstate.EntityPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if label == "" || !validName(label, v.c.limits) {
		return graphstate.EntityPage{}, ErrInvalid
	}
	query := append([]byte{byte(graphstate.Node), byte(graphstate.Declared)}, label...)
	return v.indexPage(ctx, labelPostingRecord, query, labelPostingPrefix(v.c.root.namespace, label), graphstate.Node, graphstate.Declared, token, budget)
}

// EqualityMembers lists owners whose property holds a value Cypher-equal to the
// predicate value, ascending, each once. Null matches nothing.
func (v *ReadView) EqualityMembers(ctx context.Context, p graphstate.EqualityPredicate, token graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.EntityPage, error) {
	if v == nil {
		return graphstate.EntityPage{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if p.Owner != graphstate.Node && p.Owner != graphstate.Relationship || p.Visibility != graphstate.Declared && p.Visibility != graphstate.Effective || p.Name == "" || !validName(p.Name, v.c.limits) {
		return graphstate.EntityPage{}, ErrInvalid
	}
	equality, err := p.Value.EqualityKey(graphstate.Limits{})
	if err != nil {
		return graphstate.EntityPage{}, ErrInvalid
	}
	query := append([]byte{byte(p.Owner), byte(p.Visibility), 1}, p.Name...)
	digest := sha256.Sum256([]byte(equality))
	query = append(query, digest[:]...)
	lower := equalityPostingPrefix(v.c.root.namespace, p.Owner, p.Name, equality)
	if p.Value.Kind() == graphstate.ScalarNull {
		lower = nil
	}
	return v.indexPage(ctx, equalityPostingRecord, query, lower, p.Owner, p.Visibility, token, budget)
}

// indexPage scans one row family prefix on the immutable source. A nil prefix
// is an empty family. Source rows and bytes join the handle's work ledger.
func (v *ReadView) indexPage(ctx context.Context, kind recordKind, query, lower []byte, owner graphstate.EntityKind, visibility graphstate.Visibility, token graphstate.Cursor, budget graphstate.ReadBudget) (graphstate.EntityPage, error) {
	if budget.Rows < 1 || budget.Bytes < 1 {
		return graphstate.EntityPage{}, ErrInvalid
	}
	if err := v.check(ctx); err != nil {
		return graphstate.EntityPage{}, err
	}
	hash := sha256.Sum256(append(append([]byte("full:index:v1\x00"), byte(kind)), query...))
	old, err := v.oldCursor(token, kind, hash)
	if err != nil {
		return graphstate.EntityPage{}, err
	}
	limit := min(budget.Bytes, v.limits.MaxOutputBytes-v.outputBytes)
	capacity := min(budget.Rows, (limit-128)/indexEntityBytes)
	if capacity < 1 {
		return graphstate.EntityPage{}, ErrResourceLimit
	}
	q, err := v.begin(ctx, graphstate.ReadBudget{})
	if err != nil {
		return graphstate.EntityPage{}, err
	}
	out := graphstate.EntityPage{View: v.id, Version: v.version(), Complete: true}
	var consumed []byte
	last := old.relationship
	if lower != nil {
		err = v.scanIndex(ctx, q, kind, lower, old, owner, visibility, capacity, &out, &consumed, &last)
	}
	if err != nil && (!errors.Is(err, ErrResourceLimit) || len(out.Entities) == 0) {
		return graphstate.EntityPage{}, v.finish(q, err)
	}
	if err != nil {
		out.Complete = false
	}
	next := fullContinuation{}
	if !out.Complete {
		if consumed == nil {
			consumed = []byte(old.posting.name)
		}
		next = fullContinuation{kind: kind, query: hash, posting: postingKey{name: string(consumed)}, relationship: last, bytes: 256 + len(consumed)}
		out.Next, err = v.storeCursor(token, old, next)
		if err != nil {
			return graphstate.EntityPage{}, v.finish(q, err)
		}
	}
	if err := v.finish(q, nil); err != nil {
		return graphstate.EntityPage{}, err
	}
	if err := v.output(128 + indexEntityBytes*len(out.Entities)); err != nil {
		return graphstate.EntityPage{}, err
	}
	v.advanceCursor(token, out.Next, old, next)
	return out, nil
}

// scanIndex fills out with up to capacity entities, looking one past it to
// decide Complete. consumed is the last row fully processed before any stop.
func (v *ReadView) scanIndex(ctx context.Context, q *pageReader, kind recordKind, lower []byte, old fullContinuation, owner graphstate.EntityKind, visibility graphstate.Visibility, capacity int, out *graphstate.EntityPage, consumed *[]byte, last *graphstate.EntityID) error {
	n := v.c.root.namespace
	upper := prefixEnd(lower)
	marker := indexRowValue(n, kind)
	tail := 16
	if kind == equalityPostingRecord {
		tail = 24
	}
	after := []byte(old.posting.name)
	for {
		rows := min(indexScanBatch, q.q.maxRows-q.q.rows)
		bytesLeft := min(q.q.maxBytes-q.q.bytes, v.c.source().ReadLimits().Bytes)
		if rows < 1 || bytesLeft < 1 {
			return ErrResourceLimit
		}
		page, err := v.c.source().Scan(ctx, lower, upper, after, raftlog.ReadBudget{Rows: min(rows, v.c.source().ReadLimits().Rows), Bytes: bytesLeft})
		if err != nil {
			if errors.Is(err, raftlog.ErrLimit) {
				return ErrResourceLimit
			}
			return err
		}
		q.q.rows += page.Visited
		if err := q.q.materialize(page.Bytes); err != nil {
			return err
		}
		for _, row := range page.Rows {
			if len(row.Key) != len(lower)+tail || row.Deleted || !bytes.Equal(row.Value, marker) {
				return ErrCorrupt
			}
			id := graphstate.EntityID(binary.BigEndian.Uint64(row.Key[len(lower):]))
			life := graphstate.LifeID(binary.BigEndian.Uint64(row.Key[len(lower)+8:]))
			if id == 0 || life == 0 && kind == labelPostingRecord {
				return ErrCorrupt
			}
			if id != *last {
				visible := true
				if visibility == graphstate.Effective && owner == graphstate.Relationship {
					if visible, err = q.relationshipEffective(id, life); err != nil {
						return err
					}
				}
				if visible {
					if len(out.Entities) == capacity {
						out.Complete = false
						return nil
					}
					out.Entities = append(out.Entities, id)
					*last = id
				}
			}
			*consumed = row.Key
		}
		if page.Complete {
			return nil
		}
		if len(page.Next) == 0 {
			return ErrCorrupt
		}
		after = page.Next
		*consumed = page.Next
	}
}
