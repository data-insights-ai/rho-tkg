package graphstore

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strings"
)

// Fixed typed posting families. This private codec is specialized to the three
// graph access structures below; it is not a pluggable/generic index API.
const (
	uniquePostingRecord     recordKind = 0x12
	canonicalIncidentRecord recordKind = 0x13
	declaredIncidentRecord  recordKind = 0x14
)

type postingTreeLimits = componentKeyTreeLimits

type postingTreeRoot struct {
	id    uint64
	level int
	count uint64
	kind  recordKind
}

type postingTreeChild struct {
	id, count   uint64
	first, last postingKey
}

type postingTreeNode struct {
	kind     recordKind
	id       uint64
	level    int
	keys     []postingKey
	children []postingTreeChild
}

func (n postingTreeNode) summary() postingTreeChild {
	if n.level == 0 {
		if len(n.keys) == 0 {
			return postingTreeChild{id: n.id}
		}
		return postingTreeChild{n.id, uint64(len(n.keys)), n.keys[0], n.keys[len(n.keys)-1]}
	}
	var count uint64
	for _, child := range n.children {
		count += child.count // validateNode checks overflow before summary is used.
	}
	return postingTreeChild{n.id, count, n.children[0].first, n.children[len(n.children)-1].last}
}

func validatePostingKeyTreeNode(n postingTreeNode, c *Catalog, l postingTreeLimits) error {
	if !validPostingKind(n.kind) || n.id == 0 || n.level < 0 || n.level >= 8 {
		return ErrInvalid
	}
	if n.level >= l.maxLevels {
		return ErrResourceLimit
	}
	if n.level == 0 {
		if len(n.children) != 0 || len(n.keys) > 64 {
			return ErrInvalid
		}
		if len(n.keys) > l.maxKeys {
			return ErrResourceLimit
		}
		for i, key := range n.keys {
			if key.family != n.kind || !validPostingKey(key, c.limits) || i > 0 && comparePostingKeys(n.keys[i-1], key) >= 0 {
				return ErrInvalid
			}
		}
		return nil
	}
	if len(n.keys) != 0 || len(n.children) < 2 || len(n.children) > 64 {
		return ErrInvalid
	}
	if len(n.children) > l.maxChildren {
		return ErrResourceLimit
	}
	var count uint64
	for i, child := range n.children {
		if child.id == 0 || child.id == n.id || child.count == 0 || child.count > math.MaxUint64-count || child.first.family != n.kind || child.last.family != n.kind || !validPostingKey(child.first, c.limits) || !validPostingKey(child.last, c.limits) || comparePostingKeys(child.first, child.last) > 0 || i > 0 && comparePostingKeys(n.children[i-1].last, child.first) >= 0 {
			return ErrInvalid
		}
		for _, previous := range n.children[:i] {
			if previous.id == child.id {
				return ErrInvalid
			}
		}
		count += child.count
	}
	return nil
}

func encodePostingKeyTreeNode(n postingTreeNode, c *Catalog, l postingTreeLimits) ([]byte, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	if err := validatePostingKeyTreeNode(n, c, l); err != nil {
		return nil, err
	}
	b := binary.BigEndian.AppendUint64(recordHeader(c.root.namespace, n.kind), n.id)
	b = append(b, byte(n.level))
	if n.level == 0 {
		b = binary.BigEndian.AppendUint32(b, uint32(len(n.keys)))
		for _, key := range n.keys {
			b = appendPostingKey(b, key)
		}
	} else {
		b = binary.BigEndian.AppendUint32(b, uint32(len(n.children)))
		for _, child := range n.children {
			b = binary.BigEndian.AppendUint64(b, child.id)
			b = binary.BigEndian.AppendUint64(b, child.count)
			b = appendPostingKey(b, child.first)
			b = appendPostingKey(b, child.last)
		}
	}
	if len(b) > l.maxPageBytes {
		return nil, ErrResourceLimit
	}
	return checkRecord(b, c.limits)
} // #nosec G115 -- levels and entry counts are validated against eight and64.

func (q *pageReader) postingTreeNode(id uint64, kind recordKind, l postingTreeLimits) (postingTreeNode, error) {
	if err := l.validate(); err != nil {
		return postingTreeNode{}, err
	}
	if !q.validPhysical(id) {
		return postingTreeNode{}, ErrCorrupt
	}
	b, found, err := q.get(physicalKey(q.q.c.root.namespace, kind, id))
	if err != nil {
		return postingTreeNode{}, err
	}
	if !found {
		return postingTreeNode{}, ErrCorrupt
	}
	if len(b) > l.maxPageBytes {
		return postingTreeNode{}, ErrResourceLimit
	}
	c, err := inspectRecord(b, q.q.c.root.namespace, kind, q.q.c.limits)
	if err != nil {
		return postingTreeNode{}, err
	}
	self, err := c.number()
	if err != nil {
		return postingTreeNode{}, err
	}
	level, err := c.tag()
	if err != nil || self != id || level >= 8 {
		return postingTreeNode{}, ErrCorrupt
	}
	countWire, err := c.take(4)
	if err != nil {
		return postingTreeNode{}, err
	}
	count := binary.BigEndian.Uint32(countWire)
	minimum, maximum := postingKeyMinimum(kind), l.maxKeys
	if level > 0 {
		minimum, maximum = 16+2*postingKeyMinimum(kind), l.maxChildren
	}
	if count > 64 || uint64(count) > uint64(len(c.src)/minimum) || level > 0 && count < 2 {
		return postingTreeNode{}, ErrCorrupt
	}
	if int(level) >= l.maxLevels || int(count) > maximum {
		return postingTreeNode{}, ErrResourceLimit
	}
	// Count is bounded by the actual wire and policy before ownership exists.
	// Charge fixed decoded backing once, before keys/children allocation.
	if err := q.q.materialize(128 + 320*int(count)); err != nil {
		return postingTreeNode{}, err
	}
	if err := q.budget(); err != nil {
		return postingTreeNode{}, err
	}
	n := postingTreeNode{id: id, level: int(level), kind: kind}
	if level == 0 {
		n.keys = make([]postingKey, int(count))
	} else {
		n.children = make([]postingTreeChild, int(count))
	}
	bytes := 0
	for i := range int(count) {
		var child postingTreeChild
		if level > 0 {
			child.id, err = c.number()
			if err != nil || !q.validPhysical(child.id) {
				return postingTreeNode{}, ErrCorrupt
			}
			child.count, err = c.number()
			if err != nil {
				return postingTreeNode{}, err
			}
		}
		key, err := decodePostingKey(&c, kind, q.q.c.limits)
		if err != nil {
			return postingTreeNode{}, err
		}
		bytes += len(key.name) + len(key.component.Name)
		if level == 0 {
			n.keys[i] = key
		} else {
			child.first = key
			child.last, err = decodePostingKey(&c, kind, q.q.c.limits)
			if err != nil {
				return postingTreeNode{}, err
			}
			bytes += len(child.last.name) + len(child.last.component.Name)
			n.children[i] = child
		}
	}
	if c.done() != nil {
		return postingTreeNode{}, ErrCorrupt
	}
	if err := validatePostingKeyTreeNode(n, q.q.c, l); err != nil {
		return postingTreeNode{}, storedBindingError(err)
	}
	if err := q.q.materialize(bytes); err != nil {
		return postingTreeNode{}, err
	}
	if err := q.budget(); err != nil {
		return postingTreeNode{}, err
	}
	q.work.DirectoryPages++
	return n, nil
} // #nosec G115 -- counts are bounded by delivered bytes and64 before allocation.

func (q *pageReader) postingTreeRoot(root postingTreeRoot, l postingTreeLimits) (postingTreeNode, error) {
	n, err := q.postingTreeNode(root.id, root.kind, l)
	if err != nil {
		return postingTreeNode{}, err
	}
	if n.level != root.level || n.summary().count != root.count {
		return postingTreeNode{}, ErrCorrupt
	}
	return n, nil
}

func (q *pageReader) postingTreeChild(parent postingTreeNode, index int, l postingTreeLimits) (postingTreeNode, error) {
	child := parent.children[index]
	n, err := q.postingTreeNode(child.id, parent.kind, l)
	if err != nil {
		return postingTreeNode{}, err
	}
	actual := n.summary()
	if n.level != parent.level-1 || actual.count != child.count || actual.first != child.first || actual.last != child.last {
		return postingTreeNode{}, ErrCorrupt
	}
	return n, nil
}

func postingKeyChildIndex(n postingTreeNode, key postingKey) int {
	i, found := slices.BinarySearchFunc(n.children, key, func(child postingTreeChild, key postingKey) int {
		return comparePostingKeys(child.first, key)
	})
	if !found && i > 0 {
		i--
	}
	return min(i, len(n.children)-1)
}

func (q *pageReader) hasPostingKey(root postingTreeRoot, key postingKey, l postingTreeLimits) (bool, error) {
	if key.family != root.kind || !validPostingKey(key, q.q.c.limits) {
		return false, ErrInvalid
	}
	n, err := q.postingTreeRoot(root, l)
	if err != nil {
		return false, err
	}
	for n.level > 0 {
		n, err = q.postingTreeChild(n, postingKeyChildIndex(n, key), l)
		if err != nil {
			return false, err
		}
	}
	i, found := slices.BinarySearchFunc(n.keys, key, comparePostingKeys)
	if found && n.keys[i] != key {
		return false, ErrCorrupt
	}
	return found, nil
}

func (q *pageStage) putPostingKeyTreeNode(n postingTreeNode, l postingTreeLimits) error {
	b, err := encodePostingKeyTreeNode(n, q.q.c, l)
	if err != nil {
		return err
	}
	return q.q.put(physicalKey(q.root.namespace, n.kind, n.id), b)
}

func (q *pageStage) newPostingKeyTree(kind recordKind, l postingTreeLimits) (postingTreeRoot, error) {
	if err := l.validate(); err != nil {
		return postingTreeRoot{}, err
	}
	id, err := q.reserve()
	if err != nil {
		return postingTreeRoot{}, err
	}
	if err := q.putPostingKeyTreeNode(postingTreeNode{id: id, kind: kind}, l); err != nil {
		return postingTreeRoot{}, err
	}
	return postingTreeRoot{id: id, kind: kind}, nil
}

func (q *pageStage) insertPostingKeyNode(n postingTreeNode, key postingKey, l postingTreeLimits) ([]postingTreeChild, bool, error) {
	if n.level == 0 {
		i, found := slices.BinarySearchFunc(n.keys, key, comparePostingKeys)
		if found {
			if n.keys[i] != key {
				return nil, false, ErrRebinding
			}
			return []postingTreeChild{n.summary()}, false, nil
		}
		key.name = strings.Clone(key.name)
		key.component.Name = strings.Clone(key.component.Name)
		n.keys = slices.Insert(n.keys, i, key)
	} else {
		i := postingKeyChildIndex(n, key)
		child, err := q.postingTreeChild(n, i, l)
		if err != nil {
			return nil, false, err
		}
		replacements, added, err := q.insertPostingKeyNode(child, key, l)
		if err != nil {
			return nil, false, err
		}
		if !added {
			return []postingTreeChild{n.summary()}, false, nil
		}
		n.children = slices.Replace(n.children, i, i+1, replacements...)
	}
	count, maximum := len(n.keys), l.maxKeys
	if n.level > 0 {
		count, maximum = len(n.children), l.maxChildren
	}
	if count <= maximum {
		wire, err := encodePostingKeyTreeNode(n, q.q.c, l)
		if err == nil {
			if err := q.q.put(physicalKey(q.root.namespace, n.kind, n.id), wire); err != nil {
				return nil, false, err
			}
			return []postingTreeChild{n.summary()}, true, nil
		}
		if !errors.Is(err, ErrResourceLimit) {
			return nil, false, err
		}
		if count < 2 || n.level > 0 && count < 4 {
			return nil, false, ErrResourceLimit
		}
	}
	id, err := q.reserve()
	if err != nil {
		return nil, false, err
	}
	right := postingTreeNode{id: id, level: n.level, kind: n.kind}
	if n.level == 0 {
		right.keys, n.keys = n.keys[count/2:], n.keys[:count/2]
	} else {
		right.children, n.children = n.children[count/2:], n.children[:count/2]
	}
	for _, half := range []postingTreeNode{n, right} {
		if err := q.putPostingKeyTreeNode(half, l); err != nil {
			return nil, false, err
		}
	}
	return []postingTreeChild{n.summary(), right.summary()}, true, nil
}

func (q *pageStage) insertPostingKey(root postingTreeRoot, key postingKey, l postingTreeLimits) (postingTreeRoot, error) {
	if key.family != root.kind || !validPostingKey(key, q.q.c.limits) {
		return postingTreeRoot{}, ErrInvalid
	}
	n, err := q.postingTreeRoot(root, l)
	if err != nil {
		return postingTreeRoot{}, err
	}
	if root.count == math.MaxUint64 {
		return postingTreeRoot{}, ErrResourceLimit
	}
	children, added, err := q.insertPostingKeyNode(n, key, l)
	if err != nil {
		return postingTreeRoot{}, err
	}
	if !added {
		return root, nil
	}
	if len(children) == 1 {
		return postingTreeRoot{children[0].id, n.level, children[0].count, root.kind}, nil
	}
	if n.level+1 >= l.maxLevels {
		return postingTreeRoot{}, ErrResourceLimit
	}
	id, err := q.reserve()
	if err != nil {
		return postingTreeRoot{}, err
	}
	parent := postingTreeNode{id: id, level: n.level + 1, children: children, kind: n.kind}
	if err := q.putPostingKeyTreeNode(parent, l); err != nil {
		return postingTreeRoot{}, err
	}
	return postingTreeRoot{id, parent.level, parent.summary().count, root.kind}, nil
}
