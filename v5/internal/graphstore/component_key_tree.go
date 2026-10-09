package graphstore

import (
	"cmp"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// This is a private component-key tree, not an index declaration, graph view or
// admission capability. Its caller co-stages its root with component metadata.
const componentKeyTreeRecord recordKind = 0x10

type componentKeyTreeLimits struct {
	maxKeys, maxChildren, maxLevels, maxPageBytes int
}

func defaultComponentKeyTreeLimits() componentKeyTreeLimits {
	return componentKeyTreeLimits{64, 64, 8, 64 << 10}
}

func (l componentKeyTreeLimits) validate() error {
	if l.maxKeys < 2 || l.maxKeys > 64 || l.maxChildren < 3 || l.maxChildren > 64 || l.maxLevels < 1 || l.maxLevels > 8 || l.maxPageBytes < 64 || l.maxPageBytes > 1<<20 {
		return ErrInvalid
	}
	return nil
}

type componentKeyTreeRoot struct {
	id    uint64
	level int
	count uint64
}

type componentKeyTreeChild struct {
	id, count   uint64
	first, last graphstate.ComponentKey
}

type componentKeyTreeNode struct {
	id       uint64
	level    int
	keys     []graphstate.ComponentKey
	children []componentKeyTreeChild
}

func compareComponentKeys(a, b graphstate.ComponentKey) int {
	return cmp.Or(cmp.Compare(a.Owner, b.Owner), cmp.Compare(a.Life, b.Life), cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Name, b.Name), cmp.Compare(a.Member, b.Member))
}

func (n componentKeyTreeNode) summary() componentKeyTreeChild {
	if n.level == 0 {
		if len(n.keys) == 0 {
			return componentKeyTreeChild{id: n.id}
		}
		return componentKeyTreeChild{n.id, uint64(len(n.keys)), n.keys[0], n.keys[len(n.keys)-1]}
	}
	var count uint64
	for _, child := range n.children {
		count += child.count // validateNode checks overflow before summary is used.
	}
	return componentKeyTreeChild{n.id, count, n.children[0].first, n.children[len(n.children)-1].last}
}

func validateComponentKeyTreeNode(n componentKeyTreeNode, c *Catalog, l componentKeyTreeLimits) error {
	if n.id == 0 || n.level < 0 || n.level >= 8 {
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
			if !validComponent(key, c.limits) || i > 0 && compareComponentKeys(n.keys[i-1], key) >= 0 {
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
	seen := make(map[uint64]bool, len(n.children))
	var count uint64
	for i, child := range n.children {
		if child.id == 0 || child.id == n.id || seen[child.id] || child.count == 0 || child.count > math.MaxUint64-count || !validComponent(child.first, c.limits) || !validComponent(child.last, c.limits) || compareComponentKeys(child.first, child.last) > 0 || i > 0 && compareComponentKeys(n.children[i-1].last, child.first) >= 0 {
			return ErrInvalid
		}
		seen[child.id] = true
		count += child.count
	}
	return nil
}

func encodeComponentKeyTreeNode(n componentKeyTreeNode, c *Catalog, l componentKeyTreeLimits) ([]byte, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	if err := validateComponentKeyTreeNode(n, c, l); err != nil {
		return nil, err
	}
	b := binary.BigEndian.AppendUint64(recordHeader(c.root.namespace, componentKeyTreeRecord), n.id)
	b = append(b, byte(n.level))
	if n.level == 0 {
		b = binary.BigEndian.AppendUint32(b, uint32(len(n.keys)))
		for _, key := range n.keys {
			b = appendComponent(b, key)
		}
	} else {
		b = binary.BigEndian.AppendUint32(b, uint32(len(n.children)))
		for _, child := range n.children {
			b = binary.BigEndian.AppendUint64(b, child.id)
			b = binary.BigEndian.AppendUint64(b, child.count)
			b = appendComponent(b, child.first)
			b = appendComponent(b, child.last)
		}
	}
	if len(b) > l.maxPageBytes {
		return nil, ErrResourceLimit
	}
	return checkRecord(b, c.limits)
} // #nosec G115 -- levels and entry counts are validated against eight and64.

func (q *pageReader) componentKeyTreeNode(id uint64, l componentKeyTreeLimits) (componentKeyTreeNode, error) {
	if err := l.validate(); err != nil {
		return componentKeyTreeNode{}, err
	}
	if !q.validPhysical(id) {
		return componentKeyTreeNode{}, ErrCorrupt
	}
	b, found, err := q.get(physicalKey(q.q.c.root.namespace, componentKeyTreeRecord, id))
	if err != nil {
		return componentKeyTreeNode{}, err
	}
	if !found {
		return componentKeyTreeNode{}, ErrCorrupt
	}
	if len(b) > l.maxPageBytes {
		return componentKeyTreeNode{}, ErrResourceLimit
	}
	c, err := inspectRecord(b, q.q.c.root.namespace, componentKeyTreeRecord, q.q.c.limits)
	if err != nil {
		return componentKeyTreeNode{}, err
	}
	self, err := c.number()
	if err != nil {
		return componentKeyTreeNode{}, err
	}
	level, err := c.tag()
	if err != nil || self != id || level >= 8 {
		return componentKeyTreeNode{}, ErrCorrupt
	}
	countWire, err := c.take(4)
	if err != nil {
		return componentKeyTreeNode{}, err
	}
	count := binary.BigEndian.Uint32(countWire)
	minimum, maximum := 29, l.maxKeys
	if level > 0 {
		minimum, maximum = 74, l.maxChildren
	}
	if count > 64 || uint64(count) > uint64(len(c.src)/minimum) || level > 0 && count < 2 {
		return componentKeyTreeNode{}, ErrCorrupt
	}
	if int(level) >= l.maxLevels || int(count) > maximum {
		return componentKeyTreeNode{}, ErrResourceLimit
	}
	n := componentKeyTreeNode{id: id, level: int(level)}
	if level == 0 {
		n.keys = make([]graphstate.ComponentKey, int(count))
	} else {
		n.children = make([]componentKeyTreeChild, int(count))
	}
	bytes := 128 + 128*int(count)
	for i := range int(count) {
		var child componentKeyTreeChild
		if level > 0 {
			child.id, err = c.number()
			if err != nil || !q.validPhysical(child.id) {
				return componentKeyTreeNode{}, ErrCorrupt
			}
			child.count, err = c.number()
			if err != nil {
				return componentKeyTreeNode{}, err
			}
		}
		key, err := decodeComponent(&c, q.q.c.limits)
		if err != nil {
			return componentKeyTreeNode{}, err
		}
		bytes += len(key.Name)
		if level == 0 {
			n.keys[i] = key
		} else {
			child.first = key
			child.last, err = decodeComponent(&c, q.q.c.limits)
			if err != nil {
				return componentKeyTreeNode{}, err
			}
			bytes += len(child.last.Name)
			n.children[i] = child
		}
	}
	if c.done() != nil {
		return componentKeyTreeNode{}, ErrCorrupt
	}
	if err := validateComponentKeyTreeNode(n, q.q.c, l); err != nil {
		return componentKeyTreeNode{}, storedBindingError(err)
	}
	if err := q.q.materialize(bytes); err != nil {
		return componentKeyTreeNode{}, err
	}
	if err := q.budget(); err != nil {
		return componentKeyTreeNode{}, err
	}
	q.work.DirectoryPages++
	return n, nil
} // #nosec G115 -- counts are bounded by delivered bytes and64 before allocation.

func (q *pageReader) componentKeyTreeRoot(root componentKeyTreeRoot, l componentKeyTreeLimits) (componentKeyTreeNode, error) {
	n, err := q.componentKeyTreeNode(root.id, l)
	if err != nil {
		return componentKeyTreeNode{}, err
	}
	if n.level != root.level || n.summary().count != root.count {
		return componentKeyTreeNode{}, ErrCorrupt
	}
	return n, nil
}

func (q *pageReader) componentKeyTreeChild(parent componentKeyTreeNode, index int, l componentKeyTreeLimits) (componentKeyTreeNode, error) {
	child := parent.children[index]
	n, err := q.componentKeyTreeNode(child.id, l)
	if err != nil {
		return componentKeyTreeNode{}, err
	}
	actual := n.summary()
	if n.level != parent.level-1 || actual.count != child.count || actual.first != child.first || actual.last != child.last {
		return componentKeyTreeNode{}, ErrCorrupt
	}
	return n, nil
}

func componentKeyChildIndex(n componentKeyTreeNode, key graphstate.ComponentKey) int {
	i, found := slices.BinarySearchFunc(n.children, key, func(child componentKeyTreeChild, key graphstate.ComponentKey) int {
		return compareComponentKeys(child.first, key)
	})
	if !found && i > 0 {
		i--
	}
	return min(i, len(n.children)-1)
}

func (q *pageReader) hasComponentKey(root componentKeyTreeRoot, key graphstate.ComponentKey, l componentKeyTreeLimits) (bool, error) {
	if !validComponent(key, q.q.c.limits) {
		return false, ErrInvalid
	}
	n, err := q.componentKeyTreeRoot(root, l)
	if err != nil {
		return false, err
	}
	for n.level > 0 {
		n, err = q.componentKeyTreeChild(n, componentKeyChildIndex(n, key), l)
		if err != nil {
			return false, err
		}
	}
	_, found := slices.BinarySearchFunc(n.keys, key, compareComponentKeys)
	return found, nil
}

func (q *pageStage) putComponentKeyTreeNode(n componentKeyTreeNode, l componentKeyTreeLimits) error {
	b, err := encodeComponentKeyTreeNode(n, q.q.c, l)
	if err != nil {
		return err
	}
	return q.q.put(physicalKey(q.root.namespace, componentKeyTreeRecord, n.id), b)
}

func (q *pageStage) newComponentKeyTree(l componentKeyTreeLimits) (componentKeyTreeRoot, error) {
	if err := l.validate(); err != nil {
		return componentKeyTreeRoot{}, err
	}
	id, err := q.reserve()
	if err != nil {
		return componentKeyTreeRoot{}, err
	}
	if err := q.putComponentKeyTreeNode(componentKeyTreeNode{id: id}, l); err != nil {
		return componentKeyTreeRoot{}, err
	}
	return componentKeyTreeRoot{id: id}, nil
}

func (q *pageStage) insertComponentKeyNode(n componentKeyTreeNode, key graphstate.ComponentKey, l componentKeyTreeLimits) ([]componentKeyTreeChild, bool, error) {
	if n.level == 0 {
		i, found := slices.BinarySearchFunc(n.keys, key, compareComponentKeys)
		if found {
			return []componentKeyTreeChild{n.summary()}, false, nil
		}
		key.Name = strings.Clone(key.Name)
		n.keys = slices.Insert(n.keys, i, key)
	} else {
		i := componentKeyChildIndex(n, key)
		child, err := q.componentKeyTreeChild(n, i, l)
		if err != nil {
			return nil, false, err
		}
		replacements, added, err := q.insertComponentKeyNode(child, key, l)
		if err != nil {
			return nil, false, err
		}
		if !added {
			return []componentKeyTreeChild{n.summary()}, false, nil
		}
		n.children = slices.Replace(n.children, i, i+1, replacements...)
	}
	count, maximum := len(n.keys), l.maxKeys
	if n.level > 0 {
		count, maximum = len(n.children), l.maxChildren
	}
	if count <= maximum {
		wire, err := encodeComponentKeyTreeNode(n, q.q.c, l)
		if err == nil {
			if err := q.q.put(physicalKey(q.root.namespace, componentKeyTreeRecord, n.id), wire); err != nil {
				return nil, false, err
			}
			return []componentKeyTreeChild{n.summary()}, true, nil
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
	right := componentKeyTreeNode{id: id, level: n.level}
	if n.level == 0 {
		right.keys, n.keys = n.keys[count/2:], n.keys[:count/2]
	} else {
		right.children, n.children = n.children[count/2:], n.children[:count/2]
	}
	for _, half := range []componentKeyTreeNode{n, right} {
		if err := q.putComponentKeyTreeNode(half, l); err != nil {
			return nil, false, err
		}
	}
	return []componentKeyTreeChild{n.summary(), right.summary()}, true, nil
}

func (q *pageStage) insertComponentKey(root componentKeyTreeRoot, key graphstate.ComponentKey, l componentKeyTreeLimits) (componentKeyTreeRoot, error) {
	if !validComponent(key, q.q.c.limits) {
		return componentKeyTreeRoot{}, ErrInvalid
	}
	n, err := q.componentKeyTreeRoot(root, l)
	if err != nil {
		return componentKeyTreeRoot{}, err
	}
	if root.count == math.MaxUint64 {
		return componentKeyTreeRoot{}, ErrResourceLimit
	}
	children, added, err := q.insertComponentKeyNode(n, key, l)
	if err != nil {
		return componentKeyTreeRoot{}, err
	}
	if !added {
		return root, nil
	}
	if len(children) == 1 {
		return componentKeyTreeRoot{children[0].id, n.level, children[0].count}, nil
	}
	if n.level+1 >= l.maxLevels {
		return componentKeyTreeRoot{}, ErrResourceLimit
	}
	id, err := q.reserve()
	if err != nil {
		return componentKeyTreeRoot{}, err
	}
	parent := componentKeyTreeNode{id: id, level: n.level + 1, children: children}
	if err := q.putComponentKeyTreeNode(parent, l); err != nil {
		return componentKeyTreeRoot{}, err
	}
	return componentKeyTreeRoot{id, parent.level, parent.summary().count}, nil
}
