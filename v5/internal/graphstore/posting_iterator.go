package graphstore

import "cmp"

// One operation-local iterator retains at most eight charged decoded frames.
// Prefix/after seeks use tree fences, not application-key or history scans.
type postingIterator struct {
	q         *pageReader
	limits    postingTreeLimits
	prefix    postingKey
	frames    [8]postingFrame
	depth     int
	after     postingKey
	inclusive bool
}
type postingFrame struct {
	node  postingTreeNode
	index int
}

func comparePostingPrefix(k, p postingKey) int {
	if order := cmp.Compare(k.family, p.family); order != 0 {
		return order
	}
	switch p.family {
	case uniquePostingRecord:
		return cmp.Or(cmp.Compare(k.owner, p.owner), cmp.Compare(k.name, p.name), cmp.Compare(k.scalar, p.scalar), cmp.Compare(k.value, p.value))
	case canonicalIncidentRecord:
		return cmp.Or(cmp.Compare(k.endpoint, p.endpoint), cmp.Compare(k.mode, p.mode))
	default:
		return cmp.Or(cmp.Compare(k.endpoint, p.endpoint), cmp.Compare(k.bound, p.bound))
	}
}
func newPostingIterator(q *pageReader, root postingTreeRoot, prefix, after postingKey, inclusive bool) (*postingIterator, error) {
	limits := keyTreeLimits(q.limits)
	if err := q.q.materialize(1024); err != nil {
		return nil, err
	}
	n, err := q.postingTreeRoot(root, limits)
	if err != nil {
		return nil, err
	}
	return &postingIterator{q: q, limits: limits, prefix: prefix, after: after, inclusive: inclusive, frames: [8]postingFrame{{node: n}}, depth: 1}, nil
}

// afterRelationship skips the entire already emitted relationship's life run.
// It uses fence comparisons rather than decoding every duplicate posting.
func (it *postingIterator) next(afterRelationship uint64) (postingKey, bool, error) {
	for it.depth > 0 {
		frame := &it.frames[it.depth-1]
		if frame.node.level == 0 {
			for frame.index < len(frame.node.keys) {
				k := frame.node.keys[frame.index]
				frame.index++
				prefix := comparePostingPrefix(k, it.prefix)
				if prefix < 0 {
					continue
				}
				if prefix > 0 {
					it.depth = 0
					return postingKey{}, false, nil
				}
				order := comparePostingKeys(k, it.after)
				if order < 0 || !it.inclusive && order == 0 || afterRelationship != 0 && uint64(k.relationship) <= afterRelationship {
					continue
				}
				return k, true, nil
			}
			it.frames[it.depth-1] = postingFrame{}
			it.depth--
			continue
		}
		found := false
		for frame.index < len(frame.node.children) {
			i := frame.index
			frame.index++
			child := frame.node.children[i]
			prefix := comparePostingPrefix(child.last, it.prefix)
			if prefix < 0 {
				continue
			}
			if comparePostingPrefix(child.first, it.prefix) > 0 {
				it.depth = 0
				return postingKey{}, false, nil
			}
			order := comparePostingKeys(child.last, it.after)
			if order < 0 || !it.inclusive && order == 0 || prefix == 0 && afterRelationship != 0 && uint64(child.last.relationship) <= afterRelationship {
				continue
			}
			n, err := it.q.postingTreeChild(frame.node, i, it.limits)
			if err != nil {
				return postingKey{}, false, err
			}
			if it.depth >= len(it.frames) {
				return postingKey{}, false, ErrCorrupt
			}
			it.frames[it.depth] = postingFrame{node: n}
			it.depth++
			found = true
			break
		}
		if !found {
			it.frames[it.depth-1] = postingFrame{}
			it.depth--
		}
	}
	return postingKey{}, false, nil
}
