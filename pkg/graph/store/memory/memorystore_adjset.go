package memory

import (
	"iter"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// adjSetSmallMax is the largest adjacency set kept as a plain slice. Above
// it the set switches to a hash set (and stays one until it empties).
const adjSetSmallMax = 64

// adjSet is one node's outgoing or incoming relationship-ID set (P7).
//
// Most nodes have a few dozen relationships per direction, and a Go hash set
// costs ~34 B per member at that size (measured, synthday degrees); an
// unordered slice costs ~10 B. So a set holds its members in ids while it has
// at most adjSetSmallMax of them — add, remove and has binary-search at most
// 64 IDs — and in m above that, where the slice would stop being cheap. Exactly
// one of ids / m is in use. The slice is kept in ascending ID order (round 5
// request 2), so a reader that emits in relationship-ID order skips its sort
// while the set is small; iteration over m is unordered, as before.
//
// A nil *adjSet is an empty set for len, has, remove and all. Guarded by the
// owning Store's mu.
type adjSet struct {
	ids []types.RelID
	m   map[types.RelID]struct{}
}

// newAdjSetOf returns a set holding ids (duplicates collapse).
func newAdjSetOf(ids ...types.RelID) *adjSet {
	s := &adjSet{}
	for _, id := range ids {
		s.add(id)
	}
	return s
}

// add inserts id; adding a member again is a no-op.
func (s *adjSet) add(id types.RelID) {
	if s.m != nil {
		s.m[id] = struct{}{}
		return
	}
	n := len(s.ids)
	at := s.search(id)
	if at < n && s.ids[at] == id {
		return
	}
	if n < adjSetSmallMax {
		s.ids = append(s.ids, 0)
		copy(s.ids[at+1:], s.ids[at:n])
		s.ids[at] = id
		return
	}
	s.m = make(map[types.RelID]struct{}, len(s.ids)+1)
	for _, x := range s.ids {
		s.m[x] = struct{}{}
	}
	s.m[id] = struct{}{}
	s.ids = nil
}

// remove deletes id and reports whether it was a member.
func (s *adjSet) remove(id types.RelID) bool {
	if s == nil {
		return false
	}
	if s.m != nil {
		if _, ok := s.m[id]; !ok {
			return false
		}
		delete(s.m, id)
		return true
	}
	n := len(s.ids)
	at := s.search(id)
	if at == n || s.ids[at] != id {
		return false
	}
	copy(s.ids[at:], s.ids[at+1:])
	s.ids = s.ids[:n-1]
	return true
}

// search returns the position of the first member >= id in the ascending
// slice (len(ids) when none is).
func (s *adjSet) search(id types.RelID) int {
	lo, hi := 0, len(s.ids)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if s.ids[m] < id {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo
}

// has reports whether id is a member.
func (s *adjSet) has(id types.RelID) bool {
	if s == nil {
		return false
	}
	if s.m != nil {
		_, ok := s.m[id]
		return ok
	}
	at := s.search(id)
	return at < len(s.ids) && s.ids[at] == id
}

// len is the number of members.
func (s *adjSet) len() int {
	if s == nil {
		return 0
	}
	if s.m != nil {
		return len(s.m)
	}
	return len(s.ids)
}

// all yields every member once: ascending while the set is a slice,
// unordered once it is a hash set. The set must not be
// modified during the iteration (no caller does: they collect first).
func (s *adjSet) all() iter.Seq[types.RelID] {
	return func(yield func(types.RelID) bool) {
		if s == nil {
			return
		}
		if s.m != nil {
			for id := range s.m {
				if !yield(id) {
					return
				}
			}
			return
		}
		for _, id := range s.ids {
			if !yield(id) {
				return
			}
		}
	}
}

// addAdjLocked records id in the adjacency set of nid in idx.
func addAdjLocked(idx map[types.NodeID]*adjSet, nid types.NodeID, id types.RelID) {
	s := idx[nid]
	if s == nil {
		s = &adjSet{}
		idx[nid] = s
	}
	s.add(id)
}

// removeAdjLocked drops id from the adjacency set of nid in idx and drops
// the set once it is empty.
func removeAdjLocked(idx map[types.NodeID]*adjSet, nid types.NodeID, id types.RelID) {
	if s := idx[nid]; s != nil {
		s.remove(id)
		if s.len() == 0 {
			delete(idx, nid)
		}
	}
}
