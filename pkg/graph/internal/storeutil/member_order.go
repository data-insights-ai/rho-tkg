package storeutil

import (
	"slices"
	"sync"
)

// MemberOrder keeps, per label or relationship-type token, the members of a
// store's RAM membership index (a map, which cannot be iterated in pieces
// while writers run) as an ascending ID slice that a streaming scan can walk
// without first collecting and sorting the whole set.
//
// A list is built lazily, on the first scan of its token, and then kept:
//   - Add appends an ID larger than the list's last one in place; any other
//     ID (an older entity acquiring the label, an ID reused by an import)
//     drops the list, and the next scan builds it again.
//   - Remove only counts: the list is a SUPERSET of the members, so a scan
//     re-checks every row it fetches (it already does, rows can be deleted
//     between the snapshot and the fetch). When more than half the entries
//     are stale (and more than 64) the list is dropped.
//
// A scan takes the slice header (Ordered) and walks it after releasing the
// owner's lock. Appends write only beyond every header already handed out,
// and a reallocation leaves the old array to the scans that hold it, so the
// walk sees exactly the members the snapshot saw (plus deleted ones it skips),
// as the collected snapshot did.
//
// Locking: Add, Remove and Reset are called under the owner's EXCLUSIVE lock
// (the one that guards the membership map); Ordered and Install under its
// SHARED lock. Writers therefore never run beside a reader, and readers
// serialize among themselves on mu.
type MemberOrder[ID ~int64] struct {
	mu    sync.Mutex
	lists map[uint16]*memberList[ID]
	gens  map[uint16]uint64
}

type memberList[ID ~int64] struct {
	ids   []ID
	stale int
}

// Add records that id joined tok's membership (exclusive lock held).
func (o *MemberOrder[ID]) Add(tok uint16, id ID) {
	if o.gens == nil {
		o.gens = make(map[uint16]uint64)
	}
	o.gens[tok]++
	l := o.lists[tok]
	if l == nil {
		return
	}
	if n := len(l.ids); n == 0 || id > l.ids[n-1] {
		l.ids = append(l.ids, id)
		return
	}
	delete(o.lists, tok)
}

// Remove records that a member left tok's membership (exclusive lock held).
// A removal not recorded here is caught by Ordered's count check.
func (o *MemberOrder[ID]) Remove(tok uint16) {
	l := o.lists[tok]
	if l == nil {
		return
	}
	l.stale++
	if l.stale > 64 && 2*l.stale > len(l.ids) {
		delete(o.lists, tok)
	}
}

// Reset drops every list (exclusive lock held; a store Clear).
func (o *MemberOrder[ID]) Reset() {
	o.lists = nil
	o.gens = nil
}

// Ordered returns tok's ascending member list when one is kept and its
// count of live entries equals members, the size of the owner's membership
// set (shared lock held). The count check is the safety net: a membership
// change that reached the set without Add or Remove makes the counts differ,
// and the list is rebuilt instead of trusted. Otherwise ok is false and gen
// is the token's membership generation, which the caller passes to Install
// with the members it collected under the same shared lock.
func (o *MemberOrder[ID]) Ordered(tok uint16, members int) (ids []ID, gen uint64, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if l := o.lists[tok]; l != nil {
		if len(l.ids)-l.stale == members {
			return l.ids, 0, true
		}
		delete(o.lists, tok)
	}
	return nil, o.gens[tok], false
}

// SortMembers sorts members ascending; callers sort what they collected
// before they Install it, outside the owner's lock.
func SortMembers[ID ~int64](members []ID) { slices.Sort(members) }

// Install keeps sorted (ascending members of tok collected under the shared
// lock at generation gen) as tok's list when no member joined since; the
// shared lock is held again for the call. The caller may keep walking sorted
// but must not modify it.
func (o *MemberOrder[ID]) Install(tok uint16, gen uint64, sorted []ID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.gens[tok] == gen && o.lists[tok] == nil {
		if o.lists == nil {
			o.lists = make(map[uint16]*memberList[ID])
		}
		o.lists[tok] = &memberList[ID]{ids: sorted}
	}
}
