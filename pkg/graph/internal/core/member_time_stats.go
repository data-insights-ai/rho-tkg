package core

import (
	"errors"
	"math"
	"sync"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Member time statistics (sigma-tkgd round 8 request 3): per label (type)
// the current members carrying explicit valid time and the latest end of a
// member's row, so a source can declare its rows eternal at bind without
// scanning them.
//
// No write cost. The explicit-valid count is derived: the first call after a
// write to the label (type) streams its current rows once, then the count is
// cached against the store's mutation epoch for the label (badger, sharded:
// the label's own stamp; memory: the node stamp) or type (memory, badger: the
// type's stripe) and served in O(1) until the epoch moves; the same epochs
// keep the DocValues columns, valid-time columns included, fresh. LastEnded
// is O(1) where the store holds no history row of the kind at all
// (store.HistoryCountCapability), else a pass over the entities whose rows
// ever carried the label (the label membership sidecar) with a RAM presence
// check each and a stamp lookup for those with history, no row read on memory
// and badger.

// memberTimeCache keeps the explicit-valid count per (kind, token) with the
// epoch it was counted at. The zero value is ready.
type memberTimeCache struct {
	mu      sync.Mutex
	entries map[memberTimeKey]memberTimeEntry
}

type memberTimeKey struct {
	rel bool
	tok uint16
}

type memberTimeEntry struct {
	epoch             uint64
	members, explicit int64
	// LastEnded, cached under the store's history generation (lastOK).
	lastGen uint64
	last    types.Instant
	lastOK  bool
}

func (m *memberTimeCache) get(k memberTimeKey, epoch uint64) (memberTimeEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[k]
	return e, ok && e.epoch == epoch
}

func (m *memberTimeCache) put(k memberTimeKey, e memberTimeEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = make(map[memberTimeKey]memberTimeEntry)
	}
	prev := m.entries[k]
	if !e.lastOK && prev.lastOK {
		e.lastGen, e.last, e.lastOK = prev.lastGen, prev.last, true
	}
	m.entries[k] = e
}

// lastEnded returns the LastEnded cached under gen.
func (m *memberTimeCache) lastEnded(k memberTimeKey, gen uint64) (types.Instant, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[k]
	if !ok || !e.lastOK || e.lastGen != gen {
		return 0, false
	}
	return e.last, true
}

// putLastEnded caches a LastEnded under gen, keeping the explicit count.
func (m *memberTimeCache) putLastEnded(k memberTimeKey, gen uint64, last types.Instant) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.entries == nil {
		m.entries = make(map[memberTimeKey]memberTimeEntry)
	}
	e := m.entries[k]
	if e.epoch == 0 && e.members == 0 && !e.lastOK {
		e.epoch = math.MaxUint64 // no explicit count cached yet
	}
	e.lastGen, e.last, e.lastOK = gen, last, true
	m.entries[k] = e
}

// historyGenerationScanner is the store's history generation (memory,
// badger): it moves at every history row write and removal and at Clear.
type historyGenerationScanner interface {
	HistoryGeneration() uint64
}

// cachedLastEnded serves compute's LastEnded from the cache while the store's
// history generation stands: LastEnded changes only with a history row (a row
// ends by becoming one; an entity joins the ever-members with history only by
// a write that ends a row). Without the generation it computes every call.
func (c *Core) cachedLastEnded(k memberTimeKey, compute func() (types.Instant, error)) (types.Instant, error) {
	hg, ok := c.store.(historyGenerationScanner)
	if !ok || !c.storeRowsTrust {
		return compute()
	}
	gen := hg.HistoryGeneration()
	if last, hit := c.memberTime.lastEnded(k, gen); hit {
		return last, nil
	}
	last, err := compute()
	if err != nil {
		return 0, err
	}
	if hg.HistoryGeneration() == gen {
		c.memberTime.putLastEnded(k, gen, last)
	}
	return last, nil
}

// nodeLabelEpoch is the freshness stamp of a label's current rows: the
// label's own stamp, else the node stamp; ok=false without either.
func (c *Core) nodeLabelEpoch(tok uint16) (uint64, bool) {
	if !c.storeRowsTrust {
		return 0, false
	}
	if s, ok := c.store.(nodeLabelMutationEpochScanner); ok {
		return s.NodeLabelMutationEpoch(tok), true
	}
	if s, ok := c.store.(nodeMutationEpochScanner); ok {
		return s.NodeMutationEpoch(), true
	}
	return 0, false
}

// explicitValid reports whether a row carries a valid-from or valid-to of
// its own, read on the graph's valid axis a (round 11: on the literal axis
// the instant 0 is a claim). A start at -inf (types.UnboundedValidFrom, the
// untimed first version under Config.UntimedValidStart = UntimedUnbounded,
// backlog 73, and every untimed write under UntimedWholeAxis) and the open
// end bound nothing and do not count.
func explicitValid(a types.ValidAxis, from, to types.Instant) bool {
	return (a.Claimed(from) && from != types.UnboundedValidFrom) || !a.Open(to)
}

// LabelTimeStats returns store.MemberTimeStats for label: current members,
// those carrying explicit valid time, and the latest end of a member row.
// exact=false when writers kept moving the label's epoch across three counts
// (the answer is then one count's) or the store keeps no epoch (counted on
// every call). An unknown label is zero stats, exact. Errors: a malformed
// label, ErrGraphClosed, a store error.
func (s *StatOps) LabelTimeStats(label string) (storepkg.MemberTimeStats, bool, error) {
	c := s.c
	var zero storepkg.MemberTimeStats
	if err := c.checkOpen(); err != nil {
		return zero, false, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return zero, false, err
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return zero, false, err
	}
	if !ok {
		return zero, true, nil
	}
	key := memberTimeKey{tok: tok}
	var out storepkg.MemberTimeStats
	exact := false
	for attempt := 0; attempt < 3 && !exact; attempt++ {
		before, epochOK := c.nodeLabelEpoch(tok)
		entry, cached := c.memberTime.get(key, before)
		if !epochOK || !cached {
			var err error
			entry, err = c.countLabelExplicitValid(tok)
			if err != nil {
				return zero, false, err
			}
			entry.epoch = before
		}
		last, err := c.cachedLastEnded(key, func() (types.Instant, error) { return c.labelLastEnded(tok) })
		if err != nil {
			return zero, false, err
		}
		out = storepkg.MemberTimeStats{Members: entry.members, ExplicitValid: entry.explicit, LastEnded: last}
		if !epochOK {
			break
		}
		if after, _ := c.nodeLabelEpoch(tok); after == before {
			exact = true
			if !cached {
				c.memberTime.put(key, entry)
			}
		}
	}
	return out, exact, nil
}

// countLabelExplicitValid streams the label's current rows once.
func (c *Core) countLabelExplicitValid(tok uint16) (memberTimeEntry, error) {
	var e memberTimeEntry
	a := c.axis()
	visit := func(n *types.Node) bool {
		e.members++
		if from, to, _ := n.ValidRange(); explicitValid(a, from, to) {
			e.explicit++
		}
		return true
	}
	if scanner, ok := c.store.(nodeLabelScanner); ok && c.storeRowsTrust {
		return e, scanner.ForEachNodeByLabel(tok, storepkg.QueryOpts{}, visit)
	}
	var nodes []*types.Node
	if err := c.readUnderRLock(func() error {
		var err error
		nodes, err = c.store.NodesByLabel(tok, storepkg.QueryOpts{})
		return err
	}); err != nil {
		return e, err
	}
	for _, n := range nodes {
		visit(n)
	}
	return e, nil
}

// labelLastEnded is the largest end stamp (TxTo or DeletedAt) over the
// history rows of the label's ever-members: 0 at once when the store holds no
// node history row; otherwise the members without history are skipped by the
// RAM presence check and the others' stamps read without a row
// (store.HistoryStampsCapability).
func (c *Core) labelLastEnded(tok uint16) (types.Instant, error) {
	if hc, ok := c.store.(storepkg.HistoryCountCapability); ok && c.storeRowsTrust {
		n, err := hc.NodeHistoryCount()
		if err != nil {
			return 0, err
		}
		if n == 0 {
			return 0, nil
		}
	}
	var last types.Instant
	err := c.readUnderRLock(func() error {
		ids, err := c.nodeLabelCandidateIDsAt(tok, storepkg.QueryOpts{TxPin: math.MaxInt64})
		if err != nil {
			return err
		}
		// Without the label membership sidecar the candidates are every node
		// with history: keep the ones a row of which carried the label.
		verify := c.labelTxMembers == nil
		presence := c.historyPresence()
		for _, id := range ids {
			if verify {
				member, err := c.nodeEverCarried(id, tok)
				if err != nil {
					return err
				}
				if !member {
					continue
				}
			}
			if presence != nil {
				has, err := presence.HasNodeHistory(id)
				if err != nil {
					return err
				}
				if !has {
					continue
				}
			}
			_, ht, has, err := c.nodeHistoryStamps(id)
			if err != nil {
				return err
			}
			if has && ht > last {
				last = ht
			}
		}
		return nil
	})
	return last, err
}

// RelTypeTimeStats is LabelTimeStats for a relationship type (the type's
// current relationships, its ever-members' row ends; the type's epoch stripe
// on memory and badger).
func (s *StatOps) RelTypeTimeStats(typeName string) (storepkg.MemberTimeStats, bool, error) {
	c := s.c
	var zero storepkg.MemberTimeStats
	if err := c.checkOpen(); err != nil {
		return zero, false, err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return zero, false, err
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return zero, false, err
	}
	if !ok {
		return zero, true, nil
	}
	key := memberTimeKey{rel: true, tok: tok}
	var out storepkg.MemberTimeStats
	exact := false
	for attempt := 0; attempt < 3 && !exact; attempt++ {
		before, epochOK := c.relDegreeEpoch(tok)
		epochOK = epochOK && c.storeRowsTrust
		entry, cached := c.memberTime.get(key, before)
		if !epochOK || !cached {
			var err error
			entry, err = c.countTypeExplicitValid(tok)
			if err != nil {
				return zero, false, err
			}
			entry.epoch = before
		}
		last, err := c.cachedLastEnded(key, func() (types.Instant, error) { return c.typeLastEnded(tok) })
		if err != nil {
			return zero, false, err
		}
		out = storepkg.MemberTimeStats{Members: entry.members, ExplicitValid: entry.explicit, LastEnded: last}
		if !epochOK {
			break
		}
		if after, _ := c.relDegreeEpoch(tok); after == before {
			exact = true
			if !cached {
				c.memberTime.put(key, entry)
			}
		}
	}
	return out, exact, nil
}

// countTypeExplicitValid streams the type's current relationships once.
func (c *Core) countTypeExplicitValid(tok uint16) (memberTimeEntry, error) {
	var e memberTimeEntry
	a := c.axis()
	visit := func(r *types.Relationship) bool {
		e.members++
		if from, to, _ := r.ValidRange(); explicitValid(a, from, to) {
			e.explicit++
		}
		return true
	}
	if scanner, ok := c.store.(relTypeScanner); ok && c.storeRowsTrust {
		return e, scanner.ForEachRelByType(tok, storepkg.QueryOpts{}, visit)
	}
	var rels []*types.Relationship
	if err := c.readUnderRLock(func() error {
		var err error
		rels, err = c.store.RelationshipsByType(tok, storepkg.QueryOpts{})
		return err
	}); err != nil {
		return e, err
	}
	for _, r := range rels {
		visit(r)
	}
	return e, nil
}

// typeLastEnded is labelLastEnded for a relationship type.
func (c *Core) typeLastEnded(tok uint16) (types.Instant, error) {
	if hc, ok := c.store.(storepkg.HistoryCountCapability); ok && c.storeRowsTrust {
		n, err := hc.RelHistoryCount()
		if err != nil {
			return 0, err
		}
		if n == 0 {
			return 0, nil
		}
	}
	var last types.Instant
	err := c.readUnderRLock(func() error {
		ids, err := c.relTypeCandidateIDsAt(tok, storepkg.QueryOpts{TxPin: math.MaxInt64})
		if err != nil {
			return err
		}
		verify := c.relTypeTxMembers == nil
		presence := c.historyPresence()
		for _, id := range ids {
			if verify {
				member, err := c.relEverCarried(id, tok)
				if err != nil {
					return err
				}
				if !member {
					continue
				}
			}
			if presence != nil {
				has, err := presence.HasRelHistory(id)
				if err != nil {
					return err
				}
				if !has {
					continue
				}
			}
			_, ht, has, err := c.relHistoryStamps(id)
			if err != nil {
				return err
			}
			if has && ht > last {
				last = ht
			}
		}
		return nil
	})
	return last, err
}

// nodeEverCarried reports whether the current row or a history row of id
// carries the label. Callers hold c.mu (R or W).
func (c *Core) nodeEverCarried(id types.NodeID, tok uint16) (bool, error) {
	cur, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return false, err
	}
	if cur != nil && cur.HasLabelTokenRaw(tok) {
		return true, nil
	}
	hist, err := c.getNodeHistory(id)
	if err != nil {
		return false, err
	}
	for _, h := range hist {
		if h.HasLabelTokenRaw(tok) {
			return true, nil
		}
	}
	return false, nil
}

// relEverCarried is nodeEverCarried for a relationship type.
func (c *Core) relEverCarried(id types.RelID, tok uint16) (bool, error) {
	cur, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return false, err
	}
	if cur != nil && cur.HasTypeTokenRaw(tok) {
		return true, nil
	}
	hist, err := c.getRelHistory(id)
	if err != nil {
		return false, err
	}
	for _, h := range hist {
		if h.HasTypeTokenRaw(tok) {
			return true, nil
		}
	}
	return false, nil
}

// historyPresence is the store's RAM history presence check (nil without
// one, or for an untrusted store).
func (c *Core) historyPresence() storepkg.HistoryPresenceCapability {
	if p, ok := c.store.(storepkg.HistoryPresenceCapability); ok && c.storeRowsTrust {
		return p
	}
	return nil
}
