package core

import (
	"sync"
	"sync/atomic"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Degree tracking (sigma-tkgd round 5 request 5, built in round 7 request 2):
// for a relationship type a caller opted in (StatOps.TrackRelTypeDegrees),
// RelTypeDegreeStats keeps the type's per-node out- and in-counts after its
// first count and applies the relationships created since, instead of
// counting the type again after every write to it.
//
// Soundness rests on an accounting identity, not on seeing every write: the
// create kernel records each relationship of a tracked type it created
// (start, end) while it holds the graph's read lock, after the store's
// per-type mutation stamp moved by one for it. A refresh takes the graph's
// write lock, so no create is half done: if the stamp moved by exactly the
// number of recorded creates since the counts were taken, every change since
// was one of those creates, and adding them gives the counts of the current
// relationships. Any other change of the type (a delete, an update, a create
// through a door that does not record, a write of a type sharing the stamp's
// stripe, a rollback) moves the stamp without a record, the identity fails,
// and the type is counted again, as without tracking.

// maxPendingDegreeEdges bounds the creates kept between two refreshes; past
// it the next refresh counts again.
const maxPendingDegreeEdges = 1 << 16

// relDegreeTracker holds the opted-in types. any is the one load a create
// pays when no type is tracked.
type relDegreeTracker struct {
	any   atomic.Bool
	mu    sync.Mutex
	types map[string]*degreeTrack
	// counts and applies count the full counts and the applied refreshes of
	// tracked types (test seam and telemetry).
	counts, applies atomic.Uint64
}

type degreeTrack struct {
	pending  []degreeEdge
	overflow bool
	base     *degreeBase
}

type degreeEdge struct{ start, end types.NodeID }

// degreeBase is a type's per-node counts at a stamp, with the stats they fold to.
type degreeBase struct {
	tok     uint16
	epoch   uint64
	out, in map[types.NodeID]int64
	stats   storepkg.RelTypeDegreeStats
}

// add applies one created relationship to the counts and the maxima (the
// smallest node ID among the nodes holding a maximum, as
// DegreeStatsFromCounts picks it).
func (b *degreeBase) add(e degreeEdge) {
	b.stats.Rels++
	o := b.out[e.start] + 1
	if o == 1 {
		b.stats.Starts++
	}
	b.out[e.start] = o
	if o > b.stats.MaxOut || (o == b.stats.MaxOut && e.start < b.stats.MaxOutNode) {
		b.stats.MaxOut, b.stats.MaxOutNode = o, e.start
	}
	i := b.in[e.end] + 1
	if i == 1 {
		b.stats.Ends++
	}
	b.in[e.end] = i
	if i > b.stats.MaxIn || (i == b.stats.MaxIn && e.end < b.stats.MaxInNode) {
		b.stats.MaxIn, b.stats.MaxInNode = i, e.end
	}
}

// TrackRelTypeDegrees opts typeName in to maintained degree counts: after its
// next count, RelTypeDegreeStats(typeName) applies the relationships created
// since (Rels().Add / AddByID / AddByIDIfAbsent / Import, their transaction
// mirrors, GraphTx.AddRelationships, Batch) instead of counting the type
// again after a write to it; any other change of the type (a delete, an
// update, a rollback, another door) counts again, as without tracking. Cost,
// only for a tracked type: the per-node counts (two maps, about 40-50 B per
// distinct endpoint), up to 65,536 recorded creates (16 B each) between two
// reads, a short lock per create to record it, and the graph's write lock for
// O(creates since) at a read that applies them. Untracked types and graphs
// pay one atomic load per create. The type need not exist yet. Errors: a
// malformed type name, ErrGraphClosed.
func (s *StatOps) TrackRelTypeDegrees(typeName string) error {
	c := s.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	t := &c.relDegreeTrack
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.types == nil {
		t.types = make(map[string]*degreeTrack)
	}
	if _, ok := t.types[typeName]; !ok {
		t.types[typeName] = &degreeTrack{}
	}
	t.any.Store(true)
	return nil
}

// UntrackRelTypeDegrees drops typeName's maintained counts (RelTypeDegreeStats
// counts it again after each write to it). Untracking an untracked type is a
// no-op. Errors: a malformed type name, ErrGraphClosed.
func (s *StatOps) UntrackRelTypeDegrees(typeName string) error {
	c := s.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	t := &c.relDegreeTrack
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.types, typeName)
	t.any.Store(len(t.types) > 0)
	return nil
}

// noteRelsCreated records created relationships of a tracked type. The
// caller holds the graph's read (or write) lock and the store wrote rels.
func (c *Core) noteRelsCreated(typeName string, rels ...*types.Relationship) {
	t := &c.relDegreeTrack
	if !t.any.Load() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	tr := t.types[typeName]
	if tr == nil || tr.overflow {
		return
	}
	if len(tr.pending)+len(rels) > maxPendingDegreeEdges {
		tr.pending, tr.overflow = nil, true
		return
	}
	for _, r := range rels {
		tr.pending = append(tr.pending, degreeEdge{start: r.StartNodeID(), end: r.EndNodeID()})
	}
}

// trackedDegreeStats serves RelTypeDegreeStats of a tracked type:
// handled=false when the type is not tracked or the store has no per-type
// stamp (the caller then counts as without tracking).
func (c *Core) trackedDegreeStats(typeName string, tok uint16) (storepkg.RelTypeDegreeStats, bool, error) {
	var zero storepkg.RelTypeDegreeStats
	t := &c.relDegreeTrack
	if !t.any.Load() || tok == 0 {
		return zero, false, nil
	}
	if _, perType := c.store.(relTypeEpochScanner); !perType || !c.storeRowsTrust {
		return zero, false, nil
	}
	t.mu.Lock()
	_, tracked := t.types[typeName]
	t.mu.Unlock()
	if !tracked {
		return zero, false, nil
	}
	if st, ok, err := c.applyPendingDegrees(typeName, tok); err != nil || ok {
		return st, true, err
	}
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		before, _ := c.relDegreeEpoch(tok)
		t.counts.Add(1)
		out, in, rels, err := c.countRelDegreeMaps(typeName, tok)
		if err != nil {
			return zero, true, err
		}
		base := &degreeBase{tok: tok, epoch: before, out: out, in: in, stats: storepkg.DegreeStatsFromCounts(rels, out, in)}
		installed, err := c.installDegreeBase(typeName, base)
		if err != nil {
			return zero, true, err
		}
		if installed {
			st := base.stats
			st.Exact = true
			return st, true, nil
		}
	}
	return zero, false, nil // a writer kept moving the stamp: count as without tracking
}

// applyPendingDegrees brings a tracked type's counts to the current stamp from
// its recorded creates when the accounting identity holds. ok=false: the
// counts must be taken again.
func (c *Core) applyPendingDegrees(typeName string, tok uint16) (storepkg.RelTypeDegreeStats, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return storepkg.RelTypeDegreeStats{}, false, ErrGraphClosed
	}
	now, _ := c.relDegreeEpoch(tok)
	t := &c.relDegreeTrack
	t.mu.Lock()
	defer t.mu.Unlock()
	tr := t.types[typeName]
	if tr == nil || tr.base == nil || tr.base.tok != tok || tr.overflow || tr.base.epoch+uint64(len(tr.pending)) != now {
		return storepkg.RelTypeDegreeStats{}, false, nil
	}
	for _, e := range tr.pending {
		tr.base.add(e)
	}
	if len(tr.pending) > 0 {
		t.applies.Add(1)
	}
	tr.pending = tr.pending[:0]
	tr.base.epoch = now
	st := tr.base.stats
	st.Exact = true
	return st, true, nil
}

// installDegreeBase keeps counts taken at base.epoch when the stamp is still
// there under the graph's write lock (no create is half done, so every
// recorded create is in the counts): the recorded creates are dropped.
func (c *Core) installDegreeBase(typeName string, base *degreeBase) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return false, ErrGraphClosed
	}
	if now, _ := c.relDegreeEpoch(base.tok); now != base.epoch {
		return false, nil
	}
	t := &c.relDegreeTrack
	t.mu.Lock()
	defer t.mu.Unlock()
	tr := t.types[typeName]
	if tr == nil {
		return true, nil // untracked meanwhile: the counts still answer this call
	}
	tr.base, tr.pending, tr.overflow = base, nil, false
	return true, nil
}

// countRelDegreeMaps counts a type's per-node degrees: from a native store's
// adjacency index, else by streaming the type's relationships.
func (c *Core) countRelDegreeMaps(typeName string, tok uint16) (out, in map[types.NodeID]int64, rels int64, err error) {
	if native, ok := c.store.(storepkg.RelTypeDegreeCountsCapability); ok {
		var served bool
		if err := c.readUnderRLock(func() error {
			var err error
			out, in, rels, served, err = native.RelTypeDegreeCounts(tok)
			return err
		}); err != nil {
			return nil, nil, 0, err
		}
		if served {
			return out, in, rels, nil
		}
	}
	out = make(map[types.NodeID]int64)
	in = make(map[types.NodeID]int64)
	err = c.Rels.ForEachByType(typeName, storepkg.QueryOpts{}, func(r *types.Relationship) bool {
		rels++
		out[r.StartNodeID()]++
		in[r.EndNodeID()]++
		return true
	})
	return out, in, rels, err
}
