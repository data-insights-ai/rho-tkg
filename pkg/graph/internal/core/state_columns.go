package core

import (
	"math"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// State cuts (backlog 28, requested by sigma-tkgd 2026-10-09)
//
// The state form {ValidAt, TxAt} of CountByLabelAt / CountByTypeAt and of
// ScanNodeColumns / ScanRelColumns answers from a cached effective cut of the
// label or type at the pin TxAt instead of folding every candidate's rows per
// call. A cut is the label's effective-timeline segments at the pin
// (ForEachNodeEffectiveByLabel / ForEachRelEffectiveByType: for every valid
// instant t, the segments containing t are exactly ByLabel(label, {ValidAt: t,
// TxAt: pin})), so one cut answers every valid instant and the cache key holds
// no valid time: (kind, token, pin). Per segment it keeps the entity ID, the
// segment's valid bounds, the winning row's own ValidRange (and a
// relationship's endpoints), and per requested property key the value
// classified for the column drivers (store.ClassifyScalar) — no row.
//
// A count is the segments containing the valid instant; a scan replays the
// same rows' cells through store.ScanColumnsFromNodeSource /
// ScanColumnsFromRelSource, the row drivers' rules (kinds, Null, Other,
// ErrMixedNumericColumn). Ordinals are 0 at a read coordinate on every path
// (the fold's too, scan_temporal_opts.go): a winning row's stored ordinal
// changed when a later write moved or deleted it.
//
// A cut is built on the second ask for its (kind, token, pin): a pin read
// once costs the fold, as before. Invalidation is the as-of column set's
// (asOfColumnCache, shared epoch and pin bound): a state at a pin changes only by a history rewrite at or below
// it, never by a forward write. A cut is cached only once every write stamped
// at or below its pin has landed (pinLanded). Memory: one byte budget per
// graph for all cuts (stateCutBudget), least recently used evicted first; a
// cut over the budget answers its call and is not kept.
// =============================================================================

// stateCutBudget bounds the bytes all cached state cuts of a graph hold
// (stateCut.bytes). A 1 M-node label with two property keys holds ~58 MB.
const stateCutBudget int64 = 256 << 20

// stateCutKey names a cut: a node label or relationship type token at a pin.
type stateCutKey struct {
	rel bool
	tok uint16
	pin types.Instant
}

// Cell tags: absent, held but not a column scalar (ColumnData.Other), or a
// scalar of kind tag-cellScalar.
const (
	cellAbsent uint8 = iota
	cellOther
	cellScalar
)

// stateCut is one label's (type's) effective cut at a pin, as parallel
// arrays: segment i is entity ids[i] over [from[i], to[i]) (to 0 = open),
// IDs ascending and an entity's segments contiguous. vf / vt are the winning
// row's own ValidRange (the column batches report them); start / end are a
// relationship's endpoints (nil for nodes). tags[k][i] / bits[k][i] hold
// keys[k] of segment i (int64 bits, float64 bits, bool 0/1 or a dict index).
// Immutable once cached.
type stateCut struct {
	epoch      uint64
	keys       []string
	ids        []uint64
	from, to   []types.Instant
	vf, vt     []int64
	start, end []types.NodeID
	tags       [][]uint8
	bits       [][]uint64
	dict       []string
	dictIndex  map[string]uint64 // build only
}

func newStateCut(epoch uint64, keys []string) *stateCut {
	return &stateCut{
		epoch:     epoch,
		keys:      keys,
		tags:      make([][]uint8, len(keys)),
		bits:      make([][]uint64, len(keys)),
		dictIndex: map[string]uint64{},
	}
}

// add appends one segment whose winning row is row (with its endpoints for a
// relationship).
func (cut *stateCut) add(id uint64, from, to types.Instant, row interface {
	ValidRange() (types.Instant, types.Instant, bool)
	GetProperty(string) (any, bool)
}) {
	vf, vt, _ := row.ValidRange()
	cut.ids = append(cut.ids, id)
	cut.from = append(cut.from, from)
	cut.to = append(cut.to, to)
	cut.vf = append(cut.vf, int64(vf))
	cut.vt = append(cut.vt, int64(vt))
	for k, key := range cut.keys {
		tag, bits := cellAbsent, uint64(0)
		if raw, found := row.GetProperty(key); found {
			tag = cellOther
			if kind, i64, f64, s, b, ok := storepkg.ClassifyScalar(raw); ok {
				tag = cellScalar + uint8(kind) // #nosec G115 -- ColumnKind is a small enum
				switch kind {
				case storepkg.ColInt64:
					bits = uint64(i64) // #nosec G115 -- bit pattern, decoded as int64
				case storepkg.ColFloat64:
					bits = math.Float64bits(f64)
				case storepkg.ColString:
					bits = cut.intern(s)
				case storepkg.ColBool:
					if b {
						bits = 1
					}
				}
			}
		}
		cut.tags[k] = append(cut.tags[k], tag)
		cut.bits[k] = append(cut.bits[k], bits)
	}
}

func (cut *stateCut) intern(s string) uint64 {
	if i, ok := cut.dictIndex[s]; ok {
		return i
	}
	i := uint64(len(cut.dict))
	cut.dict = append(cut.dict, s)
	cut.dictIndex[s] = i
	return i
}

// seal drops the build-only state.
func (cut *stateCut) seal() *stateCut {
	cut.dictIndex = nil
	return cut
}

// bytes is the RAM the cut holds (the budget's unit).
func (cut *stateCut) bytes() int64 {
	n := int64(len(cut.ids))
	size := n * (8 + 16 + 16) // ids, from/to, vf/vt
	if cut.start != nil {
		size += n * 16
	}
	size += n * 9 * int64(len(cut.keys))
	for _, s := range cut.dict {
		size += int64(len(s)) + 16
	}
	return size
}

// hasAll reports whether the cut holds every key of keys.
func (cut *stateCut) hasAll(keys []string) bool {
	for _, k := range keys {
		if !slices.Contains(cut.keys, k) {
			return false
		}
	}
	return true
}

// each calls fn for every segment containing v whose ID is above after, at
// most limit of them (0 = no limit), in ID order: the rows ByLabel / ByType
// return at {ValidAt: v, TxAt: pin, After, Limit}.
func (cut *stateCut) each(v types.Instant, after types.EntityID, limit int, fn func(i int)) {
	n := 0
	floor := after.SnowflakeID()
	for i, id := range cut.ids {
		if cut.from[i] > v || (cut.to[i] != 0 && v >= cut.to[i]) || (floor > 0 && id <= uint64(floor)) {
			continue
		}
		fn(i)
		if n++; limit > 0 && n >= limit {
			return
		}
	}
}

// count is the number of rows each visits.
func (cut *stateCut) count(v types.Instant, after types.EntityID, limit int) int {
	n := 0
	cut.each(v, after, limit, func(int) { n++ })
	return n
}

// cutRows is a scan's view of a cut: the segments rows, the cut key of each
// requested property. It serves store.NodeColumnSource and
// RelColumnSource / RelColumnOtherSource.
type cutRows struct {
	cut  *stateCut
	rows []int
	col  []int
}

func (cut *stateCut) rowsAt(v types.Instant, opts storepkg.QueryOpts, props []string) cutRows {
	r := cutRows{cut: cut, col: make([]int, len(props))}
	for c, p := range props {
		r.col[c] = slices.Index(cut.keys, p)
	}
	cut.each(v, opts.After, opts.Limit, func(i int) { r.rows = append(r.rows, i) })
	return r
}

func (r cutRows) Len() int { return len(r.rows) }

func (r cutRows) Row(i int) (types.NodeID, int64, int64) {
	s := r.rows[i]
	return types.NodeID(r.cut.ids[s]), r.cut.vf[s], r.cut.vt[s] // #nosec G115 -- a stored snowflake ID
}

func (r cutRows) Value(i, c int) (storepkg.ColumnKind, int64, float64, string, bool, bool) {
	s, k := r.rows[i], r.col[c]
	tag, bits := r.cut.tags[k][s], r.cut.bits[k][s]
	if tag < cellScalar {
		return 0, 0, 0, "", false, false
	}
	kind := storepkg.ColumnKind(tag - cellScalar)
	switch kind {
	case storepkg.ColInt64:
		return kind, int64(bits), 0, "", false, true // #nosec G115 -- bit pattern stored by add
	case storepkg.ColFloat64:
		return kind, 0, math.Float64frombits(bits), "", false, true
	case storepkg.ColString:
		return kind, 0, 0, r.cut.dict[bits], false, true
	default:
		return kind, 0, 0, "", bits == 1, true
	}
}

func (r cutRows) Other(i, c int) bool {
	return r.cut.tags[r.col[c]][r.rows[i]] == cellOther
}

// relRows is cutRows as a store.RelColumnSource.
type relRows struct{ cutRows }

func (r relRows) Row(i int) (types.RelID, types.NodeID, types.NodeID, int64, int64) {
	s := r.rows[i]
	return types.RelID(r.cut.ids[s]), r.cut.start[s], r.cut.end[s], r.cut.vf[s], r.cut.vt[s] // #nosec G115 -- a stored snowflake ID
}

// stateCutQuery reports whether opts is the state form a cut answers — a
// TxAt with a ValidAt (or none: the fold's normalizeTxAtOnlyOpts reads it at
// readNow), no TxPin, no interval, every tier — and its valid instant and pin.
func (c *Core) stateCutQuery(opts storepkg.QueryOpts) (v, pin types.Instant, ok bool) {
	if opts.TxAt == 0 || opts.TxPin != 0 || opts.ValidStart != 0 || opts.ValidEnd != 0 || opts.Depth != storepkg.DepthAll {
		return 0, 0, false
	}
	v = opts.ValidAt
	if v == 0 {
		v = c.readNow()
	}
	return v, opts.TxAt, true
}

// pinLanded reports whether every write stamped at or below pin has landed,
// so a cut built now stays the state at pin (in-flight writes): under the
// write lock no standalone write (read lock), batch, ingest group or apply
// (write lock) is in flight; every later clock stamp is above lastInstant and
// an open transaction's writes are stamped after its start.
func (c *Core) pinLanded(pin types.Instant) bool {
	c.mu.Lock()
	landed := types.Instant(c.lastInstant.Load())
	if start := types.Instant(c.openTxStart.Load()); start != 0 && start < landed {
		landed = start
	}
	closed := c.closed.Load()
	c.mu.Unlock()
	return !closed && pin <= landed
}

// stateCutFor returns the cut of key holding keys: the cached one, else one
// built now (and cached when it fits the budget) once key was asked for
// before (admitCut). nil when the cut path does not answer — the first ask
// for key, a closed graph, the pin has writes in flight, the build failed (a per-entity
// compaction or retention stub below the pin, a closed graph) or a history
// rewrite raced the build — and the caller folds.
func (c *Core) stateCutFor(key stateCutKey, keys []string) *stateCut {
	if c.closed.Load() {
		return nil // the fold reports the closed graph
	}
	a := c.asOfColumns
	a.notePin(key.pin) // before the epoch is read (asOfColumnCache.notePin)
	epoch := a.currentEpoch()
	if cut := a.getCut(key, keys, epoch); cut != nil {
		return cut
	}
	if !a.admitCut(key) || !c.pinLanded(key.pin) {
		return nil
	}
	cut, err := c.buildStateCut(key, a.cutKeysFor(key, keys), epoch)
	if err != nil || !a.putCut(key, cut) {
		return nil
	}
	return cut
}

// buildStateCut reads the cut through the effective scans' per-entity
// timeline (the candidates ByLabel / ByType gather at the pin).
func (c *Core) buildStateCut(key stateCutKey, keys []string, epoch uint64) (*stateCut, error) {
	cut := newStateCut(epoch, keys)
	if key.rel {
		var ids []types.RelID
		if err := c.readUnderRLock(func() error {
			var err error
			ids, err = c.relTypeCandidateIDsAt(key.tok, storepkg.QueryOpts{TxPin: key.pin})
			return err
		}); err != nil {
			return nil, err
		}
		err := c.streamRelTimelines(ascending(ids), key.pin, func(r *types.Relationship) bool { return r.HasTypeTokenRaw(key.tok) },
			func(s temporal.RelSegment) bool {
				cut.add(uint64(s.Rel.ID()), s.ValidFrom, s.ValidTo, s.Rel) // #nosec G115 -- a snowflake ID
				cut.start = append(cut.start, s.Rel.StartNodeID())
				cut.end = append(cut.end, s.Rel.EndNodeID())
				return true
			})
		if err != nil {
			return nil, err
		}
		if cut.start == nil {
			cut.start, cut.end = []types.NodeID{}, []types.NodeID{}
		}
		return cut.seal(), nil
	}
	var ids []types.NodeID
	if err := c.readUnderRLock(func() error {
		var err error
		ids, err = c.nodeLabelCandidateIDsAt(key.tok, storepkg.QueryOpts{TxPin: key.pin})
		return err
	}); err != nil {
		return nil, err
	}
	err := c.streamNodeTimelines(ascending(ids), key.pin, func(n *types.Node) bool { return n.HasLabelTokenRaw(key.tok) },
		func(s temporal.NodeSegment) bool {
			cut.add(uint64(s.Node.ID()), s.ValidFrom, s.ValidTo, s.Node) // #nosec G115 -- a snowflake ID
			return true
		})
	if err != nil {
		return nil, err
	}
	return cut.seal(), nil
}

// countStateCut serves CountByLabelAt (rel=false) / CountByTypeAt from a cut:
// ok=false when opts is not the state form, the name is unknown (the fold
// counts 0) or no cut answers.
func (c *Core) countStateCut(rel bool, name string, opts storepkg.QueryOpts) (int, bool) {
	v, pin, ok := c.stateCutQuery(opts)
	if !ok {
		return 0, false
	}
	tok, known := c.stateCutToken(rel, name)
	if !known {
		return 0, false
	}
	cut := c.stateCutFor(stateCutKey{rel: rel, tok: tok, pin: pin}, nil)
	if cut == nil {
		return 0, false
	}
	return cut.count(v, opts.After, opts.Limit), true
}

func (c *Core) stateCutToken(rel bool, name string) (uint16, bool) {
	if rel {
		return c.lookupRelTypeQueryToken(name)
	}
	return c.labels.Lookup(name)
}

// scanNodeColumnsStateCut serves ScanNodeColumns at the state form from the
// label's cut; served=false leaves the call to the fold. fn runs without
// graph locks.
func (c *Core) scanNodeColumnsStateCut(label string, props []string, opts storepkg.QueryOpts,
	fn func(*storepkg.ColumnBatch) bool) (served bool, err error) {

	v, pin, ok := c.stateCutQuery(opts)
	if !ok {
		return false, nil
	}
	tok, known := c.labels.Lookup(label)
	if !known {
		return false, nil
	}
	cut := c.stateCutFor(stateCutKey{tok: tok, pin: pin}, props)
	if cut == nil {
		return false, nil
	}
	return true, storepkg.ScanColumnsFromNodeSource(cut.rowsAt(v, opts, props), len(props), fn)
}

// scanRelColumnsState serves ScanRelColumns at the state form (v, pin) for
// the given type names, type by type: from the type's cut, else ByType's
// rows (the fold, under the read lock). Each batch names its type; fn
// returning false stops every type.
func (c *Core) scanRelColumnsState(names []string, props []string, opts storepkg.QueryOpts, v, pin types.Instant,
	fn func(*storepkg.RelColumnBatch) bool) error {

	stopped := false
	for _, name := range names {
		tok, known := c.lookupRelTypeQueryToken(name)
		if name == "" || !known {
			continue
		}
		named := func(b *storepkg.RelColumnBatch) bool {
			b.RelType = name
			if !fn(b) {
				stopped = true
				return false
			}
			return true
		}
		var err error
		if cut := c.stateCutFor(stateCutKey{rel: true, tok: tok, pin: pin}, props); cut != nil {
			err = storepkg.ScanColumnsFromRelSource(relRows{cut.rowsAt(v, opts, props)}, len(props), named)
		} else {
			err = c.readUnderRLock(func() error {
				return c.scanRelColumnsTemporalLocked([]string{name}, props, opts, named)
			})
		}
		if err != nil || stopped {
			return err
		}
	}
	return nil
}

// stateCutSeen bounds the keys admitCut remembers.
const stateCutSeen = 64

// admitCut reports whether key was asked for before (among the last
// stateCutSeen keys) and remembers it: a cut is built on the second ask, so a
// pin read once (a fresh pin per statement) costs the fold, as without cuts,
// and no cut memory.
func (a *asOfColumnCache) admitCut(key stateCutKey) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if slices.Contains(a.cutSeen, key) {
		return true
	}
	if len(a.cutSeen) >= stateCutSeen {
		a.cutSeen = a.cutSeen[1:]
	}
	a.cutSeen = append(a.cutSeen, key)
	return false
}

// getCut returns the cut cached for key under epoch holding every key of
// keys (nil otherwise) and marks it most recently used.
func (a *asOfColumnCache) getCut(key stateCutKey, keys []string, epoch uint64) *stateCut {
	a.mu.Lock()
	defer a.mu.Unlock()
	cut := a.cuts[key]
	if cut == nil || cut.epoch != epoch || !cut.hasAll(keys) {
		return nil
	}
	a.dropCutOrderLocked(key)
	a.cutOrder = append(a.cutOrder, key)
	return cut
}

// cutKeysFor returns keys united with the keys a cut cached for key holds
// (whatever its epoch), so a rebuild serves both (the as-of sets' rule).
func (a *asOfColumnCache) cutKeysFor(key stateCutKey, keys []string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := slices.Clone(keys)
	if cut := a.cuts[key]; cut != nil {
		out = append(out, cut.keys...)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// putCut caches cut for key unless a history rewrite advanced the epoch since
// its build (false: the cut may be torn and must not answer). It drops the
// key's previous cut and every cut of an older epoch, then evicts least
// recently used cuts until the new one fits the budget; a cut over the
// budget is not kept (it still answers the call that built it).
func (a *asOfColumnCache) putCut(key stateCutKey, cut *stateCut) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.epoch.Load() != cut.epoch {
		return false
	}
	for k, old := range a.cuts {
		if k == key || old.epoch != cut.epoch {
			a.dropCutLocked(k)
		}
	}
	size := cut.bytes()
	if size > a.cutBudget {
		return true
	}
	a.evictCutsLocked(a.cutBudget - size)
	a.cuts[key] = cut
	a.cutOrder = append(a.cutOrder, key)
	a.cutBytes += size
	return true
}

// evictCutsLocked drops least recently used cuts until they hold at most
// limit bytes.
func (a *asOfColumnCache) evictCutsLocked(limit int64) {
	for a.cutBytes > limit && len(a.cutOrder) > 0 {
		a.dropCutLocked(a.cutOrder[0])
	}
}

func (a *asOfColumnCache) dropCutLocked(key stateCutKey) {
	if cut := a.cuts[key]; cut != nil {
		a.cutBytes -= cut.bytes()
		delete(a.cuts, key)
	}
	a.dropCutOrderLocked(key)
}

func (a *asOfColumnCache) dropCutOrderLocked(key stateCutKey) {
	if i := slices.Index(a.cutOrder, key); i >= 0 {
		a.cutOrder = slices.Delete(a.cutOrder, i, i+1)
	}
}

// stateCutBytes reports the bytes the cached cuts hold.
func (a *asOfColumnCache) stateCutBytes() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cutBytes
}
