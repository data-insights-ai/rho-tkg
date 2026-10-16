package core

import (
	"errors"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Value statistics of an indexed property (sigma-tkgd round 9 R9-3): how
// many rows a seek by one value reads without knowing the value, the mean
// (Rows / Distinct) and the worst case (MaxRows), exact.
//
// Current state: the property index's own statistics (index.ValueStats:
// one pass over its value keys at the first call after a mutation, cached
// in the index until the next), the heaviest value read from one entity
// holding it. At a pin: the rows ByLabelAndProperty(label, key, v,
// {ValidStart 1, ValidEnd MaxInt64, TxAt pin}) matches for each value v,
// counted per row (a version), from the membership sidecar's members of
// every value pruned by the pin, each entity's rows recorded at the pin
// tested (eachNodeChainInterval); cached per (kind, token, key, pin) under
// the state cuts' invalidation (cutEpoch, pinLanded) as the count
// timelines are. No label walk on either path.

// nodeValueStatser / relValueStatser are the OPTIONAL store capabilities
// behind the current-state statistics (memory, badger).
type nodeValueStatser interface {
	NodePropertyValueStats(labelToken uint16, propertyKey string) (indexpkg.ValueStats, bool, error)
}

type relValueStatser interface {
	RelPropertyValueStats(relTypeToken uint16, propertyKey string) (indexpkg.ValueStats, bool, error)
}

// valueStatsKey names cached statistics at a pin.
type valueStatsKey struct {
	cut stateCutKey
	key string
}

type valueStatsEntry struct {
	epoch uint64
	stats storepkg.PropertyValueStats
}

// valueStatsAttempts bounds the retries of a read that writers race.
const valueStatsAttempts = 3

func (a *asOfColumnCache) getValueStats(key valueStatsKey, epoch uint64) (storepkg.PropertyValueStats, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e, ok := a.valueStats[key]
	if !ok || e.epoch != epoch {
		return storepkg.PropertyValueStats{}, false
	}
	return e.stats, true
}

func (a *asOfColumnCache) putValueStats(key valueStatsKey, epoch uint64, stats storepkg.PropertyValueStats) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.valueStats == nil {
		a.valueStats = make(map[valueStatsKey]valueStatsEntry)
	}
	if len(a.valueStats) >= countTimelineCap {
		for k, e := range a.valueStats {
			if e.epoch != epoch || len(a.valueStats) >= countTimelineCap {
				delete(a.valueStats, k)
			}
		}
	}
	a.valueStats[key] = valueStatsEntry{epoch: epoch, stats: stats}
}

// valueTally folds rows into value statistics by index key.
type valueTally struct {
	rows   map[string]int64
	values map[string]any
	total  int64
}

func (t *valueTally) add(v any) {
	vk := types.IndexablePropertyValueKey(v)
	if vk == "" {
		return
	}
	if t.rows == nil {
		t.rows, t.values = map[string]int64{}, map[string]any{}
	}
	if _, seen := t.values[vk]; !seen {
		t.values[vk] = v
	}
	t.rows[vk]++
	t.total++
}

func (t *valueTally) stats() storepkg.PropertyValueStats {
	out := storepkg.PropertyValueStats{Distinct: int64(len(t.rows)), Rows: t.total}
	var maxKey string
	for vk, n := range t.rows {
		if n > out.MaxRows || (n == out.MaxRows && vk < maxKey) {
			out.MaxRows, maxKey = n, vk
		}
	}
	if out.MaxRows > 0 {
		out.MaxValue = t.values[maxKey]
	}
	return out
}

// PropertyValueStats states the values of key over label's members from
// the property index (see stats.API.PropertyValueStats).
func (s *StatOps) PropertyValueStats(label, key string, pin types.Instant) (storepkg.PropertyValueStats, bool, error) {
	return s.c.propertyValueStats(false, label, key, pin)
}

// RelPropertyValueStats is PropertyValueStats over a relationship type.
func (s *StatOps) RelPropertyValueStats(typeName, key string, pin types.Instant) (storepkg.PropertyValueStats, bool, error) {
	return s.c.propertyValueStats(true, typeName, key, pin)
}

func (c *Core) propertyValueStats(rel bool, name, key string, pin types.Instant) (storepkg.PropertyValueStats, bool, error) {
	var zero storepkg.PropertyValueStats
	if err := c.checkOpen(); err != nil {
		return zero, false, err
	}
	var err error
	if rel {
		err = c.validateRelTypeQueryName(name)
	} else {
		err = c.validateIndexLabel(name)
	}
	if err == nil {
		err = storepkg.ValidateIndexPropertyKey(key)
	}
	if err != nil {
		return zero, false, err
	}
	if pin < 0 {
		return zero, true, nil
	}
	if !c.storeRowsTrust {
		return zero, false, nil
	}
	tok, known := c.stateCutToken(rel, name)
	if !known {
		return zero, false, c.checkOpen()
	}
	if pin == 0 {
		return c.currentValueStats(rel, tok, key)
	}
	scope := scopeNodes
	if rel {
		scope = c.relTypeRetentionScope(name)
	}
	if err := c.validateTemporalQueryOptsScan(anyVersionPinOpts(pin), scope); err != nil {
		return zero, false, err
	}
	return c.pinnedValueStats(rel, tok, key, pin)
}

// currentValueStats reads the index's statistics and the heaviest value
// from its sample entity, retried when a write moved the sample's value.
func (c *Core) currentValueStats(rel bool, tok uint16, key string) (storepkg.PropertyValueStats, bool, error) {
	var zero storepkg.PropertyValueStats
	for range valueStatsAttempts {
		var s indexpkg.ValueStats
		var exact bool
		var err error
		if rel {
			st, ok := c.store.(relValueStatser)
			if !ok {
				return zero, false, nil
			}
			s, exact, err = st.RelPropertyValueStats(tok, key)
		} else {
			st, ok := c.store.(nodeValueStatser)
			if !ok {
				return zero, false, nil
			}
			s, exact, err = st.NodePropertyValueStats(tok, key)
		}
		if err != nil || !exact {
			return zero, false, err
		}
		out := storepkg.PropertyValueStats{Distinct: s.Distinct, Rows: s.Rows, MaxRows: s.MaxRows}
		if s.MaxRows == 0 {
			return out, true, nil
		}
		var value any
		var found bool
		err = c.readUnderRLock(func() error {
			var row interface{ GetProperty(string) (any, bool) }
			if rel {
				r, err := c.getCurrentRelationship(types.RelID(s.Sample))
				if err != nil {
					return err
				}
				row = r
			} else {
				n, err := c.getCurrentNode(types.NodeID(s.Sample))
				if err != nil {
					return err
				}
				row = n
			}
			value, found = row.GetProperty(key)
			return nil
		})
		if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) && !errors.Is(err, storepkg.ErrRelNotFound) {
			return zero, false, err
		}
		if err == nil && found && types.IndexablePropertyValueKey(value) == s.MaxKey {
			out.MaxValue = value
			return out, true, nil
		}
	}
	return zero, false, nil
}

// pinnedValueStats counts the rows recorded at pin per value from the
// sidecar's members of every value; cached under the cut epoch.
func (c *Core) pinnedValueStats(rel bool, tok uint16, key string, pin types.Instant) (storepkg.PropertyValueStats, bool, error) {
	var zero storepkg.PropertyValueStats
	a := c.asOfColumns
	a.notePin(pin)
	ckey := valueStatsKey{cut: stateCutKey{rel: rel, tok: tok, pin: pin}, key: key}
	for range valueStatsAttempts {
		epoch := a.cutEpoch(rel)
		if s, ok := a.getValueStats(ckey, epoch); ok {
			return s, true, nil
		}
		landed := c.pinLanded(pin)
		var tally valueTally
		served := false
		err := c.readUnderRLock(func() error {
			var err error
			if rel {
				served, err = c.relValueTallyLocked(tok, key, pin, &tally)
			} else {
				served, err = c.nodeValueTallyLocked(tok, key, pin, &tally)
			}
			return err
		})
		if errors.Is(err, storepkg.ErrIndexNotFound) {
			return zero, false, nil
		}
		if err != nil || !served {
			return zero, false, err
		}
		if a.cutEpoch(rel) != epoch {
			continue
		}
		out := tally.stats()
		if landed {
			a.putValueStats(ckey, epoch, out)
		}
		return out, true, nil
	}
	return zero, false, nil
}

// nodeValueTallyLocked tallies every row recorded at pin of the sidecar's
// members of (tok, key) that carries the label and an indexed value.
// Callers hold c.mu (R or W).
func (c *Core) nodeValueTallyLocked(tok uint16, key string, pin types.Instant, tally *valueTally) (bool, error) {
	walker, ok := c.store.(storepkg.NodePropertyTxMemberAnyCapability)
	if !ok || !c.membershipSidecarUsable(storepkg.DepthAll) {
		return false, nil
	}
	if elements, err := c.nodeIndexElements(tok, key); err != nil || elements {
		return false, err
	}
	var ids []types.NodeID
	if err := walker.ForEachNodePropertyTxMemberAny(tok, key, func(id types.NodeID, firstTx types.Instant) bool {
		if firstTx == 0 || firstTx <= pin {
			ids = append(ids, id)
		}
		return true
	}); err != nil {
		return true, err
	}
	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	for _, id := range ascending(ids) {
		chain, err := c.nodeChainOf(id)
		if err != nil {
			return true, err
		}
		c.eachNodeChainInterval(chain, anyValidStart, anyValidEnd, pin, pred, func(n *types.Node) bool {
			if v, ok := n.GetProperty(key); ok {
				tally.add(v)
			}
			return true
		})
	}
	return true, nil
}

// relValueTallyLocked is nodeValueTallyLocked for a relationship type.
func (c *Core) relValueTallyLocked(tok uint16, key string, pin types.Instant, tally *valueTally) (bool, error) {
	walker, ok := c.store.(storepkg.RelPropertyTxMemberAnyCapability)
	if !ok || !c.membershipSidecarUsable(storepkg.DepthAll) {
		return false, nil
	}
	if elements, err := c.relIndexElements(tok, key); err != nil || elements {
		return false, err
	}
	var ids []types.RelID
	if err := walker.ForEachRelPropertyTxMemberAny(tok, key, func(id types.RelID, firstTx types.Instant) bool {
		if firstTx == 0 || firstTx <= pin {
			ids = append(ids, id)
		}
		return true
	}); err != nil {
		return true, err
	}
	pred := func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	for _, id := range ascending(ids) {
		chain, err := c.relChainOf(id)
		if err != nil {
			return true, err
		}
		c.eachRelChainInterval(chain, anyValidStart, anyValidEnd, pin, pred, func(r *types.Relationship) bool {
			if v, ok := r.GetProperty(key); ok {
				tally.add(v)
			}
			return true
		})
	}
	return true, nil
}
