package core

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Value classes of a property over every version recorded at a pin
// (sigma-tkgd round 10 request 3): PropertyTypeClassCounts per row (a
// version) instead of per current member. The rows are those the
// any-version equality door reads, ByLabelAndProperty(label, key, v,
// {ValidStart: types.UnboundedValidFrom, ValidEnd: MaxInt64, TxAt: pin}):
// every valid-time version recorded at pin carrying the label, deleted
// entities' rows before the delete included, retracted lives dropped
// (eachNodeChainAnyVersion, the same
// resolver, value_stats.go and range_any_version.go). Each row counts once,
// in the class of its key's value (types.PropertyTypeClass, the rule of
// the current-state counts), or in Missing without the key. A consumer
// proves from NaN + Other == 0 that the equality door finds every row a
// seek by any value can match. Candidates: the label's (type's) members at
// the pin (the count timeline's gather); no index needed. Memoised under
// the cut epoch (cut_memo.go).

// PropertyTypeClassCountsAt counts the value classes of key over the rows
// of label recorded at pin (see stats.API.PropertyTypeClassCountsAt).
func (s *StatOps) PropertyTypeClassCountsAt(label, key string, pin types.Instant) (storepkg.PropertyTypeClassCounts, bool, error) {
	return s.c.typeClassCountsAt(false, label, key, pin)
}

// RelPropertyTypeClassCountsAt is PropertyTypeClassCountsAt over a
// relationship type.
func (s *StatOps) RelPropertyTypeClassCountsAt(typeName, key string, pin types.Instant) (storepkg.PropertyTypeClassCounts, bool, error) {
	return s.c.typeClassCountsAt(true, typeName, key, pin)
}

func (c *Core) typeClassCountsAt(rel bool, name, key string, pin types.Instant) (storepkg.PropertyTypeClassCounts, bool, error) {
	var zero storepkg.PropertyTypeClassCounts
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
	if pin <= 0 {
		return zero, false, fmt.Errorf("%w: transaction-time pin %d must be positive (the current state: PropertyTypeClassCounts)", ErrInvalidTimeRange, pin)
	}
	scope := scopeNodes
	if rel {
		scope = c.relTypeRetentionScope(name)
	}
	if err := c.validateTemporalQueryOptsScan(anyVersionPinOpts(pin), scope); err != nil {
		return zero, false, err
	}
	tok, known := c.stateCutToken(rel, name)
	if !known {
		return zero, true, c.checkOpen()
	}
	ckey := valueStatsKey{cut: stateCutKey{rel: rel, tok: tok, pin: pin}, key: key}
	return memoAtPin(c, &c.asOfColumns.valueClass, rel, pin, ckey, func() (storepkg.PropertyTypeClassCounts, bool, error) {
		var counts storepkg.PropertyTypeClassCounts
		err := c.readUnderRLock(func() error {
			if rel {
				return c.relClassCountsLocked(tok, key, pin, &counts)
			}
			return c.nodeClassCountsLocked(tok, key, pin, &counts)
		})
		return counts, err == nil, err
	})
}

// classTally adds one row's class of key (Missing without the key).
type classTally struct {
	key    string
	counts *storepkg.PropertyTypeClassCounts
}

func (t classTally) add(row interface {
	ForEachPropertyTypeClass(func(string, types.PropertyTypeClass) bool)
}) {
	found := false
	row.ForEachPropertyTypeClass(func(k string, class types.PropertyTypeClass) bool {
		if k != t.key {
			return true
		}
		found = true
		switch class {
		case types.ClassNumeric:
			t.counts.Numeric++
		case types.ClassNaN:
			t.counts.NaN++
		case types.ClassString:
			t.counts.String++
		case types.ClassBool:
			t.counts.Bool++
		default:
			t.counts.Other++
		}
		return false
	})
	if !found {
		t.counts.Missing++
	}
}

// nodeClassCountsLocked tallies every row recorded at pin of label tok's
// members at the pin. Callers hold c.mu (R or W).
func (c *Core) nodeClassCountsLocked(tok uint16, key string, pin types.Instant, counts *storepkg.PropertyTypeClassCounts) error {
	ids, err := c.nodeLabelCandidateIDsAt(tok, storepkg.QueryOpts{TxPin: pin, TxPinSet: true})
	if err != nil {
		return err
	}
	tally := classTally{key: key, counts: counts}
	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	for _, id := range ascending(ids) {
		chain, err := c.nodeChainOf(id)
		if err != nil {
			return err
		}
		c.eachNodeChainAnyVersion(chain, pin, pred, func(n *types.Node) bool {
			tally.add(n)
			return true
		})
	}
	return nil
}

// relClassCountsLocked is nodeClassCountsLocked for a relationship type.
func (c *Core) relClassCountsLocked(tok uint16, key string, pin types.Instant, counts *storepkg.PropertyTypeClassCounts) error {
	ids, err := c.relTypeCandidateIDsAt(tok, storepkg.QueryOpts{TxPin: pin, TxPinSet: true})
	if err != nil {
		return err
	}
	tally := classTally{key: key, counts: counts}
	pred := func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	for _, id := range ascending(ids) {
		chain, err := c.relChainOf(id)
		if err != nil {
			return err
		}
		c.eachRelChainAnyVersion(chain, pin, pred, func(r *types.Relationship) bool {
			tally.add(r)
			return true
		})
	}
	return nil
}
