package core

import (
	"errors"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// DocValuesColumn states, without building anything, what ForEachDocValues /
// DocValuesSnapshot build for propertyKey on label's nodes now: a numeric or a
// string column, or none (mixed or unsupported values, an empty label or one
// over the column cap, a store without the column path). It is derived from
// the store's exact value-class counters (store.DocValuesColumnOf), so it
// costs a counter read, where finding out by building costs a pass over the
// label. It holds until the next node write (NodeMutationEpoch).
//
// ok=false means the store keeps no class counters (badger with
// DisablePlannerStats, stores without NodePropertyTypeClassCountsCapability):
// the caller finds out by building. An unknown label states DocValuesNone.
// Errors: a malformed label or key, ErrGraphClosed.
func (n *NodeOps) DocValuesColumn(label, propertyKey string) (storepkg.DocValuesColumn, bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return storepkg.DocValuesNone, false, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return storepkg.DocValuesNone, false, err
	}
	if err := storepkg.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storepkg.DocValuesNone, false, err
	}
	if _, columns := c.store.(nodeDocValuesScanner); !columns {
		return storepkg.DocValuesNone, true, nil
	}
	counter, ok := c.store.(storepkg.NodePropertyTypeClassCountsCapability)
	if !ok {
		return storepkg.DocValuesNone, false, nil
	}
	var (
		kind  storepkg.DocValuesColumn
		known = true
	)
	err := c.readUnderRLock(func() error {
		tok, found := c.labels.Lookup(label)
		if !found {
			return nil
		}
		labelCount, err := c.nodeCountByLabel(tok)
		if err != nil {
			return err
		}
		counts, err := counter.NodePropertyTypeClassCounts(tok, propertyKey)
		if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
			known = false
			return nil
		}
		if err != nil {
			return err
		}
		kind = storepkg.DocValuesColumnOf(counts, labelCount, indexpkg.MaxDocValuesNodes)
		return nil
	})
	if err != nil {
		return storepkg.DocValuesNone, false, err
	}
	return kind, known, nil
}
