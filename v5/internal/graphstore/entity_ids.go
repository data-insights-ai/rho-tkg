package graphstore

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// EntityKindID is one stored entity record's identity: its ID and kind.
type EntityKindID struct {
	ID   graphstate.EntityID
	Kind graphstate.EntityKind
}

// EntityIDs lists, ascending, up to limit stored entity records of this
// partition with an ID greater than after (zero starts at the beginning);
// complete is false when more follow. It pages the entity record rows
// themselves, so the cost follows the entities stored, never the ID range.
// It reads this partition only (v5.0 has one); a multi-partition engine must
// merge per-partition pages.
func (v *ReadView) EntityIDs(ctx context.Context, after graphstate.EntityID, limit int) (ids []EntityKindID, complete bool, err error) {
	if v == nil || limit < 1 {
		return nil, false, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if err := v.check(ctx); err != nil {
		return nil, false, err
	}
	q, err := v.begin(ctx, raftlog.ReadBudget{})
	if err != nil {
		return nil, false, err
	}
	n := v.c.root.namespace
	lower := keyPrefix(n, entityRecord)
	upper := prefixEnd(lower)
	var cursor []byte
	if after > 0 {
		cursor = entityKey(n, after)
	}
	complete = true
	err = func() error {
		for {
			rows := min(limit-len(ids)+1, q.q.maxRows-q.q.rows, v.c.source().ReadLimits().Rows)
			bytesLeft := min(q.q.maxBytes-q.q.bytes, v.c.source().ReadLimits().Bytes)
			if rows < 1 || bytesLeft < 1 {
				return ErrResourceLimit
			}
			page, err := v.c.source().Scan(ctx, lower, upper, cursor, raftlog.ReadBudget{Rows: rows, Bytes: bytesLeft})
			if err != nil {
				if errors.Is(err, raftlog.ErrLimit) {
					return ErrResourceLimit
				}
				return err
			}
			q.q.rows += page.Visited
			if err := q.q.materialize(page.Bytes); err != nil {
				return err
			}
			for _, row := range page.Rows {
				if len(row.Key) != len(lower)+8 || row.Deleted {
					return ErrCorrupt
				}
				if len(ids) == limit {
					complete = false
					return nil
				}
				id := graphstate.EntityID(binary.BigEndian.Uint64(row.Key[len(lower):]))
				rec, err := readEntity(row.Value, n, v.c.limits)
				if err != nil {
					return err
				}
				if rec.ID != id || id < 1 {
					return ErrCorrupt
				}
				ids = append(ids, EntityKindID{ID: id, Kind: rec.Kind})
			}
			if page.Complete {
				return nil
			}
			if len(page.Next) == 0 {
				return ErrCorrupt
			}
			cursor = page.Next
		}
	}()
	if err = v.finish(q, err); err != nil {
		return nil, false, err
	}
	if err := v.output(64 + 16*len(ids)); err != nil {
		return nil, false, err
	}
	return ids, complete, nil
}
