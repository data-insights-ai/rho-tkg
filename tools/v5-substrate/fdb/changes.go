package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Complete groups retain the frozen request and every post/tombstone revision.
// Both prefixes are consulted even when the requested scope excludes a coordinator.
type changedValue struct {
	Group  uint8
	Key    string
	Before *value `json:",omitempty"`
	After  value
}
type changeGroup struct {
	Tx      transaction
	Outcome outcome
	Writes  []changedValue
}

func (h *harness) changeKey(group uint8, round uint64, request string) fdb.Key {
	return fdb.Key(fmt.Sprintf("%schanges/%016x/%s", h.prefix(group), round, hex.EncodeToString([]byte(request))))
}
func (h *harness) validateChange(group changeGroup) error {
	x, o := group.Tx, group.Outcome
	if err := h.validate(x); err != nil {
		return err
	}
	b, err := wire(x)
	if err != nil {
		return err
	}
	if !o.Commit || o.Round == 0 || o.Sequence == 0 || o.Sequence > h.config.Limits.Transactions || o.Round <= x.Dependency || o.Digest != sha256.Sum256(b) || o.ID != x.ID || o.Request != x.Request || o.Coordinator != x.Coordinator || o.Reason != "" {
		return errInvalid
	}
	n := 0
	for _, p := range x.Participants {
		for _, effect := range p.Effects {
			if n >= len(group.Writes) {
				return errInvalid
			}
			row := group.Writes[n]
			n++
			if row.Group != p.Group || row.Key != effect.Key || row.After.Value != effect.Value || row.After.Deleted != effect.Delete || row.After.Round != o.Round || row.After.TxID != x.ID {
				return errInvalid
			}
			prior := uint64(0)
			if row.Before != nil {
				if row.Before.Version == 0 || row.Before.Round == 0 || row.Before.TxID == "" {
					return errInvalid
				}
				prior = row.Before.Version
			}
			if prior == ^uint64(0) || row.After.Version != prior+1 {
				return errInvalid
			}
		}
	}
	if n != len(group.Writes) {
		return errInvalid
	}
	return nil
}
func touchesScope(x transaction, scope []uint8) bool {
	for _, p := range x.Participants {
		if slices.Contains(scope, p.Group) {
			return true
		}
	}
	return false
}

// changes is the bounded round range (from,to]. An entire immutable group
// is the pagination unit. Quotas retain all groups; no expiry/GC claim is made.
func (h *harness) changes(from, to cut, pageSize int) ([]changeGroup, error) {
	if h == nil || pageSize < 1 || pageSize > 128 || !validScope(from.record.Scope) || from.record.Graph != to.record.Graph || from.record.Topology != to.record.Topology || from.record.Epochs != to.record.Epochs || !slices.Equal(from.record.Scope, to.record.Scope) || from.record.Round > to.record.Round {
		return nil, errInvalid
	}
	var groups []changeGroup
	seen := map[string]bool{}
	for coordinator := uint8(0); coordinator < 2; coordinator++ {
		bound := func(round uint64) fdb.Key {
			return fdb.Key(fmt.Sprintf("%schanges/%016x0", h.prefix(coordinator), round))
		}
		lo, hi := from.record.Groups[coordinator], to.record.Groups[coordinator]
		if lo > hi || hi > h.config.Limits.Transactions {
			return nil, errInvalid
		}
		begin, end := bound(lo), bound(hi)
		count := uint64(0)
		for {
			result, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
				if err := h.verifyCut(tr, from); err != nil {
					return nil, err
				}
				if err := h.verifyCut(tr, to); err != nil {
					return nil, err
				}
				if err := h.checkConfig(tr, coordinator); err != nil {
					return nil, err
				}
				return tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: pageSize}).GetSliceWithError()
			})
			if err != nil {
				return nil, err
			}
			rows := result.([]fdb.KeyValue)
			for _, row := range rows {
				count++
				if count > h.config.Limits.Transactions {
					return nil, errLimit
				}
				group, err := decode[changeGroup](row.Value)
				if err != nil {
					return nil, err
				}
				if err = h.validateChange(group); err != nil {
					return nil, err
				}
				o := group.Outcome
				if o.Coordinator != coordinator || o.Sequence != lo+count || o.Sequence > hi || !bytes.Equal(row.Key, h.changeKey(coordinator, o.Sequence, o.Request)) {
					return nil, errInvalid
				}
				name := fmt.Sprintf("%d/%s", coordinator, o.Request)
				if seen[name] {
					return nil, errInvalid
				}
				seen[name] = true
				if touchesScope(group.Tx, from.record.Scope) {
					if o.Round <= from.record.Round || o.Round > to.record.Round {
						return nil, errInvalid
					}
					groups = append(groups, group)
				}
			}
			if len(rows) < pageSize {
				break
			}
			begin = append(bytes.Clone(rows[len(rows)-1].Key), 0)
		}
		if count != hi-lo {
			return nil, errUnavailable
		}
	}
	slices.SortFunc(groups, func(a, b changeGroup) int {
		if a.Outcome.Round < b.Outcome.Round {
			return -1
		}
		if a.Outcome.Round > b.Outcome.Round {
			return 1
		}
		if a.Outcome.Coordinator < b.Outcome.Coordinator {
			return -1
		}
		if a.Outcome.Coordinator > b.Outcome.Coordinator {
			return 1
		}
		return strings.Compare(a.Outcome.Request, b.Outcome.Request)
	})
	return groups, nil
}
