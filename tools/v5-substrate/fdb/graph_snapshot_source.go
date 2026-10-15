package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Scan emits canonical logical rows once in (group, logical-key) order. Current
// is only the retained key directory: its values never substitute for At data.
// Each emitted row carries actual native current/history/original bytes. A
// prefix is provisional until the independent original census produces EOF.
type graphSnapshotRow struct {
	Group            uint8
	Key              string
	Value            value
	Current, History graphCostStoredRecord
	Chain            []graphCostRawRow
}
type graphSnapshotEnd struct {
	Cut                                  certificate
	Rows, DirectoryRows, OriginalEffects [2]uint64
	Work                                 graphSnapshotWork
}
type graphSnapshotKey struct {
	Group uint8
	Key   string
}
type graphSnapshotPoint struct {
	Key   graphSnapshotKey
	Found bool
	Value value
}

// The work debit counts successful-page logical data reads, including duplicate
// current/history/CDC/ID/request accesses. Configuration/owner/control checks and
// native retry reads are excluded, as in the existing history policy; callbacks
// count actual SDK callback invocations separately. Neither counter is RSS.
func (s *graphSnapshot) debit(rows, bytes uint64, callbacks int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if callbacks < 0 || uint64(callbacks) > math.MaxUint64-s.work.Callbacks {
		return errLimit
	}
	s.work.Callbacks += uint64(callbacks)
	if rows > s.policy.WorkRows-s.work.Rows || bytes > s.policy.WorkBytes-s.work.Bytes {
		return errLimit
	}
	s.work.Rows += rows
	s.work.Bytes += bytes
	return nil
}

type snapshotDebit struct{ rows, bytes uint64 }

func (d *snapshotDebit) add(key, raw []byte, maxBytes int) error {
	n := uint64(len(key) + len(raw))
	if n > uint64(maxBytes)-d.bytes {
		return errLimit
	}
	d.rows++
	d.bytes += n
	return nil
}
func (s *graphSnapshot) originalIn(tr fdb.ReadTransaction, group uint8, key string, v value, debit *snapshotDebit) (changeGroup, graphCostRawRow, error) {
	h := s.owner.s.h
	original, used, err := h.historyOriginalIn(tr, group, key, v)
	if used > uint64(s.policy.PageBytes)-debit.bytes {
		return changeGroup{}, graphCostRawRow{}, errLimit
	}
	debit.bytes += used
	// historyOriginalIn visits both coordinator IDs and then request+CDC for
	// the unique authority. Count the actual logical accesses, even absent IDs.
	debit.rows += 4
	if err != nil {
		return changeGroup{}, graphCostRawRow{}, err
	}
	o := original.Outcome
	raw := graphCostRawRow{Group: o.Coordinator, Key: bytes.Clone(h.changeKey(o.Coordinator, o.Sequence, o.Request)), IDKey: bytes.Clone(h.key(o.Coordinator, "id", o.ID)), RequestKey: bytes.Clone(h.key(o.Coordinator, "request", o.Request))}
	for _, pair := range []struct {
		key   []byte
		value *[]byte
	}{{raw.Key, &raw.Value}, {raw.IDKey, &raw.IDValue}, {raw.RequestKey, &raw.RequestValue}} {
		body, err := tr.Get(fdb.Key(pair.key)).Get()
		if err != nil {
			return changeGroup{}, raw, err
		}
		if err := debit.add(pair.key, body, s.policy.PageBytes); err != nil {
			return changeGroup{}, raw, err
		}
		if len(body) == 0 || len(body) > 64<<10 {
			return changeGroup{}, raw, errUnavailable
		}
		*pair.value = bytes.Clone(body)
	}
	canonical, err := wire(original)
	ob, bodyErr := wire(o)
	if err != nil || bodyErr != nil || !bytes.Equal(raw.Value, canonical) || !bytes.Equal(raw.IDValue, ob) || !bytes.Equal(raw.RequestValue, ob) {
		return changeGroup{}, raw, errors.Join(errMismatch, err, bodyErr)
	}
	return original, raw, nil
}

// revisionIn proves the latest revision at the exact cut.
// A genuine Before=nil after the cut proves absence; a missing stored head does
// not. Every step is admitted and authenticated before it supplies authority.
func (s *graphSnapshot) revisionIn(tr fdb.ReadTransaction, group uint8, key string, debit *snapshotDebit) (graphSnapshotRow, bool, error) {
	h := s.owner.s.h
	row := graphSnapshotRow{Group: group, Key: key}
	headKey := h.key(group, "current", key)
	head, err := tr.Get(headKey).Get()
	if err != nil {
		return row, false, err
	}
	if err := debit.add(headKey, head, s.policy.PageBytes); err != nil {
		return row, false, err
	}
	if len(head) == 0 || len(head) > 64<<10 {
		return row, false, errUnavailable
	}
	v, err := decode[value](head)
	if err != nil || !validGraphValue(v) {
		return row, false, errors.Join(errMismatch, err)
	}
	row.Current = graphCostStoredRecord{bytes.Clone(headKey), bytes.Clone(head)}
	for step := 0; step < s.policy.ChainSteps; step++ {
		original, raw, err := s.originalIn(tr, group, key, v, debit)
		if err != nil {
			return row, false, err
		}
		row.Chain = append(row.Chain, raw)
		if v.Round <= s.owner.id.Cut.Round {
			if original.Outcome.Sequence > s.owner.id.Cut.Groups[original.Outcome.Coordinator] {
				return row, false, errMismatch
			}
			physicalKey := fdb.Key(fmt.Sprintf("%shistory/%016x/%x", h.prefix(group), v.Round, key))
			physical, err := tr.Get(physicalKey).Get()
			if err != nil {
				return row, false, err
			}
			if err := debit.add(physicalKey, physical, s.policy.PageBytes); err != nil {
				return row, false, err
			}
			canonical, err := wire(v)
			if err != nil {
				return row, false, err
			}
			if !bytes.Equal(physical, canonical) {
				return row, false, errUnavailable
			}
			row.Value, row.History = v, graphCostStoredRecord{bytes.Clone(physicalKey), bytes.Clone(physical)}
			return row, !v.Deleted, nil
		}
		var before *value
		for _, write := range original.Writes {
			if write.Group == group && write.Key == key && write.After == v {
				before = write.Before
				break
			}
		}
		if before == nil {
			return row, false, nil // Native original proves creation strictly after cut.
		}
		if before.Version == math.MaxUint64 || before.Version+1 != v.Version || before.Round >= v.Round {
			return row, false, errMismatch
		}
		v = *before
	}
	return row, false, errLimit
}

// Every historical effect needs an extant directory head with genuine original
// authority. This fixed-cost census does NOT traverse H ancestors separately for
// each of H revisions. Selected physical anchors are checked once per directory
// key by revisionIn; older physical-history completeness is a separate contract.
func (s *graphSnapshot) censusCurrentIn(tr fdb.ReadTransaction, write changedValue, debit *snapshotDebit) error {
	key := s.owner.s.h.key(write.Group, "current", write.Key)
	raw, err := tr.Get(key).Get()
	if err != nil {
		return err
	}
	if err := debit.add(key, raw, s.policy.PageBytes); err != nil {
		return err
	}
	if len(raw) == 0 || len(raw) > 64<<10 {
		return errUnavailable
	}
	head, err := decode[value](raw)
	if err != nil || !validGraphValue(head) || head.Version < write.After.Version || head.Round < write.After.Round || head.Version == write.After.Version && head != write.After || head.Version > write.After.Version && head.Round <= write.After.Round {
		return errors.Join(errMismatch, err)
	}
	_, _, err = s.originalIn(tr, write.Group, write.Key, head, debit)
	return err
}

func (s *graphSnapshot) Scan(ctx context.Context, emit func(graphSnapshotRow) error) (graphSnapshotEnd, error) {
	var end graphSnapshotEnd
	if s == nil || s.owner == nil || s.lifetime == nil || ctx == nil || emit == nil {
		return end, errInvalid
	}
	s.lifetime.mu.Lock()
	if s.lifetime.closed {
		s.lifetime.mu.Unlock()
		return end, errHistoryExpired
	}
	if s.lifetime.closing {
		s.lifetime.mu.Unlock()
		return end, errHistoryInUse
	}
	if s.lifetime.busy {
		s.lifetime.mu.Unlock()
		return end, errHistoryInUse
	}
	s.lifetime.busy = true
	s.lifetime.mu.Unlock()
	defer func() { s.lifetime.mu.Lock(); s.lifetime.busy = false; s.lifetime.mu.Unlock() }()
	end.Cut = s.owner.id.Cut
	end.Cut.Scope = slices.Clone(end.Cut.Scope)
	h := s.owner.s.h
	for _, g := range end.Cut.Scope {
		bound := prefixRange(h.prefix(g) + "current/")
		begin := bound.Begin
		for {
			type page struct {
				Rows      []graphSnapshotRow
				Directory int
				Last      []byte
				Debit     snapshotDebit
			}
			p, callbacks, err := graphCostParallelRead(ctx, h.db, func(tr fdb.ReadTransaction) (page, error) {
				var p page
				if err := s.owner.check(tr); err != nil {
					return p, err
				}
				rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: bound.End}, fdb.RangeOptions{Limit: s.policy.PageRows}).GetSliceWithError()
				if err != nil {
					return p, err
				}
				p.Directory = len(rows)
				for _, raw := range rows {
					if len(raw.Key) > 1024 || len(raw.Value) > 64<<10 {
						return p, errLimit
					}
					if err := p.Debit.add(raw.Key, raw.Value, s.policy.PageBytes); err != nil {
						return p, err
					}
					tail, found := strings.CutPrefix(string(raw.Key), h.prefix(g)+"current/")
					logical, err := hex.DecodeString(tail)
					if !found || err != nil || !validName(string(logical)) || hex.EncodeToString(logical) != tail {
						return p, errMismatch
					}
					row, live, err := s.revisionIn(tr, g, string(logical), &p.Debit)
					if err != nil {
						return p, err
					}
					if !bytes.Equal(raw.Value, row.Current.Value) {
						return p, errMismatch
					}
					if live {
						p.Rows = append(p.Rows, row)
					}
					p.Last = bytes.Clone(raw.Key)
				}
				return p, nil
			})
			if debitErr := s.debit(p.Debit.rows, p.Debit.bytes, callbacks); debitErr != nil {
				return graphSnapshotEnd{}, errors.Join(err, debitErr)
			}
			if err != nil {
				return graphSnapshotEnd{}, err
			}
			for _, row := range p.Rows {
				if err := ctx.Err(); err != nil {
					return graphSnapshotEnd{}, err
				}
				if err := emit(row); err != nil {
					return graphSnapshotEnd{}, err
				}
				end.Rows[g]++
			}
			end.DirectoryRows[g] += uint64(p.Directory)
			if p.Directory < s.policy.PageRows {
				break
			}
			begin = fdb.Key(append(p.Last, 0))
		}
	}
	// Native unique directory keys alone do not prove completeness. Independently
	// require EVERY original effect's actual authenticated current head. Selected
	// physical anchors were checked once per directory key. Counts cannot hide
	// omission, and this independent census is linear in the original effects.
	for coordinator := uint8(0); coordinator < 2; coordinator++ {
		sequence := uint64(0)
		for {
			type census struct {
				Count    int
				Sequence uint64
				Effects  [2]uint64
				Debit    snapshotDebit
			}
			p, callbacks, err := graphCostParallelRead(ctx, h.db, func(tr fdb.ReadTransaction) (census, error) {
				p := census{Sequence: sequence}
				if err := s.owner.check(tr); err != nil {
					return p, err
				}
				begin := fdb.Key(fmt.Sprintf("%schanges/%016x0", h.prefix(coordinator), sequence))
				limit := s.owner.id.Cut.Groups[coordinator]
				bound := fdb.Key(fmt.Sprintf("%schanges/%016x0", h.prefix(coordinator), limit))
				rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: bound}, fdb.RangeOptions{Limit: s.policy.PageRows}).GetSliceWithError()
				if err != nil {
					return p, err
				}
				p.Count = len(rows)
				for _, raw := range rows {
					if len(raw.Key) > 1024 || len(raw.Value) == 0 || len(raw.Value) > 64<<10 {
						return p, errLimit
					}
					if err := p.Debit.add(raw.Key, raw.Value, s.policy.PageBytes); err != nil {
						return p, err
					}
					group, err := decode[changeGroup](raw.Value)
					if err != nil {
						return p, err
					}
					if err := h.validateChange(group); err != nil {
						return p, err
					}
					o := group.Outcome
					if o.Coordinator != coordinator || p.Sequence == math.MaxUint64 || o.Sequence != p.Sequence+1 || o.Sequence > limit || !bytes.Equal(raw.Key, h.changeKey(coordinator, o.Sequence, o.Request)) || touchesScope(group.Tx, end.Cut.Scope) && o.Round > end.Cut.Round {
						return p, errMismatch
					}
					for _, write := range group.Writes {
						if !slices.Contains(end.Cut.Scope, write.Group) {
							continue
						}
						if err := s.censusCurrentIn(tr, write, &p.Debit); err != nil {
							return p, err
						}
						p.Effects[write.Group]++
					}
					// Even groups outside data scope authenticate both actual originals.
					ob, err := wire(o)
					if err != nil {
						return p, err
					}
					for _, key := range []fdb.Key{h.key(coordinator, "id", o.ID), h.key(coordinator, "request", o.Request)} {
						body, err := tr.Get(key).Get()
						if err != nil {
							return p, err
						}
						if err := p.Debit.add(key, body, s.policy.PageBytes); err != nil {
							return p, err
						}
						if !bytes.Equal(body, ob) {
							return p, errMismatch
						}
					}
					p.Sequence = o.Sequence
				}
				return p, nil
			})
			if debitErr := s.debit(p.Debit.rows, p.Debit.bytes, callbacks); debitErr != nil {
				return graphSnapshotEnd{}, errors.Join(err, debitErr)
			}
			if err != nil {
				return graphSnapshotEnd{}, err
			}
			sequence = p.Sequence
			for g := range 2 {
				end.OriginalEffects[g] += p.Effects[g]
			}
			if p.Count < s.policy.PageRows {
				if sequence != end.Cut.Groups[coordinator] {
					return graphSnapshotEnd{}, errUnavailable
				}
				break
			}
		}
	}
	_, callbacks, err := graphCostParallelRead(ctx, h.db, func(tr fdb.ReadTransaction) (bool, error) { return true, s.owner.check(tr) })
	if debitErr := s.debit(0, 0, callbacks); debitErr != nil {
		return graphSnapshotEnd{}, errors.Join(err, debitErr)
	}
	if err != nil {
		return graphSnapshotEnd{}, err
	}
	end.Work = s.Work()
	return end, nil
}

// Points batches a fixed <=32-key set into ONE complete authenticated Scan.
// This is explicitly scan-based fixture access, never indexed point latency.
func (s *graphSnapshot) Points(ctx context.Context, keys []graphSnapshotKey) ([]graphSnapshotPoint, error) {
	if s == nil || ctx == nil || len(keys) == 0 || len(keys) > 32 {
		return nil, errInvalid
	}
	points := make([]graphSnapshotPoint, len(keys))
	for i, key := range keys {
		if key.Group > 1 || !validName(key.Key) || s.owner == nil || !slices.Contains(s.owner.id.Cut.Scope, key.Group) {
			return nil, errInvalid
		}
		for j := 0; j < i; j++ {
			if keys[j] == key {
				return nil, errInvalid
			}
		}
		key.Key = strings.Clone(key.Key)
		points[i].Key = key
	}
	_, err := s.Scan(ctx, func(row graphSnapshotRow) error {
		for i := range points {
			if points[i].Key.Group == row.Group && points[i].Key.Key == row.Key {
				points[i].Found, points[i].Value = true, row.Value
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return points, nil
}
