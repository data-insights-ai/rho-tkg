package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// PageBytes admits raw copied records after SDK receipt. RangeOptions exposes
// a row limit, not a byte limit; this is not a hard native-client/RSS budget.
// Callbacks consume owned rows after ReadTransact succeeds and must not retain
// an entire dataset. No stream reconstructs a full snapshot or graph model.
type graphCostStreamPolicy struct{ PageRows, PageBytes int }

func (p graphCostStreamPolicy) validate() error {
	if p.PageRows < 1 || p.PageRows > 128 || p.PageBytes < 1 || p.PageBytes > 16<<20 {
		return errInvalid
	}
	return nil
}

type graphCostRawRow struct {
	Group                                    uint8
	Key, Value                               []byte
	IDKey, IDValue, RequestKey, RequestValue []byte  `json:",omitempty"`
	original                                 outcome // Decoded actual CDC head, never caller authority.
}
type graphCostStreamEnd struct {
	Rows, Groups [2]uint64
	Metadata     [2]metadata
}
type graphCostStream struct {
	ctx      context.Context
	h        *harness
	from, to cut
	policy   graphCostStreamPolicy
	metadata [2]metadata
}

func newGraphCostStream(h *harness, from, to cut, p graphCostStreamPolicy, emit func(graphCostRawRow) error) (*graphCostStream, error) {
	return newGraphCostStreamContext(nil, h, from, to, p, emit)
}

// Nil deliberately preserves historical native retry/default IO. Fresh callers
// supply their SAME run context; cancellation remains attached through OnError.
type graphCostContextResult struct{ value any }

// A fresh per-call cancellation can shorten, never replace/extend, the original
// run lifetime. Nil run retains historical native IO regardless of caller ctx.
func graphCostIOContext(run, caller context.Context) (context.Context, func()) {
	if run == nil || caller == nil {
		return run, func() {}
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if deadline, ok := caller.Deadline(); ok {
		ctx, cancel = context.WithDeadline(run, deadline)
	} else {
		ctx, cancel = context.WithCancel(run)
	}
	stop := context.AfterFunc(caller, cancel)
	if caller.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}

func graphCostReadContext(ctx context.Context, db fdb.Database, fn func(fdb.ReadTransaction) (any, error)) (any, error) {
	if ctx == nil {
		return db.ReadTransact(fn)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result, _, err := graphCostParallelRead(ctx, db, func(tr fdb.ReadTransaction) (graphCostContextResult, error) {
		value, err := fn(tr)
		return graphCostContextResult{value}, err
	})
	return result.value, err
}
func graphCostWriteContext(ctx context.Context, db fdb.Database, fn func(fdb.Transaction) (any, error)) (any, error) {
	if ctx == nil {
		return db.Transact(fn)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result, _, err := historyTransactContext(ctx, db, func() error { return nil }, func(tr fdb.Transaction) (graphCostContextResult, error) {
		value, err := fn(tr)
		return graphCostContextResult{value}, err
	})
	return result.value, err
}
func newGraphCostStreamContext(ctx context.Context, h *harness, from, to cut, p graphCostStreamPolicy, emit func(graphCostRawRow) error) (*graphCostStream, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if h == nil || emit == nil || p.validate() != nil || !validGraphCostProfile(h.config) || !slices.Equal(from.record.Scope, []uint8{0, 1}) || !slices.Equal(to.record.Scope, []uint8{0, 1}) || from.record.Graph != h.config.Graph || to.record.Graph != h.config.Graph || from.record.Topology != h.config.Topology || to.record.Topology != h.config.Topology || from.record.Epochs != h.config.Epochs || to.record.Epochs != h.config.Epochs || from.record.Round == 0 || from.record.Round > to.record.Round {
		return nil, errInvalid
	}
	for g := range 2 {
		if from.record.Groups[g] > to.record.Groups[g] || to.record.Groups[g] > h.config.Limits.Transactions {
			return nil, errInvalid
		}
	}
	from.record.Scope, to.record.Scope = slices.Clone(from.record.Scope), slices.Clone(to.record.Scope)
	s := &graphCostStream{ctx: ctx, h: &harness{db: h.db, config: h.config}, from: from, to: to, policy: p}
	result, err := graphCostReadContext(s.ctx, s.h.db, func(tr fdb.ReadTransaction) (any, error) { return s.check(tr, false) })
	if err != nil {
		return nil, err
	}
	s.metadata = result.([2]metadata)
	return s, nil
}

// Exact full-scope certificates and quiescent metadata are rechecked in the
// same native transaction as each page and again at EOF. Current is explicitly
// a quiescent census; it is never presented as a retained At snapshot.
func (s *graphCostStream) check(tr fdb.ReadTransaction, captured bool) ([2]metadata, error) {
	var actual [2]metadata
	for _, c := range []cut{s.from, s.to} {
		if err := s.h.verifyCutIdentity(tr, c); err != nil {
			return actual, err
		}
	}
	for g := uint8(0); g < 2; g++ {
		if err := s.h.checkConfig(tr, g); err != nil {
			return actual, err
		}
		m, err := s.h.meta(tr, g)
		if err != nil {
			return actual, err
		}
		if m.Groups != s.to.record.Groups[g] || m.Floor != s.to.record.Round || captured && m != s.metadata[g] {
			return actual, errMismatch
		}
		actual[g] = m
	}
	return actual, nil
}

func (s *graphCostStream) finish(rows [2]uint64) (graphCostStreamEnd, error) {
	_, err := graphCostReadContext(s.ctx, s.h.db, func(tr fdb.ReadTransaction) (any, error) { return s.check(tr, true) })
	if err != nil {
		return graphCostStreamEnd{}, err
	}
	return graphCostStreamEnd{Rows: rows, Groups: s.to.record.Groups, Metadata: s.metadata}, nil
}

func (h *harness) graphCostValidateRaw(row graphCostRawRow, history bool, round uint64) error {
	if h != nil && h.config.Cost.Version == 4 {
		return h.graphEdgeUniqueValidateRaw(row, history, round)
	}
	if row.Group > 1 {
		return errInvalid
	}
	v, err := decode[value](row.Value)
	if err != nil || !validGraphValue(v) || v.Round > round {
		return errors.Join(errInvalid, err)
	}
	prefix := h.prefix(row.Group) + "current/"
	if history {
		prefix = fmt.Sprintf("%shistory/%016x/", h.prefix(row.Group), v.Round)
	}
	encoded, found := strings.CutPrefix(string(row.Key), prefix)
	key, err := hex.DecodeString(encoded)
	if !found || err != nil || !validName(string(key)) || hex.EncodeToString(key) != encoded {
		return errInvalid
	}
	if !bytes.Equal(row.Key, []byte(prefix+hex.EncodeToString(key))) {
		return errInvalid
	}
	// Validate the fixed physical key grammar/owner using existing declarations.
	for _, n := range h.config.Cost.nodes() {
		if string(key) == lifeKey(n) {
			if row.Group != n.Owner || v.Value != 1 || v.Deleted {
				return errInvalid
			}
			return nil
		}
	}
	kind, _, found := strings.Cut(string(key), "/")
	if !found || kind != "edge" && kind != "out" && kind != "in" || v.Deleted || v.Value != 5 && v.Value != 6 {
		return errInvalid
	}
	edge, err := parseGraphEdge(kind, string(key), v.Value, h.config)
	if err != nil || edge.ID < 7 || (edge.ID-7)%2 != 0 || (edge.ID-7)/2 >= h.config.Cost.Facts || edge != h.config.Cost.edge((edge.ID-7)/2, v.Value) || kind != "in" && row.Group != 0 || kind == "in" && row.Group != edge.Target.Owner {
		return errors.Join(errInvalid, err)
	}
	return nil
}

// kind is history H, current C, or original change D. No output effects or
// cross-page counters are touched inside the retryable native callback.
func (s *graphCostStream) page(group uint8, begin, end fdb.Key, kind byte) ([]graphCostRawRow, error) {
	result, err := graphCostReadContext(s.ctx, s.h.db, func(tr fdb.ReadTransaction) (any, error) {
		if _, err := s.check(tr, true); err != nil {
			return nil, err
		}
		if kind != 'D' {
			if err := s.h.historyCutIn(tr, s.from); err != nil {
				return nil, err
			}
			if err := s.h.historyCutIn(tr, s.to); err != nil {
				return nil, err
			}
		}
		rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{Limit: s.policy.PageRows}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(rows) > s.policy.PageRows {
			return nil, errLimit
		}
		out := make([]graphCostRawRow, 0, len(rows))
		used := 0
		var previous []byte
		for _, raw := range rows {
			if len(raw.Key) > 1024 || len(raw.Value) == 0 || len(raw.Value) > 64<<10 || bytes.Compare(raw.Key, begin) < 0 || bytes.Compare(raw.Key, end) >= 0 || previous != nil && bytes.Compare(raw.Key, previous) <= 0 {
				return nil, errInvalid
			}
			row := graphCostRawRow{Group: group, Key: raw.Key, Value: raw.Value}
			if kind == 'D' {
				change, err := decode[changeGroup](row.Value)
				if err != nil {
					return nil, err
				}
				if err := s.h.validateChange(change); err != nil {
					return nil, err
				}
				o := change.Outcome
				if o.Coordinator != group || !bytes.Equal(row.Key, s.h.changeKey(group, o.Sequence, o.Request)) || o.Round <= s.from.record.Round || o.Round > s.to.record.Round {
					return nil, errInvalid
				}
				row.IDKey, row.RequestKey = s.h.key(group, "id", o.ID), s.h.key(group, "request", o.Request)
				row.IDValue, err = tr.Get(fdb.Key(row.IDKey)).Get()
				if err != nil {
					return nil, err
				}
				row.RequestValue, err = tr.Get(fdb.Key(row.RequestKey)).Get()
				if err != nil {
					return nil, err
				}
				original, err := wire(o)
				if err != nil || !bytes.Equal(row.IDValue, original) || !bytes.Equal(row.RequestValue, original) {
					return nil, errors.Join(errMismatch, err)
				}
				row.original = o
			} else if err := s.h.graphCostValidateRaw(row, kind == 'H', s.to.record.Round); err != nil {
				return nil, err
			}
			used += len(row.Key) + len(row.Value) + len(row.IDKey) + len(row.IDValue) + len(row.RequestKey) + len(row.RequestValue)
			if used > s.policy.PageBytes {
				return nil, errLimit
			}
			row.Key, row.Value = bytes.Clone(row.Key), bytes.Clone(row.Value)
			row.IDValue, row.RequestValue = bytes.Clone(row.IDValue), bytes.Clone(row.RequestValue)
			out = append(out, row)
			previous = raw.Key
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]graphCostRawRow), nil
}

func (h *harness) graphCostWalkHistory(old, final cut, p graphCostStreamPolicy, emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
	return h.graphCostWalkRows(old, final, p, emit, true)
}
func (h *harness) graphCostWalkCurrent(final cut, p graphCostStreamPolicy, emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
	return h.graphCostWalkRows(final, final, p, emit, false)
}
func (h *harness) graphCostWalkRows(from, final cut, p graphCostStreamPolicy, emit func(graphCostRawRow) error, history bool) (graphCostStreamEnd, error) {
	return h.graphCostWalkRowsContext(nil, from, final, p, emit, history)
}
func (h *harness) graphCostWalkRowsContext(ctx context.Context, from, final cut, p graphCostStreamPolicy, emit func(graphCostRawRow) error, history bool) (graphCostStreamEnd, error) {
	s, err := newGraphCostStreamContext(ctx, h, from, final, p, emit)
	if err != nil {
		return graphCostStreamEnd{}, err
	}
	var counts [2]uint64
	for g := uint8(0); g < 2; g++ {
		kind := byte('C')
		keyRange := prefixRange(s.h.prefix(g) + "current/")
		if history {
			kind = 'H'
			keyRange = fdb.KeyRange{Begin: fdb.Key(s.h.prefix(g) + "history/"), End: fdb.Key(fmt.Sprintf("%shistory/%016x0", s.h.prefix(g), final.record.Round))}
		}
		begin, end := keyRange.Begin.(fdb.Key), keyRange.End.(fdb.Key)
		for {
			rows, err := s.page(g, begin, end, kind)
			if err != nil {
				return graphCostStreamEnd{}, err
			}
			for _, row := range rows {
				counts[g]++
				if counts[g] > s.metadata[g].Versions {
					return graphCostStreamEnd{}, errLimit
				}
				if err := emit(row); err != nil {
					return graphCostStreamEnd{}, err
				}
			}
			if len(rows) < p.PageRows {
				break
			}
			begin = fdb.Key(append(bytes.Clone(rows[len(rows)-1].Key), 0))
		}
		want := s.metadata[g].Versions
		if !history {
			cross := s.h.config.Cost.Facts * s.h.config.Cost.CrossPercent / 100
			want = 1 + cross
			if g == 0 {
				want = 2 + 3*s.h.config.Cost.Facts - cross
			}
		}
		if counts[g] != want {
			return graphCostStreamEnd{}, errUnavailable
		}
	}
	return s.finish(counts)
}

type graphCostChangeCursor struct {
	s          *graphCostStream
	group      uint8
	begin, end fdb.Key
	page       []graphCostRawRow
	next       int
	count      uint64
	done       bool
}

func (c *graphCostChangeCursor) advance() (*graphCostRawRow, error) {
	if c.next == len(c.page) && !c.done {
		page, err := c.s.page(c.group, c.begin, c.end, 'D')
		if err != nil {
			return nil, err
		}
		c.page, c.next, c.done = page, 0, len(page) < c.s.policy.PageRows
		if len(page) > 0 {
			c.begin = fdb.Key(append(bytes.Clone(page[len(page)-1].Key), 0))
		}
	}
	if c.next == len(c.page) {
		if c.count != c.s.to.record.Groups[c.group]-c.s.from.record.Groups[c.group] {
			return nil, errUnavailable
		}
		return nil, nil
	}
	row := &c.page[c.next]
	if row.original.Sequence != c.s.from.record.Groups[c.group]+c.count+1 || row.original.Sequence > c.s.to.record.Groups[c.group] {
		return nil, errInvalid
	}
	c.count++
	c.next++
	return row, nil
}

func graphCostChangeOrder(a, b outcome) int {
	if n := cmp.Compare(a.Round, b.Round); n != 0 {
		return n
	}
	if n := cmp.Compare(a.Coordinator, b.Coordinator); n != 0 {
		return n
	}
	return strings.Compare(a.Request, b.Request)
}

func (h *harness) graphCostWalkChanges(from, final cut, p graphCostStreamPolicy, emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
	return h.graphCostWalkChangesContext(nil, from, final, p, emit)
}
func (h *harness) graphCostWalkChangesContext(ctx context.Context, from, final cut, p graphCostStreamPolicy, emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
	s, err := newGraphCostStreamContext(ctx, h, from, final, p, emit)
	if err != nil {
		return graphCostStreamEnd{}, err
	}
	var cursors [2]graphCostChangeCursor
	var heads [2]*graphCostRawRow
	for g := uint8(0); g < 2; g++ {
		bound := func(sequence uint64) fdb.Key {
			return fdb.Key(fmt.Sprintf("%schanges/%016x0", s.h.prefix(g), sequence))
		}
		cursors[g] = graphCostChangeCursor{s: s, group: g, begin: bound(from.record.Groups[g]), end: bound(final.record.Groups[g])}
		heads[g], err = cursors[g].advance()
		if err != nil {
			return graphCostStreamEnd{}, err
		}
	}
	var previous outcome
	for heads[0] != nil || heads[1] != nil {
		g := 0
		if heads[0] == nil || heads[1] != nil && graphCostChangeOrder(heads[1].original, heads[0].original) < 0 {
			g = 1
		}
		row := *heads[g]
		if previous.Round != 0 && graphCostChangeOrder(previous, row.original) >= 0 {
			return graphCostStreamEnd{}, errInvalid
		}
		previous = row.original
		if err := emit(row); err != nil {
			return graphCostStreamEnd{}, err
		}
		heads[g], err = cursors[g].advance()
		if err != nil {
			return graphCostStreamEnd{}, err
		}
	}
	return s.finish([2]uint64{cursors[0].count, cursors[1].count})
}
