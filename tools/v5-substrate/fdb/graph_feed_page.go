package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphFeedPage struct {
	Version    uint8
	Request    string
	Original   [32]byte
	From, Next [2]uint64
	Boundary   certificate
	Last       outcome
	Rows       []graphCostRawRow
	EOF        bool
}
type graphFeedDelivery struct {
	Page   graphFeedPage
	Record graphCostStoredRecord
	Replay bool
}
type graphFeedAck struct {
	Request    string
	Page       [32]byte
	From, Next [2]uint64
	Completed  certificate
}
type graphFeedAcknowledgement struct {
	Ack    graphFeedAck
	Record graphCostStoredRecord
	Cursor graphFeedCursor
}

func feedBound(h *harness, g uint8, sequence uint64) fdb.Key {
	return fdb.Key(fmt.Sprintf("%schanges/%016x0", h.prefix(g), sequence))
}

func (f *graphFeed) headIn(tr fdb.ReadTransaction, g uint8, next, through uint64) (*graphCostRawRow, error) {
	if next > through {
		return nil, errInvalid
	}
	if next == through {
		return nil, nil
	}
	rows, err := tr.GetRange(fdb.KeyRange{Begin: feedBound(f.s.source.h, g, next), End: feedBound(f.s.source.h, g, through)}, fdb.RangeOptions{Limit: 1}).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, errUnavailable
	}
	native := rows[0]
	if len(native.Key) > 1024 || len(native.Value) == 0 || len(native.Value) > 64<<10 {
		return nil, errLimit
	}
	group, err := decode[changeGroup](native.Value)
	if err != nil {
		return nil, err
	}
	if err := f.s.source.h.validateChange(group); err != nil {
		return nil, err
	}
	o := group.Outcome
	if next == math.MaxUint64 || o.Sequence != next+1 || o.Coordinator != g || !bytes.Equal(native.Key, f.s.source.h.changeKey(g, o.Sequence, o.Request)) {
		return nil, errMismatch
	}
	row := &graphCostRawRow{Group: g, Key: bytes.Clone(native.Key), Value: bytes.Clone(native.Value), original: o}
	canonical, err := wire(group)
	if err != nil || !bytes.Equal(canonical, row.Value) {
		return nil, errors.Join(errMismatch, err)
	}
	ob, err := wire(o)
	if err != nil {
		return nil, err
	}
	row.IDKey = f.s.source.h.key(g, "id", o.ID)
	row.RequestKey = f.s.source.h.key(g, "request", o.Request)
	row.IDValue, err = tr.Get(row.IDKey).Get()
	if err != nil {
		return nil, err
	}
	row.RequestValue, err = tr.Get(row.RequestKey).Get()
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(row.IDValue, ob) || !bytes.Equal(row.RequestValue, ob) {
		return nil, errUnavailable
	}
	row.IDValue, row.RequestValue = bytes.Clone(row.IDValue), bytes.Clone(row.RequestValue)
	return row, nil
}
func (f *graphFeed) validateBoundary(tr fdb.ReadTransaction, c graphFeedCursor, to certificate) error {
	h := f.s.source.h
	if !bytes.Equal(to.Scope, []byte{0, 1}) || to.Graph != h.config.Graph || to.Topology != h.config.Topology || to.Epochs != h.config.Epochs || to.Round < c.Completed.Round {
		return errInvalid
	}
	if err := h.verifyCutIdentity(tr, cut{to}); err != nil {
		return err
	}
	for g := range 2 {
		m, err := h.meta(tr, uint8(g))
		if err != nil {
			return err
		}
		if c.Positions[g] > to.Groups[g] || to.Groups[g] > m.Groups {
			return errMismatch
		}
	}
	return nil
}

// A stored page is authority only after its exact rows are rejoined to genuine
// immutable source groups. Equal counts or a caller-supplied hash cannot bless
// substituted vectors, skipped originals or a different physical before/after.
func (f *graphFeed) sealedIn(tr fdb.ReadTransaction, p graphFeedPage) error {
	if p.Version != 1 || !validName(p.Request) || p.Original != f.token.Original || len(p.Rows) > f.original.Policy.PageRows {
		return errMismatch
	}
	c := graphFeedCursor{Positions: p.From, Completed: f.original.Cut}
	if err := f.validateBoundary(tr, c, p.Boundary); err != nil {
		return err
	}
	next := p.From
	var previous outcome
	var heads [2]*graphCostRawRow
	for g := uint8(0); g < 2; g++ {
		if next[g] < f.original.Cut.Groups[g] {
			return errMismatch
		}
		if next[g] > f.original.Cut.Groups[g] {
			prior, err := f.headIn(tr, g, next[g]-1, next[g])
			if err != nil {
				return err
			}
			if previous.Round == 0 || graphCostChangeOrder(previous, prior.original) < 0 {
				previous = prior.original
			}
		}
		var err error
		heads[g], err = f.headIn(tr, g, next[g], p.Boundary.Groups[g])
		if err != nil {
			return err
		}
	}
	for _, row := range p.Rows {
		g := 0
		if heads[0] == nil || heads[1] != nil && graphCostChangeOrder(heads[1].original, heads[0].original) < 0 {
			g = 1
		}
		actual := heads[g]
		if actual == nil || row.Group != uint8(g) || !bytes.Equal(row.Key, actual.Key) || !bytes.Equal(row.Value, actual.Value) || !bytes.Equal(row.IDKey, actual.IDKey) || !bytes.Equal(row.IDValue, actual.IDValue) || !bytes.Equal(row.RequestKey, actual.RequestKey) || !bytes.Equal(row.RequestValue, actual.RequestValue) {
			return errMismatch
		}
		if actual.original.Round <= f.original.Cut.Round || actual.original.Round > p.Boundary.Round || previous.Round != 0 && graphCostChangeOrder(previous, actual.original) >= 0 {
			return errMismatch
		}
		previous = actual.original
		next[g]++
		var err error
		heads[g], err = f.headIn(tr, uint8(g), next[g], p.Boundary.Groups[g])
		if err != nil {
			return err
		}
	}
	if next != p.Next || p.Last != previous || p.EOF != (next == p.Boundary.Groups) || p.EOF != (heads[0] == nil && heads[1] == nil) || len(p.Rows) == 0 && !p.EOF {
		return errMismatch
	}
	return nil
}
func (f *graphFeed) enter() error {
	if f != nil && f.retained != nil {
		return f.retained.enter()
	}
	if f == nil || f.s == nil || f.reader == nil || f.lifetime == nil {
		return errInvalid
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.busy {
		return errHistoryInUse
	}
	f.busy = true
	f.lifetime.mu.Lock()
	defer f.lifetime.mu.Unlock()
	if f.lifetime.closed || f.lifetime.closing {
		f.busy = false
		return errHistoryExpired
	}
	if f.lifetime.busy {
		f.busy = false
		return errHistoryInUse
	}
	f.lifetime.busy = true
	return nil
}
func (f *graphFeed) leave() {
	if f.retained != nil {
		f.retained.leave()
		return
	}
	f.lifetime.mu.Lock()
	f.lifetime.busy = false
	f.lifetime.mu.Unlock()
	f.mu.Lock()
	f.busy = false
	f.mu.Unlock()
}

// Page seals one complete, bounded transaction-envelope page. It never advances
// a consumer position; the immutable native page is replayed until exact ACK.
// At most two coordinator heads are resident. SDK callback IO stays native-only.
func (f *graphFeed) Page(ctx context.Context, request string, boundary certificate) (graphFeedDelivery, error) {
	var zero graphFeedDelivery
	if ctx == nil || !validName(request) {
		return zero, errInvalid
	}
	if err := f.enter(); err != nil {
		return zero, err
	}
	defer f.leave()
	ctx, cancel, boundErr := f.s.feedRunContext(ctx, f.token)
	if boundErr != nil {
		return zero, boundErr
	}
	defer cancel()
	if !bytes.Equal(boundary.Scope, []uint8{0, 1}) || boundary.Graph != f.s.binding.Config.Graph || boundary.Topology != f.s.binding.Config.Topology || boundary.Epochs != f.s.binding.Config.Epochs {
		return zero, errInvalid
	}
	boundary.Scope = bytes.Clone(boundary.Scope)
	input := struct{ Boundary certificate }{boundary}
	if err := f.s.reserve(ctx, f.token, "page/"+request, "page", input); err != nil {
		return zero, err
	}
	replay := false
	_, _, err := historyTransactContext(ctx, f.s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		replay = false
		c, err := f.s.cursorIn(tr, f.token)
		if err != nil {
			return false, err
		}
		if err := f.checkReaderIn(tr); err != nil {
			return false, err
		}
		key := f.s.key("page/"+f.token.Request, request)
		prior, err := tr.Get(key).Get()
		if err != nil {
			return false, err
		}
		if prior != nil {
			if len(prior) > f.original.Policy.PageBytes {
				return false, errLimit
			}
			page, err := decode[graphFeedPage](prior)
			if err != nil || page.Request != request || page.Original != f.token.Original || !sameCertificate(page.Boundary, boundary) {
				return false, errors.Join(errMismatch, err)
			}
			if err := f.sealedIn(tr, page); err != nil {
				return false, err
			}
			replay = true
			return true, nil
		}
		if c.State != graphFeedActive {
			return false, errHistoryExpired
		}
		if err := f.validateBoundary(tr, c, boundary); err != nil {
			return false, err
		}
		if c.Pending != "" {
			return false, errHistoryInUse
		}
		if c.Pages >= f.original.Policy.MaxPages {
			return false, errLimit
		}
		page := graphFeedPage{Version: 1, Request: request, Original: f.token.Original, From: c.Positions, Next: c.Positions, Boundary: boundary, Last: c.Last}
		var heads [2]*graphCostRawRow
		for g := uint8(0); g < 2; g++ {
			heads[g], err = f.headIn(tr, g, page.Next[g], boundary.Groups[g])
			if err != nil {
				return false, err
			}
		}
		for len(page.Rows) < f.original.Policy.PageRows && (heads[0] != nil || heads[1] != nil) {
			g := 0
			if heads[0] == nil || heads[1] != nil && graphCostChangeOrder(heads[1].original, heads[0].original) < 0 {
				g = 1
			}
			row := *heads[g]
			if row.original.Round <= f.original.Cut.Round || row.original.Round > boundary.Round || page.Last.Round != 0 && graphCostChangeOrder(page.Last, row.original) >= 0 {
				return false, errMismatch
			}
			proposed := page
			proposed.Rows = append(append([]graphCostRawRow(nil), page.Rows...), row)
			proposed.Next[g]++
			proposed.Last = row.original
			encoded, err := wire(proposed)
			if err != nil {
				return false, err
			}
			if len(encoded) > f.original.Policy.PageBytes {
				if len(page.Rows) == 0 {
					return false, errLimit
				}
				break
			}
			page = proposed
			heads[g], err = f.headIn(tr, uint8(g), page.Next[g], boundary.Groups[g])
			if err != nil {
				return false, err
			}
		}
		page.EOF = page.Next == boundary.Groups
		if !page.EOF && len(page.Rows) == 0 {
			return false, errUnavailable
		}
		body, err := wire(page)
		if err != nil || len(body) > f.original.Policy.PageBytes {
			return false, errors.Join(errLimit, err)
		}
		if err := f.s.createIn(tr, key, page, uint64(len(key))+graphFeedControlBytes); err != nil {
			return false, err
		}
		c.Pages++
		c.Pending = request
		return true, put(tr, f.s.key("cursor", f.token.Request), c)
	})
	if err != nil {
		return zero, err
	}
	// The returned witness is the genuine persisted page, not the submitted Set.
	result, _, err := graphCostParallelRead(ctx, f.s.source.h.db, func(tr fdb.ReadTransaction) (graphFeedDelivery, error) {
		if err := f.checkReaderIn(tr); err != nil {
			return zero, err
		}
		record, err := snapshotStored(tr, f.s.key("page/"+f.token.Request, request), f.original.Policy.PageBytes)
		if err != nil {
			return zero, err
		}
		page, err := decode[graphFeedPage](record.Value)
		if err != nil || page.Request != request || !sameCertificate(page.Boundary, boundary) {
			return zero, errors.Join(errMismatch, err)
		}
		canonical, err := wire(page)
		if err != nil || !bytes.Equal(canonical, record.Value) {
			return zero, errors.Join(errMismatch, err)
		}
		if err := f.sealedIn(tr, page); err != nil {
			return zero, err
		}
		return graphFeedDelivery{page, record, replay}, nil
	})
	if err != nil {
		return zero, err
	}
	return result, nil
}

func (f *graphFeed) Ack(ctx context.Context, request string, pageDigest [32]byte) (graphFeedAcknowledgement, error) {
	var zero graphFeedAcknowledgement
	if ctx == nil || !validName(request) || pageDigest == [32]byte{} {
		return zero, errInvalid
	}
	if err := f.enter(); err != nil {
		return zero, err
	}
	defer f.leave()
	ctx, cancel, boundErr := f.s.feedRunContext(ctx, f.token)
	if boundErr != nil {
		return zero, boundErr
	}
	defer cancel()
	if err := f.s.reserve(ctx, f.token, "ack/"+request, "ack", pageDigest); err != nil {
		return zero, err
	}
	_, _, err := historyTransactContext(ctx, f.s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		c, err := f.s.cursorIn(tr, f.token)
		if err != nil {
			return false, err
		}
		if err := f.checkReaderIn(tr); err != nil {
			return false, err
		}
		ackKey := f.s.key("ack/"+f.token.Request, request)
		previous, found, err := feedGet[graphFeedAck](tr, ackKey)
		if err != nil {
			return false, err
		}
		raw, err := tr.Get(f.s.key("page/"+f.token.Request, request)).Get()
		if err != nil {
			return false, err
		}
		if len(raw) == 0 || len(raw) > f.original.Policy.PageBytes || sha256.Sum256(raw) != pageDigest {
			return false, errMismatch
		}
		page, err := decode[graphFeedPage](raw)
		if err != nil || page.Version != 1 || page.Original != f.token.Original || page.Request != request {
			return false, errors.Join(errMismatch, err)
		}
		if err := f.sealedIn(tr, page); err != nil {
			return false, err
		}
		if found {
			if previous.Page != pageDigest || previous.Request != request || previous.From != page.From || previous.Next != page.Next || page.EOF && !sameCertificate(previous.Completed, page.Boundary) {
				return false, errMismatch
			}
			return true, nil
		}
		if c.State != graphFeedActive {
			return false, errHistoryExpired
		}
		if page.From != c.Positions || c.Pending != request {
			return false, errMismatch
		}
		if err := f.validateBoundary(tr, c, page.Boundary); err != nil {
			return false, err
		}
		// Page completeness was established natively before its immutable seal. ACK
		// advances only that sealed vector; it cannot accept caller-supplied positions.
		for g := range 2 {
			if page.Next[g] < page.From[g] || page.Next[g] > page.Boundary.Groups[g] {
				return false, errMismatch
			}
		}
		if page.EOF != (page.Next == page.Boundary.Groups) {
			return false, errMismatch
		}
		completed := c.Completed
		if page.EOF {
			completed = page.Boundary
		}
		ack := graphFeedAck{request, pageDigest, page.From, page.Next, completed}
		if err := f.s.createIn(tr, ackKey, ack, uint64(len(ackKey))+8192); err != nil {
			return false, err
		}
		c.Positions, c.Completed, c.Last, c.Pending = page.Next, completed, page.Last, ""
		return true, put(tr, f.s.key("cursor", f.token.Request), c)
	})
	if err != nil {
		return zero, err
	}
	result, _, err := graphCostParallelRead(ctx, f.s.source.h.db, func(tr fdb.ReadTransaction) (graphFeedAcknowledgement, error) {
		if err := f.checkReaderIn(tr); err != nil {
			return zero, err
		}
		record, err := snapshotStored(tr, f.s.key("ack/"+f.token.Request, request), 8192)
		if err != nil {
			return zero, err
		}
		ack, err := decode[graphFeedAck](record.Value)
		if err != nil || ack.Page != pageDigest {
			return zero, errors.Join(errMismatch, err)
		}
		cursor, err := f.s.cursorIn(tr, f.token)
		if err != nil {
			return zero, err
		}
		return graphFeedAcknowledgement{ack, record, cursor}, nil
	})
	return result, err
}
