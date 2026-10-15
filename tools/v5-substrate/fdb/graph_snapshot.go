package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Selection inventory is small, independently admitted proof state, never the
// graph snapshot. Native page-byte limits are post-SDK admission, not RSS caps.
type graphSnapshotPolicy struct {
	PageRows, PageBytes, InventoryBytes int
	WorkRows, WorkBytes                 uint64
	ChainSteps                          int
	Cleanup                             graphSnapshotCleanupPolicy
}
type graphSnapshotCleanupPolicy struct {
	Rows, Bytes uint64
	Millis      int
}

func graphSnapshotCleanupDefault() graphSnapshotCleanupPolicy {
	return graphSnapshotCleanupPolicy{1024, 64 << 20, 30000}
}

func (p graphSnapshotPolicy) validate() error {
	if p.PageRows < 1 || p.PageRows > 128 || p.PageBytes < 1024 || p.PageBytes > 4<<20 || p.InventoryBytes < 1024 || p.InventoryBytes > 32<<20 || p.WorkRows < 1 || p.WorkRows > 1<<40 || p.WorkBytes < uint64(p.PageBytes) || p.WorkBytes > 1<<50 || p.ChainSteps < 1 || p.ChainSteps > 128 || p.Cleanup.Rows < 64 || p.Cleanup.Rows > 1024 || p.Cleanup.Bytes < 4<<20 || p.Cleanup.Bytes > 64<<20 || p.Cleanup.Millis < 1 || p.Cleanup.Millis > 30000 {
		return errInvalid
	}
	return nil
}

type graphSnapshotReceipt struct {
	Tx      transaction
	Outcome outcome
}
type graphSnapshotRequest struct {
	Mode  string
	Scope []uint8
	At    certificate
	After *graphSnapshotReceipt `json:",omitempty"`
}

func (q graphSnapshotRequest) validate() error {
	if !validScope(q.Scope) {
		return errInvalid
	}
	empty := graphColumnSame(q.At, certificate{})
	switch q.Mode {
	case "Fresh", "Stable":
		if !empty || q.After != nil {
			return errInvalid
		}
	case "At":
		if empty || q.After != nil || !slices.Equal(q.Scope, q.At.Scope) {
			return errInvalid
		}
	case "After":
		if !empty || q.After == nil || !q.After.Outcome.Commit || q.After.Outcome.Round == 0 {
			return errInvalid
		}
	default:
		return errInvalid
	}
	return nil
}

type graphSnapshotCandidate struct {
	Cut certificate
	Raw []byte
}

func graphSnapshotPrefer(next, prior graphSnapshotCandidate) bool {
	return next.Cut.Round > prior.Cut.Round || next.Cut.Round == prior.Cut.Round && bytes.Compare(next.Raw, prior.Raw) > 0
}

type graphSnapshotSelection struct {
	Request                       graphSnapshotRequest
	Selected                      certificate
	Config, Metadata              [2]graphCostStoredRecord
	Retention                     []graphCostStoredRecord
	Inventory                     []graphCostStoredRecord
	InventoryRows, InventoryBytes uint64
	ReceiptPair                   [2]graphCostStoredRecord
	ReceiptCDC                    graphCostStoredRecord
	AcquiredOwner                 graphCostStoredRecord
	Acquisition                   []graphCostStoredRecord
}

// OpeningRows/Bytes are conservative declared native-access reservations,
// charged BEFORE every opening SDK callback, including retries. They are not
// measured I/O. Later Rows/Bytes add successful-page logical data debit.
type graphSnapshotWork struct{ Rows, Bytes, Callbacks, OpeningRows, OpeningBytes, OpeningCallbacks, CleanupRows, CleanupBytes, CleanupCallbacks uint64 }
type graphSnapshot struct {
	mu        sync.Mutex
	owner     *graphHistoryRead
	policy    graphSnapshotPolicy
	selection graphSnapshotSelection
	work      graphSnapshotWork
	lifetime  *graphSnapshotLifetime
}

// Fresh pending cleanup exposes only the SAME bounded lifetime, never the
// legacy unbounded owner.Close. Its allowance/deadline cannot silently reset.
type graphSnapshotReadPending struct {
	lifetime *graphSnapshotLifetime
	Cause    error
}

func (e *graphSnapshotReadPending) Error() string {
	message := "fdb-snapshot: bounded cleanup pending; retry the same cleanup handle"
	if e != nil && e.Cause != nil {
		return message + ": " + e.Cause.Error()
	}
	return message
}
func (e *graphSnapshotReadPending) Unwrap() error { return e.Cause }
func (e *graphSnapshotReadPending) Close() error {
	if e == nil || e.lifetime == nil {
		return errInvalid
	}
	return e.lifetime.close()
}
func (e *graphSnapshotReadPending) Identity() graphHistoryOwner {
	if e == nil || e.lifetime == nil {
		return graphHistoryOwner{}
	}
	e.lifetime.mu.Lock()
	defer e.lifetime.mu.Unlock()
	o := e.lifetime.owner.id
	o.Cut.Scope = slices.Clone(o.Cut.Scope)
	o.Required.Scope = slices.Clone(o.Required.Scope)
	return o
}
func (e *graphSnapshotReadPending) Work() graphSnapshotWork {
	if e == nil || e.lifetime == nil {
		return graphSnapshotWork{}
	}
	return e.lifetime.work()
}

type graphSnapshotLifetime struct {
	mu                    sync.Mutex
	owner                 *graphHistoryRead
	policy                graphSnapshotCleanupPolicy
	allowance             graphSnapshotOpening
	deadline              time.Time
	busy, closing, closed bool
	// Fresh feed owners may carry a durable prepaid cleanup ticket. Nil keeps
	// historical selector cleanup and its encoded authority unchanged.
	ownedClose func() (graphSnapshotWork, error)
}

func newSnapshotLifetime(r *graphHistoryRead, p graphSnapshotCleanupPolicy) *graphSnapshotLifetime {
	return &graphSnapshotLifetime{owner: r, policy: p, allowance: graphSnapshotOpening{policy: graphSnapshotPolicy{WorkRows: p.Rows, WorkBytes: p.Bytes}}}
}
func (l *graphSnapshotLifetime) work() graphSnapshotWork {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ownedClose != nil {
		return graphSnapshotWork{CleanupRows: l.allowance.work.CleanupRows, CleanupBytes: l.allowance.work.CleanupBytes, CleanupCallbacks: l.allowance.work.CleanupCallbacks}
	}
	return graphSnapshotWork{CleanupRows: l.allowance.work.Rows, CleanupBytes: l.allowance.work.Bytes, CleanupCallbacks: l.allowance.work.Callbacks}
}
func (l *graphSnapshotLifetime) close() error {
	if l == nil || l.owner == nil {
		return errInvalid
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	if l.busy {
		return &graphSnapshotReadPending{l, errHistoryInUse}
	}
	l.closing = true
	if l.ownedClose != nil {
		work, err := l.ownedClose()
		l.allowance.work = work
		if err != nil {
			return &graphSnapshotReadPending{l, err}
		}
		l.closed = true
		return nil
	}
	if l.deadline.IsZero() {
		l.deadline = time.Now().Add(time.Duration(l.policy.Millis) * time.Millisecond)
	}
	ctx, cancel := context.WithDeadline(context.Background(), l.deadline)
	defer cancel()
	callbacks, err := l.owner.closeContext(ctx, func() error { return l.allowance.reserve(64, 4<<20) })
	l.allowance.work.Callbacks += uint64(callbacks)
	if err != nil {
		return &graphSnapshotReadPending{l, err}
	}
	l.closed = true
	return nil
}

type graphSnapshotOpening struct {
	policy graphSnapshotPolicy
	work   graphSnapshotWork
}

func (o *graphSnapshotOpening) reserve(rows, bytes uint64) error {
	if rows > o.policy.WorkRows-o.work.Rows || bytes > o.policy.WorkBytes-o.work.Bytes {
		return errLimit
	}
	o.work.Rows += rows
	o.work.Bytes += bytes
	o.work.OpeningRows += rows
	o.work.OpeningBytes += bytes
	return nil
}
func snapshotOpeningRead[T any](ctx context.Context, db fdb.Database, o *graphSnapshotOpening, rows, bytes uint64, fn func(fdb.ReadTransaction) (T, error)) (T, error) {
	result, callbacks, err := graphCostParallelRead(ctx, db, func(tr fdb.ReadTransaction) (T, error) {
		if err := o.reserve(rows, bytes); err != nil {
			var zero T
			return zero, err
		}
		return fn(tr)
	})
	o.work.Callbacks += uint64(callbacks)
	o.work.OpeningCallbacks += uint64(callbacks)
	return result, err
}

// Cancellation stays attached to this SAME native transaction through Commit
// and OnError. Retry and callback caps cannot create a new immutable request.
func snapshotOpeningWrite[T any](ctx context.Context, db fdb.Database, o *graphSnapshotOpening, fn func(fdb.Transaction) (T, error)) (T, error) {
	result, callbacks, err := historyTransactContext(ctx, db, func() error { return o.reserve(64, 4<<20) }, fn)
	o.work.Callbacks += uint64(callbacks)
	o.work.OpeningCallbacks += uint64(callbacks)
	return result, err
}

func snapshotStored(tr fdb.ReadTransaction, key fdb.Key, cap int) (graphCostStoredRecord, error) {
	raw, err := tr.Get(key).Get()
	if err != nil {
		return graphCostStoredRecord{}, err
	}
	if len(raw) == 0 || len(raw) > cap {
		return graphCostStoredRecord{}, errUnavailable
	}
	return graphCostStoredRecord{bytes.Clone(key), bytes.Clone(raw)}, nil
}

// stableIn scans the complete bounded native inventory in one read version.
// Key suffixes are double-hex JSON, NOT round indexes. This function never writes
// a fence, certificate, owner or metadata. Exact inventory bytes survive retries
// only after ReadTransact returns, for external proof IO outside the callback.
func (s *graphHistoryRetention) stableIn(tr fdb.ReadTransaction, q graphSnapshotRequest, p graphSnapshotPolicy) (graphSnapshotSelection, error) {
	e := graphSnapshotSelection{Request: q}
	state, err := s.state(tr)
	if err != nil {
		return e, err
	}
	for g := uint8(0); g < 2; g++ {
		if err := s.h.checkConfig(tr, g); err != nil {
			return e, err
		}
		e.Config[g], err = snapshotStored(tr, s.h.key(g, "meta", "config"), 64<<10)
		if err != nil {
			return e, err
		}
		e.Metadata[g], err = snapshotStored(tr, s.h.key(g, "meta", "state"), 4096)
		if err != nil {
			return e, err
		}
	}
	for _, name := range []string{"binding", "ledger", "state"} {
		r, err := snapshotStored(tr, s.h.historyKey(name), 64<<10)
		if err != nil {
			return e, err
		}
		e.Retention = append(e.Retention, r)
	}
	if state.Request != "" {
		r, err := snapshotStored(tr, s.h.historyKey("intent/"+state.Request), 4096)
		if err != nil {
			return e, err
		}
		e.Retention = append(e.Retention, r)
	}
	if state.Completed.Request != "" {
		r, err := snapshotStored(tr, s.h.historyKey("seal/"+state.Completed.Request), 4096)
		if err != nil {
			return e, err
		}
		e.Retention = append(e.Retention, r)
	}
	m, err := s.h.meta(tr, q.Scope[0])
	if err != nil {
		return e, err
	}
	bound := prefixRange(s.h.prefix(q.Scope[0]) + "cut/")
	begin := bound.Begin
	var best graphSnapshotCandidate
	expired := false
	for {
		rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: bound.End}, fdb.RangeOptions{Limit: p.PageRows}).GetSliceWithError()
		if err != nil {
			return e, err
		}
		pageBytes := uint64(0)
		for _, row := range rows {
			// Admission precedes decoding/cloning delivered native bytes.
			n := uint64(len(row.Key) + len(row.Value))
			if len(row.Key) > 17<<10 || len(row.Value) == 0 || len(row.Value) > 4096 || n > uint64(p.PageBytes)-pageBytes || n > uint64(p.InventoryBytes)-e.InventoryBytes || e.InventoryRows >= s.h.config.Limits.Certificates {
				return e, errLimit
			}
			pageBytes += n
			e.InventoryBytes += n
			e.InventoryRows++
			c, err := decode[certificate](row.Value)
			if err != nil || !validScope(c.Scope) || c.Scope[0] != q.Scope[0] || c.Graph != s.h.config.Graph || c.Topology != s.h.config.Topology || c.Epochs != s.h.config.Epochs || c.Round == 0 || !bytes.Equal(row.Key, s.h.key(c.Scope[0], "cut", hex.EncodeToString(row.Value))) {
				return e, errors.Join(errMismatch, err)
			}
			for g := uint8(0); g < 2; g++ {
				actual, err := decode[metadata](e.Metadata[g].Value)
				if err != nil {
					return e, err
				}
				if c.Groups[g] > actual.Groups || slices.Contains(c.Scope, g) && c.Round > actual.Floor {
					return e, errMismatch
				}
			}
			e.Inventory = append(e.Inventory, graphCostStoredRecord{bytes.Clone(row.Key), bytes.Clone(row.Value)})
			if !slices.Equal(c.Scope, q.Scope) {
				continue
			}
			if c.Round < s.floor(state) {
				expired = true
				continue
			}
			next := graphSnapshotCandidate{c, row.Value}
			if graphSnapshotPrefer(next, best) {
				best = graphSnapshotCandidate{c, bytes.Clone(row.Value)}
			}
		}
		if len(rows) < p.PageRows {
			break
		}
		begin = fdb.Key(append(bytes.Clone(rows[len(rows)-1].Key), 0))
	}
	if e.InventoryRows != m.Certificates {
		return e, errUnavailable
	}
	if best.Cut.Round == 0 {
		if expired {
			return e, errHistoryExpired
		}
		return e, errUnavailable
	}
	e.Selected = best.Cut
	return e, nil
}

// OpenGraphSnapshot owns the selected retained source before returning any data.
// Stable selection is read-only; acquisition is a genuine guarded owner write.
// A collector winning that race yields expired/busy, never a different selector.
func (s *graphHistoryRetention) OpenGraphSnapshot(ctx context.Context, q graphSnapshotRequest, p graphSnapshotPolicy) (*graphSnapshot, error) {
	if s == nil || ctx == nil || q.validate() != nil || p.validate() != nil {
		return nil, errInvalid
	}
	// Own every nested request/receipt byte before retaining it or retrying SDK IO.
	body, err := wire(q)
	if err != nil {
		return nil, err
	}
	q, err = decode[graphSnapshotRequest](body)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opening := &graphSnapshotOpening{policy: p}
	_, err = snapshotOpeningRead(ctx, s.h.db, opening, 64, 4<<20, func(tr fdb.ReadTransaction) (bool, error) {
		actual, err := s.h.historyRetentionIn(tr)
		if err != nil {
			return false, err
		}
		if actual == nil || !graphColumnSame(actual.binding, s.binding) {
			return false, errMismatch
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	var selected graphSnapshotSelection
	if q.Mode == "Stable" {
		e, err := snapshotOpeningRead(ctx, s.h.db, opening, s.h.config.Limits.Certificates+64, uint64(p.InventoryBytes)+(4<<20), func(tr fdb.ReadTransaction) (graphSnapshotSelection, error) { return s.stableIn(tr, q, p) })
		if err != nil {
			return nil, err
		}
		selected = e
	} else {
		selected.Request = q
		c := cut{q.At}
		var after *outcome
		if q.Mode == "After" {
			body, err := wire(q.After.Tx)
			if err != nil {
				return nil, err
			}
			proof, err := snapshotOpeningRead(ctx, s.h.db, opening, 64, 4<<20, func(tr fdb.ReadTransaction) (graphCostParallelProof, error) {
				return s.snapshotReceiptIn(tr, q.After.Tx, body)
			})
			if err != nil {
				return nil, err
			}
			if proof.Outcome != q.After.Outcome || !proof.Outcome.Commit || !touchesScope(q.After.Tx, q.Scope) {
				return nil, errMismatch
			}
			selected.ReceiptPair, selected.ReceiptCDC = proof.Pair, proof.Original
			after = new(proof.Outcome)
		}
		if q.Mode == "Fresh" || q.Mode == "After" {
			var err error
			c, err = snapshotOpeningWrite(ctx, s.h.db, opening, func(tr fdb.Transaction) (cut, error) {
				result, err := s.h.freshIn(tr, q.Scope, after)
				if err != nil {
					return cut{}, err
				}
				return result.(cut), nil
			})
			if err != nil {
				return nil, err
			}
		}
		selected.Selected = c.record
	}
	r, callbacks, err := s.newReadContext(ctx, cut{selected.Selected}, nil, func() error { return opening.reserve(64, 4<<20) })
	opening.work.Callbacks += uint64(callbacks)
	opening.work.OpeningCallbacks += uint64(callbacks)
	if err != nil {
		return nil, err
	}
	lifetime := newSnapshotLifetime(r, p.Cleanup)
	_, err = snapshotOpeningWrite(ctx, s.h.db, opening, func(tr fdb.Transaction) (bool, error) {
		return true, s.acquireIn(tr, r.id)
	})
	if err != nil {
		err = graphHistoryCommitError(err)
		return nil, errors.Join(err, lifetime.close())
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, lifetime.close())
	}
	return s.finishGraphSnapshot(ctx, r, selected, opening, lifetime)
}

// Feed callers reach this only AFTER their exact factory identity, shared pin
// and cursor have been natively published. This grants no acquisition itself.
func (s *graphHistoryRetention) observeGraphSnapshot(ctx context.Context, r *graphHistoryRead, q graphSnapshotRequest, p graphSnapshotPolicy, work graphSnapshotWork, lifetime *graphSnapshotLifetime) (*graphSnapshot, error) {
	if s == nil || r == nil || ctx == nil || p.validate() != nil || q.validate() != nil {
		return nil, errInvalid
	}
	if lifetime == nil {
		lifetime = newSnapshotLifetime(r, p.Cleanup)
	}
	return s.finishGraphSnapshot(ctx, r, graphSnapshotSelection{Request: q, Selected: r.id.Cut}, &graphSnapshotOpening{policy: p, work: work}, lifetime)
}

func (s *graphHistoryRetention) finishGraphSnapshot(ctx context.Context, r *graphHistoryRead, selected graphSnapshotSelection, opening *graphSnapshotOpening, lifetime *graphSnapshotLifetime) (*graphSnapshot, error) {
	// An acknowledged acquisition is not replaced by submitted owner bytes.
	// Observe the exact durable owner/certificate/config/control records before
	// returning a usable session, and keep the genuine identity on cleanup loss.
	observed, observeErr := snapshotOpeningRead(ctx, s.h.db, opening, 64, 4<<20, func(tr fdb.ReadTransaction) (graphSnapshotSelection, error) {
		e := selected
		if err := r.check(tr); err != nil {
			return e, err
		}
		var err error
		e.AcquiredOwner, err = snapshotStored(tr, s.ownerKey(r.id.Request), historyControlBytes)
		if err != nil {
			return e, err
		}
		ownerBody, err := wire(r.id)
		if err != nil || !bytes.Equal(e.AcquiredOwner.Value, ownerBody) {
			return e, errors.Join(errMismatch, err)
		}
		for _, key := range []fdb.Key{s.h.historyKey("binding"), s.h.historyKey("state"), s.h.historyKey("ledger")} {
			row, err := snapshotStored(tr, key, 64<<10)
			if err != nil {
				return e, err
			}
			e.Acquisition = append(e.Acquisition, row)
		}
		body, err := wire(e.Selected)
		if err != nil {
			return e, err
		}
		row, err := snapshotStored(tr, s.h.key(e.Selected.Scope[0], "cut", hex.EncodeToString(body)), 4096)
		if err != nil {
			return e, err
		}
		e.Acquisition = append(e.Acquisition, row)
		for g := uint8(0); g < 2; g++ {
			for _, name := range []string{"config", "state"} {
				row, err := snapshotStored(tr, s.h.key(g, "meta", name), 64<<10)
				if err != nil {
					return e, err
				}
				e.Acquisition = append(e.Acquisition, row)
			}
		}
		return e, nil
	})
	if observeErr != nil {
		return nil, errors.Join(observeErr, lifetime.close())
	}
	selected = observed
	return &graphSnapshot{owner: r, policy: opening.policy, selection: selected, work: opening.work, lifetime: lifetime}, nil
}

func (s *graphHistoryRetention) snapshotReceiptIn(tr fdb.ReadTransaction, x transaction, body []byte) (graphCostParallelProof, error) {
	var proof graphCostParallelProof
	if err := s.h.checkConfig(tr, x.Coordinator); err != nil {
		return proof, err
	}
	var err error
	for i, key := range []fdb.Key{s.h.key(x.Coordinator, "id", x.ID), s.h.key(x.Coordinator, "request", x.Request)} {
		proof.Pair[i], err = snapshotStored(tr, key, 64<<10)
		if err != nil {
			return proof, err
		}
	}
	proof.Outcome, err = s.h.graphCostParallelOriginal(x, body, proof.Pair)
	if err != nil {
		return proof, err
	}
	if !proof.Outcome.Commit {
		return proof, errInvalid
	}
	proof.Original, err = snapshotStored(tr, s.h.changeKey(x.Coordinator, proof.Outcome.Sequence, x.Request), 64<<10)
	if err != nil {
		return proof, err
	}
	proof.Group, err = decode[changeGroup](proof.Original.Value)
	if err != nil {
		return proof, err
	}
	if err := s.h.validateChange(proof.Group); err != nil {
		return proof, err
	}
	if proof.Group.Outcome != proof.Outcome || !graphColumnSame(proof.Group.Tx, x) {
		return proof, errMismatch
	}
	return proof, nil
}

func (s *graphSnapshot) Cut() (certificate, error) {
	if s == nil || s.lifetime == nil {
		return certificate{}, errInvalid
	}
	s.lifetime.mu.Lock()
	defer s.lifetime.mu.Unlock()
	if s.lifetime.closed {
		return certificate{}, errHistoryExpired
	}
	if s.lifetime.closing {
		return certificate{}, errHistoryInUse
	}
	c := s.owner.id.Cut
	c.Scope = slices.Clone(c.Scope)
	return c, nil
}
func (s *graphSnapshot) Work() graphSnapshotWork {
	if s == nil {
		return graphSnapshotWork{}
	}
	s.mu.Lock()
	w := s.work
	s.mu.Unlock()
	if s.lifetime != nil {
		c := s.lifetime.work()
		w.CleanupRows, w.CleanupBytes, w.CleanupCallbacks = c.CleanupRows, c.CleanupBytes, c.CleanupCallbacks
	}
	return w
}
func (s *graphSnapshot) Close() error {
	if s == nil || s.owner == nil || s.lifetime == nil {
		return errInvalid
	}
	return s.lifetime.close()
}
