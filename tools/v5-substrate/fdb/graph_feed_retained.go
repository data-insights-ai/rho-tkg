package main

// Feed3 explicitly relinquishes physical snapshot authority. It retains native
// complete CDC, paired originals and certificates at their stable source keys;
// it never reconstructs a deleted physical snapshot or fabricates a read owner.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphFeedRetainedPhase struct {
	Version                                 uint8
	Original                                [32]byte
	Request                                 string // Empty means the original physical snapshot mode.
	OwnerRows                               uint64
	OwnersSHA256, SnapshotSHA256, PinSHA256 [32]byte
}

func newGraphRetainedFeedStore(ctx context.Context, catalog *graphOwnerCatalog, p graphFeedPolicy) (*graphFeedStore, error) {
	if catalog == nil || catalog.h == nil || catalog.binding.Version != 2 || catalog.binding.Transfer == nil || catalog.binding.validate() != nil {
		return nil, errInvalid
	}
	ctx, cancel, err := catalog.operationContext(ctx)
	if err != nil {
		return nil, err
	}
	source := &graphHistoryRetention{ctx: ctx, h: catalog.h, binding: catalog.binding.Source}
	// Retain this immutable normal run context for later physical snapshot IO;
	// it cancels with its caller and expires at the originally admitted deadline.
	// Cleanup uses a separate already-funded context, never this normal lifetime.
	store, err := installGraphFeedStore(ctx, source, p, nil, &catalog.binding)
	if err != nil {
		cancel()
	} else {
		store.stopNormal = cancel
	}
	return store, err
}

func (s *graphFeedStore) retainedAdmissionIn(tr fdb.ReadTransaction) error {
	p := s.binding.Retention
	if s.binding.Version != 3 || s.binding.Admission != nil || p == nil || p.Version != 2 || p.validate() != nil || p.Config != s.binding.Config || !graphColumnSame(p.Source, s.binding.Source) {
		return errMismatch
	}
	actual, err := s.source.h.ownerCatalogIn(tr)
	if err != nil {
		return err
	}
	if actual == nil || !graphColumnSame(actual.binding, *p) {
		return errMismatch
	}
	return nil
}

func (s *graphFeedStore) retainedRunContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if s == nil || s.binding.Retention == nil {
		return nil, nil, errInvalid
	}
	if ctx == nil {
		return nil, nil, errInvalid
	}
	ioctx, stop := graphCostIOContext(s.source.ctx, ctx)
	c := &graphOwnerCatalog{h: s.source.h, binding: *s.binding.Retention}
	bounded, cancel, err := c.operationContext(ioctx)
	if err != nil {
		stop()
		return nil, nil, err
	}
	return bounded, func() { cancel(); stop() }, nil
}

func (s *graphFeedStore) feedRunContext(ctx context.Context, token graphFeedToken) (context.Context, context.CancelFunc, error) {
	if s.binding.Retention == nil {
		return feedContext(ctx, token)
	}
	if s.binding.Retention.Transfer == nil || s.binding.Retention.Transfer.validate() != nil {
		return nil, nil, errMismatch
	}
	if !validName(token.Request) || token.Secret == [32]byte{} || token.Original == [32]byte{} || token.DeadlineNanos != s.binding.Retention.Transfer.DeadlineUnixNanos {
		return nil, nil, errInvalid
	}
	bounded, stop, err := s.retainedRunContext(ctx)
	if err != nil {
		return nil, nil, err
	}
	return bounded, stop, nil
}

func (s *graphFeedStore) feedCleanupCeiling(deadline int64) int64 {
	if s.binding.Retention != nil {
		return s.binding.Retention.Transfer.DeadlineUnixNanos + int64(30*time.Second)
	}
	return deadline
}
func (s *graphFeedStore) feedCleanupContext(ctx context.Context, token graphFeedToken) (context.Context, context.CancelFunc, error) {
	if s == nil || ctx == nil {
		return nil, nil, errInvalid
	}
	if s.binding.Retention == nil {
		return feedContext(ctx, token)
	}
	p := s.binding.Retention.Transfer
	if p == nil || p.validate() != nil || token.DeadlineNanos != p.DeadlineUnixNanos {
		return nil, nil, errMismatch
	}
	now, ceiling := time.Now().UnixNano(), s.feedCleanupCeiling(token.DeadlineNanos)
	if now < p.AdmittedUnixNanos || now >= ceiling {
		return nil, nil, context.DeadlineExceeded
	}
	bounded, cancel := context.WithDeadline(context.WithoutCancel(ctx), time.Unix(0, now+min(ceiling-now, int64(30*time.Second))))
	return bounded, cancel, nil
}

func (s *graphFeedStore) retainedPhaseIn(tr fdb.ReadTransaction, token graphFeedToken) (graphFeedRetainedPhase, error) {
	var zero graphFeedRetainedPhase
	if s.binding.Retention == nil {
		return zero, errInvalid
	}
	if _, err := s.originalIn(tr, token); err != nil {
		return zero, err
	}
	p, found, err := feedGet[graphFeedRetainedPhase](tr, s.key("retained-phase", token.Request))
	if err != nil {
		return p, err
	}
	if !found || p.Version != 1 || p.Original != token.Original {
		return p, errUnavailable
	}
	if p.Request == "" {
		if p.OwnerRows != 0 || p.OwnersSHA256 != [32]byte{} || p.SnapshotSHA256 != [32]byte{} || p.PinSHA256 != [32]byte{} {
			return p, errMismatch
		}
	} else if !validName(p.Request) || p.OwnerRows < 2 || p.OwnerRows > s.source.binding.Policy.MaxOwners || p.OwnersSHA256 == [32]byte{} || p.SnapshotSHA256 == [32]byte{} || p.PinSHA256 == [32]byte{} {
		return p, errMismatch
	}
	return p, nil
}
func (s *graphFeedStore) retainedSnapshotIn(tr fdb.ReadTransaction, token graphFeedToken) error {
	if s.binding.Retention == nil {
		return nil
	} // Literal legacy path: no extra IO.
	p, err := s.retainedPhaseIn(tr, token)
	if err != nil {
		return err
	}
	if p.Request != "" {
		return errHistoryExpired
	}
	return nil
}
func (s *graphFeedStore) isRetainedOnlyIn(tr fdb.ReadTransaction, token graphFeedToken) (bool, error) {
	if s.binding.Retention == nil {
		return false, nil
	}
	p, err := s.retainedPhaseIn(tr, token)
	if err != nil {
		return false, err
	}
	if p.Request == "" {
		return false, nil
	}
	o, err := s.originalIn(tr, token)
	if err != nil {
		return false, err
	}
	for i, id := range []graphHistoryOwner{o.Snapshot, o.Pin} {
		r, err := snapshotStored(tr, s.source.ownerKey(id.Request), historyControlBytes)
		if err != nil {
			return false, err
		}
		want := p.SnapshotSHA256
		if i == 1 {
			want = p.PinSHA256
		}
		if sha256.Sum256(r.Value) != want {
			return false, errMismatch
		}
		actual, err := decode[graphHistoryOwner](r.Value)
		if err != nil {
			return false, err
		}
		if actual.State != historyReleased {
			return false, errMismatch
		}
		actual.State, actual.TerminalRequest = id.State, id.TerminalRequest
		if !graphColumnSame(actual, id) {
			return false, errMismatch
		}
	}
	return true, nil
}

// ToRetainedOnly irreversibly releases only this token's physical pin. Initial
// snapshot Close must have AUTHENTICALLY committed, and no token physical
// reader may remain acquired. Open/Publish conflict on this same phase key and
// the owner range; no new physical read can slip between the census and release.
// The hash is produced by this complete native census, never accepted as input.
func (s *graphFeedStore) ToRetainedOnly(ctx context.Context, token graphFeedToken, request string) (graphCostStoredRecord, error) {
	var zero graphCostStoredRecord
	if s == nil || s.binding.Retention == nil || ctx == nil || !validName(request) || !validName(token.Request) {
		return zero, errInvalid
	}
	ctx, cancel, err := s.feedRunContext(ctx, token)
	if err != nil {
		return zero, err
	}
	defer cancel()
	if err := s.reserve(ctx, token, "retained/"+request, "retained", token); err != nil {
		return zero, err
	}
	_, _, err = historyTransactContext(ctx, s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		o, err := s.originalIn(tr, token)
		if err != nil {
			return false, err
		}
		p, err := s.retainedPhaseIn(tr, token)
		if err != nil {
			return false, err
		}
		if p.Request != "" {
			if p.Request != request {
				return false, errMismatch
			}
			return s.isRetainedOnlyIn(tr, token)
		}
		cursor, err := s.cursorIn(tr, token)
		if err != nil {
			return false, err
		}
		if cursor.State != graphFeedActive {
			return false, errHistoryExpired
		}
		ticket, err := s.cleanupIn(tr, token, o.Snapshot.Request)
		if err != nil {
			return false, err
		}
		if !ticket.Closed || ticket.TerminalState != historyReleased || ticket.TerminalRequest != "close" {
			return false, errHistoryInUse
		}
		rows, err := tr.GetRange(prefixRange(s.source.ownerPrefix()), fdb.RangeOptions{Limit: int(s.source.binding.Policy.MaxOwners) + 1}).GetSliceWithError()
		if err != nil {
			return false, err
		}
		if uint64(len(rows)) > s.source.binding.Policy.MaxOwners {
			return false, errLimit
		}
		state, err := s.source.state(tr)
		if err != nil {
			return false, err
		}
		live := uint64(0)
		snapshotFound, pinFound := false, false
		for _, row := range rows {
			if len(row.Key) > 1024 || len(row.Value) == 0 || len(row.Value) > historyControlBytes {
				return false, errLimit
			}
			owner, err := decode[graphHistoryOwner](row.Value)
			if err != nil {
				return false, err
			}
			body, err := wire(owner)
			if err != nil || !historyOwnerValid(owner) || !bytes.Equal(row.Key, s.source.ownerKey(owner.Request)) || !bytes.Equal(body, row.Value) {
				return false, errors.Join(errMismatch, err)
			}
			if owner.State == historyActive {
				live++
			}
			if owner.Request == o.Snapshot.Request {
				if owner.State != historyReleased || owner.TerminalRequest != "close" {
					return false, errHistoryInUse
				}
				identity := owner
				identity.State, identity.TerminalRequest = o.Snapshot.State, o.Snapshot.TerminalRequest
				if !graphColumnSame(identity, o.Snapshot) {
					return false, errMismatch
				}
				snapshotFound = true
				p.SnapshotSHA256 = sha256.Sum256(row.Value)
			} else if owner.PinRequest == o.Pin.Request && owner.State == historyActive {
				return false, errHistoryInUse
			}
			if owner.Request == o.Pin.Request {
				if !graphColumnSame(owner, o.Pin) {
					return false, errHistoryExpired
				}
				pinFound = true
			}
			p.OwnersSHA256 = historyChain(p.OwnersSHA256, row.Key, row.Value)
		}
		if !snapshotFound || !pinFound || live != state.Owners {
			return false, errUnavailable
		}
		if err := s.source.terminateIn(tr, o.Pin.Request, o.Pin.Secret, historyReleased, request); err != nil {
			return false, err
		}
		pin, err := snapshotStored(tr, s.source.ownerKey(o.Pin.Request), historyControlBytes)
		if err != nil {
			return false, err
		}
		p.Request, p.OwnerRows, p.PinSHA256 = request, uint64(len(rows)), sha256.Sum256(pin.Value)
		return true, put(tr, s.key("retained-phase", token.Request), p)
	})
	if err != nil {
		return zero, err
	}
	record, _, err := graphCostParallelRead(ctx, s.source.h.db, func(tr fdb.ReadTransaction) (graphCostStoredRecord, error) {
		ok, err := s.isRetainedOnlyIn(tr, token)
		if err != nil {
			return zero, err
		}
		if !ok {
			return zero, errUnavailable
		}
		return snapshotStored(tr, s.key("retained-phase", token.Request), 8192)
	})
	return record, err
}

// These are genuine retained-feed control identities, never history owners.
// The mutable record itself prepays Close before native State1 acquisition.
type graphFeedRetainedIdentity struct {
	Request          string
	Secret, Original [32]byte
}
type graphFeedRetainedRecord struct {
	Version  uint8
	Identity graphFeedRetainedIdentity
	State    uint8 // 0 reserved; 1 acquired; 2 closed, including absent acquisition.
	Cleanup  graphOwnerCleanup
}
type graphFeedRetainedReader struct {
	s                     *graphFeedStore
	token                 graphFeedToken
	id                    graphFeedRetainedIdentity
	mu                    sync.Mutex
	busy, closing, closed bool
	spent                 graphSnapshotWork
}
type graphFeedRetainedPending struct {
	reader *graphFeedRetainedReader
	Cause  error
}

func (e *graphFeedRetainedPending) Error() string {
	message := "fdb-feed: same retained reader cleanup pending"
	if e != nil && e.Cause != nil {
		return message + ": " + e.Cause.Error()
	}
	return message
}
func (e *graphFeedRetainedPending) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
func (e *graphFeedRetainedPending) Close() error {
	if e == nil || e.reader == nil {
		return errInvalid
	}
	return e.reader.close()
}
func (e *graphFeedRetainedPending) Identity() graphFeedRetainedIdentity {
	if e == nil || e.reader == nil {
		return graphFeedRetainedIdentity{}
	}
	return e.reader.id
}
func (e *graphFeedRetainedPending) Work() graphSnapshotWork {
	if e == nil || e.reader == nil {
		return graphSnapshotWork{}
	}
	return e.reader.work()
}

func retainedFeedIdentity(token graphFeedToken, request string) graphFeedRetainedIdentity {
	return graphFeedRetainedIdentity{strings.Clone(request), feedIdentity(token.Secret, "retained-reader/"+request), token.Original}
}
func (s *graphFeedStore) retainedReaderIn(tr fdb.ReadTransaction, token graphFeedToken, id graphFeedRetainedIdentity) (graphFeedRetainedRecord, error) {
	var zero graphFeedRetainedRecord
	if s == nil || s.binding.Retention == nil || !validName(id.Request) || id != retainedFeedIdentity(token, id.Request) {
		return zero, errMismatch
	}
	if _, err := s.originalIn(tr, token); err != nil {
		return zero, err
	}
	r, found, err := feedGet[graphFeedRetainedRecord](tr, s.key("retained-reader/"+token.Request, id.Request))
	if err != nil {
		return r, err
	}
	if !found {
		return r, errUnavailable
	}
	p := s.binding.Retention.Transfer
	if r.Version != 1 || r.Identity != id || r.State > 2 || r.State == 2 && r.Cleanup.Calls == 0 || r.Cleanup.Request != id.Request || r.Cleanup.Calls > p.MaxCleanupCalls || r.Cleanup.DeadlineUnixNanos < 0 || r.Cleanup.DeadlineUnixNanos > s.feedCleanupCeiling(token.DeadlineNanos) || (r.Cleanup.Calls == 0) != (r.Cleanup.DeadlineUnixNanos == 0) {
		return r, errMismatch
	}
	return r, nil
}
func (s *graphFeedStore) OpenRetained(ctx context.Context, token graphFeedToken, request string) (*graphFeed, error) {
	if s == nil || s.binding.Retention == nil || ctx == nil || !validName(request) || !validName(token.Request) {
		return nil, errInvalid
	}
	ctx, cancel, err := s.feedRunContext(ctx, token)
	if err != nil {
		return nil, err
	}
	defer cancel()
	token.Request = strings.Clone(token.Request)
	if err := s.reserve(ctx, token, "retained-open/"+request, "retained-open", token); err != nil {
		return nil, err
	}
	id := retainedFeedIdentity(token, request)
	o, _, err := historyTransactContext(ctx, s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphFeedOriginal, error) {
		var zero graphFeedOriginal
		ok, err := s.isRetainedOnlyIn(tr, token)
		if err != nil {
			return zero, err
		}
		if !ok {
			return zero, errHistoryInUse
		}
		c, err := s.cursorIn(tr, token)
		if err != nil {
			return zero, err
		}
		if c.State != graphFeedActive {
			return zero, errHistoryExpired
		}
		key := s.key("retained-reader/"+token.Request, request)
		_, found, err := feedGet[graphFeedRetainedRecord](tr, key)
		if err != nil {
			return zero, err
		}
		if found {
			r, err := s.retainedReaderIn(tr, token, id)
			if err != nil {
				return zero, err
			}
			if r.State == 2 || r.Cleanup.Calls != 0 {
				return zero, errHistoryExpired
			}
		} else {
			ledger, found, err := feedGet[graphFeedLedger](tr, s.key("meta", "ledger"))
			if err != nil {
				return zero, err
			}
			if !found {
				return zero, errUnavailable
			}
			if ledger.CleanupTickets >= 64 {
				return zero, errLimit
			}
			r := graphFeedRetainedRecord{Version: 1, Identity: id, Cleanup: graphOwnerCleanup{Request: request}}
			if err := s.createIn(tr, key, r, uint64(len(key))+graphFeedControlBytes); err != nil {
				return zero, err
			}
			ledger, _, err = feedGet[graphFeedLedger](tr, s.key("meta", "ledger"))
			if err != nil {
				return zero, err
			}
			ledger.CleanupTickets++
			if err := put(tr, s.key("meta", "ledger"), ledger); err != nil {
				return zero, err
			}
		}
		return s.originalIn(tr, token)
	})
	if err != nil {
		return nil, err
	} // Caller still knows request for CloseRetained.
	r := &graphFeedRetainedReader{s: s, token: token, id: id}
	_, _, err = historyTransactContext(ctx, s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		ok, err := s.isRetainedOnlyIn(tr, token)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, errHistoryInUse
		}
		c, err := s.cursorIn(tr, token)
		if err != nil {
			return false, err
		}
		if c.State != graphFeedActive {
			return false, errHistoryExpired
		}
		stored, err := s.retainedReaderIn(tr, token, id)
		if err != nil {
			return false, err
		}
		if stored.State == 2 || stored.Cleanup.Calls != 0 {
			return false, errHistoryExpired
		}
		stored.State = 1
		return true, put(tr, s.key("retained-reader/"+token.Request, request), stored)
	})
	if err != nil {
		return nil, errors.Join(err, r.close())
	}
	_, _, err = graphCostParallelRead(ctx, s.source.h.db, func(tr fdb.ReadTransaction) (bool, error) { return true, r.checkIn(tr) })
	if err != nil {
		return nil, errors.Join(err, r.close())
	}
	return &graphFeed{s: s, token: token, original: o, retained: r}, nil
}
func (f *graphFeed) checkReaderIn(tr fdb.ReadTransaction) error {
	if f.retained != nil {
		return f.retained.checkIn(tr)
	}
	return f.reader.check(tr)
}
func (r *graphFeedRetainedReader) checkIn(tr fdb.ReadTransaction) error {
	ok, err := r.s.isRetainedOnlyIn(tr, r.token)
	if err != nil {
		return err
	}
	if !ok {
		return errHistoryExpired
	}
	actual, err := r.s.retainedReaderIn(tr, r.token, r.id)
	if err != nil {
		return err
	}
	if actual.State != 1 || actual.Cleanup.Calls != 0 {
		return errHistoryExpired
	}
	return nil
}
func (r *graphFeedRetainedReader) enter() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.closing {
		return errHistoryExpired
	}
	if r.busy {
		return errHistoryInUse
	}
	r.busy = true
	return nil
}
func (r *graphFeedRetainedReader) leave() { r.mu.Lock(); r.busy = false; r.mu.Unlock() }
func (r *graphFeedRetainedReader) work() graphSnapshotWork {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.spent
}
func (s *graphFeedStore) CloseRetained(ctx context.Context, token graphFeedToken, request string) (graphSnapshotWork, error) {
	if s == nil || s.binding.Retention == nil || ctx == nil || !validName(request) || !validName(token.Request) {
		return graphSnapshotWork{}, errInvalid
	}
	token.Request = strings.Clone(token.Request)
	r := &graphFeedRetainedReader{s: s, token: token, id: retainedFeedIdentity(token, request)}
	err := r.closeWith(ctx)
	return r.work(), err
}
func (r *graphFeedRetainedReader) close() error { return r.closeWith(context.Background()) }
func (r *graphFeedRetainedReader) closeWith(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	if r.busy {
		return &graphFeedRetainedPending{r, errHistoryInUse}
	}
	r.closing = true
	bounded, cancel, err := r.s.feedCleanupContext(ctx, r.token)
	if err != nil {
		return &graphFeedRetainedPending{r, err}
	}
	defer cancel()
	ticket, _, err := historyTransactContext(bounded, r.s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphFeedRetainedRecord, error) {
		x, err := r.s.retainedReaderIn(tr, r.token, r.id)
		if err != nil {
			return x, err
		}
		if x.State == 2 {
			return x, nil
		}
		x.Cleanup, err = r.s.binding.Retention.Transfer.cleanupDebit(x.Cleanup, time.Now().UnixNano())
		if err != nil {
			return x, err
		}
		return x, put(tr, r.s.key("retained-reader/"+r.token.Request, r.id.Request), x)
	})
	// This is durable admission returned by a committed native call. Failed
	// admission callbacks are not falsely reported as installed credit.
	if err != nil {
		return &graphFeedRetainedPending{r, err}
	}
	r.spent = graphSnapshotWork{CleanupRows: ticket.Cleanup.Calls * 1024, CleanupBytes: ticket.Cleanup.Calls * (64 << 20)}
	if ticket.State == 2 {
		r.closed = true
		return nil
	}
	terminal, stop := context.WithDeadline(bounded, time.Unix(0, ticket.Cleanup.DeadlineUnixNanos))
	defer stop()
	_, _, err = historyTransactContext(terminal, r.s.source.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		x, err := r.s.retainedReaderIn(tr, r.token, r.id)
		if err != nil {
			return false, err
		}
		if x.Cleanup.DeadlineUnixNanos != ticket.Cleanup.DeadlineUnixNanos || x.Cleanup.Calls < ticket.Cleanup.Calls {
			return false, errMismatch
		}
		// State0 closure fences an uncertain concurrent acquisition on THIS same
		// durable record; no absence/death inference and no source owner is minted.
		x.State = 2
		return true, put(tr, r.s.key("retained-reader/"+r.token.Request, r.id.Request), x)
	})
	if err != nil {
		return &graphFeedRetainedPending{r, err}
	}
	r.closed = true
	return nil
}
