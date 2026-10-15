package graphapply

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeColumnsMarker = "GCLS1"

// Actual persistent cache bytes are bounded independently of prepaid work;
// cache capacity never allocates a buffer of this size or certifies a source.
const nativeColumnsCacheBytes uint64 = 1 << 40

// Publication, native/cache-fill/cached reads, a post-mutation old-cut read,
// and native/cached physical-restart reads each retire a complete prefix grant.
const nativeColumnsPasses uint64 = 7
const nativeColumnsInvocationAttempts uint64 = nativeGraphCostMaxAttempts

// This grant proof belongs ONLY to the current contiguous retain-all source
// contract: every prefix index has genuine Root+Outcome+CDC records. Sparse AS7
// or GC cannot adopt absolute Through as a retained-frame count. This is prepaid
// theoretical accounting, never allocation, RSS, emitted bytes or measured work.
func nativeGraphCostColumnsPolicy(application raftlog.ApplicationPolicy, l materializerLimits) (nativeColumnPolicy, error) {
	p := nativeColumnPolicy{ChunkRecords: 16, ChunkRows: 2, ChunkBytes: 64 << 10, FrameBytes: 32 << 10, MaxGenerations: 2, MaxChunks: 1 << 40, MaxRetainedBytes: 1 << 62, MaxWorkRecords: ^uint64(0), MaxWorkBytes: ^uint64(0), MaxDecodedRows: ^uint64(0), MaxOutputBytes: ^uint64(0), MaxRPCs: ^uint64(0), MaxWireBytes: ^uint64(0), CacheBytes: 2 << 20, MemTableBytes: 1 << 20, Lifetime: time.Minute}
	opening, grant, err := nativeColumnLimitAllowance(application, l, p)
	if err != nil {
		return p, err
	}
	opening, err = opening.scaled(2)
	if err != nil {
		return p, err
	}
	opening, err = nativeColumnPeerOpening(opening, l)
	if err != nil {
		return p, err
	}
	frames := application.RetainedApplicationRecords / 3
	if frames == 0 || frames >= 1<<40 || !nativeColumnAdd(frames, 1) {
		return p, errLimit
	}
	// At least one committed frame per nonempty page. EOF, discarded lookahead,
	// and four attempts are paid even if a lost/canceled invocation never returns.
	invocations := frames + 1
	if invocations > ^uint64(0)/(nativeColumnsPasses*nativeColumnsInvocationAttempts) {
		return p, errLimit
	}
	invocations *= nativeColumnsPasses * nativeColumnsInvocationAttempts
	pages, err := grant.scaled(invocations)
	if err != nil {
		return p, err
	}
	// Constructor ownership adoption, full At authentication and local seal are
	// funded separately, including physical restart reacquisition of original At.
	captures, err := opening.scaled(4 * nativeColumnsPasses * nativeColumnsInvocationAttempts)
	if err != nil {
		return p, err
	}
	budget, err := pages.plus(captures)
	if err != nil {
		return p, err
	}
	p.MaxWorkRecords, p.MaxWorkBytes, p.MaxDecodedRows, p.MaxOutputBytes, p.MaxRPCs, p.MaxWireBytes = budget.Records, budget.Bytes, budget.DecodedRows, budget.OutputBytes, budget.RPCs, budget.WireBytes
	p.MaxChunks = 2 * frames
	perChunk := uint64(p.ChunkBytes + 128)
	if frames > ((1<<62)-8*nativeColumnMetadataBytes)/(2*perChunk) {
		return p, errLimit
	}
	p.MaxRetainedBytes = 8*nativeColumnMetadataBytes + 2*frames*perChunk
	if err := p.validate(); err != nil {
		return p, err
	}
	if !opening.admit(grant, p) {
		return p, errLimit
	}
	return p, nil
}

const (
	nativeColumnsOpen byte = iota + 1
	nativeColumnsPublish
	nativeColumnsRead
	nativeColumnsNext
	nativeColumnsClose
	nativeColumnsRetire
	nativeColumnsRecoverRetire
	nativeColumnsPinProbe
)

type nativeGraphCostColumnCookie struct {
	Owner   uint64
	Request [16]byte
}
type nativeGraphCostColumnRequest struct {
	Action               byte
	Initial, Final, At   []byte
	Generation           [16]byte
	Create, Cached, Fill bool
	Cookie               nativeGraphCostColumnCookie
	Sequence             uint64
	RetainedExpires      uint64 `json:",omitzero"` // explicit fresh durable profile; zero preserves legacy requests
	RetainedToken        []byte `json:",omitzero"` // only the explicit durable token probe
}
type nativeGraphCostColumnReceipt struct {
	Cookie                          nativeGraphCostColumnCookie
	Sequence                        uint64
	Binding                         nativeColumnBinding
	Manifest                        nativeColumnManifest
	Ordinal                         uint64
	Body                            []byte
	Rows                            []nativeColumnRow
	EOF, Closed                     bool
	Cached, Fill                    bool
	Charged, DurableWork            nativeColumnWork
	Ledger                          nativeColumnLedger
	CacheLedger                     nativeColumnCacheLedger
	RPCs, RequestBytes, ReplyBytes  uint64
	DeadlineUnixNanos, ElapsedNanos int64
	Accounting                      string
	Retention                       *nativeColumnProcessRetentionReceipt `json:",omitzero"`
	SourceStarts, SourceExpirations uint64                               `json:",omitzero"`
	SourceDeadlineUnixNanos         int64                                `json:",omitzero"`
}
type nativeGraphCostColumnOwner struct {
	mu                                            sync.Mutex
	gate                                          graphRequestGate
	source                                        *nativeColumnSource
	store                                         *nativeColumnStore
	cache                                         *nativeColumnCache
	cursor                                        *nativeColumnReadCursor
	generation                                    [16]byte
	cacheDir                                      string
	cookie                                        nativeGraphCostColumnCookie
	manifest                                      nativeColumnManifest
	fill                                          bool
	cached                                        bool
	mode                                          byte
	started                                       time.Time
	lastSequence                                  uint64
	cachedInput, cachedReply                      []byte
	replayed, closed                              bool
	cancel                                        context.CancelFunc
	closeOnce                                     sync.Once
	closeDone                                     chan struct{}
	closeErr                                      error
	terminal                                      nativeGraphCostColumnReceipt
	ctx                                           context.Context // fresh fixed whole-query owner; source TTL cannot extend it
	coordinator                                   *crossProcessCoordinator
	retired                                       nativeColumnWork
	retiredCalls, retiredRequests, retiredReplies uint64
	starts, expirations, failedStarts             uint64
	startsLimit                                   uint64
}
type nativeGraphCostColumnSessions struct {
	mu          sync.Mutex
	ctx         context.Context
	coordinator *crossProcessCoordinator
	dir         string
	spec        *nativeGraphCostSpec
	next        uint64
	probes      uint8 // fresh diagnostic attempts only; ordinary/default paths never spend it
	openProbes  uint8 // fresh lost-Open observations; independent of terminal diagnostics
	pending     bool
	owner       *nativeGraphCostColumnOwner
	// Installed once before worker publication. The fresh bootstrap authenticates
	// its actual Host/profile; nil preserves the compact diagnostic policy.
	policyProvider func() (nativeColumnPolicy, error)
}

func (s *nativeGraphCostColumnSessions) policy() (nativeColumnPolicy, error) {
	if s == nil || s.coordinator == nil || s.coordinator.host == nil {
		return nativeColumnPolicy{}, errInvalid
	}
	if s.policyProvider != nil {
		return s.policyProvider()
	}
	return nativeColumnProcessPolicy(s.coordinator.host)
}

// A lost constructor reply can observe ONLY this actual same child owner.
// Caller recipes provide equality questions, never ownership or absence proof.
// Both fresh workload families share this exact lifetime/keeper audit.
func (s *nativeGraphCostColumnSessions) observeOpen(ctx context.Context, body []byte) ([]byte, error) {
	if s == nil || ctx == nil {
		return nil, errInvalid
	}
	policy, err := s.policy()
	if err != nil || policy.Version != 1 || policy.Admission == ([32]byte{}) {
		return nil, errors.Join(errInvalid, err)
	}
	q, err := nativeGraphCostColumnDecode(processRequest{data: body})
	if err != nil || q.Action != nativeColumnsOpen || q.RetainedExpires == 0 {
		return nil, errors.Join(errInvalid, err)
	}
	canonical, err := nativeGraphCostColumnInput(q)
	if err != nil || !bytes.Equal(canonical, body) {
		return nil, errors.Join(errInvalid, err)
	}
	s.mu.Lock()
	if s.openProbes >= 4 {
		s.mu.Unlock()
		return nil, errLimit
	}
	s.openProbes++
	owner, pending := s.owner, s.pending
	s.mu.Unlock()
	if owner == nil || pending {
		return nil, errCrossProcessPending
	}
	if err := owner.gate.lock(ctx); err != nil {
		return nil, err
	}
	defer owner.gate.unlock()
	if owner.generation != q.Generation || owner.store == nil || owner.store.binding.Policy != policy {
		return nil, errInvalid
	}
	if owner.source == nil {
		if err := owner.renew(ctx); err != nil {
			return nil, err
		}
	}
	initial, err := nativeGraphCostColumnAt(q.Initial)
	if err != nil {
		return nil, err
	}
	final, err := nativeGraphCostColumnAt(q.Final)
	if err != nil {
		return nil, err
	}
	first, err := owner.source.cutPair(initial)
	if err != nil {
		return nil, err
	}
	last, err := owner.source.cutPair(final)
	if err != nil || first != owner.source.initial || last != owner.source.final {
		return nil, errors.Join(errInvalid, err)
	}
	r, err := owner.store.retentionRecord(q.Generation)
	if err != nil {
		return nil, err
	}
	m, err := decodeGraphPinManifest(r.Original)
	if err != nil || m.expires != q.RetainedExpires {
		return nil, errors.Join(errInvalid, err)
	}
	if err := errors.Join(ctx.Err(), owner.ctx.Err()); err != nil {
		return nil, err
	}
	receipt, err := owner.receipt(0)
	if err != nil {
		return nil, err
	}
	return nativeGraphCostColumnEncode(receipt)
}

func nativeGraphCostColumnInput(q nativeGraphCostColumnRequest) ([]byte, error) {
	body, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	if len(body) > 16<<10 {
		return nil, errLimit
	}
	return append([]byte(nativeColumnsMarker), body...), nil
}
func nativeGraphCostColumnDecode(r processRequest) (q nativeGraphCostColumnRequest, err error) {
	if r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 || !bytes.HasPrefix(r.data, []byte(nativeColumnsMarker)) || len(r.data) > 16<<10+len(nativeColumnsMarker) {
		return q, errInvalid
	}
	d := json.NewDecoder(bytes.NewReader(r.data[len(nativeColumnsMarker):]))
	d.DisallowUnknownFields()
	if err = d.Decode(&q); err != nil {
		return q, errors.Join(errInvalid, err)
	}
	if err = d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return q, errors.Join(errInvalid, err)
	}
	if q.Action == nativeColumnsOpen {
		if len(q.Initial) == 0 || len(q.Final) == 0 || len(q.At)+len(q.RetainedToken) != 0 || q.Generation == ([16]byte{}) || q.Cookie != (nativeGraphCostColumnCookie{}) || q.Sequence != 0 || q.Cached || q.Fill {
			return q, errInvalid
		}
	} else if q.Action == nativeColumnsPinProbe {
		if q.Generation == ([16]byte{}) || q.Cookie != (nativeGraphCostColumnCookie{}) || q.Sequence != 0 || len(q.Initial)+len(q.Final)+len(q.At) != 0 || len(q.RetainedToken) == 0 || len(q.RetainedToken) > nativeColumnMetadataBytes || q.RetainedExpires != 0 || q.Create || q.Cached || q.Fill {
			return q, errInvalid
		}
	} else if q.Action == nativeColumnsRecoverRetire {
		if q.Generation == ([16]byte{}) || q.Cookie != (nativeGraphCostColumnCookie{}) || q.Sequence != 0 || len(q.Initial)+len(q.Final)+len(q.At)+len(q.RetainedToken) != 0 || q.RetainedExpires != 0 || q.Create || q.Cached || q.Fill {
			return q, errInvalid
		}
	} else {
		if q.RetainedExpires != 0 || q.Cookie.Owner == 0 || q.Cookie.Request == ([16]byte{}) || len(q.Initial)+len(q.Final)+len(q.RetainedToken) != 0 || q.Generation != ([16]byte{}) || q.Create || q.Action < nativeColumnsPublish || q.Action > nativeColumnsRetire {
			return q, errInvalid
		}
		if q.Action == nativeColumnsClose || q.Action == nativeColumnsRetire {
			if q.Sequence != 0 {
				return q, errInvalid
			}
		} else if q.Sequence == 0 {
			return q, errInvalid
		}
		if q.Action == nativeColumnsRead {
			if len(q.At) == 0 || q.Cached && q.Fill {
				return q, errInvalid
			}
		} else if len(q.At) != 0 || q.Cached || q.Fill {
			return q, errInvalid
		}
	}
	return q, nil
}
func nativeGraphCostColumnCloseControl(r processRequest) bool {
	if r.op != processCostObserve {
		return false
	}
	q, err := nativeGraphCostColumnDecode(r)
	// Durable retirement orchestrates ordinary observations and nested native
	// Close/Cancel RPCs, so it must not occupy that same reserved control slot.
	// Busy retirement refuses admission; transient Close frees its owner first.
	return err == nil && q.Action == nativeColumnsClose
}
func nativeGraphCostColumnAt(body []byte) (graphCut, error) {
	q, err := decodeGraphE15ReadRequest(body)
	if err != nil || q.mode != graphReadAt || len(q.cut.certificates) != 2 || q.transaction != ([16]byte{}) || q.coordinator != 0 {
		return graphCut{}, errors.Join(errInvalid, err)
	}
	return q.cut, nil
}
func nativeGraphCostColumnEncode(r nativeGraphCostColumnReceipt) ([]byte, error) {
	body, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(body) > nativeGraphCostPageBytes {
		return nil, errLimit
	}
	return body, nil
}
func nativeGraphCostColumnDecodeReceipt(body []byte) (r nativeGraphCostColumnReceipt, err error) {
	if len(body) > nativeGraphCostPageBytes {
		return r, errLimit
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err = d.Decode(&r); err != nil {
		return r, errors.Join(errCorrupt, err)
	}
	if err = d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return r, errors.Join(errCorrupt, err)
	}
	if r.Cookie.Owner == 0 || r.Cookie.Request == ([16]byte{}) || r.Binding.Coverage == 0 || r.Binding.Coverage > 2 || r.Binding.Policy.validate() != nil || len(r.Body) > r.Binding.Policy.ChunkBytes || len(r.Rows) > r.Binding.Policy.ChunkRows || r.DeadlineUnixNanos <= 0 || r.ElapsedNanos < 0 || r.RPCs > r.Charged.RPCs || r.RequestBytes > r.Charged.WireBytes || r.ReplyBytes > r.Charged.WireBytes-r.RequestBytes || r.EOF && r.Manifest.Generation.ID == ([16]byte{}) || r.Closed && (r.EOF || len(r.Body)+len(r.Rows) != 0) {
		return r, errCorrupt
	}
	cacheMaximum := nativeColumnsCacheBytes
	if r.Binding.Policy.Version == 1 {
		cacheMaximum = r.Binding.Policy.MaxRetainedBytes
	}
	if r.Cached && r.Fill || (r.Cached || r.Fill) && r.CacheLedger.Version != 1 || r.CacheLedger.Version != 0 && (r.CacheLedger.Version != 1 || r.CacheLedger.Maximum != cacheMaximum || r.CacheLedger.Charged < 2*nativeColumnMetadataBytes || r.CacheLedger.Charged > r.CacheLedger.Maximum) {
		return r, errCorrupt
	}
	if r.Binding.Policy.Version == 0 {
		if r.SourceStarts != 0 || r.SourceExpirations != 0 || r.SourceDeadlineUnixNanos != 0 {
			return r, errCorrupt
		}
	} else if r.SourceStarts == 0 || r.SourceStarts > nativeContentionOwnerStarts || r.SourceExpirations > r.SourceStarts || !r.Closed && (r.SourceExpirations == r.SourceStarts || r.SourceDeadlineUnixNanos <= 0) {
		return r, errCorrupt
	}
	if r.Binding.Version == 2 {
		if r.Retention == nil || r.Retention.validate(false) != nil || r.Binding.TerminalAttempts != nativeColumnTerminalAttempts {
			return r, errCorrupt
		}
	} else if r.Retention != nil {
		return r, errCorrupt
	}
	return r, nil
}
func (o *nativeGraphCostColumnOwner) receipt(sequence uint64) (nativeGraphCostColumnReceipt, error) {
	charged := o.retired
	calls, request, reply := o.retiredCalls, o.retiredRequests, o.retiredReplies
	var deadline, sourceDeadline time.Time
	if o.source != nil {
		o.source.mu.Lock()
		current := o.source.charged
		o.source.mu.Unlock()
		var err error
		charged, err = charged.plus(current)
		if err != nil {
			return nativeGraphCostColumnReceipt{}, err
		}
		c, q, r := o.source.meter.snapshot()
		if !nativeColumnAdd(calls, c) || !nativeColumnAdd(request, q) || !nativeColumnAdd(reply, r) {
			return nativeGraphCostColumnReceipt{}, errLimit
		}
		calls += c
		request += q
		reply += r
		deadline, _ = o.source.ctx.Deadline()
		sourceDeadline = deadline
	}
	if o.store.binding.Policy.Version == 1 {
		deadline, _ = o.ctx.Deadline()
	}
	p, err := o.store.progress(o.generation)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return nativeGraphCostColumnReceipt{}, err
	}
	ledger, err := o.store.ledger()
	if err != nil {
		return nativeGraphCostColumnReceipt{}, err
	}
	var cached nativeColumnCacheLedger
	if o.cache != nil {
		o.cache.mu.Lock()
		cached = o.cache.ledger
		o.cache.mu.Unlock()
	}
	r := nativeGraphCostColumnReceipt{Cookie: o.cookie, Sequence: sequence, Binding: o.store.binding, Manifest: o.manifest, Cached: o.cached, Fill: o.fill, Charged: charged, DurableWork: p.Work, Ledger: ledger, CacheLedger: cached, RPCs: calls, RequestBytes: request, ReplyBytes: reply, DeadlineUnixNanos: deadline.UnixNano(), ElapsedNanos: time.Since(o.started).Nanoseconds(), Accounting: "Prepaid theoretical source/decode/output/RPC/wire quotas, including retired failed attempts; not actual bytes/RSS. Actual authenticated peer calls and received frames include failure and Close. Direct local native original reads are not peer RPCs. Only accepted sealed source EOF covers this declared local mask."}
	if o.store.binding.Policy.Version == 1 {
		r.SourceStarts, r.SourceExpirations = o.starts, o.expirations
		if !sourceDeadline.IsZero() {
			r.SourceDeadlineUnixNanos = sourceDeadline.UnixNano()
		}
	}
	if o.store.binding.Version == 2 {
		retention, err := nativeColumnProcessRetention(o.store, o.generation)
		if err != nil {
			return r, err
		}
		r.Retention = &retention
		r.Accounting += " Fresh durable-pin publication/retirement/probes use separately prepaid native invocations in the same durable generation ledger; their peer calls are not the transient source meter. This small profile proves no comparative cost."
	}
	return r, nil
}
func (s *nativeGraphCostColumnSessions) open(ctx context.Context, q nativeGraphCostColumnRequest) (body []byte, err error) {
	initial, err := nativeGraphCostColumnAt(q.Initial)
	if err != nil {
		return nil, err
	}
	final, err := nativeGraphCostColumnAt(q.Final)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.owner != nil {
		select {
		case <-s.owner.closeDone:
			s.owner.mu.Lock()
			retired := s.owner.closed && s.owner.closeErr == nil
			s.owner.mu.Unlock()
			if retired {
				s.owner = nil
			}
		default:
		}
	}
	if s.owner != nil || s.pending || s.next == ^uint64(0) {
		s.mu.Unlock()
		return nil, errLimit
	}
	s.pending = true
	s.next++
	number := s.next
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.pending = false; s.mu.Unlock() }()
	h := s.coordinator.host
	var policy nativeColumnPolicy
	if q.RetainedExpires != 0 {
		policy, err = s.policy()
	} else if durableColumnGraphAgreement(h.machine.SemanticContractID()) {
		return nil, errInvalid // the fresh profile never silently creates a v1 owner
	} else {
		policy, err = nativeGraphCostColumnsPolicy(h.machine.store.ApplicationLimits(), h.machine.limits)
	}
	if err != nil {
		return nil, err
	}
	if policy.Version == 1 {
		if _, bounded := s.ctx.Deadline(); !bounded {
			return nil, errInvalid
		}
	}
	ownerCtx, cancel := context.WithCancel(s.ctx)
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	started := time.Now()
	var source *nativeColumnSource
	var store *nativeColumnStore
	if q.RetainedExpires == 0 {
		source, err = newPartitionNativeColumnSource(ownerCtx, s.coordinator, initial, final, policy)
		if err != nil {
			cancel()
			return nil, err
		}
	}
	storeDir := filepath.Join(s.dir, "native-columns")
	if q.RetainedExpires != 0 {
		storeDir = filepath.Join(storeDir, hex.EncodeToString(q.Generation[:]))
	}
	_, statErr := os.Stat(storeDir)
	create := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !create || q.Create && !create {
		cancel()
		if source != nil {
			return nil, errors.Join(errInvalid, statErr, source.Close())
		}
		return nil, errors.Join(errInvalid, statErr)
	}
	if q.RetainedExpires != 0 {
		if create {
			store, source, err = createDurablePartitionNativeColumns(ownerCtx, storeDir, vfs.Default, s.coordinator, q.Generation, initial, final, policy, q.RetainedExpires)
		} else {
			store, source, err = reopenDurablePartitionNativeColumns(ownerCtx, storeDir, vfs.Default, s.coordinator, q.Generation, policy)
		}
		if err == nil {
			r, e := store.retentionRecord(q.Generation)
			if e != nil {
				err = e
			} else {
				m, e := decodeGraphPinManifest(r.Original)
				first, e1 := source.cutPair(initial)
				last, e2 := source.cutPair(final)
				if e != nil || e1 != nil || e2 != nil || m.expires != q.RetainedExpires || first != source.initial || last != source.final {
					err = errors.Join(errInvalid, e, e1, e2)
				}
			}
		}
	} else {
		store, err = openNativeColumnStore(storeDir, vfs.Default, create, source)
	}
	if err != nil {
		cancel()
		if source != nil {
			err = errors.Join(err, source.Close())
		}
		if store != nil {
			err = errors.Join(err, store.Close())
		}
		return nil, err
	}
	o := &nativeGraphCostColumnOwner{gate: newGraphRequestGate(), source: source, store: store, generation: q.Generation, cacheDir: filepath.Join(s.dir, "native-column-cache"), cookie: nativeGraphCostColumnCookie{number, source.owner}, started: started, cancel: cancel, closeDone: make(chan struct{}), ctx: ownerCtx, coordinator: s.coordinator}
	if policy.Version == 1 {
		o.starts = 1
		deadline, _ := ownerCtx.Deadline()
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > nativeContentionWholeSeconds*time.Second {
			cancel()
			return nil, errors.Join(errInvalid, source.Close(), store.Close())
		}
		// This owner's fixed remaining deadline is no later than the immutable
		// admitted run deadline. Source TTL renewals cannot restart that budget.
		o.startsLimit = uint64(remaining/policy.Lifetime) + 13 // #nosec G115 -- positive admitted duration.
	}
	defer func() {
		if err != nil {
			cancel()
			err = errors.Join(err, o.close(context.Background()))
		}
	}()
	// Opening an existing generation checks identity; a caller cannot select an
	// arbitrary manifest/cached summary instead of this original native source.
	if _, e := store.existing(q.Generation, source); e != nil && !errors.Is(e, errNativeColumnIncomplete) {
		return nil, e
	}
	receipt, err := o.receipt(0)
	if err != nil {
		return nil, err
	}
	body, err = nativeGraphCostColumnEncode(receipt)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	err = errors.Join(ctx.Err(), s.ctx.Err(), source.ctx.Err())
	if err == nil {
		s.owner = o
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	// Deadline cleanup owns exactly one asynchronous join. Publication occurs
	// before this callback can retire resources; no constructor assignments race.
	expiry := source.ctx
	if policy.Version == 1 {
		expiry = ownerCtx
	}
	context.AfterFunc(expiry, func() {
		cleanup, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		_ = o.close(cleanup)
	})
	return body, nil
}
func (s *nativeGraphCostColumnSessions) serve(ctx context.Context, r processRequest) ([]byte, error) {
	if s == nil || s.ctx == nil || ctx == nil || s.coordinator == nil || s.spec == nil || s.dir == "" {
		return nil, errInvalid
	}
	q, err := nativeGraphCostColumnDecode(r)
	if err != nil {
		return nil, err
	}
	if err := errors.Join(ctx.Err(), s.ctx.Err()); err != nil {
		return nil, err
	}
	if q.Action == nativeColumnsOpen {
		return s.open(ctx, q)
	}
	if q.Action == nativeColumnsRecoverRetire {
		return s.recoverRetirement(ctx, q.Generation)
	}
	if q.Action == nativeColumnsPinProbe {
		return s.probeOriginalPin(ctx, q.Generation, q.RetainedToken)
	}
	s.mu.Lock()
	o := s.owner
	s.mu.Unlock()
	if o == nil || o.cookie != q.Cookie {
		return nil, errInvalid
	}
	if q.Action == nativeColumnsRetire {
		if o.store.binding.Version != 2 {
			return nil, errInvalid
		}
		if err := o.gate.lock(ctx); err != nil {
			return nil, err
		}
		o.cancel()
		var err error
		if o.store.binding.Policy.Version == 1 {
			err = o.retireSource()
		} else if o.source != nil {
			err = o.source.Close()
		}
		if err == nil {
			p, e := newRetainedPinPublisher(s.coordinator)
			if e != nil {
				err = e
			} else {
				err = o.store.Retire(ctx, o.generation, p)
			}
		}
		o.gate.unlock()
		if err != nil {
			return nil, err
		}
		q.Action = nativeColumnsClose
	}
	if q.Action == nativeColumnsClose {
		if err := o.close(ctx); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.owner == o {
			s.owner = nil
		}
		s.mu.Unlock()
		o.mu.Lock()
		receipt := o.terminal
		o.mu.Unlock()
		return nativeGraphCostColumnEncode(receipt)
	}
	return o.next(ctx, q)
}

// The query gate owns this transition. Expiry retires only a transient genuine
// source; it cannot replace the query cookie, cursor, original cuts or ledger.
func (o *nativeGraphCostColumnOwner) renew(ctx context.Context) error {
	if o.store.binding.Policy.Version != 1 {
		return nil
	}
	if err := errors.Join(ctx.Err(), o.ctx.Err()); err != nil {
		return err
	}
	if o.source != nil && o.source.ctx.Err() == nil {
		return nil
	}
	if o.source != nil {
		deadline, bounded := o.source.ctx.Deadline()
		whole, fixed := o.ctx.Deadline()
		if !bounded || !fixed || !o.source.fullTTL || !deadline.Before(whole) || !errors.Is(o.source.ctx.Err(), context.DeadlineExceeded) {
			return errCrossProcessPending
		}
		if err := o.retireSource(); err != nil {
			return err
		}
		o.expirations++
	}
	if o.starts >= o.startsLimit || o.failedStarts >= 4 {
		return errLimit
	}
	o.starts++
	call, cancel := context.WithCancel(o.ctx)
	stop := context.AfterFunc(ctx, cancel)
	_, source, err := reopenDurablePartitionNativeColumnsStore(call, o.store, o.coordinator, o.generation, o.store.binding.Policy, false)
	if err == nil && o.cursor != nil {
		// These are the actual private cursor's certificates, never caller bytes.
		// Reauthenticate its predicate at the SAME genuine replacement source.
		var progress nativeColumnProgress
		progress, err = o.store.existing(o.generation, source)
		if err == nil {
			err = o.store.reserveWork(&progress, source, source.openWork)
		}
		if err == nil {
			var pair [2]graphCertificate
			pair, err = source.at(call, graphCut{certificates: []graphCertificate{o.cursor.at[0], o.cursor.at[1]}})
			if err == nil && pair != o.cursor.at {
				err = errInvalid
			}
		}
	}
	stop()
	if err == nil {
		err = errors.Join(ctx.Err(), o.ctx.Err(), source.ctx.Err())
	}
	if err != nil {
		failure, hadFailure := errors.AsType[*nativeColumnCreationFailure](err)
		if !hadFailure && source != nil {
			// The replacement may finish capture and then expire during SAME-cut
			// cursor authentication. Classify its actual context before canceling;
			// a shorter request timeout cannot buy another full source TTL.
			failure = nativeColumnCreationInterruption(call, source)
		}
		cancel()
		if failure != nil && failure.FullTTL && failure.Expired && failure.DeadlineUnixNanos-failure.IssuedUnixNanos == int64(o.store.binding.Policy.Lifetime) {
			o.expirations++
		} else {
			o.failedStarts++
		}
		if source != nil {
			// Attach only for uniform, once-only metering/cleanup. No cursor or
			// sequence changes until the entire native replacement succeeds.
			o.source = source
			err = errors.Join(err, o.retireSource())
			if failure != nil && !hadFailure {
				source.mu.Lock()
				work := source.charged
				source.mu.Unlock()
				calls, request, reply := source.meter.snapshot()
				failure.Failure = &nativeColumnSourceFailure{Charged: work, RPCs: calls, RequestBytes: request, ReplyBytes: reply, Cause: err}
				err = failure
			}
		} else if failed, ok := errors.AsType[*nativeColumnSourceFailure](err); ok {
			var sumErr error
			o.retired, sumErr = o.retired.plus(failed.Charged)
			err = errors.Join(err, sumErr)
			if !nativeColumnAdd(o.retiredCalls, failed.RPCs) || !nativeColumnAdd(o.retiredRequests, failed.RequestBytes) || !nativeColumnAdd(o.retiredReplies, failed.ReplyBytes) {
				err = errors.Join(err, errLimit)
			} else {
				o.retiredCalls += failed.RPCs
				o.retiredRequests += failed.RequestBytes
				o.retiredReplies += failed.ReplyBytes
			}
		}
		return err
	}
	// The nested capture context lives until this source actually terminates;
	// successful return does not cancel it or extend the fixed outer deadline.
	context.AfterFunc(source.ctx, cancel)
	o.source = source
	if o.cursor != nil {
		prospective := *o.cursor
		prospective.source = source
		o.cursor = &prospective
	}
	return nil
}

func (o *nativeGraphCostColumnOwner) retireSource() error {
	if o.source == nil {
		return nil
	}
	source := o.source
	err := source.Close()
	source.mu.Lock()
	work := source.charged
	source.mu.Unlock()
	combined, sumErr := o.retired.plus(work)
	if sumErr != nil {
		return errors.Join(err, sumErr)
	}
	calls, request, reply := source.meter.snapshot() // after actual cleanup
	if !nativeColumnAdd(o.retiredCalls, calls) || !nativeColumnAdd(o.retiredRequests, request) || !nativeColumnAdd(o.retiredReplies, reply) {
		return errors.Join(err, errLimit)
	}
	o.retired = combined
	o.retiredCalls += calls
	o.retiredRequests += request
	o.retiredReplies += reply
	o.source = nil
	return err
}

func (o *nativeGraphCostColumnOwner) next(ctx context.Context, q nativeGraphCostColumnRequest) ([]byte, error) {
	if !o.gate.tryLock() {
		return nil, errLimit
	}
	defer o.gate.unlock()
	canonical, err := nativeGraphCostColumnInput(q)
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil, errInvalid
	}
	active := ctx.Err()
	if o.store.binding.Policy.Version == 1 {
		active = errors.Join(active, o.ctx.Err())
	} else {
		active = errors.Join(active, o.source.ctx.Err())
	}
	if active != nil {
		o.mu.Unlock()
		return nil, active
	}
	if q.Sequence == o.lastSequence && o.cachedReply != nil {
		if o.replayed || !bytes.Equal(canonical, o.cachedInput) {
			o.mu.Unlock()
			return nil, errLimit
		}
		o.replayed = true
		result := bytes.Clone(o.cachedReply)
		o.mu.Unlock()
		return result, nil
	}
	if o.lastSequence == ^uint64(0) || q.Sequence != o.lastSequence+1 {
		o.mu.Unlock()
		return nil, errInvalid
	}
	o.mu.Unlock()
	if err := o.renew(ctx); err != nil {
		return nil, err
	}
	var result []byte
	var completedCursor *nativeColumnReadCursor
	switch q.Action {
	case nativeColumnsPublish:
		if o.mode > nativeColumnsPublish {
			return nil, errInvalid
		}
		manifest, err := o.store.publishPage(ctx, o.generation, o.source)
		if err != nil {
			return nil, err
		}
		receipt, err := o.receipt(q.Sequence)
		if err != nil {
			return nil, err
		}
		receipt.Manifest, receipt.EOF = manifest, manifest.Generation.ID != ([16]byte{})
		result, err = nativeGraphCostColumnEncode(receipt)
		if err != nil {
			return nil, err
		}
		if receipt.EOF {
			o.manifest = manifest
		}
		o.mode = nativeColumnsPublish
	case nativeColumnsRead:
		at, err := nativeGraphCostColumnAt(q.At)
		if err != nil {
			return nil, err
		}
		var cache *nativeColumnCache
		if q.Cached || q.Fill {
			if o.cache == nil {
				dir := o.cacheDir
				_, err := os.Stat(dir)
				create := errors.Is(err, os.ErrNotExist)
				if err != nil && !create {
					return nil, err
				}
				if o.store.binding.Policy.Version == 1 {
					o.cache, err = openNativeColumnCacheForPolicy(dir, vfs.Default, create, o.store.binding.Policy)
				} else {
					o.cache, err = openNativeColumnCache(dir, vfs.Default, create, nativeColumnsCacheBytes)
				}
				if err != nil {
					return nil, err
				}
			}
			if q.Cached {
				cache = o.cache
			}
		}
		cursor, err := o.store.openReadCursor(ctx, o.generation, o.source, at, cache)
		if err != nil {
			return nil, err
		}
		receipt, err := o.receipt(q.Sequence)
		if err != nil {
			return nil, err
		}
		receipt.Manifest = cursor.manifest
		receipt.Cached, receipt.Fill = q.Cached, q.Fill
		result, err = nativeGraphCostColumnEncode(receipt)
		if err != nil {
			return nil, err
		}
		o.cursor, o.manifest, o.fill, o.cached, o.mode = cursor, cursor.manifest, q.Fill, q.Cached, nativeColumnsRead
	case nativeColumnsNext:
		if o.mode != nativeColumnsRead || o.cursor == nil {
			return nil, errInvalid
		}
		prospective := *o.cursor
		_, err = o.store.nextReadChunk(ctx, &prospective, func(ordinal uint64, body []byte, frames []nativeColumnFrame, eof bool) error {
			o.mu.Lock()
			closed := o.closed
			o.mu.Unlock()
			if closed {
				return context.Canceled
			}
			if o.fill {
				if eof {
					wire, err := nativeColumnMetadata(o.cursor.manifest)
					if err != nil {
						return err
					}
					if err = o.cache.put(nativeColumnCacheManifestName(o.cursor.manifest.Generation), wire); err != nil {
						return err
					}
				} else if err := o.cache.put(nativeColumnCacheChunkName(o.cursor.manifest.Generation, ordinal), body); err != nil {
					return err
				}
			}
			receipt, err := o.receipt(q.Sequence)
			if err != nil {
				return err
			}
			receipt.Ordinal, receipt.Body, receipt.EOF = ordinal, body, eof
			receipt.Rows = []nativeColumnRow{}
			for _, frame := range frames {
				group := o.source.frameGroup(frame.Partition)
				if group < 0 {
					return errCorrupt
				}
				if frame.Index <= o.cursor.at[group].sourceIndex {
					receipt.Rows = append(receipt.Rows, frame.Rows...)
				}
			}
			result, err = nativeGraphCostColumnEncode(receipt)
			return err
		})
		if err != nil {
			return nil, err
		}
		completedCursor = &prospective
	default:
		return nil, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || errors.Join(ctx.Err(), o.source.ctx.Err()) != nil {
		return nil, errors.Join(context.Canceled, ctx.Err(), o.source.ctx.Err())
	}
	if completedCursor != nil {
		o.cursor = completedCursor
	}
	o.lastSequence, o.cachedInput, o.cachedReply, o.replayed = q.Sequence, canonical, result, false
	return bytes.Clone(result), nil
}
func (o *nativeGraphCostColumnOwner) close(ctx context.Context) error {
	o.cancel()
	o.mu.Lock()
	o.closed = true
	o.mu.Unlock()
	o.closeOnce.Do(func() {
		// One cleanup owner waits for the canceled in-flight operation. A caller's
		// finite cleanup deadline cannot abandon the actual native views/dir locks.
		go func() {
			err := o.gate.lock(context.Background())
			if err == nil {
				if o.store.binding.Policy.Version == 1 {
					err = errors.Join(err, o.retireSource())
				}
				receipt, e := o.receipt(0)
				err = errors.Join(err, e)
				receipt.Closed, receipt.EOF, receipt.Body, receipt.Rows = true, false, nil, nil
				if o.source != nil {
					err = errors.Join(err, o.source.Close())
				}
				if o.cache != nil {
					err = errors.Join(err, o.cache.Close())
				}
				err = errors.Join(err, o.store.Close())
				o.mu.Lock()
				o.terminal, o.closeErr = receipt, err
				o.mu.Unlock()
				o.gate.unlock()
			} else {
				o.mu.Lock()
				o.closeErr = err
				o.mu.Unlock()
			}
			close(o.closeDone)
		}()
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.closeDone:
		o.mu.Lock()
		defer o.mu.Unlock()
		return o.closeErr
	}
}
func (s *nativeGraphCostColumnSessions) close() error {
	if s == nil {
		return errInvalid
	}
	s.mu.Lock()
	o := s.owner
	s.mu.Unlock()
	if o == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return o.close(ctx)
}

// The parent receives actual child-pipe replies only. This trace is an embedding
// receipt, not a portable source capability; each physical restart opens a new
// genuine owner at the original certificates. No parent Host is reconstructed.
type nativeGraphCostColumnTrace struct {
	Stage                                  string
	Partition                              uint64
	Action                                 byte
	ControlRequestBytes, ControlReplyBytes uint64
	Receipt                                nativeGraphCostColumnReceipt
}
type nativeGraphCostColumnPair struct {
	Initial, Final graphCut
	ID             [16]byte
	Updated        bool
	Bindings       [2]nativeColumnBinding
	Manifests      [2]nativeColumnManifest
}
type nativeGraphCostColumnClient struct {
	t                              *testing.T
	n                              *certifiedProcessNetwork
	stream                         *json.Encoder
	stage                          string
	group                          int
	sequence                       uint64
	cookie                         nativeGraphCostColumnCookie
	binding                        nativeColumnBinding
	charged, durable               nativeColumnWork
	rpcs, requestBytes, replyBytes uint64
	deadline                       int64
}

func (c *nativeGraphCostColumnClient) call(q nativeGraphCostColumnRequest) nativeGraphCostColumnReceipt {
	c.t.Helper()
	if q.Action != nativeColumnsOpen {
		q.Cookie = c.cookie
		if q.Action != nativeColumnsClose && q.Action != nativeColumnsRetire {
			if c.sequence == ^uint64(0) {
				c.t.Fatal("column invocation overflow")
			}
			c.sequence++
			q.Sequence = c.sequence
		}
	}
	input, err := nativeGraphCostColumnInput(q)
	if err != nil {
		c.t.Fatal(err)
	}
	out := c.n.all[c.n.leaders[c.group]].call(c.t, processRequest{op: processCostObserve, data: input})
	r, err := nativeGraphCostColumnDecodeReceipt(out.aux)
	if err != nil || r.Sequence != q.Sequence || r.Binding.Coverage != 1<<c.group {
		c.t.Fatal("actual local column receipt", err, r.Sequence)
	}
	if q.Action == nativeColumnsOpen {
		c.cookie, c.binding, c.deadline = r.Cookie, r.Binding, r.DeadlineUnixNanos
	} else if r.Cookie != c.cookie || !nativeColumnSame(r.Binding, c.binding) || r.DeadlineUnixNanos != c.deadline {
		c.t.Fatal("column owner substituted source/configuration/deadline")
	}
	// Monotone source and durable ledgers are distinct; neither is actual RSS.
	if _, err := nativeColumnWorkDelta(c.charged, r.Charged); err != nil {
		c.t.Fatal(err)
	}
	if _, err := nativeColumnWorkDelta(c.durable, r.DurableWork); err != nil {
		c.t.Fatal(err)
	}
	if r.RPCs < c.rpcs || r.RequestBytes < c.requestBytes || r.ReplyBytes < c.replyBytes {
		c.t.Fatal("column transport counters refunded")
	}
	c.charged, c.durable, c.rpcs, c.requestBytes, c.replyBytes = r.Charged, r.DurableWork, r.RPCs, r.RequestBytes, r.ReplyBytes
	terminal := q.Action == nativeColumnsClose || q.Action == nativeColumnsRetire
	if terminal && !r.Closed || !terminal && r.Closed {
		c.t.Fatal("column cleanup receipt")
	}
	if err := c.stream.Encode(nativeGraphCostColumnTrace{c.stage, uint64(3 + 5*c.group), q.Action, uint64(len(input)), uint64(len(out.aux)), r}); err != nil {
		c.t.Fatal(err)
	}
	return r
}
func nativeColumnWorkDelta(before, after nativeColumnWork) (nativeColumnWork, error) {
	if after.Records < before.Records || after.Bytes < before.Bytes || after.DecodedRows < before.DecodedRows || after.OutputBytes < before.OutputBytes || after.RPCs < before.RPCs || after.WireBytes < before.WireBytes {
		return nativeColumnWork{}, errCorrupt
	}
	return nativeColumnWork{Records: after.Records - before.Records, Bytes: after.Bytes - before.Bytes, DecodedRows: after.DecodedRows - before.DecodedRows, OutputBytes: after.OutputBytes - before.OutputBytes, RPCs: after.RPCs - before.RPCs, WireBytes: after.WireBytes - before.WireBytes}, nil
}
func nativeGraphCostColumnCutInput(t *testing.T, cut graphCut) []byte {
	t.Helper()
	body, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadAt, cut: cut})
	if err != nil {
		t.Fatal(err)
	}
	return body
}
func nativeGraphCostColumnClients(t *testing.T, n *certifiedProcessNetwork, pair *nativeGraphCostColumnPair, stage string, create bool, stream *json.Encoder) [2]*nativeGraphCostColumnClient {
	t.Helper()
	var clients [2]*nativeGraphCostColumnClient
	for g := range 2 {
		c := &nativeGraphCostColumnClient{t: t, n: n, stream: stream, stage: stage, group: g}
		r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsOpen, Initial: nativeGraphCostColumnCutInput(t, pair.Initial), Final: nativeGraphCostColumnCutInput(t, pair.Final), Generation: pair.ID, Create: create})
		if pair.Bindings[g].Coverage != 0 && !nativeColumnSame(pair.Bindings[g], r.Binding) {
			t.Fatal("physical restart changed column binding")
		}
		pair.Bindings[g] = r.Binding
		clients[g] = c
	}
	return clients
}
func nativeGraphCostColumnCloseClients(clients [2]*nativeGraphCostColumnClient) {
	for _, c := range clients {
		c.call(nativeGraphCostColumnRequest{Action: nativeColumnsClose})
	}
}
func nativeGraphCostColumnPublish(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, initial, final graphCut, id [16]byte, stage string, create, updated bool, stream *json.Encoder) nativeGraphCostColumnPair {
	t.Helper()
	pair := nativeGraphCostColumnPair{Initial: initial, Final: final, ID: id, Updated: updated}
	clients := nativeGraphCostColumnClients(t, n, &pair, stage, create, stream)
	for g, c := range clients {
		for attempt := uint64(0); ; attempt++ {
			if attempt > c.binding.Policy.MaxChunks {
				t.Fatal("column finite publication allowance")
			}
			r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsPublish})
			if r.EOF {
				pair.Manifests[g] = r.Manifest
				break
			}
			if r.Manifest.Generation.ID != ([16]byte{}) {
				t.Fatal("incomplete publisher supplied immutable seal")
			}
		}
	}
	// Both actual native publishers returned sealed source EOFs before this
	// structural join. A mask alone never proves an omitted partition complete.
	if err := nativeColumnJoinBoundaries(pair.Bindings, pair.Manifests); err != nil {
		t.Fatal("two actual native EOF join", err)
	}
	for _, mode := range []struct {
		name         string
		cached, fill bool
	}{{"native", false, false}, {"cache-fill", false, true}, {"cached", true, false}} {
		for _, c := range clients {
			c.stage = stage + "/" + mode.name
		}
		nativeGraphCostColumnReadPair(t, clients, &pair, spec, final, mode.cached, mode.fill)
	}
	nativeGraphCostColumnCloseClients(clients)
	return pair
}
func nativeGraphCostColumnReadPair(t *testing.T, clients [2]*nativeGraphCostColumnClient, pair *nativeGraphCostColumnPair, spec nativeGraphCostSpec, at graphCut, cached, fill bool) {
	t.Helper()
	var census [3]uint64 // presence, initial-p5, update-p6: fixed metadata only.
	for g, c := range clients {
		r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsRead, At: nativeGraphCostColumnCutInput(t, at), Cached: cached, Fill: fill})
		if !nativeColumnSame(r.Manifest, pair.Manifests[g]) || r.Cached != cached || r.Fill != fill || r.EOF || len(r.Body)+len(r.Rows) != 0 {
			t.Fatal("column read changed immutable source generation")
		}
		initial, err := decodeGraphCertificate(r.Manifest.Generation.Sources[g].Initial)
		if err != nil {
			t.Fatal(err)
		}
		var selected graphCertificate
		for _, certificate := range at.certificates {
			if certificate.scope.partition == initial.scope.partition {
				selected = certificate
			}
		}
		position, ordinal := initial.sourceIndex, uint64(0)
		var chain [32]byte
		var framesCount, rowsCount, retained uint64
		for {
			r = c.call(nativeGraphCostColumnRequest{Action: nativeColumnsNext})
			if !nativeColumnSame(r.Manifest, pair.Manifests[g]) || r.Cached != cached || r.Fill != fill || r.Ordinal != ordinal {
				t.Fatal("column continuation changed sealed identity/ordinal")
			}
			if r.EOF {
				m := pair.Manifests[g]
				if len(r.Body)+len(r.Rows) != 0 || ordinal != m.Chunks || framesCount != m.Frames || rowsCount != m.Rows || retained != m.Bytes || position != m.Positions[g] || chain != m.Chain {
					t.Fatal("column genuine complete EOF mismatch")
				}
				break
			}
			if ordinal >= pair.Manifests[g].Chunks {
				t.Fatal("column supplied data beyond immutable manifest")
			}
			frames, err := decodeNativeColumnChunk(r.Manifest.Generation, ordinal, chain, r.Body, c.binding.Policy)
			if err != nil {
				t.Fatal("actual typed numeric column page", err)
			}
			selectedRows := []nativeColumnRow{}
			for _, frame := range frames {
				if frame.Partition != initial.scope.partition || frame.Index != position+1 || frame.Outcome == nil || frame.CDC == nil {
					t.Fatal("column skipped original retained prefix")
				}
				position++
				framesCount++
				rowsCount += uint64(len(frame.Rows))
				if frame.Index <= selected.sourceIndex {
					for _, row := range frame.Rows {
						class, err := nativeGraphCostColumnRow(spec, row)
						if err != nil {
							t.Fatal("column exact business row", err, row)
						}
						census[class]++
						selectedRows = append(selectedRows, row)
					}
				}
			}
			if !reflect.DeepEqual(selectedRows, r.Rows) {
				t.Fatal("column selected row receipt differs from actual binary columns")
			}
			retained += uint64(len(nativeColumnChunkKey(pair.ID, ordinal)) + len(r.Body))
			chain = nativeColumnChain(chain, r.Body)
			ordinal++
		}
	}
	wantUpdates := uint64(0)
	if reflect.DeepEqual(at, pair.Final) {
		// The old seal contains no p6 changes; the final seal includes all of them.
		if pair.Updated {
			wantUpdates = spec.Facts
		}
	}
	if census != ([3]uint64{3 + spec.Facts, spec.Facts, wantUpdates}) {
		t.Fatal("column exact retained change census", census)
	}
}

// Fixed V2 cost semantics are validated without a whole-degree map. Genuine
// prefix/original uniqueness is provided by the source-bound frame stream; the
// existing raw CDC checkpoint joins authenticate request cardinality separately.
func nativeScalarGuardedPresenceRow(s nativeGraphCostSpec, row nativeColumnRow) (int, error) {
	if s.Version != 7 && s.Version != 9 || row.Kind != 2 || row.Component != 1 || row.Owner < 7 || (row.Owner-7)%2 != 0 {
		return 0, errCorrupt
	}
	fact := (row.Owner - 7) / 2
	if fact >= s.Facts {
		return 0, errCorrupt
	}
	owner, life, target, targetLife := s.edge(fact)
	if row.Owner != owner || row.Life != 0 || row.Name != "" || row.Type != "edge" || row.Mode != 1 || row.Source != 1 || row.Target != target || row.SourceLife != 2 || row.TargetLife != targetLife || row.After.Handle != life || row.After.Value != 0 || row.After.Provenance != 4+s.Facts+fact || !row.Before.Present || row.Before.Null || row.Before.Handle != life || row.Before.Value != 0 || row.Before.Provenance != 4+fact || row.Before.Revision == 0 || row.After.Revision <= row.Before.Revision {
		return 0, errCorrupt
	}
	return 0, nil
}

func nativeGraphCostColumnRow(spec nativeGraphCostSpec, row nativeColumnRow) (int, error) {
	if len(row.Axis) == 0 || len(row.Scope) == 0 || !row.After.Present || row.After.Null || row.After.Revision == 0 {
		return 0, errCorrupt
	}
	if row.Kind == byte(graphstate.Node) {
		phase := uint64(1)
		if row.Owner == 3 {
			phase = 2
		} else if row.Owner == spec.remoteNode() {
			phase = 3
		} else if row.Owner != 1 {
			return 0, errCorrupt
		}
		if row.Component != byte(graphstate.Presence) || row.Life != 0 || row.After.Handle != row.Owner+1 || row.After.Provenance != phase || row.Before != (nativeColumnCell{}) || row.Name != "" || row.Type != "" || row.Source != 0 || row.Target != 0 || row.SourceLife != 0 || row.TargetLife != 0 {
			return 0, errCorrupt
		}
		return 0, nil
	}
	if row.Kind != byte(graphstate.Relationship) || row.Owner < 7 || (row.Owner-7)%2 != 0 {
		return 0, errCorrupt
	}
	fact := (row.Owner - 7) / 2
	if fact >= spec.Facts {
		return 0, errCorrupt
	}
	owner, life, target, targetLife := spec.edge(fact)
	if row.Owner != owner || row.Type != "edge" || row.Mode != byte(graphstate.LifeBound) || row.Source != 1 || row.Target != target || row.SourceLife != 2 || row.TargetLife != targetLife {
		return 0, errCorrupt
	}
	if row.Component == byte(graphstate.Presence) {
		if spec.Version == 7 && row.Before.Present {
			return nativeFeedRetainedPresenceRow(spec, row)
		}
		if spec.Version == 9 && row.Before.Present {
			return nativeCreateUpdatePresenceRow(spec, row)
		}
		if row.Life != 0 || row.Name != "" || row.After.Handle != life || row.After.Provenance != 4+fact || row.Before != (nativeColumnCell{}) {
			return 0, errCorrupt
		}
		return 0, nil
	}
	if row.Component != byte(graphstate.ScalarProperty) || row.Life != life || row.Name != "p" {
		return 0, errCorrupt
	}
	if row.After.Handle == 5 && row.After.Value == 5 && row.After.Provenance == 4+fact && row.Before == (nativeColumnCell{}) {
		return 1, nil
	}
	if row.After.Handle == 6 && row.After.Value == 6 && row.After.Provenance == 4+fact+spec.Facts && row.Before.Present && !row.Before.Null && row.Before.Handle == 5 && row.Before.Value == 5 && row.Before.Provenance == 4+fact && row.Before.Revision > 0 && row.After.Revision > row.Before.Revision {
		return 2, nil
	}
	return 0, errCorrupt
}
func nativeGraphCostColumnReacquire(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, pair *nativeGraphCostColumnPair, stage string, stream *json.Encoder, cached bool) {
	t.Helper()
	clients := nativeGraphCostColumnClients(t, n, pair, stage, false, stream)
	// Existing seals are checked against a new genuine original source capture.
	// Publication is not re-run after a later mutation; no current substitution.
	nativeGraphCostColumnReadPair(t, clients, pair, spec, pair.Final, cached, false)
	nativeGraphCostColumnCloseClients(clients)
}
func nativeGraphCostLayout() (string, error) {
	layout := os.Getenv("RHO_GRAPH_COST_LAYOUT")
	if layout == "" {
		return "kv", nil
	}
	if layout != "kv" && layout != "columns" {
		return "", errInvalid
	}
	return layout, nil
}
