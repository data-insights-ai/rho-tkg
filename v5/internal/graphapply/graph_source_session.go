package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

type graphSourceLimits struct {
	sessions, calls, rows, bytes, output, frame int
	lifetime                                    time.Duration
}

func defaultGraphSourceLimits(l materializerLimits) graphSourceLimits {
	return graphSourceLimits{sessions: 2, calls: 1024, rows: l.sourceRows, bytes: l.sourceBytes, output: l.outputBytes, frame: min(2<<20, l.outputBytes), lifetime: time.Minute}
}
func (l graphSourceLimits) validate() error {
	if l.sessions < 1 || l.sessions > 16 || l.calls < 1 || l.calls > 65536 || l.rows < 1 || l.rows > 1<<20 || l.bytes < 1 || l.bytes > 64<<20 || l.output < 1 || l.output > 64<<20 || l.frame < 512 || l.frame > 2<<20 || l.lifetime <= 0 || l.lifetime > time.Minute {
		return errInvalid
	}
	return nil
}

// A cookie identifies one server-owned retained view. It is not a certificate,
// durability result or client-supplied ownership/absence assertion.
type graphSourceCookie struct {
	issuer   [16]byte
	sequence uint64
	secret   [16]byte
}
type graphSourceInfo struct {
	cookie      graphSourceCookie
	readID      allocationReadID
	nonce       [16]byte
	observation allocationObservation
	root        raftlog.ApplicationRoot
	floor       uint64
	limits      raftlog.ReadBudget
	lifetime    time.Duration
}
type graphSourceLease struct {
	mu                         sync.Mutex
	view                       *raftlog.ApplicationView
	moveMeta                   *raftlog.ApplicationView
	moveID                     [16]byte
	moveIntent                 [32]byte
	read                       graphstore.ApplicationReadSource
	info                       graphSourceInfo
	limits                     graphSourceLimits
	ctx                        context.Context
	cancel                     context.CancelFunc
	deadline                   time.Time
	stop                       func() bool
	closed                     bool
	released                   atomic.Bool
	closeErr                   error
	calls, rows, bytes, output int
}
type graphSourceServer struct {
	pending   int
	captureWG sync.WaitGroup
	closeDone chan struct{}
	cancel    context.CancelFunc
	mu        sync.Mutex
	host      *allocationHost
	limits    graphSourceLimits
	ctx       context.Context
	next      uint64
	slots     []*graphSourceLease
	closed    bool
	closeErr  error
}

func newGraphSourceServer(ctx context.Context, h *allocationHost, l graphSourceLimits) (*graphSourceServer, error) {
	if ctx == nil || h == nil || h.machine == nil || !crossGraphAgreement(h.machine.SemanticContractID()) {
		return nil, errInvalid
	}
	if err := l.validate(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, replica.ErrStopped
	}
	if h.sourceServer != nil {
		return nil, errInvalid
	}
	l.sessions = min(l.sessions, h.machine.store.ApplicationLimits().MaxViews)
	if l.sessions < 1 {
		return nil, errLimit
	}
	ownerCtx, cancel := context.WithCancel(ctx)
	s := &graphSourceServer{host: h, limits: l, ctx: ownerCtx, cancel: cancel, slots: make([]*graphSourceLease, l.sessions)}
	h.sourceServer = s
	return s, nil
}

// open borrows only this issuer's genuine completed configuration proof. Capture
// is serialized with Host events; lifetime ownership moves to the source server.
func (s *graphSourceServer) open(ctx context.Context, proof allocationProof) (info graphSourceInfo, err error) {
	if s == nil || ctx == nil {
		return info, errInvalid
	}
	h := s.host
	h.mu.Lock()
	err = h.ownProof(proof, observeConfiguration)
	h.mu.Unlock()
	if err != nil {
		return info, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return info, replica.ErrStopped
	}
	if err := errors.Join(ctx.Err(), s.ctx.Err()); err != nil {
		s.mu.Unlock()
		return info, err
	}
	for _, lease := range s.slots {
		if lease != nil && lease.info.readID == proof.id {
			if lease.info.nonce != proof.question.nonce || lease.info.observation != proof.observation {
				s.mu.Unlock()
				return info, errInvalid
			}
			if lease.released.Load() || lease.ctx.Err() != nil {
				s.mu.Unlock()
				return info, graphstore.ErrSourceUnavailable
			}
			info = ownGraphSourceInfo(lease.info)
			s.mu.Unlock()
			return info, nil
		}
	}
	active := s.pending
	for _, lease := range s.slots {
		if lease != nil && !lease.released.Load() {
			active++
		}
	}
	if active >= s.limits.sessions || s.next == math.MaxUint64 {
		s.mu.Unlock()
		return info, errLimit
	}
	s.pending++
	s.captureWG.Add(1)
	defer s.captureWG.Done()
	s.next++
	sequence := s.next
	s.mu.Unlock()
	var view *raftlog.ApplicationView
	captureCtx, cancelCapture := context.WithCancel(ctx)
	stopCapture := context.AfterFunc(s.ctx, cancelCapture)
	info, view, err = s.capture(captureCtx, proof)
	stopCapture()
	cancelCapture()
	if err == nil {
		info.cookie.sequence = sequence
		_, err = rand.Read(info.cookie.secret[:])
	}
	var lease *graphSourceLease
	if err == nil {
		leaseCtx, cancel := context.WithTimeout(s.ctx, s.limits.lifetime)
		lease = &graphSourceLease{view: view, read: view, info: info, limits: s.limits, ctx: leaseCtx, cancel: cancel, deadline: time.Now().Add(s.limits.lifetime), bytes: 4096 + 2*cap(info.root.Image), output: 4096 + 2*cap(info.root.Image)}
		if lease.bytes > s.limits.bytes || lease.output > s.limits.output {
			cancel()
			err = errLimit
		}
	}
	s.mu.Lock()
	s.pending--
	if err == nil && s.closed {
		err = replica.ErrStopped
	}
	if err == nil {
		for i, current := range s.slots {
			if current == nil || current.released.Load() {
				s.slots[i] = lease
				break
			}
		}
		lease.stop = context.AfterFunc(lease.ctx, func() { _ = lease.close() })
	}
	s.mu.Unlock()
	if err != nil {
		if lease != nil {
			lease.cancel()
		}
		if view != nil {
			err = errors.Join(err, view.Close())
		}
		return graphSourceInfo{}, err
	}
	return ownGraphSourceInfo(info), nil
}

// capture serializes only bounded native authority/data reads with Host events.
// It returns any acquired owner to its caller for cleanup OUTSIDE the Host lock.
func (s *graphSourceServer) capture(ctx context.Context, proof allocationProof) (info graphSourceInfo, view *raftlog.ApplicationView, err error) {
	h := s.host
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.ownProof(proof, observeConfiguration); err != nil {
		return info, nil, err
	}
	view, err = h.machine.store.ApplicationView(proof.observation.source.index)
	if err != nil {
		return info, view, err
	}
	root, err := view.RootBounded(ctx, 172)
	if err != nil {
		return info, view, err
	}
	metadata, err := graphstore.DecodeRoot(root.Image)
	if err != nil || metadata.SemanticEpoch() != proof.observation.epoch || metadata.EffectDigest() != proof.observation.effect {
		return info, view, errors.Join(errInvalid, err)
	}
	l := h.machine.limits.allocation
	l.readRows, l.readBytes = min(l.readRows, s.limits.rows), min(l.readBytes, s.limits.bytes)
	q := reader{ctx: ctx, view: view, ns: h.machine.ns, base: root, limits: l, bytes: cap(root.Image)}
	floor, err := readPartitionRoundFloor(&q, proof.observation.configuration)
	if err != nil {
		return info, view, err
	}

	info = graphSourceInfo{cookie: graphSourceCookie{issuer: h.session}, readID: proof.id, nonce: proof.question.nonce, observation: proof.observation, root: root, floor: floor, limits: view.ReadLimits(), lifetime: s.limits.lifetime}
	return info, view, nil
}

func ownGraphSourceInfo(info graphSourceInfo) graphSourceInfo {
	info.root.Image = owned(info.root.Image)
	return info
}
func (l *graphSourceLease) done() bool { return l.released.Load() }
func (l *graphSourceLease) close() error {
	// A direct cookie close must interrupt owned work before waiting for it.
	l.cancel()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return l.closeErr
	}
	l.closed = true
	l.cancel()
	if l.stop != nil {
		l.stop()
	}
	l.closeErr = l.view.Close()
	if l.moveMeta != nil {
		l.closeErr = errors.Join(l.closeErr, l.moveMeta.Close())
		l.moveMeta = nil
	}
	l.released.Store(true)
	return l.closeErr
}
func (s *graphSourceServer) lease(cookie graphSourceCookie) (*graphSourceLease, error) {
	if s == nil {
		return nil, errInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, replica.ErrStopped
	}
	for _, l := range s.slots {
		if l != nil && l.info.cookie == cookie {
			return l, nil
		}
	}
	return nil, graphstore.ErrSourceUnavailable
}
func (s *graphSourceServer) close() error {
	if s == nil {
		return errInvalid
	}
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		if done != nil {
			<-done
		}
		s.mu.Lock()
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	leases := s.slots
	s.slots = nil
	s.closeDone = make(chan struct{})
	s.mu.Unlock()
	s.cancel()
	for _, lease := range leases {
		if lease != nil {
			lease.cancel()
		}
	}
	s.captureWG.Wait()
	var err error
	for _, lease := range leases {
		if lease != nil {
			err = errors.Join(err, lease.close())
		}
	}
	s.mu.Lock()
	s.closeErr = err
	close(s.closeDone)
	s.mu.Unlock()
	return err
}

func (s *graphSourceServer) closeSource(cookie graphSourceCookie) error {
	l, err := s.lease(cookie)
	if err != nil {
		return err
	}
	return l.close()
}

// begin/charge serialize one bounded operation against the fixed retained view.
// Lifetime expiry remains owner-side even if a client never sends another frame.
func (l *graphSourceLease) begin(ctx context.Context, rows, bytes int) error {
	if ctx == nil || rows < 0 || bytes < 0 {
		return errInvalid
	}
	if err := errors.Join(ctx.Err(), l.ctx.Err()); err != nil {
		return errors.Join(graphstore.ErrSourceUnavailable, err)
	}
	if l.closed || time.Now().After(l.deadline) {
		return graphstore.ErrSourceUnavailable
	}
	if l.calls >= l.limits.calls || rows > l.limits.rows-l.rows || bytes > l.limits.bytes-l.bytes {
		return errLimit
	}
	l.calls++
	l.rows += rows
	l.bytes += bytes
	return nil
}
func (l *graphSourceLease) charge(bytes int) error {
	if bytes < 0 || bytes > l.limits.bytes-l.bytes || bytes > l.limits.output-l.output {
		return errLimit
	}
	l.bytes += bytes
	l.output += bytes
	return nil
}
func (s *graphSourceServer) root(ctx context.Context, cookie graphSourceCookie, maximum int) (root raftlog.ApplicationRoot, err error) {
	if ctx == nil {
		return root, errInvalid
	}
	l, err := s.lease(cookie)
	if err != nil {
		return root, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(l.ctx, cancel)
	defer func() { stop(); cancel() }()
	if maximum < 0 || maximum > 172 {
		return root, errLimit
	}
	if err := l.begin(ctx, 0, 512); err != nil {
		return root, err
	}
	root, err = l.view.RootBounded(ctx, maximum)
	if err == nil && ctx.Err() != nil {
		err = errors.Join(graphstore.ErrSourceUnavailable, ctx.Err())
	}
	if err == nil {
		err = l.charge(256 + 4*cap(root.Image))
	}
	if err != nil {
		if l.ctx.Err() != nil {
			err = errors.Join(graphstore.ErrSourceUnavailable, err)
		}
		return raftlog.ApplicationRoot{}, err
	}
	return root, nil
}
func (s *graphSourceServer) get(ctx context.Context, cookie graphSourceCookie, key []byte, maximum int) (row raftlog.KV, found bool, err error) {
	if ctx == nil {
		return row, false, errInvalid
	}
	l, err := s.lease(cookie)
	if err != nil {
		return row, false, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(l.ctx, cancel)
	defer func() { stop(); cancel() }()
	if maximum < 0 || maximum > min(l.info.limits.Bytes, l.limits.frame-512) {
		return row, false, errLimit
	}
	// Admit the worst bounded native copy before data access, not after it.
	prefix := 512 + 4*len(key)
	if maximum > (l.limits.bytes-l.bytes-prefix-256)/4 || maximum > (l.limits.output-l.output-256)/4 {
		return row, false, errLimit
	}
	if err := l.begin(ctx, 1, prefix); err != nil {
		return row, false, err
	}
	row, found, err = l.read.Get(ctx, key, maximum)
	if err == nil && found && !bytes.Equal(row.Key, key) {
		err = errCorrupt
	}
	if err == nil && ctx.Err() != nil {
		err = errors.Join(graphstore.ErrSourceUnavailable, ctx.Err())
	}
	if err == nil {
		err = l.charge(256 + 4*(cap(row.Key)+cap(row.Value)))
	}
	if err != nil {
		if l.ctx.Err() != nil {
			err = errors.Join(graphstore.ErrSourceUnavailable, err)
		}
		return raftlog.KV{}, false, err
	}
	return row, found, nil
}
func (s *graphSourceServer) scan(ctx context.Context, cookie graphSourceCookie, lower, upper, after []byte, budget raftlog.ReadBudget) (page raftlog.ApplicationPage, err error) {
	if ctx == nil {
		return page, errInvalid
	}
	l, err := s.lease(cookie)
	if err != nil {
		return page, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(l.ctx, cancel)
	defer func() { stop(); cancel() }()
	if budget.Rows < 1 || budget.Bytes < 1 || budget.Rows > min(l.info.limits.Rows, l.limits.rows-l.rows) || budget.Bytes > min(l.info.limits.Bytes, l.limits.frame-512, (l.limits.bytes-l.bytes)/4, (l.limits.output-l.output)/4) {
		return page, errLimit
	}
	if err := l.begin(ctx, 0, 512+4*(len(lower)+len(upper)+len(after))); err != nil {
		return page, err
	}
	page, err = l.read.Scan(ctx, lower, upper, after, budget)
	if page.Visited > l.limits.rows-l.rows {
		err = errors.Join(errLimit, err)
	}
	l.rows += page.Visited
	if err == nil && ctx.Err() != nil {
		err = errors.Join(graphstore.ErrSourceUnavailable, ctx.Err())
	}
	if err == nil {
		err = l.charge(256 + 4*page.Bytes)
	}
	if err != nil {
		if l.ctx.Err() != nil {
			err = errors.Join(graphstore.ErrSourceUnavailable, err)
		}
		return raftlog.ApplicationPage{}, err
	}
	return page, nil
}
