package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"math"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Historical capture metadata is not an allocation/quorum observation. The
// source Host validates the persisted certificate before retaining its root.
type graphCertifiedSourceInfo struct {
	cookie        graphSourceCookie
	nonce         [16]byte
	certificate   graphCertificate
	configuration genesisAllocationConfig
	root          raftlog.ApplicationRoot
	limits        raftlog.ReadBudget
	lifetime      time.Duration
}

func encodeGraphCertifiedSourceInfo(i graphCertifiedSourceInfo) ([]byte, error) {
	cert, e := encodeGraphCertificate(i.certificate)
	if e != nil {
		return nil, e
	}
	cfg, e := encodeGraphGenesisConfig(i.configuration)
	if e != nil {
		return nil, e
	}
	digest, e := i.configuration.digest()
	if e != nil {
		return nil, e
	}
	if digest != i.certificate.configuration || i.cookie.issuer == ([16]byte{}) || i.cookie.sequence == 0 || i.cookie.secret == ([16]byte{}) || i.nonce == ([16]byte{}) || i.root.Generation == 0 || i.root.Index != i.certificate.sourceIndex || len(i.root.Image) != 172 || !bytes.Equal(i.root.Image, i.certificate.image[:]) || sha256.Sum256(i.root.Image) != i.root.ImageHash || i.limits.Rows < 1 || i.limits.Rows > 4096 || i.limits.Bytes < 1 || i.limits.Bytes > 64<<20 || i.lifetime <= 0 || i.lifetime > time.Minute {
		return nil, errInvalid
	}
	return boundedEncoding(4096, func(w *boundedWriter) {
		w.add([]byte{'G', 'C', 'I', 1})
		emitSourceCookie(w, i.cookie)
		w.add(i.nonce[:])
		w.field(cert)
		w.field(cfg)
		emitSourceRoot(w, i.root)
		w.u32(i.limits.Rows)
		w.u32(i.limits.Bytes)
		w.u64(uint64(i.lifetime))
	}) // #nosec G115 -- lifetime admitted positive and at most one minute.
}
func decodeGraphCertifiedSourceInfo(wire []byte) (graphCertifiedSourceInfo, error) {
	b, e := graphBody(wire, "GCI\x01", 4096)
	if e != nil {
		return graphCertifiedSourceInfo{}, e
	}
	c := graphCursor{b: b}
	i := graphCertifiedSourceInfo{cookie: readSourceCookie(&c), nonce: c.array()}
	cert := c.field(512)
	cfg := c.field(partitionGenesisConfigBytes)
	i.root = readSourceRoot(&c)
	i.limits = raftlog.ReadBudget{Rows: c.count(4096, 0), Bytes: c.count(64<<20, 0)}
	ttl := c.u64()
	if c.err != nil || len(c.b) != 0 || ttl == 0 || ttl > uint64(time.Minute) {
		return graphCertifiedSourceInfo{}, errors.Join(errCorrupt, c.err)
	}
	i.lifetime = time.Duration(ttl) // #nosec G115 -- admitted duration is at most one minute.
	i.certificate, e = decodeGraphCertificate(cert)
	if e != nil {
		return graphCertifiedSourceInfo{}, e
	}
	i.configuration, e = decodeGraphGenesisConfig(cfg, i.certificate.scope.graph)
	if e != nil {
		return graphCertifiedSourceInfo{}, e
	}
	canonical, e := encodeGraphCertifiedSourceInfo(i)
	if e != nil || !bytes.Equal(canonical, wire) {
		return graphCertifiedSourceInfo{}, errors.Join(errCorrupt, e)
	}
	i.root.Image = owned(i.root.Image)
	return i, nil
}

func (s *graphSourceServer) openCertified(ctx context.Context, cert graphCertificate, nonce [16]byte) (info graphCertifiedSourceInfo, err error) {
	if s == nil || ctx == nil || nonce == ([16]byte{}) {
		return info, errInvalid
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return info, replica.ErrStopped
	}
	if e := errors.Join(ctx.Err(), s.ctx.Err()); e != nil {
		s.mu.Unlock()
		return info, e
	}
	active := s.pending
	for _, l := range s.slots {
		if l != nil && !l.released.Load() {
			active++
		}
	}
	if active >= s.limits.sessions || s.next == math.MaxUint64 {
		s.mu.Unlock()
		return info, errLimit
	}
	s.pending++
	s.next++
	seq := s.next
	s.captureWG.Add(1)
	s.mu.Unlock()
	defer s.captureWG.Done()
	capture, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	view, e := s.host.AtGraphCertificate(capture, cert)
	var lease *graphSourceLease
	if e == nil {
		info.root, e = view.RootBounded(capture, 172)
		if e == nil {
			s.host.mu.Lock()
			if s.host.unavailableLocked() {
				e = replica.ErrStopped
			} else {
				info.configuration, e = s.host.currentConfiguration()
			}
			s.host.mu.Unlock()
		}
		if e == nil {
			info.cookie = graphSourceCookie{issuer: s.host.session, sequence: seq}
			_, e = rand.Read(info.cookie.secret[:])
			info.nonce = nonce
			info.certificate = cert
			info.limits = view.ReadLimits()
			info.lifetime = s.limits.lifetime
		}
	}
	stop()
	cancel()
	if e == nil {
		owner, cancelOwner := context.WithTimeout(s.ctx, s.limits.lifetime)
		lease = &graphSourceLease{view: view, read: view, info: graphSourceInfo{cookie: info.cookie, root: info.root, limits: info.limits, lifetime: info.lifetime}, limits: s.limits, ctx: owner, cancel: cancelOwner, deadline: time.Now().Add(s.limits.lifetime), bytes: 8192, output: 8192}
		if _, e = encodeGraphCertifiedSourceInfo(info); e == nil && (lease.bytes > s.limits.bytes || lease.output > s.limits.output) {
			e = errLimit
		}
	}
	if e == nil && s.beforePublication != nil {
		s.beforePublication(lease)
	}
	s.mu.Lock()
	s.pending--
	if e == nil && s.closed {
		e = replica.ErrStopped
	}
	if e == nil {
		for j, l := range s.slots {
			if l == nil || l.released.Load() {
				s.slots[j] = lease
				break
			}
		}
		lease.stop = context.AfterFunc(lease.ctx, func() { _ = lease.close() })
	}
	s.mu.Unlock()
	if e != nil {
		if lease != nil {
			lease.cancel()
		}
		if view != nil {
			e = errors.Join(e, view.Close())
		}
		return graphCertifiedSourceInfo{}, e
	}
	info.root.Image = owned(info.root.Image)
	return info, nil
}

func (e *graphAuthenticatedEndpoint) openCertifiedSource(ctx context.Context, cert graphCertificate) (*remoteGraphSource, genesisAllocationConfig, error) {
	if e == nil || ctx == nil || cert.scope != e.peer.scope {
		return nil, genesisAllocationConfig{}, errInvalid
	}
	wire, err := encodeGraphCertificate(cert)
	if err != nil {
		return nil, genesisAllocationConfig{}, err
	}
	cleanup, err := e.budget.cleanup()
	if err != nil {
		return nil, genesisAllocationConfig{}, err
	}
	started := time.Now()
	r, err := e.call(ctx, graphSourceRequest{kind: sourceCertifiedOpen, command: wire}, 8192)
	if err != nil {
		return nil, genesisAllocationConfig{}, err
	}
	i, err := decodeGraphCertifiedSourceInfo(r.body)
	if err != nil {
		return nil, genesisAllocationConfig{}, err
	}
	if i.certificate != cert || i.cookie.issuer != r.issuer || i.nonce != r.nonce {
		return nil, genesisAllocationConfig{}, errInvalid
	}
	owner, cancel := context.WithDeadline(ctx, started.Add(i.lifetime))
	s := &remoteGraphSource{mu: newGraphRequestGate(), endpoint: e, certified: &i, info: graphSourceInfo{cookie: i.cookie, root: i.root, limits: i.limits, lifetime: i.lifetime}, ctx: owner, cancel: cancel, deadline: started.Add(i.lifetime), cleanup: cleanup}
	return s, i.configuration, nil
}

func (s *remoteGraphSource) cancelCertifiedReads() {
	if s != nil {
		s.cancel()
	}
}
