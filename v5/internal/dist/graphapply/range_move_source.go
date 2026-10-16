package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"math"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This is retained capture metadata for an admitted handoff, not a public cut
// certificate or an allocation proof. Only the source-owned capture opens it.
type graphMoveSourceInfo struct {
	cookie        graphSourceCookie
	nonce         [16]byte
	readID        allocationReadID
	scope         allocationScope
	moveID        [16]byte
	intentHash    [32]byte
	manifestIndex uint64
	manifestHash  [32]byte
	configuration genesisAllocationConfig
	root          raftlog.ApplicationRoot
	floor         uint64
	limits        raftlog.ReadBudget
	lifetime      time.Duration
}

func encodeGraphMoveSourceInfo(i graphMoveSourceInfo) ([]byte, error) {
	cfg, err := encodeGraphGenesisConfig(i.configuration)
	if err != nil {
		return nil, err
	}
	if !i.scope.valid() || !rangeMoveGraphAgreement(i.scope.semantic) || i.moveID == ([16]byte{}) || i.intentHash == ([32]byte{}) || (i.manifestIndex == 0) != (i.manifestHash == ([32]byte{})) || i.manifestIndex != 0 && i.manifestIndex < i.root.Index || i.nonce == ([16]byte{}) || i.cookie.issuer != i.readID.session || i.cookie.issuer == ([16]byte{}) || i.cookie.sequence == 0 || i.cookie.secret == ([16]byte{}) || i.readID.sequence == 0 || i.root.Generation == 0 || i.root.Index == 0 || len(i.root.Image) != 172 || sha256.Sum256(i.root.Image) != i.root.ImageHash || i.limits.Rows < 1 || i.limits.Rows > 4096 || i.limits.Bytes < 1 || i.limits.Bytes > 64<<20 || i.lifetime <= 0 || i.lifetime > time.Minute {
		return nil, errInvalid
	}
	return boundedEncoding(4096, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'S', 2})
		emitSourceCookie(w, i.cookie)
		w.add(i.nonce[:])
		w.add(i.readID.session[:])
		w.u64(i.readID.sequence)
		w.add(appendAllocationScope(nil, i.scope))
		w.add(i.moveID[:])
		w.add(i.intentHash[:])
		w.u64(i.manifestIndex)
		w.add(i.manifestHash[:])
		w.field(cfg)
		emitSourceRoot(w, i.root)
		w.u64(i.floor)
		w.u32(i.limits.Rows)
		w.u32(i.limits.Bytes)
		w.u64(uint64(i.lifetime)) // #nosec G115 -- positive duration admitted at most one minute.
	})
}

func decodeGraphMoveSourceInfo(wire []byte) (graphMoveSourceInfo, error) {
	body, err := graphBody(wire, "GMS\x02", 4096)
	if err != nil {
		return graphMoveSourceInfo{}, err
	}
	c := graphCursor{b: body}
	i := graphMoveSourceInfo{cookie: readSourceCookie(&c), nonce: c.array(), readID: allocationReadID{session: c.array(), sequence: c.u64()}}
	i.scope = readAllocationScope(&decoder{b: c.take(120)})
	i.moveID = c.array()
	copy(i.intentHash[:], c.take(32))
	i.manifestIndex = c.u64()
	copy(i.manifestHash[:], c.take(32))
	cfg := c.field(partitionGenesisConfigBytes)
	i.root = readSourceRoot(&c)
	i.floor = c.u64()
	i.limits = raftlog.ReadBudget{Rows: c.count(4096, 0), Bytes: c.count(64<<20, 0)}
	ttl := c.u64()
	if c.err != nil || len(c.b) != 0 || ttl == 0 || ttl > uint64(time.Minute) {
		return graphMoveSourceInfo{}, errors.Join(errCorrupt, c.err)
	}
	i.lifetime = time.Duration(ttl) // #nosec G115 -- duration admitted at most one minute.
	i.configuration, err = decodeGraphGenesisConfig(cfg, i.scope.graph)
	if err != nil {
		return graphMoveSourceInfo{}, err
	}
	canonical, err := encodeGraphMoveSourceInfo(i)
	if err != nil || !bytes.Equal(canonical, wire) {
		return graphMoveSourceInfo{}, errors.Join(errCorrupt, err)
	}
	i.root.Image = owned(i.root.Image)
	return i, nil
}

func (s *graphSourceServer) openMove(ctx context.Context, proof rangeMoveProof) (info graphMoveSourceInfo, err error) {
	if s == nil || ctx == nil || proof.owner != s.host {
		return info, errInvalid
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
	var view, metadata *raftlog.ApplicationView
	var lease *graphSourceLease
	h := s.host
	h.mu.Lock()
	if h.unavailableLocked() {
		err = replica.ErrStopped
	} else {
		err = checkRangeMoveProof(proof)
	}
	if err == nil && (proof.observation.index > h.driver.Applied() || proof.observation.status.phase < rangeMoveCapture || rangeMoveTerminal(proof.observation.status) || proof.observation.status.result != rangeMoveApplied) {
		err = errInvalid
	}
	if err == nil {
		currentView, e := h.machine.store.ApplicationView(h.driver.Applied())
		err = e
		if err == nil {
			base, e := currentView.RootBounded(capture, 172)
			err = e
			q := reader{ctx: capture, view: currentView, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation}
			var current rangeMoveStatus
			var present bool
			if err == nil {
				current, present, err = readRangeMoveStatus(&q, proof.question.move)
			}
			if err == nil && (!present || current.intent != proof.observation.status.intent || current.phase < rangeMoveCapture || rangeMoveTerminal(current)) {
				err = errInvalid
			}
			var record raftlog.ApplicationControlPut
			if err == nil {
				var found bool
				var control raftlog.ApplicationControlRecord
				control, found, err = crossControl(&q, rangeMoveControlKey(q.ns, proof.question.move, 'C'), 1024)
				if err == nil && !found {
					err = errCorrupt
				}
				record.Value = control.Value
			}
			if err == nil {
				info.root, info.floor, err = moveCaptureRoot(record.Value, h.machine.scope())
			}
			if err == nil {
				info.configuration, err = h.machine.readGenesisConfig(&q)
			}
			if err == nil {
				var configuration [32]byte
				configuration, err = info.configuration.digest()
				if err == nil && configuration != proof.observation.configuration {
					err = errInvalid
				}
			}
			err = errors.Join(err, currentView.Close())
		}
	}
	if err == nil {
		view, err = h.machine.store.ApplicationView(info.root.Index)
		if err == nil {
			var root raftlog.ApplicationRoot
			root, err = view.RootBounded(capture, 172)
			if err == nil && (root.Index != info.root.Index || root.ImageHash != info.root.ImageHash || !bytes.Equal(root.Image, info.root.Image)) {
				err = errInvalid
			}
			info.root = root
		}
	}

	if err == nil && proof.observation.status.phase == rangeMoveDescribe {
		// The later completed quorum coordinate owns P receipts, which did not
		// exist at C. Pin it separately from the frozen graph-data view.
		info.manifestIndex, info.manifestHash = proof.observation.index, proof.observation.root.ImageHash
		metadata, err = h.machine.store.ApplicationView(proof.observation.index)
		if err == nil {
			actual, e := metadata.RootBounded(capture, 172)
			err = e
			if err == nil && (actual.Index != proof.observation.index || actual.ImageHash != proof.observation.root.ImageHash || !bytes.Equal(actual.Image, proof.observation.root.Image)) {
				err = errInvalid
			}
		}
	}
	if err == nil {
		info.cookie = graphSourceCookie{issuer: h.session, sequence: seq}
		_, err = rand.Read(info.cookie.secret[:])
		info.nonce, info.readID, info.scope, info.moveID, info.intentHash = proof.question.nonce, proof.id, h.machine.scope(), proof.question.move, proof.observation.status.intent
		info.limits, info.lifetime = view.ReadLimits(), s.limits.lifetime
	}
	h.mu.Unlock()
	stop()
	cancel()
	if err == nil {
		owner, cancelOwner := context.WithTimeout(s.ctx, s.limits.lifetime)
		lease = &graphSourceLease{view: view, read: view, moveMeta: metadata, moveID: proof.question.move, moveIntent: proof.observation.status.intent, info: graphSourceInfo{cookie: info.cookie, root: info.root, limits: info.limits, lifetime: info.lifetime}, limits: s.limits, ctx: owner, cancel: cancelOwner, deadline: s.lifetimeTime().Add(s.limits.lifetime), lifetimeNow: s.lifetimeNow, bytes: 8192, output: 8192}
		if _, err = encodeGraphMoveSourceInfo(info); err == nil && (s.limits.bytes < 8192 || s.limits.output < 8192) {
			err = errLimit
		}
	}
	if err == nil && s.beforePublication != nil {
		s.beforePublication(lease)
	}
	s.mu.Lock()
	s.pending--
	if err == nil {
		err = s.publicationContextLocked(ctx, lease)
	}
	if err == nil {
		for i, l := range s.slots {
			if l == nil || l.released.Load() {
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
		if metadata != nil {
			err = errors.Join(err, metadata.Close())
		}
		return graphMoveSourceInfo{}, err
	}
	info.root.Image = owned(info.root.Image)
	return info, nil
}

func (e *graphAuthenticatedEndpoint) openMoveSource(ctx context.Context, move [16]byte) (*remoteGraphSource, error) {
	if e == nil || ctx == nil || move == ([16]byte{}) || !rangeMoveGraphAgreement(e.peer.scope.semantic) {
		return nil, errInvalid
	}
	cleanup, err := e.budget.cleanup()
	if err != nil {
		return nil, err
	}
	// Root/config/capture point reads have fixed bounded framing. Their native
	// work is prepaid on the original operation allowance before dispatch.
	// The native quorum observation has an explicit128-row/128KiB work and
	//512KiB owned envelope. Additional status/capture/config point reads are
	// fixed <=32KiB/16rows; receive's4x ownership charge covers both paths.
	if err := e.budget.receive(160<<10, 144); err != nil {
		return nil, err
	}
	started := time.Now()
	r, err := e.call(ctx, graphSourceRequest{kind: sourceMoveOpen, tx: move}, 8192)
	if err != nil {
		return nil, err
	}
	i, err := decodeGraphMoveSourceInfo(r.body)
	if err != nil {
		return nil, err
	}
	if i.scope != e.peer.scope || i.moveID != move || i.cookie.issuer != r.issuer || i.nonce != r.nonce || i.readID.session != r.issuer {
		return nil, errInvalid
	}
	owner, cancel := context.WithDeadline(ctx, started.Add(i.lifetime))
	return &remoteGraphSource{mu: newGraphRequestGate(), endpoint: e, move: &i, info: graphSourceInfo{cookie: i.cookie, root: i.root, limits: i.limits, lifetime: i.lifetime}, ctx: owner, cancel: cancel, deadline: started.Add(i.lifetime), cleanup: cleanup}, nil
}
