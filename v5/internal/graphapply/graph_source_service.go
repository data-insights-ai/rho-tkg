package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// The embedding drives actual Host events and returns only their completed,
// correlated replies or committed native application outcomes. Implementations
// must keep the Host event lane moving while a coordinator awaits remote work.
type graphHostReadPump interface {
	Allocation(context.Context, allocationQuery) (allocationReply, error)
	Cross(context.Context, crossQuery) (crossReply, error)
	Request(context.Context, crossRequestQuestion) (crossRequestReply, error)
	Cut(context.Context, graphCutQuery) (graphCutReply, error)
	Submit(context.Context, allocationProposal) ([]byte, error)
}
type graphPeerService struct {
	mu              graphRequestGate
	host            *allocationHost
	sources         *graphSourceServer
	pump            graphHostReadPump
	peer            graphPeerIdentity
	last            uint64
	digest          [32]byte
	reply           []byte
	replacementOnly bool
}

func newGraphPeerService(h *allocationHost, sources *graphSourceServer, pump graphHostReadPump) (*graphPeerService, error) {
	if h == nil || sources == nil || sources.host != h || pump == nil {
		return nil, errInvalid
	}
	return &graphPeerService{mu: newGraphRequestGate(), host: h, sources: sources, pump: pump, peer: graphPeerIdentity{scope: h.machine.scope(), voter: h.machine.store.ApplicationLimits().LocalVoter}}, nil
}
func encodeGraphSourceProposal(p allocationProposal) ([]byte, error) {
	scope, err := encodeAllocationScope(p.target)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(graphSourceFrameBytes, func(w *boundedWriter) { w.add([]byte{'G', 'S', 'C', 1}); w.field(scope); w.field(p.wire) })
}
func decodeGraphSourceProposal(wire []byte) (allocationProposal, error) {
	body, err := graphBody(wire, "GSC\x01", graphSourceFrameBytes)
	if err != nil {
		return allocationProposal{}, err
	}
	c := graphCursor{b: body}
	scope := c.field(allocationScopeBytes)
	command := c.field(1 << 20)
	if c.err != nil || len(c.b) != 0 {
		return allocationProposal{}, errors.Join(errCorrupt, c.err)
	}
	target, err := decodeAllocationScope(scope)
	if err != nil {
		return allocationProposal{}, err
	}
	return allocationProposal{target: target, wire: owned(command)}, nil
}

func encodeCrossRequestBody(p crossRequestProof) ([]byte, error) {
	if p.owner == nil || p.scope != p.owner.machine.scope() || p.id.session != p.owner.session || p.id.sequence == 0 || p.id.sequence > p.owner.issuedReads.Load() || p.question.tx == ([16]byte{}) || p.question.nonce == ([16]byte{}) || p.index == 0 {
		return nil, errInvalid
	}
	scope, err := encodeAllocationScope(p.scope)
	if err != nil {
		return nil, err
	}
	var status []byte
	if p.present {
		status, err = encodeCrossStatus(p.status)
		if err != nil {
			return nil, err
		}
		if p.status.tx != p.question.tx || p.status.index > p.index || sha256.Sum256(p.manifest) != p.status.digest {
			return nil, errInvalid
		}
	} else if len(p.manifest) != 0 || p.status != (crossStatus{}) {
		return nil, errInvalid
	}
	return boundedEncoding(graphSourceFrameBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'S', 'L', 1})
		w.field(scope)
		w.add(p.id.session[:])
		w.u64(p.id.sequence)
		w.add(p.question.tx[:])
		w.add(p.question.nonce[:])
		w.add(p.configuration[:])
		w.u64(p.index)
		if p.present {
			w.tag(1)
		} else {
			w.tag(0)
		}
		w.field(status)
		w.field(p.manifest)
	})
}

type crossRequestBody struct {
	scope         allocationScope
	id            allocationReadID
	question      crossRequestQuestion
	configuration [32]byte
	index         uint64
	status        crossStatus
	manifest      []byte
	present       bool
}

func decodeCrossRequestBody(wire []byte) (crossRequestBody, error) {
	body, err := graphBody(wire, "GSL\x01", graphSourceFrameBytes)
	if err != nil {
		return crossRequestBody{}, err
	}
	c := graphCursor{b: body}
	scope := c.field(allocationScopeBytes)
	p := crossRequestBody{id: allocationReadID{session: c.array(), sequence: c.u64()}, question: crossRequestQuestion{tx: c.array(), nonce: c.array()}}
	copy(p.configuration[:], c.take(32))
	p.index = c.u64()
	flag := c.tag()
	p.present = flag == 1
	status := c.field(256)
	p.manifest = c.field(1 << 20)
	if c.err != nil || len(c.b) != 0 || flag > 1 || p.index == 0 || p.id.sequence == 0 || p.configuration == ([32]byte{}) {
		return crossRequestBody{}, errors.Join(errCorrupt, c.err)
	}
	p.scope, err = decodeAllocationScope(scope)
	if err != nil {
		return crossRequestBody{}, err
	}
	if p.present {
		p.status, err = decodeCrossStatus(status)
		if err != nil || p.status.tx != p.question.tx || p.status.ns != (namespace{p.scope.graph, p.scope.partition}) || p.status.index > p.index || p.status.digest != sha256.Sum256(p.manifest) {
			return crossRequestBody{}, errors.Join(errCorrupt, err)
		}
	} else if len(status)+len(p.manifest) != 0 {
		return crossRequestBody{}, errCorrupt
	}
	p.manifest = owned(p.manifest)
	return p, nil
}

// serve serializes one endpoint request stream, with a bounded exact last reply
// cache. Its mutex is independent of the Host/event mutex. Paused data access
// can never occupy the event lane or prevent its separate cancel/close handling.
func (s *graphPeerService) serve(ctx context.Context, wire []byte) ([]byte, error) {
	if s == nil || ctx == nil {
		return nil, errInvalid
	}
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if err := s.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.unlock()
	digest := sha256.Sum256(wire)
	if r.sequence <= s.last {
		if r.sequence == s.last && digest == s.digest {
			return owned(s.reply), nil
		}
		return nil, errInvalid
	}
	if s.replacementOnly && r.kind != sourceReplacementRead {
		return nil, errInvalid
	}
	var body []byte
	switch r.kind {
	case sourceReplacementRead:
		body, err = s.readReplacement(ctx, r)
	case sourceMoveOpen:
		pump, ok := s.pump.(graphHostRangeMovePump)
		if !ok || !s.host.machine.rangeMoves() {
			err = errInvalid
			break
		}
		q := rangeMoveQuery{move: r.tx, nonce: r.nonce, sourceRows: 128, sourceBytes: 128 << 10, outputBytes: 512 << 10}
		var reply rangeMoveReply
		reply, err = pump.Move(ctx, q)
		if err == nil {
			err = reply.err
		}
		if err == nil && (reply.question != q || reply.id != reply.proof.id || reply.proof.owner != s.host) {
			err = errInvalid
		}
		if err == nil {
			var info graphMoveSourceInfo
			info, err = s.sources.openMove(ctx, reply.proof)
			if err == nil {
				body, err = encodeGraphMoveSourceInfo(info)
				if err != nil {
					err = errors.Join(err, s.sources.closeSource(info.cookie))
				}
			}
		}
	case sourceMoveRead:
		if r.cookie != (graphSourceCookie{}) {
			body, err = s.sources.moveFrozenPage(ctx, r)
		} else {
			body, err = s.readRangeMove(ctx, r)
		}
	case sourceCutRead:
		var q graphCutQuery
		q, err = decodeGraphCutQuery(r.command)
		q.nonce = r.nonce
		if err == nil && (q.sourceRows < 1 || q.sourceBytes < 1 || q.outputBytes < 1) {
			err = errInvalid
		}
		if err == nil {
			var reply graphCutReply
			reply, err = s.pump.Cut(ctx, q)
			if err == nil {
				err = reply.err
			}
			var p graphCutAuthorityBody
			if err == nil {
				s.host.mu.Lock()
				if reply.id != reply.proof.id || reply.question != q || reply.proof.owner != s.host || reply.proof.observation.index > s.host.driver.Applied() {
					err = errInvalid
				} else {
					err = checkGraphCutProof(reply.proof, q)
				}
				if err == nil {
					p = graphCutAuthorityBody{scope: s.host.machine.scope(), id: reply.id, question: q, observation: reply.proof.observation}
				}
				s.host.mu.Unlock()
			}
			if err == nil || errors.Is(err, errGraphCutPending) || errors.Is(err, errGraphCutScope) {
				body, err = encodeGraphCutReplyBody(p, err)
			}
		}
	case sourceCertifiedOpen:
		if bytes.HasPrefix(r.command, []byte{'G', 'U', 'Q', 1}) {
			body, err = s.captureConditional(ctx, r)
			break
		}
		var cert graphCertificate
		cert, err = decodeGraphCertificate(r.command)
		if err == nil && cert.scope != s.peer.scope {
			err = errInvalid
		}
		if err == nil {
			var info graphCertifiedSourceInfo
			info, err = s.sources.openCertified(ctx, cert, r.nonce)
			if err == nil {
				body, err = encodeGraphCertifiedSourceInfo(info)
				if err != nil {
					err = errors.Join(err, s.sources.closeSource(info.cookie))
				}
			}
		}
	case sourceOpen:
		var reply allocationReply
		reply, err = s.pump.Allocation(ctx, allocationQuery{kind: observeConfiguration, nonce: r.nonce})
		if err == nil {
			err = reply.err
		}
		if err == nil && (reply.proof.owner != s.host || reply.id != reply.proof.id || reply.question.nonce != r.nonce || reply.question.kind != observeConfiguration) {
			err = errInvalid
		}
		if err == nil {
			var info graphSourceInfo
			info, err = s.sources.open(ctx, reply.proof)
			if err == nil {
				body, err = encodeGraphSourceInfo(info)
				if err != nil {
					err = errors.Join(err, s.sources.closeSource(info.cookie))
				}
			}
		}
	case sourceRoot:
		var root raftlog.ApplicationRoot
		root, err = s.sources.root(ctx, r.cookie, r.maximum)
		if err == nil {
			body, err = boundedEncoding(512, func(w *boundedWriter) { w.add([]byte{'G', 'S', 'B', 1}); emitSourceRoot(w, root) })
		}
	case sourceGet:
		var row raftlog.KV
		var found bool
		row, found, err = s.sources.get(ctx, r.cookie, r.key, r.maximum)
		if err == nil {
			body, err = boundedEncoding(graphSourceFrameBytes-512, func(w *boundedWriter) {
				w.add([]byte{'G', 'S', 'K', 1})
				if found {
					w.tag(1)
					emitSourceKV(w, row)
				} else {
					w.tag(0)
				}
			})
		}
	case sourceScan:
		var page raftlog.ApplicationPage
		page, err = s.sources.scan(ctx, r.cookie, r.key, r.upper, r.after, raftlog.ReadBudget{Rows: r.rows, Bytes: r.maximum})
		if err == nil {
			body, err = encodeGraphSourcePage(page, graphSourceFrameBytes-512)
		}
	case sourceClose:
		err = s.sources.closeSource(r.cookie)
	case sourceCrossRead:
		if len(r.command) != 0 {
			body, err = s.readCapacity(ctx, r)
			break
		}
		var reply crossReply
		reply, err = s.pump.Cross(ctx, crossQuery{tx: r.tx, nonce: r.nonce, phase: r.phase})
		if err == nil {
			err = reply.err
		}
		if err == nil && (reply.proof.owner != s.host || reply.proof.question != (crossQuery{tx: r.tx, nonce: r.nonce, phase: r.phase})) {
			err = errInvalid
		}
		if err == nil {
			var authority crossAuthority
			authority, err = s.host.exportCrossAuthority(reply.proof)
			if err == nil {
				body, err = encodeCrossAuthorityBody(authority.crossAuthorityBody)
			}
		}
	case sourceRequestLookup:
		var reply crossRequestReply
		reply, err = s.pump.Request(ctx, crossRequestQuestion{tx: r.tx, nonce: r.nonce})
		if err == nil {
			err = reply.err
		}
		if err == nil && (reply.proof.owner != s.host || reply.proof.question != (crossRequestQuestion{tx: r.tx, nonce: r.nonce})) {
			err = errInvalid
		}
		if err == nil {
			body, err = encodeCrossRequestBody(reply.proof)
		}
	case sourceSubmit:
		var p allocationProposal
		p, err = decodeGraphSourceProposal(r.command)
		if err == nil && p.target != s.peer.scope {
			err = errInvalid
		}
		if err == nil {
			body, err = s.pump.Submit(ctx, p)
		}
	default:
		err = errInvalid
	}
	if err != nil {
		body = nil
	}
	reply, encodeErr := encodeGraphWireResponse(graphWireResponse{peer: s.peer, issuer: s.host.session, kind: r.kind, sequence: r.sequence, nonce: r.nonce, body: body, err: err})
	if encodeErr != nil {
		return nil, encodeErr
	}
	s.last, s.digest, s.reply = r.sequence, digest, owned(reply)
	return reply, nil
}

func (e *graphAuthenticatedEndpoint) cross(ctx context.Context, tx [16]byte, phase crossPhase, manifestBytes int) (crossAuthority, error) {
	r, err := e.call(ctx, graphSourceRequest{kind: sourceCrossRead, tx: tx, phase: phase}, min(graphSourceFrameBytes, manifestBytes+4096))
	if err != nil {
		return crossAuthority{}, err
	}
	p, err := decodeCrossAuthorityBody(r.body)
	if err != nil {
		return crossAuthority{}, err
	}
	if p.scope != e.peer.scope || p.id.session != r.issuer || p.question.tx != tx || p.question.phase != phase || p.question.nonce != r.nonce {
		return crossAuthority{}, errInvalid
	}
	return sealCrossAuthority(p), nil
}
func (e *graphAuthenticatedEndpoint) lookup(ctx context.Context, tx [16]byte) (crossRequestBody, error) {
	r, err := e.call(ctx, graphSourceRequest{kind: sourceRequestLookup, tx: tx}, graphSourceFrameBytes)
	if err != nil {
		return crossRequestBody{}, err
	}
	p, err := decodeCrossRequestBody(r.body)
	if err != nil {
		return crossRequestBody{}, err
	}
	if p.scope != e.peer.scope || p.id.session != r.issuer || p.question.tx != tx || p.question.nonce != r.nonce {
		return crossRequestBody{}, errInvalid
	}
	return p, nil
}
func (e *graphAuthenticatedEndpoint) submit(ctx context.Context, p allocationProposal) (crossStatus, error) {
	if p.target != e.peer.scope {
		return crossStatus{}, errInvalid
	}
	wire, err := encodeGraphSourceProposal(p)
	if err != nil {
		return crossStatus{}, err
	}
	r, err := e.call(ctx, graphSourceRequest{kind: sourceSubmit, command: wire}, 1024)
	if err != nil {
		return crossStatus{}, err
	}
	status, err := decodeCrossStatus(r.body)
	if err != nil || status.ns != (namespace{e.peer.scope.graph, e.peer.scope.partition}) {
		return crossStatus{}, errors.Join(errInvalid, err)
	}
	return status, nil
}
