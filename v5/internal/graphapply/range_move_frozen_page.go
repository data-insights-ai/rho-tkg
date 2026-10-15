package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// An authenticated session pins C graph data AND the later completed manifest
// coordinate. Page receipts are read at the latter; page contents at the former.
// This is immutable data access, not a new current-state or quorum observation.
func (s *graphSourceServer) moveFrozenPage(ctx context.Context, request graphSourceRequest) (body []byte, err error) {
	if s == nil || ctx == nil {
		return nil, errInvalid
	}
	question, err := decodeRangeMoveQuery(request.command)
	if err != nil || question.page == 0 || question.origin != 0 {
		return nil, errors.Join(errInvalid, err)
	}
	question.nonce = request.nonce
	lease, err := s.lease(request.cookie)
	if err != nil {
		return nil, err
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.moveMeta == nil || question.move != lease.moveID {
		return nil, errInvalid
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lease.ctx, cancel)
	defer func() { stop(); cancel() }()
	if err := lease.begin(ctx, 0, 2048); err != nil {
		return nil, err
	}
	l := s.host.machine.limits
	l.allocation.readRows = min(l.allocation.readRows, question.sourceRows, lease.limits.rows-lease.rows)
	l.allocation.readBytes = min(l.allocation.readBytes, question.sourceBytes, lease.limits.bytes-lease.bytes)
	allowance := min(l.outputBytes, question.outputBytes, lease.limits.output-lease.output)
	if l.allocation.readRows < 1 || l.allocation.readBytes < 2048 || allowance < 2048 {
		return nil, errLimit
	}
	arena, err := graphstate.NewOutputBudget(allowance)
	if err != nil {
		return nil, err
	}
	if err := arena.Reserve(2048); err != nil {
		return nil, outputBudgetFailure(err)
	}
	root, err := lease.moveMeta.RootBounded(ctx, 172)
	if err != nil {
		return nil, err
	}
	q := reader{ctx: ctx, view: lease.moveMeta, base: root, ns: s.host.machine.ns, limits: l.allocation, arena: arena, bytes: cap(root.Image)}
	defer func() {
		lease.rows += q.rows
		lease.bytes += q.bytes
		lease.output += allowance - arena.Remaining()
		if err != nil {
			body = nil
		}
	}()
	wire, present, err := q.getBounded(rangeMoveProgressKey(q.ns, question.move), 1<<20)
	if err != nil || !present {
		return nil, errors.Join(errCorrupt, err)
	}
	progress, err := decodeRangeMoveProgress(wire, arena)
	if err != nil || !progress.transferComplete() || progress.imported != nil || question.spans != rangeMoveSpans(s.host.machine.SemanticContractID()) || question.page > progress.graphPages+progress.replayPageCount() {
		return nil, errors.Join(errInvalid, err)
	}
	intentRecord, present, err := crossControl(&q, rangeMoveControlKey(q.ns, question.move, 'I'), 1024)
	if err != nil || !present || sha256.Sum256(intentRecord.Value) != lease.moveIntent {
		return nil, errors.Join(errCorrupt, err)
	}
	intent, err := decodeRangeMoveIntent(intentRecord.Value)
	if err != nil || intent.source != s.host.machine.scope() || progress.descriptor.Source.Index != lease.info.root.Index || progress.descriptor.Source.ImageHash != lease.info.root.ImageHash || !bytes.Equal(progress.descriptor.Source.Image[:], lease.info.root.Image) {
		return nil, errors.Join(errInvalid, err)
	}
	receipt, present, err := crossControl(&q, rangeMovePageControlKey(q.ns, question.move, question.page), (129<<10)+1)
	if err != nil || !present || len(receipt.Value) < 2 {
		return nil, errors.Join(errCorrupt, err)
	}
	var page []byte
	switch receipt.Value[0] {
	case 1:
		before, e := graphstore.DecodeRangeMoveCursor(receipt.Value[1:], arena)
		if e != nil {
			return nil, e
		}
		limits := l.graph
		limits.MaxSourceRows = min(limits.MaxSourceRows, q.limits.readRows-q.rows)
		limits.MaxSourceBytes = min(limits.MaxSourceBytes, q.limits.readBytes-q.bytes)
		if limits.MaxSourceRows < 1 || limits.MaxSourceBytes < 1 {
			return nil, errLimit
		}
		p, work, e := graphstore.ExportRangeMovePage(ctx, lease.view, progress.descriptor, before, raftlog.ReadBudget{Rows: intent.pageRows, Bytes: intent.pageBytes}, l.catalog, limits, arena)
		if err := errors.Join(e, chargeGraphWork(&q, work)); err != nil {
			return nil, err
		}
		page, err = graphstore.EncodeRangeMovePage(p, arena)
	case 3:
		if !rangeMoveSpans(s.host.machine.SemanticContractID()) {
			return nil, errInvalid
		}
		before, e := decodeRangeMoveReplayCursor(receipt.Value[1:], arena)
		if e != nil {
			return nil, e
		}
		frozen := q
		frozen.view, frozen.base = lease.view, lease.info.root
		batch, e := exportMoveReplayBatch(&frozen, q.ns, before, intent.pageBytes, intent.pageRows)
		q.rows, q.bytes = frozen.rows, frozen.bytes
		if e != nil {
			return nil, e
		}
		page, err = encodeMoveReplayBatch(batch, arena, intent.pageBytes)
	case 2:
		if rangeMoveSpans(s.host.machine.SemanticContractID()) {
			return nil, errInvalid
		}
		before, e := decodeRangeMoveReplayCursor(receipt.Value[1:], arena)
		if e != nil {
			return nil, e
		}
		frozen := q
		frozen.view = lease.view
		frozen.base = lease.info.root
		p, e := exportRangeMoveReplayPage(&frozen, q.ns, before, raftlog.ReadBudget{Rows: intent.pageRows, Bytes: intent.pageBytes})
		q.rows, q.bytes = frozen.rows, frozen.bytes
		if e != nil {
			return nil, e
		}
		page, err = encodeRangeMoveReplayPage(p, arena)
	default:
		return nil, errCorrupt
	}
	if err != nil {
		return nil, err
	}
	if err := arena.Reserve(4096 + 8*len(page)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	query, err := encodeRangeMoveQuery(question)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(graphSourceFrameBytes-512, func(w *boundedWriter) {
		w.add([]byte{'G', 'M', 'V', 1})
		w.field(query)
		w.add(lease.moveIntent[:])
		w.u64(root.Index)
		w.add(root.ImageHash[:])
		w.u32(q.rows)
		w.u32(q.bytes)
		w.u32(allowance - arena.Remaining())
		w.tag(receipt.Value[0])
		w.field(page)
	})
}

func (s *remoteGraphSource) moveFrozenPage(ctx context.Context, sequence uint64) (page []byte, err error) {
	if s == nil || ctx == nil || s.move == nil || sequence == 0 || sequence > uint64(rangeMovePageLimit(s.move.scope.semantic)) {
		return nil, errInvalid
	}
	if err := s.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	if s.closed {
		return nil, raftlog.ErrClosed
	}
	if err := errors.Join(ctx.Err(), s.ctx.Err()); err != nil {
		return nil, err
	}
	if time.Now().After(s.deadline) {
		return nil, graphstore.ErrSourceUnavailable
	}
	q := rangeMoveQuery{spans: rangeMoveSpans(s.move.scope.semantic), move: s.move.moveID, page: sequence, nonce: [16]byte{1}}
	b := s.endpoint.budget
	const framing = graphSourceFrameBytes + 2048
	b.mu.Lock()
	q.sourceRows = min(1<<20, b.maxRows-b.rows)
	q.sourceBytes = min(64<<20, b.maxBytes-b.bytes-framing)
	q.outputBytes = min(64<<20, b.output.Remaining()-4*framing-1024)
	b.mu.Unlock()
	if !q.valid() {
		return nil, errLimit
	}
	wire, err := encodeRangeMoveQuery(q)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			b.mu.Lock()
			b.rows += q.sourceRows
			b.bytes += q.sourceBytes
			owned := b.output.Reserve(q.outputBytes)
			b.mu.Unlock()
			err = errors.Join(err, outputBudgetFailure(owned))
			page = nil
		}
	}()
	response, err := s.endpoint.call(ctx, graphSourceRequest{kind: sourceMoveRead, cookie: s.info.cookie, command: wire}, graphSourceFrameBytes)
	if err != nil {
		return nil, err
	}
	body, err := graphBody(response.body, "GMV\x01", graphSourceFrameBytes-512)
	if err != nil {
		return nil, err
	}
	c := graphCursor{b: body}
	query := c.field(160)
	var intent [32]byte
	copy(intent[:], c.take(32))
	index := c.u64()
	var image [32]byte
	copy(image[:], c.take(32))
	rows, nativeBytes, owned := c.count(1<<20, 0), c.count(64<<20, 0), c.count(64<<20, 0)
	kind := c.tag()
	data := c.field(1 << 20)
	actual, e := decodeRangeMoveQuery(query)
	q.nonce = response.nonce
	if c.err != nil || len(c.b) != 0 || e != nil || actual != q || intent != s.move.intentHash || index != s.move.manifestIndex || image != s.move.manifestHash || kind < 1 || kind > func() byte {
		if q.spans {
			return 3
		}
		return 2
	}() || q.spans && kind == 2 || rows > q.sourceRows || nativeBytes > q.sourceBytes || owned > q.outputBytes {
		return nil, errors.Join(errInvalid, c.err, e)
	}
	b.mu.Lock()
	e = b.output.Reserve(owned + 64 + len(data))
	if e == nil && rows <= b.maxRows-b.rows && nativeBytes <= b.maxBytes-b.bytes {
		b.rows += rows
		b.bytes += nativeBytes
	} else if e == nil {
		e = errLimit
	}
	b.mu.Unlock()
	if e != nil {
		return nil, errors.Join(errLimit, e)
	}
	return append([]byte{kind}, data...), nil
}
