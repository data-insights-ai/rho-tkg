package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const nativeIncidentMarker = "GCIS1"
const (
	nativeIncidentOpen byte = iota + 1
	nativeIncidentNext
	nativeIncidentClose
	nativeIncidentCloseEndpoint // guard-only actual native mutation, never cost accounting
)

type nativeGraphCostIncidentCookie struct {
	Owner   uint64
	Request [16]byte
}
type nativeGraphCostIncidentRequest struct {
	Action            byte
	Cut               []byte
	Endpoint, Life    uint64
	Rows, Invocations int
	LifetimeNanos     int64
	Cookie            nativeGraphCostIncidentCookie
	Sequence          uint64
}
type nativeGraphCostIncidentMeter struct {
	mu                              sync.Mutex
	Calls, RequestBytes, ReplyBytes uint64
}
type nativeGraphCostIncidentTransport struct {
	actual graphPeerWireTransport
	meter  *nativeGraphCostIncidentMeter
}

func (t *nativeGraphCostIncidentTransport) Peer() graphPeerIdentity { return t.actual.Peer() }
func (t *nativeGraphCostIncidentTransport) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	t.meter.mu.Lock()
	if t.meter.Calls == math.MaxUint64 || uint64(len(wire)) > math.MaxUint64-t.meter.RequestBytes {
		t.meter.mu.Unlock()
		return nil, errLimit
	}
	t.meter.Calls++
	t.meter.RequestBytes += uint64(len(wire))
	t.meter.mu.Unlock()
	reply, err := t.actual.RoundTrip(ctx, wire)
	t.meter.mu.Lock()
	if uint64(len(reply)) > math.MaxUint64-t.meter.ReplyBytes {
		err = errors.Join(err, errLimit)
	} else {
		t.meter.ReplyBytes += uint64(len(reply))
	}
	t.meter.mu.Unlock()
	return reply, err
}
func (m *nativeGraphCostIncidentMeter) snapshot() (uint64, uint64, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Calls, m.RequestBytes, m.ReplyBytes
}

type nativeGraphCostIncidentReceipt struct {
	Cookie                         nativeGraphCostIncidentCookie
	Sequence                       uint64
	DeadlineUnixNanos              int64
	Index                          nativeGraphCostIndexPage
	Charged                        [7]int // source rows/bytes, decoded cells, RPC calls, wire bytes, output bytes, invocations
	CompletedSourceWork            graphstore.PageWork
	RPCs, RequestBytes, ReplyBytes uint64 // actual dispatched requests and delivered transport frames
	ElapsedNanos                   int64
	Accounting                     string
}
type nativeGraphCostIncidentOwner struct {
	mu               sync.Mutex
	gate             graphRequestGate
	stream           *certifiedIncidentStream
	cancel           context.CancelFunc
	meter            *nativeGraphCostIncidentMeter
	cookie           nativeGraphCostIncidentCookie
	deadline         time.Time
	started          time.Time
	work             graphstore.PageWork
	staged           nativeGraphCostIndexPage
	lastSequence     uint64
	cached           []byte
	replayed, closed bool
}
type nativeGraphCostIncidentSessions struct {
	mu          sync.Mutex
	ctx         context.Context
	coordinator *crossProcessCoordinator
	spec        *nativeGraphCostSpec
	next        uint64
	pending     bool
	owner       *nativeGraphCostIncidentOwner // exactly one pin+chunk query under ordinary source lease2
}

func nativeGraphCostIncidentInput(r nativeGraphCostIncidentRequest) ([]byte, error) {
	wire, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(wire) > 16<<10 {
		return nil, errLimit
	}
	return append([]byte(nativeIncidentMarker), wire...), nil
}
func nativeGraphCostIncidentDecodeInput(r processRequest) (nativeGraphCostIncidentRequest, error) {
	var q nativeGraphCostIncidentRequest
	if r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 || len(r.data) > 16<<10+len(nativeIncidentMarker) || !bytes.HasPrefix(r.data, []byte(nativeIncidentMarker)) {
		return q, errInvalid
	}
	d := json.NewDecoder(bytes.NewReader(r.data[len(nativeIncidentMarker):]))
	d.DisallowUnknownFields()
	if err := d.Decode(&q); err != nil {
		return q, errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return q, errors.Join(errInvalid, err)
	}
	switch q.Action {
	case nativeIncidentOpen:
		if q.Endpoint == 0 || q.Rows < 1 || q.Rows > 16 || q.Invocations < 1 || q.Invocations > 1_000_002 || q.LifetimeNanos <= 0 || q.LifetimeNanos > int64(time.Minute) || q.Cookie != (nativeGraphCostIncidentCookie{}) || q.Sequence != 0 || len(q.Cut) == 0 {
			return q, errInvalid
		}
	case nativeIncidentNext, nativeIncidentClose:
		if q.Cookie.Owner == 0 || q.Cookie.Request == ([16]byte{}) || q.Action == nativeIncidentNext && q.Sequence == 0 || q.Action == nativeIncidentClose && q.Sequence != 0 || len(q.Cut) != 0 || q.Endpoint != 0 || q.Life != 0 || q.Rows != 0 || q.Invocations != 0 || q.LifetimeNanos != 0 {
			return q, errInvalid
		}
	case nativeIncidentCloseEndpoint:
		if q.Endpoint == 0 || q.Life != 0 || q.Cookie != (nativeGraphCostIncidentCookie{}) || q.Sequence != 0 || len(q.Cut) != 0 || q.Rows != 0 || q.Invocations != 0 || q.LifetimeNanos != 0 {
			return q, errInvalid
		}
	default:
		return q, errInvalid
	}
	return q, nil
}

// Cleanup admission selects only the existing one-control-worker lane. The
// cookie is checked by serve; this predicate grants no owner/data authority.
func nativeGraphCostIncidentCloseControl(r processRequest) bool {
	if r.op != processCostObserve || !bytes.HasPrefix(r.data, []byte(nativeIncidentMarker)) {
		return false
	}
	q, err := nativeGraphCostIncidentDecodeInput(r)
	return err == nil && q.Action == nativeIncidentClose
}
func nativeGraphCostIncidentBudget(l materializerLimits, rows, invocations int) (certifiedIncidentBudget, error) {
	grant, err := incidentGrant(l, rows)
	if err != nil {
		return certifiedIncidentBudget{}, err
	}
	var values [7]int
	for i, n := range grant.values() {
		values[i], err = incidentProduct(n, invocations)
		if err != nil {
			return certifiedIncidentBudget{}, err
		}
	}
	return certifiedIncidentBudget{values[0], values[1], values[2], values[3], values[4], values[5], values[6]}, nil
}
func (s *nativeGraphCostIncidentSessions) serve(ctx context.Context, r processRequest) ([]byte, error) {
	if s == nil || ctx == nil || s.coordinator == nil || s.spec == nil {
		return nil, errInvalid
	}
	q, err := nativeGraphCostIncidentDecodeInput(r)
	if err != nil {
		return nil, err
	}
	if err := errors.Join(ctx.Err(), s.ctx.Err()); err != nil {
		return nil, err
	}
	if q.Action == nativeIncidentCloseEndpoint {
		if q.Endpoint != s.spec.remoteNode() && q.Endpoint != 3 {
			return nil, errInvalid
		}
		h := s.coordinator.host
		h.mu.Lock()
		cfg, err := h.currentConfiguration()
		h.mu.Unlock()
		if err != nil {
			return nil, err
		}
		scope, err := temporal.All(cfg.defaultAxis.Axis())
		if err != nil {
			return nil, err
		}
		nonce, err := graphReadNonce()
		if err != nil {
			return nil, err
		}
		result, err := s.coordinator.Run(ctx, requestID(nonce), []graphstate.Operation{{Kind: graphstate.Close, Owner: graphstate.EntityID(q.Endpoint), Life: graphstate.LifeID(q.Endpoint + 1), Scope: scope}}, 99, nil)
		if err != nil {
			return nil, err
		}
		return encodeGraphProcessCoordinatorResult(result, nil)
	}
	if q.Action == nativeIncidentOpen {
		return s.open(ctx, q)
	}
	s.mu.Lock()
	owner := s.owner
	if owner == nil || owner.cookie != q.Cookie {
		s.mu.Unlock()
		return nil, errInvalid
	}
	if q.Action == nativeIncidentClose {
		s.owner = nil
	}
	s.mu.Unlock()
	if q.Action == nativeIncidentClose {
		err := owner.close()
		if err != nil {
			return nil, err
		}
		receipt := owner.receipt(0, nativeGraphCostIndexPage{})
		return nativeGraphCostIncidentEncodeReceipt(receipt)
	}
	return owner.next(ctx, q.Sequence)
}
func (s *nativeGraphCostIncidentSessions) open(ctx context.Context, q nativeGraphCostIncidentRequest) (out []byte, err error) {
	request, err := decodeGraphE15ReadRequest(q.Cut)
	if err != nil || request.mode != graphReadAt || len(request.cut.certificates) != 2 || request.transaction != ([16]byte{}) || request.coordinator != 0 {
		return nil, errors.Join(errInvalid, err)
	}
	s.mu.Lock()
	if s.owner != nil || s.pending || s.next == math.MaxUint64 {
		s.mu.Unlock()
		return nil, errLimit
	}
	s.pending = true
	s.next++
	id := s.next
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.pending = false; s.mu.Unlock() }()
	c := s.coordinator
	h := c.host
	h.mu.Lock()
	cfg, err := h.currentConfiguration()
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	at, err := temporal.RationalPosition(cfg.defaultAxis.Axis(), temporal.RationalInt64(0))
	if err != nil {
		return nil, err
	}
	point, err := temporal.Point(at)
	if err != nil {
		return nil, err
	}
	budget, err := nativeGraphCostIncidentBudget(h.machine.limits, q.Rows, q.Invocations)
	if err != nil {
		return nil, err
	}
	meter := &nativeGraphCostIncidentMeter{}
	transports := make(map[uint64]graphPeerWireTransport, len(c.transports))
	for partition, actual := range c.transports {
		transports[partition] = &nativeGraphCostIncidentTransport{actual, meter}
	}
	// Every endpoint shares the existing authenticated association/stream; the
	// wrapper observes frames, and grants no new peer or source authority.
	observed := &crossProcessCoordinator{host: h, transports: transports, streams: c.streams}
	ownerCtx, cancel := context.WithCancel(s.ctx)
	stop := context.AfterFunc(ctx, cancel)
	started := time.Now()
	stream, err := observed.OpenIncidentStream(ownerCtx, request.cut, nil, graphstate.IncidentPredicate{Endpoint: graphstate.EntityID(q.Endpoint), Life: graphstate.LifeID(q.Life), Window: point}, at, graphstate.Effective, q.Rows, started.Add(time.Duration(q.LifetimeNanos)), budget)
	stop()
	if err == nil {
		err = errors.Join(ctx.Err(), s.ctx.Err())
	}
	if err != nil {
		cancel()
		if stream != nil {
			err = errors.Join(err, stream.Close())
		}
		return nil, err
	}
	// The stream owns its cancellation; this detached parent stays alive until
	// Close/deadline and is never the one-call worker context.
	context.AfterFunc(stream.ctx, cancel)
	deadline, _ := stream.ctx.Deadline()
	stream.mu.Lock()
	var pinWork graphstore.PageWork
	if stream.pin != nil {
		pinWork = stream.pin.Work()
	}
	err = stream.ctx.Err()
	stream.mu.Unlock()
	if err != nil {
		cancel()
		return nil, errors.Join(err, stream.Close())
	}
	owner := &nativeGraphCostIncidentOwner{gate: newGraphRequestGate(), stream: stream, cancel: cancel, meter: meter, cookie: nativeGraphCostIncidentCookie{id, stream.request}, deadline: deadline, started: started, work: pinWork}
	stream.hydrateChunk = func(ctx context.Context, v graphstate.ReadView, chunk *certifiedIncidentChunk) error {
		page := nativeGraphCostIndexPage{Kind: 1, Endpoint: q.Endpoint, Life: q.Life, Cursor: chunk.sequence, Complete: chunk.complete, Rows: make([]nativeGraphCostIndexRow, 0, len(chunk.rows))}
		if !chunk.complete {
			page.Next = chunk.sequence + 1
		}
		for _, projection := range chunk.rows {
			entity := nativeLifecycleEntity{ID: uint64(projection.Record.ID), Found: projection.Exists, Kind: byte(projection.Record.Kind), Source: uint64(projection.Record.Source), Target: uint64(projection.Record.Target), Mode: byte(projection.Record.Mode), Type: projection.Record.Type, Effective: projection.Active, EffectiveLife: uint64(projection.Life), Labels: projection.Labels}
			var cellErr error
			entity.Presence, cellErr = nativeLifecycleCellRead(ctx, v, graphstate.ComponentKey{Owner: projection.Record.ID, Kind: graphstate.Presence}, point, at, h.machine.limits.graph.Planner.Component)
			if cellErr != nil {
				return cellErr
			}
			entity.Property, cellErr = nativeLifecycleCellRead(ctx, v, graphstate.ComponentKey{Owner: projection.Record.ID, Life: projection.Life, Kind: graphstate.ScalarProperty, Name: "p"}, point, at, h.machine.limits.graph.Planner.Component)
			if cellErr != nil {
				return cellErr
			}
			life, e := v.Life(ctx, projection.Record.ID, projection.Life)
			if e != nil || !life.Found {
				return errors.Join(errCorrupt, e)
			}
			page.Rows = append(page.Rows, nativeGraphCostIndexRow{entity, uint64(life.Record.SourceLife), uint64(life.Record.TargetLife)})
		}
		owner.staged = page
		return nil
	}
	out, err = nativeGraphCostIncidentEncodeReceipt(owner.receipt(0, nativeGraphCostIndexPage{}))
	if err != nil {
		return nil, errors.Join(err, owner.close())
	}
	s.mu.Lock()
	err = errors.Join(ctx.Err(), s.ctx.Err(), stream.ctx.Err())
	if err == nil {
		s.owner = owner
	}
	s.mu.Unlock()
	if err != nil {
		return nil, errors.Join(err, owner.close())
	}
	return out, nil
}
func nativeIncidentWork(a, b graphstore.PageWork) graphstore.PageWork {
	return graphstore.PageWork{Records: a.Records + b.Records, Bytes: a.Bytes + b.Bytes, DirectoryPages: a.DirectoryPages + b.DirectoryPages, CheckpointPages: a.CheckpointPages + b.CheckpointPages, PatchPages: a.PatchPages + b.PatchPages, DecodedCells: a.DecodedCells + b.DecodedCells}
}
func (o *nativeGraphCostIncidentOwner) receipt(sequence uint64, page nativeGraphCostIndexPage) nativeGraphCostIncidentReceipt {
	calls, request, reply := o.meter.snapshot()
	return nativeGraphCostIncidentReceipt{Cookie: o.cookie, Sequence: sequence, DeadlineUnixNanos: o.deadline.UnixNano(), Index: page, Charged: o.stream.Charged().values(), CompletedSourceWork: o.work, RPCs: calls, RequestBytes: request, ReplyBytes: reply, ElapsedNanos: time.Since(o.started).Nanoseconds(), Accounting: "Completed capture/chunk PageWork; failures retire full prepaid grants. Source RPC/frame bytes include dispatch failures and delivered replies, including Close. Output is prepaid consumptive representation, not heap/RSS. Same fixed source TTL; no million-degree runtime capacity claim."}
}
func (o *nativeGraphCostIncidentOwner) next(ctx context.Context, sequence uint64) ([]byte, error) {
	if !o.gate.tryLock() {
		return nil, errLimit
	}
	defer o.gate.unlock()
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return nil, errInvalid
	}
	if err := errors.Join(ctx.Err(), o.stream.ctx.Err()); err != nil {
		o.mu.Unlock()
		return nil, err
	}
	if sequence == o.lastSequence && o.cached != nil {
		if o.replayed {
			o.mu.Unlock()
			return nil, errLimit
		}
		// Exactly one replay/copy is funded within the original finite output grant.
		o.replayed = true
		out := bytes.Clone(o.cached)
		o.mu.Unlock()
		return out, nil
	}
	o.mu.Unlock()
	var staged []byte
	chunk, err := o.stream.Next(ctx, o.cookie.Request, sequence, func(chunk *certifiedIncidentChunk) error {
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.closed {
			return context.Canceled
		}
		receipt := o.receipt(sequence, o.staged)
		receipt.CompletedSourceWork = nativeIncidentWork(o.work, chunk.work)
		var err error
		staged, err = nativeGraphCostIncidentEncodeReceipt(receipt)
		return err
	})
	if err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.stream.ctx.Err() != nil {
		return nil, errors.Join(context.Canceled, o.stream.ctx.Err())
	}
	o.work = nativeIncidentWork(o.work, chunk.work)
	o.lastSequence, o.cached, o.replayed = sequence, staged, false
	return bytes.Clone(staged), nil
}
func (o *nativeGraphCostIncidentOwner) close() error {
	o.mu.Lock()
	o.closed = true
	o.mu.Unlock()
	o.cancel()
	return o.stream.Close()
}
func (s *nativeGraphCostIncidentSessions) close() error {
	if s == nil {
		return errInvalid
	}
	s.mu.Lock()
	o := s.owner
	s.owner = nil
	s.mu.Unlock()
	if o != nil {
		return o.close()
	}
	return nil
}
func nativeGraphCostIncidentEncodeReceipt(r nativeGraphCostIncidentReceipt) ([]byte, error) {
	if len(r.Index.Rows) > 16 {
		return nil, errLimit
	}
	rows := r.Index.Rows
	r.Index.Rows = nil
	metadata, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(metadata) > 8<<10 {
		return nil, errLimit
	}
	return boundedEncoding(nativeGraphCostPageBytes, func(w *boundedWriter) {
		w.add([]byte("GCR\x01"))
		w.field(metadata)
		w.u32(len(rows))
		for _, row := range rows {
			wire, err := json.Marshal(row)
			if err != nil {
				w.err = err
				return
			}
			if len(wire) > nativeLifecycleBytes {
				w.err = errLimit
				return
			}
			w.field(wire)
		}
	})
}
func nativeGraphCostIncidentDecodeReceipt(wire []byte) (nativeGraphCostIncidentReceipt, error) {
	var r nativeGraphCostIncidentReceipt
	if len(wire) > nativeGraphCostPageBytes {
		return r, errLimit
	}
	body, err := graphBody(wire, "GCR\x01", nativeGraphCostPageBytes)
	if err != nil {
		return r, err
	}
	c := graphCursor{b: body}
	metadata := c.field(8 << 10)
	d := json.NewDecoder(bytes.NewReader(metadata))
	d.DisallowUnknownFields()
	if err := d.Decode(&r); err != nil {
		return r, errors.Join(errCorrupt, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return r, errors.Join(errCorrupt, err)
	}
	if r.Index.Rows != nil {
		return r, errCorrupt
	}
	count := c.count(16, 4)
	if c.err != nil {
		return r, errors.Join(errCorrupt, c.err)
	}
	r.Index.Rows = make([]nativeGraphCostIndexRow, count)
	for i := range r.Index.Rows {
		row := c.field(nativeLifecycleBytes)
		if c.err != nil {
			return r, errors.Join(errCorrupt, c.err)
		}
		d := json.NewDecoder(bytes.NewReader(row))
		d.DisallowUnknownFields()
		if err := d.Decode(&r.Index.Rows[i]); err != nil {
			return r, errors.Join(errCorrupt, err)
		}
		if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
			return r, errors.Join(errCorrupt, err)
		}
	}
	if c.err != nil || len(c.b) != 0 || r.Cookie.Owner == 0 || r.Cookie.Request == ([16]byte{}) || r.DeadlineUnixNanos <= 0 || r.ElapsedNanos < 0 || r.Sequence != 0 && (r.Index.Kind != 1 || r.Index.Endpoint == 0 || r.Sequence == math.MaxUint64 || r.Index.ValueFound || r.Index.ValueID != 0 || r.Index.Value != 0 || r.Index.Cursor != r.Sequence || r.Index.Complete != (r.Index.Next == 0) || !r.Index.Complete && r.Index.Next != r.Sequence+1) {
		return r, errors.Join(errCorrupt, c.err)
	}
	for _, n := range r.Charged {
		if n < 1 {
			return r, errCorrupt
		}
	}
	if r.RPCs > uint64(r.Charged[3]) || r.RequestBytes > uint64(r.Charged[4]) || r.ReplyBytes > uint64(r.Charged[4])-r.RequestBytes {
		return r, errCorrupt
	} // #nosec G115 -- positive prepaid ledger admitted above; subtraction ordered after request bound.
	return r, nil
}
func nativeGraphCostIncidentCall(t *testing.T, n *certifiedProcessNetwork, q nativeGraphCostIncidentRequest) nativeGraphCostIncidentReceipt {
	t.Helper()
	wire, err := nativeGraphCostIncidentInput(q)
	if err != nil {
		t.Fatal(err)
	}
	response := n.all[2].call(t, processRequest{op: processCostObserve, data: wire})
	receipt, err := nativeGraphCostIncidentDecodeReceipt(response.aux)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
func nativeGraphCostIncidentOpenAt(t *testing.T, n *certifiedProcessNetwork, cut graphCut, endpoint, life uint64, rows, invocations int) nativeGraphCostIncidentReceipt {
	t.Helper()
	wire, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadAt, cut: cut})
	if err != nil {
		t.Fatal(err)
	}
	return nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentOpen, Cut: wire, Endpoint: endpoint, Life: life, Rows: rows, Invocations: invocations, LifetimeNanos: int64(time.Minute)})
}

// Each endpoint/life uses one actual child owner/cookie and monotone sequence.
// No At per page, ID reset, census or whole-degree allocation occurs in the parent.
func nativeGraphCostIncidentIndexes(t *testing.T, n *certifiedProcessNetwork, spec nativeGraphCostSpec, cut graphCut, want int64, stream *json.Encoder) {
	t.Helper()
	metrics, err := os.OpenFile(filepath.Join(n.dir, "incident-query-pages.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := metrics.Close(); err != nil {
			t.Error(err)
		}
	}()
	raw := json.NewEncoder(metrics)
	for _, endpoint := range []uint64{1, 3, spec.remoteNode()} {
		for _, life := range []uint64{0, endpoint + 1} {
			invocations := int((spec.Facts+15)/16) + 2 // #nosec G115 -- validated spec.Facts<=1M.
			opened := nativeGraphCostIncidentOpenAt(t, n, cut, endpoint, life, 16, invocations)
			if err := raw.Encode(opened); err != nil {
				t.Fatal(err)
			}
			var seen, nextFact uint64
			complete := false
			for sequence := uint64(1); sequence <= spec.Facts+1; sequence++ {
				receipt := nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: opened.Cookie, Sequence: sequence})
				page := receipt.Index
				if receipt.Cookie != opened.Cookie || receipt.Sequence != sequence || receipt.DeadlineUnixNanos != opened.DeadlineUnixNanos || page.Endpoint != endpoint || page.Life != life {
					t.Fatal("incident owner/sequence/cut replaced", receipt)
				}
				for _, row := range page.Rows {
					for nextFact < spec.Facts && endpoint != 1 && (endpoint == 3) == spec.cross(nextFact) {
						nextFact++
					}
					if nextFact >= spec.Facts {
						t.Fatal("extra incident candidate", endpoint, row)
					}
					edge, bound, target, targetLife := spec.edge(nextFact)
					x := row.Entity
					if x.ID != edge || !x.Found || !x.Effective || x.Kind != byte(graphstate.Relationship) || x.Type != "edge" || x.Mode != byte(graphstate.LifeBound) || x.Source != 1 || x.Target != target || x.EffectiveLife != bound || !x.Presence.Present || x.Presence.Kind != "life" || x.Presence.Handle != bound || len(x.Labels) != 0 || row.SourceLife != 2 || row.TargetLife != targetLife || !x.Property.Present || x.Property.Kind != "i64" || x.Property.Revision == 0 || x.Property.I64 != want || x.Property.Handle != uint64(want) || x.Property.Provenance != 4+nextFact+boolUint(want == 6)*spec.Facts {
						t.Fatal("exact retained incident hydration", endpoint, life, nextFact, row)
					}
					seen++
					nextFact++
				}
				if err := stream.Encode(page); err != nil {
					t.Fatal(err)
				}
				if err := raw.Encode(receipt); err != nil {
					t.Fatal(err)
				}
				if page.Complete {
					replay := nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: opened.Cookie, Sequence: sequence})
					if !reflect.DeepEqual(replay, receipt) {
						t.Fatal("last genuine page replay differs")
					}
					complete = true
					break
				}
			}
			for nextFact < spec.Facts && endpoint != 1 && (endpoint == 3) == spec.cross(nextFact) {
				nextFact++
			}
			if !complete || nextFact != spec.Facts || endpoint == 1 && seen != spec.Facts {
				t.Fatal("incident query omitted/incomplete", endpoint, life, seen)
			}
			closed := nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentClose, Cookie: opened.Cookie})
			if err := raw.Encode(closed); err != nil {
				t.Fatal(err)
			}
		}
	}
}
