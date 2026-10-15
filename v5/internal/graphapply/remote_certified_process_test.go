package graphapply

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// These IPC values are inert references. Opening At still goes through the
// production source Host's persisted-certificate and applied-index validation.
type graphE15ReadRequest struct {
	mode        graphReadMode
	cut         graphCut
	transaction [16]byte
	coordinator uint64
	partitions  []uint64
}

func graphE15ReadPartitionsValid(partitions []uint64) bool {
	if len(partitions) > 16 {
		return false
	}
	var prior uint64
	for _, partition := range partitions {
		if partition <= prior {
			return false
		}
		prior = partition
	}
	return true
}

func encodeGraphE15ReadRequest(r graphE15ReadRequest) ([]byte, error) {
	if r.mode < graphReadFresh || r.mode > graphReadAfter || len(r.cut.certificates) > 16 || !graphE15ReadPartitionsValid(r.partitions) {
		return nil, errInvalid
	}
	return boundedEncoding(16<<10, func(w *boundedWriter) {
		version := byte(1)
		if len(r.partitions) != 0 {
			version = 2
		}
		w.add([]byte{'G', 'E', 'Q', version})
		w.tag(byte(r.mode))
		w.add(r.transaction[:])
		w.u64(r.coordinator)
		w.u32(len(r.cut.certificates))
		for _, cert := range r.cut.certificates {
			b, e := encodeGraphCertificate(cert)
			if e != nil {
				w.err = e
				return
			}
			w.field(b)
		}
		if version == 2 {
			w.u32(len(r.partitions))
			for _, partition := range r.partitions {
				w.u64(partition)
			}
		}
	})
}
func decodeGraphE15ReadRequest(wire []byte) (graphE15ReadRequest, error) {
	version := byte(1)
	if len(wire) >= 4 && wire[3] == 2 {
		version = 2
	}
	b, e := graphBody(wire, string([]byte{'G', 'E', 'Q', version}), 16<<10)
	if e != nil {
		return graphE15ReadRequest{}, e
	}
	c := graphCursor{b: b}
	r := graphE15ReadRequest{mode: graphReadMode(c.tag()), transaction: c.array(), coordinator: c.u64()}
	n := c.count(16, 4)
	if c.err != nil {
		return graphE15ReadRequest{}, c.err
	}
	r.cut.certificates = make([]graphCertificate, n)
	for j := range n {
		r.cut.certificates[j], e = decodeGraphCertificate(c.field(512))
		if e != nil {
			return graphE15ReadRequest{}, e
		}
	}
	if version == 2 {
		n := c.count(16, 8)
		if c.err != nil || n == 0 {
			return graphE15ReadRequest{}, errors.Join(errInvalid, c.err)
		}
		r.partitions = make([]uint64, n)
		for i := range n {
			r.partitions[i] = c.u64()
		}
	}
	if c.err != nil || len(c.b) != 0 || r.mode < graphReadFresh || r.mode > graphReadAfter || !graphE15ReadPartitionsValid(r.partitions) {
		return graphE15ReadRequest{}, errors.Join(errInvalid, c.err)
	}
	return r, nil
}
func encodeGraphE15ReadToken(handle uint64, cut graphCut) ([]byte, error) {
	request, e := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadAt, cut: cut})
	if e != nil {
		return nil, e
	}
	if handle == 0 {
		return nil, errInvalid
	}
	return boundedEncoding(16<<10, func(w *boundedWriter) { w.add([]byte{'G', 'E', 'T', 1}); w.u64(handle); w.field(request) })
}
func decodeGraphE15ReadToken(wire []byte) (uint64, graphCut, error) {
	b, e := graphBody(wire, "GET\x01", 16<<10)
	if e != nil {
		return 0, graphCut{}, e
	}
	c := graphCursor{b: b}
	handle := c.u64()
	r, e := decodeGraphE15ReadRequest(c.field(16 << 10))
	if e != nil || c.err != nil || len(c.b) != 0 || handle == 0 || len(r.cut.certificates) == 0 {
		return 0, graphCut{}, errors.Join(errInvalid, e, c.err)
	}
	return handle, r.cut, nil
}

type graphProcessCertifiedOwner struct {
	read   *certifiedGraphRead
	cancel context.CancelFunc
}
type graphProcessCertifiedSessions struct {
	mu          sync.Mutex
	ctx         context.Context
	coordinator *crossProcessCoordinator
	next        uint64
	pending     int
	owners      map[uint64]graphProcessCertifiedOwner
}

func (s *graphProcessCertifiedSessions) serve(ctx context.Context, r processRequest) ([]byte, error) {
	switch r.op {
	case processSourceCertifiedOpen:
		request, e := decodeGraphE15ReadRequest(r.data)
		if e != nil {
			return nil, e
		}
		s.mu.Lock()
		if len(s.owners)+s.pending >= 2 || s.next == math.MaxUint64 {
			s.mu.Unlock()
			return nil, errLimit
		}
		s.pending++
		s.next++
		id := s.next
		s.mu.Unlock()
		owner, cancel := context.WithCancel(s.ctx)
		stop := context.AfterFunc(ctx, cancel)
		spec := graphReadRequest{mode: request.mode, cut: request.cut}
		if request.mode == graphReadAfter {
			l := s.coordinator.host.machine.limits
			arena, err := graphstate.NewOutputBudget(min(l.sourceBytes, l.outputBytes))
			e = err
			if e == nil {
				var endpoints map[uint64]*graphAuthenticatedEndpoint
				endpoints, e = s.coordinator.endpoints(newGraphRemoteBudget(l, arena))
				if e == nil {
					endpoint := endpoints[request.coordinator]
					if endpoint == nil {
						e = errInvalid
					} else {
						var authority crossAuthority
						authority, e = endpoint.cross(owner, request.transaction, crossDecide, 1<<20)
						if e == nil {
							spec.receipt, e = graphReceiptFromAuthority(authority)
						}
					}
				}
			}
		}
		var read *certifiedGraphRead
		if e == nil {
			read, e = s.coordinator.OpenCertified(owner, spec, request.partitions)
		}
		stop()
		if e == nil {
			e = ctx.Err()
		}
		var token []byte
		if e == nil {
			var cut graphCut
			cut, e = read.Cut()
			if e == nil {
				token, e = encodeGraphE15ReadToken(id, cut)
			}
		}
		s.mu.Lock()
		s.pending--
		if e == nil {
			s.owners[id] = graphProcessCertifiedOwner{read, cancel}
		}
		s.mu.Unlock()
		if e != nil {
			cancel()
			if read != nil {
				e = errors.Join(e, read.Close())
			}
			return nil, e
		}
		return token, nil
	case processSourceCertifiedObserve:
		s.mu.Lock()
		owner, ok := s.owners[r.index]
		s.mu.Unlock()
		if !ok {
			return nil, errInvalid
		}
		ids, e := decodeGraphProcessObserveIDs(r.data)
		if e != nil {
			return nil, e
		}
		view, e := owner.read.ReadView()
		if e != nil {
			return nil, e
		}
		facts := make([]graphProcessFactTuple, len(ids))
		for j, id := range ids {
			facts[j].id = id
			facts[j].fact, e = graphProcessSourceFact(ctx, s.coordinator.host, view, id)
			if e != nil {
				return nil, e
			}
		}
		return encodeGraphProcessObservationBatch(facts)
	case processSourceCertifiedClose:
		s.mu.Lock()
		owner, ok := s.owners[r.index]
		delete(s.owners, r.index)
		s.mu.Unlock()
		if !ok {
			return nil, errInvalid
		}
		owner.cancel()
		return nil, owner.read.Close()
	default:
		return nil, errInvalid
	}
}
func (s *graphProcessCertifiedSessions) close() error {
	s.mu.Lock()
	owners := s.owners
	s.owners = nil
	s.mu.Unlock()
	var e error
	for _, owner := range owners {
		owner.cancel()
		e = errors.Join(e, owner.read.Close())
	}
	return e
}
