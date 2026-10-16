package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
)

const nativeEdgeRecords = "GEP1"

type nativeGraphEdgeUniqueRecordRequest struct {
	Certificate []byte
	After       uint64
}
type nativeGraphEdgeUniqueRecordPage struct {
	Certificate []byte
	After, Next uint64
	Complete    bool
	Frames      []nativeColumnFrame
}

// The cost legacy reducer deliberately recognizes its old continuous contract
// only. The fresh family uses actual native frames, including authenticated
// pin/retention originals; GPO/GGO are never waved through by magic alone.
func nativeGraphEdgeUniqueRecords(ctx context.Context, h *allocationHost, r processRequest) (wire []byte, err error) {
	if ctx == nil || h == nil || h.machine == nil || h.machine.SemanticContractID() != durableColumnGraphSemanticContractID() || r.entity != 0 || r.index != 0 || !bytes.HasPrefix(r.data, []byte(nativeEdgeRecords)) || len(r.data) > 4096 {
		return nil, errInvalid
	}
	var request nativeGraphEdgeUniqueRecordRequest
	d := json.NewDecoder(bytes.NewReader(r.data[len(nativeEdgeRecords):]))
	d.DisallowUnknownFields()
	if err := d.Decode(&request); err != nil {
		return nil, errors.Join(errInvalid, err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.Join(errInvalid, err)
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonical, r.data[len(nativeEdgeRecords):]) {
		return nil, errInvalid
	}
	certificate, err := decodeGraphCertificate(request.Certificate)
	if err != nil || certificate.scope != h.machine.scope() || request.After == 0 || request.After > certificate.sourceIndex {
		return nil, errors.Join(errInvalid, err)
	}
	view, err := h.AtGraphCertificate(ctx, certificate)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			wire = nil
		}
	}()
	root, err := view.RootBounded(ctx, 172)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	cfg, err := h.currentConfiguration()
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	// This is an internal borrowed frame decoder, NOT a fabricated source lease.
	// Its only view is the actual authenticated At above, owned until return.
	source := nativeColumnSource{binding: nativeColumnBinding{Version: 2, Policy: nativeColumnPolicy{FrameBytes: 32 << 10, ChunkRows: 16}}}
	source.hosts[0], source.views[0], source.roots[0], source.final[0], source.configurations[0] = h, view, root, certificate, cfg
	page := nativeGraphEdgeUniqueRecordPage{Certificate: request.Certificate, After: request.After, Next: request.After, Frames: []nativeColumnFrame{}}
	for len(page.Frames) < 4 && page.Next < certificate.sourceIndex {
		frame, e := source.frame(ctx, 0, page.Next+1)
		if e != nil {
			return nil, e
		}
		page.Frames = append(page.Frames, frame)
		page.Next++
	}
	page.Complete = page.Next == certificate.sourceIndex
	wire, err = json.Marshal(page)
	if len(wire) > 1<<20 {
		return nil, errLimit
	}
	return wire, err
}
