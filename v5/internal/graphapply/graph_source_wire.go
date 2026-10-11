package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const graphSourceFrameBytes = 2 << 20
const (
	sourceOpen byte = iota + 1
	sourceRoot
	sourceGet
	sourceScan
	sourceClose
	sourceCrossRead
	sourceRequestLookup
	sourceSubmit
	sourceCutRead
	sourceCertifiedOpen
)

type graphSourceRequest struct {
	kind                       byte
	sequence                   uint64
	nonce                      [16]byte
	cookie                     graphSourceCookie
	maximum                    int
	rows                       int
	key, upper, after, command []byte
	tx                         [16]byte
	phase                      crossPhase
}

func emitSourceCookie(w *boundedWriter, c graphSourceCookie) {
	w.add(c.issuer[:])
	w.u64(c.sequence)
	w.add(c.secret[:])
}
func readSourceCookie(c *graphCursor) graphSourceCookie {
	return graphSourceCookie{issuer: c.array(), sequence: c.u64(), secret: c.array()}
}
func encodeGraphSourceRequest(r graphSourceRequest) ([]byte, error) {
	if r.kind < sourceOpen || r.kind > sourceCertifiedOpen || r.sequence == 0 || r.nonce == ([16]byte{}) || r.maximum < 0 || r.maximum > graphSourceFrameBytes || r.rows < 0 || r.rows > 65536 || r.phase > crossResolve {
		return nil, errInvalid
	}
	if len(r.key) > 64<<10 || len(r.upper) > 64<<10 || len(r.after) > 64<<10 || len(r.command) > 1<<20 {
		return nil, errLimit
	}
	return boundedEncoding(graphSourceFrameBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'S', 'Q', 1})
		w.tag(r.kind)
		w.u64(r.sequence)
		w.add(r.nonce[:])
		emitSourceCookie(w, r.cookie)
		w.u32(r.maximum)
		w.u32(r.rows)
		w.field(r.key)
		w.field(r.upper)
		w.field(r.after)
		w.field(r.command)
		w.add(r.tx[:])
		w.tag(byte(r.phase))
	})
}
func decodeGraphSourceRequest(wire []byte) (graphSourceRequest, error) {
	body, err := graphBody(wire, "GSQ\x01", graphSourceFrameBytes)
	if err != nil {
		return graphSourceRequest{}, err
	}
	c := graphCursor{b: body}
	r := graphSourceRequest{kind: c.tag(), sequence: c.u64(), nonce: c.array(), cookie: readSourceCookie(&c), maximum: c.count(graphSourceFrameBytes, 0), rows: c.count(65536, 0)}
	r.key = c.field(64 << 10)
	r.upper = c.field(64 << 10)
	r.after = c.field(64 << 10)
	r.command = c.field(1 << 20)
	r.tx = c.array()
	r.phase = crossPhase(c.tag())
	if c.err != nil || len(c.b) != 0 || r.kind < sourceOpen || r.kind > sourceCertifiedOpen || r.sequence == 0 || r.nonce == ([16]byte{}) || r.phase > crossResolve {
		return graphSourceRequest{}, errors.Join(errInvalid, c.err)
	}
	return r, nil
}
func emitSourceRoot(w *boundedWriter, r raftlog.ApplicationRoot) {
	w.u64(r.Generation)
	w.u64(r.Index)
	w.add(r.ImageHash[:])
	w.field(r.Image)
}
func readSourceRoot(c *graphCursor) raftlog.ApplicationRoot {
	r := raftlog.ApplicationRoot{Generation: c.u64(), Index: c.u64()}
	copy(r.ImageHash[:], c.take(32))
	r.Image = c.field(172)
	if c.err == nil && (r.Index == 0 || len(r.Image) != 172 || sha256.Sum256(r.Image) != r.ImageHash) {
		c.err = errCorrupt
	}
	return r
}
func encodeGraphSourceInfo(info graphSourceInfo) ([]byte, error) {
	observation, err := encodeAllocationObservation(info.observation)
	if err != nil {
		return nil, err
	}
	if info.cookie.issuer == ([16]byte{}) || info.cookie.issuer != info.readID.session || info.cookie.sequence == 0 || info.cookie.secret == ([16]byte{}) || info.readID.sequence == 0 || info.nonce == ([16]byte{}) || info.observation.kind != observeConfiguration || info.root.Index != info.observation.source.index || sha256.Sum256(info.root.Image) != info.root.ImageHash || len(info.root.Image) != 172 || info.limits.Rows < 1 || info.limits.Rows > 4096 || info.limits.Bytes < 1 || info.lifetime <= 0 || info.lifetime > time.Minute {
		return nil, errInvalid
	}
	return boundedEncoding(4096, func(w *boundedWriter) {
		w.add([]byte{'G', 'S', 'I', 1})
		emitSourceCookie(w, info.cookie)
		w.add(info.readID.session[:])
		w.u64(info.readID.sequence)
		w.add(info.nonce[:])
		w.field(observation)
		emitSourceRoot(w, info.root)
		w.u64(info.floor)
		w.u32(info.limits.Rows)
		w.u32(info.limits.Bytes)
		w.u64(uint64(info.lifetime)) // #nosec G115 -- admitted positive lifetime is at most one minute.
	})
}
func decodeGraphSourceInfo(wire []byte) (graphSourceInfo, error) {
	body, err := graphBody(wire, "GSI\x01", 4096)
	if err != nil {
		return graphSourceInfo{}, err
	}
	c := graphCursor{b: body}
	info := graphSourceInfo{cookie: readSourceCookie(&c), readID: allocationReadID{session: c.array(), sequence: c.u64()}, nonce: c.array()}
	observation := c.field(allocationObservationMaxBytes)
	info.root = readSourceRoot(&c)
	info.floor = c.u64()
	info.limits = raftlog.ReadBudget{Rows: c.count(4096, 0), Bytes: c.count(64<<20, 0)}
	lifetime := c.u64()
	if lifetime == 0 || lifetime > uint64(time.Minute) {
		c.err = errCorrupt
	}
	info.lifetime = time.Duration(lifetime) // #nosec G115 -- admitted positive lifetime is at most one minute.
	if c.err != nil || len(c.b) != 0 {
		return graphSourceInfo{}, errors.Join(errCorrupt, c.err)
	}
	info.observation, err = decodeAllocationObservation(observation)
	if err != nil {
		return graphSourceInfo{}, err
	}
	checked, err := encodeGraphSourceInfo(info)
	if err != nil || !bytes.Equal(checked, wire) {
		return graphSourceInfo{}, errors.Join(errCorrupt, err)
	}
	info.root.Image = owned(info.root.Image)
	return info, nil
}

func emitSourceKV(w *boundedWriter, r raftlog.KV) {
	w.field(r.Key)
	w.field(r.Value)
	if r.Deleted {
		w.tag(1)
	} else {
		w.tag(0)
	}
}
func readSourceKV(c *graphCursor, maximum int) raftlog.KV {
	r := raftlog.KV{Key: c.field(64 << 10), Value: c.field(maximum)}
	flag := c.tag()
	if flag > 1 || len(r.Key)+len(r.Value) > maximum {
		c.err = errCorrupt
	}
	r.Deleted = flag == 1
	return r
}
func encodeGraphSourcePage(page raftlog.ApplicationPage, maximum int) ([]byte, error) {
	if maximum < 128 || maximum > graphSourceFrameBytes || len(page.Rows) > 4096 || page.Visited < 0 || page.Visited > 65536 || page.Bytes < 0 || page.Bytes > 64<<20 {
		return nil, errLimit
	}
	return boundedEncoding(maximum, func(w *boundedWriter) {
		w.add([]byte{'G', 'S', 'P', 1})
		w.u32(page.Visited)
		w.u32(page.Bytes)
		if page.Complete {
			w.tag(1)
		} else {
			w.tag(0)
		}
		w.field(page.Next)
		w.u32(len(page.Rows))
		for _, row := range page.Rows {
			emitSourceKV(w, row)
		}
	})
}
func decodeGraphSourcePage(wire []byte, b raftlog.ReadBudget) (raftlog.ApplicationPage, error) {
	body, err := graphBody(wire, "GSP\x01", graphSourceFrameBytes)
	if err != nil {
		return raftlog.ApplicationPage{}, err
	}
	if b.Rows < 1 || b.Bytes < 1 {
		return raftlog.ApplicationPage{}, errInvalid
	}
	c := graphCursor{b: body}
	page := raftlog.ApplicationPage{Visited: c.count(b.Rows, 0), Bytes: c.count(b.Bytes, 0)}
	flag := c.tag()
	page.Complete = flag == 1
	page.Next = c.field(64 << 10)
	n := c.count(min(b.Rows, 4096), 9)
	if c.err != nil {
		return raftlog.ApplicationPage{}, c.err
	}
	// The frame length bounds bytes, and native advertised work must independently
	// cover every retained row header/key/value/continuation before allocating.
	if flag > 1 || !page.Complete && page.Visited == 0 || n > page.Visited || 65*page.Visited > page.Bytes || 64*n+len(page.Next) > page.Bytes || page.Bytes > b.Bytes {
		return raftlog.ApplicationPage{}, errCorrupt
	}
	page.Rows = make([]raftlog.KV, n)
	cost := 64*n + len(page.Next)
	for i := range page.Rows {
		row := readSourceKV(&c, b.Bytes)
		cost += len(row.Key) + len(row.Value)
		if len(row.Key) == 0 || row.Deleted || i > 0 && bytes.Compare(page.Rows[i-1].Key, row.Key) >= 0 {
			c.err = errCorrupt
		}
		page.Rows[i] = row
	}
	if !page.Complete && n > 0 && bytes.Compare(page.Next, page.Rows[n-1].Key) < 0 {
		c.err = errCorrupt
	}
	if c.err != nil || len(c.b) != 0 || cost > page.Bytes || !page.Complete && len(page.Next) == 0 || page.Complete && len(page.Next) != 0 {
		return raftlog.ApplicationPage{}, errors.Join(errCorrupt, c.err)
	}
	for i := range page.Rows {
		page.Rows[i].Key = owned(page.Rows[i].Key)
		page.Rows[i].Value = owned(page.Rows[i].Value)
	}
	page.Next = owned(page.Next)
	return page, nil
}

func encodeCrossAuthorityBody(p crossAuthorityBody) ([]byte, error) {
	scope, err := encodeAllocationScope(p.scope)
	if err != nil {
		return nil, err
	}
	status, err := encodeCrossStatus(p.status)
	if err != nil {
		return nil, err
	}
	if p.id.session == ([16]byte{}) || p.id.sequence == 0 || p.question.nonce == ([16]byte{}) || p.index < p.status.index || p.question.tx != p.status.tx || p.status.digest != sha256.Sum256(p.manifest) || p.status.ns != (namespace{p.scope.graph, p.scope.partition}) {
		return nil, errInvalid
	}
	return boundedEncoding(graphSourceFrameBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'S', 'E', 1})
		w.field(scope)
		w.add(p.id.session[:])
		w.u64(p.id.sequence)
		w.add(p.question.tx[:])
		w.add(p.question.nonce[:])
		w.tag(byte(p.question.phase))
		w.u64(p.index)
		w.field(status)
		w.field(p.manifest)
	})
}

// decodeCrossAuthorityBody is deliberately inert. The source/nonce/request checks
// on an authenticated endpoint must precede sealing it as protocol evidence.
func decodeCrossAuthorityBody(wire []byte) (crossAuthorityBody, error) {
	body, err := graphBody(wire, "GSE\x01", graphSourceFrameBytes)
	if err != nil {
		return crossAuthorityBody{}, err
	}
	c := graphCursor{b: body}
	scope := c.field(allocationScopeBytes)
	p := crossAuthorityBody{id: allocationReadID{session: c.array(), sequence: c.u64()}, question: crossQuery{tx: c.array(), nonce: c.array(), phase: crossPhase(c.tag())}, index: c.u64()}
	status := c.field(256)
	p.manifest = c.field(1 << 20)
	if c.err != nil || len(c.b) != 0 {
		return crossAuthorityBody{}, errors.Join(errCorrupt, c.err)
	}
	p.scope, err = decodeAllocationScope(scope)
	if err != nil {
		return crossAuthorityBody{}, err
	}
	p.status, err = decodeCrossStatus(status)
	if err != nil {
		return crossAuthorityBody{}, err
	}
	checked, err := encodeCrossAuthorityBody(p)
	if err != nil || !bytes.Equal(checked, wire) {
		return crossAuthorityBody{}, errors.Join(errCorrupt, err)
	}
	p.manifest = owned(p.manifest)
	return p, nil
}
