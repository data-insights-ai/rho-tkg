package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// This fresh agreement is opt-in. Ordinary certified/P05 opens, identities and
// response envelopes retain their existing contracts. A participant outcome is
// not an all-participant portable pin token or permission to collect history.
func retainedGraphSemanticContractID() raftlog.ApplicationSemanticContractID {
	base := conditionalGraphSemanticContractID()
	return raftlog.ApplicationSemanticContractID(sha256.Sum256(append(base[:], []byte("durable-cut-feed-pins=participant-ledger-v1\x00terminal-bank=native-v2\x00lease-policy=replicated-epoch-v1\x00original-identity=issuer-request-v1\x00")...)))
}
func conditionalGraphAgreement(id raftlog.ApplicationSemanticContractID) bool {
	return id == conditionalGraphSemanticContractID() || id == retainedGraphSemanticContractID()
}
func (m *declaredMaterializer) retainedPins() bool {
	return m != nil && m.SemanticContractID() == retainedGraphSemanticContractID()
}
func newRetainedGraphMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationControls().Version != 1 || s.ApplicationRecovery().Version != 2 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, retainedGraphSemanticContractID())
}

const (
	pinPublish            byte = 1
	pinAdvanceFeed        byte = 2
	pinRelease            byte = 3
	pinExpire             byte = 4
	pinClose              byte = 5
	pinAdvanceEpoch       byte = 6
	pinApplied            byte = 1
	pinPending            byte = 2
	pinStale              byte = 3
	pinExpired            byte = 4
	pinReleased           byte = 5
	maxGraphPins               = 16
	graphPinManifestBytes      = 16384
	graphPinCommandBytes       = 17408
)

var errGraphPinExpired = errors.New("graphapply: retained pin expired under replicated policy")
var errGraphPinReleased = errors.New("graphapply: retained pin released")

type graphPinPart struct {
	certificate graphCertificate
	feedStart   uint64
	lineage     [32]byte
}
type graphPinManifest struct {
	publisher       allocationScope
	issuer, request [16]byte
	epoch, expires  uint64
	parts           []graphPinPart
}
type graphPinCommand struct {
	scope                           allocationScope
	issuer, action                  [16]byte
	id, digest                      [32]byte
	phase                           byte
	epoch, expectedEpoch, feedStart uint64
	manifest                        []byte
}
type graphPinHead struct {
	epoch, certificateFloor, sourceFloor, feedFloor uint64
	owners                                          [][32]byte
}
type graphPinOwner struct {
	digest                                [32]byte
	publication, feedStart, terminalIndex uint64
	terminal                              byte
}
type graphPinResult struct {
	id, digest                           [32]byte
	action                               [16]byte
	index, publication, epoch, feedStart uint64
	phase, status                        byte
}

func graphPinHeadKey(n namespace) []byte                  { return recordKey(n, 21) }
func graphPinOwnerKey(n namespace, id [32]byte) []byte    { return append(recordKey(n, 22), id[:]...) }
func graphPinOriginalKey(n namespace, id [32]byte) []byte { return append(recordKey(n, 23), id[:]...) }
func graphPinActionKey(n namespace, issuer, action [16]byte) []byte {
	return append(append(recordKey(n, 24), issuer[:]...), action[:]...)
}
func (m graphPinManifest) id() [32]byte {
	b := appendAllocationScope([]byte("rho-tkg:retained-graph-pin:v1\x00"), m.publisher)
	b = append(b, m.issuer[:]...)
	b = append(b, m.request[:]...)
	return sha256.Sum256(b)
}
func pinLineage(p graphPinPart) ([32]byte, error) {
	cert, err := encodeGraphCertificate(p.certificate)
	if err != nil {
		return [32]byte{}, err
	}
	// The exact immutable certificate already binds namespace/group, meaning,
	// configuration, topology, source round/index and the complete root image.
	b := append([]byte("rho-tkg:retained-pin-source-lineage:v1\x00"), cert...)
	b = binary.BigEndian.AppendUint64(b, p.feedStart)
	return sha256.Sum256(b), nil
}
func (m graphPinManifest) valid() bool {
	if !m.publisher.valid() || m.publisher.semantic != retainedGraphSemanticContractID() || m.issuer == ([16]byte{}) || m.request == ([16]byte{}) || m.epoch >= math.MaxUint64-1 || m.expires <= m.epoch || m.expires == math.MaxUint64 || len(m.parts) == 0 || len(m.parts) > maxGraphPins {
		return false
	}
	for i, p := range m.parts {
		c := p.certificate
		if c.scope.semantic != m.publisher.semantic || c.scope.graph != m.publisher.graph || c.scope.topology != m.publisher.topology || c.scope.declaration != m.publisher.declaration || p.feedStart == 0 || p.feedStart > c.sourceIndex+1 || p.lineage == ([32]byte{}) {
			return false
		}
		if i > 0 && (m.parts[i-1].certificate.scope.partition >= c.scope.partition || m.parts[0].certificate.configuration != c.configuration || m.parts[0].certificate.round != c.round) {
			return false
		}
		if _, err := encodeGraphCertificate(c); err != nil {
			return false
		}
		lineage, err := pinLineage(p)
		if err != nil || lineage != p.lineage {
			return false
		}
	}
	return true
}
func encodeGraphPinManifest(m graphPinManifest) ([]byte, error) {
	if !m.valid() {
		return nil, errInvalid
	}
	return boundedEncoding(graphPinManifestBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'P', 'M', 1})
		w.add(appendAllocationScope(nil, m.publisher))
		w.add(m.issuer[:])
		w.add(m.request[:])
		w.u64(m.epoch)
		w.u64(m.expires)
		w.u32(len(m.parts))
		for _, p := range m.parts {
			cert, err := encodeGraphCertificate(p.certificate)
			if err != nil {
				w.err = err
				return
			}
			w.field(cert)
			w.u64(p.feedStart)
			w.add(p.lineage[:])
		}
	})
}
func decodeGraphPinManifest(wire []byte) (graphPinManifest, error) {
	body, err := graphBody(wire, "GPM\x01", graphPinManifestBytes)
	if err != nil {
		return graphPinManifest{}, err
	}
	c := graphCursor{b: body}
	scope := c.take(allocationScopeBytes - 36)
	m := graphPinManifest{issuer: c.array(), request: c.array(), epoch: c.u64(), expires: c.u64()}
	count := c.count(maxGraphPins, 436)
	if c.err != nil || count == 0 {
		return graphPinManifest{}, errors.Join(errCorrupt, c.err)
	}
	m.publisher = readAllocationScope(&decoder{b: scope})
	m.parts = make([]graphPinPart, count)
	for i := range m.parts {
		cert := c.field(512)
		m.parts[i].feedStart = c.u64()
		copy(m.parts[i].lineage[:], c.take(32))
		if c.err != nil {
			return graphPinManifest{}, c.err
		}
		m.parts[i].certificate, err = decodeGraphCertificate(cert)
		if err != nil {
			return graphPinManifest{}, err
		}
	}
	if c.err != nil || len(c.b) != 0 || !m.valid() {
		return graphPinManifest{}, errors.Join(errCorrupt, c.err)
	}
	canonical, err := encodeGraphPinManifest(m)
	if err != nil || !bytes.Equal(canonical, wire) {
		return graphPinManifest{}, errors.Join(errCorrupt, err)
	}
	return m, nil
}
func (c graphPinCommand) valid() bool {
	if !c.scope.valid() || c.scope.semantic != retainedGraphSemanticContractID() || c.issuer == ([16]byte{}) || c.action == ([16]byte{}) || c.phase < 1 || c.phase > 6 {
		return false
	}
	if c.phase == pinAdvanceEpoch {
		return c.id == ([32]byte{}) && c.digest == ([32]byte{}) && len(c.manifest) == 0 && c.feedStart == 0 && c.expectedEpoch < math.MaxUint64-1 && c.epoch == c.expectedEpoch+1
	}
	if c.id == ([32]byte{}) || c.digest == ([32]byte{}) || c.expectedEpoch != 0 {
		return false
	}
	if c.phase == pinPublish {
		m, err := decodeGraphPinManifest(c.manifest)
		return err == nil && m.id() == c.id && m.issuer == c.issuer && m.request == c.action && sha256.Sum256(c.manifest) == c.digest && c.epoch == m.epoch && c.feedStart == 0
	}
	return len(c.manifest) == 0 && (c.phase == pinAdvanceFeed) == (c.feedStart != 0) && (c.phase == pinExpire || c.epoch == 0)
}
func encodeGraphPinCommand(c graphPinCommand) ([]byte, error) {
	if !c.valid() {
		return nil, errInvalid
	}
	return boundedEncoding(graphPinCommandBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'P', 'L', 1})
		w.add(appendAllocationScope(nil, c.scope))
		w.tag(c.phase)
		w.add(c.issuer[:])
		w.add(c.action[:])
		w.add(c.id[:])
		w.add(c.digest[:])
		w.u64(c.epoch)
		w.u64(c.expectedEpoch)
		w.u64(c.feedStart)
		w.field(c.manifest)
	})
}
func decodeGraphPinCommand(wire []byte) (graphPinCommand, error) {
	body, err := graphBody(wire, "GPL\x01", graphPinCommandBytes)
	if err != nil {
		return graphPinCommand{}, err
	}
	d := graphCursor{b: body}
	scope := d.take(allocationScopeBytes - 36)
	c := graphPinCommand{phase: d.tag(), issuer: d.array(), action: d.array()}
	copy(c.id[:], d.take(32))
	copy(c.digest[:], d.take(32))
	c.epoch = d.u64()
	c.expectedEpoch = d.u64()
	c.feedStart = d.u64()
	c.manifest = d.field(graphPinManifestBytes)
	if d.err != nil || len(d.b) != 0 {
		return graphPinCommand{}, errors.Join(errCorrupt, d.err)
	}
	c.scope = readAllocationScope(&decoder{b: scope})
	canonical, err := encodeGraphPinCommand(c)
	if err != nil || !bytes.Equal(canonical, wire) {
		return graphPinCommand{}, errors.Join(errCorrupt, err)
	}
	c.manifest = owned(c.manifest)
	return c, nil
}
func isGraphPinWire(wire []byte) bool {
	return len(wire) >= 4 && bytes.Equal(wire[:4], []byte{'G', 'P', 'L', 1})
}
func encodeGraphPinResult(r graphPinResult) ([]byte, error) {
	if r.index == 0 || r.phase < 1 || r.phase > 6 || r.status < 1 || r.status > 5 {
		return nil, errInvalid
	}
	return boundedEncoding(256, func(w *boundedWriter) {
		w.add([]byte{'G', 'P', 'O', 1})
		w.tag(r.phase)
		w.tag(r.status)
		w.add(r.id[:])
		w.add(r.digest[:])
		w.add(r.action[:])
		w.u64(r.index)
		w.u64(r.publication)
		w.u64(r.epoch)
		w.u64(r.feedStart)
	})
}
func decodeGraphPinResult(wire []byte) (graphPinResult, error) {
	body, err := graphBody(wire, "GPO\x01", 256)
	if err != nil {
		return graphPinResult{}, err
	}
	c := graphCursor{b: body}
	r := graphPinResult{phase: c.tag(), status: c.tag()}
	copy(r.id[:], c.take(32))
	copy(r.digest[:], c.take(32))
	r.action = c.array()
	r.index = c.u64()
	r.publication = c.u64()
	r.epoch = c.u64()
	r.feedStart = c.u64()
	if c.err != nil || len(c.b) != 0 {
		return graphPinResult{}, errors.Join(errCorrupt, c.err)
	}
	canonical, err := encodeGraphPinResult(r)
	if err != nil || !bytes.Equal(canonical, wire) {
		return graphPinResult{}, errors.Join(errCorrupt, err)
	}
	return r, nil
}
func encodePinHead(h graphPinHead) ([]byte, error) {
	if h.epoch == math.MaxUint64 || len(h.owners) > maxGraphPins {
		return nil, errInvalid
	}
	for i, id := range h.owners {
		if id == ([32]byte{}) || i > 0 && bytes.Compare(h.owners[i-1][:], id[:]) >= 0 {
			return nil, errInvalid
		}
	}
	return boundedEncoding(1024, func(w *boundedWriter) {
		w.add([]byte{'G', 'P', 'H', 1})
		w.u64(h.epoch)
		w.u64(h.certificateFloor)
		w.u64(h.sourceFloor)
		w.u64(h.feedFloor)
		w.u32(len(h.owners))
		for _, id := range h.owners {
			w.add(id[:])
		}
	})
}
func readPinHead(q *reader) (graphPinHead, error) {
	wire, found, err := q.getBounded(graphPinHeadKey(q.ns), 1024)
	if err != nil || !found {
		return graphPinHead{}, err
	}
	body, err := graphBody(wire, "GPH\x01", 1024)
	if err != nil {
		return graphPinHead{}, err
	}
	c := graphCursor{b: body}
	h := graphPinHead{epoch: c.u64(), certificateFloor: c.u64(), sourceFloor: c.u64(), feedFloor: c.u64()}
	count := c.count(maxGraphPins, 32)
	if c.err != nil {
		return graphPinHead{}, c.err
	}
	if err := reserveComposer(q, 64+32*count); err != nil {
		return graphPinHead{}, err
	}
	h.owners = make([][32]byte, count)
	for i := range h.owners {
		copy(h.owners[i][:], c.take(32))
	}
	canonical, err := encodePinHead(h)
	if c.err != nil || err != nil || len(c.b) != 0 || !bytes.Equal(canonical, wire) {
		return graphPinHead{}, errors.Join(errCorrupt, c.err, err)
	}
	return h, nil
}
func encodePinOwner(o graphPinOwner) ([]byte, error) {
	if o.digest == ([32]byte{}) || o.publication == 0 || o.feedStart == 0 || o.terminal != 0 && o.terminal != pinRelease && o.terminal != pinExpire && o.terminal != pinClose || (o.terminal == 0) != (o.terminalIndex == 0) {
		return nil, errInvalid
	}
	return boundedEncoding(128, func(w *boundedWriter) {
		w.add([]byte{'G', 'P', 'A', 1})
		w.add(o.digest[:])
		w.u64(o.publication)
		w.u64(o.feedStart)
		w.tag(o.terminal)
		w.u64(o.terminalIndex)
	})
}
func readPinOwner(q *reader, id [32]byte) (graphPinOwner, bool, error) {
	wire, found, err := q.getBounded(graphPinOwnerKey(q.ns, id), 128)
	if err != nil || !found {
		return graphPinOwner{}, found, err
	}
	body, err := graphBody(wire, "GPA\x01", 128)
	if err != nil {
		return graphPinOwner{}, false, err
	}
	c := graphCursor{b: body}
	o := graphPinOwner{}
	copy(o.digest[:], c.take(32))
	o.publication = c.u64()
	o.feedStart = c.u64()
	o.terminal = c.tag()
	o.terminalIndex = c.u64()
	canonical, err := encodePinOwner(o)
	if c.err != nil || err != nil || len(c.b) != 0 || !bytes.Equal(canonical, wire) || o.publication > q.base.Index || o.terminalIndex > q.base.Index {
		return graphPinOwner{}, false, errors.Join(errCorrupt, c.err, err)
	}
	return o, true, nil
}
func readPinOriginal(q *reader, id [32]byte) (graphPinManifest, []byte, bool, error) {
	row, found, err := crossControl(q, graphPinOriginalKey(q.ns, id), graphPinManifestBytes)
	if err != nil || !found {
		return graphPinManifest{}, nil, found, err
	}
	if err := reserveComposer(q, 2048+8*len(row.Value)); err != nil {
		return graphPinManifest{}, nil, false, err
	}
	m, err := decodeGraphPinManifest(row.Value)
	if err != nil || m.id() != id {
		return graphPinManifest{}, nil, false, errors.Join(errCorrupt, err)
	}
	owner, present, err := readPinOwner(q, id)
	if err != nil {
		return graphPinManifest{}, nil, false, err
	}
	if present && owner.publication != row.Index {
		return graphPinManifest{}, nil, false, errCorrupt
	}
	return m, row.Value, true, nil
}
func pinPart(m graphPinManifest, partition uint64) (graphPinPart, bool) {
	for _, p := range m.parts {
		if p.certificate.scope.partition == partition {
			return p, true
		}
	}
	return graphPinPart{}, false
}
func pinReservation(scope allocationScope, id, digest [32]byte) raftlog.ApplicationRecoveryReservation {
	b := appendAllocationScope([]byte("rho-tkg:retained-pin-terminal-owner:v1\x00"), scope)
	b = append(b, id[:]...)
	// One fixed-size cleanup command, two native data rows, one immutable original
	// and four envelopes. The native recovery root can contain the full64KiB bank
	// envelope; its repeated retained growth is deliberately included, not free.
	return raftlog.ApplicationRecoveryReservation{ID: sha256.Sum256(b), Digest: digest, Credits: []raftlog.ApplicationRecoveryCredit{
		{Phase: 1, CommandBytes: 512, RetainedBytes: 65536 + 4096, RetainedRecords: 8},
		{Phase: 2, CommandBytes: 512, RetainedBytes: 65536 + 4096, RetainedRecords: 8},
		{Phase: 3, CommandBytes: 512, RetainedBytes: 65536 + 4096, RetainedRecords: 8},
	}}
}
func (m *declaredMaterializer) bindPinManifest(manifest graphPinManifest) error {
	if !manifest.valid() || len(manifest.parts) != m.declaration.Len() || manifest.publisher.graph != m.ns.graph || manifest.publisher.declaration != m.declaration.Digest() || manifest.publisher.topology != m.declaration.TopologyEpoch() {
		return errInvalid
	}
	publisher, ok := m.declaration.Partition(manifest.publisher.partition)
	if !ok || publisher.Group != manifest.publisher.group || publisher.OwnershipEpoch != manifest.publisher.ownership {
		return errInvalid
	}
	for i, p := range manifest.parts {
		part, _ := m.declaration.PartitionAt(i)
		c := p.certificate.scope
		if c.partition != part.Partition || c.group != part.Group || c.ownership != part.OwnershipEpoch {
			return errInvalid
		}
	}
	return nil
}
func (m *declaredMaterializer) checkPinSource(q *reader, p graphPinPart, h graphPinHead) error {
	c := p.certificate
	if c.scope != m.scope() || c.index > q.base.Index || c.sourceIndex > q.base.Index {
		return replica.ErrUnavailable
	}
	if c.index < h.certificateFloor || c.sourceIndex < h.sourceFloor || p.feedStart < h.feedFloor {
		return errGraphPinExpired
	}
	actual, found, err := q.getBounded(graphCertificateKey(m.ns, c.round), 512)
	if err != nil {
		return err
	}
	if !found {
		return replica.ErrUnavailable
	}
	cert, err := decodeGraphCertificate(actual)
	if err != nil || cert != c {
		return errors.Join(errInvalid, err)
	}
	// Validate graph meaning from the actual retained certificate's complete
	// image, then compare that image hash and real feed position to native frames.
	decoded, err := graphstore.DecodeRoot(c.image[:])
	if err != nil || decoded.Namespace().Partition != m.ns.partition || [16]byte(decoded.Namespace().Graph) != [16]byte(m.ns.graph) || decoded.OwnershipEpoch() != c.scope.ownership {
		return errors.Join(errCorrupt, err)
	}
	return checkPinRootFeed(q, c.sourceIndex, sha256.Sum256(c.image[:]), p.feedStart)
}
func checkPinRootFeed(q *reader, source uint64, image [32]byte, feed uint64) error {
	rows := min(2, q.limits.readRows-q.rows)
	byteBudget := min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes)
	if rows < 1 || byteBudget < 1024 {
		return errLimit
	}
	// The inspection returns fixed scalars and reports actual borrowed work; it
	// allocates no payload and must not precharge a configured-max page buffer.
	if err := reserveComposer(q, 1024); err != nil {
		return err
	}
	equal, work, err := q.view.CheckRetainedRootFeed(q.ctx, source, image, feed, raftlog.ReadBudget{Rows: rows, Bytes: byteBudget})
	q.rows += work.Records
	q.bytes += work.Bytes
	if err != nil {
		return err
	}
	if !equal {
		return errCorrupt
	}
	return nil
}

func (m *declaredMaterializer) checkRetainedPinLedger(q *reader) error {
	return m.checkRetainedPinRoster(q, true)
}

func (m *declaredMaterializer) checkRetainedPinRoster(q *reader, sources bool) error {
	if !m.retainedPins() {
		return nil
	}
	if q.arena == nil {
		if err := joinGraphOutputBudget(q, graphRequest{}, 4096, m.limits); err != nil {
			return err
		}
	}
	h, err := readPinHead(q)
	if err != nil {
		return err
	}
	if err := reserveComposer(q, 4096+256*len(h.owners)); err != nil {
		return err
	}
	expected := make([]raftlog.ApplicationRecoveryReservation, 0, len(h.owners))
	for _, id := range h.owners {
		owner, found, err := readPinOwner(q, id)
		if err != nil {
			return err
		}
		if !found || owner.terminal != 0 {
			return errCorrupt
		}
		if sources {
			manifest, wire, found, err := readPinOriginal(q, id)
			if err != nil {
				return err
			}
			if !found || sha256.Sum256(wire) != owner.digest {
				return errCorrupt
			}
			if err := m.bindPinManifest(manifest); err != nil {
				return err
			}
			p, found := pinPart(manifest, m.ns.partition)
			if !found {
				return errCorrupt
			}
			p.feedStart = owner.feedStart
			if err := m.checkPinSource(q, p, h); err != nil {
				return err
			}
		}
		expected = append(expected, pinReservation(m.scope(), id, owner.digest))
	}
	slices.SortFunc(expected, func(a, b raftlog.ApplicationRecoveryReservation) int { return bytes.Compare(a.ID[:], b.ID[:]) })
	rows, byteBudget := min(1, q.limits.readRows-q.rows), min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes)
	if rows < 1 || byteBudget < 1024 {
		return errLimit
	}
	// Native preflight bounds its actual decode ownership before allocating. The
	// grant is capped by this same arena; charge the reported actual work below,
	// rather than reserving a configured-max metadata buffer that is not built.
	byteBudget = min(byteBudget, q.arena.Remaining())
	equal, work, err := q.view.CheckTerminalBank(q.ctx, expected, raftlog.ReadBudget{Rows: rows, Bytes: byteBudget})
	q.rows += work.Records
	q.bytes += work.Bytes
	chargeErr := reserveComposer(q, work.Bytes)
	if err != nil || chargeErr != nil {
		return errors.Join(err, chargeErr)
	}
	if !equal {
		return errCorrupt
	}
	return nil
}
func pinActionOriginal(q *reader, c graphPinCommand, wire []byte) (graphPinResult, bool, error) {
	row, found, err := crossControl(q, graphPinActionKey(q.ns, c.issuer, c.action), 512)
	if err != nil || !found {
		return graphPinResult{}, found, err
	}
	body, err := graphBody(row.Value, "GPX\x01", 512)
	if err != nil || len(body) < 32 {
		return graphPinResult{}, false, errors.Join(errCorrupt, err)
	}
	hash := sha256.Sum256(wire)
	if !bytes.Equal(body[:32], hash[:]) {
		return graphPinResult{}, false, errInvalid
	}
	result, err := decodeGraphPinResult(body[32:])
	if err != nil || result.id != c.id || result.digest != c.digest || result.action != c.action || result.phase != c.phase || result.index != row.Index {
		return graphPinResult{}, false, errors.Join(errCorrupt, err)
	}
	return result, true, nil
}
func (m *declaredMaterializer) retainedPinPurpose(q *reader, c graphPinCommand, wire []byte) (raftlog.ApplicationRecoveryPurpose, error) {
	if !m.retainedPins() || c.scope != m.scope() {
		return raftlog.ApplicationRecoveryPurpose{}, errInvalid
	}
	if _, found, err := pinActionOriginal(q, c, wire); err != nil || found {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	if c.phase == pinPublish {
		manifest, err := decodeGraphPinManifest(c.manifest)
		if err != nil {
			return raftlog.ApplicationRecoveryPurpose{}, err
		}
		if err := m.bindPinManifest(manifest); err != nil {
			return raftlog.ApplicationRecoveryPurpose{}, err
		}
		reservation := pinReservation(m.scope(), c.id, c.digest)
		return raftlog.ApplicationRecoveryPurpose{TerminalReserve: &reservation}, nil
	}
	if c.phase != pinRelease && c.phase != pinExpire && c.phase != pinClose {
		return raftlog.ApplicationRecoveryPurpose{}, nil
	}
	owner, found, err := readPinOwner(q, c.id)
	if err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	if found {
		if owner.digest != c.digest {
			return raftlog.ApplicationRecoveryPurpose{}, errInvalid
		}
		if owner.terminal != 0 {
			return raftlog.ApplicationRecoveryPurpose{}, nil
		}
		manifest, _, present, err := readPinOriginal(q, c.id)
		if err != nil || !present {
			return raftlog.ApplicationRecoveryPurpose{}, errors.Join(replica.ErrUnavailable, err)
		}
		if manifest.issuer != c.issuer {
			return raftlog.ApplicationRecoveryPurpose{}, errInvalid
		}
	}
	// An unapplied Publish may precede this command in the same Ready. This is
	// only its canonical capacity recipe: Store must bind a genuine applied or
	// queued TerminalReserve; Stage then validates its immutable owner/issuer.

	reservation := pinReservation(m.scope(), c.id, c.digest)
	phase := uint32(1)
	if c.phase == pinExpire {
		phase = 2
	}
	if c.phase == pinClose {
		phase = 3
	}
	use := raftlog.ApplicationRecoveryUse{ID: reservation.ID, Digest: reservation.Digest, CommandDigest: sha256.Sum256(wire), Phase: phase}
	return raftlog.ApplicationRecoveryPurpose{TerminalUse: &use}, nil
}

// These observational predicates are shared by Host pre-admission and Stage.
// Pre-admission is not authorization: Stage rechecks its actual applied prefix.
func (m *declaredMaterializer) pinPublication(q *reader, c graphPinCommand, h graphPinHead) (graphPinManifest, graphPinPart, byte, error) {
	manifest, err := decodeGraphPinManifest(c.manifest)
	if err != nil {
		return manifest, graphPinPart{}, 0, err
	}
	if err := m.bindPinManifest(manifest); err != nil {
		return manifest, graphPinPart{}, 0, err
	}
	cfg, err := m.initialized(q)
	if err != nil {
		return manifest, graphPinPart{}, 0, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return manifest, graphPinPart{}, 0, err
	}
	p, found := pinPart(manifest, m.ns.partition)
	if !found || p.certificate.configuration != configuration {
		return manifest, p, 0, errInvalid
	}
	if h.epoch != manifest.epoch {
		return manifest, p, pinStale, nil
	}
	if err := m.checkPinSource(q, p, h); err != nil {
		if errors.Is(err, errGraphPinExpired) {
			return manifest, p, pinExpired, nil
		}
		return manifest, p, 0, err
	}
	if len(h.owners) >= maxGraphPins {
		return manifest, p, 0, errLimit
	}
	if _, present, err := readPinOwner(q, c.id); err != nil || present {
		return manifest, p, 0, errors.Join(errInvalid, err)
	}
	return manifest, p, pinApplied, nil
}
func (m *declaredMaterializer) pinKnownOwner(q *reader, c graphPinCommand) (graphPinOwner, graphPinManifest, bool, error) {
	owner, found, err := readPinOwner(q, c.id)
	if err != nil || !found {
		return owner, graphPinManifest{}, false, err
	}
	manifest, wire, found, err := readPinOriginal(q, c.id)
	if err != nil || !found {
		return owner, manifest, false, errors.Join(errCorrupt, err)
	}
	if owner.digest != c.digest || sha256.Sum256(wire) != owner.digest || manifest.issuer != c.issuer {
		return owner, manifest, false, errInvalid
	}
	if err := m.bindPinManifest(manifest); err != nil {
		return owner, manifest, false, err
	}
	return owner, manifest, true, nil
}
func pinFeedAllowed(q *reader, c graphPinCommand, owner graphPinOwner, h graphPinHead, manifest graphPinManifest) error {
	if h.epoch >= manifest.expires {
		return errGraphPinExpired
	}
	if c.feedStart < owner.feedStart || c.feedStart < h.feedFloor || c.feedStart > q.base.Index+1 {
		return errInvalid
	}
	return checkPinRootFeed(q, q.base.Index, q.base.ImageHash, c.feedStart)
}
func pinTerminalAllowed(c graphPinCommand, h graphPinHead, manifest graphPinManifest) error {
	if c.phase == pinExpire {
		if c.epoch != manifest.expires {
			return errInvalid
		}
		if h.epoch < manifest.expires {
			return replica.ErrUnavailable
		}
	}
	return nil
}
func (h *allocationHost) preflightRetainedPin(c graphPinCommand, wire []byte) (err error) {
	v, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, v.Close()) }()
	base, err := v.RootBounded(context.Background(), 172)
	if err != nil {
		return err
	}
	m := h.machine
	l := m.limits.allocation
	l.readRows, l.readBytes = min(l.readRows, m.limits.sourceRows), min(l.readBytes, m.limits.sourceBytes)
	q := reader{ctx: context.Background(), view: v, ns: m.ns, limits: l, base: base, bytes: cap(base.Image)}
	if err := joinGraphOutputBudget(&q, graphRequest{}, 4096+8*len(wire), m.limits); err != nil {
		return err
	}
	if _, found, err := pinActionOriginal(&q, c, wire); err != nil || found {
		return err
	}
	if _, err := m.readOwnership(q.ctx, v, &q); err != nil {
		return err
	}
	if err := m.checkRetainedPinRoster(&q, false); err != nil {
		return err
	}
	head, err := readPinHead(&q)
	if err != nil {
		return err
	}
	if c.phase == pinAdvanceEpoch {
		return nil
	}
	if c.phase == pinPublish {
		_, _, _, err := m.pinPublication(&q, c, head)
		return err
	}
	owner, manifest, found, err := m.pinKnownOwner(&q, c)
	if err != nil {
		return err
	}
	if !found {
		return replica.ErrUnavailable
	}
	if owner.terminal != 0 {
		return nil
	}
	if !slices.Contains(head.owners, c.id) {
		return errCorrupt
	}
	if c.phase == pinAdvanceFeed {
		err := pinFeedAllowed(&q, c, owner, head, manifest)
		if errors.Is(err, errGraphPinExpired) {
			return nil
		}
		return err
	}
	return pinTerminalAllowed(c, head, manifest)
}

func (m *declaredMaterializer) stageRetainedPin(q *reader, entry replica.Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if !m.retainedPins() {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	if err := joinGraphOutputBudget(q, graphRequest{}, 4096+8*len(entry.Data), m.limits); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	c, err := decodeGraphPinCommand(entry.Data)
	if err != nil || c.scope != m.scope() {
		return raftlog.ApplicationBatch{}, errors.Join(errInvalid, err)
	}
	if original, found, err := pinActionOriginal(q, c, entry.Data); err != nil {
		return raftlog.ApplicationBatch{}, err
	} else if found {
		// Replaying a terminated original needs its authenticated immutable outcome,
		// not its formerly protected historical data. GC must preserve these rows.
		out, err := encodeGraphPinResult(original)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		b := batchAt(q.base)
		b.Outcome = out
		return b, m.preflight(b, budget)
	}
	// The authenticated native bank and graph roster suffice for cleanup. Do not
	// make the owner's funded terminal path scan unrelated historical payloads.
	if err := m.checkRetainedPinRoster(q, false); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	h, err := readPinHead(q)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	result := graphPinResult{id: c.id, digest: c.digest, action: c.action, index: entry.Index, phase: c.phase, status: pinApplied, epoch: h.epoch}
	var puts []raftlog.ApplicationControlPut
	var purpose raftlog.ApplicationRecoveryPurpose
	finish := func(changed, remember bool) (raftlog.ApplicationBatch, error) {
		out, err := encodeGraphPinResult(result)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if remember {
			if err := reserveComposer(q, 1024+8*len(out)); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			record, err := boundedEncoding(512, func(w *boundedWriter) {
				w.add([]byte{'G', 'P', 'X', 1})
				hash := sha256.Sum256(entry.Data)
				w.add(hash[:])
				w.add(out)
			})
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			puts = append(puts, raftlog.ApplicationControlPut{Key: graphPinActionKey(m.ns, c.issuer, c.action), Value: record})
		}
		b := batchAt(q.base)
		b.Outcome = out
		b.Writes = q.writes
		b.ControlPuts = puts
		if changed {
			b.Changes = owned(out)
		}
		b.TerminalReserve, b.TerminalUse = purpose.TerminalReserve, purpose.TerminalUse
		if err := sortPartitionWrites(b.Writes); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		slices.SortFunc(b.ControlPuts, func(a, b raftlog.ApplicationControlPut) int { return bytes.Compare(a.Key, b.Key) })
		return b, m.preflight(b, budget)
	}
	writeHead := func() error {
		wire, err := encodePinHead(h)
		if err != nil {
			return err
		}
		return q.put(graphPinHeadKey(m.ns), wire)
	}
	if c.phase == pinAdvanceEpoch {
		if h.epoch != c.expectedEpoch {
			result.status = pinStale
			return finish(false, true)
		}
		h.epoch = c.epoch
		result.epoch = h.epoch
		if err := writeHead(); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		return finish(true, true)
	}
	if c.phase == pinPublish {
		_, p, status, err := m.pinPublication(q, c, h)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if status != pinApplied {
			result.status = status
			return finish(false, true)
		}

		purpose, err = m.retainedPinPurpose(q, c, entry.Data)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if err := reserveComposer(q, 1024+4*len(c.manifest)); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		puts = append(puts, raftlog.ApplicationControlPut{Key: graphPinOriginalKey(m.ns, c.id), Value: owned(c.manifest)})
		if err := reserveComposer(q, 64+32*(len(h.owners)+1)); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		h.owners = append(h.owners, c.id)
		slices.SortFunc(h.owners, func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) })
		owner := graphPinOwner{digest: c.digest, publication: entry.Index, feedStart: p.feedStart}
		row, err := encodePinOwner(owner)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if err := q.put(graphPinOwnerKey(m.ns, c.id), row); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if err := writeHead(); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		result.publication, result.feedStart = owner.publication, owner.feedStart
		return finish(true, true)
	}
	owner, manifest, found, err := m.pinKnownOwner(q, c)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if !found {
		result.status = pinPending
		return finish(false, false)
	}

	result.publication, result.feedStart = owner.publication, owner.feedStart
	if owner.terminal != 0 {
		result.status = pinReleased
		if owner.terminal == pinExpire {
			result.status = pinExpired
		}
		return finish(false, true)
	}
	if !slices.Contains(h.owners, c.id) {
		return raftlog.ApplicationBatch{}, errCorrupt
	}
	if c.phase == pinAdvanceFeed {
		if err := pinFeedAllowed(q, c, owner, h, manifest); err != nil {
			if errors.Is(err, errGraphPinExpired) {
				result.status = pinExpired
				return finish(false, true)
			}
			return raftlog.ApplicationBatch{}, err
		}
		owner.feedStart = c.feedStart
		result.feedStart = c.feedStart
	} else {
		if err := pinTerminalAllowed(c, h, manifest); err != nil {
			if errors.Is(err, replica.ErrUnavailable) {
				result.status = pinPending
				return finish(false, false)
			}
			return raftlog.ApplicationBatch{}, err
		}
		purpose, err = m.retainedPinPurpose(q, c, entry.Data)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		owner.terminal, owner.terminalIndex = c.phase, entry.Index
		h.owners = slices.DeleteFunc(h.owners, func(id [32]byte) bool { return id == c.id })
		if err := writeHead(); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	row, err := encodePinOwner(owner)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := q.put(graphPinOwnerKey(m.ns, c.id), row); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return finish(true, true)
}

// The proposal remains inert until the actual bound Host admits and applies it.
func (h *allocationHost) retainedPinProposal(c graphPinCommand) (allocationProposal, error) {
	if h == nil || h.machine == nil || !h.machine.retainedPins() || c.scope != h.machine.scope() {
		return allocationProposal{}, errInvalid
	}
	wire, err := encodeGraphPinCommand(c)
	if err != nil {
		return allocationProposal{}, err
	}
	return allocationProposal{target: c.scope, wire: wire}, nil
}
