package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This profile relinquishes future snapshot-token opens, not the native data
// required to resolve unread envelopes. Unlike a detached CDC store, its GC
// protects a rolling acknowledged predecessor and the unread native prefix.
func retainedOnlyFeedSemanticContractID() raftlog.ApplicationSemanticContractID {
	base := retainedFeedSemanticContractID()
	return raftlog.ApplicationSemanticContractID(sha256.Sum256(append(base[:], []byte("feed-retention=irreversible-rolling-ack-anchor-v1\x00snapshot-reopen=authenticated-expired-v1\x00payload-anchor=exact-plm-round-image-v1\x00")...)))
}

func retainedFeedAgreement(id raftlog.ApplicationSemanticContractID) bool {
	return id == retainedFeedSemanticContractID() || retainedOnlyFeedAgreement(id)
}

func retainedFeedOutcomeFor(id raftlog.ApplicationSemanticContractID, wire []byte) bool {
	return retainedFeedAgreement(id) && bytes.HasPrefix(wire, []byte("GFO\x01")) || retainedOnlyFeedAgreement(id) && bytes.HasPrefix(wire, []byte("GFO\x02"))
}

func newRetainedOnlyFeedMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationRetention().Version != 1 || s.ApplicationRecovery().Version != 2 || s.ApplicationControls().Version != 1 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, retainedOnlyFeedSemanticContractID())
}

// The installed native row is authority only with its exact GFL2/GFX original,
// live pin and cursor. No caller can supply an anchor or manufacture a pin by
// copying this value. Later ACKs update only the physical source coordinate.
type retainedFeedRetention struct {
	action       [16]byte
	installed    uint64
	index, round uint64
	image        [172]byte
}

func retainedFeedRetentionKey(n namespace, id [32]byte) []byte {
	return append(recordKey(n, 38), id[:]...)
}

func encodeFeedRetention(r retainedFeedRetention) ([]byte, error) {
	if r.action == ([16]byte{}) || r.installed == 0 || r.index == 0 {
		return nil, errInvalid
	}
	if _, err := graphstore.DecodeRoot(r.image[:]); err != nil {
		return nil, errors.Join(errInvalid, err)
	}
	return boundedEncoding(512, func(w *boundedWriter) {
		w.add([]byte("GFK\x01"))
		w.add(r.action[:])
		w.u64(r.installed)
		w.u64(r.index)
		w.u64(r.round)
		w.add(r.image[:])
	})
}

func decodeFeedRetention(wire []byte) (retainedFeedRetention, error) {
	body, err := graphBody(wire, "GFK\x01", 512)
	if err != nil {
		return retainedFeedRetention{}, err
	}
	c := graphCursor{b: body}
	r := retainedFeedRetention{action: c.array(), installed: c.u64(), index: c.u64(), round: c.u64()}
	copy(r.image[:], c.take(172))
	canonical, err := encodeFeedRetention(r)
	if c.err != nil || len(c.b) != 0 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedFeedRetention{}, errors.Join(errCorrupt, c.err, err)
	}
	return r, nil
}

func feedLocalPosition(m graphPinManifest, partition uint64, positions []uint64) (uint64, bool) {
	if len(positions) != len(m.parts) {
		return 0, false
	}
	for i, p := range m.parts {
		if p.certificate.scope.partition == partition {
			return positions[i], positions[i] > 1
		}
	}
	return 0, false
}

// Snapshot/recovery validation stays on the supplied bank. In particular, this
// never opens an active-bank historical view for an incoming snapshot.
func (m *declaredMaterializer) feedRetention(q *reader, manifest graphPinManifest) (retainedFeedRetention, bool, error) {
	var zero retainedFeedRetention
	if !retainedOnlyFeedAgreement(m.SemanticContractID()) {
		return zero, false, nil
	}
	wire, found, err := q.getBounded(retainedFeedRetentionKey(m.ns, manifest.id()), 512)
	if err != nil || !found {
		return zero, false, err
	}
	if err = reserveComposer(q, 2048); err != nil {
		return zero, false, err
	}
	r, err := decodeFeedRetention(wire)
	if err != nil || r.installed > q.base.Index || r.index > q.base.Index {
		return zero, false, errors.Join(errCorrupt, err)
	}
	definition, present, err := crossControl(q, retainedFeedDefinitionKey(m.ns, manifest.id()), graphPinManifestBytes+128)
	if err != nil || !present {
		return zero, false, errors.Join(errCorrupt, err)
	}
	if err = reserveComposer(q, 8192+8*len(definition.Value)); err != nil {
		return zero, false, err
	}
	d, err := decodeRetainedFeedDefinition(definition.Value)
	expected, err2 := encodeGraphPinManifest(manifest)
	if err != nil || err2 != nil || !bytes.Equal(d.manifest, expected) {
		return zero, false, errors.Join(errCorrupt, err, err2)
	}
	c := retainedFeedCommand{scope: m.scope(), issuer: manifest.issuer, action: r.action, phase: feedRetainOnly, definition: definition.Value, original: manifest.id()}
	if err = reserveComposer(q, 4096+8*len(c.definition)); err != nil {
		return zero, false, err
	}
	command, err := encodeRetainedFeedCommand(c)
	if err != nil {
		return zero, false, err
	}
	result, present, err := feedActionOriginal(q, c, command)
	if err != nil || !present || result.status != feedApplied || result.index != r.installed {
		return zero, false, errors.Join(errCorrupt, err)
	}
	cursor, present, err := readFeedCursor(q, manifest.id())
	if err != nil || !present {
		return zero, false, errors.Join(errCorrupt, err)
	}
	next, ok := feedLocalPosition(manifest, m.ns.partition, cursor.positions)
	if !ok || r.index != next-1 || result.sequence > cursor.sequence {
		return zero, false, errCorrupt
	}
	if err = chargeGraphWork(q, graphstore.PageWork{Records: 1, Bytes: 2048}); err != nil {
		return zero, false, err
	}
	if err = reserveComposer(q, 2048); err != nil {
		return zero, false, err
	}
	root, err := q.view.RootAt(q.ctx, r.index, 172)
	if err != nil {
		return zero, false, err
	}
	if !bytes.Equal(root.Image, r.image[:]) {
		return zero, false, errCorrupt
	}
	// The immutable PLM must still be precisely this installed round and image.
	round, original, image, err := readGraphRoundRecord(q, manifest.parts[0].certificate.configuration, r.round)
	if err != nil || round != r.round || original > r.index || image != r.image {
		return zero, false, errors.Join(errCorrupt, err)
	}
	return r, true, nil
}

func (m *declaredMaterializer) feedAnchor(q *reader, manifest graphPinManifest, positions []uint64, r retainedFeedRetention) (retainedFeedRetention, error) {
	next, ok := feedLocalPosition(manifest, m.ns.partition, positions)
	if !ok {
		return r, errInvalid
	}
	r.index = next - 1
	err := m.feedAt(q, r.index, func(at *reader) error {
		var err error
		r.round, err = readPartitionRoundFloor(at, manifest.parts[0].certificate.configuration)
		if err != nil {
			return err
		}
		copy(r.image[:], at.base.Image)
		round, original, image, err := readGraphRoundRecord(at, manifest.parts[0].certificate.configuration, r.round)
		if err != nil || round != r.round || original > r.index || image != r.image {
			return errors.Join(errCorrupt, err)
		}
		return nil
	})
	return r, err
}

func (m *declaredMaterializer) checkPinRetainedSource(q *reader, manifest graphPinManifest, owner graphPinOwner, head graphPinHead) error {
	r, retained, err := m.feedRetention(q, manifest)
	if err != nil {
		return err
	}
	if retained {
		return checkPinRootFeed(q, r.index, sha256.Sum256(r.image[:]), owner.feedStart)
	}
	part, found := pinPart(manifest, m.ns.partition)
	if !found {
		return errCorrupt
	}
	part.feedStart = owner.feedStart
	return m.checkPinSource(q, part, head)
}

func (m *declaredMaterializer) feedSnapshotAvailable(q *reader, manifest graphPinManifest) error {
	_, retained, err := m.feedRetention(q, manifest)
	if err != nil {
		return err
	}
	if retained {
		return errors.Join(errGraphHistoryExpired, raftlog.ErrApplicationExpired)
	}
	return nil
}

// A private feed source may use the ACK predecessor for an unchanged
// participant. It grants no At, routing, pin or original-result authority.
func (m *declaredMaterializer) feedPayloadSource(q *reader, manifest graphPinManifest, configuration [32]byte, bound uint64) (round, index uint64, image [172]byte, err error) {
	r, retained, err := m.feedRetention(q, manifest)
	if err != nil || !retained {
		if err != nil {
			return 0, 0, image, err
		}
		return readGraphRoundPredecessor(q, configuration, bound)
	}
	round, index, image, err = readGraphRoundRecord(q, configuration, bound)
	if err != nil {
		return
	}
	if index < r.index {
		if round != r.round || image != r.image {
			return 0, 0, image, errors.Join(errGraphHistoryExpired, raftlog.ErrApplicationExpired)
		}
		index = r.index
	}
	if err = chargeGraphWork(q, graphstore.PageWork{Records: 1, Bytes: 2048}); err != nil {
		return
	}
	if err = reserveComposer(q, 2048); err != nil {
		return
	}
	root, e := q.view.RootAt(q.ctx, index, 172)
	if e != nil || !bytes.Equal(root.Image, image[:]) {
		return 0, 0, image, errors.Join(errCorrupt, e)
	}
	return
}

// ToRetainedOnly irreversibly refuses new snapshot opens through this pin.
// Existing held sources remain valid owners until Close and prevent physical
// collection. Generic certified At follows actual GC floors, not this mode bit.
// Partial/unknown installs retain their originals and retry the SAME request.
func (o *retainedFeedOwner) ToRetainedOnly(ctx context.Context, request [16]byte) error {
	if !o.valid() || ctx == nil || request == ([16]byte{}) || !retainedOnlyFeedAgreement(o.publisher.coordinator.host.machine.SemanticContractID()) {
		return errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return errRetainedFeedClosed
	}
	operation, finish, err := o.operation(ctx)
	if err != nil {
		return err
	}
	defer finish()
	for _, part := range o.pin.definition.parts {
		e := o.pin.invocation.endpoints[part.certificate.scope.partition]
		c := o.command(e, feedRetainOnly, retainedFeedPhaseAction(request, feedRetainOnly), nil)
		result, err := e.submitRetainedFeed(operation, c)
		if err != nil {
			return errors.Join(errRetainedFeedPending, err)
		}
		if err = retainedFeedStatusError(result.status); err != nil {
			return err
		}
	}
	return nil
}
