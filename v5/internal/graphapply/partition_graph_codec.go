package graphapply

import (
	"bytes"
	"errors"
)

// partitionGraphCommand is a typed proposal, not a caller Delta or grant. Its
// graph-local genesis fences are revalidated by the materializer's retained view.
type partitionGraphCommand struct {
	configuration, declaration, routing [32]byte
	request                             graphRequest
}

func (r partitionGraphCommand) validate(l materializerLimits) error {
	if r.configuration == ([32]byte{}) || r.declaration == ([32]byte{}) || r.routing == ([32]byte{}) || !isGraphMutation(r.request.kind) {
		return errInvalid
	}
	return validateGraphRequest(r.request, l)
}
func encodePartitionGraphCommand(r partitionGraphCommand, l materializerLimits) ([]byte, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	if err := r.validate(l); err != nil {
		return nil, err
	}
	inner, err := encodeTypedGraphRequest(r.request, l, true)
	if err != nil {
		return nil, err
	}
	// The inner decoder covers its full representation. Only the enclosing
	// fixed fields and extra frame bytes are additional simultaneous backing.
	overhead := 128 + 8*(requestHeaderBytes+3*32+4+32)
	if l.commandOwnedBytes <= overhead {
		return nil, errLimit
	}
	nested := l
	nested.commandOwnedBytes -= overhead
	if _, err := decodeTypedGraphRequest(inner, nested, true); err != nil {
		return nil, err
	}
	return boundedEncoding(l.commandBytes, func(w *boundedWriter) {
		w.add([]byte{'P', 'G', 'Q', 1})
		w.tag(byte(r.request.kind))
		w.add(r.request.ns.graph[:])
		w.u64(r.request.ns.partition)
		id := r.request.identity()
		w.add(id[:])
		w.add(r.configuration[:])
		w.add(r.declaration[:])
		w.add(r.routing[:])
		w.field(inner)
	})
}
func decodePartitionGraphCommand(b []byte, l materializerLimits) (partitionGraphCommand, error) {
	return decodePartitionGraphCommandOwned(b, l, nil)
}
func decodePartitionGraphCommandOwned(b []byte, l materializerLimits, admitted *int) (partitionGraphCommand, error) {
	if admitted != nil {
		*admitted = 0
	}
	if err := l.validate(); err != nil {
		return partitionGraphCommand{}, err
	}
	body, err := graphBody(b, "PGQ\x01", l.commandBytes)
	if err != nil {
		return partitionGraphCommand{}, err
	}
	c := graphCursor{b: body, maxOwnedBytes: l.commandOwnedBytes}
	overhead := 128 + 8*(requestHeaderBytes+3*32+4+32)
	if !c.charge(overhead) {
		return partitionGraphCommand{}, c.err
	}
	kind := commandKind(c.tag())
	graph := c.array()
	partition := c.u64()
	identity := c.array()
	var r partitionGraphCommand
	copy(r.configuration[:], c.take(32))
	copy(r.declaration[:], c.take(32))
	copy(r.routing[:], c.take(32))
	inner := c.field(l.commandBytes)
	if c.err != nil || len(c.b) != 0 {
		return partitionGraphCommand{}, errors.Join(errCorrupt, c.err)
	}
	nested := l
	nested.commandOwnedBytes -= overhead
	if nested.commandOwnedBytes < 1 {
		return partitionGraphCommand{}, errLimit
	}
	innerOwned := 0
	r.request, err = decodeTypedGraphRequestOwned(inner, nested, true, &innerOwned)
	if err != nil {
		return partitionGraphCommand{}, err
	}
	if [16]byte(r.request.ns.graph) != graph || r.request.ns.partition != partition || r.request.identity() != identity || r.request.kind != kind {
		return partitionGraphCommand{}, errCorrupt
	}
	if err := r.validate(l); err != nil {
		return partitionGraphCommand{}, errors.Join(errCorrupt, err)
	}
	wire, err := encodePartitionGraphCommand(r, l)
	if err != nil {
		return partitionGraphCommand{}, err
	}
	if !bytes.Equal(wire, b) {
		return partitionGraphCommand{}, errCorrupt
	}
	if admitted != nil {
		*admitted = c.ownedBytes + innerOwned
	}
	return r, nil
}

const (
	reasonRemoteParticipant reason = 9
	reasonRoutingUnknown    reason = 10
)

func validTypedGraphOutcomeReason(kind commandKind, why reason, partition bool) bool {
	if partition && (why == reasonRemoteParticipant || why == reasonRoutingUnknown) {
		return isGraphMutation(kind)
	}
	return validGraphOutcomeReason(kind, why)
}
func decodePartitionGraphOutcome(b []byte, n namespace) (outcome, error) {
	return decodeTypedGraphOutcome(b, n, true)
}
func decodePartitionMappedOutcome(b []byte, n namespace) (outcome, error) {
	if len(b) >= 5 && bytes.Equal(b[:4], []byte{'G', 'R', 'O', 4}) {
		return decodePartitionGraphOutcome(b, n)
	}
	if len(b) >= 5 && (commandKind(b[4]) == initDeclaredPartition || isAllocationProtocolCommand(commandKind(b[4]))) {
		return decodeDeclaredOutcome(b, n)
	}
	return outcome{}, errCorrupt
}
