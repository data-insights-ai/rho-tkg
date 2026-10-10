package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This agreement supports declared local initialization and the adapted
// txnproto allocation protocol. It deliberately has no graph-write or prepare
// command: adding actual claims/intents and fence quiescence needs a new reviewed
// agreement. Legacy graphapply's agreement and wire vectors remain unchanged.
const declaredSemanticDescriptor = "rho-tkg:graphapply:declared-partition-contract:v1\x00" +
	"typed-native-values=1\x00" +
	"partition-initialization=1\x00" +
	"allocation-home=genesis-first-v1\x00" +
	"recipient-fencing=txnproto-v1\x00" +
	"request-recovery=1\x00" +
	"logical-initialization-effects=2\x00" +
	"graph-writes=unavailable\x00"

func declaredSemanticContractID() raftlog.ApplicationSemanticContractID {
	return raftlog.ApplicationSemanticContractID(sha256.Sum256([]byte(declaredSemanticDescriptor)))
}

// allocationScope is a qualified application observation, never an expected
// caller group, quorum certificate or lease. Repeated Group bytes in different
// partitions remain distinct. Store-local generations are deliberately absent.
type allocationScope struct {
	graph       idalloc.GraphID
	partition   uint64
	group       [16]byte
	ownership   uint64
	topology    uint64
	declaration [32]byte
	semantic    raftlog.ApplicationSemanticContractID
}

const allocationScopeBytes = 156

func (s allocationScope) valid() bool {
	return s.graph != (idalloc.GraphID{}) && s.partition != 0 && s.group != ([16]byte{}) && s.ownership != 0 && s.topology != 0 && s.declaration != ([32]byte{}) && s.semantic != (raftlog.ApplicationSemanticContractID{})
}

func (s allocationScope) check(d graphstore.OwnershipDeclaration, cfg genesisAllocationConfig) error {
	if !s.valid() || s.semantic != declaredSemanticContractID() || s.graph != cfg.graph || graphstate.GraphID(s.graph) != d.Graph() || s.topology != d.TopologyEpoch() || s.declaration != d.Digest() {
		return errInvalid
	}
	if err := cfg.checkGenesisDeclaration(d); err != nil {
		return err
	}
	entry, found := d.Partition(s.partition)
	if !found || entry.Group != s.group || entry.OwnershipEpoch != s.ownership {
		return errInvalid
	}
	return nil
}

func appendAllocationScope(b []byte, s allocationScope) []byte {
	b = append(b, s.graph[:]...)
	b = binary.BigEndian.AppendUint64(b, s.partition)
	b = append(b, s.group[:]...)
	b = binary.BigEndian.AppendUint64(b, s.ownership)
	b = binary.BigEndian.AppendUint64(b, s.topology)
	b = append(b, s.declaration[:]...)
	return append(b, s.semantic[:]...)
}

func readAllocationScope(d *decoder) allocationScope {
	s := allocationScope{graph: idalloc.GraphID(d.array()), partition: d.number(), group: d.array(), ownership: d.number(), topology: d.number()}
	copy(s.declaration[:], d.b[:32])
	d.b = d.b[32:]
	copy(s.semantic[:], d.b[:32])
	d.b = d.b[32:]
	return s
}

func encodeAllocationScope(s allocationScope) ([]byte, error) {
	if !s.valid() {
		return nil, errInvalid
	}
	b := make([]byte, 0, allocationScopeBytes)
	b = append(b, 'A', 'P', 'S', 1)
	return seal(appendAllocationScope(b, s)), nil
}

func decodeAllocationScope(b []byte) (allocationScope, error) {
	body, err := wireBody(b, "APS\x01", allocationScopeBytes)
	if err != nil {
		return allocationScope{}, err
	}
	d := decoder{b: body}
	s := readAllocationScope(&d)
	if len(d.b) != 0 || !s.valid() {
		return allocationScope{}, errCorrupt
	}
	return s, nil
}

// A coordinate always carries its qualified source. It is an application replay
// coordinate, not graph time. Source and recipient indexes cannot be ordered
// numerically against each other.
type allocationCoordinate struct {
	scope allocationScope
	index uint64
}

func (p allocationCoordinate) valid() bool { return p.scope.valid() && p.index != 0 }

// protocolGrant is the same immutable globally allocated range at source/home.
// source identifies the ORIGINAL reservation entry, not the later quorum-read
// observation. installed is a separate local installation coordinate. Replaying
// the observation never changes either range or creates fresh cursor authority.
type protocolGrant struct {
	configuration [32]byte
	source        allocationCoordinate
	grant         idalloc.Grant
	installed     uint64
}

const protocolGrantBytes = 372

func (g protocolGrant) valid(n namespace) bool {
	return n.valid() && g.configuration != ([32]byte{}) && g.source.valid() && g.source.scope.graph == n.graph && g.grant.Request.Graph == n.graph && idalloc.ValidateGrant(g.grant) == nil && g.installed != 0
}

func encodeProtocolGrant(n namespace, g protocolGrant) ([]byte, error) {
	if !g.valid(n) {
		return nil, errInvalid
	}
	b := make([]byte, 0, protocolGrantBytes)
	b = appendNamespace(append(b, 'A', 'P', 'G', 1), n)
	b = append(b, g.configuration[:]...)
	b = appendAllocationScope(b, g.source.scope)
	b = binary.BigEndian.AppendUint64(b, g.source.index)
	b = appendGrant(b, g.grant)
	b = binary.BigEndian.AppendUint64(b, g.installed)
	return seal(b), nil
}

func decodeProtocolGrant(b []byte, n namespace) (protocolGrant, error) {
	body, err := wireBody(b, "APG\x01", protocolGrantBytes)
	if err != nil {
		return protocolGrant{}, err
	}
	d := decoder{b: body}
	stored := d.ns()
	var g protocolGrant
	copy(g.configuration[:], d.b[:32])
	d.b = d.b[32:]
	g.source = allocationCoordinate{scope: readAllocationScope(&d), index: d.number()}
	g.grant, err = d.grant()
	g.installed = d.number()
	if err != nil {
		return protocolGrant{}, errors.Join(errCorrupt, err)
	}
	if stored != n || len(d.b) != 0 || !g.valid(n) {
		return protocolGrant{}, errCorrupt
	}
	return g, nil
}

type recipientPhase byte

const (
	recipientAbsent recipientPhase = iota
	recipientPending
	recipientFenced
	recipientActive
)

// Source/home phases remain separate when the allocation home is also the
// recipient home. This prevents source activation from accidentally publishing
// home issuance. Delayed fences must preserve each active phase and sequence.
type protocolRecipient struct {
	configuration            [32]byte
	session                  idalloc.RecipientSession
	home, previous           uint64
	sourcePhase, homePhase   recipientPhase
	sourceSequence, sequence uint64
	sourceFence, homeFence   uint64
	homeAck, activation      allocationCoordinate
}

const protocolRecipientBaseBytes = 183
const protocolRecipientMaxBytes = protocolRecipientBaseBytes + 2*128

func (r protocolRecipient) valid() bool {
	if r.configuration == ([32]byte{}) || idalloc.ValidateRecipientSession(r.session) != nil || r.home == 0 || r.previous == math.MaxUint64 || r.session.Epoch != r.previous+1 || r.sourcePhase > recipientActive || r.homePhase != recipientAbsent && r.homePhase != recipientFenced && r.homePhase != recipientActive || r.sourcePhase == recipientAbsent && r.homePhase == recipientAbsent {
		return false
	}
	if r.sourcePhase != recipientActive && r.sourceSequence != 0 || r.homePhase != recipientActive && r.sequence != 0 || (r.sourcePhase >= recipientFenced) != (r.sourceFence != 0) || (r.homePhase >= recipientFenced) != (r.homeFence != 0) {
		return false
	}
	if r.homePhase == recipientActive && r.sourcePhase != recipientAbsent && r.sourcePhase != recipientActive {
		return false
	}
	hasAck := r.homeAck != (allocationCoordinate{})
	hasActivation := r.activation != (allocationCoordinate{})
	if hasAck && (!r.homeAck.valid() || r.homeAck.scope.partition != r.home || r.sourcePhase < recipientFenced) || r.sourcePhase == recipientActive && !hasAck {
		return false
	}
	if hasActivation && (!r.activation.valid() || r.sourcePhase != recipientActive && r.homePhase != recipientActive) || (r.sourcePhase == recipientActive || r.homePhase == recipientActive) && !hasActivation {
		return false
	}
	return r.sourcePhase != recipientPending || r.homePhase == recipientAbsent && !hasAck && !hasActivation
}

func encodeProtocolRecipient(n namespace, r protocolRecipient) ([]byte, error) {
	if !n.valid() || !r.valid() {
		return nil, errInvalid
	}
	flags := byte(0)
	if r.homeAck != (allocationCoordinate{}) {
		flags |= 1
	}
	if r.activation != (allocationCoordinate{}) {
		flags |= 2
	}
	size := protocolRecipientBaseBytes
	if flags&1 != 0 {
		size += 128
	}
	if flags&2 != 0 {
		size += 128
	}
	b := make([]byte, 0, size)
	b = appendNamespace(append(b, 'A', 'P', 'R', 1), n)
	b = append(b, r.configuration[:]...)
	b = appendSession(b, r.session)
	for _, v := range []uint64{r.home, r.previous} {
		b = binary.BigEndian.AppendUint64(b, v)
	}
	b = append(b, byte(r.sourcePhase), byte(r.homePhase))
	for _, v := range []uint64{r.sourceSequence, r.sequence, r.sourceFence, r.homeFence} {
		b = binary.BigEndian.AppendUint64(b, v)
	}
	b = append(b, flags)
	for _, p := range []allocationCoordinate{r.homeAck, r.activation} {
		if p == (allocationCoordinate{}) {
			continue
		}
		b = appendAllocationScope(b, p.scope)
		b = binary.BigEndian.AppendUint64(b, p.index)
	}
	return seal(b), nil
}

func decodeProtocolRecipient(b []byte, n namespace) (protocolRecipient, error) {
	if len(b) < protocolRecipientBaseBytes || len(b) > protocolRecipientMaxBytes {
		return protocolRecipient{}, errCorrupt
	}
	flags := b[protocolRecipientBaseBytes-33]
	if flags > 3 {
		return protocolRecipient{}, errCorrupt
	}
	size := protocolRecipientBaseBytes
	if flags&1 != 0 {
		size += 128
	}
	if flags&2 != 0 {
		size += 128
	}
	body, err := wireBody(b, "APR\x01", size)
	if err != nil {
		return protocolRecipient{}, err
	}
	d := decoder{b: body}
	stored := d.ns()
	var r protocolRecipient
	copy(r.configuration[:], d.b[:32])
	d.b = d.b[32:]
	r.session = d.session()
	r.home, r.previous = d.number(), d.number()
	r.sourcePhase, r.homePhase = recipientPhase(d.b[0]), recipientPhase(d.b[1])
	d.b = d.b[2:]
	r.sourceSequence, r.sequence, r.sourceFence, r.homeFence = d.number(), d.number(), d.number(), d.number()
	d.b = d.b[1:]
	if flags&1 != 0 {
		r.homeAck = allocationCoordinate{scope: readAllocationScope(&d), index: d.number()}
	}
	if flags&2 != 0 {
		r.activation = allocationCoordinate{scope: readAllocationScope(&d), index: d.number()}
	}
	if stored != n || len(d.b) != 0 || !r.valid() {
		return protocolRecipient{}, errCorrupt
	}
	return r, nil
}

func sameProtocolRecipient(a, b protocolRecipient) bool {
	return a.configuration == b.configuration && a.session == b.session && a.home == b.home && a.previous == b.previous
}

func beginProtocolRecipient(old *protocolRecipient, next protocolRecipient) (protocolRecipient, bool, reason, error) {
	if !next.valid() || next.sourcePhase != recipientPending || next.homePhase != recipientAbsent {
		return protocolRecipient{}, false, reasonInvalid, nil
	}
	if old == nil {
		if next.previous != 0 {
			return protocolRecipient{}, false, reasonStale, nil
		}
		return next, true, reasonNone, nil
	}
	if !old.valid() {
		return protocolRecipient{}, false, reasonNone, errCorrupt
	}
	if sameProtocolRecipient(*old, next) {
		return *old, false, reasonNone, nil // never reset active sequence/phase.
	}
	if old.sourcePhase != recipientActive || old.session.Epoch != next.previous || old.home != next.home || old.configuration != next.configuration {
		return protocolRecipient{}, false, reasonStale, nil
	}
	return next, true, reasonNone, nil
}

func fenceProtocolSource(old *protocolRecipient, observed protocolRecipient, source allocationScope, index uint64) (protocolRecipient, bool, reason, error) {
	if index == 0 || !source.valid() || !observed.valid() || observed.sourcePhase != recipientPending && observed.sourcePhase != recipientFenced {
		return protocolRecipient{}, false, reasonInvalid, nil
	}
	if old == nil || !sameProtocolRecipient(*old, observed) {
		return protocolRecipient{}, false, reasonStale, nil
	}
	if !old.valid() {
		return protocolRecipient{}, false, reasonNone, errCorrupt
	}
	if old.sourcePhase == recipientFenced || old.sourcePhase == recipientActive {
		return *old, false, reasonNone, nil
	}
	if old.sourcePhase != recipientPending {
		return protocolRecipient{}, false, reasonStale, nil
	}
	next := *old
	next.sourcePhase, next.sourceFence = recipientFenced, index
	if observed.home == source.partition {
		next.homePhase, next.homeFence = recipientFenced, index
	}
	return next, true, reasonNone, nil
}

func fenceProtocolHome(old *protocolRecipient, observed protocolRecipient, index uint64) (protocolRecipient, bool, reason, error) {
	if index == 0 || !observed.valid() || observed.sourcePhase != recipientPending && observed.sourcePhase != recipientFenced {
		return protocolRecipient{}, false, reasonInvalid, nil
	}
	if old != nil {
		if !old.valid() {
			return protocolRecipient{}, false, reasonNone, errCorrupt
		}
		if sameProtocolRecipient(*old, observed) && (old.homePhase == recipientFenced || old.homePhase == recipientActive) {
			return *old, false, reasonNone, nil
		}
		if old.homePhase != recipientActive && old.homePhase != recipientFenced || old.session.Epoch != observed.previous || old.home != observed.home || old.configuration != observed.configuration {
			return protocolRecipient{}, false, reasonStale, nil
		}
	} else if observed.previous != 0 {
		return protocolRecipient{}, false, reasonStale, nil
	}
	next := protocolRecipient{configuration: observed.configuration, session: observed.session, home: observed.home, previous: observed.previous, homePhase: recipientFenced, homeFence: index}
	return next, true, reasonNone, nil
}

func acknowledgeProtocolHome(old *protocolRecipient, home protocolRecipient, scope allocationScope) (protocolRecipient, bool, reason, error) {
	if old == nil || !home.valid() || home.homePhase != recipientFenced || !scope.valid() || scope.partition != home.home || !sameProtocolRecipient(*old, home) {
		return protocolRecipient{}, false, reasonStale, nil
	}
	if !old.valid() {
		return protocolRecipient{}, false, reasonNone, errCorrupt
	}
	ack := allocationCoordinate{scope: scope, index: home.homeFence}
	if old.homeAck != (allocationCoordinate{}) {
		if old.homeAck != ack {
			return protocolRecipient{}, false, reasonMismatch, nil
		}
		return *old, false, reasonNone, nil
	}
	if old.sourcePhase != recipientFenced {
		return protocolRecipient{}, false, reasonStale, nil
	}
	next := *old
	next.homeAck = ack
	return next, true, reasonNone, nil
}

func activateProtocolRecipient(old *protocolRecipient, fenced protocolRecipient, ack allocationCoordinate, source allocationScope, index uint64) (protocolRecipient, bool, reason, error) {
	if old == nil || index == 0 || !fenced.valid() || fenced.sourcePhase != recipientFenced || !source.valid() || !ack.valid() || !sameProtocolRecipient(*old, fenced) || old.homeAck != ack || ack.scope.partition != old.home {
		return protocolRecipient{}, false, reasonStale, nil
	}
	if !old.valid() {
		return protocolRecipient{}, false, reasonNone, errCorrupt
	}
	if old.sourcePhase == recipientActive {
		return *old, false, reasonNone, nil
	}
	if old.sourcePhase != recipientFenced || old.sourceFence != fenced.sourceFence {
		return protocolRecipient{}, false, reasonStale, nil
	}
	next := *old
	next.sourcePhase = recipientActive
	next.activation = allocationCoordinate{scope: source, index: index}
	return next, true, reasonNone, nil
}

func publishProtocolRecipient(old *protocolRecipient, active protocolRecipient) (protocolRecipient, bool, reason, error) {
	if old == nil || !active.valid() || active.sourcePhase != recipientActive || !sameProtocolRecipient(*old, active) {
		return protocolRecipient{}, false, reasonStale, nil
	}
	if !old.valid() {
		return protocolRecipient{}, false, reasonNone, errCorrupt
	}
	if old.homePhase == recipientActive {
		if old.activation != active.activation {
			return protocolRecipient{}, false, reasonMismatch, nil
		}
		return *old, false, reasonNone, nil
	}
	if old.homePhase != recipientFenced || active.homeAck.index != old.homeFence {
		return protocolRecipient{}, false, reasonStale, nil
	}
	next := *old
	next.homePhase, next.activation = recipientActive, active.activation
	return next, true, reasonNone, nil
}

type allocationObservationKind byte

const (
	observeConfiguration allocationObservationKind = iota + 1
	observeRecipient
	observeAllocator
	observeGrant
)

// Observations are bounded wire data carried by source-created proposals, not
// publicly constructible completed-read proofs. The host constructs proofs only
// from its matching applied ReadIndex event and actual same-view storage reads.
type allocationObservation struct {
	kind          allocationObservationKind
	source        allocationCoordinate
	configuration [32]byte
	epoch         uint64
	effect        [32]byte
	config        genesisAllocationConfig
	recipient     protocolRecipient
	allocator     idalloc.State
	grant         protocolGrant
}

const allocationObservationMaxBytes = 1024

const (
	transferDeclaredService commandKind = iota + 9
	beginDeclaredRecipient
	fenceDeclaredRecipient
	acknowledgeDeclaredHome
	activateDeclaredRecipient
	publishDeclaredRecipient
	reserveDeclaredGrant
	installDeclaredGrant
)

type allocationProtocolCommand struct {
	ns              namespace
	id              requestID
	kind            commandKind
	configuration   [32]byte
	session         idalloc.RecipientSession
	home, previous  uint64
	replacement     idalloc.Authority
	sequence, count uint64
	remote          allocationObservation
}

func isAllocationProtocolCommand(kind commandKind) bool {
	switch kind {
	case transferDeclaredService, beginDeclaredRecipient, fenceDeclaredRecipient, acknowledgeDeclaredHome, activateDeclaredRecipient, publishDeclaredRecipient, reserveDeclaredGrant, installDeclaredGrant:
		return true
	default:
		return false
	}
}

func (r allocationProtocolCommand) validate() error {
	if !r.ns.valid() || r.id == (requestID{}) || r.configuration == ([32]byte{}) || !isAllocationProtocolCommand(r.kind) || r.remote.configuration != r.configuration || r.remote.source.scope.graph != r.ns.graph {
		return errInvalid
	}
	if _, err := r.remote.payload(); err != nil {
		return err
	}
	expected := allocationProtocolCommand{ns: r.ns, id: r.id, kind: r.kind, configuration: r.configuration, remote: r.remote}
	switch r.kind {
	case transferDeclaredService:
		if r.remote.kind != observeAllocator || r.replacement.Owner() == ([16]byte{}) || r.replacement.Epoch() == 0 {
			return errInvalid
		}
		expected.replacement = r.replacement
	case beginDeclaredRecipient:
		if r.remote.kind != observeConfiguration && r.remote.kind != observeRecipient || idalloc.ValidateRecipientSession(r.session) != nil || r.home == 0 || r.previous == math.MaxUint64 || r.session.Epoch != r.previous+1 {
			return errInvalid
		}
		expected.session, expected.home, expected.previous = r.session, r.home, r.previous
	case fenceDeclaredRecipient, acknowledgeDeclaredHome, activateDeclaredRecipient, publishDeclaredRecipient:
		if r.remote.kind != observeRecipient {
			return errInvalid
		}
	case reserveDeclaredGrant:
		if r.remote.kind != observeAllocator || idalloc.ValidateGrantRequest(idalloc.GrantRequest{Graph: r.ns.graph, Session: r.session, Sequence: r.sequence, Count: r.count}) != nil {
			return errInvalid
		}
		expected.session, expected.sequence, expected.count = r.session, r.sequence, r.count
	case installDeclaredGrant:
		if r.remote.kind != observeGrant {
			return errInvalid
		}
	}
	if r != expected {
		return errInvalid
	}
	return nil
}

const allocationCommandMetadataBytes = 4096

func emitAllocationProtocolCommand(w *boundedWriter, r allocationProtocolCommand) {
	w.add([]byte{'A', 'Q', 'P', 1, byte(r.kind)})
	w.add(r.ns.graph[:])
	w.u64(r.ns.partition)
	w.add(r.id[:])
	w.add(r.configuration[:])
	switch r.kind {
	case transferDeclaredService:
		w.add(appendAuthority(nil, r.replacement))
	case beginDeclaredRecipient:
		w.add(appendSession(nil, r.session))
		w.u64(r.home)
		w.u64(r.previous)
	case reserveDeclaredGrant:
		w.add(appendSession(nil, r.session))
		w.u64(r.sequence)
		w.u64(r.count)
	}
	observation, err := encodeAllocationObservation(r.remote)
	if err != nil {
		w.err = err
		return
	}
	w.field(observation)
}

func encodeAllocationProtocolCommand(r allocationProtocolCommand, l materializerLimits) ([]byte, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	// The fixed maximum bounds nested codec scratch before validation. The
	// replica uses this same precharge, so host admission cannot underfund it.
	if allocationCommandMetadataBytes+8*(requestHeaderBytes+32+56+4+allocationObservationMaxBytes+32) > l.commandOwnedBytes {
		return nil, errLimit
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	return boundedEncoding(l.commandBytes, func(w *boundedWriter) { emitAllocationProtocolCommand(w, r) })
}

func decodeAllocationProtocolCommand(b []byte, l materializerLimits) (allocationProtocolCommand, error) {
	if err := l.validate(); err != nil {
		return allocationProtocolCommand{}, err
	}
	if allocationCommandMetadataBytes+8*(requestHeaderBytes+32+56+4+allocationObservationMaxBytes+32) > l.commandOwnedBytes {
		return allocationProtocolCommand{}, errLimit
	}
	if len(b) < requestHeaderBytes || len(b) > 4<<20 || !bytes.Equal(b[:4], []byte{'A', 'Q', 'P', 1}) || !isAllocationProtocolCommand(commandKind(b[4])) {
		return allocationProtocolCommand{}, errCorrupt
	}
	body, err := graphBody(b, "AQP\x01", l.commandBytes)
	if err != nil {
		return allocationProtocolCommand{}, err
	}
	c := graphCursor{b: body}
	r := allocationProtocolCommand{kind: commandKind(c.tag()), ns: namespace{graph: idalloc.GraphID(c.array()), partition: c.u64()}, id: requestID(c.array())}
	copy(r.configuration[:], c.take(32))
	switch r.kind {
	case transferDeclaredService:
		r.replacement, err = idalloc.NewAuthority(c.array(), c.u64())
	case beginDeclaredRecipient:
		r.session = idalloc.RecipientSession{ID: c.array(), Incarnation: c.array(), Epoch: c.u64()}
		r.home, r.previous = c.u64(), c.u64()
	case reserveDeclaredGrant:
		r.session = idalloc.RecipientSession{ID: c.array(), Incarnation: c.array(), Epoch: c.u64()}
		r.sequence, r.count = c.u64(), c.u64()
	}
	if c.err != nil || err != nil {
		return allocationProtocolCommand{}, errors.Join(errCorrupt, c.err, err)
	}
	r.remote, err = decodeAllocationObservation(c.field(allocationObservationMaxBytes))
	if c.err != nil || err != nil || len(c.b) != 0 {
		return allocationProtocolCommand{}, errors.Join(errCorrupt, c.err, err)
	}
	if err := r.validate(); err != nil {
		return allocationProtocolCommand{}, errors.Join(errCorrupt, err)
	}
	return r, nil
}

type allocationRecordReader struct {
	q           *reader
	scope       allocationScope
	config      genesisAllocationConfig
	declaration graphstore.OwnershipDeclaration
}

func (a allocationRecordReader) recipient(id [16]byte) (*protocolRecipient, error) {
	wire, found, err := a.q.getBounded(recipientKey(a.q.ns, id), protocolRecipientMaxBytes)
	if err != nil || !found {
		return nil, err
	}
	r, err := decodeProtocolRecipient(wire, a.q.ns)
	if err != nil {
		return nil, err
	}
	digest, err := a.config.digest()
	if err != nil || r.session.ID != id || r.configuration != digest {
		return nil, errors.Join(errCorrupt, err)
	}
	sourceHere, homeHere := a.q.ns.partition == a.config.home, a.q.ns.partition == r.home
	if !sourceHere && !homeHere || sourceHere != (r.sourcePhase != recipientAbsent) || !homeHere && r.homePhase != recipientAbsent || r.sourceFence > a.q.base.Index || r.homeFence > a.q.base.Index {
		return nil, errCorrupt
	}
	for _, coord := range []allocationCoordinate{r.homeAck, r.activation} {
		if coord == (allocationCoordinate{}) {
			continue
		}
		if err := coord.scope.check(a.declaration, a.config); err != nil {
			return nil, errors.Join(errCorrupt, err)
		}
		if coord.scope == a.scope && coord.index > a.q.base.Index {
			return nil, errCorrupt
		}
	}
	if r.activation != (allocationCoordinate{}) && r.activation.scope.partition != a.config.home {
		return nil, errCorrupt
	}
	return &r, nil
}

func (a allocationRecordReader) putRecipient(r protocolRecipient) error {
	wire, err := encodeProtocolRecipient(a.q.ns, r)
	if err != nil {
		return err
	}
	return a.q.put(recipientKey(a.q.ns, r.session.ID), wire)
}

func (a allocationRecordReader) grant(session idalloc.RecipientSession, sequence uint64) (*protocolGrant, error) {
	wire, found, err := a.q.getBounded(grantKey(a.q.ns, session, sequence), protocolGrantBytes)
	if err != nil || !found {
		return nil, err
	}
	g, err := decodeProtocolGrant(wire, a.q.ns)
	if err != nil {
		return nil, err
	}
	digest, err := a.config.digest()
	if err != nil || g.configuration != digest || g.installed > a.q.base.Index || g.grant.Request.Session != session || g.grant.Request.Sequence != sequence || g.source.scope.partition != a.config.home {
		return nil, errors.Join(errCorrupt, err)
	}
	if err := g.source.scope.check(a.declaration, a.config); err != nil {
		return nil, errors.Join(errCorrupt, err)
	}
	if g.source.scope == a.scope && g.source.index > a.q.base.Index {
		return nil, errCorrupt
	}
	return &g, nil
}

func (a allocationRecordReader) putGrant(g protocolGrant) error {
	wire, err := encodeProtocolGrant(a.q.ns, g)
	if err != nil {
		return err
	}
	return a.q.put(grantKey(a.q.ns, g.grant.Request.Session, g.grant.Request.Sequence), wire)
}

func (a allocationRecordReader) checkObservation(o allocationObservation) error {
	digest, err := a.config.digest()
	if err != nil {
		return err
	}
	if o.configuration != digest || o.source.scope.check(a.declaration, a.config) != nil || o.source.scope == a.scope && o.source.index > a.q.base.Index {
		return errInvalid
	}
	if _, err := o.payload(); err != nil {
		return err
	}
	if o.kind == observeGrant {
		g := o.grant
		if g.source.scope.partition != a.config.home || g.source.scope == o.source.scope && g.source.index > o.source.index || g.installed > o.source.index {
			return errInvalid
		}
		if err := g.source.scope.check(a.declaration, a.config); err != nil {
			return err
		}
	}
	if o.kind == observeRecipient {
		r := o.recipient
		for _, coordinate := range []allocationCoordinate{r.homeAck, r.activation} {
			if coordinate == (allocationCoordinate{}) {
				continue
			}
			if err := coordinate.scope.check(a.declaration, a.config); err != nil {
				return err
			}
			if coordinate.scope == o.source.scope && coordinate.index > o.source.index {
				return errInvalid
			}
		}
		if r.activation != (allocationCoordinate{}) && r.activation.scope.partition != a.config.home {
			return errInvalid
		}
		if o.source.scope.partition != a.config.home && o.source.scope.partition != r.home {
			return errInvalid
		}
		if o.source.scope.partition == a.config.home && r.sourcePhase == recipientAbsent || o.source.scope.partition == r.home && r.homePhase == recipientAbsent && r.sourcePhase != recipientPending || r.sourceFence > o.source.index || r.homeFence > o.source.index {
			return errInvalid
		}
	}
	return nil
}

func (a allocationRecordReader) transition(r allocationProtocolCommand, index uint64) (outcome, error) {
	o := outcome{ns: a.q.ns, kind: r.kind, identity: [16]byte(r.id), index: index, disposition: applied}
	if err := a.checkObservation(r.remote); err != nil {
		return o, err
	}
	digest, err := a.config.digest()
	if err != nil || digest != r.configuration {
		return o, errors.Join(errInvalid, err)
	}
	if r.kind == installDeclaredGrant {
		g := r.remote.grant
		if r.remote.source.scope.partition != a.config.home || g.source.scope.partition != a.config.home {
			o.reason = reasonInvalid
			return o, nil
		}
		current, err := a.recipient(g.grant.Request.Session.ID)
		if err != nil {
			return o, err
		}
		if current == nil || current.home != a.q.ns.partition || current.homePhase != recipientActive || current.session != g.grant.Request.Session {
			o.reason = reasonStale
			return o, nil
		}
		existing, err := a.grant(g.grant.Request.Session, g.grant.Request.Sequence)
		if err != nil {
			return o, err
		}
		if existing != nil {
			if existing.configuration != g.configuration || existing.source != g.source || existing.grant != g.grant {
				o.reason = reasonMismatch
			} else {
				o.disposition = controlRecovery
			}
			return o, nil
		}
		g.installed = index
		if err := a.putGrant(g); err != nil {
			return o, err
		}
		current.sequence = max(current.sequence, g.grant.Request.Sequence)
		return o, a.putRecipient(*current)
	}
	if r.kind == transferDeclaredService || r.kind == reserveDeclaredGrant {
		return a.allocatorTransition(r, index, o)
	}
	var id [16]byte
	if r.kind == beginDeclaredRecipient {
		id = r.session.ID
	} else {
		id = r.remote.recipient.session.ID
	}
	current, err := a.recipient(id)
	if err != nil {
		return o, err
	}
	var next protocolRecipient
	var changed bool
	sourceHere := a.q.ns.partition == a.config.home
	switch r.kind {
	case beginDeclaredRecipient:
		if !sourceHere || r.remote.source.scope.partition != r.home || r.remote.kind == observeRecipient && (r.remote.recipient.session.ID != id || r.remote.recipient.session.Epoch != r.previous || r.remote.recipient.home != r.home || r.remote.recipient.homePhase != recipientActive && r.remote.recipient.homePhase != recipientFenced) || r.remote.kind == observeConfiguration && r.previous != 0 {
			o.reason = reasonStale
			return o, nil
		}
		next, changed, o.reason, err = beginProtocolRecipient(current, protocolRecipient{configuration: digest, session: r.session, home: r.home, previous: r.previous, sourcePhase: recipientPending})
	case fenceDeclaredRecipient:
		if r.remote.source.scope.partition != a.config.home {
			o.reason = reasonInvalid
			return o, nil
		}
		if sourceHere {
			next, changed, o.reason, err = fenceProtocolSource(current, r.remote.recipient, a.scope, index)
		} else if a.q.ns.partition == r.remote.recipient.home {
			next, changed, o.reason, err = fenceProtocolHome(current, r.remote.recipient, index)
		} else {
			o.reason = reasonInvalid
		}
	case acknowledgeDeclaredHome:
		if !sourceHere || r.remote.source.scope.partition != r.remote.recipient.home {
			o.reason = reasonInvalid
			return o, nil
		}
		next, changed, o.reason, err = acknowledgeProtocolHome(current, r.remote.recipient, r.remote.source.scope)
	case activateDeclaredRecipient:
		if !sourceHere || r.remote.source.scope.partition != a.config.home {
			o.reason = reasonInvalid
			return o, nil
		}
		next, changed, o.reason, err = activateProtocolRecipient(current, r.remote.recipient, r.remote.recipient.homeAck, a.scope, index)
	case publishDeclaredRecipient:
		if a.q.ns.partition != r.remote.recipient.home || r.remote.source.scope.partition != a.config.home {
			o.reason = reasonInvalid
			return o, nil
		}
		next, changed, o.reason, err = publishProtocolRecipient(current, r.remote.recipient)
	default:
		return o, errInvalid
	}
	if err != nil || o.reason != reasonNone {
		return o, err
	}
	if !changed {
		o.disposition = controlRecovery
		return o, nil
	}
	return o, a.putRecipient(next)
}

func (a allocationRecordReader) allocatorTransition(r allocationProtocolCommand, index uint64, o outcome) (outcome, error) {
	if a.q.ns.partition != a.config.home || r.remote.source.scope.partition != a.config.home {
		o.reason = reasonInvalid
		return o, nil
	}
	if r.kind == reserveDeclaredGrant {
		existing, err := a.grant(r.session, r.sequence)
		if err != nil {
			return o, err
		}
		if existing != nil {
			if existing.grant.Request.Count != r.count {
				o.reason = reasonMismatch
			} else {
				o.disposition = grantRecovery
			}
			return o, nil
		}
	}
	state, found, err := a.q.allocator()
	if err != nil {
		return o, err
	}
	if !found {
		return o, errCorrupt
	}
	current, err := idalloc.Inspect(&state)
	if err != nil {
		return o, err
	}
	observed, err := idalloc.Inspect(&r.remote.allocator)
	if err != nil {
		return o, err
	}
	var next idalloc.State
	if r.kind == transferDeclaredService {
		if current.Authority == r.replacement {
			o.disposition = controlRecovery
			return o, nil
		}
		next, err = idalloc.Transfer(&state, observed.Authority, r.replacement)
	} else {
		recipient, err := a.recipient(r.session.ID)
		if err != nil {
			return o, err
		}
		if recipient == nil || recipient.sourcePhase != recipientActive || recipient.session != r.session || current.Authority != observed.Authority {
			o.reason = reasonStale
			return o, nil
		}
		if r.sequence <= recipient.sourceSequence {
			o.reason = reasonMismatch
			return o, nil
		}
		if current.LastSequence == math.MaxUint64 {
			o.reason = reasonExhausted
			return o, nil
		}
		var block idalloc.Reservation
		var replay bool
		next, block, replay, err = idalloc.Reserve(&state, idalloc.Request{Graph: a.q.ns.graph, Authority: current.Authority, Sequence: current.LastSequence + 1, Count: r.count})
		if err == nil {
			if replay {
				return o, errCorrupt
			}
			grant := protocolGrant{configuration: r.configuration, source: allocationCoordinate{scope: a.scope, index: index}, grant: idalloc.Grant{Request: idalloc.GrantRequest{Graph: a.q.ns.graph, Session: r.session, Sequence: r.sequence, Count: r.count}, Reservation: block}, installed: index}
			if err := a.putGrant(grant); err != nil {
				return o, err
			}
			recipient.sourceSequence = r.sequence
			if err := a.putRecipient(*recipient); err != nil {
				return o, err
			}
		}
	}
	if err != nil {
		if why, ok := rejectReason(err); ok {
			o.reason = why
			return o, nil
		}
		return o, err
	}
	wire, err := idalloc.MarshalCheckpoint(&next)
	if err != nil {
		return o, err
	}
	return o, a.q.put(allocatorKey(a.q.ns), wire)
}

func (o allocationObservation) payload() ([]byte, error) {
	if !o.source.valid() || o.configuration == ([32]byte{}) || o.epoch == 0 || o.effect == ([32]byte{}) {
		return nil, errInvalid
	}
	expected := allocationObservation{kind: o.kind, source: o.source, configuration: o.configuration, epoch: o.epoch, effect: o.effect}
	n := namespace{graph: o.source.scope.graph, partition: o.source.scope.partition}
	var wire []byte
	var err error
	switch o.kind {
	case observeConfiguration:
		expected.config = o.config
		digest, e := o.config.digest()
		if e != nil || digest != o.configuration || o.config.graph != n.graph {
			return nil, errors.Join(errInvalid, e)
		}
		wire, err = encodeGenesisAllocationConfig(o.config)
	case observeRecipient:
		expected.recipient = o.recipient
		if o.recipient.configuration != o.configuration {
			return nil, errInvalid
		}
		wire, err = encodeProtocolRecipient(n, o.recipient)
	case observeAllocator:
		expected.allocator = o.allocator
		view, e := idalloc.Inspect(&o.allocator)
		if e != nil || view.Graph != n.graph {
			return nil, errors.Join(errInvalid, e)
		}
		wire, err = idalloc.MarshalCheckpoint(&o.allocator)
	case observeGrant:
		expected.grant = o.grant
		if o.grant.configuration != o.configuration {
			return nil, errInvalid
		}
		wire, err = encodeProtocolGrant(n, o.grant)
	default:
		return nil, errInvalid
	}
	if o != expected {
		return nil, errInvalid
	}
	return wire, err
}

func encodeAllocationObservation(o allocationObservation) ([]byte, error) {
	payload, err := o.payload()
	if err != nil {
		return nil, err
	}
	return boundedEncoding(allocationObservationMaxBytes, func(w *boundedWriter) {
		w.add([]byte{'A', 'P', 'O', 1})
		w.tag(byte(o.kind))
		w.add(appendAllocationScope(nil, o.source.scope))
		w.u64(o.source.index)
		w.add(o.configuration[:])
		w.u64(o.epoch)
		w.add(o.effect[:])
		w.field(payload)
	})
}

func decodeAllocationObservation(b []byte) (allocationObservation, error) {
	body, err := graphBody(b, "APO\x01", allocationObservationMaxBytes)
	if err != nil {
		return allocationObservation{}, err
	}
	c := graphCursor{b: body}
	o := allocationObservation{kind: allocationObservationKind(c.tag())}
	wire := c.take(120)
	if c.err != nil {
		return allocationObservation{}, c.err
	}
	d := decoder{b: wire}
	o.source = allocationCoordinate{scope: readAllocationScope(&d), index: c.u64()}
	copy(o.configuration[:], c.take(32))
	o.epoch = c.u64()
	copy(o.effect[:], c.take(32))
	payload := c.field(protocolRecipientMaxBytes)
	if c.err != nil || len(c.b) != 0 {
		return allocationObservation{}, errors.Join(errCorrupt, c.err)
	}
	n := namespace{graph: o.source.scope.graph, partition: o.source.scope.partition}
	switch o.kind {
	case observeConfiguration:
		o.config, err = decodeGenesisAllocationConfig(payload, n.graph)
	case observeRecipient:
		o.recipient, err = decodeProtocolRecipient(payload, n)
	case observeAllocator:
		o.allocator, err = idalloc.DecodeCheckpoint(payload)
	case observeGrant:
		o.grant, err = decodeProtocolGrant(payload, n)
	default:
		return allocationObservation{}, errCorrupt
	}
	if err != nil {
		return allocationObservation{}, errors.Join(errCorrupt, err)
	}
	if _, err := o.payload(); err != nil {
		return allocationObservation{}, errors.Join(errCorrupt, err)
	}
	return o, nil
}
