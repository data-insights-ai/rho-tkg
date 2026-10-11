package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

// The immutable application binding selects this protocol on fresh stores.
// Genesis/allocation content is unchanged; old graph command frames are refused.
func crossPartitionSemanticContractID() raftlog.ApplicationSemanticContractID {
	return raftlog.ApplicationSemanticContractID(sha256.Sum256([]byte(partitionWriteSemanticDescriptor + "cross-graph=ordered-coarse-prepare-kernel-v1\x00recovery=protected-v2\x00capacity-abort=global-TxID-owner-v1\x00tx-id=sha256-128-graph-coordinator-request-v1\x00")))
}

// crossTransactionID names one coordinator-scoped retry key within a graph.
// Content, logical rounds and ownership epochs cannot change a retry's identity.
// The original raw key remains in the immutable request for local deduplication.
func crossTransactionID(n namespace, id requestID) [16]byte {
	b := appendNamespace([]byte("rho-tkg:cross-transaction-id:v1\x00"), n)
	b = append(b, id[:]...)
	digest := sha256.Sum256(b)
	return [16]byte(digest[:16])
}
func partitionGraphAgreement(id raftlog.ApplicationSemanticContractID) bool {
	return id == partitionWriteSemanticContractID() || crossGraphAgreement(id)
}
func configurationSemanticMatches(c genesisAllocationConfig, id raftlog.ApplicationSemanticContractID) bool {
	return c.semanticContractID() == id || c.version() == 3 && crossGraphAgreement(id)
}

type crossPhase byte

const (
	crossRegister crossPhase = iota + 1
	crossPrepare
	crossDecide
	crossResolve
)

// A manifest is source-created from the typed planner. Every declared authority
// participates: the coarse lock covers all positive and negative dependencies.
type crossManifest struct {
	tx                                            [16]byte
	coordinator                                   uint64
	original                                      []byte
	request                                       graphRequest
	configuration, declaration, routing, template [32]byte
	digest                                        [32]byte
	parts                                         []crossPart
}
type crossPart struct {
	binding    graphstore.PreparedKernelBinding
	identities graphstore.PreparedKernelIdentities
	kernel     []byte
	floor      uint64
}
type crossCommand struct {
	ns       namespace
	phase    crossPhase
	manifest []byte
	proofs   []crossStatus
	conflict *crossCapacityObservation
}

func isCrossPartitionWire(data []byte) bool {
	return len(data) >= 4 && string(data[:3]) == "GCT" && (data[3] == 1 || data[3] == 2)
}

type crossStatus struct {
	ns                  namespace
	tx                  [16]byte
	digest              [32]byte
	phase               crossPhase
	index, floor, round uint64
	yes, commit         bool
	why                 reason
}

func crossKey(n namespace, tx [16]byte) []byte {
	return append(recordKey(n, 11), tx[:]...)
}
func crossControlKey(n namespace, tx [16]byte, tag byte) []byte {
	b := append([]byte{'G', 'P', tag, 1}, n.graph[:]...)
	b = binary.BigEndian.AppendUint64(b, n.partition)
	return append(b, tx[:]...)
}
func crossLockKey(n namespace) []byte { return recordKey(n, 12) }

func emitCrossStatus(w *boundedWriter, s crossStatus) {
	w.add(s.ns.graph[:])
	w.u64(s.ns.partition)
	w.add(s.tx[:])
	w.add(s.digest[:])
	w.tag(byte(s.phase))
	w.u64(s.index)
	w.u64(s.floor)
	w.u64(s.round)
	flags := byte(0)
	if s.yes {
		flags |= 1
	}
	if s.commit {
		flags |= 2
	}
	w.tag(flags)
	w.tag(byte(s.why)) // #nosec G115 -- Internal refusal enum and decoded one-byte reasons fit the wire tag.
}
func readCrossStatus(c *graphCursor) crossStatus {
	s := crossStatus{ns: namespace{idalloc.GraphID(c.array()), c.u64()}, tx: c.array()}
	copy(s.digest[:], c.take(32))
	s.phase = crossPhase(c.tag())
	s.index = c.u64()
	s.floor = c.u64()
	s.round = c.u64()
	flags := c.tag()
	s.yes = flags&1 != 0
	s.commit = flags&2 != 0
	s.why = reason(c.tag())
	if flags > 3 || !s.ns.valid() || s.tx == ([16]byte{}) || s.digest == ([32]byte{}) || s.phase < crossRegister || s.phase > crossResolve || s.index == 0 {
		c.err = errCorrupt
	}
	return s
}
func encodeCrossStatus(s crossStatus) ([]byte, error) {
	return boundedEncoding(256, func(w *boundedWriter) { w.add([]byte{'G', 'C', 'S', 1}); emitCrossStatus(w, s) })
}
func decodeCrossStatus(wire []byte) (crossStatus, error) {
	body, err := graphBody(wire, "GCS\x01", 256)
	if err != nil {
		return crossStatus{}, err
	}
	c := graphCursor{b: body}
	s := readCrossStatus(&c)
	if c.err != nil || len(c.b) != 0 {
		return crossStatus{}, errors.Join(errCorrupt, c.err)
	}
	return s, nil
}
func encodeCrossCommand(r crossCommand, l materializerLimits) ([]byte, error) {
	if !r.ns.valid() || r.phase < crossRegister || r.phase > crossResolve || len(r.manifest) == 0 || len(r.proofs) > 256 {
		return nil, errInvalid
	}
	return boundedEncoding(l.commandBytes, func(w *boundedWriter) {
		version := byte(1)
		if r.conflict != nil {
			version = 2
		}
		w.add([]byte{'G', 'C', 'T', version})
		w.tag(byte(r.phase))
		w.add(r.ns.graph[:])
		w.u64(r.ns.partition)
		w.field(r.manifest)
		w.u32(len(r.proofs))
		for _, p := range r.proofs {
			emitCrossStatus(w, p)
		}
		if r.conflict != nil {
			emitCrossCapacityObservation(w, *r.conflict)
		}
	})
}
func decodeCrossCommand(wire []byte, l materializerLimits) (crossCommand, int, error) {
	version := byte(1)
	if len(wire) >= 4 && wire[3] == 2 {
		version = 2
	}
	body, err := graphBody(wire, string([]byte{'G', 'C', 'T', version}), l.commandBytes)
	if err != nil {
		return crossCommand{}, 0, err
	}
	c := graphCursor{b: body, maxOwnedBytes: l.commandOwnedBytes}
	if !c.charge(2048 + 8*len(wire)) {
		return crossCommand{}, 0, c.err
	}
	r := crossCommand{phase: crossPhase(c.tag()), ns: namespace{idalloc.GraphID(c.array()), c.u64()}}
	r.manifest = owned(c.field(l.commandBytes))
	n := c.count(256, 99)
	if !c.charge(256 * n) {
		return crossCommand{}, 0, c.err
	}
	r.proofs = make([]crossStatus, n)
	for i := range r.proofs {
		r.proofs[i] = readCrossStatus(&c)
	}
	if version == 2 {
		if !c.charge(1024) {
			return crossCommand{}, 0, c.err
		}
		observation := readCrossCapacityObservation(&c)
		r.conflict = new(observation)
	}
	if c.err != nil || len(c.b) != 0 {
		return crossCommand{}, 0, errors.Join(errCorrupt, c.err)
	}
	canonical, err := encodeCrossCommand(r, l)
	if err != nil || !bytes.Equal(canonical, wire) {
		return crossCommand{}, 0, errors.Join(errCorrupt, err)
	}
	return r, c.ownedBytes, nil
}

func encodeCrossManifest(m crossManifest, l materializerLimits) ([]byte, error) {
	return boundedEncoding(l.commandBytes, func(w *boundedWriter) {
		w.add([]byte{'G', 'C', 'M', 1})
		w.add(m.tx[:])
		w.u64(m.coordinator)
		w.field(m.original)
		for _, h := range [][32]byte{m.configuration, m.declaration, m.routing, m.template} {
			w.add(h[:])
		}
		w.u32(len(m.parts))
		for _, p := range m.parts {
			w.u64(p.binding.Partition)
			w.u64(p.floor)
			w.field(p.kernel)
		}
	})
}
func decodeCrossManifest(wire []byte, l materializerLimits, arena *graphstate.OutputBudget) (crossManifest, error) {
	body, err := graphBody(wire, "GCM\x01", l.commandBytes)
	if err != nil {
		return crossManifest{}, err
	}
	if err := arena.Reserve(2048 + 8*len(wire)); err != nil {
		return crossManifest{}, outputBudgetFailure(err)
	}
	c := graphCursor{b: body, maxOwnedBytes: l.commandOwnedBytes}
	m := crossManifest{tx: c.array(), coordinator: c.u64(), digest: sha256.Sum256(wire)}
	m.original = c.field(l.commandBytes)
	for _, h := range [][]byte{m.configuration[:], m.declaration[:], m.routing[:], m.template[:]} {
		copy(h, c.take(32))
	}
	n := c.count(256, 24)
	if !c.charge(1024 * n) {
		return crossManifest{}, c.err
	}
	m.parts = make([]crossPart, n)
	for i := range m.parts {
		partition := c.u64()
		m.parts[i].floor = c.u64()
		m.parts[i].kernel = c.field(l.catalog.MaxRecordBytes)
		if c.err != nil {
			return crossManifest{}, c.err
		}
		binding, rev, e := graphstore.InspectPreparedPartitionKernel(m.parts[i].kernel, l.catalog, l.graph, arena)
		if e != nil {
			return crossManifest{}, e
		}
		m.parts[i].binding = binding
		identities, e := graphstore.InspectPreparedKernelIdentities(m.parts[i].kernel, l.catalog, l.graph, arena)
		if e != nil {
			return crossManifest{}, e
		}
		m.parts[i].identities = identities
		if binding.Partition != partition || binding.TxID != m.tx || binding.Configuration != m.configuration || binding.DeclarationDigest != m.declaration || binding.RoutingDigest != m.routing || binding.TemplateDigest != m.template || i > 0 && partition <= m.parts[i-1].binding.Partition {
			return crossManifest{}, errCorrupt
		}
		if i == 0 {
			m.request.revision = rev
		} else if rev != m.request.revision {
			return crossManifest{}, errCorrupt
		}
	}
	if c.err != nil || len(c.b) != 0 || n == 0 {
		return crossManifest{}, errors.Join(errCorrupt, c.err)
	}
	revision := m.request.revision
	r, err := decodePartitionGraphCommand(m.original, l)
	if err != nil {
		return crossManifest{}, err
	}
	m.request = r.request
	if m.tx != crossTransactionID(m.request.ns, m.request.id) || m.coordinator != m.request.ns.partition || r.configuration != m.configuration || r.declaration != m.declaration || r.routing != m.routing || m.request.revision != revision {
		return crossManifest{}, errCorrupt
	}
	for _, p := range m.parts {
		if p.binding.Graph != graphstate.GraphID(m.request.ns.graph) || p.floor >= revision.ID() {
			return crossManifest{}, errCorrupt
		}
	}
	return m, nil
}

func (m crossManifest) part(partition uint64) (crossPart, bool) {
	for _, p := range m.parts {
		if p.binding.Partition == partition {
			return p, true
		}
	}
	return crossPart{}, false
}
func (m crossManifest) revision(round uint64) (state.Revision, error) {
	return state.NewRevision(round, m.request.revision.Provenance())
}

// Shape checks are shared by proposal admission and committed replay. A missing
// vote is pending, never evidence for a timeout-driven abort.
func validateCrossPhaseProofs(r crossCommand, m crossManifest) error {
	if r.conflict != nil {
		if r.phase != crossDecide || len(r.proofs) != 0 || r.ns.partition != m.coordinator {
			return errInvalid
		}
		return r.conflict.checkManifest(m)
	}
	match := func(p crossStatus, partition uint64, phase crossPhase) bool {
		return p.ns.graph == m.request.ns.graph && p.ns.partition == partition && p.tx == m.tx && p.digest == m.digest && p.phase == phase
	}
	switch r.phase {
	case crossRegister:
		if r.ns.partition != m.coordinator || len(r.proofs) != 0 {
			return errInvalid
		}
	case crossPrepare:
		position := -1
		for i, p := range m.parts {
			if p.binding.Partition == r.ns.partition {
				position = i
			}
		}
		if position < 0 || len(r.proofs) != position+1 || !match(r.proofs[0], m.coordinator, crossRegister) {
			return errInvalid
		}
		for i, p := range r.proofs[1:] {
			if !match(p, m.parts[i].binding.Partition, crossPrepare) || !p.yes {
				return errInvalid
			}
		}
	case crossDecide:
		if r.ns.partition != m.coordinator || len(r.proofs) > len(m.parts) {
			return errInvalid
		}
		if len(r.proofs) == 0 {
			return replica.ErrUnavailable
		}
		all := true
		for i, p := range r.proofs {
			if !match(p, m.parts[i].binding.Partition, crossPrepare) {
				return errInvalid
			}
			if !p.yes {
				all = false
			}
		}
		if all && len(r.proofs) != len(m.parts) {
			return replica.ErrUnavailable
		}
	case crossResolve:
		if len(r.proofs) != 1 || !match(r.proofs[0], m.coordinator, crossDecide) {
			return errInvalid
		}
		if r.proofs[0].commit && r.proofs[0].round <= m.request.revision.ID() || !r.proofs[0].commit && r.proofs[0].round != 0 {
			return errInvalid
		}
	default:
		return errInvalid
	}
	return nil
}
