package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Feed controls have their own immutable agreement. In particular, an older
// durable-column agreement cannot interpret a GFL command as ordinary work.
// The feed retains the existing coarse, complete-participant prepare protocol.
func retainedFeedSemanticContractID() raftlog.ApplicationSemanticContractID {
	base := durableColumnGraphSemanticContractID()
	return raftlog.ApplicationSemanticContractID(sha256.Sum256(append(base[:], []byte("snapshot-feed=complete-native-envelopes-v1\x00cursor=full-vector-pending-page-v1\x00ack=same-original-all-keepers-v1\x00payload=original-round-root-contributions-v1\x00progress=bounded-native-prefix-census32-v1\x00work=durable-admitted-operation-ceilings-v1\x00")...)))
}

func durableColumnGraphAgreement(id raftlog.ApplicationSemanticContractID) bool {
	return id == durableColumnGraphSemanticContractID() || id == retainedFeedSemanticContractID()
}

const (
	feedInitialize byte = iota + 1
	feedSealPage
	feedAcknowledge
	feedAdmitOperation
)

const (
	feedApplied byte = iota + 1
	feedPending
	feedExpired
	feedReleased
	feedMismatch
	feedLimited
)

const (
	feedLocalEnvelope byte = iota + 1
	feedCrossEnvelope
	feedMoveEnvelope
)

const (
	retainedFeedMaxBytes      = 256 << 10
	retainedFeedMaxProofs     = 4096
	retainedFeedProgressBatch = 32
	retainedFeedCommandBytes  = retainedFeedMaxBytes + 2*graphPinManifestBytes + 4096
)

var errRetainedFeedPending = errors.New("graphapply: snapshot feed pending; resume exact page or ACK original")
var errRetainedFeedClosed = errors.New("graphapply: local feed owner closed; durable pin state requires keeper authentication")

// Policy bounds durable metadata as well as transient work. It does not enlarge
// the store, source or materializer quotas: those independently admit each use.
type retainedFeedPolicy struct {
	pageBytes, pageRows                            int
	maxPages                                       uint64
	maxAttempts                                    uint64
	operationRows, operationBytes, operationOutput int
	deadlineUnixNanos                              int64 // API lifetime only, never replicated GC/lease authority
}

func (p retainedFeedPolicy) valid() bool {
	return p.pageBytes >= 4096 && p.pageBytes <= retainedFeedMaxBytes && p.pageRows >= 1 && p.pageRows <= 4096 && p.maxPages > 0 && p.maxPages < math.MaxUint64 && p.maxAttempts > 0 && p.maxAttempts <= math.MaxUint64/(64<<20) && p.operationRows >= 16 && p.operationRows <= 1<<20 && p.operationBytes >= 32768 && p.operationBytes <= 64<<20 && p.operationOutput >= 32768 && p.operationOutput <= 64<<20 && p.deadlineUnixNanos > 0
}
func (p retainedFeedPolicy) fits(l materializerLimits) bool {
	return p.valid() && p.operationRows <= l.sourceRows && p.operationBytes <= l.sourceBytes && p.operationOutput <= l.outputBytes && p.pageBytes+2*graphPinManifestBytes+4096 <= l.commandBytes
}

type retainedFeedDefinition struct {
	manifest []byte
	policy   retainedFeedPolicy
}

// A raw frame is bounded native evidence, not a column projection. Protocol
// retries retain their original sidecars so empty current CDC cannot conceal a
// different earlier outcome. Only the first installed business CDC is emitted.
type retainedFeedFrame struct {
	partition, index, round          uint64
	root                             [172]byte
	outcome, changes                 []byte
	registration                     []byte
	intent, decision                 []byte
	originalOutcome, originalChanges []byte
	originalBeforeCut                bool
	// Nonzero only for an authenticated protocol-only contiguous census. The
	// first actual frame remains visible; census commits every native frame in
	// [index, progressNext), stopping before the first business envelope.
	progressNext uint64
	census       [32]byte
}

// Each proof records a bounded actual native lookup at one exact root. A scan
// also retains its exact bounds/continuation and completeness. Neither a copied
// row nor a missing row in this DTO establishes authority before native sealing.
type retainedFeedLookup struct {
	kind               byte // 1 Get, 2 Scan
	key, upper, after  []byte
	rows, bytes        int
	visited, workBytes int
	found, complete    bool
	next               []byte
	values             []raftlog.KV
}
type retainedFeedPayload struct {
	partition, index uint64
	root             [172]byte
	lookups          []retainedFeedLookup
}
type retainedFeedTip struct {
	index uint64
	root  [172]byte
}

type retainedFeedPage struct {
	request            [16]byte
	sequence           uint64
	original, previous [32]byte
	from, next         []uint64 // next unconsumed physical application index
	boundary           graphCut
	priorCut           graphCut
	// Actual completed native observations, independent of immutable GSC/PLM
	// business coordinates. Future business heads remain unconsumed.
	tips     []retainedFeedTip
	future   []bool
	frames   []retainedFeedFrame
	payloads []retainedFeedPayload
	kind     byte // zero is explicit protocol-only progress, never an empty graph proof
	tx       [16]byte
	round    uint64
	eof      bool
}

type retainedFeedCursor struct {
	original              [32]byte
	sequence              uint64
	positions             []uint64
	completed             graphCut
	pending, acknowledged [32]byte
	// Full offered work is spent at native admission, even if its reply or the
	// subsequent operation is lost. Actual pre-admission transport/decode work is
	// observed caller/server cost, not invented durable application work.
	admitted, spentRows, spentBytes, spentOutput, spentCalls uint64
}

type retainedFeedCommand struct {
	scope          allocationScope
	issuer, action [16]byte
	phase          byte
	definition     []byte
	page           []byte
	original       [32]byte // derived from canonical definition; not another wire field
}
type retainedFeedResult struct {
	original, command, page [32]byte
	action                  [16]byte
	index, sequence         uint64
	phase, status           byte
}

func retainedFeedCursorKey(n namespace, id [32]byte) []byte {
	return append(recordKey(n, 33), id[:]...)
}
func retainedFeedDefinitionKey(n namespace, id [32]byte) []byte {
	return append(recordKey(n, 34), id[:]...)
}
func retainedFeedOriginalKey(n namespace, issuer, action [16]byte) []byte {
	return append(append(recordKey(n, 35), issuer[:]...), action[:]...)
}
func retainedFeedPageKey(n namespace, id, page [32]byte) []byte {
	return append(append(recordKey(n, 36), id[:]...), page[:]...)
}
func retainedFeedPhaseAction(request [16]byte, phase byte) [16]byte {
	wire := append([]byte("rho-tkg:retained-feed-action:v1\x00"), request[:]...)
	wire = append(wire, phase)
	sum := sha256.Sum256(wire)
	return [16]byte(sum[:16])
}

func encodeRetainedFeedDefinition(d retainedFeedDefinition) ([]byte, error) {
	m, err := decodeGraphPinManifest(d.manifest)
	if err != nil || m.publisher.semantic != retainedFeedSemanticContractID() || !d.policy.valid() {
		return nil, errors.Join(errInvalid, err)
	}
	return boundedEncoding(graphPinManifestBytes+128, func(w *boundedWriter) {
		w.add([]byte("GFD\x01"))
		w.field(d.manifest)
		w.u32(d.policy.pageBytes)
		w.u32(d.policy.pageRows)
		w.u64(d.policy.maxPages)
		w.u64(d.policy.maxAttempts)
		w.u32(d.policy.operationRows)
		w.u32(d.policy.operationBytes)
		w.u32(d.policy.operationOutput)
		w.u64(uint64(d.policy.deadlineUnixNanos))
	})
}
func decodeRetainedFeedDefinition(wire []byte) (retainedFeedDefinition, error) {
	body, err := graphBody(wire, "GFD\x01", graphPinManifestBytes+128)
	if err != nil {
		return retainedFeedDefinition{}, err
	}
	c := graphCursor{b: body}
	d := retainedFeedDefinition{manifest: c.field(graphPinManifestBytes), policy: retainedFeedPolicy{pageBytes: c.count(retainedFeedMaxBytes, 0), pageRows: c.count(4096, 0), maxPages: c.u64()}}
	d.policy.maxAttempts = c.u64()
	d.policy.operationRows, d.policy.operationBytes, d.policy.operationOutput = c.count(1<<20, 0), c.count(64<<20, 0), c.count(64<<20, 0)
	deadline := c.u64()
	if deadline > math.MaxInt64 {
		c.err = errInvalid
	}
	d.policy.deadlineUnixNanos = int64(deadline)
	canonical, err := encodeRetainedFeedDefinition(d)
	if c.err != nil || len(c.b) != 0 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedFeedDefinition{}, errors.Join(errInvalid, c.err, err)
	}
	return d, nil
}

func emitFeedVector(w *boundedWriter, positions []uint64) {
	w.u32(len(positions))
	for _, n := range positions {
		w.u64(n)
	}
}
func readFeedVector(c *graphCursor) []uint64 {
	n := c.count(maxGraphPins, 8)
	if !c.charge(8 * n) {
		return nil
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = c.u64()
		if out[i] == 0 {
			c.err = errInvalid
		}
	}
	return out
}
func emitFeedCut(w *boundedWriter, cut graphCut) {
	w.u32(len(cut.certificates))
	for _, c := range cut.certificates {
		b, err := encodeGraphCertificate(c)
		if err != nil {
			w.err = err
			return
		}
		w.field(b)
	}
}
func readFeedCut(c *graphCursor) graphCut {
	n := c.count(maxGraphPins, 396)
	if !c.charge(512 * n) {
		return graphCut{}
	}
	out := graphCut{certificates: make([]graphCertificate, n)}
	for i := range out.certificates {
		var err error
		out.certificates[i], err = decodeGraphCertificate(c.field(512))
		if err != nil {
			c.err = err
		}
		if i > 0 && (out.certificates[i-1].scope.partition >= out.certificates[i].scope.partition || out.certificates[0].round != out.certificates[i].round) {
			c.err = errInvalid
		}
	}
	return out
}
func feedCutEqual(a, b graphCut) bool {
	if len(a.certificates) != len(b.certificates) {
		return false
	}
	for i := range a.certificates {
		if a.certificates[i] != b.certificates[i] {
			return false
		}
	}
	return true
}
func feedVectorEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func emitFeedFrame(w *boundedWriter, f retainedFeedFrame) {
	w.u64(f.partition)
	w.u64(f.index)
	w.u64(f.round)
	w.add(f.root[:])
	w.field(f.outcome)
	w.field(f.changes)
	w.field(f.registration)
	w.field(f.intent)
	w.field(f.decision)
	w.field(f.originalOutcome)
	w.field(f.originalChanges)
	if f.originalBeforeCut {
		w.tag(1)
	} else {
		w.tag(0)
	}
	w.u64(f.progressNext)
	w.add(f.census[:])
}
func readFeedFrame(c *graphCursor) retainedFeedFrame {
	f := retainedFeedFrame{partition: c.u64(), index: c.u64(), round: c.u64()}
	copy(f.root[:], c.take(172))
	f.outcome = c.field(retainedFeedMaxBytes)
	f.changes = c.field(retainedFeedMaxBytes)
	f.registration = c.field(retainedFeedMaxBytes)
	f.intent = c.field(1024)
	f.decision = c.field(256)
	f.originalOutcome = c.field(retainedFeedMaxBytes)
	f.originalChanges = c.field(retainedFeedMaxBytes)
	flag := c.tag()
	f.originalBeforeCut = flag == 1
	if flag > 1 {
		c.err = errInvalid
	}
	f.progressNext = c.u64()
	copy(f.census[:], c.take(32))
	return f
}
func emitFeedPayload(w *boundedWriter, p retainedFeedPayload) {
	w.u64(p.partition)
	w.u64(p.index)
	w.add(p.root[:])
	w.u32(len(p.lookups))
	for _, q := range p.lookups {
		w.tag(q.kind)
		w.field(q.key)
		w.field(q.upper)
		w.field(q.after)
		w.u32(q.rows)
		w.u32(q.bytes)
		w.u32(q.visited)
		w.u32(q.workBytes)
		var flags byte
		if q.found {
			flags |= 1
		}
		if q.complete {
			flags |= 2
		}
		w.tag(flags)
		w.field(q.next)
		w.u32(len(q.values))
		for _, r := range q.values {
			w.field(r.Key)
			w.field(r.Value)
		}
	}
}
func readFeedPayload(c *graphCursor) retainedFeedPayload {
	p := retainedFeedPayload{partition: c.u64(), index: c.u64()}
	copy(p.root[:], c.take(172))
	n := c.count(retainedFeedMaxProofs, 26)
	if !c.charge(256 * n) {
		return p
	}
	p.lookups = make([]retainedFeedLookup, n)
	for i := range p.lookups {
		q := &p.lookups[i]
		q.kind = c.tag()
		q.key = c.field(64 << 10)
		q.upper = c.field(64 << 10)
		q.after = c.field(64 << 10)
		q.rows = c.count(4096, 0)
		q.bytes = c.count(retainedFeedMaxBytes, 0)
		q.visited, q.workBytes = c.count(4096, 0), c.count(4*retainedFeedMaxBytes, 0)
		flags := c.tag()
		q.found, q.complete = flags&1 != 0, flags&2 != 0
		if flags > 3 || q.kind < 1 || q.kind > 2 {
			c.err = errInvalid
		}
		q.next = c.field(64 << 10)
		m := c.count(4096, 8)
		if !c.charge(64 * m) {
			return p
		}
		q.values = make([]raftlog.KV, m)
		for j := range q.values {
			q.values[j] = raftlog.KV{Key: c.field(64 << 10), Value: c.field(retainedFeedMaxBytes)}
		}
	}
	return p
}

func validFeedPageIdentity(p retainedFeedPage) bool {
	if p.kind == 0 {
		return p.tx == ([16]byte{}) && p.round == 0
	}
	return p.kind <= feedMoveEnvelope && p.tx != ([16]byte{}) && p.round > 0 && len(p.frames) > 0
}

func encodeRetainedFeedPage(p retainedFeedPage, maximum int) ([]byte, error) {
	if len(p.priorCut.certificates) != len(p.from) {
		return nil, errInvalid
	}
	if maximum < 4096 || maximum > retainedFeedMaxBytes || p.request == ([16]byte{}) || p.original == ([32]byte{}) || p.sequence == 0 || len(p.from) == 0 || len(p.from) > maxGraphPins || len(p.from) != len(p.next) || len(p.boundary.certificates) != len(p.from) || len(p.tips) != len(p.from) || len(p.future) != len(p.from) || len(p.frames) > maxGraphPins || len(p.payloads) > maxGraphPins || !validFeedPageIdentity(p) {
		return nil, errInvalid
	}
	for i, n := range p.from {
		if n == 0 || p.next[i] < n || p.tips[i].index == math.MaxUint64 || p.tips[i].index < p.boundary.certificates[i].index || p.next[i] > max(n, p.tips[i].index+1) {
			return nil, errInvalid
		}
	}
	for i, f := range p.frames {
		if f.partition == 0 || f.index == 0 || i > 0 && p.frames[i-1].partition >= f.partition {
			return nil, errInvalid
		}
		if f.progressNext != 0 && (p.kind != 0 || f.progressNext <= f.index || f.progressNext-f.index > retainedFeedProgressBatch || f.census == ([32]byte{})) || f.progressNext == 0 && f.census != ([32]byte{}) {
			return nil, errInvalid
		}
	}
	for i, v := range p.payloads {
		if v.partition == 0 || v.index == 0 || len(v.lookups) > retainedFeedMaxProofs || i > 0 && p.payloads[i-1].partition >= v.partition {
			return nil, errInvalid
		}
		for _, q := range v.lookups {
			if q.kind < 1 || q.kind > 2 || q.rows < 1 || q.rows > 4096 || q.bytes < 1 || q.bytes > retainedFeedMaxBytes || q.visited < 0 || q.visited > q.rows || q.workBytes < 0 || q.workBytes > q.bytes || len(q.key) == 0 || len(q.key) > 64<<10 || len(q.upper) > 64<<10 || len(q.after) > 64<<10 || len(q.next) > 64<<10 || len(q.values) > q.rows {
				return nil, errInvalid
			}
			if q.kind == 1 && (q.rows != 1 || q.complete || len(q.upper)+len(q.after)+len(q.next) != 0 || q.found != (len(q.values) == 1)) || q.kind == 2 && q.found {
				return nil, errInvalid
			}
			for _, row := range q.values {
				if len(row.Key) == 0 || len(row.Key) > 64<<10 || len(row.Value) > retainedFeedMaxBytes {
					return nil, errInvalid
				}
			}
		}
	}
	return boundedEncoding(maximum, func(w *boundedWriter) {
		w.add([]byte("GFP\x01"))
		w.add(p.request[:])
		w.u64(p.sequence)
		w.add(p.original[:])
		w.add(p.previous[:])
		emitFeedVector(w, p.from)
		emitFeedVector(w, p.next)
		emitFeedCut(w, p.boundary)
		emitFeedCut(w, p.priorCut)
		for i, t := range p.tips {
			w.u64(t.index)
			w.add(t.root[:])
			if p.future[i] {
				w.tag(1)
			} else {
				w.tag(0)
			}
		}
		w.tag(p.kind)
		w.add(p.tx[:])
		w.u64(p.round)
		if p.eof {
			w.tag(1)
		} else {
			w.tag(0)
		}
		w.u32(len(p.frames))
		for _, f := range p.frames {
			emitFeedFrame(w, f)
		}
		w.u32(len(p.payloads))
		for _, v := range p.payloads {
			emitFeedPayload(w, v)
		}
	})
}
func decodeRetainedFeedPage(wire []byte, maximum int, arena *graphstate.OutputBudget) (retainedFeedPage, error) {
	if arena == nil || maximum < 4096 || maximum > retainedFeedMaxBytes {
		return retainedFeedPage{}, errInvalid
	}
	// Counts, all delivered byte ownership, canonical scratch and maximum native
	// lookup structs are charged before decoding or copying. No default arena.
	if err := arena.Reserve(4096 + 16*len(wire)); err != nil {
		return retainedFeedPage{}, outputBudgetFailure(err)
	}
	body, err := graphBody(wire, "GFP\x01", maximum)
	if err != nil {
		return retainedFeedPage{}, err
	}
	c := graphCursor{b: body, maxOwnedBytes: 8*len(wire) + 4096}
	p := retainedFeedPage{request: c.array(), sequence: c.u64()}
	copy(p.original[:], c.take(32))
	copy(p.previous[:], c.take(32))
	p.from = readFeedVector(&c)
	p.next = readFeedVector(&c)
	p.boundary = readFeedCut(&c)
	p.priorCut = readFeedCut(&c)
	if !c.charge(256 * len(p.from)) {
		return retainedFeedPage{}, c.err
	}
	p.tips = make([]retainedFeedTip, len(p.from))
	p.future = make([]bool, len(p.from))
	for i := range p.tips {
		p.tips[i].index = c.u64()
		copy(p.tips[i].root[:], c.take(172))
		flag := c.tag()
		if flag > 1 {
			c.err = errInvalid
		}
		p.future[i] = flag == 1
	}
	p.kind = c.tag()
	p.tx = c.array()
	p.round = c.u64()
	flag := c.tag()
	p.eof = flag == 1
	n := c.count(maxGraphPins, 216)
	if !c.charge(512 * n) {
		return retainedFeedPage{}, c.err
	}
	p.frames = make([]retainedFeedFrame, n)
	for i := range p.frames {
		p.frames[i] = readFeedFrame(&c)
	}
	n = c.count(maxGraphPins, 192)
	if !c.charge(256 * n) {
		return retainedFeedPage{}, c.err
	}
	p.payloads = make([]retainedFeedPayload, n)
	for i := range p.payloads {
		p.payloads[i] = readFeedPayload(&c)
	}
	canonical, err := encodeRetainedFeedPage(p, maximum)
	if c.err != nil || len(c.b) != 0 || flag > 1 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedFeedPage{}, errors.Join(errInvalid, c.err, err)
	}
	return p, nil
}

func encodeRetainedFeedCursor(c retainedFeedCursor) ([]byte, error) {
	if c.original == ([32]byte{}) || len(c.positions) == 0 || len(c.positions) > maxGraphPins || len(c.completed.certificates) != len(c.positions) {
		return nil, errInvalid
	}
	return boundedEncoding(graphPinManifestBytes+2048, func(w *boundedWriter) {
		w.add([]byte("GFC\x01"))
		w.add(c.original[:])
		w.u64(c.sequence)
		emitFeedVector(w, c.positions)
		emitFeedCut(w, c.completed)
		w.add(c.pending[:])
		w.add(c.acknowledged[:])
		w.u64(c.admitted)
		w.u64(c.spentRows)
		w.u64(c.spentBytes)
		w.u64(c.spentOutput)
		w.u64(c.spentCalls)
	})
}
func decodeRetainedFeedCursor(wire []byte) (retainedFeedCursor, error) {
	body, err := graphBody(wire, "GFC\x01", graphPinManifestBytes+2048)
	if err != nil {
		return retainedFeedCursor{}, err
	}
	q := graphCursor{b: body}
	c := retainedFeedCursor{}
	copy(c.original[:], q.take(32))
	c.sequence = q.u64()
	c.positions = readFeedVector(&q)
	c.completed = readFeedCut(&q)
	copy(c.pending[:], q.take(32))
	copy(c.acknowledged[:], q.take(32))
	c.admitted, c.spentRows, c.spentBytes, c.spentOutput, c.spentCalls = q.u64(), q.u64(), q.u64(), q.u64(), q.u64()
	canonical, err := encodeRetainedFeedCursor(c)
	if q.err != nil || len(q.b) != 0 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedFeedCursor{}, errors.Join(errCorrupt, q.err, err)
	}
	return c, nil
}

func encodeRetainedFeedCommand(c retainedFeedCommand) ([]byte, error) {
	if !c.scope.valid() || c.scope.semantic != retainedFeedSemanticContractID() || c.issuer == ([16]byte{}) || c.action == ([16]byte{}) || c.phase < feedInitialize || c.phase > feedAdmitOperation || (c.phase == feedInitialize || c.phase == feedAdmitOperation) != (len(c.page) == 0) || len(c.page) > retainedFeedMaxBytes {
		return nil, errInvalid
	}
	d, err := decodeRetainedFeedDefinition(c.definition)
	if err != nil {
		return nil, err
	}
	m, err := decodeGraphPinManifest(d.manifest)
	if err != nil || m.issuer != c.issuer || m.publisher.graph != c.scope.graph || m.publisher.semantic != c.scope.semantic {
		return nil, errors.Join(errInvalid, err)
	}
	if c.original != ([32]byte{}) && c.original != m.id() {
		return nil, errInvalid
	}
	return boundedEncoding(retainedFeedCommandBytes, func(w *boundedWriter) {
		w.add([]byte("GFL\x01"))
		w.add(appendAllocationScope(nil, c.scope))
		w.add(c.issuer[:])
		w.add(c.action[:])
		w.tag(c.phase)
		w.field(c.definition)
		w.field(c.page)
	})
}
func decodeRetainedFeedCommand(wire []byte) (retainedFeedCommand, error) {
	body, err := graphBody(wire, "GFL\x01", retainedFeedCommandBytes)
	if err != nil {
		return retainedFeedCommand{}, err
	}
	q := graphCursor{b: body}
	scope := q.take(allocationScopeBytes - 36)
	c := retainedFeedCommand{scope: readAllocationScope(&decoder{b: scope}), issuer: q.array(), action: q.array(), phase: q.tag(), definition: q.field(graphPinManifestBytes + 128), page: q.field(retainedFeedMaxBytes)}
	canonical, err := encodeRetainedFeedCommand(c)
	if q.err != nil || len(q.b) != 0 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedFeedCommand{}, errors.Join(errInvalid, q.err, err)
	}
	d, err := decodeRetainedFeedDefinition(c.definition)
	if err != nil {
		return retainedFeedCommand{}, err
	}
	m, err := decodeGraphPinManifest(d.manifest)
	if err != nil {
		return retainedFeedCommand{}, err
	}
	c.original = m.id()
	return c, nil
}
func isRetainedFeedWire(wire []byte) bool { return bytes.HasPrefix(wire, []byte("GFL\x01")) }

func encodeRetainedFeedResult(r retainedFeedResult) ([]byte, error) {
	if r.index == 0 || r.original == ([32]byte{}) || r.command == ([32]byte{}) || r.action == ([16]byte{}) || r.phase < feedInitialize || r.phase > feedAdmitOperation || r.status < feedApplied || r.status > feedLimited {
		return nil, errInvalid
	}
	return boundedEncoding(256, func(w *boundedWriter) {
		w.add([]byte("GFO\x01"))
		w.add(r.original[:])
		w.add(r.command[:])
		w.add(r.page[:])
		w.add(r.action[:])
		w.u64(r.index)
		w.u64(r.sequence)
		w.tag(r.phase)
		w.tag(r.status)
	})
}
func decodeRetainedFeedResult(wire []byte) (retainedFeedResult, error) {
	body, err := graphBody(wire, "GFO\x01", 256)
	if err != nil {
		return retainedFeedResult{}, err
	}
	q := graphCursor{b: body}
	r := retainedFeedResult{}
	copy(r.original[:], q.take(32))
	copy(r.command[:], q.take(32))
	copy(r.page[:], q.take(32))
	r.action = q.array()
	r.index = q.u64()
	r.sequence = q.u64()
	r.phase = q.tag()
	r.status = q.tag()
	canonical, err := encodeRetainedFeedResult(r)
	if q.err != nil || len(q.b) != 0 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedFeedResult{}, errors.Join(errCorrupt, q.err, err)
	}
	return r, nil
}

func newRetainedFeedMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationRetention().Version != 1 || s.ApplicationRecovery().Version != 2 || s.ApplicationControls().Version != 1 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, retainedFeedSemanticContractID())
}

func encodeRetainedFeedToken(t retainedFeedToken) ([]byte, error) {
	d, err := decodeRetainedFeedDefinition(t.definition)
	if err != nil || !bytes.Equal(d.manifest, t.pin.manifest) {
		return nil, errors.Join(errInvalid, err)
	}
	pin, err := encodeRetainedPinToken(t.pin)
	if err != nil {
		return nil, err
	}
	return boundedEncoding(2*graphPinManifestBytes+8192, func(w *boundedWriter) { w.add([]byte("GFT\x01")); w.field(t.definition); w.field(pin) })
}
func decodeRetainedFeedToken(wire []byte) (retainedFeedToken, error) {
	body, err := graphBody(wire, "GFT\x01", 2*graphPinManifestBytes+8192)
	if err != nil {
		return retainedFeedToken{}, err
	}
	c := graphCursor{b: body}
	t := retainedFeedToken{definition: c.field(graphPinManifestBytes + 128)}
	t.pin, err = decodeRetainedPinToken(c.field(graphPinManifestBytes + 4096))
	if err != nil {
		return retainedFeedToken{}, err
	}
	canonical, err := encodeRetainedFeedToken(t)
	if c.err != nil || len(c.b) != 0 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedFeedToken{}, errors.Join(errInvalid, c.err, err)
	}
	t.definition = owned(t.definition)
	return t, nil
}
