package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Fresh-only bounded formats remove per-page recovery metadata and repeated
// lineage bodies. Numerical store/log/generation quotas are never increased.
const rangeMoveSpanMaxPages = 1 << 24
const rangeMoveSpanProgressBytes = 32 << 10
const rangeMoveSpanBatchRows = 32
const rangeMoveBatchFramingBytes = 4 + 4 + sha256.Size // magic, count, outer seal
const (
	moveSpanDescribe uint32 = 0x10000000
	moveSpanImport   uint32 = 0x20000000
	moveSpanSeal     uint32 = 0x30000000
	moveSpanDecide   uint32 = 0x30000001
	moveSpanActivate uint32 = 0x30000002
	moveSpanUndo     uint32 = 0x40000000
	moveSpanTerminal uint32 = 0xf0000000
)

func rangeMoveSpanSemanticContractID() raftlog.ApplicationSemanticContractID {
	base := retainedOnlyFeedSemanticContractID()
	return raftlog.ApplicationSemanticContractID(sha256.Sum256(append(base[:], []byte("range-move=compact-recovery-spans-v1\x00retention=immutable-native-reference-v1\x00replay=bounded-original-batches32-v1\x00")...)))
}
func rangeMoveSpans(id raftlog.ApplicationSemanticContractID) bool {
	return id == rangeMoveSpanSemanticContractID() || rangeMoveGraphBatches(id)
}
func rangeMovePageLimit(id raftlog.ApplicationSemanticContractID) int {
	if rangeMoveSpans(id) {
		return rangeMoveSpanMaxPages
	}
	return 1024
}
func rangeMoveStatusFormat(id raftlog.ApplicationSemanticContractID) byte {
	if rangeMovePoolsEnabled(id) {
		return 3
	}
	if rangeMoveSpans(id) {
		return 2
	}
	return 0
}
func newRangeMoveSpanMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationRecovery().Version != 3 || s.ApplicationRetention().Version != 1 || s.ApplicationControls().Version != 1 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, rangeMoveSpanSemanticContractID())
}
func (m *declaredMaterializer) spanMoveReservation(r rangeMoveIntent) (raftlog.ApplicationRecoveryReservation, error) {
	wire, err := encodeRangeMoveIntent(r)
	if err != nil {
		return raftlog.ApplicationRecoveryReservation{}, err
	}
	move := rangeMoveID(namespace{r.source.graph, r.source.partition}, r.id)
	out := raftlog.ApplicationRecoveryReservation{ID: rangeMoveRecoveryID(m.ns, move), Digest: sha256.Sum256(wire)}
	add := func(first, last uint32, command, retained, rows int) {
		out.Spans = append(out.Spans, raftlog.ApplicationRecoverySpan{First: first, Next: first, Last: last, CommandBytes: command, RetainedBytes: uint64(retained), RetainedRecords: uint64(rows)})
	} // #nosec G115 -- positive fixed bounds and validated <=64KiB page size.
	add(1, 1, 4096, rangeMoveRetentionBytes+(128<<10), 32)
	if m.ns.partition == r.source.partition {
		add(moveSpanDescribe, moveSpanDescribe+uint32(r.maxPages), 8192, 128<<10, 16)
	} else { // #nosec G115 -- maxPages <=2^24, disjoint phase band.
		add(moveSpanImport, moveSpanImport, (1<<20)-2048, 2<<20, 128)
		add(moveSpanImport+1, moveSpanImport+uint32(r.maxPages), min((1<<20)-2048, r.pageBytes+8192), (64<<10)+16*r.pageBytes+16384, 128) // #nosec G115 -- checked fresh maxPages/pageBytes admission.
	}
	add(moveSpanSeal, moveSpanSeal, 8192, 128<<10, 16)
	add(moveSpanDecide, moveSpanDecide, 8192, 128<<10, 16)
	add(moveSpanActivate, moveSpanActivate, 8192, 512<<10, 128)
	if m.ns.partition == r.target.partition {
		add(moveSpanUndo, moveSpanUndo+uint32(r.maxPages)-1, 8192, (64<<10)+16*r.pageBytes+16384, 128)
	} // #nosec G115 -- maxPages1..2^24, disjoint ascending band.
	add(moveSpanTerminal, moveSpanTerminal, 8192, 128<<10, 16)
	if rangeMovePoolsEnabled(m.SemanticContractID()) {
		// Terminal also restores the genuine three-row target fence GMU0.
		if err := applyMovePools(&out, r.pools); err != nil {
			return raftlog.ApplicationRecoveryReservation{}, err
		}
	}
	return out, nil
}
func moveSpanCredit(c rangeMoveCommand) uint32 {
	switch c.phase {
	case rangeMoveCapture:
		return 1
	case rangeMoveDescribe:
		return moveSpanDescribe + uint32(c.sequence) // #nosec G115 -- decoded fresh sequence<=2^24.
	case rangeMoveImport:
		return moveSpanImport + uint32(c.sequence) // #nosec G115 -- same finite sequence.
	case rangeMoveSeal:
		return moveSpanSeal
	case rangeMoveDecide:
		return moveSpanDecide
	case rangeMoveActivate:
		return moveSpanActivate
	case rangeMoveFinish:
		return moveSpanTerminal
	case rangeMoveAbort:
		if c.sequence == 0 {
			return moveSpanTerminal
		}
		return moveSpanUndo + uint32(c.sequence) - 1 // #nosec G115 -- native reverse undo publishes ascending undoNext+1, independent of actual undoTotal/maxPages.
	}
	return 0
}
func (m *declaredMaterializer) spanMoveFits(r raftlog.ApplicationRecoveryReservation) bool {
	if len(r.Spans) == 0 || len(r.Spans) > 8 || len(r.Credits) != 0 {
		return false
	}
	p, l := m.store.ApplicationLimits(), m.store.Limits()
	var totals [4]uint64
	for i, s := range r.Spans {
		if s.First == 0 || s.Next != s.First || s.Last < s.First || i > 0 && r.Spans[i-1].Last >= s.First || s.CommandBytes < 0 || s.CommandBytes > l.MaxEntryBytes-74 {
			return false
		}
		count := uint64(s.Last) - uint64(s.Next) + 1
		for j, each := range []uint64{s.RetainedBytes, s.RetainedRecords, uint64(s.CommandBytes + 83), 1} { // #nosec G115 -- command validated0..16MiB above.
			if each == 0 || count > math.MaxUint64/each || count*each > math.MaxUint64-totals[j] {
				return false
			}
			growth := count * each
			if j == 0 && s.BytePool.RetainedLimit != 0 {
				growth = s.BytePool.RetainedLimit
			}
			totals[j] += growth
		}
	}
	if rangeMovePoolsEnabled(m.SemanticContractID()) {
		if p.MaxInstallBytes < 1 || p.MaxTailEntries > math.MaxUint64/uint64(p.MaxInstallBytes) || p.MaxTailEntries*uint64(p.MaxInstallBytes) > math.MaxUint64-totals[0] {
			return false
		}
		totals[0] += p.MaxTailEntries * uint64(p.MaxInstallBytes) // #nosec G115 -- positive validated native policy.
	}
	return totals[0] <= p.RetainedApplicationBytes && totals[1] <= p.RetainedApplicationRecords && totals[2] <= l.MaxRetainedBytes && totals[3] <= l.MaxRetainedEntries
}
func spanCrossReservation(r raftlog.ApplicationRecoveryReservation) raftlog.ApplicationRecoveryReservation {
	out := raftlog.ApplicationRecoveryReservation{ID: r.ID, Digest: r.Digest, Spans: make([]raftlog.ApplicationRecoverySpan, len(r.Credits))}
	for i, c := range r.Credits {
		out.Spans[i] = raftlog.ApplicationRecoverySpan{First: c.Phase, Next: c.Phase, Last: c.Phase, CommandBytes: c.CommandBytes, RetainedBytes: c.RetainedBytes, RetainedRecords: c.RetainedRecords}
	}
	return out
}

// GMP4 references the SOURCE's actual immutable H0 header, plus the hash and
// complete length/row census of its GLB1 packet. Target J controls retain those
// same bytes once; they never become live original-group pin authority.
type moveRetentionReference struct {
	key            []byte
	header, packet [32]byte
	bytes, rows    uint64
}

func (p rangeMoveProgress) retainedBytes() uint64 {
	if rangeMoveSpans(p.descriptor.Source.Binding.SemanticContractID) {
		return p.reference.bytes
	}
	return uint64(len(p.retention))
}
func (p rangeMoveProgress) retainedMatches(wire []byte) bool {
	if !rangeMoveSpans(p.descriptor.Source.Binding.SemanticContractID) {
		return bytes.Equal(wire, p.retention)
	}
	return uint64(len(wire)) == p.reference.bytes && sha256.Sum256(wire) == p.reference.packet
}
func (p *rangeMoveProgress) bindRetention(packet rangeMoveRetainedLineage, wire []byte) error {
	if checkMoveRetentionDescriptor(packet, p.descriptor) != nil {
		return errCorrupt
	}
	ref := moveRetentionReference{key: rangeMoveRetentionKey(namespace{packet.scope.graph, packet.scope.partition}, packet.move, false, 0), header: sha256.Sum256(packet.header), packet: sha256.Sum256(wire), bytes: uint64(len(wire)), rows: uint64(len(packet.owners) + 1)}
	if p.reference.packet != ([32]byte{}) && (!bytes.Equal(p.reference.key, ref.key) || p.reference.header != ref.header || p.reference.packet != ref.packet || p.reference.bytes != ref.bytes || p.reference.rows != ref.rows) {
		return errCorrupt
	}
	p.reference = ref
	p.retentionRows = ref.rows
	return nil
}
func (m *declaredMaterializer) hydrateMoveRetention(q *reader, move [16]byte, p *rangeMoveProgress) error {
	if !rangeMoveSpans(m.SemanticContractID()) {
		return nil
	}
	shadow := m.ns.partition == p.descriptor.Spec.To.Partition
	packet, found, err := readMoveRetainedLineage(q, move, shadow)
	if err != nil || !found {
		return errors.Join(errCorrupt, err)
	}
	if packet.move != move || packet.scope.partition != p.descriptor.Spec.From.Partition {
		return errCorrupt
	}
	wire, err := encodeMoveRetainedLineage(packet, q.arena)
	if err != nil {
		return err
	}
	if err = p.bindRetention(packet, wire); err != nil {
		return err
	}
	p.retention = wire
	return nil
}
func moveStatusWireFor(id raftlog.ApplicationSemanticContractID, wire []byte) bool {
	if rangeMovePoolsEnabled(id) {
		return bytes.HasPrefix(wire, []byte("GMO\x03"))
	}
	if rangeMoveSpans(id) {
		return bytes.HasPrefix(wire, []byte("GMO\x02"))
	}
	return bytes.HasPrefix(wire, []byte("GMO\x01"))
}
func rangeMoveStatusPageLimit(format byte) int {
	if format == 2 || format == 3 {
		return rangeMoveSpanMaxPages
	}
	return 1024
}
func (r moveRetentionReference) valid(d graphstore.RangeMoveDescriptor) bool {
	if len(r.key) != 52 || r.header == ([32]byte{}) || r.packet == ([32]byte{}) || r.bytes == 0 || r.bytes > rangeMoveRetentionBytes || r.rows == 0 || r.rows > maxGraphPins+1 {
		return false
	}
	n := namespace{graph: idalloc.GraphID(d.Source.Binding.Identity.Graph), partition: d.Source.Binding.Identity.Partition}
	// H0 is a canonical immutable control, not a caller-selected physical prefix.
	return bytes.Equal(r.key[:4], []byte{'G', 'M', 'H', 1}) && bytes.Equal(r.key[4:28], appendNamespace(nil, n)) && !bytes.Equal(r.key[28:44], make([]byte, 16)) && bytes.Equal(r.key[44:], make([]byte, 8))
}
func (p rangeMoveProgress) emitReference(w *boundedWriter) {
	w.field(p.reference.key)
	w.add(p.reference.header[:])
	w.add(p.reference.packet[:])
	w.u64(p.reference.bytes)
	w.u64(p.reference.rows)
}
func readMoveReference(c *graphCursor) (r moveRetentionReference) {
	r.key = c.field(52)
	copy(r.header[:], c.take(32))
	copy(r.packet[:], c.take(32))
	r.bytes, r.rows = c.u64(), c.u64()
	return r
}

// Each bounded envelope contains genuine canonical old GEP1 subframes. Their
// original cursor/fragment counters remain independent of native batch pages.
type rangeMoveReplayBatch struct{ pages []rangeMoveReplayPage }

func encodeMoveReplayBatch(b rangeMoveReplayBatch, output *graphstate.OutputBudget, maximum int) ([]byte, error) {
	if output == nil || len(b.pages) == 0 || len(b.pages) > rangeMoveSpanBatchRows || maximum < 2048 || maximum > 1<<20 {
		return nil, errInvalid
	}
	if err := output.Reserve(32 * len(b.pages)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	fields := make([][]byte, 0, len(b.pages))
	size := rangeMoveBatchFramingBytes
	for i, p := range b.pages {
		if i > 0 && !rangeMoveReplaySame(b.pages[i-1].next, p.before) || i < len(b.pages)-1 && len(p.key) > 0 && !p.recordDone {
			return nil, errInvalid
		}
		wire, err := encodeRangeMoveReplayPage(p, output)
		if err != nil {
			return nil, err
		}
		if len(wire) > maximum-size-4 {
			return nil, errLimit
		}
		size += 4 + len(wire)
		fields = append(fields, wire)
	}
	if err := output.Reserve(4*size + 1024); err != nil {
		return nil, outputBudgetFailure(err)
	}
	return boundedEncoding(maximum, func(w *boundedWriter) {
		w.add([]byte("GEB\x01"))
		w.u32(len(fields))
		for _, p := range fields {
			w.field(p)
		}
	})
}
func decodeMoveReplayBatch(wire []byte, output *graphstate.OutputBudget, maximum int) (rangeMoveReplayBatch, error) {
	var out rangeMoveReplayBatch
	if output == nil || maximum < 2048 || maximum > 1<<20 {
		return out, errInvalid
	}
	body, err := graphBody(wire, "GEB\x01", maximum)
	if err != nil {
		return out, err
	}
	c := graphCursor{b: body}
	n := c.count(rangeMoveSpanBatchRows, 4)
	if c.err != nil || n == 0 {
		return out, errors.Join(errCorrupt, c.err)
	}
	if err := output.Reserve(1024 + 4*len(wire) + 512*n); err != nil {
		return out, outputBudgetFailure(err)
	}
	out.pages = make([]rangeMoveReplayPage, 0, n)
	for range n {
		p, err := decodeRangeMoveReplayPage(c.field(maximum), output)
		if err != nil {
			return rangeMoveReplayBatch{}, err
		}
		if len(out.pages) > 0 && !rangeMoveReplaySame(out.pages[len(out.pages)-1].next, p.before) {
			return rangeMoveReplayBatch{}, errCorrupt
		}
		out.pages = append(out.pages, p)
		if len(p.key) > 0 && !p.recordDone && len(out.pages) != n {
			return rangeMoveReplayBatch{}, errCorrupt
		}
	}
	if c.err != nil || len(c.b) != 0 {
		return rangeMoveReplayBatch{}, errors.Join(errCorrupt, c.err)
	}
	return out, nil
}
func exportMoveReplayBatch(q *reader, n namespace, before rangeMoveReplayCursor, maximum, rows int) (rangeMoveReplayBatch, error) {
	var out rangeMoveReplayBatch
	if q == nil || q.arena == nil || maximum < 2048 || maximum > 64<<10 || rows < 1 || rows > rangeMoveSpanBatchRows {
		return out, errInvalid
	}
	if err := q.arena.Reserve(512 * rows); err != nil {
		return out, outputBudgetFailure(err)
	}
	out.pages = make([]rangeMoveReplayPage, 0, rows)
	// A first page that cannot fit refuses before any native subcall, including
	// the declaration read (a framing refusal must not consume source work).
	if before.complete || maximum-rangeMoveBatchFramingBytes-4 < 1024+4*(len(before.after)+len(before.key))+2048 {
		return rangeMoveReplayBatch{}, errLimit
	}
	declaration, err := rangeMoveReplayReader(q, n)
	if err != nil {
		return rangeMoveReplayBatch{}, err
	}
	// One immutable declaration from this exact captured view, not one full
	// declaration allocation per page. No ownership escapes the atomic batch.
	// One shared reader ledger/arena, not32 independent full grants. The wire
	// allowance also pays every subframe cursor/header before another call.
	size := rangeMoveBatchFramingBytes
	cursor := before
	for len(out.pages) < rows && !cursor.complete {
		remaining := maximum - size - 4
		framing := 1024 + 4*(len(cursor.after)+len(cursor.key))
		if remaining < framing+2048 {
			break
		}
		p, err := exportRangeMoveReplayPageDeclared(q, n, cursor, raftlog.ReadBudget{Rows: 1, Bytes: remaining - framing}, declaration)
		if err != nil {
			return rangeMoveReplayBatch{}, err
		}
		wire, err := encodeRangeMoveReplayPage(p, q.arena)
		if err != nil {
			return rangeMoveReplayBatch{}, err
		}
		if len(wire) > remaining {
			return rangeMoveReplayBatch{}, errLimit
		}
		size += 4 + len(wire)
		out.pages = append(out.pages, p)
		cursor = p.next
		// The existing reducer reads fragments from the native view, never an
		// uncommitted sibling write. End this batch at its first partial record.
		if len(p.key) > 0 && !p.recordDone {
			break
		}
	}
	if len(out.pages) == 0 {
		return rangeMoveReplayBatch{}, errLimit
	}
	return out, nil
}
func stageMoveReplayBatch(q *reader, origin, target namespace, move [16]byte, b rangeMoveReplayBatch, before rangeMoveReplayCursor) (rangeMoveReplayCursor, error) {
	cursor := before
	if q == nil || len(b.pages) == 0 || len(b.pages) > rangeMoveSpanBatchRows {
		return cursor, errInvalid
	}
	declaration, err := rangeMoveReplayReader(q, target)
	if err != nil {
		return before, err
	}
	for i, p := range b.pages {
		if !rangeMoveReplaySame(cursor, p.before) || i < len(b.pages)-1 && len(p.key) > 0 && !p.recordDone {
			return before, errInvalid
		}
		start := len(q.writes)
		if err := stageRangeMoveReplayPageDeclared(q, origin, target, move, p, declaration); err != nil {
			return before, err
		}
		if err := coalesceMoveReplayWrites(q, start); err != nil {
			return before, err
		}
		cursor = p.next
	}
	// q is one prospective native batch: any subframe refusal publishes NONE of
	// its writes/cursor, and the one co-committed undo covers the complete batch.
	return cursor, nil
}

// The declared stage-row limit bounds this pairwise check, without a map or
// borrowed-key ownership. Native and inherited streams may describe the same
// original: coalesce ONLY byte-identical puts, keeping every work/arena/stage
// debit. No read overlay or capacity refund is introduced.
func coalesceMoveReplayWrites(q *reader, start int) error {
	if q == nil || start < 0 || start > len(q.writes) || len(q.writes) > q.limits.stageRows {
		return errInvalid
	}
	for i := start; i < len(q.writes); {
		duplicate := false
		for j := 0; j < i; j++ {
			if !bytes.Equal(q.writes[i].Key, q.writes[j].Key) {
				continue
			}
			if q.writes[i].Deleted != q.writes[j].Deleted || !bytes.Equal(q.writes[i].Value, q.writes[j].Value) {
				return errCorrupt
			}
			copy(q.writes[i:], q.writes[i+1:])
			q.writes[len(q.writes)-1] = raftlog.KV{}
			q.writes = q.writes[:len(q.writes)-1]
			duplicate = true
			break
		}
		if !duplicate {
			i++
		}
	}
	return nil
}

func (m *declaredMaterializer) checkMoveReference(q *reader, move [16]byte, p rangeMoveProgress) error {
	if !rangeMoveSpans(m.SemanticContractID()) {
		return nil
	}
	shadow := m.ns.partition == p.descriptor.Spec.To.Partition
	header, found, err := crossControl(q, rangeMoveRetentionKey(m.ns, move, shadow, 0), rangeMoveRetentionHeaderBytes)
	if err != nil || !found {
		return errors.Join(errCorrupt, err)
	}
	if sha256.Sum256(header.Value) != p.reference.header {
		return errCorrupt
	}
	packet, sums, err := decodeMoveRetentionHeader(header.Value)
	if err != nil || packet.move != move || len(sums)+1 != int(p.reference.rows) || checkMoveRetentionDescriptor(packet, p.descriptor) != nil || !bytes.Equal(p.reference.key, rangeMoveRetentionKey(namespace{packet.scope.graph, packet.scope.partition}, move, false, 0)) || !shadow && header.Index != packet.root.Index+1 {
		return errors.Join(errCorrupt, err)
	} // #nosec G115 -- rows prevalidated1..17.
	return nil
}
func retainedOnlyFeedAgreement(id raftlog.ApplicationSemanticContractID) bool {
	return id == retainedOnlyFeedSemanticContractID() || rangeMoveSpans(id)
}
func rangeMoveQueryPageLimit(spans bool) int {
	if spans {
		return rangeMoveSpanMaxPages
	}
	return 1024
}
func (p rangeMoveProgress) replayPageCount() uint64 {
	if rangeMoveSpans(p.descriptor.Source.Binding.SemanticContractID) {
		return p.replayPages
	}
	return p.replay.sequence
}
func (m *declaredMaterializer) exportMoveBatch(q *reader, p rangeMoveProgress, r rangeMoveIntent) (b rangeMoveReplayBatch, err error) {
	if err = reserveComposer(q, 2048); err != nil {
		return b, err
	}
	view, err := m.store.ApplicationView(p.descriptor.Source.Index)
	if err != nil {
		return b, err
	}
	defer func() { err = errors.Join(err, view.Close()) }()
	root, err := view.RootBounded(q.ctx, 172)
	if err != nil {
		return b, err
	}
	if root.ImageHash != p.descriptor.Source.ImageHash || !bytes.Equal(root.Image, p.descriptor.Source.Image[:]) {
		return b, errCorrupt
	}
	frozen := *q
	frozen.view, frozen.base = view, root
	frozen.rows++
	frozen.bytes += cap(root.Image) + 1024
	defer func() { q.rows, q.bytes = frozen.rows, frozen.bytes }()
	return exportMoveReplayBatch(&frozen, m.ns, p.replay, r.pageBytes, r.pageRows)
}
