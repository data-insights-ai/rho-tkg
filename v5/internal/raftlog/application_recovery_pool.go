package raftlog

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
)

// ApplicationRecoveryStepBudget is observation of one immutable native root,
// not a reservation or permission to discount the trusted materializer's work.
type ApplicationRecoveryStepBudget struct {
	Maximum ApplicationRecoveryCredit
	Pool    ApplicationRecoveryBytePool
}

// RecoveryStepBudget authenticates the exact owner and exact-next Use at this
// registered cut. Both native reads are charged before their bounded decodes.
// Older roots remain descriptive; the caller must stage against this SAME base.
func (v *ApplicationView) RecoveryStepBudget(ctx context.Context, use ApplicationRecoveryUse, budget ReadBudget) (out ApplicationRecoveryStepBudget, work ApplicationProofWork, err error) {
	defer func() {
		if err != nil {
			out = ApplicationRecoveryStepBudget{}
		}
	}()
	// Reuse native root/active/prepared equality validation, rather than adding
	// a second interpretation of snapshot recovery authority.
	owner, work, err := v.RecoveryOwner(ctx, budget)
	if err != nil {
		return out, work, err
	}
	if owner.ID != use.ID || owner.Digest != use.Digest {
		return out, work, ErrInvalid
	}
	if err = v.lock(ctx); err != nil {
		return out, work, err
	}
	defer v.unlock()
	const fixed = 1024
	if v.s.meta.Recovery.Config.Version != 4 {
		return out, work, ErrInvalid
	}
	if work.Records >= budget.Rows || fixed > budget.Bytes-work.Bytes {
		return out, work, ErrLimit
	}
	work.Records++
	work.Bytes += fixed
	key := appIndexKey(recoveryRootTag+v.bank, v.index)
	raw, closer, err := v.s.db.Get(key)
	if err != nil {
		return out, work, v.fail(storedReadFailure(err))
	}
	defer func() {
		if closeErr := closer.Close(); closeErr != nil {
			err = errors.Join(err, v.fail(closeErr))
		}
	}()
	policy := v.s.applicationPolicy
	maximum := min(policy.MaxValueBytes, maxMetadataBytes)
	if len(raw) < appFrameBytes || len(raw)-appFrameBytes > maximum {
		return out, work, v.fail(ErrCorrupt)
	}
	// The fixed grant funds lookup/length inspection; actual delivered length
	// funds frame hashing, the same native decoder and owned spans/terminal bank
	// BEFORE any of those operations. The hard64KiB bound checks multiplication.
	extra := 8 * (len(raw) - appFrameBytes)
	if extra > budget.Bytes-work.Bytes {
		return out, work, ErrLimit
	}
	work.Bytes += extra
	if err := ctx.Err(); err != nil {
		return out, work, err
	}
	data, deleted, err := inspectAppFrame(key, raw, maximum)
	if err != nil || deleted {
		return out, work, v.fail(ErrCorrupt)
	}
	r, _, err := readNativeRecoveryRoot(data, policy, v.s.limits)
	if err != nil {
		return out, work, v.fail(err)
	}
	if data[2] != byte(v.s.meta.Recovery.Config.Version) {
		return out, work, v.fail(ErrCorrupt)
	}
	m := applicationRecoveryMetadata{Config: ApplicationRecoveryConfig{Version: 4}, Reservation: r}
	out.Maximum, err = m.credit(use)
	if err != nil {
		return ApplicationRecoveryStepBudget{}, work, err
	}
	for _, span := range r.Spans {
		if !span.Exhausted && span.Next == use.Phase {
			out.Pool = span.BytePool
			return out, work, ctx.Err()
		}
	}
	return ApplicationRecoveryStepBudget{}, work, ErrInvalid
}

// PooledRecoveryBatchBound measures a complete prospective application batch
// using native framing, then adds the maximum RS4+TB1 envelope. Padding may only
// INCREASE a bound (e.g. a fixed future progress envelope); it never discounts
// actual writes, outcomes, controls, record count or native installation work.
// This pure helper neither reserves nor calls a materializer under Store.mu.
func (p ApplicationPolicy) PooledRecoveryBatchBound(b ApplicationBatch, padding uint64) (retained, records, work uint64, err error) {
	retained, records, err = p.measure(b)
	if err != nil {
		return 0, 0, 0, err
	}
	baseControl, _, controlWork, err := controlGrowth(p, b)
	if err != nil {
		return 0, 0, 0, err
	}
	// This is the same owned/native work charged by measure, including control
	// copies, plus a maximum native root and any declared future-value padding.
	work = 2*(retained-baseControl) + uint64(len(b.Image)+1024) + 64*uint64(len(b.Writes)) + controlWork
	const root = recoveryRootFixedBytes + recoveryPoolSpanBytes*maxApplicationRecoverySpans + 8 + maxApplicationTerminalOwners*(4+recoveryRootFixedBytes+recoveryCreditBytes*maxApplicationTerminalAlternatives)
	if root > p.MaxValueBytes {
		return 0, 0, 0, ErrLimit
	}
	added, err := checkedRecoveryAdd(uint64(9+appFrameBytes+root), padding)
	if err != nil {
		return 0, 0, 0, err
	}
	retained, err = checkedRecoveryAdd(retained, added)
	if err != nil {
		return 0, 0, 0, err
	}
	twice, err := checkedRecoveryMul(2, added)
	if err != nil {
		return 0, 0, 0, err
	}
	work, err = checkedRecoveryAdd(work, twice)
	if err != nil || work > unsignedLimit(p.MaxInstallBytes) {
		return 0, 0, 0, errors.Join(ErrLimit, err)
	}
	return retained, records + 1, work, nil
}

// ApplicationRecoveryBytePool is finite protected backing, not graph authority.
// The selected trusted materializer must bound the SUM of every remaining
// installation and command, including the complete independent undo schedule.
// Native installation checks actual growth; maxima alone do not prove the sum.
// Neither native log reclamation nor a retry refills these monotone totals.
type ApplicationRecoveryBytePool struct {
	RetainedLimit, RetainedRemaining uint64
	LogLimit, LogRemaining           uint64
}

const recoveryPoolSpanBytes = 72

func (p ApplicationRecoveryBytePool) enabled() bool { return p != (ApplicationRecoveryBytePool{}) }
func hasRecoveryPools(r ApplicationRecoveryReservation) bool {
	for _, s := range r.Spans {
		if s.BytePool.enabled() || s.Exhausted {
			return true
		}
	}
	return false
}

func validateRecoveryPool(s ApplicationRecoverySpan) error {
	p := s.BytePool
	if !p.enabled() {
		if s.Exhausted {
			return ErrInvalid
		}
		return nil
	}
	if p.RetainedLimit == 0 || p.LogLimit == 0 || p.RetainedRemaining > p.RetainedLimit || p.LogRemaining > p.LogLimit || s.Exhausted && s.Next != s.Last {
		return ErrInvalid
	}
	count := uint64(s.Last) - uint64(s.First) + 1
	retained, err := checkedRecoveryMul(count, s.RetainedBytes)
	if err != nil {
		return err
	}
	log, err := checkedRecoveryMul(count, uint64(s.CommandBytes+frameOverhead+9)) // #nosec G115 -- caller validates command0..16MiB before pool validation.
	if err != nil {
		return err
	}
	if p.RetainedLimit > retained || p.LogLimit > log {
		return ErrLimit
	}
	return nil
}

// Pool queue projection advances exact phases but NEVER speculatively spends
// retained bytes: independent pending backing covers each queued installation.
// Otherwise maxima80/80 would strand actual10/70 inside a legitimate100 pool.
// Actual log charge is known from the persisted native entry, not a maximum.
func (m applicationRecoveryMetadata) consumePool(u ApplicationRecoveryUse, credit ApplicationRecoveryCredit, retained, log uint64, installed bool) (applicationRecoveryMetadata, ApplicationRecoveryCredit, error) {
	r := m.Reservation
	r.Spans = slices.Clone(r.Spans)
	for i := range r.Spans {
		s := &r.Spans[i]
		if s.Exhausted || s.Next != u.Phase {
			continue
		}
		if !s.BytePool.enabled() {
			return m.consumeSpan(u, credit)
		}
		if retained > credit.RetainedBytes || log > uint64(credit.CommandBytes+frameOverhead+9) {
			return m, ApplicationRecoveryCredit{}, ErrLimit
		} // #nosec G115 -- concrete command bound checked by credit admission.
		if retained > s.BytePool.RetainedRemaining || log > s.BytePool.LogRemaining {
			return m, ApplicationRecoveryCredit{}, ErrLimit
		}
		if installed {
			s.BytePool.RetainedRemaining -= retained
		}
		s.BytePool.LogRemaining -= log
		if s.Next == s.Last {
			s.Exhausted = true
		} else {
			s.Next++
		}
		if installed && u.RetireBefore != 0 {
			// Only credit()'s actual authenticated terminal singleton permits
			// release. Even an exhausted ordinary band keeps all surplus until now.
			r.Spans = slices.DeleteFunc(r.Spans, func(v ApplicationRecoverySpan) bool { return v.Last <= u.RetireBefore })
		}
		if len(r.Spans) == 0 {
			r = ApplicationRecoveryReservation{}
		}
		m.Reservation = r
		return m, credit, nil
	}
	return m, ApplicationRecoveryCredit{}, ErrInvalid
}

func recoveryPoolRootBytes(r ApplicationRecoveryReservation) int {
	return recoveryRootFixedBytes + recoveryPoolSpanBytes*len(r.Spans)
}

func recoverySpanWireBytes(config uint32) int {
	if config == 4 {
		return recoveryPoolSpanBytes
	}
	return recoverySpanBytes
}
func recoveryReservationWireBytes(config uint32, r ApplicationRecoveryReservation) int {
	if config == 4 {
		return recoveryPoolRootBytes(r)
	}
	return recoveryReservationBytes(r)
}

// A pooled sum cannot fund hypothetical maxima for several committed entries
// at once. Keep one immutable native tail window in addition to the pool.
// Only distinct live pooled Uses transfer their pending backing into it:
// ordinary entries and cached replays do not reduce the pool when installed.
func recoveryPoolPendingWindow(r ApplicationRecoveryReservation, p ApplicationPolicy, eligible uint64) (uint64, error) {
	if !hasRecoveryPools(r) {
		return 0, nil
	}
	if p.MaxInstallBytes < 1 || p.MaxTailEntries < 2 {
		return 0, ErrInvalid
	}
	window, err := checkedRecoveryMul(p.MaxTailEntries, unsignedLimit(p.MaxInstallBytes))
	if err != nil {
		return 0, err
	}
	if eligible > window {
		return 0, ErrLimit
	}
	return window - eligible, nil
}
func addRecoveryPoolPendingWindow(g applicationRecoveryGrowth, r ApplicationRecoveryReservation, p ApplicationPolicy, eligible uint64) (applicationRecoveryGrowth, error) {
	w, err := recoveryPoolPendingWindow(r, p, eligible)
	if err != nil {
		return applicationRecoveryGrowth{}, err
	}
	g.bytes, err = checkedRecoveryAdd(g.bytes, w)
	return g, err
}
func liveRecoveryPoolUse(m applicationRecoveryMetadata, u *ApplicationRecoveryUse) bool {
	if m.Config.Version != 4 || u == nil || m.Reservation.ID != u.ID || m.Reservation.Digest != u.Digest {
		return false
	}
	for _, s := range m.Reservation.Spans {
		if s.BytePool.enabled() && !s.Exhausted && s.Next == u.Phase {
			return true
		}
	}
	return false
}

// A fixed-width root lets Install determine the exact prospective root length
// before measuring its actual retained bytes, then debit and encode once. No
// hypothetical maximum is first spent and later refunded.
func (m applicationRecoveryMetadata) consumeMeasured(u ApplicationRecoveryUse, retained, log uint64) (applicationRecoveryMetadata, ApplicationRecoveryCredit, error) {
	c, err := m.credit(u)
	if err != nil {
		return m, c, err
	}
	if m.Config.Version == 4 {
		return m.consumePool(u, c, retained, log, true)
	}
	return m.consume(u)
}

func appendRecoveryPoolRoot(r ApplicationRecoveryReservation) []byte {
	b := make([]byte, 0, recoveryRootFixedBytes+recoveryPoolSpanBytes*len(r.Spans))
	b = append(b, 'R', 'S', 4, 0)
	b = append(b, r.ID[:]...)
	b = append(b, r.Digest[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(r.Spans))) // #nosec G115 -- <=8 admitted spans.
	for _, s := range r.Spans {
		for _, n := range []uint32{s.First, s.Next, s.Last, uint32(s.CommandBytes)} {
			b = binary.BigEndian.AppendUint32(b, n)
		} // #nosec G115 -- concrete0..16MiB command bound.
		for _, n := range []uint64{s.RetainedBytes, s.RetainedRecords} {
			b = binary.BigEndian.AppendUint64(b, n)
		}
		flags := uint64(0)
		if s.BytePool.enabled() {
			flags |= 1
		}
		if s.Exhausted {
			flags |= 2
		}
		for _, n := range []uint64{flags, s.BytePool.RetainedLimit, s.BytePool.RetainedRemaining, s.BytePool.LogLimit, s.BytePool.LogRemaining} {
			b = binary.BigEndian.AppendUint64(b, n)
		}
	}
	return b
}

func readRecoveryPoolRoot(b []byte, p ApplicationPolicy, l Limits) (ApplicationRecoveryReservation, int, error) {
	var r ApplicationRecoveryReservation
	if len(b) < recoveryRootFixedBytes || len(b) > maxMetadataBytes || string(b[:4]) != "RS\x04\x00" {
		return r, 0, ErrCorrupt
	}
	copy(r.ID[:], b[4:36])
	copy(r.Digest[:], b[36:68])
	n := binary.BigEndian.Uint32(b[68:72])
	if n > maxApplicationRecoverySpans || uint64(n)*recoveryPoolSpanBytes > uint64(len(b)-recoveryRootFixedBytes) {
		return r, 0, ErrCorrupt
	}
	if n > 0 {
		r.Spans = make([]ApplicationRecoverySpan, int(n))
	} // #nosec G115 -- delivered bytes and <=8 checked before allocation.
	for i := range r.Spans {
		v := b[72+i*recoveryPoolSpanBytes : 72+(i+1)*recoveryPoolSpanBytes]
		command := binary.BigEndian.Uint32(v[12:16])
		if uint64(command) > unsignedLimit(l.MaxEntryBytes-frameOverhead) {
			return ApplicationRecoveryReservation{}, 0, ErrCorrupt
		}
		s := ApplicationRecoverySpan{First: binary.BigEndian.Uint32(v[:4]), Next: binary.BigEndian.Uint32(v[4:8]), Last: binary.BigEndian.Uint32(v[8:12]), CommandBytes: int(command), RetainedBytes: binary.BigEndian.Uint64(v[16:24]), RetainedRecords: binary.BigEndian.Uint64(v[24:32])} // #nosec G115 -- command checked0..16MiB.
		flags := binary.BigEndian.Uint64(v[32:40])
		if flags & ^uint64(3) != 0 {
			return ApplicationRecoveryReservation{}, 0, ErrCorrupt
		}
		s.BytePool = ApplicationRecoveryBytePool{binary.BigEndian.Uint64(v[40:48]), binary.BigEndian.Uint64(v[48:56]), binary.BigEndian.Uint64(v[56:64]), binary.BigEndian.Uint64(v[64:72])}
		s.Exhausted = flags&2 != 0
		if (flags&1 != 0) != s.BytePool.enabled() {
			return ApplicationRecoveryReservation{}, 0, ErrCorrupt
		}
		r.Spans[i] = s
	}
	if _, err := recoveryGrowthIfPresent(r, p, l); err != nil {
		return ApplicationRecoveryReservation{}, 0, ErrCorrupt
	}
	return r, recoveryRootFixedBytes + int(n)*recoveryPoolSpanBytes, nil // #nosec G115 -- n<=8.
}
