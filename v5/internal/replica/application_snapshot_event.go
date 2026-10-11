package replica

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	pb "go.etcd.io/raft/v3/raftpb"
)

const applicationPendingFixed uint64 = 4096
const applicationAuditCandidateBytes uint64 = 4096
const applicationCleanupWork uint64 = 1024
const applicationProtoObjectBytes uint64 = 1024
const applicationProtoEntryBytes uint64 = 256

const (
	applicationAuditScanning byte = iota
	applicationAuditFinishing
	applicationAuditFinished
)

// PendingApplicationSnapshot owns one exact Driver event. Driver.mu protects
// mutable fields; self/driver remain immutable. No consensus object escapes.
type PendingApplicationSnapshot struct {
	self                   *PendingApplicationSnapshot
	driver                 *Driver
	event                  *applicationEvent
	prepared               *raftlog.PreparedApplicationSnapshot
	message                *pb.Message
	context                ApplicationSnapshotContext
	audit                  ApplicationSnapshotAudit
	owned, auditOwned      uint64
	phase                  byte
	busy, canceled, closed bool
	rawStarted             bool
	opDone, done           chan struct{}
	closeErr               error
}

func applicationAuditAdmit(b ApplicationAuditBudget, work, owned uint64) error {
	if b.WorkBytes == 0 || b.OwnedBytes == 0 {
		return ErrInvalid
	}
	if work > b.WorkBytes || owned > b.OwnedBytes {
		return ErrLimit
	}
	return nil
}

// Before decode, bound every expansion accepted by the current wire preflight.
// Fixed object widths include protobuf scalar targets; sizeof tests pin them.
func applicationMessageMaximum(l raftlog.Limits) uint64 {
	entries := uint64(l.MaxReadEntries) // #nosec G115 -- Config validates positive entry count<=4096.
	data := uint64(l.MaxReadBytes)      // #nosec G115 -- Config validates positive read bytes<=32MiB.
	// Fresh consumeBytes uses append(emptyBuf[0:],v...). Visible capacity may
	// round up by one Go page8192 per Entry.Data/Context/Snapshot.Data.
	// Single-entry pointer append has nominal cap<=2q, then roundup<=8192;
	// its old+new backing peak<=24q+8192. ConfState includes mixed packed
	// make/copy and unpacked append, funded independently for all four lists.
	return 4*applicationProtoObjectBytes + 24*entries + 8192 + entries*applicationProtoEntryBytes + data + 8192*entries + 1024 + 8192 + 512 + 8192 + 4*(24*256+8192)
}

// Capacity initialization/copy credit, not heap/profiling. Pointer append's
// geometric sum<=9 finalbackings; mixed packed/unpacked uint64 can allocate
// each of256 steps, so sum(24q+8192), not a guessed final-cap multiplier.
func applicationDecodeCapacityWork(l raftlog.Limits) uint64 {
	entries := uint64(l.MaxReadEntries) // #nosec G115 -- immutable Config bound1..4096.
	return 8192*entries + 2*8192 + 9*(16*entries+8192) + 4*(24*(256*257/2)+8192*256)
}

func applicationMessageOwned(m *pb.Message) uint64 {
	n := 4*applicationProtoObjectBytes + uint64(cap(m.Entries))*8 + uint64(len(m.Entries))*applicationProtoEntryBytes // #nosec G115 -- nonnegative caps/counts bounded by checked wire preflight.
	n += uint64(cap(m.Context) + cap(m.GetSnapshot().GetData()))                                                      // #nosec G115 -- nonnegative owned caps from bounded protobuf decoding.
	for _, e := range m.Entries {
		n += uint64(cap(e.GetData()))
	} // #nosec G115 -- nonnegative caps bounded by MaxReadBytes before decode.
	cs := m.GetSnapshot().GetMetadata().GetConfState()
	n += uint64(cap(cs.GetVoters())+cap(cs.GetLearners())+cap(cs.GetVotersOutgoing())+cap(cs.GetLearnersNext())) * 8 // #nosec G115 -- each preflight-bounded list contains<=256 elements; growth bound checked against prior maximum.
	return n
}
func (d *Driver) applicationInputAdmission(packet Packet, b ApplicationAuditBudget) error {
	if len(packet.Payload) > d.config.MaxPacketBytes {
		return ErrLimit
	}
	if len(packet.Payload) < applicationPacketHeaderBytes {
		return ErrInvalid
	}
	n := uint64(len(packet.Payload) - applicationPacketHeaderBytes) // #nosec G115 -- preceding header bound makes subtraction nonnegative.
	borrowed := uint64(cap(packet.Payload))                         // #nosec G115 -- slice capacity is nonnegative.
	// RP copy/backing2N; recursive preflight at most4 ancestry walks;
	// application outer/entry2N; protobuf parse plus owned backing/copy2N.
	cleanup, err := applicationProduct(2, d.callbackLimits.CleanupWork)
	if err != nil {
		return err
	}
	work, err := applicationSum(applicationPendingFixed, 80, 10*n, 4*applicationProtoObjectBytes, uint64(d.config.Limits.MaxReadEntries)*applicationProtoEntryBytes, 4*256*8, 512, applicationDecodeCapacityWork(d.config.Limits), 2*applicationCleanupWork, cleanup) // #nosec G115 -- Config validates positive finite read-entry count.
	if err != nil {
		return err
	}
	owned := applicationPendingFixed + borrowed + n + applicationMessageMaximum(d.config.Limits)
	return applicationAuditAdmit(b, work, owned)
}

// Called only after CheckPreparedApplicationSnapshot authenticated the full
// actual descriptor/membership. It extracts fixed data, not a manifest decoder.
func applicationSnapshotContext(wire []byte, md *pb.SnapshotMetadata) (ApplicationSnapshotContext, error) {
	if len(wire) < 4 || len(wire) > 512 || !bytes.Equal(wire[:2], []byte("AD")) || wire[3] != 0 || md == nil || md.GetIndex() == 0 || md.GetTerm() == 0 {
		return ApplicationSnapshotContext{}, ErrInvalid
	}
	cut := 196
	switch wire[2] {
	case 1:
	case 2:
		cut += 32
	case 3:
		cut += 48
	default:
		return ApplicationSnapshotContext{}, ErrInvalid
	}
	if len(wire) < cut+68 || binary.BigEndian.Uint32(wire[cut+64:cut+68]) > 64 || uint64(binary.BigEndian.Uint32(wire[cut+64:cut+68])) != uint64(len(wire)-cut-68) {
		return ApplicationSnapshotContext{}, ErrInvalid
	} // #nosec G115 -- prior complete fixed-prefix bound guarantees nonnegative tail.
	cs := md.GetConfState()
	if cs == nil || len(md.ProtoReflect().GetUnknown()) != 0 || len(cs.ProtoReflect().GetUnknown()) != 0 || len(cs.Voters) != 3 || len(cs.Learners) != 0 || len(cs.LearnersNext) != 0 || len(cs.VotersOutgoing) != 0 || cs.GetAutoLeave() || cs.Voters[0] == 0 || cs.Voters[0] >= cs.Voters[1] || cs.Voters[1] >= cs.Voters[2] {
		return ApplicationSnapshotContext{}, ErrInvalid
	}
	out := ApplicationSnapshotContext{Index: md.GetIndex(), Term: md.GetTerm(), DescriptorHash: sha256.Sum256(wire)}
	copy(out.Voters[:], cs.Voters)
	copy(out.CutID[:], wire[cut:cut+32])
	copy(out.ManifestID[:], wire[cut+32:cut+64])
	return out, nil
}

// BeginApplicationSnapshot retains the actual decoded message and fences RawNode while bounded
// semantic work is performed outside Driver.mu. It creates no claim or Ready.
func (d *Driver) BeginApplicationSnapshot(ctx context.Context, packet Packet, prepared *raftlog.PreparedApplicationSnapshot, b ApplicationAuditBudget) (*PendingApplicationSnapshot, error) {
	if d == nil || ctx == nil {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	owner, err := d.beginPacketEvent()
	if err != nil {
		_, err = d.failEventAdmission(err)
		return nil, err
	}
	fail := func(err error) (*PendingApplicationSnapshot, error) {
		_, err = d.finishEvent(owner, Output{}, err)
		return nil, err
	}
	provider, ok := d.applicationMachine.(ApplicationSnapshotAuditor)
	if !ok || isNilMachine(provider) || !d.boundApplication() {
		return fail(ErrInvalid)
	}
	if err := d.applicationInputAdmission(packet, b); err != nil {
		return fail(err)
	}
	message, err := d.decodeBoundPacket(packet, true)
	if err != nil {
		return fail(err)
	}
	if err := d.store.CheckPreparedApplicationSnapshot(prepared, message.GetSnapshot()); err != nil {
		return fail(err)
	}
	c, err := applicationSnapshotContext(message.GetSnapshot().Data, message.GetSnapshot().GetMetadata())
	if err != nil {
		return fail(err)
	}
	borrowed := uint64(cap(packet.Payload)) // #nosec G115 -- nonnegative cap already admitted before decoding.
	held := applicationPendingFixed + applicationMessageOwned(message) + borrowed
	if held > applicationPendingFixed+applicationMessageMaximum(d.config.Limits)+borrowed {
		return fail(ErrLimit)
	}
	cleanup, err := applicationProduct(2, d.callbackLimits.CleanupWork)
	if err != nil {
		return fail(err)
	}
	work, err := applicationSum(applicationPendingFixed, 80, 10*uint64(len(packet.Payload)-80), 4*applicationProtoObjectBytes, uint64(d.config.Limits.MaxReadEntries)*applicationProtoEntryBytes, 4*256*8, 512, applicationDecodeCapacityWork(d.config.Limits), 2*applicationCleanupWork, cleanup) // #nosec G115 -- header and immutable Config count bounds checked before this private operation.
	if err != nil {
		return fail(err)
	}
	owned, err := applicationSum(held, applicationAuditCandidateBytes, d.callbackLimits.CleanupOwned)
	if err != nil {
		return fail(err)
	}
	if err := applicationAuditAdmit(b, work, owned); err != nil {
		return fail(err)
	}
	if b.WorkBytes == work {
		return fail(ErrLimit)
	}
	child := ApplicationAuditBudget{WorkBytes: b.WorkBytes - work, OwnedBytes: b.OwnedBytes - held - d.callbackLimits.CleanupOwned}
	if child.WorkBytes == 0 {
		return fail(ErrLimit)
	}
	p := &PendingApplicationSnapshot{driver: d, event: owner, prepared: prepared, message: message, context: c, owned: held, auditOwned: child.OwnedBytes, busy: true, opDone: make(chan struct{}), done: make(chan struct{})}
	p.self = p
	owner.pending = p
	owner.rawPhase = false
	d.mu.Unlock()
	audit, err := provider.BeginApplicationSnapshotAudit(prepared, c, child)
	d.mu.Lock()
	p.audit = audit
	p.busy = false
	close(p.opDone)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && isNilMachine(audit) {
		err = ErrInvalid
	}
	if err != nil || p.canceled || d.closeOwner != nil {
		if err == nil {
			err = ErrStopped
		}
		_, err = p.finishLocked(err)
		return nil, err
	}
	if d.event != owner {
		return fail(ErrInvalid)
	}
	packet = Packet{} // The constructor caller span is no longer used.
	p.owned -= borrowed
	d.mu.Unlock()
	return p, nil
}

func (p *PendingApplicationSnapshot) valid() bool {
	return p != nil && p.self == p && p.driver != nil && p.event != nil
}

// Advance performs one bounded audit phase; Finish follows EOF on a later call.
// It returns true only after Finish succeeds; Commit performs no audit work.
func (p *PendingApplicationSnapshot) Advance(ctx context.Context, b ApplicationAuditBudget) (bool, error) {
	if !p.valid() || ctx == nil {
		return false, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	d := p.driver
	d.mu.Lock()
	if p.event.pending != p {
		d.mu.Unlock()
		return false, ErrInvalid
	}
	if p.closed {
		d.mu.Unlock()
		return false, raftlog.ErrClosed
	}
	if d.event != p.event {
		d.mu.Unlock()
		return false, ErrInvalid
	}
	if p.busy || d.closeOwner != nil {
		d.mu.Unlock()
		return false, ErrUnavailable
	}
	cleanup, err := applicationProduct(2, d.callbackLimits.CleanupWork)
	if err != nil {
		d.mu.Unlock()
		return false, err
	}
	work, err := applicationSum(applicationPendingFixed, 2*applicationCleanupWork, cleanup)
	if err != nil {
		d.mu.Unlock()
		return false, err
	}
	owned, err := applicationSum(p.owned, p.auditOwned, d.callbackLimits.CleanupOwned)
	if err != nil {
		d.mu.Unlock()
		return false, err
	}
	if err := applicationAuditAdmit(b, work, owned); err != nil {
		d.mu.Unlock()
		return false, err
	}
	if p.phase == applicationAuditFinished {
		d.mu.Unlock()
		return true, nil
	}
	if b.WorkBytes == work {
		d.mu.Unlock()
		return false, ErrLimit
	}
	child := ApplicationAuditBudget{WorkBytes: b.WorkBytes - work, OwnedBytes: min(p.auditOwned, b.OwnedBytes-p.owned-d.callbackLimits.CleanupOwned)}
	p.busy = true
	p.opDone = make(chan struct{})
	audit, phase := p.audit, p.phase
	d.mu.Unlock()
	var done bool
	if phase == applicationAuditScanning {
		done, err = audit.Advance(ctx, child)
	} else {
		err = audit.Finish(ctx, child)
	}
	d.mu.Lock()
	p.busy = false
	close(p.opDone)
	if p.canceled || d.closeOwner != nil {
		_, err = p.finishLocked(errors.Join(ErrStopped, err))
		return false, err
	}
	if err != nil {
		if errors.Is(err, ErrLimit) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			d.mu.Unlock()
			return false, err
		}
		_, err = p.finishLocked(err)
		return false, err
	}
	if phase == applicationAuditScanning {
		if done {
			p.phase = applicationAuditFinishing
		}
		d.mu.Unlock()
		return false, nil
	}
	p.phase = applicationAuditFinished
	p.auditOwned = applicationAuditCandidateBytes
	d.mu.Unlock()
	return true, nil
}

// Commit walks no audit. The actual Snapshot remains private and Claim repeats
// exact matching after Finish released all readers. RawNode alone decides Ready.
func (p *PendingApplicationSnapshot) Commit(ctx context.Context, b ApplicationAuditBudget) (Output, error) {
	if !p.valid() || ctx == nil {
		return Output{}, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return Output{}, err
	}
	d := p.driver
	d.mu.Lock()
	if p.event.pending != p {
		d.mu.Unlock()
		return Output{}, ErrInvalid
	}
	if p.closed {
		d.mu.Unlock()
		return Output{}, raftlog.ErrClosed
	}
	if d.event != p.event {
		d.mu.Unlock()
		return Output{}, ErrInvalid
	}
	if p.busy || p.phase != applicationAuditFinished || d.closeOwner != nil {
		d.mu.Unlock()
		return Output{}, ErrUnavailable
	}
	if err := applicationAuditAdmit(b, applicationPendingFixed+2*applicationCleanupWork+1024, p.owned+p.auditOwned); err != nil {
		d.mu.Unlock()
		return Output{}, err
	}
	contract, err := p.prepared.ApplicationContract(ctx)
	if err != nil {
		return p.finishLocked(err)
	}
	work, peak, err := d.applicationCommitRequirements(p.owned, p.auditOwned, contract)
	if err != nil {
		d.mu.Unlock()
		return Output{}, err
	}
	if err := applicationAuditAdmit(b, work, peak); err != nil {
		d.mu.Unlock()
		return Output{}, err
	}
	if err := d.store.CheckPreparedApplicationSnapshot(p.prepared, p.message.GetSnapshot()); err != nil {
		return p.finishLocked(err)
	}
	claim, err := d.store.ClaimPreparedApplicationSnapshot(p.prepared, p.message.GetSnapshot())
	if err != nil {
		return p.finishLocked(err)
	}
	p.event.claim = claim
	p.event.rawPhase = true
	p.busy = true
	p.opDone = make(chan struct{})
	if err := d.validateBoundMessage(p.message, true); err != nil {
		p.busy = false
		close(p.opDone)
		return p.finishLocked(err)
	}
	p.rawStarted = true
	if err := d.raw.Step(p.message); err != nil {
		p.busy = false
		close(p.opDone)
		return p.finishLocked(err)
	}
	out, err := d.drain()
	p.busy = false
	close(p.opDone)
	return p.finishOutputLocked(out, err)
}

func (p *PendingApplicationSnapshot) finishLocked(err error) (Output, error) {
	return p.finishOutputLocked(Output{}, err)
}

// Called with Driver.mu and no pending operation in flight. The event remains
// installed through auditor/claim/sender cleanup; another Close waits on done.
func (p *PendingApplicationSnapshot) finishOutputLocked(out Output, opErr error) (Output, error) {
	d := p.driver
	if p.canceled && opErr == nil {
		opErr = raftlog.ErrClosed
		out = Output{}
		if p.rawStarted {
			_, _ = d.stop(errors.Join(d.stopped, raftlog.ErrClosed))
		}
	}
	p.closed = true
	p.event.rawPhase = false
	audit := p.audit
	p.audit = nil
	// Retain exact event→owner identity for repeated closed-handle operations.
	// Driver drops its active event after cleanup; there is no resident registry.
	d.mu.Unlock()
	var cleanupErr error
	if !isNilMachine(audit) {
		cleanupErr = audit.Close()
	}
	d.mu.Lock()
	if cleanupErr != nil {
		d.applicationCleanupErr = errors.Join(d.applicationCleanupErr, cleanupErr)
		_, _ = d.stop(errors.Join(d.stopped, cleanupErr))
	}
	p.message = nil
	p.prepared = nil
	p.owned, p.auditOwned = 0, 0
	out, err := d.finishEvent(p.event, out, errors.Join(opErr, cleanupErr))
	d.mu.Lock()
	p.closeErr = errors.Join(cleanupErr, d.applicationCleanupErr, d.claimCleanupErr)
	close(p.done)
	d.mu.Unlock()
	return out, err
}

// Close actively cancels idle events; it requires no future Advance. A live
// bounded operation is waited for outside Driver.mu, then one owner closes it.
func (p *PendingApplicationSnapshot) Close() error {
	if !p.valid() {
		return ErrInvalid
	}
	d := p.driver
	for {
		d.mu.Lock()
		if p.event.pending != p {
			d.mu.Unlock()
			return ErrInvalid
		}
		if p.closed {
			done := p.done
			d.mu.Unlock()
			<-done
			return p.closeErr
		}
		if d.event != p.event {
			d.mu.Unlock()
			return ErrInvalid
		}
		p.canceled = true
		if p.busy {
			done := p.opDone
			d.mu.Unlock()
			<-done
			continue
		}
		_, _ = p.finishLocked(nil)
		return p.closeErr
	}
}

// Commit funds configured framework quotas PLUS the compiled callback profile
// before Claim/RawStep. All bounds are per event, never a total audit/journal cap.
func (d *Driver) applicationCommitRequirements(held, candidate uint64, c raftlog.ApplicationContract) (uint64, uint64, error) {
	l, policy, cb := d.config.Limits, d.store.ApplicationLimits(), d.callbackLimits
	readyCount, err := applicationSum(policy.MaxTailEntries, 2)
	if err != nil {
		return 0, 0, err
	}
	readyWork, err := applicationSum(4*uint64(l.MaxReadyBytes), 8*uint64(c.MaxImageBytes), 9*65536, applicationProtoEntryBytes*uint64(l.MaxReadEntries), 2*uint64(l.MaxEntryBytes), 1024) // #nosec G115 -- concrete Config/contract positive finite fields validated before capture.
	if err != nil {
		return 0, 0, err
	}
	readyWork, err = applicationProduct(readyCount, readyWork)
	if err != nil {
		return 0, 0, err
	}
	// Store shape/key/frame emission, absence and direct Set plus geometric
	// batch-grow copies are jointly bounded by6*Install (component table).
	// Metadata is separate;4*Entry funds install reads and reclaim lookups.
	cleanupWork, err := applicationProduct(2, cb.CleanupWork)
	if err != nil {
		return 0, 0, err
	}
	tailWork, err := applicationSum(cb.StageWork, cb.RestoreWork, cleanupWork, 6*uint64(policy.MaxInstallBytes), 4*uint64(l.MaxEntryBytes), 2*uint64(c.MaxImageBytes), 3*9*65536) // #nosec G115 -- actual immutable policy/Config fields validated finite positive.
	if err != nil {
		return 0, 0, err
	}
	tailWork, err = applicationProduct(policy.MaxTailEntries, tailWork)
	if err != nil {
		return 0, 0, err
	}
	outputSlots := uint64(d.config.MaxOutputBytes)/applicationPacketHeaderBytes + 1                                                                                                                                                                                // #nosec G115 -- Open validates positive MaxOutputBytes<=256MiB.
	outputWork := 256*outputSlots + 32*128 + uint64(d.config.ApplicationSnapshotSends.MaxOffers)*16                                                                                                                                                                // #nosec G115 -- validated finite offers0..16; fixed public output slots.
	work, err := applicationSum(applicationPendingFixed, 2*applicationCleanupWork, 1024+512+128, readyWork, tailWork, outputWork, cb.RestoreWork, cb.CleanupWork, 4*uint64(d.config.MaxOutputBytes), 2*uint64(d.config.MaxPacketBytes), 2*uint64(l.MaxEntryBytes)) // #nosec G115 -- Open bounds positive packet/output/entry fields before immutable capture.
	if err != nil {
		return 0, 0, err
	}
	// These coexist: actual retained message/candidate, claim+returned image,
	// Ready/read buffers and typed entries, Entry clone, Stage batch, Restore
	// image clone/module state, detached cleanup, outgoing output and sender.
	outputBacking := 192*outputSlots + 32*96 + uint64(d.config.ApplicationSnapshotSends.MaxOffers)*16                                                                                                                                                                                                                                                                                                                     // #nosec G115 -- same finite output/offer bounds; old+new backing admitted.
	owned, err := applicationSum(held, candidate, outputBacking, d.applicationStoreStagingMaximum(c), 2048, 3*uint64(c.MaxImageBytes), 2*uint64(l.MaxReadyBytes), 2*uint64(l.MaxReadBytes), 2*applicationProtoEntryBytes*uint64(l.MaxReadEntries), uint64(l.MaxEntryBytes), cb.StageOwned, cb.RestoreOwned, cb.CleanupOwned, uint64(d.config.MaxOutputBytes), d.config.ApplicationSnapshotSends.MaxOwnedBytes, 3*9*65536) // #nosec G115 -- Config/actual contract validated finite positive; callback fields checked by checked sum.
	if err != nil {
		return 0, 0, err
	}
	return work, owned, nil
}

// All fields here come from the actual immutable validated Store/contract and
// Config. Each is<=64MiB; writes/entries<=4096, key<=64KiB. Every intermediate
// below is<1GiB (proved in the component table), so uint64 arithmetic cannot
// overflow. The final combined callback/global allowance uses applicationSum.
// B is a batch representation bound incl11-byte record overhead/header/metadata.
// Pebble rawalloc has exact visible cap; pooled baseline is1MiB, and geometric
// growth can simultaneously retain old+new<3B+64. Sequential doors use MAX.
func (d *Driver) applicationStoreStagingMaximum(c raftlog.ApplicationContract) uint64 {
	p, l := d.store.ApplicationLimits(), d.config.Limits
	image := uint64(c.MaxImageBytes)                                                           // #nosec G115 -- actual owner contract validates1..64MiB.
	entry := uint64(l.MaxEntryBytes)                                                           // #nosec G115 -- immutable Config validates74..16MiB.
	install := uint64(p.MaxInstallBytes) + image + 11*uint64(p.MaxInstallWrites+5) + 2048 + 32 // #nosec G115 -- validated policy<=64MiB and writes<=4096.
	activation := 2*image + uint64(l.MaxReadyBytes) + 11*(4096+6) + 2048 + 128                 // #nosec G115 -- validated Ready bound<=64MiB.
	persist := uint64(l.MaxReadyBytes) + 11*(4096+4) + 2048 + 128                              // #nosec G115 -- same immutable Ready ceiling.
	reclaim := image + 11*6 + 2048 + 128
	// The module Stage DTO is retained separately. Store-owned AV1/key scratch,
	// log frame/decoded-entry copies and reclaimed image are additional owners.
	frame := uint64(max(p.MaxValueBytes, p.MaxImageBytes, p.MaxChangeBytes, p.MaxOutcomeBytes)) + 36                                    // #nosec G115 -- all positive validated fields<=64MiB.
	key := 6*uint64(p.MaxKeyBytes) + 8192 + 64                                                                                          // #nosec G115 -- logical key<=64KiB; pinned Go append rounding law.
	temp := [4]uint64{frame + key, 3 * entry, 3 * entry, 2*image + uint64(max(p.MaxChangeBytes, p.MaxOutcomeBytes)) + 2*8192 + 5*entry} // #nosec G115 -- actual policy validates both record caps1..64MiB.
	var peak uint64
	for i, batch := range [4]uint64{install, activation, persist, reclaim} {
		peak = max(peak, max(1<<20, 3*batch+64)+temp[i])
	}
	return peak
}
