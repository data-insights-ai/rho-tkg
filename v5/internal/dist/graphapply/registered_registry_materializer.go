package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

var errRegisteredRegistryStageUnavailable = errors.New("registered Registry Stage unavailable")

const registryStageFixed uint64 = inventoryFixed + 4096

type registeredRegistryStageLimits struct {
	workBytes, ownedBytes uint64
	// Zero selects the explicit full immutable R0 format profile. This local
	// admission ceiling applies only to absent keys, never original recovery.
	newPreparedBytes int
}
type registryStageLedger struct {
	limits     registeredRegistryStageLimits
	used, held uint64
}

func (q *registryStageLedger) spend(work, peak uint64) error {
	if q.used > q.limits.workBytes || work > q.limits.workBytes-q.used || q.held > q.limits.ownedBytes || peak > q.limits.ownedBytes-q.held {
		return errLimit
	}
	q.used += work
	return nil
}
func registryStageStart(l registeredRegistryStageLimits, borrowed int) (*registryStageLedger, error) {
	if l.workBytes == 0 || l.ownedBytes == 0 || borrowed < 0 || l.newPreparedBytes < 0 || l.newPreparedBytes > registryPreparedMax {
		return nil, errInvalid
	}
	q := &registryStageLedger{limits: l, held: registryStageFixed}
	if err := q.spend(registryStageFixed, uint64(borrowed)); err != nil { // #nosec G115 -- preceding nonnegative borrowed check makes widening lossless.
		return nil, err
	}
	q.held += uint64(borrowed) // #nosec G115 -- same checked nonnegative cap admitted against finite caller ownership before addition.
	return q, nil
}

type registryInstalledDelta struct {
	index, term uint64
	status      byte
	ref         registeredRegistryRef
	outcomeHash [32]byte
	usage       raftlog.ApplicationControlUsage
}
type registeredRegistryMaterializer struct {
	mu           sync.Mutex
	self         *registeredRegistryMaterializer
	store        *raftlog.Store
	limits       registeredRegistryStageLimits
	base         raftlog.ApplicationRoot
	binding      raftlog.ApplicationBinding
	contract     raftlog.ApplicationContract
	origin       registeredOrigin
	usage        raftlog.ApplicationControlUsage
	pending      *registryInstalledDelta
	busy, closed bool
	closeErr     error
}

var _ replica.ApplicationMachine = (*registeredRegistryMaterializer)(nil)
var _ replica.ApplicationSemanticProvider = (*registeredRegistryMaterializer)(nil)

// This unit constructor adopts complete current evidence, not public admission.
// It ignores caller-mutable summary fields and consumes the genuine audit once.
func newRegisteredRegistryMaterializer(s *raftlog.Store, a *registeredInventoryAudit, l registeredRegistryStageLimits) (*registeredRegistryMaterializer, error) {
	if s == nil || a == nil || a.self != a {
		return nil, errInvalid
	}
	q, err := registryStageStart(l, 0)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	if a.closed || a.store != s || a.prepared != nil || a.failed != nil || a.phase != inventoryEOF || a.summary == nil {
		a.mu.Unlock()
		return nil, errInvalid
	}
	// Both the original audit owners and the new copied base coexist throughout
	// fresh capture. Count genuine held state, including its separate summary.
	carried := a.held() + uint64(cap(a.base.Image)) // #nosec G115 -- nonnegative cap admitted with bounded genuine audit owners before copying.
	if err := q.spend(1024, carried); err != nil {
		a.mu.Unlock()
		return nil, err
	}
	q.held += carried
	base, binding, c, origin, usage := a.base, a.binding, a.contract, a.origin, a.usage
	base.Image = make([]byte, len(a.base.Image))
	copy(base.Image, a.base.Image)
	a.mu.Unlock()
	m := &registeredRegistryMaterializer{store: s, limits: l, base: base, binding: binding, contract: c, origin: origin, usage: usage}
	m.self = m
	v, err := m.capture(q, base, usage)
	if err != nil {
		return nil, errors.Join(err, registryCloseView(v))
	}
	err = errors.Join(m.originAt(q, v), v.Close())
	if err != nil {
		return nil, err
	}
	// A second constructor cannot consume this owner while fresh reads run.
	a.mu.Lock()
	if a.closed || a.self != a || a.store != s || !sameBase(a.base, base) || a.origin != origin || a.usage != usage {
		a.mu.Unlock()
		return nil, errInvalid
	}
	a.closed = true
	owned := a.view
	a.view = nil
	a.base.Image, a.cursor, a.pendingKey = nil, nil, nil
	a.page, a.pair = raftlog.ApplicationControlPage{}, raftlog.ApplicationControlRecord{}
	a.mu.Unlock()
	err = owned.Close() // Never under audit/materializer/Driver locks.
	a.mu.Lock()
	a.closeErr = err
	close(a.closeDone)
	a.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return m, nil
}
func (m *registeredRegistryMaterializer) SemanticContractID() raftlog.ApplicationSemanticContractID {
	if m == nil {
		return raftlog.ApplicationSemanticContractID{}
	}
	return registeredInventorySemanticContractID()
}
func (m *registeredRegistryMaterializer) scope() registeredOriginScope {
	_, lineage := registeredGenesisIdentity(m.origin.source)
	return registeredOriginScope{graph: m.binding.Identity.Graph, partition: m.binding.Identity.Partition, group: m.binding.Identity.Group, lineage: lineage, protocol: 1}
}

// capture proves a genuine CURRENT checkpoint plus a retained view of that cut.
// Advancement after capture is still rejected by InstallApplication base checks.
func (m *registeredRegistryMaterializer) capture(q *registryStageLedger, expected raftlog.ApplicationRoot, wanted raftlog.ApplicationControlUsage) (v *raftlog.ApplicationView, err error) {
	if err = q.spend(1024+5*uint64(m.contract.MaxImageBytes), 2*uint64(m.contract.MaxImageBytes)+280); err != nil {
		return nil, err
	} // #nosec G115 -- adopted genuine contract validated positive<=64MiB.
	current, image, err := m.store.Checkpoint()
	if err != nil {
		return nil, err
	}
	if current != expected.Index {
		return nil, errRegisteredRegistryStageUnavailable
	}
	v, err = m.store.ApplicationView(expected.Index)
	if err != nil {
		return nil, err
	}
	root, e := v.RootBounded(context.Background(), 140)
	if e != nil {
		return v, e
	}
	binding, e := v.ApplicationBinding(context.Background())
	if e != nil {
		return v, e
	}
	usage, e := v.ControlInventoryUsage(context.Background())
	if e != nil {
		return v, e
	}
	if !sameBase(root, expected) || !bytes.Equal(image, root.Image) || binding != m.binding || usage != wanted {
		return v, errRegisteredRegistryStageUnavailable
	}
	return v, nil
}
func (m *registeredRegistryMaterializer) originAt(q *registryStageLedger, v *raftlog.ApplicationView) error {
	if err := q.spend(1300+6971, 2600); err != nil {
		return err
	}
	key, _ := registeredGenesisIdentity(m.origin.source)
	r, found, _, err := v.GetControl(context.Background(), key[:], raftlog.ReadBudget{Rows: 2, Bytes: 1300})
	if err != nil {
		return err
	}
	if !found {
		return errRegisteredInventoryUnavailable
	}
	o, err := decodeInventoryRegisteredOrigin(r.Value, registeredJournalBudget{walkBytes: 6971})
	if err != nil {
		return inventoryDataError(err)
	}
	if o != m.origin || r.Index != o.index || !bytes.Equal(r.Key, key[:]) {
		return errCorrupt
	}
	return nil
}
func (m *registeredRegistryMaterializer) term(q *registryStageLedger, index, term uint64) error {
	if err := q.spend(2*uint64(m.store.Limits().MaxEntryBytes)+128, 0); err != nil {
		return err
	} // #nosec G115 -- genuine Store validated entry bound positive<=16MiB.
	return registeredKnownTerm(m.store, index, term)
}
func (m *registeredRegistryMaterializer) point(q *registryStageLedger, v *raftlog.ApplicationView, key []byte, limit int) (raftlog.ApplicationControlRecord, bool, error) {
	if err := q.spend(uint64(limit), 2*uint64(limit)); err != nil {
		return raftlog.ApplicationControlRecord{}, false, err
	} // #nosec G115 -- private callsites use only positive fixed1819/4195950 bounds.
	r, found, _, err := v.GetControl(context.Background(), key, raftlog.ReadBudget{Rows: 2, Bytes: limit})
	if err == nil && found {
		q.held += uint64(cap(r.Key) + cap(r.Value)) // #nosec G115 -- exact-cap output already admitted within preceding2*limit peak; both caps are nonnegative.
	}
	return r, found, err
}
func (m *registeredRegistryMaterializer) original(q *registryStageLedger, v *raftlog.ApplicationView, lookup raftlog.ApplicationControlRecord, through uint64) (registeredRegistryRef, error) {
	if err := q.spend(4714, 4096); err != nil {
		return registeredRegistryRef{}, err
	}
	ref, n, err := decodeRegistryLookup(lookup.Key, lookup.Value, lookup.Index, m.scope(), registeredRegistryCodecBudget{workBytes: 4714, ownedBytes: 4096})
	if err != nil {
		return registeredRegistryRef{}, inventoryDataError(err)
	}
	if ref.position <= m.origin.index || ref.position > through || ref.term < m.origin.term {
		return registeredRegistryRef{}, errCorrupt
	}
	stored, found, err := m.point(q, v, ref.storedKey[:], inventoryStoredPoint)
	if err != nil {
		return registeredRegistryRef{}, err
	}
	if !found {
		return registeredRegistryRef{}, errCorrupt
	}
	if err := q.spend(inventoryStoredWork, 4096); err != nil {
		return registeredRegistryRef{}, err
	}
	actual, pij, err := decodeRegistryStored(stored.Key, stored.Value, stored.Index, m.scope(), registeredInventoryFormat(), registeredRegistryCodecBudget{workBytes: registryWorkMax, ownedBytes: 4096})
	if err != nil || actual != ref || len(pij) != n {
		return registeredRegistryRef{}, errors.Join(errCorrupt, err)
	}
	// This function returns only the fixed ref. Its stored owner and borrowed
	// PIJ scope end on return; the caller clears its own lookup before history.
	q.held -= uint64(cap(stored.Key) + cap(stored.Value)) // #nosec G115 -- exact-cap owned point result is already pre-admitted and nonnegative.
	return ref, nil
}

// historyRoot owns its RootAt image only within this call.
func (m *registeredRegistryMaterializer) historyRoot(q *registryStageLedger, v *raftlog.ApplicationView, ref *registeredRegistryRef) error {
	if err := q.spend(1082+4*uint64(m.contract.MaxImageBytes), 1173); err != nil {
		return err
	} // #nosec G115 -- genuine adopted MaxImageBytes positive<=64MiB.
	root, err := v.RootAt(context.Background(), ref.position, 140)
	if err != nil {
		return inventoryDataError(err)
	}
	if root.Generation != m.base.Generation || root.Index != ref.position || root.ImageHash != m.base.ImageHash || !bytes.Equal(root.Image, m.base.Image) {
		return errCorrupt
	}
	return nil
}

// historyOutcome ends the owned JOR scope before the subsequent term read.
func (m *registeredRegistryMaterializer) historyOutcome(q *registryStageLedger, v *raftlog.ApplicationView, ref *registeredRegistryRef) error {
	if err := q.spend(1082+3*uint64(m.contract.MaxOutcomeBytes)+5034, 1502); err != nil {
		return err
	} // #nosec G115 -- genuine adopted MaxOutcomeBytes positive<=64MiB.
	wire, err := v.ApplicationRecord(context.Background(), ref.position, true, 469)
	if err != nil {
		return inventoryDataError(err)
	}
	out, err := decodeRegistryOutcome(wire, registeredRegistryCodecBudget{workBytes: 5034, ownedBytes: 4096})
	if err != nil || out.status != 1 || out.evaluationIndex != ref.position || out.submitted != ref.submitted || out.original != *ref {
		return errors.Join(errCorrupt, err)
	}
	return nil
}

func (m *registeredRegistryMaterializer) history(q *registryStageLedger, v *raftlog.ApplicationView, ref registeredRegistryRef) error {
	if err := m.historyRoot(q, v, &ref); err != nil {
		return err
	}
	if err := m.historyOutcome(q, v, &ref); err != nil {
		return err
	}
	return m.term(q, ref.position, ref.term)
}

func registryCloseView(v *raftlog.ApplicationView) error {
	if v == nil {
		return nil
	}
	return v.Close()
}

func registryDelta(base raftlog.ApplicationControlUsage, batch raftlog.ApplicationBatch, index, term uint64, status byte, ref registeredRegistryRef) (registryInstalledDelta, error) {
	control := uint64(0)
	for _, row := range batch.ControlPuts {
		n := uint64(len(row.Key) + bytes.Count(row.Key, []byte{0}) + 47 + len(row.Value)) // #nosec G115 -- generatedkeys<=96, generatedvalues<=4194524; sum<=4194763.
		control += n
	}
	graph := uint64(135 + len(batch.Image) + len(batch.Outcome)) // #nosec G115 -- generated140image and0/469outcome bound nonnegative total<=744.
	n := uint64(len(batch.ControlPuts))                          // #nosec G115 -- generated controlcount0or2.
	if base.Bytes > math.MaxUint64-control || base.Records > math.MaxUint64-n || base.TotalBytes > math.MaxUint64-control || base.TotalBytes+control > math.MaxUint64-graph || base.TotalRecords > math.MaxUint64-3-n {
		return registryInstalledDelta{}, errCorrupt
	}
	return registryInstalledDelta{index: index, term: term, status: status, ref: ref, outcomeHash: sha256.Sum256(batch.Outcome), usage: raftlog.ApplicationControlUsage{Bytes: base.Bytes + control, Records: base.Records + n, TotalBytes: base.TotalBytes + control + graph, TotalRecords: base.TotalRecords + 3 + n}}, nil
}

// Joint output admission precedes every variable output backing. This is
// separate from source quota and the later independent two-pass Preflight.
func registryJointAdmission(p raftlog.ApplicationPolicy, b raftlog.ApplicationBudget, status byte, n int, ref registeredRegistryRef) error {
	if min(p.MaxImageBytes, b.ImageBytes) < 140 {
		return errLimit
	}
	work := uint64(1714) // 2*(3*(9+36)+140)+(140+1024).
	if status != 0 {
		if min(p.MaxOutcomeBytes, b.OutcomeBytes) < 469 {
			return errLimit
		}
		work = 2652 // Same formula plus two traversals of469-byte outcome.
	}
	if status == 1 {
		if n < 136 || n > registryPreparedMax {
			return errInvalid
		}
		if min(p.MaxInstallWrites, b.Writes) < 2 || p.MaxKeyBytes < 96 || p.MaxValueBytes < n+220 {
			return errLimit
		}
		a := 96 + bytes.Count(ref.submitted.lookupKey[:], []byte{0}) + 11
		v := 89 + bytes.Count(ref.storedKey[:], []byte{0}) + 11
		work = 2*uint64(n) + 6*uint64(a+v) + 4238 // #nosec G115 -- n136..4194304, a+v<=392 before widening; bounded sum<=8395198.
	}
	if min(p.MaxInstallBytes, b.Bytes) < 1 || work > uint64(min(p.MaxInstallBytes, b.Bytes)) {
		return errLimit
	} // #nosec G115 -- left positive bound before widening.
	return nil
}
func (m *registeredRegistryMaterializer) preflight(q *registryStageLedger, batch raftlog.ApplicationBatch, b raftlog.ApplicationBudget) error {
	p := m.store.ApplicationLimits()
	p.MaxInstallWrites = min(p.MaxInstallWrites, b.Writes)
	p.MaxInstallBytes = min(p.MaxInstallBytes, b.Bytes)
	p.MaxImageBytes = min(p.MaxImageBytes, b.ImageBytes)
	p.MaxChangeBytes = min(p.MaxChangeBytes, b.ChangeBytes)
	p.MaxOutcomeBytes = min(p.MaxOutcomeBytes, b.OutcomeBytes)
	// Preflight->measure invokes controlGrowth, then subtotal controlGrowth
	// traverses keys again. Reserve both full installation allowances plus
	// explicit key comparisons and fixed policy/DTO scalars BEFORE either pass.
	if err := q.spend(2*uint64(p.MaxInstallBytes)+1024+4*(96+89), 0); err != nil {
		return err
	} // #nosec G115 -- genuinepolicy positive<=64MiB narrowed only by validated positive b.Bytes.
	_, err := p.PreflightControlBatch(batch, m.store.Limits())
	return err
}
func (m *registeredRegistryMaterializer) stage(q *registryStageLedger, v *raftlog.ApplicationView, e replica.Entry, b raftlog.ApplicationBudget) (raftlog.ApplicationBatch, registryInstalledDelta, error) {
	zero := func(err error) (raftlog.ApplicationBatch, registryInstalledDelta, error) {
		return raftlog.ApplicationBatch{}, registryInstalledDelta{}, err
	}
	if m.base.Index == math.MaxUint64 || e.Index != m.base.Index+1 || e.Generation != m.base.Generation || e.Term == 0 {
		return zero(errInvalid)
	}
	if err := q.spend(2*uint64(m.store.Limits().MaxEntryBytes)+128, 0); err != nil {
		return zero(err)
	} // #nosec G115 -- genuineStore positiveentry<=16MiB.
	term, err := m.store.Term(e.Index)
	if err != nil {
		return zero(err)
	}
	if term != e.Term {
		return zero(errInvalid)
	}
	if err := m.originAt(q, v); err != nil {
		return zero(err)
	}
	batch := raftlog.ApplicationBatch{BaseGeneration: m.base.Generation, BaseIndex: m.base.Index, BaseImageHash: m.base.ImageHash}
	status := byte(0)
	var ref registeredRegistryRef
	if len(e.Data) != 0 {
		if len(e.Data) < 147 {
			return zero(errInvalid)
		}
		if err := q.spend(uint64(len(e.Data)), 0); err != nil {
			return zero(err)
		} // #nosec G115 -- entry wire admitted against immutable4MiB+11 format ceiling before this helper.
		if !bytes.Equal(e.Data[:7], []byte("RJQ1\x00\x01\x00")) || uint64(binary.BigEndian.Uint32(e.Data[7:11])) != uint64(len(e.Data)-11) {
			return zero(errInvalid)
		} // #nosec G115 -- left len>=147 and prior4MiB+11 length gate bound remainder136..4MiB.
		// This immutable-format loan is independent of conditional new-request
		// and supplied install/output caps. Old originals are never denied by
		// those caps before fresh lookup and full reciprocal original validation.
		loan := min(uint64(registryWorkMax), q.limits.workBytes-q.used)
		if loan < registryFixed+32 {
			return zero(errLimit)
		}
		if err := q.spend(loan, uint64(4096+309+len(e.Data)-11+220)); err != nil {
			return zero(err)
		} // #nosec G115 -- admittedlen bounds sum positive<=4198929 before widening.
		plan, err := prepareRegistryRecordEmission(e.Data[11:], e.Index, e.Term, registeredInventoryFormat(), registeredRegistryCodecBudget{workBytes: int(loan), ownedBytes: registryOwnedMax}) // #nosec G115 -- loan<=46144850 before narrowing.
		if err != nil {
			return zero(err)
		}
		q.used -= loan - plan.work // Exactly one settlement of the previously admitted child+emission loan.
		if plan.inspection.parsed.o != m.scope() {
			return zero(errInvalid)
		}
		incoming := registryRef(&plan.inspection, e.Index, e.Term)
		lookup, found, err := m.point(q, v, incoming.submitted.lookupKey[:], inventoryLookupPoint)
		if err != nil {
			return zero(err)
		}
		if found {
			ref, err = m.original(q, v, lookup, m.base.Index)
			q.held -= uint64(cap(lookup.Key) + cap(lookup.Value)) // #nosec G115 -- nonnegative exact-cap point output already admitted and held.
			lookup = raftlog.ApplicationControlRecord{}
			if err != nil {
				return zero(err)
			}
			if ref.term > e.Term {
				return zero(errCorrupt)
			}
			plan = nil // Its borrowed input spans cannot survive original history.
			if err := m.history(q, v, ref); err != nil {
				return zero(err)
			}
			status = 3
			if incoming.submitted.kind == ref.submitted.kind && incoming.submitted.submissionHash == ref.submitted.submissionHash {
				status = 2
				incoming.submitted = ref.submitted // Always return original PH/address, not a fresh candidate identity.
			}
		} else {
			if m.limits.newPreparedBytes != 0 && len(e.Data)-11 > m.limits.newPreparedBytes {
				return zero(errLimit)
			}
			if err := registryJointAdmission(m.store.ApplicationLimits(), b, 1, len(e.Data)-11, incoming); err != nil {
				return zero(err)
			}
			r, found, err := m.point(q, v, incoming.storedKey[:], inventoryStoredPoint)
			if err != nil {
				return zero(err)
			}
			if found {
				q.held -= uint64(cap(r.Key) + cap(r.Value)) // #nosec G115 -- exact-cap pre-admitted point owner.
				return zero(errCorrupt)
			}
			if err := q.spend(0, 185+96+uint64(309+len(e.Data)-11+220)); err != nil {
				return zero(err)
			} // #nosec G115 -- generated output size<=4194833; other terms fixed.
			hki, lookup, hkv, stored, err := emitRegistryRecords(plan)
			if err != nil {
				return zero(err)
			}
			batch.ControlPuts = []raftlog.ApplicationControlPut{{Key: hki[:], Value: lookup}, {Key: hkv[:], Value: stored}}
			q.held += uint64(185 + 96 + cap(lookup) + cap(stored)) // #nosec G115 -- exact-cap output admitted before emission; sum<=4195114.
			ref = incoming
			status = 1
		}
		if err := registryJointAdmission(m.store.ApplicationLimits(), b, status, len(e.Data)-11, ref); err != nil {
			return zero(err)
		}
		if err := q.spend(5503, 4565); err != nil {
			return zero(err)
		}
		batch.Outcome, err = encodeRegistryOutcome(status, e.Index, incoming.submitted, ref, registeredRegistryCodecBudget{workBytes: 5503, ownedBytes: 4565})
		if err != nil {
			return zero(err)
		}
		q.held += uint64(cap(batch.Outcome)) // #nosec G115 -- exact469 output admitted before encoder.
	}
	if status == 0 {
		if err := registryJointAdmission(m.store.ApplicationLimits(), b, 0, 0, ref); err != nil {
			return zero(err)
		}
	}
	if err := q.spend(280, 140); err != nil {
		return zero(err)
	}
	batch.Image = make([]byte, 140)
	copy(batch.Image, m.base.Image)
	q.held += 140
	if err := m.preflight(q, batch, b); err != nil {
		return zero(err)
	}
	if err := q.spend(uint64(len(batch.Outcome)), 0); err != nil {
		return zero(err)
	} // #nosec G115 -- generated outcome has length0or469; explicit additional hash traversal.
	delta, err := registryDelta(m.usage, batch, e.Index, e.Term, status, ref)
	if err != nil {
		return zero(err)
	}
	return batch, delta, nil
}

// Stage is a bounded UNIT callback. Genuine view cleanup runs outside m.mu;
// actual Driver lock-detached cleanup handoff remains a separate prerequisite.
func (m *registeredRegistryMaterializer) Stage(e replica.Entry, b raftlog.ApplicationBudget) (batch raftlog.ApplicationBatch, err error) {
	if m == nil || m.self != m || b.Writes < 1 || b.Bytes < 1 || b.ImageBytes < 1 || b.ChangeBytes < 1 || b.OutcomeBytes < 1 {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	if len(e.Data) > registryPreparedMax+11 {
		return raftlog.ApplicationBatch{}, errLimit
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return raftlog.ApplicationBatch{}, raftlog.ErrClosed
	}
	if m.busy || m.pending != nil {
		m.mu.Unlock()
		return raftlog.ApplicationBatch{}, errInvalid
	}
	m.busy = true
	q, err := registryStageStart(m.limits, cap(e.Data))
	var v *raftlog.ApplicationView
	var delta registryInstalledDelta
	if err == nil {
		if err = q.spend(0, uint64(cap(m.base.Image))); err == nil {
			q.held += uint64(cap(m.base.Image))
		} // #nosec G115 -- genuine copiedbase image cap140, pre-admitted before carrying.
	}
	if err == nil {
		v, err = m.capture(q, m.base, m.usage)
	}
	if err == nil {
		q.held += 140
		batch, delta, err = m.stage(q, v, e, b)
	} // Captured view owns one exact140 image while helper root copies have ended.
	m.mu.Unlock()
	cleanupErr := registryCloseView(v)
	err = errors.Join(err, cleanupErr)
	m.mu.Lock()
	m.closeErr = errors.Join(m.closeErr, cleanupErr)
	m.busy = false
	if err == nil {
		m.pending = &delta
	} else {
		batch = raftlog.ApplicationBatch{}
	}
	m.mu.Unlock()
	return batch, err
}

// installedOutcome owns the outcome bytes through hash/union validation only.
func (m *registeredRegistryMaterializer) installedOutcome(q *registryStageLedger, v *raftlog.ApplicationView, p *registryInstalledDelta) error {
	if err := q.spend(1082+3*uint64(m.contract.MaxOutcomeBytes)+5034, 1502); err != nil {
		return err
	} // #nosec G115 -- genuine adopted outcome maximum positive<=64MiB.
	maximum := 1
	if p.status != 0 {
		maximum = 469
	}
	wire, err := v.ApplicationRecord(context.Background(), p.index, true, maximum)
	if err != nil {
		return inventoryDataError(err)
	}
	if err := q.spend(uint64(len(wire)), 0); err != nil {
		return err
	} // #nosec G115 -- actual ApplicationRecord output admitted with maximum1or469 before extra hash.
	if wire == nil || sha256.Sum256(wire) != p.outcomeHash {
		return errCorrupt
	}
	if p.status != 0 {
		out, err := decodeRegistryOutcome(wire, registeredRegistryCodecBudget{workBytes: 5034, ownedBytes: 4096})
		if err != nil || out.status != p.status || out.evaluationIndex != p.index || out.original != p.ref {
			return errors.Join(errCorrupt, err)
		}
	}
	return nil
}

// installedChange releases its present-empty record scope before term lookup.
func (m *registeredRegistryMaterializer) installedChange(q *registryStageLedger, v *raftlog.ApplicationView, index uint64) error {
	if err := q.spend(1082+3*uint64(m.contract.MaxChangeBytes), 1034); err != nil {
		return err
	} // #nosec G115 -- genuine adopted change maximum positive<=64MiB.
	change, err := v.ApplicationRecord(context.Background(), index, false, 1)
	if err != nil {
		return inventoryDataError(err)
	}
	if change == nil || len(change) != 0 {
		return errCorrupt
	}
	return nil
}

func (m *registeredRegistryMaterializer) verifyInstalled(q *registryStageLedger, v *raftlog.ApplicationView, p registryInstalledDelta) error {
	if err := m.originAt(q, v); err != nil {
		return err
	}
	if err := m.installedOutcome(q, v, &p); err != nil {
		return err
	}
	if p.status == 1 {
		lookup, found, err := m.point(q, v, p.ref.submitted.lookupKey[:], inventoryLookupPoint)
		if err != nil {
			return err
		}
		if !found {
			return errCorrupt
		}
		ref, err := m.original(q, v, lookup, p.index)
		q.held -= uint64(cap(lookup.Key) + cap(lookup.Value)) // #nosec G115 -- exact-cap pre-admitted owner.
		lookup = raftlog.ApplicationControlRecord{}
		if err != nil || ref != p.ref {
			return errors.Join(errCorrupt, err)
		}
		if err := m.history(q, v, ref); err != nil {
			return err
		}
	}
	if err := m.installedChange(q, v, p.index); err != nil {
		return err
	}
	return m.term(q, p.index, p.term)
}

// Unknown/nonadjacent/generation-changing restores require complete audit
// outside Driver. A pending verified adjacent delta is not cached readiness.
func (m *registeredRegistryMaterializer) Restore(index uint64, image []byte) error {
	if m == nil || m.self != m || index == 0 {
		return errInvalid
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return raftlog.ErrClosed
	}
	if m.busy {
		m.mu.Unlock()
		return errInvalid
	}
	if len(image) != 140 {
		m.mu.Unlock()
		return errInvalid
	}
	q, err := registryStageStart(m.limits, cap(image))
	if err != nil {
		m.mu.Unlock()
		return err
	}
	if err := q.spend(280, uint64(cap(m.base.Image))); err != nil {
		m.mu.Unlock()
		return err
	} // #nosec G115 -- genuine140 copiedbase cap; incoming cap admitted before comparison.
	q.held += uint64(cap(m.base.Image)) // #nosec G115 -- pre-admitted nonnegative image cap.
	if !bytes.Equal(image, m.base.Image) {
		m.mu.Unlock()
		return errRegisteredRegistryStageUnavailable
	}
	target, usage := m.base, m.usage
	pending := m.pending
	if pending != nil {
		if index != pending.index {
			m.mu.Unlock()
			return errRegisteredRegistryStageUnavailable
		}
		target.Index, usage = pending.index, pending.usage
	} else if index != target.Index {
		m.mu.Unlock()
		return errRegisteredRegistryStageUnavailable
	}
	m.busy = true
	v, err := m.capture(q, target, usage)
	if err == nil {
		q.held += 140
		if pending != nil {
			err = m.verifyInstalled(q, v, *pending)
		} else {
			err = m.originAt(q, v)
		}
	}
	m.mu.Unlock()
	cleanupErr := registryCloseView(v)
	err = errors.Join(err, cleanupErr)
	m.mu.Lock()
	m.closeErr = errors.Join(m.closeErr, cleanupErr)
	m.busy = false
	if err == nil {
		m.base.Index = index
		m.usage = usage
		m.pending = nil
	}
	m.mu.Unlock()
	return err
}
func (m *registeredRegistryMaterializer) Close() error {
	if m == nil || m.self != m {
		return errInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return m.closeErr
	}
	if m.busy {
		return errLimit
	} // No cancellation/closing a live operation's owner underneath it.
	m.closed = true
	m.base.Image = nil
	m.pending = nil
	return m.closeErr // No persistent view or Store ownership; callbacks detach/close views themselves.
}
