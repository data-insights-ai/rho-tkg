package graphapply

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

var errRegisteredInventoryUnavailable = errors.New("registered inventory origin unavailable")

const inventoryFixed uint64 = 625278
const inventoryStoredWork uint64 = 41947576
const inventoryStoredPoint = 4195950
const inventoryLookupPoint = 1819

const (
	inventoryOrigin byte = iota
	inventorySourceRoot
	inventorySourceChange
	inventorySourceOutcome
	inventoryGenesisRoot
	inventoryGenesisChange
	inventoryGenesisOutcome
	inventoryHeadTerm
	inventorySourceTerm
	inventoryGenesisTerm
	inventoryPage
	inventoryRow
	inventoryPoint
	inventoryStored
	inventoryDrop
	inventoryRowRoot
	inventoryRowChange
	inventoryRowOutcome
	inventoryRowTerm
	inventoryCommit
	inventoryEOF
)

type registeredInventoryBudget struct{ sourceBytes, ownedBytes uint64 }
type registeredInventoryPreparedContext struct {
	index, term uint64
	voters      [3]uint64
}
type registeredInventorySummary struct {
	owner                        *registeredInventoryAudit
	prepared                     *raftlog.PreparedApplicationSnapshot
	base                         raftlog.ApplicationRoot
	origin                       registeredOrigin
	controlBytes, controlRecords uint64
	graphBytes, graphRecords     uint64
	pairs, maxOriginalTerm       uint64
	expected                     registeredInventoryPreparedContext
}
type registeredInventoryAudit struct {
	mu                           sync.Mutex
	self                         *registeredInventoryAudit
	closed                       bool
	closeDone                    chan struct{}
	closeErr, failed             error
	store                        *raftlog.Store
	prepared                     *raftlog.PreparedApplicationSnapshot
	view                         *raftlog.ApplicationView
	base                         raftlog.ApplicationRoot
	binding                      raftlog.ApplicationBinding
	contract                     raftlog.ApplicationContract
	expected                     registeredInventoryPreparedContext
	entry                        uint64
	maximum                      registeredInventoryBudget
	usage                        raftlog.ApplicationControlUsage
	origin                       registeredOrigin
	seedKey                      [47]byte
	phase                        byte
	page                         raftlog.ApplicationControlPage
	pair                         raftlog.ApplicationControlRecord
	cursor, pendingKey           []byte
	ref                          registeredRegistryRef
	pendingFamily                byte
	pendingLength, pendingCost   uint64
	controlBytes, controlRecords uint64
	lookups, stored, seeds       uint64
	maxOriginalTerm              uint64
	summary                      *registeredInventorySummary
}

func inventoryAdmit(b registeredInventoryBudget, work, owned uint64) error {
	if b.sourceBytes == 0 || b.ownedBytes == 0 {
		return errInvalid
	}
	if work > b.sourceBytes || owned > b.ownedBytes {
		return errLimit
	}
	return nil
}
func inventoryContractNumbers(c raftlog.ApplicationContract) (out [10]uint64) {
	ns := [10]int{int(c.Version), c.MaxKeyBytes, c.MaxValueBytes, c.MaxImageBytes, c.MaxPageRows, c.MaxPageBytes, c.MaxInstallWrites, c.MaxInstallBytes, c.MaxChangeBytes, c.MaxOutcomeBytes}
	for i, n := range ns {
		out[i] = uint64(n) // #nosec G115 -- Each genuine contract field is positive and <=64MiB before this call. inventoryContract validated Version2 and every positive field<=64MiB before this private capture-contract use.
	}
	return out
}
func inventoryContract(c raftlog.ApplicationContract) error {
	if c.Version != 2 || c.MaxKeyBytes < 96 || c.MaxValueBytes < registryPreparedMax+220 || c.MaxImageBytes < 140 || c.MaxPageRows < 2 || c.MaxPageBytes < 16781682 || c.MaxOutcomeBytes < 469 {
		return errLimit
	}
	for _, n := range [9]int{c.MaxKeyBytes, c.MaxValueBytes, c.MaxImageBytes, c.MaxPageRows, c.MaxPageBytes, c.MaxInstallWrites, c.MaxInstallBytes, c.MaxChangeBytes, c.MaxOutcomeBytes} {
		if n < 1 || n > 64<<20 {
			return errInvalid
		}
	}
	return nil
}
func inventoryRequirements(c raftlog.ApplicationContract, entry uint64) (registeredInventoryBudget, error) {
	if err := inventoryContract(c); err != nil {
		return registeredInventoryBudget{}, err
	}
	if entry > 64<<20 {
		return registeredInventoryBudget{}, errInvalid
	}
	i, q, t, p, k := uint64(c.MaxImageBytes), uint64(c.MaxOutcomeBytes), uint64(c.MaxChangeBytes), uint64(c.MaxPageBytes), uint64(c.MaxKeyBytes) // #nosec G115 -- immediate inventoryContract success bounds these positive fields to64MiB before widening.
	work := max(1024+5*i, 1024+1082+4*i, 1300+6971, 1082+3*q+5034, 1082+3*t, 2*entry+128, p+1024+280, inventoryStoredWork, inventoryStoredPoint+4714)
	// Six140 slots were explicit in the selectedF. Substitute actual carried
	// image/cursor owners instead of implicitly spending a metadata gap.
	pageExtra := uint64(0)
	if 2*k+560 > 840 {
		pageExtra = 2*k + 560 - 840
	}
	owned := max(i+280, 1024+280, p+pageExtra, 2*(uint64(registryPreparedMax)+220+96)+128+96+inventoryStoredPoint, 1033+469+145)
	return registeredInventoryBudget{sourceBytes: inventoryFixed + work, ownedBytes: inventoryFixed + owned}, nil
}
func inventorySeed(image []byte, binding raftlog.ApplicationBinding) error {
	if len(image) != 140 || !bytes.Equal(image[:4], []byte{'G', 'R', 2, 2}) {
		return errCorrupt
	}
	r, err := graphstore.DecodeRoot(image)
	if err != nil {
		return errCorrupt
	}
	t, err := r.SinglePartition()
	n := r.Namespace()
	fresh, e := graphstore.NewRoot(n, r.OwnershipEpoch())
	if err != nil || e != nil || n.Graph != binding.Identity.Graph || n.Partition != binding.Identity.Partition || t.TopologyEpoch != 1 || t.SchemaVersion != 1 || t.IndexVersion != 0 || r.SemanticEpoch() != 0 || r.NextPhysicalID() != 1 || r.EffectDigest() != fresh.EffectDigest() {
		return errCorrupt
	}
	return nil
}
func finishInventoryCapture(ctx context.Context, v *raftlog.ApplicationView, c raftlog.ApplicationContract, expected registeredInventoryPreparedContext, s *raftlog.Store, p *raftlog.PreparedApplicationSnapshot) (a *registeredInventoryAudit, err error) {
	defer func() {
		if err != nil {
			err = errors.Join(err, v.Close())
			a = nil
		}
	}()
	binding, err := v.ApplicationBinding(ctx)
	if err != nil {
		return nil, err
	}
	if binding.Validate() != nil || binding.SemanticContractID != registeredInventorySemanticContractID() {
		return nil, errInvalid
	}
	root, err := v.RootBounded(ctx, 140)
	if err != nil {
		return nil, err
	}
	if err := inventorySeed(root.Image, binding); err != nil {
		return nil, err
	}
	usage, err := v.ControlInventoryUsage(ctx)
	if err != nil {
		return nil, err
	}
	if usage.TotalBytes < usage.Bytes || usage.TotalRecords < usage.Records || root.Index > math.MaxUint64/3 || usage.TotalRecords-usage.Records != 3*root.Index || p != nil && root.Index != expected.index {
		return nil, errCorrupt
	}
	a = &registeredInventoryAudit{view: v, store: s, prepared: p, base: root, binding: binding, contract: c, expected: expected, usage: usage, closeDone: make(chan struct{})}
	a.self = a
	if s != nil {
		a.entry = uint64(s.Limits().MaxEntryBytes) // #nosec G115 -- Store validated finite positive entry limit. genuine Store.Open validates MaxEntryBytes positive and<=16MiB; Limits returns that immutable value.
	}
	a.maximum, err = inventoryRequirements(c, a.entry)
	return a, err
}
func newRegisteredCurrentInventory(ctx context.Context, s *raftlog.Store, b registeredInventoryBudget) (*registeredInventoryAudit, error) {
	if ctx == nil || s == nil {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := inventoryAdmit(b, 1024, 1024); err != nil {
		return nil, err
	}
	c, err := raftlog.ApplicationContractForPolicyWithControls(s.ApplicationLimits(), s.ApplicationControls())
	if err != nil {
		return nil, err
	}
	if err := inventoryContract(c); err != nil {
		return nil, err
	}
	i := uint64(c.MaxImageBytes) // #nosec G115 -- preceding inventoryContract success bounds MaxImageBytes to1..64MiB before widening.
	if err := inventoryAdmit(b, inventoryFixed+1024+5*i, inventoryFixed+i+280); err != nil {
		return nil, err
	}
	index, image, err := s.Checkpoint()
	if err != nil {
		return nil, err
	}
	if len(image) != 140 {
		return nil, errCorrupt
	}
	_, cs, err := s.InitialState()
	if err != nil {
		return nil, err
	}
	if cs == nil || len(cs.ProtoReflect().GetUnknown()) != 0 || len(cs.GetVoters()) != 3 || len(cs.GetVotersOutgoing()) != 0 || len(cs.GetLearners()) != 0 || len(cs.GetLearnersNext()) != 0 || cs.GetAutoLeave() {
		return nil, errCorrupt
	}
	var expected registeredInventoryPreparedContext
	copy(expected.voters[:], cs.GetVoters())
	v, err := s.ApplicationView(index)
	if err != nil {
		return nil, err
	}
	a, err := finishInventoryCapture(ctx, v, c, expected, s, nil)
	if err == nil && !bytes.Equal(image, a.base.Image) {
		err = errors.Join(errCorrupt, a.Close())
		a = nil
	}
	return a, err
}
func newRegisteredPreparedInventory(ctx context.Context, p *raftlog.PreparedApplicationSnapshot, expected registeredInventoryPreparedContext, b registeredInventoryBudget) (*registeredInventoryAudit, error) {
	if ctx == nil || p == nil || expected.index == 0 || expected.term == 0 || expected.voters[0] == 0 || expected.voters[0] >= expected.voters[1] || expected.voters[1] >= expected.voters[2] {
		return nil, errInvalid
	}
	if err := inventoryAdmit(b, 1024, 1024); err != nil {
		return nil, err
	}
	c, err := p.ApplicationContract(ctx)
	if err != nil {
		return nil, err
	}
	if err := inventoryContract(c); err != nil {
		return nil, err
	}
	if err := inventoryAdmit(b, inventoryFixed+1024+1082+4*uint64(c.MaxImageBytes), inventoryFixed+280+1024); err != nil { // #nosec G115 -- preceding inventoryContract success bounds MaxImageBytes to1..64MiB before widening.
		return nil, err
	}
	v, err := p.OpenReadView(ctx, expected.index, 140)
	if err != nil {
		return nil, err
	}
	return finishInventoryCapture(ctx, v, c, expected, nil, p)
}

// Ownership is logical exact-cap payload accounting, not heap/RSS. The view's
// captured image and the independent base copy are separate live backings.
// F explicitly contained six140-byte image slots; replace those slots with
// actual capacities here, including unknown/future cursors. No ABI/RSS claim.
func (a *registeredInventoryAudit) held() uint64 {
	n := inventoryFixed - 6*140 + uint64(2*cap(a.base.Image)+cap(a.cursor)+cap(a.pendingKey)+64*cap(a.page.Records)+cap(a.page.After)+cap(a.pair.Key)+cap(a.pair.Value))
	for _, row := range a.page.Records {
		n += uint64(cap(row.Key) + cap(row.Value))
	}
	if a.summary != nil {
		n += uint64(cap(a.summary.base.Image))
	}
	return n
}
func (a *registeredInventoryAudit) need(b registeredInventoryBudget, work, extra uint64) error {
	if inventoryFixed+work > a.maximum.sourceBytes || a.held()+extra > a.maximum.ownedBytes {
		return errLimit
	}
	return inventoryAdmit(b, inventoryFixed+work, a.held()+extra)
}
func inventoryDataError(err error) error {
	if errors.Is(err, errInvalid) || errors.Is(err, errLimit) || errors.Is(err, raftlog.ErrLimit) {
		return errors.Join(errCorrupt, err)
	}
	return err
}
func (a *registeredInventoryAudit) scope() registeredOriginScope {
	_, lineage := registeredGenesisIdentity(a.origin.source)
	return registeredOriginScope{graph: a.binding.Identity.Graph, partition: a.binding.Identity.Partition, group: a.binding.Identity.Group, lineage: lineage, protocol: 1}
}
func (a *registeredInventoryAudit) point(ctx context.Context, key []byte, budget int) (raftlog.ApplicationControlRecord, error) {
	r, found, _, err := a.view.GetControl(ctx, key, raftlog.ReadBudget{Rows: 2, Bytes: budget})
	if err != nil {
		return raftlog.ApplicationControlRecord{}, err
	}
	if !found {
		return raftlog.ApplicationControlRecord{}, errCorrupt
	}
	return r, nil
}
func (a *registeredInventoryAudit) originPhase(ctx context.Context) error {
	key, _ := registeredGenesisIdentity(registeredGenesisSource{Scope: a.binding.Identity})
	r, found, _, err := a.view.GetControl(ctx, key[:], raftlog.ReadBudget{Rows: 2, Bytes: 1300})
	if err != nil {
		return err
	}
	if !found {
		if a.usage.Records == 0 && a.usage.Bytes == 0 {
			return errRegisteredInventoryUnavailable
		}
		return errCorrupt
	}
	o, err := decodeInventoryRegisteredOrigin(r.Value, registeredJournalBudget{walkBytes: 6971})
	if err != nil {
		return inventoryDataError(err)
	}
	if !bytes.Equal(r.Key, key[:]) || r.Index != o.index || o.index > a.base.Index || o.source.Scope != a.binding.Identity || o.source.Meaning != [32]byte(a.binding.SemanticContractID) || o.source.ImageHash != a.base.ImageHash || o.source.SharedContract != inventoryContractNumbers(a.contract) || o.source.Voters != a.expected.voters {
		return errCorrupt
	}
	root, err := graphstore.DecodeRoot(a.base.Image)
	if err != nil || root.OwnershipEpoch() != o.source.InitialOwnerEpoch || a.prepared != nil && o.term > a.expected.term {
		return errCorrupt
	}
	a.origin, a.seedKey, a.maxOriginalTerm = o, key, o.term
	return nil
}
func (a *registeredInventoryAudit) rootAt(ctx context.Context, index uint64) error {
	r, err := a.view.RootAt(ctx, index, 140)
	if err != nil {
		return inventoryDataError(err)
	}
	if r.Index != index || r.Generation != a.base.Generation || r.ImageHash != a.base.ImageHash || !bytes.Equal(r.Image, a.base.Image) {
		return errCorrupt
	}
	return nil
}
func (a *registeredInventoryAudit) envelope(ctx context.Context, index uint64, outcome bool, kind byte) error {
	maxBytes := 1
	if outcome && kind != 0 {
		maxBytes = 469
	}
	wire, err := a.view.ApplicationRecord(ctx, index, outcome, maxBytes)
	if err != nil {
		return inventoryDataError(err)
	}
	if kind == 0 {
		if wire == nil || len(wire) != 0 {
			return errCorrupt
		}
		return nil
	}
	if kind == 1 {
		expected, err := encodeInventoryRegisteredGenesisOutcome(a.origin.source, a.origin.index, a.origin.term, a.origin.index, 4, registeredJournalBudget{walkBytes: 3372})
		if err != nil || !bytes.Equal(wire, expected) {
			return errCorrupt
		}
		return nil
	}
	out, err := decodeRegistryOutcome(wire, registeredRegistryCodecBudget{workBytes: 5034, ownedBytes: 4096})
	if err != nil || out.status != 1 || out.evaluationIndex != a.ref.position || out.submitted != a.ref.submitted || out.original != a.ref {
		return errCorrupt
	}
	return nil
}
func (a *registeredInventoryAudit) row() error {
	r := a.page.Records[0]
	key := r.Key
	var family byte
	switch {
	case len(key) == 47 && bytes.Equal(key[:5], []byte("HKS1\x01")):
		family = 1
	case len(key) == 96 && bytes.Equal(key[:5], []byte("HKI1\x02")):
		family = 2
	case len(key) == 89 && bytes.Equal(key[:5], []byte("HKV1\x03")):
		family = 3
	default:
		return errCorrupt
	}
	cost := uint64(len(key) + bytes.Count(key, []byte{0}) + 47 + len(r.Value)) // #nosec G115 -- Advance width gate bounds key<=96, zeroCount<=96 and value<=4194524; nonnegative subtotal<=4194763 fits int and uint64.
	if a.controlBytes > a.usage.Bytes || cost > a.usage.Bytes-a.controlBytes || a.controlRecords >= a.usage.Records {
		return errCorrupt
	}
	if family == 1 {
		o, err := decodeInventoryRegisteredOrigin(r.Value, registeredJournalBudget{walkBytes: 6971})
		if err != nil || o != a.origin || r.Index != o.index || !bytes.Equal(key, a.seedKey[:]) {
			return errCorrupt
		}
	} else {
		var ref registeredRegistryRef
		var n int
		var err error
		if family == 2 {
			ref, n, err = decodeRegistryLookup(key, r.Value, r.Index, a.scope(), registeredRegistryCodecBudget{workBytes: 4714, ownedBytes: 4096})
		} else {
			var pij []byte
			ref, pij, err = decodeRegistryStored(key, r.Value, r.Index, a.scope(), registeredInventoryFormat(), registeredRegistryCodecBudget{workBytes: registryWorkMax, ownedBytes: 4096})
			n = len(pij)
		}
		if err != nil || ref.position <= a.origin.index || ref.position > a.base.Index || ref.term < a.origin.term || ref.term > a.expected.term {
			return errors.Join(errCorrupt, err)
		}
		a.ref, a.pendingLength = ref, uint64(n)
	}
	a.pendingKey = make([]byte, len(key))
	copy(a.pendingKey, key)
	a.pendingFamily, a.pendingCost = family, cost
	return nil
}

// This independent immutable child allowance is the selected complete format,
// not a new admission cap or a test helper. Old generic codec APIs are unchanged.
func registeredInventoryFormat() registeredFormatBudget {
	return registeredFormatBudget{preparedBytes: 4 << 20, domainBytes: 4 << 20, axes: 1024, descriptorBytes: 4096, schemas: 4096, schemaNameBytes: 256, walkBytes: 32 << 20}
}
func (a *registeredInventoryAudit) paired(ctx context.Context) error {
	key, budget := a.ref.storedKey[:], inventoryStoredPoint
	if a.pendingFamily == 3 {
		key, budget = a.ref.submitted.lookupKey[:], inventoryLookupPoint
	}
	r, err := a.point(ctx, key, budget)
	if err != nil {
		return err
	}
	if r.Index != a.ref.position || len(r.Key) > 96 || len(r.Value) > registryPreparedMax+220 {
		return errCorrupt
	}
	if a.pendingFamily == 3 {
		ref, n, err := decodeRegistryLookup(r.Key, r.Value, r.Index, a.scope(), registeredRegistryCodecBudget{workBytes: 4714, ownedBytes: 4096})
		if err != nil || ref != a.ref || uint64(n) != a.pendingLength { // #nosec G115 -- left err!=nil short-circuit excludes decode failure; successful Lookup bounds n to136..4194304 before widening.
			return errors.Join(errCorrupt, err)
		}
	}
	a.pair = r
	return nil
}
func (a *registeredInventoryAudit) storedPair() error {
	r := a.pair
	ref, pij, err := decodeRegistryStored(r.Key, r.Value, r.Index, a.scope(), registeredInventoryFormat(), registeredRegistryCodecBudget{workBytes: registryWorkMax, ownedBytes: 4096})
	if err != nil || ref != a.ref || uint64(len(pij)) != a.pendingLength {
		return errors.Join(errCorrupt, err)
	}
	return nil
}
func (a *registeredInventoryAudit) commitRow() error {
	if a.controlRecords == math.MaxUint64 || a.pendingCost > math.MaxUint64-a.controlBytes {
		return errCorrupt
	}
	a.controlBytes += a.pendingCost
	a.controlRecords++
	switch a.pendingFamily {
	case 1:
		a.seeds++
	case 2:
		a.lookups++
	case 3:
		a.stored++
	}
	a.maxOriginalTerm = max(a.maxOriginalTerm, a.ref.term)
	a.cursor, a.pendingKey = a.pendingKey, nil
	a.page, a.pair = raftlog.ApplicationControlPage{}, raftlog.ApplicationControlRecord{}
	return nil
}

func (a *registeredInventoryAudit) Advance(ctx context.Context, b registeredInventoryBudget) (done bool, err error) {
	if a == nil || a.self != a || ctx == nil {
		return false, errInvalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return false, raftlog.ErrClosed
	}
	if a.failed != nil {
		return false, a.failed
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var work, extra uint64
	i, q, t := uint64(a.contract.MaxImageBytes), uint64(a.contract.MaxOutcomeBytes), uint64(a.contract.MaxChangeBytes) // #nosec G115 -- genuine constructors validate and privately capture positive immutable contract fields<=64MiB before any phase.
	switch a.phase {
	case inventoryOrigin:
		work, extra = 1300+6971, 1300
	case inventorySourceRoot, inventoryGenesisRoot, inventoryRowRoot:
		work, extra = 1082+4*i, 1033+140
	case inventorySourceChange, inventoryGenesisChange, inventoryRowChange:
		work, extra = 1082+3*t, 1034
	case inventorySourceOutcome:
		work, extra = 1082+3*q, 1034
	case inventoryGenesisOutcome:
		work, extra = 1082+3*q+3372, 1033+469+145
	case inventoryHeadTerm, inventorySourceTerm, inventoryGenesisTerm, inventoryRowTerm:
		work = 2*a.entry + 128
	case inventoryPage:
		work, extra = uint64(a.contract.MaxPageBytes), uint64(a.contract.MaxPageBytes) // #nosec G115 -- genuine constructors validate and privately capture MaxPageBytes to1..64MiB before any phase.
	case inventoryRow:
		work, extra = max(inventoryStoredWork, 6971), 96
	case inventoryPoint:
		work, extra = inventoryStoredPoint+4714, inventoryStoredPoint
	case inventoryStored:
		work = inventoryStoredWork
	case inventoryRowOutcome:
		work, extra = 1082+3*q+5034, 1033+469
	}
	if err := a.need(b, work, extra); err != nil {
		return false, err
	}
	defer func() {
		if err != nil && (errors.Is(err, errCorrupt) || !errors.Is(err, errLimit) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) {
			a.failed = err
		}
	}()
	switch a.phase {
	case inventoryOrigin:
		err = a.originPhase(ctx)
	case inventorySourceRoot:
		err = a.rootAt(ctx, 1)
	case inventorySourceChange:
		err = a.envelope(ctx, 1, false, 0)
	case inventorySourceOutcome:
		err = a.envelope(ctx, 1, true, 0)
	case inventoryGenesisRoot:
		err = a.rootAt(ctx, a.origin.index)
	case inventoryGenesisChange:
		err = a.envelope(ctx, a.origin.index, false, 0)
	case inventoryGenesisOutcome:
		err = a.envelope(ctx, a.origin.index, true, 1)
	case inventoryHeadTerm:
		if a.store != nil {
			a.expected.term, err = a.store.Term(a.base.Index)
		}
		if err == nil && a.expected.term < a.origin.term {
			err = errCorrupt
		}
	case inventorySourceTerm:
		if a.store != nil {
			err = registeredKnownTerm(a.store, 1, 1)
		}
	case inventoryGenesisTerm:
		if a.store != nil {
			err = registeredKnownTerm(a.store, a.origin.index, a.origin.term)
		}
	case inventoryPage:
		var page raftlog.ApplicationControlPage
		page, err = a.view.ControlPage(ctx, a.cursor, raftlog.ReadBudget{Rows: 2, Bytes: a.contract.MaxPageBytes})
		if err != nil {
			break
		}
		for _, r := range page.Records {
			if len(r.Key) > 96 || len(r.Value) > registryPreparedMax+220 {
				return false, errCorrupt
			}
		}
		if len(page.Records) == 0 {
			if page.Complete {
				a.phase = inventoryEOF
				return true, nil
			}
			if len(page.After) == 0 || bytes.Compare(page.After, a.cursor) <= 0 {
				return false, errCorrupt
			}
			a.cursor = page.After
			return false, nil
		}
		a.page = page
	case inventoryRow:
		err = a.row()
		if err == nil && a.pendingFamily == 1 {
			a.phase = inventoryCommit
			return false, nil
		}
	case inventoryPoint:
		err = a.paired(ctx)
		if err == nil && a.pendingFamily == 3 {
			a.phase = inventoryDrop
			return false, nil
		}
	case inventoryStored:
		err = a.storedPair()
	case inventoryDrop:
		// Pending fixed ref/key/cost are now complete; no page/pair survives history.
		a.page, a.pair = raftlog.ApplicationControlPage{}, raftlog.ApplicationControlRecord{}
	case inventoryRowRoot:
		err = a.rootAt(ctx, a.ref.position)
	case inventoryRowChange:
		err = a.envelope(ctx, a.ref.position, false, 0)
	case inventoryRowOutcome:
		err = a.envelope(ctx, a.ref.position, true, 2)
	case inventoryRowTerm:
		if a.store != nil {
			err = registeredKnownTerm(a.store, a.ref.position, a.ref.term)
		}
	case inventoryCommit:
		err = a.commitRow()
		if err == nil {
			a.phase = inventoryPage
			return false, nil
		}
	case inventoryEOF:
		return true, nil
	default:
		return false, errInvalid
	}
	if err == nil {
		a.phase++
	}
	return false, err
}
func (a *registeredInventoryAudit) Finish(ctx context.Context, b registeredInventoryBudget) (*registeredInventorySummary, error) {
	if a == nil || a.self != a || ctx == nil {
		return nil, errInvalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, raftlog.ErrClosed
	}
	if a.failed != nil {
		return nil, a.failed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.phase != inventoryEOF {
		return nil, errInvalid
	}
	if err := a.need(b, uint64(a.contract.MaxPageBytes)+1024+280, uint64(a.contract.MaxPageBytes)+140); err != nil { // #nosec G115 -- genuine constructors validate and privately capture MaxPageBytes to1..64MiB before Finish.
		return nil, err
	}
	usage, err := a.view.ControlInventoryUsage(ctx)
	if err != nil {
		return nil, err
	}
	if usage != a.usage {
		return nil, errCorrupt
	}
	if a.seeds != 1 || a.lookups != a.stored || a.lookups > (math.MaxUint64-1)/2 || a.controlRecords != 1+2*a.lookups || a.controlBytes != a.usage.Bytes || a.controlRecords != a.usage.Records {
		return nil, errCorrupt
	}
	if a.store != nil {
		proof, err := a.view.ProveNoApplicationData(ctx)
		if err != nil {
			return nil, err
		}
		if !sameBase(proof, a.base) {
			return nil, errCorrupt
		}
	} else {
		page, err := a.view.Scan(ctx, nil, nil, nil, raftlog.ReadBudget{Rows: 1, Bytes: a.contract.MaxPageBytes})
		if err != nil {
			return nil, err
		}
		if !page.Complete || page.Visited != 0 || page.Rows != nil || page.Next != nil {
			return nil, errCorrupt
		}
	}
	if a.summary == nil {
		root := a.base
		root.Image = make([]byte, len(a.base.Image))
		copy(root.Image, a.base.Image)
		a.summary = &registeredInventorySummary{owner: a, prepared: a.prepared, base: root, origin: a.origin, controlBytes: a.controlBytes, controlRecords: a.controlRecords, graphBytes: a.usage.TotalBytes - a.usage.Bytes, graphRecords: a.usage.TotalRecords - a.usage.Records, pairs: a.lookups, maxOriginalTerm: a.maxOriginalTerm, expected: a.expected}
	}
	return a.summary, nil
}
func (a *registeredInventoryAudit) Close() error {
	if a == nil || a.self != a {
		return errInvalid
	}
	a.mu.Lock()
	if a.closed {
		done := a.closeDone
		a.mu.Unlock()
		<-done
		a.mu.Lock()
		err := a.closeErr
		a.mu.Unlock()
		return err
	}
	a.closed = true
	v := a.view
	a.view = nil
	a.base.Image, a.cursor, a.pendingKey = nil, nil, nil
	a.page, a.pair = raftlog.ApplicationControlPage{}, raftlog.ApplicationControlRecord{}
	a.mu.Unlock()
	// Detach under audit lock; actual owner release never runs under that lock.
	err := v.Close()
	a.mu.Lock()
	a.closeErr = err
	close(a.closeDone)
	a.mu.Unlock()
	return err
}
