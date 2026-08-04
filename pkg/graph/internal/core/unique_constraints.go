package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/vmihailenco/msgpack/v5"

	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/locks"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Unique property constraints (ADR-0002) — CORE stages a/b/c.
//
// A unique constraint forbids two CURRENT nodes carrying the same value for a
// constrained (label, property) pair (UniqueCurrent scope — the only scope
// implemented in this release). History may hold duplicates; a value freed by
// supersession or delete is immediately reusable.
//
// Persistence mirrors the asof_tags pattern exactly: one msgpack map under a
// MetaKV key, decoded through SafeUnmarshal (fail closed), durable across
// reopen, reaped by Admin().Reset, declines without MetaKV via
// ErrCapabilityNotSupported, rejected on a read-only replica.
//
// Enforcement (Stage C) currently covers the STANDALONE node doors; batch / tx
// / import enforcement is a follow-up wave (landing before any release tag).
// =============================================================================

// uniqueConstraintsMeta is the MetaKV key holding the durable unique-constraint
// registry. Distinct from asofTagsMeta / graphEpochMeta / replicaAppliedLSNMeta.
const uniqueConstraintsMeta = "unique_constraints"

// maxUniqueConstraints bounds the durable registry (keeps the meta blob small).
const maxUniqueConstraints = 4096

// maxUniqueExistingOffenders caps how many duplicate IDs CreateUnique lists in
// an ErrUniqueViolationExisting error.
const maxUniqueExistingOffenders = 5

// uniqueConstraintState is the in-memory registry entry.
type uniqueConstraintState struct {
	label  string
	scope  constraintspkg.UniqueScope
	active bool // false = pending (still enforced), true = validated + persisted
}

// uniqueConstraintRecord is the durable (msgpack) shape. Only active
// constraints are persisted, so a decoded record is always active.
type uniqueConstraintRecord struct {
	Label   string `msgpack:"label"`
	PropKey string `msgpack:"prop"`
	Scope   uint8  `msgpack:"scope"`
}

func uniqueConstraintCompositeKey(label, propKey string) string {
	return label + "\x00" + propKey
}

// uniqueMetaKV returns the store's MetaKV capability or a wrapped
// ErrCapabilityNotSupported. The tiered backend is NO LONGER declined outright
// (ADR-0005 §3.5 supersedes ADR-0002 Decision 5): reference-label constraints
// enforce globally on the reference shard, and the durable registry rides
// refShard's MetaKV. Event-label constraints are rejected up front in
// createUnique with ErrUniqueEventLabelUnsupported, so they never reach the
// registry — the classification is the gate, not this capability probe.
func (c *Core) uniqueMetaKV() (storepkg.MetaKVCapability, error) {
	mk := c.metaKV
	if mk == nil {
		return nil, fmt.Errorf("graph: unique constraints: %w", storepkg.ErrCapabilityNotSupported)
	}
	return mk, nil
}

// rejectTieredEventLabel returns ErrUniqueEventLabelUnsupported when the store is
// tiered AND the constrained label is event-class. Reference labels (and every
// non-tiered backend) pass through. Classification anchors to the tiered store's
// own ontology (the SAME classifier the router uses), so ref/event is consistent
// with where the label's entities live. Called BEFORE any label token is minted
// so a rejected event-label create never leaves an orphan token behind.
func (c *Core) rejectTieredEventLabel(label string) error {
	ts, ok := c.store.(*tiered.Store)
	if !ok {
		return nil
	}
	if !ts.IsReferenceLabel(label) {
		return fmt.Errorf("%w: label %q (values span unbounded time shards)", ErrUniqueEventLabelUnsupported, label)
	}
	return nil
}

// isFloatValueKey reports whether a canonical index value key encodes a
// floating-point value (rejected for unique constraints — bit-pattern equality
// is user-hostile, ADR-0002 default).
func isFloatValueKey(valueKey string) bool {
	return strings.HasPrefix(valueKey, "f32:") || strings.HasPrefix(valueKey, "f64:")
}

// uniqueValueStripe computes the value-lock stripe for a constrained value.
// The property key is folded into the hashed bytes (rather than passed as a
// registry token) so the stripe is stable regardless of property-key
// tokenization timing.
func uniqueValueStripe(labelTok uint16, key, valueKey string) uint8 {
	b := make([]byte, 0, len(key)+1+len(valueKey))
	b = append(b, key...)
	b = append(b, 0x1f)
	b = append(b, valueKey...)
	// keyToken 0: the property key is already folded into the hashed bytes, so a
	// stable stripe does not depend on property-key tokenization timing.
	return locks.ValueStripe(labelTok, 0, b)
}

// -----------------------------------------------------------------------------
// Load / persist
// -----------------------------------------------------------------------------

// loadUniqueConstraints rehydrates the in-memory registry from MetaKV at open.
// A missing key / no MetaKV yields an empty registry. Corrupt bytes fail closed.
func (c *Core) loadUniqueConstraints() error {
	c.uniqueConstraints = make(map[uint16]map[string]*uniqueConstraintState)
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	recs, err := loadUniqueConstraintRecords(mk)
	if err != nil {
		return err
	}
	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	for _, r := range recs {
		tok, ok := c.labels.Lookup(r.Label)
		if !ok {
			// A persisted constraint whose label token is gone means the label
			// registry and the constraint registry diverged on disk — fail
			// closed rather than silently drop enforcement.
			return fmt.Errorf("graph: load unique constraints: %w: label %q not in registry", storepkg.ErrCorruptWire, r.Label)
		}
		c.installConstraintLocked(tok, r.PropKey, r.Label, constraintspkg.UniqueScope(r.Scope), true)
	}
	return nil
}

func loadUniqueConstraintRecords(mk storepkg.MetaKVCapability) ([]uniqueConstraintRecord, error) {
	v, err := mk.MetaGet(uniqueConstraintsMeta)
	if err != nil {
		return nil, fmt.Errorf("graph: read unique constraints: %w", err)
	}
	if len(v) == 0 {
		return nil, nil
	}
	m := make(map[string]uniqueConstraintRecord)
	if err := storeutil.SafeUnmarshal(v, &m); err != nil {
		return nil, fmt.Errorf("graph: decode unique constraints: %w", err)
	}
	recs := make([]uniqueConstraintRecord, 0, len(m))
	for _, r := range m {
		if strings.TrimSpace(r.Label) == "" || strings.TrimSpace(r.PropKey) == "" {
			return nil, fmt.Errorf("graph: decode unique constraints: %w: blank label or key", storepkg.ErrCorruptWire)
		}
		recs = append(recs, r)
	}
	return recs, nil
}

// storeUniqueConstraintsLocked persists all ACTIVE constraints. Caller holds
// c.uniqueMu.
func (c *Core) storeUniqueConstraintsLocked(mk storepkg.MetaKVCapability) error {
	m := make(map[string]uniqueConstraintRecord)
	for _, byKey := range c.uniqueConstraints {
		for key, st := range byKey {
			if !st.active {
				continue
			}
			m[uniqueConstraintCompositeKey(st.label, key)] = uniqueConstraintRecord{
				Label:   st.label,
				PropKey: key,
				Scope:   uint8(st.scope),
			}
		}
	}
	b, err := msgpack.Marshal(m)
	if err != nil {
		return fmt.Errorf("graph: encode unique constraints: %w", err)
	}
	if err := mk.MetaSet(uniqueConstraintsMeta, b); err != nil {
		return fmt.Errorf("graph: persist unique constraints: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// In-memory registry helpers (caller holds c.uniqueMu for the *Locked variants)
// -----------------------------------------------------------------------------

func (c *Core) lookupConstraintLocked(labelTok uint16, key string) (*uniqueConstraintState, bool) {
	byKey, ok := c.uniqueConstraints[labelTok]
	if !ok {
		return nil, false
	}
	st, ok := byKey[key]
	return st, ok
}

func (c *Core) installConstraintLocked(labelTok uint16, key, label string, scope constraintspkg.UniqueScope, active bool) {
	byKey, ok := c.uniqueConstraints[labelTok]
	if !ok {
		byKey = make(map[string]*uniqueConstraintState)
		c.uniqueConstraints[labelTok] = byKey
	}
	byKey[key] = &uniqueConstraintState{label: label, scope: scope, active: active}
	c.hasUniqueConstraints.Store(true)
}

func (c *Core) removeConstraintLocked(labelTok uint16, key string) {
	byKey, ok := c.uniqueConstraints[labelTok]
	if !ok {
		return
	}
	delete(byKey, key)
	if len(byKey) == 0 {
		delete(c.uniqueConstraints, labelTok)
	}
	c.refreshHasUniqueLocked()
}

func (c *Core) refreshHasUniqueLocked() {
	c.hasUniqueConstraints.Store(len(c.uniqueConstraints) > 0)
}

func (c *Core) uninstallPendingConstraint(labelTok uint16, key string) {
	c.uniqueMu.Lock()
	c.removeConstraintLocked(labelTok, key)
	c.uniqueMu.Unlock()
}

func (c *Core) totalConstraintsLocked() int {
	n := 0
	for _, byKey := range c.uniqueConstraints {
		n += len(byKey)
	}
	return n
}

// -----------------------------------------------------------------------------
// CreateUnique / DropUnique / UniqueConstraints
// -----------------------------------------------------------------------------

func (c *Core) createUnique(ctx context.Context, label, propKey string, scope constraintspkg.UniqueScope) error {
	if err := c.checkWritable(); err != nil {
		return err
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}
	switch scope {
	case constraintspkg.UniqueCurrent, constraintspkg.UniqueForever:
		// implemented
	default:
		return fmt.Errorf("%w: scope %s is not yet implemented", ErrUniqueUnsupportedType, scope)
	}
	if err := c.validateName(label); err != nil {
		return err
	}
	if err := c.validateIndexPropertyKey(propKey); err != nil {
		return err
	}
	// Tiered: reject event-class labels up front (before minting a token) — their
	// values are spread across unbounded time shards with no global value index.
	if err := c.rejectTieredEventLabel(label); err != nil {
		return err
	}
	mk, err := c.uniqueMetaKV()
	if err != nil {
		return err
	}

	// Resolve (and persist) the label token so future writes carrying the label
	// bind to it and the durable record can round-trip through Lookup at reopen.
	//
	// This resolves the token BEFORE validating existing data (ADR-0002 verifier
	// note 2). Minting up front is safe: getOrCreateLabelPersisted only ALLOCATES
	// a new token when the label is absent from the registry, and an absent label
	// has no nodes — so validateUniqueExisting below cannot find offenders or a
	// float value for it. A validation FAILURE therefore only happens for a label
	// that already existed (whose token is not newly minted), so a failed create
	// never leaves an orphan token behind. Keeping the token resolved up front
	// also lets the pending overlay key its enforcement to it (Phase 1) so
	// concurrent writers enforce during the validation window.
	labelTok, err := c.getOrCreateLabelPersisted(label)
	if err != nil {
		return err
	}

	// Phase 1: install a PENDING entry under lock so concurrent writers already
	// enforce it while we validate existing data.
	//
	// The install itself also runs under c.mu.Lock() (BACKLOG 9c): a
	// standalone write's addNodeInternal holds c.mu.RLock() for its ENTIRE
	// duration (enforceUniqueForNodeHeld's check through the actual store
	// write). If that check ran while hasUniqueConstraints() was still false,
	// it took no value-stripe lock — so without this, such a write could
	// still be in flight when installConstraintLocked runs, then commit its
	// duplicate AFTER Phase 3 activates the constraint, with nothing having
	// ever enforced it. Taking c.mu.Lock() here forces the install to wait
	// until every already-in-flight write (holding c.mu.RLock()) has fully
	// committed, so Phase 2's unlocked scan below is guaranteed to observe
	// it; any write starting after this point sees the PENDING entry and
	// self-enforces via the stripe lock. Released before Phase 2 (which
	// takes its own c.mu.RLock() internally via validateUniqueExisting) —
	// only the install itself needs the exclusion, not the whole scan.
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return ErrGraphClosed
	}
	c.uniqueMu.Lock()
	if _, exists := c.lookupConstraintLocked(labelTok, propKey); exists {
		c.uniqueMu.Unlock()
		c.mu.Unlock()
		return fmt.Errorf("%w: label %q key %q", ErrUniqueConstraintExists, label, propKey)
	}
	if c.totalConstraintsLocked() >= maxUniqueConstraints {
		c.uniqueMu.Unlock()
		c.mu.Unlock()
		return fmt.Errorf("%w: at capacity (%d)", ErrUniqueConstraintExists, maxUniqueConstraints)
	}
	c.installConstraintLocked(labelTok, propKey, label, scope, false)
	c.uniqueMu.Unlock()
	c.mu.Unlock()

	// Auto-ensure a property index on (label, propKey) — an acceleration for the
	// enforcement lookup (ADR default). Correctness holds without it (the store
	// falls back to a label scan), so ErrIndexExists is benign.
	if err := c.Index.CreateProperty(label, propKey); err != nil && !errors.Is(err, storepkg.ErrIndexExists) {
		c.uninstallPendingConstraint(labelTok, propKey)
		return fmt.Errorf("graph: unique constraint: ensure property index: %w", err)
	}

	// Phase 2: validate existing data (unlocked writers still enforce pending).
	offenders, unsupported, scanErr := c.validateUniqueExisting(labelTok, propKey)
	if scanErr != nil {
		c.uninstallPendingConstraint(labelTok, propKey)
		return scanErr
	}
	if unsupported {
		c.uninstallPendingConstraint(labelTok, propKey)
		return fmt.Errorf("%w: label %q key %q holds a float value", ErrUniqueUnsupportedType, label, propKey)
	}
	if len(offenders) > 0 {
		c.uninstallPendingConstraint(labelTok, propKey)
		return fmt.Errorf("%w: label %q key %q; offenders %v", ErrUniqueViolationExisting, label, propKey, offenders)
	}

	// Phase 3: activate + persist. BACKLOG 13j: on a persist failure, the
	// uninstall happens INSIDE this same critical section (never releasing
	// uniqueMu between the failed persist and the removal), so no concurrent
	// reader can ever observe st.active=true for a constraint whose
	// activation is about to be rolled back — the prior code unlocked
	// uniqueMu before checking the persist error, then re-acquired it in
	// uninstallPendingConstraint, leaving a narrow window where a concurrent
	// writer could see (and be transiently rejected by) a constraint that
	// was never actually durable.
	c.uniqueMu.Lock()
	st, ok := c.lookupConstraintLocked(labelTok, propKey)
	if !ok {
		// A concurrent DropUnique removed the pending entry; nothing to activate.
		c.uniqueMu.Unlock()
		return nil
	}
	st.active = true
	err = c.storeUniqueConstraintsLocked(mk)
	if err != nil {
		c.removeConstraintLocked(labelTok, propKey)
		c.uniqueMu.Unlock()
		return err
	}
	c.uniqueMu.Unlock()

	// UniqueForever: seed ownership from the current values so existing entities
	// own the values they already hold (validation above guaranteed no current
	// duplicates). A seed failure uninstalls the just-activated constraint.
	if scope == constraintspkg.UniqueForever {
		if err := c.seedForeverOwnersFromCurrent(labelTok, propKey); err != nil {
			c.uninstallActiveConstraint(labelTok, propKey, mk)
			return err
		}
	}
	return nil
}

// uninstallActiveConstraint removes an active constraint and re-persists the
// registry (best-effort) — used to unwind a constraint whose post-activation
// seeding failed.
func (c *Core) uninstallActiveConstraint(labelTok uint16, key string, mk storepkg.MetaKVCapability) {
	c.uniqueMu.Lock()
	c.removeConstraintLocked(labelTok, key)
	_ = c.storeUniqueConstraintsLocked(mk)
	c.uniqueMu.Unlock()
}

// validateUniqueExisting scans current nodes carrying labelTok and reports
// duplicate (offenders, capped) and whether any value is an unsupported float.
func (c *Core) validateUniqueExisting(labelTok uint16, key string) (offenders []types.NodeID, unsupported bool, err error) {
	seen := make(map[string]types.NodeID)
	dup := make(map[types.NodeID]struct{})
	scanErr := c.readUnderRLock(func() error {
		nodes, e := c.store.NodesByLabel(labelTok, storepkg.QueryOpts{})
		if e != nil {
			return e
		}
		for _, n := range nodes {
			vk, found := n.IndexablePropertyValueKey(key)
			if !found || vk == "" {
				continue // node does not carry an indexable value → unconstrained
			}
			if isFloatValueKey(vk) {
				unsupported = true
				return nil
			}
			if first, ok := seen[vk]; ok {
				dup[first] = struct{}{}
				dup[n.ID()] = struct{}{}
			} else {
				seen[vk] = n.ID()
			}
		}
		return nil
	})
	if scanErr != nil {
		return nil, false, scanErr
	}
	if unsupported {
		return nil, true, nil
	}
	if len(dup) > 0 {
		offenders = make([]types.NodeID, 0, len(dup))
		for id := range dup {
			offenders = append(offenders, id)
		}
		sort.Slice(offenders, func(i, j int) bool { return offenders[i] < offenders[j] })
		if len(offenders) > maxUniqueExistingOffenders {
			offenders = offenders[:maxUniqueExistingOffenders]
		}
	}
	return offenders, false, nil
}

func (c *Core) dropUnique(ctx context.Context, label, propKey string) error {
	if err := c.checkWritable(); err != nil {
		return err
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}
	if err := c.validateName(label); err != nil {
		return err
	}
	if err := c.validateIndexPropertyKey(propKey); err != nil {
		return err
	}
	mk, err := c.uniqueMetaKV()
	if err != nil {
		return err
	}
	labelTok, ok := c.labels.Lookup(label)
	if !ok {
		return fmt.Errorf("%w: label %q key %q", ErrUniqueConstraintNotFound, label, propKey)
	}

	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	st, exists := c.lookupConstraintLocked(labelTok, propKey)
	if !exists {
		return fmt.Errorf("%w: label %q key %q", ErrUniqueConstraintNotFound, label, propKey)
	}
	removed := *st
	c.removeConstraintLocked(labelTok, propKey)
	if err := c.storeUniqueConstraintsLocked(mk); err != nil {
		// Reinstall so the in-memory registry never diverges from disk.
		c.installConstraintLocked(labelTok, propKey, removed.label, removed.scope, removed.active)
		return err
	}
	// Dropping a UniqueForever constraint releases all its ownership claims, so a
	// later re-create seeds cleanly from current state rather than inheriting
	// stale owners (a dead ID would otherwise bar a value forever).
	if removed.scope == constraintspkg.UniqueForever {
		if err := c.purgeForeverOwnersLocked(labelTok, propKey, mk); err != nil {
			return err
		}
	}
	return nil
}

func (c *Core) uniqueConstraintsList() []constraintspkg.UniqueConstraint {
	c.uniqueMu.RLock()
	defer c.uniqueMu.RUnlock()
	out := make([]constraintspkg.UniqueConstraint, 0)
	for _, byKey := range c.uniqueConstraints {
		for key, st := range byKey {
			if !st.active {
				continue // pending constraints are not surfaced for introspection
			}
			out = append(out, constraintspkg.UniqueConstraint{
				Label:       st.label,
				PropertyKey: key,
				Scope:       st.scope,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Label != out[j].Label {
			return out[i].Label < out[j].Label
		}
		return out[i].PropertyKey < out[j].PropertyKey
	})
	return out
}

// reapUniqueConstraintsForReset clears the durable + in-memory registry as part
// of Admin.Reset (mirrors reapAsOfTagsForReset). No-op without MetaKV.
func (c *Core) reapUniqueConstraintsForReset() error {
	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	c.uniqueConstraints = make(map[uint16]map[string]*uniqueConstraintState)
	c.hasUniqueConstraints.Store(false)
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	if err := mk.MetaSet(uniqueConstraintsMeta, nil); err != nil {
		return fmt.Errorf("graph: reset unique constraints: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Enforcement kernel (Stage C)
// -----------------------------------------------------------------------------

// enforceUniqueForNode is consulted by the standalone create/update/CAS/label
// doors after the finalized node state is built and BEFORE the store write. It
// holds the value stripe(s) for every constrained value the node binds across
// the index lookup and (via the returned release) the caller's store write, so
// two writers racing to claim the same value serialize to exactly one winner.
//
// prev (may be nil) is the pre-mutation state: when an update changes a
// constrained value, the OLD value's stripe is also held so a concurrent
// claimer of the freed value serializes with this write (ADR lock rule).
//
// Returns (noop, nil) fast when no constraints exist. On a violation or
// unsupported (float) value it releases everything and returns the error; the
// caller must NOT proceed with the write. On success the caller defers release.
func (c *Core) enforceUniqueForNode(node *types.Node, prev *types.Node, selfID types.NodeID) (func(), error) {
	return c.enforceUniqueForNodeHeld(node, prev, selfID, nil)
}

// enforceUniqueForNodeHeld is enforceUniqueForNode with a set of value stripes
// the caller ALREADY holds (`held`). Those stripes are NOT re-locked (a stripe
// mutex is not reentrant), but their constrained values are still checked — the
// caller's held stripe already serializes concurrent writers of any value that
// maps to it (including hash collisions), so the check runs safely under it.
// GetOrCreateByKey uses this to create a node while holding the keyed value's
// stripe without self-deadlocking.
func (c *Core) enforceUniqueForNodeHeld(node *types.Node, prev *types.Node, selfID types.NodeID, held []uint8) (func(), error) {
	noop := func() {}
	if node == nil || !c.hasUniqueConstraints.Load() {
		return noop, nil
	}

	type uniqueCheck struct {
		labelTok uint16
		key      string
		raw      any
		valueKey string
		scope    constraintspkg.UniqueScope
	}
	var checks []uniqueCheck
	var stripes []uint8

	c.uniqueMu.RLock()
	if len(c.uniqueConstraints) == 0 {
		c.uniqueMu.RUnlock()
		return noop, nil
	}
	labelCount := node.LabelTokenCount()
	for i := 0; i < labelCount; i++ {
		labelTok := node.LabelTokenRawAt(i)
		byKey, ok := c.uniqueConstraints[labelTok]
		if !ok {
			continue
		}
		for key, st := range byKey {
			valueKey, found := node.IndexablePropertyValueKey(key)
			if !found || valueKey == "" {
				continue // unconstrained (missing key or unindexable value)
			}
			if isFloatValueKey(valueKey) {
				c.uniqueMu.RUnlock()
				return noop, fmt.Errorf("%w: label %q key %q holds a float value", ErrUniqueUnsupportedType, c.labels.Resolve(labelTok), key)
			}
			raw, _ := node.GetProperty(key)
			checks = append(checks, uniqueCheck{labelTok: labelTok, key: key, raw: raw, valueKey: valueKey, scope: st.scope})
			stripes = append(stripes, uniqueValueStripe(labelTok, key, valueKey))
			if prev != nil {
				if oldKey, ok := prev.IndexablePropertyValueKey(key); ok && oldKey != "" && oldKey != valueKey {
					stripes = append(stripes, uniqueValueStripe(labelTok, key, oldKey))
				}
			}
		}
	}
	c.uniqueMu.RUnlock()

	if len(checks) == 0 {
		return noop, nil
	}

	ordered := c.valueLocks.LockStripesExcept(stripes, held)
	release := func() { c.valueLocks.UnlockStripes(ordered) }

	// Pass 1: validate EVERY tuple read-only before claiming anything. A node
	// can bind more than one constrained tuple (e.g. two UniqueForever keys,
	// or one UniqueForever + one UniqueCurrent) — claiming a UniqueForever
	// value as each tuple is checked, then failing on a LATER tuple, would
	// abort the whole create/update while leaving the earlier tuple's claim
	// durably persisted: the value becomes permanently owned by an entity
	// that never came into existence (BACKLOG 9e). checkForeverOwnership is
	// the same read-only check the dry-run door uses, so this pass can never
	// itself leave a claim behind. All of `checks`' value stripes are already
	// held for the whole call (LockStripesExcept above), so no concurrent
	// writer can claim any of these exact values between this pass and the
	// claim pass below.
	for _, ck := range checks {
		matches, err := c.nodesByLabelAndProperty(ck.labelTok, ck.key, ck.raw, storepkg.QueryOpts{})
		if err != nil {
			release()
			return noop, fmt.Errorf("graph: unique constraint lookup: %w", err)
		}
		for _, m := range matches {
			if m.ID() == selfID {
				continue // the node's own current index entry
			}
			winner := m.ID()
			release()
			return noop, fmt.Errorf("%w: label %q key %q already held by node %d",
				ErrUniqueViolation, c.labels.Resolve(ck.labelTok), ck.key, winner)
		}
		if ck.scope == constraintspkg.UniqueForever {
			if err := c.checkForeverOwnership(ck.labelTok, ck.key, ck.valueKey, selfID); err != nil {
				release()
				return noop, err
			}
		}
	}
	// Pass 2: every tuple passed its check — now durably claim each
	// UniqueForever value. Registry hit + different entity => violation
	// (only possible here via the same-tuple TOCTOU checkAndClaimForever
	// itself still guards against); same entity (any version) => pass; miss
	// => claim + persist. Barred forever, across delete and reopen.
	for _, ck := range checks {
		if ck.scope != constraintspkg.UniqueForever {
			continue
		}
		if err := c.checkAndClaimForever(ck.labelTok, ck.key, ck.valueKey, selfID); err != nil {
			release()
			return noop, err
		}
	}
	return release, nil
}

// -----------------------------------------------------------------------------
// ConstraintOps surface (g.Constraints())
// -----------------------------------------------------------------------------

// CreateUnique registers a unique property constraint on (label, propertyKey)
// with the default UniqueCurrent scope. See the constraints sub-API docs.
//
// The 3-phase install matters for concurrency: a PENDING placeholder is
// installed under lock BEFORE existing-data validation runs, so concurrent
// writers already enforce the constraint DURING the validation window — a write
// racing CreateUnique is rejected as if the constraint were fully active. This
// is by design: it prevents a duplicate slipping in between "existing data is
// clean" and "constraint is active". Only if validation itself fails is the
// pending placeholder removed and no enforcement remains.
func (co *ConstraintOps) CreateUnique(ctx context.Context, label, propertyKey string) error {
	return co.c.createUnique(ctx, label, propertyKey, constraintspkg.UniqueCurrent)
}

// CreateUniqueForever registers a UniqueForever value-ownership constraint on
// (label, propertyKey): the FIRST entity to hold a value owns it permanently —
// only that entity (any later version) may ever hold it, and every other node is
// barred forever, across supersession, hard delete, and reopen. See the
// constraints sub-API docs and ADR-0002 Decision 2.
//
// The ownership claim is made under the value lock and persisted immediately, so
// it is durable and race-free. One consequence: a claim made inside a
// transaction that later ROLLS BACK is NOT auto-released (the durable claim is
// not part of the tx snapshot) — the value stays barred (a dead entity ID owns
// it). This is conservative (never admits a duplicate); an operator frees such a
// value with Constraints().ReleaseOwnership. UniqueCurrent claims, by contrast,
// are freed automatically on rollback (removing the created node removes its
// index entry).
func (co *ConstraintOps) CreateUniqueForever(ctx context.Context, label, propertyKey string) error {
	return co.c.createUnique(ctx, label, propertyKey, constraintspkg.UniqueForever)
}

// ReleaseOwnership removes a UniqueForever ownership claim so an
// operator-corrected value may be reclaimed by a different entity. Idempotent
// (releasing an unowned value is a no-op). Returns ErrUniqueConstraintNotFound
// if no UniqueForever constraint exists on (label, propertyKey).
func (co *ConstraintOps) ReleaseOwnership(ctx context.Context, label, propertyKey string, value any) error {
	return co.c.releaseOwnership(ctx, label, propertyKey, value)
}

// DropUnique removes a unique property constraint (leaves any property index).
func (co *ConstraintOps) DropUnique(ctx context.Context, label, propertyKey string) error {
	return co.c.dropUnique(ctx, label, propertyKey)
}

// UniqueConstraints returns a snapshot of the registered unique constraints.
func (co *ConstraintOps) UniqueConstraints() []constraintspkg.UniqueConstraint {
	return co.c.uniqueConstraintsList()
}
