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

// Unique property constraints (ADR-0002), CORE stages a/b/c.
//
// A unique constraint forbids two CURRENT nodes carrying the same value for a
// constrained (label, property) (UniqueCurrent scope). History may hold
// duplicates; a freed value is immediately reusable. Persistence mirrors
// asof_tags: one msgpack map under a MetaKV key, decoded via SafeUnmarshal (fail
// closed), reaped by Admin().Reset, ErrCapabilityNotSupported without MetaKV,
// rejected on a read-only replica. Enforcement covers the standalone node doors;
// batch / tx / import enforcement is a follow-up.

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
// ErrCapabilityNotSupported. Tiered is accepted (ADR-0005 3.5 supersedes ADR-0002
// Decision 5): reference-label constraints enforce on the reference shard and the
// registry rides refShard's MetaKV; event labels are refused up front in createUnique.
func (c *Core) uniqueMetaKV() (storepkg.MetaKVCapability, error) {
	mk := c.metaKV
	if mk == nil {
		return nil, fmt.Errorf("graph: unique constraints: %w", storepkg.ErrCapabilityNotSupported)
	}
	return mk, nil
}

// rejectTieredEventLabel returns ErrUniqueEventLabelUnsupported when the store is
// tiered AND the label is event-class (the router's own classifier). Called BEFORE
// any label token is minted so a rejected create leaves no orphan token.
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

// isFloatValueKey reports whether a canonical value key encodes a float (rejected
// for unique constraints, ADR-0002).
func isFloatValueKey(valueKey string) bool {
	return strings.HasPrefix(valueKey, "f32:") || strings.HasPrefix(valueKey, "f64:")
}

// uniqueValueStripe computes the value-lock stripe. The property key is folded into
// the hashed bytes, so the stripe does not depend on key tokenization timing.
func uniqueValueStripe(labelTok uint16, key, valueKey string) uint8 {
	b := make([]byte, 0, len(key)+1+len(valueKey))
	b = append(b, key...)
	b = append(b, 0x1f)
	b = append(b, valueKey...)
	// keyToken 0: the key is already in the hash, see uniqueValueStripe.
	return locks.ValueStripe(labelTok, 0, b)
}

// --- Load / persist ---

// loadUniqueConstraints rehydrates the registry from MetaKV at open; a missing key
// or no MetaKV gives an empty registry, corrupt bytes fail closed.
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
			// A persisted constraint whose label token is gone means the registries diverged
			// on disk: fail closed rather than drop enforcement.
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

// storeUniqueConstraintsLocked persists all ACTIVE constraints; caller holds c.uniqueMu.
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

// --- In-memory registry helpers (caller holds c.uniqueMu for *Locked variants) ---

// refuseUniqueKey returns ErrElementsIndexUnsupported when (label, key) is
// under a unique constraint: an Elements index (backlog 63) cannot serve it.
func (c *Core) refuseUniqueKey(label, key string) error {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil
	}
	c.uniqueMu.RLock()
	_, exists := c.lookupConstraintLocked(tok, key)
	c.uniqueMu.RUnlock()
	if exists {
		return fmt.Errorf("%w: key %q of label %q has a unique constraint", storepkg.ErrElementsIndexUnsupported, key, label)
	}
	return nil
}

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

// --- CreateUnique / DropUnique / UniqueConstraints ---

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
	// Tiered: reject event-class labels up front (no global value index across time shards).
	if err := c.rejectTieredEventLabel(label); err != nil {
		return err
	}
	mk, err := c.uniqueMetaKV()
	if err != nil {
		return err
	}

	// Resolve (and persist) the label token before validating existing data
	// (ADR-0002 verifier note 2). Safe: a token is only allocated for a label absent
	// from the registry, which has no nodes, so validateUniqueExisting cannot fail
	// for it and a failed create leaves no orphan token. The pending overlay keys to it.
	// Persisted so the record round-trips through Lookup at reopen
	// (loadUniqueConstraints fails closed on a missing label).
	labelTok, err := c.getOrCreateLabelPersisted(label)
	if err != nil {
		return err
	}

	// Phase 1: install a PENDING entry so concurrent writers enforce during validation.
	// The install runs under c.mu.Lock() (BACKLOG 9c): a standalone write holds
	// c.mu.RLock() from its unique check through the store write, and a check made
	// while hasUniqueConstraints() was false took no stripe lock. The exclusive lock
	// waits for every such in-flight write, so Phase 2's scan observes it; later
	// writes see PENDING and self-enforce. Released before Phase 2, which takes its
	// own RLock.
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return ErrGraphClosed
	}
	// An Elements index (backlog 63) keys list elements, not values: no uniqueness
	// over it. Checked under the exclusive lock: CreatePropertyWithOptions holds
	// c.mu.RLock for its whole create, so no Elements index can appear before the
	// install, and a later one sees the pending constraint (refuseUniqueKey).
	if err := c.refuseNodeElementsIndex(labelTok, propKey, "unique constraint"); err != nil {
		c.mu.Unlock()
		return err
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

	// Auto-ensure a property index as acceleration for enforcement lookups; correct
	// without it (label scan), so ErrIndexExists is benign.
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

	// Phase 3: activate + persist. BACKLOG 13j: on persist failure the uninstall
	// happens inside the same critical section (uniqueMu never released between), so
	// no reader sees st.active=true for a constraint that was never durable.
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

	// UniqueForever: seed ownership from current values (validation guaranteed no
	// current duplicates). A seed failure uninstalls the just-activated constraint.
	if scope == constraintspkg.UniqueForever {
		if err := c.seedForeverOwnersFromCurrent(labelTok, propKey); err != nil {
			c.uninstallActiveConstraint(labelTok, propKey, mk)
			return err
		}
	}
	return nil
}

// uninstallActiveConstraint removes an active constraint and re-persists
// (best-effort); unwinds a failed post-activation seed.
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
			vk, found := uniqueValueKey(n, key)
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
	// Dropping a UniqueForever constraint releases its ownership claims so a re-create
	// seeds cleanly (a dead ID would otherwise bar a value forever).
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

// --- Enforcement kernel (Stage C) ---

// enforceUniqueForNode is consulted by the standalone create/update/CAS/label
// doors after the final node state is built and BEFORE the store write. It holds
// the value stripe(s) of every constrained value the node binds across the index
// lookup and (via the returned hold) the store write, so racing claimers
// serialize to one winner. prev (may be nil) is the pre-mutation state: an
// update that changes a constrained value also holds the OLD value's stripe.
//
// Returns a no-op hold when no constraints exist. On a violation, float value or
// failed claim it withdraws its claims, releases everything and returns the
// error; the caller must NOT write. On success the caller defers hold.release()
// and answers a failed store write with hold.storeWriteFailed(err) first.
func (c *Core) enforceUniqueForNode(node *types.Node, prev *types.Node, selfID types.NodeID) (*uniqueHold, error) {
	return c.enforceUniqueForNodeHeld(node, prev, selfID, nil)
}

// enforceUniqueForNodeHeld is enforceUniqueForNode with stripes the caller
// ALREADY holds (`held`): not re-locked (stripe mutexes are not reentrant) but
// their values are still checked, safe because the held stripe serializes any
// value mapping to it. GetOrCreateByKey uses it. The hold releases only the
// stripes this call locked.
func (c *Core) enforceUniqueForNodeHeld(node *types.Node, prev *types.Node, selfID types.NodeID, held []uint8) (*uniqueHold, error) {
	noop := &uniqueHold{}
	if node == nil || !c.hasUniqueConstraints.Load() {
		return noop, nil
	}

	var checks []uniqueCheckTuple
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
			valueKey, found := uniqueValueKey(node, key)
			if !found || valueKey == "" {
				continue // unconstrained (missing key or unindexable value)
			}
			if unchangedConstrainedValue(prev, node, labelTok, key) {
				// The write leaves the value alone: nothing to check or claim,
				// so a legacy width twin (int(3) beside int64(3), stored before
				// v4.57.1) keeps accepting writes to its other properties.
				continue
			}
			if isFloatValueKey(valueKey) {
				c.uniqueMu.RUnlock()
				return noop, fmt.Errorf("%w: label %q key %q holds a float value", ErrUniqueUnsupportedType, c.labels.Resolve(labelTok), key)
			}
			raw, _ := node.GetProperty(key)
			checks = append(checks, uniqueCheckTuple{labelTok: labelTok, key: key, raw: raw, valueKey: valueKey, scope: st.scope})
			stripes = append(stripes, uniqueValueStripe(labelTok, key, valueKey))
			if prev != nil {
				if oldKey, ok := uniqueValueKey(prev, key); ok && oldKey != "" && oldKey != valueKey {
					stripes = append(stripes, uniqueValueStripe(labelTok, key, oldKey))
				}
			}
		}
	}
	c.uniqueMu.RUnlock()

	if len(checks) == 0 {
		return noop, nil
	}
	// Deterministic claim order (the cascade's), independent of map iteration.
	sort.Slice(checks, func(i, j int) bool {
		return uniqueSeenKey(checks[i].labelTok, checks[i].key, checks[i].valueKey) <
			uniqueSeenKey(checks[j].labelTok, checks[j].key, checks[j].valueKey)
	})

	hold := &uniqueHold{c: c, id: selfID, held: c.valueLocks.LockStripesExcept(stripes, held)}

	// Pass 1: validate EVERY tuple read-only before claiming anything, so a refusal on
	// a later tuple leaves no earlier claim behind (BACKLOG 9e). Same check as the
	// dry-run door (checkForeverOwnership); the held stripes keep these values
	// unclaimable by others until Pass 2.
	for _, ck := range checks {
		matches, err := c.nodesByLabelAndProperty(ck.labelTok, ck.key, ck.raw, storepkg.QueryOpts{})
		if err != nil {
			hold.release()
			return noop, fmt.Errorf("graph: unique constraint lookup: %w", err)
		}
		for _, m := range matches {
			if m.ID() == selfID {
				continue // the node's own current index entry
			}
			winner := m.ID()
			hold.release()
			return noop, fmt.Errorf("%w: label %q key %q already held by node %d",
				ErrUniqueViolation, c.labels.Resolve(ck.labelTok), ck.key, winner)
		}
		if ck.scope == constraintspkg.UniqueForever {
			if err := c.checkForeverOwnership(ck.labelTok, ck.key, ck.valueKey, selfID); err != nil {
				hold.release()
				return noop, err
			}
		}
	}
	// Pass 2: claim each UniqueForever value (other owner => violation; same entity
	// => pass, not recorded; miss => claim + persist, recorded in the hold). A failed
	// claim (persist, item 29) withdraws this pass's claims, judged against the
	// node's stored row, while the stripes are still held.
	for _, ck := range checks {
		if ck.scope != constraintspkg.UniqueForever {
			continue
		}
		if err := hold.claim(ck); err != nil {
			err = hold.storeWriteFailed(err)
			hold.release()
			return noop, err
		}
	}
	return hold, nil
}

// --- ConstraintOps surface (g.Constraints()) ---

// CreateUnique registers a unique constraint on (label, propertyKey) with the
// default UniqueCurrent scope. Install is 3-phase: a PENDING placeholder is
// installed BEFORE existing-data validation, so writes racing CreateUnique are
// rejected as if active and no duplicate slips in; if validation fails the
// placeholder is removed.
func (co *ConstraintOps) CreateUnique(ctx context.Context, label, propertyKey string) error {
	return co.c.createUnique(ctx, label, propertyKey, constraintspkg.UniqueCurrent)
}

// CreateUniqueForever registers a UniqueForever constraint on (label,
// propertyKey): the FIRST entity to hold a value owns it permanently (any later
// version of it may hold it; everyone else is barred across supersession, hard
// delete and reopen). ADR-0002 Decision 2.
//
// The claim is made under the value lock and persisted immediately. A call that
// then fails withdraws its claims under the same lock unless the node's stored
// row carries the value (uniqueHold, unique_hold.go). A claim made in a tx that
// ROLLS BACK is NOT auto-released (not part of the tx snapshot): the value stays
// barred until Constraints().ReleaseOwnership. UniqueCurrent claims are freed on
// rollback.
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

// uniqueValueKey is the key a unique constraint holds e's key value under:
// its index key with an integer of any width mapped to the one key its equal
// integers share (types.CanonicalIndexKey), so int(3) and int64(3) are one
// value, as the equality lookups the constraint checks with find them.
func uniqueValueKey(e interface {
	IndexablePropertyValueKey(string) (string, bool)
}, key string) (string, bool) {
	vk, found := e.IndexablePropertyValueKey(key)
	return types.CanonicalIndexKey(vk), found
}

// unchangedConstrainedValue reports whether prev already carried the label
// and node's key value in the same width (identical index key).
func unchangedConstrainedValue(prev, node *types.Node, labelTok uint16, key string) bool {
	if prev == nil || !prev.HasLabelTokenRaw(labelTok) {
		return false
	}
	prevKey, ok := prev.IndexablePropertyValueKey(key)
	if !ok || prevKey == "" {
		return false
	}
	curKey, _ := node.IndexablePropertyValueKey(key)
	return curKey == prevKey
}
