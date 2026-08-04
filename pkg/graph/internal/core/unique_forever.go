package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/vmihailenco/msgpack/v5"

	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// UniqueForever value ownership (ADR-0002 Stage F).
//
// A UniqueForever constraint gives a value to the FIRST entity that claims it:
// only that entity ID (any later version of the same node) may ever hold it,
// and every other node is barred forever — across supersession, hard delete,
// and reopen. Mechanics: a durable ownership registry, deliberately NOT derived
// from history, so it survives delete and compaction.
//
// Persistence mirrors asof_tags / unique_constraints: one msgpack blob under a
// MetaKV key, decoded via SafeUnmarshal (fail closed). The blob carries a
// self-hash (ADR risk note) so a corrupt / tampered blob fails closed rather
// than silently granting or revoking ownership.
// =============================================================================

// uniqueForeverOwnersMeta is the MetaKV key holding the durable ownership map.
const uniqueForeverOwnersMeta = "unique_forever_owners"

// foreverOwnerRecord is one durable ownership entry. Keyed by label STRING (not
// token) so the blob round-trips through Lookup at reopen like the constraint
// records.
type foreverOwnerRecord struct {
	Label    string `msgpack:"label"`
	PropKey  string `msgpack:"prop"`
	ValueKey string `msgpack:"vk"`
	Owner    int64  `msgpack:"owner"`
}

// foreverOwnersBlob is the durable envelope with an integrity self-hash.
type foreverOwnersBlob struct {
	SelfHash string               `msgpack:"self_hash"`
	Owners   []foreverOwnerRecord `msgpack:"owners"`
}

// foreverOwnerKey composes the in-memory ownership map key. Property key folded
// as a string (not a token) so the key is stable regardless of tokenization.
func foreverOwnerKey(labelTok uint16, propKey, valueKey string) string {
	return uniqueSeenKey(labelTok, propKey, valueKey)
}

// hashForeverOwners computes a deterministic self-hash over the ownership
// records (order-independent). Used to detect a corrupt/tampered durable blob.
func hashForeverOwners(recs []foreverOwnerRecord) string {
	lines := make([]string, 0, len(recs))
	for _, r := range recs {
		lines = append(lines, fmt.Sprintf("%s\x00%s\x00%s\x00%d", r.Label, r.PropKey, r.ValueKey, r.Owner))
	}
	sort.Strings(lines)
	h := sha256.New()
	for _, l := range lines {
		_, _ = h.Write([]byte(l))
		_, _ = h.Write([]byte{0x0a})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// -----------------------------------------------------------------------------
// Load / persist
// -----------------------------------------------------------------------------

// loadUniqueForeverOwners rehydrates the in-memory ownership map from MetaKV at
// open. Missing key / no MetaKV yields an empty map. A corrupt blob or a
// self-hash mismatch fails closed. Caller runs before serving writes.
func (c *Core) loadUniqueForeverOwners() error {
	c.uniqueOwners = make(map[string]types.NodeID)
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	v, err := mk.MetaGet(uniqueForeverOwnersMeta)
	if err != nil {
		return fmt.Errorf("graph: read unique-forever owners: %w", err)
	}
	if len(v) == 0 {
		return nil
	}
	var blob foreverOwnersBlob
	if err := storeutil.SafeUnmarshal(v, &blob); err != nil {
		return fmt.Errorf("graph: decode unique-forever owners: %w", err)
	}
	if got := hashForeverOwners(blob.Owners); got != blob.SelfHash {
		return fmt.Errorf("graph: decode unique-forever owners: %w: self-hash mismatch", storepkg.ErrCorruptWire)
	}
	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	for _, r := range blob.Owners {
		tok, ok := c.labels.Lookup(r.Label)
		if !ok {
			return fmt.Errorf("graph: load unique-forever owners: %w: label %q not in registry", storepkg.ErrCorruptWire, r.Label)
		}
		c.uniqueOwners[foreverOwnerKey(tok, r.PropKey, r.ValueKey)] = types.NodeID(r.Owner)
	}
	return nil
}

// storeForeverOwnersLocked persists the ownership map. Caller holds uniqueMu.
func (c *Core) storeForeverOwnersLocked(mk storepkg.MetaKVCapability) error {
	recs := make([]foreverOwnerRecord, 0, len(c.uniqueOwners))
	for key, owner := range c.uniqueOwners {
		labelTok, propKey, valueKey, ok := parseForeverOwnerKey(key)
		if !ok {
			continue
		}
		recs = append(recs, foreverOwnerRecord{
			Label:    c.labels.Resolve(labelTok),
			PropKey:  propKey,
			ValueKey: valueKey,
			Owner:    int64(owner),
		})
	}
	blob := foreverOwnersBlob{SelfHash: hashForeverOwners(recs), Owners: recs}
	b, err := msgpack.Marshal(blob)
	if err != nil {
		return fmt.Errorf("graph: encode unique-forever owners: %w", err)
	}
	if err := mk.MetaSet(uniqueForeverOwnersMeta, b); err != nil {
		return fmt.Errorf("graph: persist unique-forever owners: %w", err)
	}
	return nil
}

// parseForeverOwnerKey splits an in-memory ownership key back into its parts.
// Format: "<labelTok>\x00<propKey>\x00<valueKey>" (valueKey may itself contain
// no NUL — index value-keys never do).
func parseForeverOwnerKey(key string) (labelTok uint16, propKey, valueKey string, ok bool) {
	// Two NUL separators; the value-key is the remainder after the second.
	first := -1
	second := -1
	for i := 0; i < len(key); i++ {
		if key[i] == 0 {
			if first < 0 {
				first = i
			} else {
				second = i
				break
			}
		}
	}
	if first < 0 || second < 0 {
		return 0, "", "", false
	}
	var tok uint64
	if _, err := fmt.Sscanf(key[:first], "%d", &tok); err != nil {
		return 0, "", "", false
	}
	return uint16(tok), key[first+1 : second], key[second+1:], true
}

// foreverOwnerSnapshot returns the set of node IDs that currently OWN a
// UniqueForever value, or nil when the ownership registry is empty (the common
// case in an event-heavy graph, where the retention-purge caller pays nothing).
// The retention purge uses it to reap only owners it actually removes.
func (c *Core) foreverOwnerSnapshot() map[types.NodeID]struct{} {
	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	if len(c.uniqueOwners) == 0 {
		return nil
	}
	owners := make(map[types.NodeID]struct{}, len(c.uniqueOwners))
	for _, owner := range c.uniqueOwners {
		owners[owner] = struct{}{}
	}
	return owners
}

// reapForeverOwnersForPurged releases the UniqueForever claims held by purged
// nodes (ADR-0008 R2 gotcha): a retention purge removes whole entities, and an
// owner that vanishes must free its value or the value is barred forever by a
// ghost. It removes every ownership entry whose owner is in `purged` and
// re-persists the durable blob. No-op when nothing matches. Idempotent.
func (c *Core) reapForeverOwnersForPurged(purged map[types.NodeID]struct{}) error {
	if len(purged) == 0 {
		return nil
	}
	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	changed := false
	for k, owner := range c.uniqueOwners {
		if _, ok := purged[owner]; ok {
			delete(c.uniqueOwners, k)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	return c.storeForeverOwnersLocked(mk)
}

// reapUniqueForeverOwnersForReset clears the durable + in-memory ownership map
// as part of Admin.Reset. No-op without MetaKV.
func (c *Core) reapUniqueForeverOwnersForReset() error {
	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	c.uniqueOwners = make(map[string]types.NodeID)
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	if err := mk.MetaSet(uniqueForeverOwnersMeta, nil); err != nil {
		return fmt.Errorf("graph: reset unique-forever owners: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Kernel branch — consulted by enforceUniqueForNodeHeld under the value stripe
// -----------------------------------------------------------------------------

// checkAndClaimForever consults the ownership registry for one UniqueForever
// constrained value, under the value stripe the caller already holds. Registry
// hit + different entity => ErrUniqueViolation; same entity (any version) =>
// pass; miss => claim (owner = selfID) and persist. Returns nil on pass/claim.
func (c *Core) checkAndClaimForever(labelTok uint16, propKey, valueKey string, selfID types.NodeID) error {
	key := foreverOwnerKey(labelTok, propKey, valueKey)
	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	if owner, ok := c.uniqueOwners[key]; ok {
		if owner == selfID {
			return nil // same entity (any version) may keep the value
		}
		return fmt.Errorf("%w: label %q key %q value permanently owned by node %d (UniqueForever)",
			ErrUniqueViolation, c.labels.Resolve(labelTok), propKey, owner)
	}
	// Miss — claim under the stripe. Persist before returning so a crash does not
	// lose the claim (the node write follows; a rare failed write leaves a
	// conservative claim, correctable via ReleaseOwnership).
	mk := c.metaKV
	if mk == nil {
		return fmt.Errorf("graph: unique-forever claim: %w", storepkg.ErrCapabilityNotSupported)
	}
	c.uniqueOwners[key] = selfID
	if err := c.storeForeverOwnersLocked(mk); err != nil {
		delete(c.uniqueOwners, key) // roll the in-memory claim back on persist failure
		return err
	}
	return nil
}

// checkForeverOwnership is the READ-ONLY sibling of checkAndClaimForever for
// dry-run validation: it reports whether selfID could hold the value under a
// UniqueForever constraint WITHOUT claiming or persisting. Registry hit +
// different entity => ErrUniqueViolation; same entity or a miss => nil (a miss
// passes because a real assert would claim it). Takes only c.uniqueMu.RLock.
func (c *Core) checkForeverOwnership(labelTok uint16, propKey, valueKey string, selfID types.NodeID) error {
	key := foreverOwnerKey(labelTok, propKey, valueKey)
	c.uniqueMu.RLock()
	defer c.uniqueMu.RUnlock()
	if owner, ok := c.uniqueOwners[key]; ok && owner != selfID {
		return fmt.Errorf("%w: label %q key %q value permanently owned by node %d (UniqueForever)",
			ErrUniqueViolation, c.labels.Resolve(labelTok), propKey, owner)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Seeding at constraint creation
// -----------------------------------------------------------------------------

// seedForeverOwnersFromCurrent records ownership for every current node holding
// a value under a freshly activated UniqueForever constraint, so existing values
// are owned from day one. Caller holds no uniqueMu (takes it internally). Runs
// after existing-data duplicate validation has already passed.
func (c *Core) seedForeverOwnersFromCurrent(labelTok uint16, propKey string) error {
	mk := c.metaKV
	if mk == nil {
		return fmt.Errorf("graph: unique-forever seed: %w", storepkg.ErrCapabilityNotSupported)
	}
	var owners map[string]types.NodeID
	scanErr := c.readUnderRLock(func() error {
		nodes, e := c.store.NodesByLabel(labelTok, storepkg.QueryOpts{})
		if e != nil {
			return e
		}
		owners = make(map[string]types.NodeID)
		for _, n := range nodes {
			vk, found := n.IndexablePropertyValueKey(propKey)
			if !found || vk == "" || isFloatValueKey(vk) {
				continue
			}
			owners[foreverOwnerKey(labelTok, propKey, vk)] = n.ID()
		}
		return nil
	})
	if scanErr != nil {
		return scanErr
	}
	if len(owners) == 0 {
		return nil
	}
	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	for k, id := range owners {
		if _, exists := c.uniqueOwners[k]; !exists {
			c.uniqueOwners[k] = id
		}
	}
	return c.storeForeverOwnersLocked(mk)
}

// purgeForeverOwnersLocked removes every ownership claim for (labelTok, propKey)
// and re-persists. Caller holds uniqueMu.
func (c *Core) purgeForeverOwnersLocked(labelTok uint16, propKey string, mk storepkg.MetaKVCapability) error {
	changed := false
	for key := range c.uniqueOwners {
		lt, pk, _, ok := parseForeverOwnerKey(key)
		if !ok || lt != labelTok || pk != propKey {
			continue
		}
		delete(c.uniqueOwners, key)
		changed = true
	}
	if !changed {
		return nil
	}
	return c.storeForeverOwnersLocked(mk)
}

// -----------------------------------------------------------------------------
// ReleaseOwnership admin door
// -----------------------------------------------------------------------------

// releaseOwnership removes a UniqueForever ownership claim so a corrected value
// may be reclaimed by a different entity. Idempotent: releasing an unowned value
// is a no-op. Requires a UniqueForever constraint on (label, propKey) to exist.
func (c *Core) releaseOwnership(ctx context.Context, label, propKey string, value any) error {
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
	valueKey := types.IndexablePropertyValueKey(value)
	if valueKey == "" || isFloatValueKey(valueKey) {
		return fmt.Errorf("%w: ReleaseOwnership value for key %q is not an indexable non-float scalar", ErrUniqueUnsupportedType, propKey)
	}
	labelTok, ok := c.labels.Lookup(label)
	if !ok {
		return fmt.Errorf("%w: label %q key %q", ErrUniqueConstraintNotFound, label, propKey)
	}

	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	st, exists := c.lookupConstraintLocked(labelTok, propKey)
	if !exists || st.scope != constraintspkg.UniqueForever {
		return fmt.Errorf("%w: no UniqueForever constraint on label %q key %q", ErrUniqueConstraintNotFound, label, propKey)
	}
	key := foreverOwnerKey(labelTok, propKey, valueKey)
	if _, owned := c.uniqueOwners[key]; !owned {
		return nil // idempotent — nothing to release
	}
	delete(c.uniqueOwners, key)
	if err := c.storeForeverOwnersLocked(mk); err != nil {
		return err
	}
	return nil
}
