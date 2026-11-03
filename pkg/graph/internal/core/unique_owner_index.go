package core

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The owner index of unique keys (SHARD step 10, UNIQIDX; UNIQUELOOK). The
// store's owner index (storepkg.UniqueOwnerIndexCapability: tiered, badger)
// holds the ID that last claimed each value of a UniqueCurrent (label, key).
// The graph declares it with the constraint, seeds it from the rows that
// exist, marks it ready, and claims a value under the value lock before the
// row holding it is written (enforceUniqueForNodeHeld, the cascade, the batch
// pre-check, the replica apply and import: claimOwnerIndexForRow). A lookup
// reads the claim and then the claimant's row, and ignores a claim the row
// does not back (a crash between claim and row, a rollback, a later change, a
// delete, a dropped shard), so the index never misses a holder and needs no
// replay. A tiered event label's rows live on the shard of their day, so its
// key is kept only there (no property index); a reference label and a plain
// badger label keep their property index too, and the owner index spares
// their unique check and GetOrCreateByKey its seeks (ai-soc's admission spent
// 21.3 s CPU in them, UNIQUELOOK).

// Seed and log seams (tests lower them): the page a seed reads at a time,
// the interval between its progress lines, and the logger.
var (
	ownerSeedPageSize      = 10_000
	ownerSeedProgressEvery = 60 * time.Second
	ownerIndexLogger       = slog.Default
)

// uniqueOwnerIndex returns the owner index a unique constraint on label
// uses, or nil when it uses none (a store without the index, UniqueForever
// outside a tiered event label), and whether label is a tiered event label.
// It refuses UniqueForever on an event label (its claims live in one map in
// MetaKV) and a tiered store without the index.
func (c *Core) uniqueOwnerIndex(label string, scope constraintspkg.UniqueScope) (storepkg.UniqueOwnerIndexCapability, bool, error) {
	ts, isTiered := c.store.(*tiered.Store)
	event := isTiered && !ts.IsReferenceLabel(label)
	if event && scope == constraintspkg.UniqueForever {
		return nil, false, fmt.Errorf("%w: label %q: UniqueForever on an event label", ErrUniqueEventLabelUnsupported, label)
	}
	oi, ok := c.store.(storepkg.UniqueOwnerIndexCapability)
	if !ok {
		if event {
			return nil, false, fmt.Errorf("%w: label %q (values span unbounded time shards)", ErrUniqueEventLabelUnsupported, label)
		}
		return nil, false, nil
	}
	if scope == constraintspkg.UniqueForever {
		return nil, false, nil // its ownership map answers; the property index finds holders
	}
	return oi, event, nil
}

// ownerIndexFor is the store's owner index when (labelTok, key) is declared
// on it: writers claim there.
func (c *Core) ownerIndexFor(labelTok uint16, key string) (storepkg.UniqueOwnerIndexCapability, bool) {
	oi, ok := c.store.(storepkg.UniqueOwnerIndexCapability)
	if !ok || !oi.UniqueOwnerIndexDeclared(labelTok, key) {
		return nil, false
	}
	return oi, true
}

// readyOwnerIndex is ownerIndexFor when the index may answer lookups: ready,
// and the key under an active UniqueCurrent constraint (an index a crash
// left behind a dropped constraint answers nothing).
func (c *Core) readyOwnerIndex(labelTok uint16, key string) (storepkg.UniqueOwnerIndexCapability, bool) {
	if !c.hasUniqueConstraints.Load() {
		return nil, false
	}
	c.uniqueMu.RLock()
	st, ok := c.lookupConstraintLocked(labelTok, key)
	active := ok && st.active && st.scope == constraintspkg.UniqueCurrent
	c.uniqueMu.RUnlock()
	if !active {
		return nil, false
	}
	oi, ok := c.ownerIndexFor(labelTok, key)
	if !ok || !oi.UniqueOwnerIndexReady(labelTok, key) {
		return nil, false
	}
	return oi, true
}

// ownerIndexAnswers is the ready owner index when it answers the current
// equality lookup of (labelTok, key) under opts: no time coordinate, and
// every depth on a store or label without time shards (badger, a tiered
// reference label: every depth sees every row), else DepthAll only.
func (c *Core) ownerIndexAnswers(labelTok uint16, key string, opts storepkg.QueryOpts) (storepkg.UniqueOwnerIndexCapability, bool) {
	if opts.HasTimeCoordinate() {
		return nil, false
	}
	oi, ok := c.readyOwnerIndex(labelTok, key)
	if !ok {
		return nil, false
	}
	if opts.Depth != storepkg.DepthAll {
		if ts, isTiered := c.store.(*tiered.Store); isTiered && !ts.IsReferenceLabel(c.labels.Resolve(labelTok)) {
			return nil, false // an event label's holder may lie outside the depth
		}
	}
	return oi, true
}

// claimBacked reads id's row and reports whether it holds tok and value key
// vk: whether a claim of vk by id is backed.
func (c *Core) claimBacked(id types.NodeID, tok uint16, key, vk string) (*types.Node, bool, error) {
	n, err := c.store.GetNode(id)
	if errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, false, nil // the claimant is gone: the claim backs nothing
	}
	if err != nil {
		return nil, false, err
	}
	if got, _ := uniqueValueKey(n, key); !n.HasLabelTokenRaw(tok) || got != vk {
		return nil, false, nil // the claimant's row no longer holds the value
	}
	return n, true, nil
}

// nodesByOwnerIndex answers the current equality lookup of (tok, key) from
// the owner index: for each value form, the claimant whose current row
// carries tok and the value. Same rows as the label-wide lookup (each holder
// has the latest claim of its value), in ID order, paged by opts.After /
// opts.Limit.
func (c *Core) nodesByOwnerIndex(oi storepkg.UniqueOwnerIndexCapability, tok uint16, key string, forms []any, opts storepkg.QueryOpts) ([]*types.Node, error) {
	seen := make(map[string]struct{}, len(forms))
	var out []*types.Node
	for _, f := range forms {
		vk := types.CanonicalIndexKey(types.IndexablePropertyValueKey(f))
		if _, dup := seen[vk]; dup || vk == "" {
			continue
		}
		seen[vk] = struct{}{}
		id, ok, err := oi.UniqueOwner(tok, key, vk)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		n, backed, err := c.claimBacked(id, tok, key, vk)
		if err != nil {
			return nil, err
		}
		if backed {
			out = append(out, n)
		}
	}
	return uniteNodes(out, opts), nil
}

// seedOwnerIndex claims, for every current row of (labelTok, key), its value
// unless a backed claim exists, then marks the index ready. Runs after the
// existing-data validation (or at open), beside writers that claim on their
// own: each value is seeded under its value lock, so a writer's newer claim
// is either seen as backed or written after the seed's. It reads the label
// in ID-ordered pages of ownerSeedPageSize rows (RAM bounded by a page, not
// the label), logs progress every ownerSeedProgressEvery and a final count,
// and returns the rows it read.
func (c *Core) seedOwnerIndex(oi storepkg.UniqueOwnerIndexCapability, labelTok uint16, key string) (int, error) {
	label := c.labels.Resolve(labelTok)
	start := time.Now()
	lastLog := start
	rows, pages := 0, 0
	var after types.EntityID
	for {
		var nodes []*types.Node
		if err := c.readUnderRLock(func() error {
			var err error
			nodes, err = c.store.NodesByLabel(labelTok, storepkg.QueryOpts{After: after, Limit: ownerSeedPageSize})
			return err
		}); err != nil {
			return rows, fmt.Errorf("graph: seed owner index: %w", err)
		}
		if len(nodes) == 0 {
			break
		}
		pages++
		after = types.EntityID(nodes[len(nodes)-1].ID())
		for _, n := range nodes {
			if err := c.seedOwnerRow(oi, labelTok, key, n); err != nil {
				return rows, fmt.Errorf("graph: seed owner index: %w", err)
			}
		}
		rows += len(nodes)
		if time.Since(lastLog) >= ownerSeedProgressEvery {
			lastLog = time.Now()
			ownerIndexLogger().Info("graph: seeding unique owner index", "label", label, "key", key,
				"rows", rows, "elapsed", time.Since(start).Round(time.Millisecond))
		}
		if len(nodes) < ownerSeedPageSize {
			break
		}
	}
	if err := oi.MarkUniqueOwnerIndexReady(labelTok, key); err != nil {
		return rows, err
	}
	ownerIndexLogger().Info("graph: seeded unique owner index", "label", label, "key", key,
		"rows", rows, "pages", pages, "elapsed", time.Since(start).Round(time.Millisecond))
	return rows, nil
}

// seedOwnerRow claims n's value of key unless a backed claim exists; two
// holders of one value fail the seed.
func (c *Core) seedOwnerRow(oi storepkg.UniqueOwnerIndexCapability, labelTok uint16, key string, n *types.Node) error {
	vk, ok := uniqueValueKey(n, key)
	if !ok || vk == "" {
		return nil
	}
	return c.readUnderRLock(func() error {
		ordered := c.valueLocks.LockStripes([]uint8{uniqueValueStripe(labelTok, key, vk)})
		defer c.valueLocks.UnlockStripes(ordered)
		if _, backed, err := c.claimBacked(n.ID(), labelTok, key, vk); err != nil || !backed {
			return err // the row moved on since the scan: its writer claimed
		}
		if id, found, err := oi.UniqueOwner(labelTok, key, vk); err != nil {
			return err
		} else if found {
			if _, backed, err := c.claimBacked(id, labelTok, key, vk); err != nil {
				return err
			} else if backed && id != n.ID() {
				// Two holders (a replica's verbatim duplicate): one owner
				// per value cannot answer; the index stays unready.
				return fmt.Errorf("%w: label token %d key %q held by nodes %d and %d", ErrUniqueViolation, labelTok, key, id, n.ID())
			} else if backed {
				return nil
			}
		}
		return oi.PutUniqueOwner(labelTok, key, vk, n.ID())
	})
}

// ensureOwnerIndexesAtOpen declares and seeds, once, the owner index of
// every active constraint that should have one and has none ready: a
// constraint created by a release before UNIQUELOOK (badger, tiered reference
// labels), or one whose declaration a crash cut. Best effort: an index left
// unready (a read-only store, a failed seed: logged) only leaves its lookups
// on the property index; a failed seed is tried again at the next open.
func (c *Core) ensureOwnerIndexesAtOpen() {
	if _, ok := c.store.(storepkg.UniqueOwnerIndexCapability); !ok {
		return
	}
	type want struct {
		tok        uint16
		label, key string
		scope      constraintspkg.UniqueScope
	}
	var todo []want
	c.uniqueMu.RLock()
	for tok, byKey := range c.uniqueConstraints {
		for key, st := range byKey {
			if st.active {
				todo = append(todo, want{tok, st.label, key, st.scope})
			}
		}
	}
	c.uniqueMu.RUnlock()
	for _, w := range todo {
		oi, _, err := c.uniqueOwnerIndex(w.label, w.scope)
		if err != nil || oi == nil || oi.UniqueOwnerIndexReady(w.tok, w.key) {
			continue
		}
		declared := oi.UniqueOwnerIndexDeclared(w.tok, w.key)
		if err := oi.DeclareUniqueOwnerIndex(w.tok, w.key); err != nil {
			ownerIndexLogger().Warn("graph: unique owner index not declared at open; lookups stay on the property index",
				"label", w.label, "key", w.key, "error", err)
			continue
		}
		if !declared {
			ownerIndexLogger().Info("graph: unique owner index declared at open; store format marker raised to 16 (older releases refuse the store)",
				"label", w.label, "key", w.key)
		}
		ownerIndexLogger().Info("graph: seeding unique owner index at open", "label", w.label, "key", w.key)
		if rows, err := c.seedOwnerIndex(oi, w.tok, w.key); err != nil {
			ownerIndexLogger().Warn("graph: unique owner index left unready; lookups stay on the property index",
				"label", w.label, "key", w.key, "rows", rows, "error", err)
		}
	}
}

// claimOwnerIndexForRow claims, in every declared owner index, the
// constrained values n holds: the doors that write rows without the unique
// kernel (the replica apply, import and their rollbacks) call it before the
// write.
func (c *Core) claimOwnerIndexForRow(n *types.Node) error {
	if n == nil || !c.hasUniqueConstraints.Load() {
		return nil
	}
	type claim struct {
		tok uint16
		key string
	}
	var claims []claim
	c.uniqueMu.RLock()
	for i := 0; i < n.LabelTokenCount(); i++ {
		tok := n.LabelTokenRawAt(i)
		for key := range c.uniqueConstraints[tok] {
			claims = append(claims, claim{tok, key})
		}
	}
	c.uniqueMu.RUnlock()
	for _, cl := range claims {
		oi, ok := c.ownerIndexFor(cl.tok, cl.key)
		if !ok {
			continue
		}
		vk, found := uniqueValueKey(n, cl.key)
		if !found || vk == "" {
			continue
		}
		// These doors reproduce rows verbatim, a duplicate included (the
		// primary had no constraint): an index holds one owner per value, so
		// it stands down and the key's lookups return to the property index.
		if id, ok, err := oi.UniqueOwner(cl.tok, cl.key, vk); err != nil {
			return fmt.Errorf("graph: unique owner claim: %w", err)
		} else if ok && id != n.ID() {
			if _, backed, err := c.claimBacked(id, cl.tok, cl.key, vk); err != nil {
				return err
			} else if backed {
				if err := oi.DropUniqueOwnerIndex(cl.tok, cl.key); err != nil {
					return fmt.Errorf("graph: unique owner index: %w", err)
				}
				ownerIndexLogger().Warn("graph: unique owner index dropped: a replayed row holds a value another node holds; lookups return to the property index",
					"label", c.labels.Resolve(cl.tok), "key", cl.key, "values", 1, "holders", 2)
				continue
			}
		}
		if err := oi.PutUniqueOwner(cl.tok, cl.key, vk, n.ID()); err != nil {
			return fmt.Errorf("graph: unique owner claim: %w", err)
		}
	}
	return nil
}
