package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Row 104, part A (sigma O1-3): is a retained pin's belief immutable under a
// retention PURGE? sigma's fingerprint check assumes that a pin rho still
// answers returns the same belief later. Compaction keeps that (a prefix trim
// refused below the stub's LastTrimmedTxTo). A purge does NOT: it hard-removes
// whole entities (rows, history, no tombstone) and advances a watermark to
// policy.Before, but a purged entity minted below Before was believed at every
// pin from its creation on, including the pins at and above the watermark the
// gates still answer. This test pins the behaviour as it is (René decides
// whether purge semantics change):
//
//   - node purge (PurgeExpiredNodes): the purged node answers ErrNodeNotFound
//     (NodeAsOf: ErrNoVersionAsOf) at EVERY pin, above the watermark (answered before) and below it (the
//     node gate finds no labels to consult, checkNodePointRetention); a
//     relationship removed with it answers ErrRelNotFound at and above the
//     node watermark and ErrRetentionExpired below it (checkRelPointRetention,
//     the node family);
//   - relationship purge (PurgeExpiredRels): the purged relationship answers
//     ErrRelNotFound at and above the type watermark (answered before) and
//     ErrRetentionExpired below it (checkRelTypePointRetention);
//   - entities the purge kept answer every pin as before.
//
// Catches a change of either side: a purge that starts refusing the retained
// pins (ErrRetentionExpired above the watermark) or one that keeps answering
// the purged entity.
func TestRetentionPurgeChangesRetainedPins(t *testing.T) {
	ctx := context.Background()
	for _, be := range txbBackends(func(c *Config) { c.AllowRetentionPurge = true }) {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			tp := g.Temporal
			ref, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": "ref"})
			if err != nil {
				t.Fatal(err)
			}
			ref2, err := g.Nodes.Add(ctx, []string{"Ref"}, map[string]any{"k": "ref2"})
			if err != nil {
				t.Fatal(err)
			}
			old, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"x": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes.Update(ctx, old.ID(), map[string]any{"x": int64(2)}); err != nil {
				t.Fatal(err)
			}
			link, err := g.Rels.AddByID(ctx, "LINK", ref.ID(), old.ID(), map[string]any{"x": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			ev, err := g.Rels.AddByID(ctx, "EVENT", ref.ID(), ref2.ID(), map[string]any{"x": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(ctx, ev.ID(), map[string]any{"x": int64(2)}); err != nil {
				t.Fatal(err)
			}
			pOld, err := tp.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			// The commit clock runs ahead of the wall clock by one instant
			// per write: wait past pOld before minting the kept entities.
			time.Sleep(time.Duration(30) * time.Millisecond)
			kept, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"x": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			evKept, err := g.Rels.AddByID(ctx, "EVENT", ref.ID(), ref2.ID(), map[string]any{"x": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			nodeB, relB := kept.ID().MintInstant(), evKept.ID().MintInstant()
			time.Sleep(3 * time.Millisecond)
			pAbove, err := tp.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if old.ID().MintInstant() >= nodeB || ev.ID().MintInstant() >= relB || pOld >= nodeB || pOld >= relB || pAbove < nodeB || pAbove < relB {
				t.Fatalf("fixture: mints old %d ev %d, boundaries %d %d, pins %d %d", old.ID().MintInstant(), ev.ID().MintInstant(), nodeB, relB, pOld, pAbove)
			}

			// Before the purges every pin answers every entity.
			for _, p := range []types.Instant{pOld, pAbove} {
				if s, err := tp.NodeEffectiveTimeline(old.ID(), p); err != nil || len(s) == 0 {
					t.Fatalf("before: node at %d = %v, %v; want answered", p, s, err)
				}
				for _, id := range []types.RelID{link.ID(), ev.ID()} {
					if s, err := tp.RelEffectiveTimeline(id, p); err != nil || len(s) == 0 {
						t.Fatalf("before: rel %d at %d = %v, %v; want answered", id, p, s, err)
					}
				}
			}
			keptBefore, err := tp.NodeEffectiveTimeline(kept.ID(), pAbove)
			if err != nil || len(keptBefore) == 0 {
				t.Fatalf("before: kept node = %v, %v", keptBefore, err)
			}
			evKeptBefore, err := tp.RelEffectiveTimeline(evKept.ID(), pAbove)
			if err != nil || len(evKeptBefore) == 0 {
				t.Fatalf("before: kept rel = %v, %v", evKeptBefore, err)
			}

			nrep, err := g.Admin.PurgeExpiredNodes(ctx, PurgePolicy{Label: "Ev", Before: nodeB})
			if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
				t.Skipf("node purge: %v", err)
			}
			if err != nil || nrep.NodesPurged != 1 {
				t.Fatalf("PurgeExpiredNodes = %+v, %v; want 1 node", nrep, err)
			}
			rrep, err := g.Admin.PurgeExpiredRels(ctx, RelPurgePolicy{Type: "EVENT", Before: relB})
			if err != nil || rrep.RelsPurged != 1 {
				t.Fatalf("PurgeExpiredRels = %+v, %v; want 1 rel", rrep, err)
			}

			// The purged node: not found at every pin, above the watermark too.
			for _, p := range []types.Instant{pOld, pAbove} {
				if s, err := tp.NodeEffectiveTimeline(old.ID(), p); !errors.Is(err, storepkg.ErrNodeNotFound) {
					t.Errorf("purged node at %d (watermark %d) = %v, %v; want ErrNodeNotFound", p, nodeB, s, err)
				}
				if _, err := tp.NodeAsOf(old.ID(), p); !errors.Is(err, ErrNoVersionAsOf) {
					t.Errorf("purged node NodeAsOf at %d = %v; want ErrNoVersionAsOf", p, err)
				}
			}
			// The relationships purged (with the node, by type): refused below
			// the watermark, not found at and above it.
			for name, id := range map[string]types.RelID{"link (node purge)": link.ID(), "ev (type purge)": ev.ID()} {
				if s, err := tp.RelEffectiveTimeline(id, pOld); !errors.Is(err, ErrRetentionExpired) {
					t.Errorf("%s below the watermark = %v, %v; want ErrRetentionExpired", name, s, err)
				}
				if s, err := tp.RelEffectiveTimeline(id, pAbove); !errors.Is(err, storepkg.ErrRelNotFound) {
					t.Errorf("%s above the watermark (answered before) = %v, %v; want ErrRelNotFound", name, s, err)
				}
			}
			// Kept entities: the same belief as before.
			if s, err := tp.NodeEffectiveTimeline(kept.ID(), pAbove); err != nil || etSegsString(etNodeSegs(s)) != etSegsString(etNodeSegs(keptBefore)) {
				t.Errorf("kept node after = %v, %v; want %v", s, err, keptBefore)
			}
			if s, err := tp.RelEffectiveTimeline(evKept.ID(), pAbove); err != nil || etSegsString(etRelSegs(s)) != etSegsString(etRelSegs(evKeptBefore)) {
				t.Errorf("kept rel after = %v, %v; want %v", s, err, evKeptBefore)
			}
		})
	}
}
