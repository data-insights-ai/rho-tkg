package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The pointwise property of the effective timeline (handover
// effective-read-cost fix 1c; the contract sigma-tkgd's tests rely on): for an
// entity and a pin, Node/RelEffectiveTimeline returns segments ascending by
// ValidFrom, non-overlapping, half-open, ValidFrom never 0, adjacent segments
// never holding the same row; and for EVERY valid instant t the segment
// containing t carries exactly the row NodeAtTx(id, t, pin) / RelAtTx returns
// (same version, same stamps, same content), and no segment contains t when
// the point door answers ErrNoVersionValidAt. "Every t" is decided on the
// instants where either side can change: every stamp of every row of the
// entity, every segment bound, each ±1, segment midpoints, 1 and far beyond.
//
// The chains come from the cross-backend bitemporal oracle's generator
// (tx_backfill_oracle_test.go: creates, AddWithTx backfills, Update, label
// changes, bounded / open / one-tick cascades, CloseVersion, Delete, DeleteWithTx
// and UpdateWithTx through standalone / GraphTx / Batch / ingest doors), then a
// tail of GraphTx rollbacks, re-imports of deleted IDs and a history compaction,
// on memory, badger, sharded and tiered, node and relationship, at several
// pins per entity (every recorded instant of the entity and ±1, each intent's
// t-1 / t, the oracle's probe pins, a final pin). A pin the point door refuses
// (ErrHistoryCompacted after the compaction) must be refused by the timeline
// with the same sentinel.
//
// Catches: a segment end off by one, a derived start reported as 0 or as the
// write time, unmerged or wrongly merged neighbours, the current row reported
// where the pin selects an older one, a gap dropped or invented, a deleted
// entity's last segment running past the delete, a skeleton winner returned
// un-hydrated or un-normalized.

// etRowKey renders everything a reader can observe on a row.
func etNodeKey(n *types.Node) string {
	if n == nil {
		return "<nil>"
	}
	tm := types.TemporalMetadata{}
	if p := n.Temporal(); p != nil {
		tm = *p
	}
	labels := n.AllLabelTokens()
	slices.Sort(labels)
	return fmt.Sprintf("v%d %+v labels=%v props=%v", n.Version(), tm, labels, etProps(n.Properties()))
}

func etRelKey(r *types.Relationship) string {
	if r == nil {
		return "<nil>"
	}
	tm := types.TemporalMetadata{}
	if p := r.Temporal(); p != nil {
		tm = *p
	}
	return fmt.Sprintf("v%d %+v type=%d %d->%d props=%v", r.Version(), tm, r.TypeToken(), r.StartNodeID(), r.EndNodeID(), etProps(r.Properties()))
}

func etProps(ps types.PropertySlice) string {
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		parts = append(parts, fmt.Sprintf("%s=%v", p.Key, p.Value))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// etSeg is a segment with its row rendered.
type etSeg struct {
	from, to types.Instant
	key      string
	version  uint32
	frozen   bool
}

func etNodeSegs(ss []temporal.NodeSegment) []etSeg {
	out := make([]etSeg, len(ss))
	for i, s := range ss {
		out[i] = etSeg{s.ValidFrom, s.ValidTo, etNodeKey(s.Node), s.Node.Version(), s.Node.IsFrozen()}
	}
	return out
}

func etRelSegs(ss []temporal.RelSegment) []etSeg {
	out := make([]etSeg, len(ss))
	for i, s := range ss {
		out[i] = etSeg{s.ValidFrom, s.ValidTo, etRelKey(s.Rel), s.Rel.Version(), s.Rel.IsFrozen()}
	}
	return out
}

func etSegsString(ss []etSeg) string {
	var b strings.Builder
	for _, s := range ss {
		fmt.Fprintf(&b, "\n    [%d,%d) %s", s.from, s.to, s.key)
	}
	return b.String()
}

// etStructure checks the shape rules of a segment list.
func etStructure(ss []etSeg) error {
	for i, s := range ss {
		if s.from <= 0 {
			return fmt.Errorf("segment %d starts at %d (want > 0)", i, s.from)
		}
		if s.to != 0 && s.to <= s.from {
			return fmt.Errorf("segment %d is empty or reversed [%d,%d)", i, s.from, s.to)
		}
		if s.to == 0 && i != len(ss)-1 {
			return fmt.Errorf("segment %d is open-ended but not last", i)
		}
		if !s.frozen {
			return fmt.Errorf("segment %d row is not frozen", i)
		}
		if i > 0 {
			p := ss[i-1]
			if s.from < p.to {
				return fmt.Errorf("segments %d and %d overlap or are out of order", i-1, i)
			}
			if s.from == p.to && s.version == p.version && s.key == p.key {
				return fmt.Errorf("segments %d and %d hold the same row and touch (not merged)", i-1, i)
			}
		}
	}
	return nil
}

// etAt returns the segment containing t.
func etAt(ss []etSeg, t types.Instant) (etSeg, bool) {
	for _, s := range ss {
		if t >= s.from && (s.to == 0 || t < s.to) {
			return s, true
		}
	}
	return etSeg{}, false
}

// etProbes are the valid instants the pointwise check visits.
func etProbes(stamps []types.Instant, ss []etSeg) []types.Instant {
	set := map[types.Instant]bool{1: true, 1 << 62: true}
	add := func(v types.Instant) {
		for _, d := range []types.Instant{-1, 0, 1} {
			if v+d > 0 {
				set[v+d] = true
			}
		}
	}
	for _, v := range stamps {
		if v != 0 {
			add(v)
		}
	}
	for _, s := range ss {
		add(s.from)
		if s.to != 0 {
			add(s.to)
			set[s.from+(s.to-s.from)/2] = true
		}
	}
	out := make([]types.Instant, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}

func etStamps(tms []*types.TemporalMetadata, mint types.Instant) []types.Instant {
	out := []types.Instant{mint}
	for _, tm := range tms {
		if tm == nil {
			continue
		}
		out = append(out, tm.ValidFrom, tm.ValidTo, tm.TxFrom, tm.TxTo, tm.UpdatedAt, tm.CreatedAt, tm.DeletedAt)
	}
	return out
}

// etSameRefusal: the timeline refused a pin with a compaction / retention
// sentinel and the point door refused it with the same one.
func etSameRefusal(err, perr error) bool {
	if perr == nil || (!errors.Is(err, perr) && !errors.Is(perr, err)) {
		return false
	}
	return errors.Is(err, ErrHistoryCompacted) || errors.Is(err, ErrRetentionExpired)
}

// etChecker runs the pointwise property for one graph.
type etChecker struct {
	t      *testing.T
	g      *Core
	label  string
	checks int
	pins   int
	segs   int
	errs   int
}

func (k *etChecker) fail(format string, args ...any) {
	k.t.Helper()
	k.t.Fatalf("[%s] "+format, append([]any{k.label}, args...)...)
}

// nodeStampsAndPins returns every stamp of node id's rows (history and current).
func (k *etChecker) nodeStamps(id types.NodeID) []types.Instant {
	var tms []*types.TemporalMetadata
	hist, _ := k.g.Nodes.History(id)
	for _, h := range hist {
		tms = append(tms, h.Temporal())
	}
	if cur, err := k.g.Nodes.Get(context.Background(), id); err == nil {
		tms = append(tms, cur.Temporal())
	}
	return etStamps(tms, storeutil.SnowflakeInstant(id.SnowflakeID()))
}

func (k *etChecker) relStamps(id types.RelID) []types.Instant {
	var tms []*types.TemporalMetadata
	hist, _ := k.g.Rels.History(id)
	for _, h := range hist {
		tms = append(tms, h.Temporal())
	}
	if cur, err := k.g.Rels.Get(context.Background(), id); err == nil {
		tms = append(tms, cur.Temporal())
	}
	return etStamps(tms, storeutil.SnowflakeInstant(id.SnowflakeID()))
}

// etPins: every recorded instant of the entity and ±1, plus the extra pins,
// bounded to (0, maxPin].
func etPins(stamps, extra []types.Instant, maxPin types.Instant) []types.Instant {
	set := map[types.Instant]bool{maxPin: true}
	for _, v := range append(append([]types.Instant(nil), stamps...), extra...) {
		for _, d := range []types.Instant{-1, 0, 1} {
			if p := v + d; p > 0 && p <= maxPin {
				set[p] = true
			}
		}
	}
	out := make([]types.Instant, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}

func (k *etChecker) node(id types.NodeID, extraPins []types.Instant, maxPin types.Instant) {
	k.t.Helper()
	stamps := k.nodeStamps(id)
	for _, pin := range etPins(stamps, extraPins, maxPin) {
		k.pins++
		segsRaw, err := k.g.Temporal.NodeEffectiveTimeline(id, pin)
		if err != nil {
			_, perr := k.g.Temporal.NodeAtTx(id, 1, pin)
			if !etSameRefusal(err, perr) {
				k.fail("node %v pin %d: timeline err %v, point door err %v", id, pin, err, perr)
			}
			k.errs++
			continue
		}
		segs := etNodeSegs(segsRaw)
		k.segs += len(segs)
		if err := etStructure(segs); err != nil {
			k.fail("node %v pin %d: %v%s", id, pin, err, etSegsString(segs))
		}
		for _, va := range etProbes(stamps, segs) {
			k.checks++
			want, perr := k.g.Temporal.NodeAtTx(id, va, pin)
			got, ok := etAt(segs, va)
			switch {
			case errors.Is(perr, storepkg.ErrNoVersionValidAt) || errors.Is(perr, storepkg.ErrNodeNotFound):
				if ok {
					k.fail("node %v pin %d valid %d: point door none (%v), timeline %s%s", id, pin, va, perr, got.key, etSegsString(segs))
				}
			case perr != nil:
				k.fail("node %v pin %d valid %d: point door err %v", id, pin, va, perr)
			default:
				if !ok {
					k.fail("node %v pin %d valid %d: point door %s, timeline none%s", id, pin, va, etNodeKey(want), etSegsString(segs))
				}
				if w := etNodeKey(want); w != got.key {
					k.fail("node %v pin %d valid %d:\n  point    %s\n  timeline %s%s", id, pin, va, w, got.key, etSegsString(segs))
				}
			}
		}
	}
}

func (k *etChecker) rel(id types.RelID, extraPins []types.Instant, maxPin types.Instant) {
	k.t.Helper()
	stamps := k.relStamps(id)
	for _, pin := range etPins(stamps, extraPins, maxPin) {
		k.pins++
		segsRaw, err := k.g.Temporal.RelEffectiveTimeline(id, pin)
		if err != nil {
			_, perr := k.g.Temporal.RelAtTx(id, 1, pin)
			if !etSameRefusal(err, perr) {
				k.fail("rel %v pin %d: timeline err %v, point door err %v", id, pin, err, perr)
			}
			k.errs++
			continue
		}
		segs := etRelSegs(segsRaw)
		k.segs += len(segs)
		if err := etStructure(segs); err != nil {
			k.fail("rel %v pin %d: %v%s", id, pin, err, etSegsString(segs))
		}
		for _, va := range etProbes(stamps, segs) {
			k.checks++
			want, perr := k.g.Temporal.RelAtTx(id, va, pin)
			got, ok := etAt(segs, va)
			switch {
			case errors.Is(perr, storepkg.ErrNoVersionValidAt) || errors.Is(perr, storepkg.ErrRelNotFound):
				if ok {
					k.fail("rel %v pin %d valid %d: point door none (%v), timeline %s%s", id, pin, va, perr, got.key, etSegsString(segs))
				}
			case perr != nil:
				k.fail("rel %v pin %d valid %d: point door err %v", id, pin, va, perr)
			default:
				if !ok {
					k.fail("rel %v pin %d valid %d: point door %s, timeline none%s", id, pin, va, etRelKey(want), etSegsString(segs))
				}
				if w := etRelKey(want); w != got.key {
					k.fail("rel %v pin %d valid %d:\n  point    %s\n  timeline %s%s", id, pin, va, w, got.key, etSegsString(segs))
				}
			}
		}
	}
}

// etTail adds the shapes the oracle's generator does not draw: a GraphTx that
// updates, cascades and deletes and is rolled back; a re-import of a deleted
// node and relationship ID (the second life); then a history compaction
// (KeepVersions 1; sharded declines it).
func etTail(o *txbOracle) (compacted bool) {
	ctx := context.Background()
	g := o.g
	o.tick()
	if nodes, rels := o.aliveNodes(), o.liveRels(); len(nodes) > 0 && len(rels) > 0 {
		tx, err := g.BeginTx()
		if err != nil {
			o.t.Fatalf("BeginTx: %v", err)
		}
		_, _ = tx.UpdateNode(nodes[0], map[string]any{"tail": int64(1)})
		_, _ = tx.SetRelVersionInterval(rels[0], o.nextVF(), 0, map[string]any{"tail": int64(2)})
		_ = tx.DeleteRelationship(rels[len(rels)-1])
		if err := tx.Rollback(); err != nil {
			o.t.Fatalf("Rollback: %v", err)
		}
	}
	o.tick()
	for _, id := range o.nodeIDs {
		if _, err := g.Nodes.Get(ctx, id); errors.Is(err, storepkg.ErrNodeNotFound) {
			if _, err := g.Nodes.Import(ctx, id, []string{"Re"}, map[string]any{"tkg_valid_from": o.nextVF(), "life": int64(2)}); err != nil {
				o.t.Fatalf("Nodes.Import(%v): %v", id, err)
			}
			break
		}
	}
	o.tick()
	for _, id := range o.relIDs {
		if _, err := g.Rels.Get(ctx, id); !errors.Is(err, storepkg.ErrRelNotFound) {
			continue
		}
		s, err1 := g.Nodes.Get(ctx, o.anchors[0])
		e, err2 := g.Nodes.Get(ctx, o.anchors[1])
		if err1 != nil || err2 != nil {
			break
		}
		if _, err := g.Rels.Import(ctx, id, o.relType[id], s, e, map[string]any{"tkg_valid_from": o.nextVF(), "life": int64(2)}); err != nil {
			o.t.Fatalf("Rels.Import(%v): %v", id, err)
		}
		break
	}
	o.tick()
	_, errN := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 1})
	_, errR := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 1})
	for _, err := range []error{errN, errR} {
		if err != nil && !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
			o.t.Fatalf("compact: %v", err)
		}
	}
	o.tick()
	return errN == nil
}

// etX0 is a test-clock origin above the wall clock, shared by every backend of
// a seed (see TestTxBackfillOracle_CrossBackend).
func etX0() types.Instant {
	return types.Instant(time.Now().Add(time.Hour).UnixMilli()/1_000_000*1_000_000 + 1_000_000)
}

func TestEffectiveTimeline_PointwiseOracle(t *testing.T) {
	t.Parallel()
	seeds, nOps := 6, 40
	if !testing.Short() {
		seeds, nOps = 16, 48
	}
	if n, err := strconv.Atoi(os.Getenv("ET_ORACLE_SEEDS")); err == nil && n > 0 {
		seeds = n
	}
	const base uint64 = 0x1C_E7 // "1c effective timeline"
	x0 := types.Instant(time.Now().Add(time.Hour).UnixMilli()/1_000_000*1_000_000 + 1_000_000)
	var total etChecker
	for _, be := range txbBackends() {
		for i := 0; i < seeds; i++ {
			seed := base + uint64(i)
			t.Run(fmt.Sprintf("%s/seed=%d", be.name, seed), func(t *testing.T) {
				_, _, _, pins, o := txbOracleRun(t, be, seed, nOps, x0, nil, nil)
				var extra []types.Instant
				for _, in := range o.intents {
					extra = append(extra, in.at-1, in.at)
				}
				extra = append(extra, pins...)
				k := &etChecker{t: t, g: o.g, label: be.name}
				check := func(phase string) {
					maxPin := o.x
					k.label = be.name + " " + phase
					for _, id := range o.allNodeIDs() {
						k.node(id, extra, maxPin)
					}
					for _, id := range o.relIDs {
						k.rel(id, extra, maxPin)
					}
				}
				check("after the oracle sequence")
				compacted := etTail(o)
				check("after rollback, re-import, compaction")
				if compacted && k.errs == 0 {
					t.Fatalf("compaction ran but no pin was refused with ErrHistoryCompacted")
				}
				total.checks += k.checks
				total.pins += k.pins
				total.segs += k.segs
				total.errs += k.errs
			})
		}
	}
	t.Logf("pointwise checks %d at %d (entity, pin) pairs, %d segments, %d refused pins", total.checks, total.pins, total.segs, total.errs)
}
