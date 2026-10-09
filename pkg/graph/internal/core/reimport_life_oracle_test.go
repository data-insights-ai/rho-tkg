package core

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReImportLifeOracle — the generative half of backlog 38. A seeded random
// sequence of creates, updates, label changes, bounded and open cascades,
// deletes and re-imports of deleted node and relationship IDs (plain; a
// backfilled tkg_tx_from after every stamp of the chain, accepted; a
// backfilled tkg_tx_from at or below a stamp of the chain, refused) runs on
// memory, badger, sharded and tiered. After every op:
//
//   - every history row ever read back is still stored byte-identical (no
//     version key is reused: the data-loss detector);
//   - the as-of and bitemporal point doors answer every pin taken before the
//     op as they did (rule 15);
//   - a refused re-import (errors.Is ErrTxOrder) changed nothing.
//
// At the end every named and generic door, point, interval, as-of and TxPin,
// equals the brute-force oracle of bitemporaloracle_test.go (runProbe /
// runAsOfProbe), the effective timeline equals the point door pointwise, and
// every chain verifies.
func TestReImportLifeOracle(t *testing.T) {
	t.Parallel()
	seeds, nOps := 4, 28
	if !testing.Short() {
		seeds, nOps = 12, 36
	}
	if isRaceEnabled() {
		seeds = 2
	}
	const base uint64 = 0x38_11FE
	for _, be := range txbBackends() {
		for i := 0; i < seeds; i++ {
			seed := base + uint64(i)
			t.Run(fmt.Sprintf("%s/seed=%d", be.name, seed), func(t *testing.T) {
				t.Parallel()
				runReImportOracle(t, be, seed, nOps)
			})
		}
	}
}

// rlWorld is the oracle world plus the re-import ops and the per-op checks.
type rlWorld struct {
	*world
	rows     map[string]string // "n<id>v<ver>" / "r<id>v<ver>" -> fingerprint
	pins     []types.Instant
	views    map[string]string
	refused  int
	accepted int
}

func runReImportOracle(t *testing.T, be txbBackend, seed uint64, nOps int) {
	g := be.open(t, true)
	rng := rand.New(rand.NewPCG(seed, seed^0x5EED_38))
	w := &rlWorld{world: newWorld(t, g, rng), rows: map[string]string{}}
	w.setup()
	w.observe("setup")
	for i := 0; i < nOps; i++ {
		var desc string
		switch r := rng.IntN(16); {
		case r < 2:
			w.addNode()
			desc = "addNode"
		case r < 3:
			w.addRel()
			desc = "addRel"
		case r < 4:
			w.updateNode()
			desc = "updateNode"
		case r < 5:
			w.updateRel()
			desc = "updateRel"
		case r < 6:
			if rng.IntN(2) == 0 {
				w.addLabel()
			} else {
				w.removeLabel()
			}
			desc = "label"
		case r < 7:
			w.cascadeNode()
			desc = "cascadeNode"
		case r < 8:
			w.cascadeRel()
			desc = "cascadeRel"
		case r < 10:
			w.deleteNode()
			desc = "deleteNode"
		case r < 12:
			w.deleteRel()
			desc = "deleteRel"
		case r < 14:
			desc = w.reimportNode()
		default:
			desc = w.reimportRel()
		}
		w.observe(fmt.Sprintf("op %d %s", i, desc))
	}
	if w.accepted == 0 {
		t.Fatalf("seed %d: no re-import was accepted; the sequence tests nothing (log: %s)", seed, strings.Join(w.log, "; "))
	}

	snap := w.capture()
	var maxStampV types.Instant
	for _, v := range snap.interestingInstants() {
		maxStampV = max(maxStampV, v)
	}
	far := maxStampV + 1_000_000
	probes := snap.buildProbes(rand.New(rand.NewPCG(seed^0xD1B5, seed)), 30, far)
	seen := map[types.Instant]bool{}
	for _, p := range append(probes, probe{validAt: 1500, txAt: far}) {
		w.runProbe(be.name, seed, snap, p)
		if !seen[p.txAt] {
			seen[p.txAt] = true
			w.runAsOfProbe(be.name, seed, snap, p.txAt)
		}
	}
	for _, p := range w.pins {
		w.runAsOfProbe(be.name, seed, snap, p)
	}
	k := &etChecker{t: t, g: g, label: be.name}
	maxPin := w.pinNow()
	for _, id := range w.nodeIDs {
		k.node(id, w.pins, maxPin)
		if ok, err := g.Hash.VerifyNodeChain(id); err != nil || !ok {
			t.Fatalf("seed %d: VerifyNodeChain(%v) = %v, %v", seed, id, ok, err)
		}
	}
	for _, id := range w.relIDs {
		k.rel(id, w.pins, maxPin)
		if ok, err := g.Hash.VerifyRelChain(id); err != nil || !ok {
			t.Fatalf("seed %d: VerifyRelChain(%v) = %v, %v", seed, id, ok, err)
		}
	}
	t.Logf("seed %d: %d re-imports accepted, %d refused, %d pins, %d history rows", seed, w.accepted, w.refused, len(w.pins), len(w.rows))
}

func (w *rlWorld) pinNow() types.Instant {
	p, err := w.g.Temporal.NowTx()
	if err != nil {
		w.t.Fatalf("NowTx: %v", err)
	}
	return p
}

// chainStamps returns the largest TxFrom/TxTo/DeletedAt and one stamp drawn
// from the chain (TxFrom of a random row).
func (w *rlWorld) chainStamps(tms []*types.TemporalMetadata) (maxS, some types.Instant) {
	for _, tm := range tms {
		if tm != nil {
			maxS = max(maxS, tm.TxFrom, tm.TxTo, tm.DeletedAt)
		}
	}
	if len(tms) > 0 {
		if tm := tms[w.rng.IntN(len(tms))]; tm != nil {
			some = tm.TxFrom
		}
	}
	return maxS, some
}

// reimportProps draws the re-import mode: plain, backfilled after every
// stamp (expected accepted) or backfilled at/below a stamp (expected
// refused). It returns the props and whether a refusal is expected.
func (w *rlWorld) reimportProps(tms []*types.TemporalMetadata) (map[string]any, bool, string) {
	props, _ := w.maybeValidTo(w.nextVF())
	if w.rng.IntN(3) == 0 {
		props["tkg_valid_from"] = types.Instant(500 + w.rng.IntN(int(w.vclock)))
		delete(props, "tkg_valid_to")
	}
	maxS, some := w.chainStamps(tms)
	switch w.rng.IntN(3) {
	case 0:
		return props, false, "plain"
	case 1:
		// After every stamp and every pin taken so far (a backfill at or
		// below a pin would legitimately change that pin's answer).
		t := maxS + 1
		if n := len(w.pins); n > 0 {
			t = max(t, w.pins[n-1]+1)
		}
		if t > w.pinNow() {
			return props, false, "plain"
		}
		props["tkg_tx_from"] = t
		return props, false, fmt.Sprintf("backfill t=%d (max stamp %d)", t, maxS)
	default:
		t := some
		if w.rng.IntN(2) == 0 {
			t = maxS
		}
		props["tkg_tx_from"] = t
		return props, true, fmt.Sprintf("backfill t=%d <= max stamp %d", t, maxS)
	}
}

func (w *rlWorld) reimportNode() string {
	var dead []types.NodeID
	for _, id := range w.nodeIDs {
		if !w.nodeAlive[id] {
			dead = append(dead, id)
		}
	}
	if len(dead) == 0 {
		w.deleteNode()
		return "deleteNode (no dead node)"
	}
	id := dead[w.rng.IntN(len(dead))]
	hist, err := w.g.Nodes.History(id)
	if err != nil {
		w.t.Fatalf("History: %v", err)
	}
	tms := make([]*types.TemporalMetadata, 0, len(hist))
	for _, h := range hist {
		tms = append(tms, h.Temporal())
	}
	props, refuse, mode := w.reimportProps(tms)
	_, err = w.g.Nodes.Import(w.ctx, id, w.pickLabels(), props)
	return w.reimportResult(fmt.Sprintf("reimportNode(%v, %s)", id, mode), err, refuse, func() { w.nodeAlive[id] = true })
}

func (w *rlWorld) reimportRel() string {
	var dead []types.RelID
	for _, id := range w.relIDs {
		if !w.relAlive[id] {
			dead = append(dead, id)
		}
	}
	if len(dead) == 0 {
		w.deleteRel()
		return "deleteRel (no dead rel)"
	}
	id := dead[w.rng.IntN(len(dead))]
	hist, err := w.g.Rels.History(id)
	if err != nil {
		w.t.Fatalf("History: %v", err)
	}
	tms := make([]*types.TemporalMetadata, 0, len(hist))
	for _, h := range hist {
		tms = append(tms, h.Temporal())
	}
	props, refuse, mode := w.reimportProps(tms)
	s, err1 := w.g.Nodes.Get(w.ctx, w.anchors[0])
	e, err2 := w.g.Nodes.Get(w.ctx, w.anchors[1])
	if err1 != nil || err2 != nil {
		w.t.Fatalf("anchors: %v %v", err1, err2)
	}
	_, err = w.g.Rels.Import(w.ctx, id, w.relType[id], s, e, props)
	return w.reimportResult(fmt.Sprintf("reimportRel(%v, %s)", id, mode), err, refuse, func() { w.relAlive[id] = true })
}

func (w *rlWorld) reimportResult(desc string, err error, refuse bool, alive func()) string {
	w.record(desc, err)
	switch {
	case refuse && errors.Is(err, ErrTxOrder):
		w.refused++
	case refuse:
		w.t.Fatalf("%s: err = %v; want ErrTxOrder (log: %s)", desc, err, strings.Join(w.log, "; "))
	case err != nil:
		w.t.Fatalf("%s: %v (log: %s)", desc, err, strings.Join(w.log, "; "))
	default:
		w.accepted++
		alive()
	}
	return desc
}

// observe runs the per-op checks and then takes a new pin.
func (w *rlWorld) observe(phase string) {
	w.t.Helper()
	// Every history row read back so far is still stored unchanged.
	cur := map[string]string{}
	for _, id := range w.nodeIDs {
		hist, err := w.g.Nodes.History(id)
		if err != nil {
			w.t.Fatalf("History: %v", err)
		}
		for _, h := range hist {
			k := fmt.Sprintf("n%dv%d", id, h.Version())
			if _, dup := cur[k]; dup {
				w.t.Fatalf("[%s] node %v holds two history rows v%d", phase, id, h.Version())
			}
			cur[k] = rlFingerprint(h.Temporal(), h.Integrity().Hash, h.Integrity().PrevHash)
		}
	}
	for _, id := range w.relIDs {
		hist, err := w.g.Rels.History(id)
		if err != nil {
			w.t.Fatalf("History: %v", err)
		}
		for _, h := range hist {
			k := fmt.Sprintf("r%dv%d", id, h.Version())
			if _, dup := cur[k]; dup {
				w.t.Fatalf("[%s] rel %v holds two history rows v%d", phase, id, h.Version())
			}
			cur[k] = rlFingerprint(h.Temporal(), h.Integrity().Hash, h.Integrity().PrevHash)
		}
	}
	var lost []string
	for k, fp := range w.rows {
		if cur[k] != fp {
			lost = append(lost, fmt.Sprintf("%s: %s -> %q", k, fp, cur[k]))
		}
	}
	if len(lost) > 0 {
		sort.Strings(lost)
		w.t.Fatalf("[%s] history rows lost or overwritten:\n  %s\n log: %s", phase, strings.Join(lost, "\n  "), strings.Join(w.log, "; "))
	}
	for k, fp := range cur {
		w.rows[k] = fp
	}
	// Every pin taken before this op answers as it did.
	now := w.answers()
	var changed []string
	for k, v := range w.views {
		if now[k] != v {
			changed = append(changed, fmt.Sprintf("%s: [%s] -> [%s]", k, v, now[k]))
		}
	}
	if len(changed) > 0 {
		sort.Strings(changed)
		w.t.Fatalf("[%s] the past changed (rule 15):\n  %s\n log: %s", phase, strings.Join(changed, "\n  "), strings.Join(w.log, "; "))
	}
	w.pins = append(w.pins, w.pinNow())
	w.views = w.answers()
}

// answers renders the as-of and bitemporal point sets at every pin taken.
func (w *rlWorld) answers() map[string]string {
	out := map[string]string{}
	valids := []types.Instant{500, 1000, 2000, 3000, 5000, 8000}
	for _, p := range w.pins {
		ns, err := w.g.Temporal.NodesAsOf(p)
		if err != nil {
			w.t.Fatalf("NodesAsOf: %v", err)
		}
		rs, err := w.g.Temporal.RelsAsOf(p)
		if err != nil {
			w.t.Fatalf("RelsAsOf: %v", err)
		}
		out[fmt.Sprintf("asof %d", p)] = fmtNodeVer(nodeSetVer(ns)) + " | " + fmtRelVer(relSetVer(rs))
		for _, va := range append(valids, w.pins...) {
			ns, err := w.g.Temporal.NodesAtTx(va, p)
			if err != nil {
				w.t.Fatalf("NodesAtTx: %v", err)
			}
			rs, err := w.g.Rels.All(storepkg.QueryOpts{ValidAt: va, TxAt: p})
			if err != nil {
				w.t.Fatalf("Rels.All: %v", err)
			}
			out[fmt.Sprintf("valid %d pin %d", va, p)] = fmtNodeVer(nodeSetVer(ns)) + " | " + fmtRelVer(relSetVer(rs))
		}
	}
	return out
}

func rlFingerprint(tm *types.TemporalMetadata, hash, prev string) string {
	var t types.TemporalMetadata
	if tm != nil {
		t = *tm
	}
	return fmt.Sprintf("vf=%d vt=%d tx=%d..%d del=%d upd=%d hash=%.12s prev=%.12s", t.ValidFrom, t.ValidTo, t.TxFrom, t.TxTo, t.DeletedAt, t.UpdatedAt, hash, prev)
}
