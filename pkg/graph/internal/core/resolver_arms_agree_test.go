package core

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestResolverArmsAgree: on every chain the write doors produce that
// sortNodeChainForResolve sends to the fast path, the positional arm answers
// as the own-bounds arm, the reference rule, at every row bound and next to
// it. Random histories of timed and untimed updates, bounded updates,
// corrections, excisions, closes and replacements (open and bounded, nested
// inside bounded rows); the nested replacement under an open one is the
// CHANGEFLOOR review's case (the fast path showed the bounded row again after
// the inner replacement ended).
func TestResolverArmsAgree(t *testing.T) {
	for seed := range uint64(64) {
		armsAgreeRun(t, seed)
	}
}

func armsAgreeRun(t *testing.T, seed uint64) {
	t.Helper()
	ctx := context.Background()
	g, err := New(Config{ReplaceVersionLabels: []string{"Ev"}})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ax := g.axis()
	rng := rand.New(rand.NewPCG(seed, 3))
	vf := types.Instant(100)
	n, err := g.Nodes.Add(ctx, []string{"Ev"}, map[string]any{"v": 0, "tkg_valid_from": int64(vf), "tkg_valid_to": int64(vf + 40)})
	if err != nil {
		t.Fatal(err)
	}
	id := n.InternalID()
	var ops []string
	for i := 1; i <= 24; i++ {
		props := map[string]any{"v": i}
		past := func(slack int) types.Instant { return 100 + types.Instant(rng.IntN(int(vf-100)+slack)) }
		var op string
		switch k := rng.IntN(100); {
		case k < 20:
			vf += types.Instant(1 + rng.IntN(20))
			props["tkg_valid_from"] = int64(vf)
			_, err = g.Nodes.Update(ctx, id, props)
			op = "update"
		case k < 30:
			vf += types.Instant(1 + rng.IntN(20))
			props["tkg_valid_from"] = int64(vf)
			props["tkg_valid_to"] = int64(vf + types.Instant(5+rng.IntN(30)))
			_, err = g.Nodes.Update(ctx, id, props)
			op = "bounded"
		case k < 42:
			from := past(10)
			_, err = g.Temporal.SetNodeVersionInterval(ctx, id, from, from+types.Instant(1+rng.IntN(60)), props)
			op = "correct"
		case k < 50:
			from := past(10)
			err = g.Temporal.ExciseNodeValidity(ctx, id, from, from+types.Instant(1+rng.IntN(30)))
			op = "excise"
		case k < 58:
			err = g.Nodes.CloseVersion(ctx, id, vf+types.Instant(1+rng.IntN(30)))
			op = "close"
		default:
			from := past(40)
			to := from + types.Instant(1+rng.IntN(40))
			if rng.IntN(2) == 0 {
				to = ax.OpenEnd()
			}
			_, err = g.Nodes.ReplaceVersion(ctx, id, props, from, to)
			op = fmt.Sprint("replace[", from, ",", to, ")")
		}
		if err != nil {
			op += "!"
		}
		ops = append(ops, op)
		armsAgreeCheck(t, g, id, fmt.Sprintf("seed %d ops %v", seed, ops))
	}
}

// armsAgreeCheck compares the two arms on id's chain when the dispatch takes
// the fast path.
func armsAgreeCheck(t *testing.T, g *Core, id types.NodeID, where string) {
	t.Helper()
	history, err := g.store.GetNodeHistory(id)
	if err != nil {
		t.Fatal(err)
	}
	current, err := g.store.GetNode(id)
	if err != nil && err != storepkg.ErrNodeNotFound {
		t.Fatal(err)
	}
	chain := versionOrdered(chainOf(history, current)) // the resolvers' input order
	if len(chain) < 2 || g.sortNodeChainForResolve(chain) {
		return
	}
	ax := g.axis()
	var probes []types.Instant
	for _, r := range chain {
		s, e := g.nodeOwnBounds(r)
		probes = append(probes, s-1, s, s+1)
		if !ax.Open(e) {
			probes = append(probes, e-1, e, e+1)
		}
	}
	for _, at := range probes {
		pos, perr := g.resolveNodePositional(chain, at, nil)
		own, oerr := g.resolveNodeOwnBounds(chain, at, nil)
		pv, ov := -1, -1
		if perr == nil {
			pv = int(pos.Version())
		}
		if oerr == nil {
			ov = int(own.Version())
		}
		if pv != ov {
			t.Fatalf("%s: at %d the fast path answers version %d, the own-bounds arm %d", where, at, pv, ov)
		}
	}
}
