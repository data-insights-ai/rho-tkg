package importmap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	fixtures "github.com/data-insights-ai/rho-tkg/tools/v5-import-fixtures"
	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	v4types "github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func du(p string) string {
	out, _ := exec.Command("du", "-sh", p).Output()
	return strings.Fields(string(out) + " ?")[0]
}

func openV4(dir string) (*graphpkg.Graph, string, error) {
	for _, m := range []struct {
		name string
		mode graphpkg.UntimedValidStart
	}{{"unbounded", graphpkg.UntimedUnbounded}, {"tx_from", graphpkg.UntimedFromTxFrom}} {
		bs, err := badger.New(badger.Config{Dir: dir, ReadOnly: true})
		if err != nil {
			return nil, "", err
		}
		g, err := graphpkg.New(graphpkg.Config{Store: bs, UntimedValidStart: m.mode})
		if err == nil {
			return g, m.name, nil
		}
		_ = bs.Close()
		if !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
			return nil, "", err
		}
	}
	return nil, "", errors.New("no untimed mode opens the store")
}

func TestRealStore(t *testing.T) {
	storeDir, work := os.Getenv("RS_STORE"), os.Getenv("RS_WORK")
	if storeDir == "" {
		t.Skip("RS_STORE unset")
	}
	nSample, nPins := 1000, 20
	fmt.Sscan(os.Getenv("RS_N"), &nSample)
	fmt.Sscan(os.Getenv("RS_PINS"), &nPins)
	ctx := context.Background()
	repF, _ := os.OpenFile(filepath.Join(work, "report.txt"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	defer repF.Close()
	rep := func(f string, a ...any) { s := fmt.Sprintf(f, a...); fmt.Fprintln(repF, s); t.Log(s) }
	t0 := time.Now()
	status := func(f string, a ...any) {
		_ = os.WriteFile(filepath.Join(work, "status.txt"), []byte(fmt.Sprintf(f, a...)+fmt.Sprintf(" (elapsed %s)\n", time.Since(t0).Round(time.Second))), 0o644)
	}

	status("opening v4 store read-only")
	g4, mode, err := openV4(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer g4.Close()
	hashes := false
	_ = g4.Nodes().ForEach(storepkg.QueryOpts{}, func(n *v4types.Node) bool { hashes = n.Integrity() != nil; return false })
	info := fixtures.Config{UntimedValidStart: mode, IntegrityHashes: hashes}
	liveN, _ := g4.Nodes().Count()
	liveR, _ := g4.Rels().Count()
	rep("v4 store %s (%s on disk): untimed=%s hashes=%v live nodes=%d live rels=%d", storeDir, du(storeDir), mode, hashes, liveN, liveR)

	status("export")
	expPath := filepath.Join(work, "v4.export")
	f, _ := os.Create(expPath)
	te := time.Now()
	if err := g4.IO().Export(f); err != nil {
		t.Fatal(err)
	}
	f.Close()
	rep("export: %s, %s", du(expPath), time.Since(te).Round(time.Second))
	raw, _ := os.ReadFile(expPath)

	status("ReadExport (v4 re-import in memory)")
	tr := time.Now()
	src, err := fixtures.ReadExport(info, raw)
	if err != nil {
		t.Fatal(err)
	}
	rep("ReadExport: %d nodes, %d rels in %s", len(src.Nodes), len(src.Rels), time.Since(tr).Round(time.Second))

	status("Map")
	tm := time.Now()
	res, err := Map(src)
	if err != nil {
		t.Fatal(err)
	}
	rep("Map: %d ops, %d mapping ambiguities, %s, seed(max v4 id)=%d", len(res.Ops), len(res.Ambiguities), time.Since(tm).Round(time.Second), res.Seed())

	v5dir := filepath.Join(work, "v5graph")
	os.RemoveAll(v5dir)
	opt := Options{DropKindConflicts: true}
	cfg, err := res.Config(baseConfig(t, v5dir), opt)
	if err != nil {
		t.Fatal(err)
	}
	g5, err := graph.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer g5.Close()
	ti := time.Now()
	opt.AfterBatch = func(n int) error {
		if n%(256*50) < 256 {
			el := time.Since(ti)
			eta := time.Duration(float64(el) * float64(len(res.Ops)-n) / float64(max(n, 1)))
			status("import %d/%d ops, ETA %s", n, len(res.Ops), eta.Round(time.Second))
		}
		return nil
	}
	irep, err := res.Import(ctx, g5, cfg, opt)
	if err != nil {
		t.Fatal(err)
	}
	impT := time.Since(ti)
	rep("Import: %s (%d ops), v5 graph %s on disk, allocator seed %d", impT.Round(time.Second), len(res.Ops), du(v5dir), cfg.Allocation.Seed)
	kinds := map[string]int{}
	for _, a := range res.Ambiguities {
		kinds[a.Kind]++
	}
	for _, a := range irep.Ambiguities {
		kinds[a.Kind]++
	}
	var ks []string
	for k, n := range kinds {
		ks = append(ks, fmt.Sprintf("%s=%d", k, n))
	}
	sort.Strings(ks)
	rep("ambiguity report (map + write): %s", strings.Join(ks, " "))
	if len(irep.Ambiguities) > 0 || len(res.Ambiguities) > 0 {
		all := append(slices.Clone(res.Ambiguities), irep.Ambiguities...)
		for i := 0; i < len(all) && i < 5; i++ {
			rep("  ambiguity sample: %+v", all[i])
		}
	}

	im := &imported{res: res, cfg: cfg, g: g5}
	rd := im.reader(t)

	// counts
	status("counts")
	labelAny, labelLive := map[string]map[int64]bool{}, map[string]int{}
	typeAny, typeLive := map[string]map[int64]bool{}, map[string]int{}
	var maxTx int64
	var txs, valids []int64
	addRow := func(r fixtures.Row, isNode bool) {
		maxTx = max(maxTx, r.TxFrom)
		txs = append(txs, r.TxFrom)
		if r.ValidFrom > 0 {
			valids = append(valids, r.ValidFrom)
		}
		if r.Void {
			return
		}
		if isNode {
			for _, l := range r.Labels {
				if labelAny[l] == nil {
					labelAny[l] = map[int64]bool{}
				}
				labelAny[l][r.ID] = true
			}
		} else {
			if typeAny[r.Type] == nil {
				typeAny[r.Type] = map[int64]bool{}
			}
			typeAny[r.Type][r.ID] = true
		}
	}
	for _, e := range src.Nodes {
		for _, r := range e.History {
			addRow(r, true)
		}
		if e.Current != nil {
			addRow(*e.Current, true)
			for _, l := range e.Current.Labels {
				labelLive[l]++
			}
		}
	}
	for _, e := range src.Rels {
		for _, r := range e.History {
			addRow(r, false)
		}
		if e.Current != nil {
			addRow(*e.Current, false)
			typeLive[e.Current.Type]++
		}
	}
	cntBad := 0
	cmp := func(kind, name string, want map[int64]bool, got []int64) {
		gs := map[int64]bool{}
		for _, id := range got {
			gs[id] = true
		}
		var missing, extra []int64
		for id := range want {
			if !gs[id] {
				missing = append(missing, id)
			}
		}
		for id := range gs {
			if !want[id] {
				extra = append(extra, id)
			}
		}
		slices.Sort(missing)
		slices.Sort(extra)
		ok := len(missing) == 0 && len(extra) == 0
		if !ok {
			cntBad++
		}
		rep("count %s %-28s v4 %7d  v5 %7d  equal=%v missing_in_v5=%d extra_in_v5=%d  first_missing=%v first_extra=%v", kind, name, len(want), len(gs), ok, len(missing), len(extra), head(missing), head(extra))
	}
	var lbls []string
	for l := range labelAny {
		lbls = append(lbls, l)
	}
	sort.Strings(lbls)
	for _, l := range lbls {
		cmp("label", l, labelAny[l], ids(rd.scan(rd.all(), []string{l}, "", nil)))
	}
	var tys []string
	for l := range typeAny {
		tys = append(tys, l)
	}
	sort.Strings(tys)
	for _, l := range tys {
		cmp("type", l, typeAny[l], ids(rd.scan(rd.all(), nil, l, nil)))
	}
	allN, allR := map[int64]bool{}, map[int64]bool{}
	for _, e := range src.Nodes {
		allN[e.ID] = true
	}
	for _, e := range src.Rels {
		allR[e.ID] = true
	}
	var gotN, gotR []int64
	for _, e := range rd.scan(rd.all(), nil, "", nil) {
		if e.Ref.IsRel() {
			gotR = append(gotR, int64(e.Ref.Rel()))
		} else {
			gotN = append(gotN, int64(e.Ref.Node()))
		}
	}
	cmp("total", "nodes (all-time ids)", allN, gotN)
	cmp("total", "rels (all-time ids)", allR, gotR)
	rep("count mismatching groups: %d", cntBad)

	// sampled IDs
	status("sampling %d ids", nSample)
	rng := rand.New(rand.NewSource(42))
	type idk struct {
		rel bool
		id  int64
	}
	var pool []idk
	for _, e := range src.Nodes {
		pool = append(pool, idk{false, e.ID})
	}
	for _, e := range src.Rels {
		pool = append(pool, idk{true, e.ID})
	}
	rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })
	var nIDs, rIDs []int64
	for _, p := range pool[:min(nSample, len(pool))] {
		if p.rel {
			rIDs = append(rIDs, p.id)
		} else {
			nIDs = append(nIDs, p.id)
		}
	}
	slices.Sort(txs)
	txs = slices.Compact(txs)
	slices.Sort(valids)
	valids = slices.Compact(valids)
	tlTx := []int64{txs[rng.Intn(len(txs))], txs[rng.Intn(len(txs))], maxTx}
	rep("tx range of v4 rows: %d distinct TxFrom, max %d; timeline pins tx=%v", len(txs), maxTx, tlTx)
	exp, err := fixtures.Answer(ctx, g4, info, nIDs, rIDs, nil, tlTx)
	if err != nil {
		t.Fatal(err)
	}
	// export fidelity: real store rows vs the re-imported export rows for the sample
	srcN, srcR := map[int64]fixtures.Entity{}, map[int64]fixtures.Entity{}
	for _, e := range src.Nodes {
		srcN[e.ID] = e
	}
	for _, e := range src.Rels {
		srcR[e.ID] = e
	}
	fid, withHist := 0, 0
	for _, set := range []struct {
		es []fixtures.Entity
		m  map[int64]fixtures.Entity
	}{{exp.Nodes, srcN}, {exp.Rels, srcR}} {
		for _, e := range set.es {
			if len(e.History) > 0 {
				withHist++
			}
			a, _ := json.Marshal(e)
			b, _ := json.Marshal(set.m[e.ID])
			if !bytes.Equal(a, b) {
				fid++
				if fid <= 5 {
					rep("  export-vs-store differs for id %d", e.ID)
				}
			}
		}
	}
	rep("sample: %d nodes + %d rels (%d with history rows); real-store vs export-reimport row mismatches: %d", len(exp.Nodes), len(exp.Rels), withHist, fid)

	im.exp = *exp
	status("compare sampled ids (declared view)")
	tlStart := len(exp.Timelines)
	_ = tlStart
	nBefore := 0
	t.Run("ids_declared", func(t *testing.T) {
		rd.t = t
		for i, tl := range exp.Timelines {
			known := rd.knownTo(tl.Tx)
			if i == len(exp.Timelines)-1 {
				known = rd.known()
			}
			for _, set := range []struct {
				kind string
				ents []fixtures.EntityTimeline
			}{{"node", tl.Nodes}, {"rel", tl.Rels}} {
				for _, et := range set.ents {
					got := func(v int64) *graph.Entity {
						req := graph.LookupRequest{Selector: graph.Selector{View: graph.ViewDeclared, Valid: rd.point(v), Known: known}, Projection: graph.Projection{AllProperties: true}, Paging: page}
						if set.kind == "rel" {
							req.Rels = []types.RelID{types.RelID(et.ID)}
						} else {
							req.Nodes = []types.NodeID{types.NodeID(et.ID)}
						}
						p, err := rd.snap.Lookup(ctx, req)
						rows := rd.drain(p, err)
						if len(rows) == 0 {
							return nil
						}
						return &rows[0]
					}
					compareState(t, rd, set.kind, et, tl.Tx, got, nil)
				}
			}
		}
	})
	_ = nBefore
	rep("sampled-id declared compare (3 tx pins x %d ids): see test log, failed=%v", len(nIDs)+len(rIDs), t.Failed())

	// pins
	status("pins")
	for i := 0; i < nPins; i++ {
		tx := txs[rng.Intn(len(txs))]
		var v int64
		if i%2 == 0 && len(valids) > 0 {
			v = valids[rng.Intn(len(valids))]
		} else {
			v = txs[rng.Intn(len(txs))]
		}
		pe, err := fixtures.Answer(ctx, g4, info, nil, nil, [][2]int64{{v, tx}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		im.exp = *pe
		ok := t.Run(fmt.Sprintf("pin%02d_valid%d_tx%d", i, v, tx), func(t *testing.T) { rd.t = t; checkPins(t, rd) })
		rep("pin %2d valid=%d tx=%d: v4 nodes=%d rels=%d  equal=%v", i, v, tx, len(pe.Pins[0].Nodes), len(pe.Pins[0].Rels), ok)
		status("pin %d/%d", i+1, nPins)
	}
	status("done")
	rep("total test time %s", time.Since(t0).Round(time.Second))
	_ = temporal.All
}

func head(s []int64) []int64 { return s[:min(len(s), 5)] }
