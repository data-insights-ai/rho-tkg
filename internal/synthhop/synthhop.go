// Package synthhop generates a deterministic, synthday-shaped relationship
// workload for the ADR-0011 (column segments) measurement harness and scale
// tests. It stands in for ai-soc's materialization of a synthetic day
// (`engine.MaterializeWithConfig` over `engine/cmd/synthday`), which lives in
// another repository and cannot be imported here.
//
// What is copied from the real workload (measured 2026-09-24 on the three
// synthday days, see docs/adr/0011-column-segments.md §8 and CHANGELOG):
//
//   - the counts per size: HOP rows, distinct (start, end) pairs (= ORIGIN
//     rows), VATTR regimes, CATTR rows (= distinct actors), host nodes;
//   - HOP's property schema, both the legacy one the analysis measured
//     (scenario, family, actor, asset_class, orch, t_lo, t_hi, support,
//     valid time) and the post-P6 one (family, actor, asset_class, orch, valid
//     time, one shared value box per distinct string);
//   - ai-soc's write order: all rows of one (start, end) pair consecutively,
//     each pair's ORIGIN right after its first HOP, then VATTR, then CATTR;
//   - the value shapes: valid time over one day, valid_to - valid_from with a
//     median near 0.5 s, tx_from = valid_from + 30 s (with a small tail), a
//     heavy-tailed number of rows per pair, a Zipf-skewed actor column.
//
// What is not copied: the exact degree distribution and string spellings.
// Byte counts that depend on them are therefore reported next to the same
// measurement on dumped real rows, never instead of it.
package synthhop

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strconv"
)

// Size is one synthday day, described by the counts the analysis measured on
// it (real harness, 2026-09-24).
type Size struct {
	Name   string // "790k", "3.15M", "12.6M"
	Rows   int    // synthday telemetry rows the day was generated from
	HOP    int    // HOP relationships
	Pairs  int    // distinct (start, end) HOP pairs = ORIGIN relationships
	VATTR  int    // VATTR relationships (volume-band regimes)
	CATTR  int    // CATTR relationships (one per actor)
	Hosts  int    // distinct HOP endpoints
	Actors int    // distinct actor values (the empty actor included)
}

// Nodes is the number of nodes the day writes: hosts, one node per actor
// (CATTR subjects) and one family node.
func (s Size) Nodes() int { return s.Hosts + s.CATTR + 1 }

// Rels is the number of relationships the full ai-soc mix writes.
func (s Size) Rels() int { return s.HOP + s.Pairs + s.VATTR + s.CATTR }

// Sizes are the three synthday days of ADR-0011's scale gates. The counts are
// measured (ai-soc real harness on synthday, 2026-09-24).
var Sizes = []Size{
	{Name: "790k", Rows: 790_000, HOP: 107_113, Pairs: 42_482, VATTR: 48_698, CATTR: 395, Hosts: 3_950, Actors: 396},
	{Name: "3.15M", Rows: 3_150_000, HOP: 408_282, Pairs: 174_305, VATTR: 199_447, CATTR: 1_575, Hosts: 15_750, Actors: 1_576},
	{Name: "12.6M", Rows: 12_600_000, HOP: 1_584_150, Pairs: 691_669, VATTR: 791_486, CATTR: 6_300, Hosts: 63_000, Actors: 6_301},
}

// SizeByName returns the named size.
func SizeByName(name string) (Size, error) {
	for _, s := range Sizes {
		if s.Name == name {
			return s, nil
		}
	}
	return Size{}, fmt.Errorf("synthhop: unknown size %q", name)
}

// Schema selects HOP's property schema.
type Schema int

const (
	// SchemaLegacy is HOP as ai-soc wrote it when the analysis measured it:
	// ten properties including the support id list and duplicated valid time.
	SchemaLegacy Schema = iota
	// SchemaP6 is HOP after ai-soc P6: family, actor, asset_class, orch and
	// valid time, with one shared value box per distinct string.
	SchemaP6
)

// Workload selects which relationship types are written.
type Workload int

const (
	// WorkloadHOP writes the host nodes and the HOP relationships only.
	WorkloadHOP Workload = iota
	// WorkloadMix writes what ai-soc writes for a net-conn day: HOP, ORIGIN,
	// VATTR and CATTR, and all their nodes.
	WorkloadMix
)

// Node is one node to create (label "Asset").
type Node struct {
	Props map[string]any
}

// Edge is one relationship to create between two node ordinals (indexes
// into the node sequence emitted before it).
type Edge struct {
	Type       string
	Start, End int
	TxFrom     int64 // 0 = write with Add (no transaction-time backfill)
	Props      map[string]any
}

// DayStartMs is the first instant of the synthetic day (2026-08-23T00:00Z).
const DayStartMs int64 = 1_787_443_200_000

const dayMs = 86_400_000

// Config selects what Generate emits.
type Config struct {
	Size     Size
	Schema   Schema
	Workload Workload
	Seed     uint64 // 0 = the fixed default seed
}

// Generate emits every node (in creation order) and then every relationship
// in ai-soc's write order. Emission is deterministic for a given Config. The
// property maps are freshly allocated per call; callbacks may keep them.
func Generate(cfg Config, node func(Node) error, edge func(Edge) error) error {
	sz := cfg.Size
	if sz.HOP <= 0 || sz.Pairs <= 0 || sz.Pairs > sz.HOP || sz.Hosts < 2 || sz.Actors < 1 {
		return fmt.Errorf("synthhop: invalid size %+v", sz)
	}
	seed := cfg.Seed
	if seed == 0 {
		seed = 20260924
	}
	rng := rand.New(rand.NewPCG(seed, 0x5eed)) // #nosec G404 -- deterministic synthetic workload, not security

	g := &gen{cfg: cfg, rng: rng, boxes: map[string]any{}}
	g.buildPools()

	// Nodes: hosts first (HOP endpoints), then actors and the family node
	// (CATTR's endpoints) when the mix is written.
	for i := 0; i < sz.Hosts; i++ {
		if err := node(Node{Props: g.nodeProps(g.hostNames[i])}); err != nil {
			return err
		}
	}
	mix := cfg.Workload == WorkloadMix
	if mix {
		for i := 0; i < sz.CATTR; i++ {
			if err := node(Node{Props: g.nodeProps("actor:" + g.actors[i%len(g.actors)] + "#" + strconv.Itoa(i))}); err != nil {
				return err
			}
		}
		if err := node(Node{Props: g.nodeProps("family:net-conn")}); err != nil {
			return err
		}
	}

	pairs := g.buildPairs()
	counts := g.rowsPerPair(len(pairs))
	for pi, p := range pairs {
		times := g.pairTimes(counts[pi])
		for j, vf := range times {
			if err := edge(g.hop(p, vf)); err != nil {
				return err
			}
			if j == 0 && mix {
				if err := edge(Edge{Type: "ORIGIN", Start: p.start, End: p.end, Props: g.originProps()}); err != nil {
					return err
				}
			}
		}
	}
	if !mix {
		return nil
	}
	// VATTR: one regime per pair plus the surplus on random pairs, in
	// (start, end) order like ai-soc's sorted regime keys.
	regimes := make([]int, len(pairs))
	for i := range regimes {
		regimes[i] = 1
	}
	for extra := sz.VATTR - len(pairs); extra > 0; extra-- {
		regimes[g.rng.IntN(len(pairs))]++
	}
	for pi, p := range pairs {
		for r := 0; r < regimes[pi]; r++ {
			if err := edge(g.vattr(p, r)); err != nil {
				return err
			}
		}
	}
	family := sz.Hosts + sz.CATTR
	for i := 0; i < sz.CATTR; i++ {
		if err := edge(g.cattr(sz.Hosts+i, family)); err != nil {
			return err
		}
	}
	return nil
}

type pair struct{ start, end int }

type gen struct {
	cfg       Config
	rng       *rand.Rand
	hostNames []string
	actors    []string
	actorZipf *rand.Zipf
	boxes     map[string]any
}

func (g *gen) buildPools() {
	sz := g.cfg.Size
	g.hostNames = make([]string, sz.Hosts)
	for i := range g.hostNames {
		g.hostNames[i] = fmt.Sprintf("10.%d.%d.%d", i>>16&255, i>>8&255, i&255)
	}
	g.actors = make([]string, sz.Actors)
	for i := 1; i < sz.Actors; i++ {
		g.actors[i] = fmt.Sprintf(`corp\user%05d`, i-1)
	}
	if sz.Actors > 1 {
		g.actorZipf = rand.NewZipf(g.rng, 1.1, 8, uint64(sz.Actors-1))
	}
}

// box returns one shared interface value per distinct string (ai-soc P6's
// m.box); the legacy schema stores the plain string.
func (g *gen) box(s string) any {
	if g.cfg.Schema == SchemaLegacy {
		return s
	}
	if v, ok := g.boxes[s]; ok {
		return v
	}
	var v any = s
	g.boxes[s] = v
	return v
}

func (g *gen) nodeProps(name string) map[string]any {
	return map[string]any{
		"name":           name,
		"asset_class":    g.box("production"),
		"tkg_valid_from": DayStartMs,
	}
}

// buildPairs spreads Pairs distinct (start, end) pairs over the hosts, every
// host starting at least one pair when there are enough pairs, and returns
// them in write order (by start, then end — ai-soc's channel order).
func (g *gen) buildPairs() []pair {
	sz := g.cfg.Size
	per := make([]int, sz.Hosts)
	for i := 0; i < sz.Pairs; i++ {
		if i < sz.Hosts {
			per[i]++
		} else {
			per[g.rng.IntN(sz.Hosts)]++
		}
	}
	out := make([]pair, 0, sz.Pairs)
	seen := map[int]struct{}{}
	for s := 0; s < sz.Hosts; s++ {
		clear(seen)
		ends := make([]int, 0, per[s])
		for len(ends) < per[s] && len(ends) < sz.Hosts-1 {
			e := g.rng.IntN(sz.Hosts)
			if e == s {
				continue
			}
			if _, dup := seen[e]; dup {
				continue
			}
			seen[e] = struct{}{}
			ends = append(ends, e)
		}
		sort.Ints(ends)
		for _, e := range ends {
			out = append(out, pair{s, e})
		}
	}
	return out
}

// rowsPerPair draws a heavy-tailed row count per pair (discrete Pareto,
// shape 1.45, matching synthday's median 1 / p90 3 / p99 ~17) and then adjusts
// the total to exactly HOP.
func (g *gen) rowsPerPair(n int) []int {
	total := g.cfg.Size.HOP
	counts := make([]int, n)
	sum := 0
	capK := max(1, total/12)
	for i := range counts {
		u := 1 - g.rng.Float64() // (0, 1]
		k := int(math.Floor(math.Pow(u, -1/1.45)))
		k = min(max(k, 1), capK)
		counts[i] = k
		sum += k
	}
	for sum < total {
		counts[g.rng.IntN(n)]++
		sum++
	}
	for sum > total {
		i := g.rng.IntN(n)
		if counts[i] > 1 {
			counts[i]--
			sum--
		}
	}
	return counts
}

// pairTimes returns k sorted valid-from instants inside the day.
func (g *gen) pairTimes(k int) []int64 {
	ts := make([]int64, k)
	for i := range ts {
		ts[i] = DayStartMs + g.rng.Int64N(dayMs)
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	return ts
}

func (g *gen) actor() string {
	if g.actorZipf == nil {
		return g.actors[0]
	}
	// Rank 0 (the most frequent) is the empty actor, as in synthday.
	r := g.actorZipf.Uint64()
	if r == 0 {
		return g.actors[0]
	}
	return g.actors[r]
}

func (g *gen) hop(p pair, vf int64) Edge {
	dur := 1 + g.rng.Int64N(1000)
	if g.rng.IntN(20) == 0 {
		dur = 1000 + g.rng.Int64N(9500)
	}
	vt := vf + dur
	tx := vf + 30_000
	if g.rng.IntN(100) == 0 {
		tx += g.rng.Int64N(10_000)
	}
	actor := g.actor()
	var props map[string]any
	switch g.cfg.Schema {
	case SchemaLegacy:
		k := 1
		if g.rng.IntN(33) == 0 {
			k = 2
		}
		support := make([]string, k)
		for i := range support {
			support[i] = fmt.Sprintf("%016x", g.rng.Uint64())
		}
		props = map[string]any{
			"scenario": "corpus", "family": "net-conn", "actor": actor,
			"asset_class": "production", "orch": "",
			"t_lo": vf, "t_hi": vt, "support": support,
			"tkg_valid_from": vf, "tkg_valid_to": vt,
		}
	default:
		props = map[string]any{
			"family": g.box("net-conn"), "actor": g.box(actor),
			"asset_class": g.box("production"), "orch": g.box(""),
			"tkg_valid_from": vf, "tkg_valid_to": vt,
		}
	}
	return Edge{Type: "HOP", Start: p.start, End: p.end, TxFrom: tx, Props: props}
}

func (g *gen) originProps() map[string]any {
	if g.cfg.Schema == SchemaLegacy {
		return map[string]any{"scenario": "corpus", "orch": ""}
	}
	return map[string]any{"orch": g.box("")}
}

var volumeBands = []string{"vol-0", "vol-1", "vol-2", "vol-3", "vol-4", "vol-5"}

func (g *gen) vattr(p pair, r int) Edge {
	lo := DayStartMs + g.rng.Int64N(dayMs)
	hi := lo + 1 + g.rng.Int64N(3_600_000)
	props := map[string]any{
		"attr": g.box(volumeBands[(p.start+p.end+r)%len(volumeBands)]),
		"obs":  1 + g.rng.IntN(40), "tkg_valid_from": lo, "tkg_valid_to": hi,
	}
	if g.cfg.Schema == SchemaLegacy {
		props["scenario"] = "corpus"
	}
	return Edge{Type: "VATTR", Start: p.start, End: p.end, Props: props}
}

func (g *gen) cattr(actorNode, familyNode int) Edge {
	lo := DayStartMs + g.rng.Int64N(dayMs/2)
	props := map[string]any{
		"attr": g.box("net-conn"), "obs": 1 + g.rng.IntN(400),
		"tkg_valid_from": lo, "tkg_valid_to": lo + 1 + g.rng.Int64N(dayMs/2),
	}
	if g.cfg.Schema == SchemaLegacy {
		props["scenario"] = "corpus"
	}
	return Edge{Type: "CATTR", Start: actorNode, End: familyNode, Props: props}
}
