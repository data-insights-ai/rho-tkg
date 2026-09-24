package synthhop

import (
	"errors"
	"reflect"
	"testing"
)

var tiny = Size{Name: "tiny", HOP: 500, Pairs: 120, VATTR: 150, CATTR: 7, Hosts: 40, Actors: 8}

type tally struct {
	nodes int
	types map[string]int
	pairs map[[2]int]struct{}
	first []Edge
}

func run(t *testing.T, cfg Config) tally {
	t.Helper()
	tl := tally{types: map[string]int{}, pairs: map[[2]int]struct{}{}}
	err := Generate(cfg, func(Node) error { tl.nodes++; return nil }, func(e Edge) error {
		if e.Start < 0 || e.Start >= tl.nodes || e.End < 0 || e.End >= tl.nodes {
			t.Fatalf("edge %+v references a node not yet emitted (%d)", e, tl.nodes)
		}
		tl.types[e.Type]++
		if e.Type == "HOP" {
			tl.pairs[[2]int{e.Start, e.End}] = struct{}{}
			if e.TxFrom <= 0 {
				t.Fatalf("HOP without transaction time: %+v", e)
			}
		}
		if len(tl.first) < 50 {
			tl.first = append(tl.first, e)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return tl
}

func TestGenerate_MixCountsAreExact(t *testing.T) {
	for _, schema := range []Schema{SchemaLegacy, SchemaP6} {
		tl := run(t, Config{Size: tiny, Schema: schema, Workload: WorkloadMix})
		want := map[string]int{"HOP": tiny.HOP, "ORIGIN": tiny.Pairs, "VATTR": tiny.VATTR, "CATTR": tiny.CATTR}
		if !reflect.DeepEqual(tl.types, want) {
			t.Fatalf("schema %d: types %v, want %v", schema, tl.types, want)
		}
		if tl.nodes != tiny.Nodes() {
			t.Fatalf("nodes %d, want %d", tl.nodes, tiny.Nodes())
		}
		if len(tl.pairs) != tiny.Pairs {
			t.Fatalf("distinct HOP pairs %d, want %d", len(tl.pairs), tiny.Pairs)
		}
	}
}

func TestGenerate_HOPWorkloadWritesOnlyHOP(t *testing.T) {
	tl := run(t, Config{Size: tiny, Schema: SchemaP6, Workload: WorkloadHOP})
	if len(tl.types) != 1 || tl.types["HOP"] != tiny.HOP || tl.nodes != tiny.Hosts {
		t.Fatalf("types %v nodes %d", tl.types, tl.nodes)
	}
}

func TestGenerate_Deterministic(t *testing.T) {
	a := run(t, Config{Size: tiny, Schema: SchemaLegacy, Workload: WorkloadMix})
	b := run(t, Config{Size: tiny, Schema: SchemaLegacy, Workload: WorkloadMix})
	if !reflect.DeepEqual(a.first, b.first) {
		t.Fatal("same config produced different edges")
	}
	c := run(t, Config{Size: tiny, Schema: SchemaLegacy, Workload: WorkloadMix, Seed: 7})
	if reflect.DeepEqual(a.first, c.first) {
		t.Fatal("different seed produced identical edges")
	}
}

func TestGenerate_SchemasCarryTheirProperties(t *testing.T) {
	legacy := run(t, Config{Size: tiny, Schema: SchemaLegacy, Workload: WorkloadHOP}).first[0].Props
	p6 := run(t, Config{Size: tiny, Schema: SchemaP6, Workload: WorkloadHOP}).first[0].Props
	for _, k := range []string{"scenario", "support", "t_lo", "t_hi"} {
		if _, ok := legacy[k]; !ok {
			t.Fatalf("legacy HOP lacks %q", k)
		}
		if _, ok := p6[k]; ok {
			t.Fatalf("P6 HOP carries %q", k)
		}
	}
	if len(p6) != 6 || len(legacy) != 10 {
		t.Fatalf("property counts legacy=%d p6=%d, want 10 and 6", len(legacy), len(p6))
	}
}

func TestGenerate_RejectsInvalidSizeAndPropagatesErrors(t *testing.T) {
	if err := Generate(Config{Size: Size{HOP: 1, Pairs: 2, Hosts: 3, Actors: 1}}, nil, nil); err == nil {
		t.Fatal("more pairs than rows accepted")
	}
	stop := errors.New("stop")
	if err := Generate(Config{Size: tiny}, func(Node) error { return stop }, nil); !errors.Is(err, stop) {
		t.Fatalf("node error not propagated: %v", err)
	}
	if err := Generate(Config{Size: tiny}, func(Node) error { return nil }, func(Edge) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("edge error not propagated: %v", err)
	}
}

func TestSizes(t *testing.T) {
	for _, s := range Sizes {
		got, err := SizeByName(s.Name)
		if err != nil || got != s {
			t.Fatalf("SizeByName(%q) = %+v, %v", s.Name, got, err)
		}
	}
	if _, err := SizeByName("nope"); err == nil {
		t.Fatal("unknown size accepted")
	}
	// The measured 790k day: 198,688 relationships and 4,346 nodes.
	if s := Sizes[0]; s.Rels() != 198_688 || s.Nodes() != 4_346 {
		t.Fatalf("790k: rels %d nodes %d", s.Rels(), s.Nodes())
	}
}
