package segment

// V3 byte ledger for the ported codec: encoded bytes per HOP at the three
// synthday sizes of v4 ADR-0011 §6 (790 K / 3.15 M / 12.6 M source signal rows
// = 107,113 / 408,282 / 1,584,150 HOP relationships), the same generator and
// seed as the v4 measurement (synthhop_test.go).

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// hopSchema is HOP after ai-soc P6: four string columns; valid and
// transaction time are system columns (as in the v4 scale test).
func hopSchema() Schema {
	return Schema{TypeName: "HOP", Columns: []Column{
		{Name: "actor", Kind: KindString}, {Name: "asset_class", Kind: KindString},
		{Name: "family", Kind: KindString}, {Name: "orch", Kind: KindString},
	}}
}

// hopRows turns the generator's HOP edges into v5 rows: IDs 1.. in write
// order, node IDs 1.. (host ordinal + 1), valid [vf, vt), transaction time
// [tx, +inf), revision 0. The four string properties are the declared columns.
func hopRows(tb testing.TB, sz Size) []Row {
	tb.Helper()
	rows := make([]Row, 0, sz.HOP)
	err := Generate(Config{Size: sz, SynthSchema: SchemaP6, Workload: WorkloadHOP},
		func(Node) error { return nil },
		func(e Edge) error {
			vf, _ := e.Props["tkg_valid_from"].(int64)
			vt, _ := e.Props["tkg_valid_to"].(int64)
			rows = append(rows, Row{
				ID: types.RelID(len(rows) + 1), Start: types.NodeID(e.Start + 1), End: types.NodeID(e.End + 1),
				ValidFrom: Finite(vf), ValidTo: Finite(vt), TxFrom: Finite(e.TxFrom), TxTo: PosInf(),
				Props: []Property{
					{"family", e.Props["family"]}, {"actor", e.Props["actor"]},
					{"asset_class", e.Props["asset_class"]}, {"orch", e.Props["orch"]},
				},
			})
			return nil
		})
	if err != nil {
		tb.Fatal(err)
	}
	return rows
}

type scaleResult struct {
	name         string
	rows, bytes  int
	encode, open time.Duration
	scan, verify time.Duration
	sections     map[string]int
}

func (r scaleResult) perRow(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / float64(r.rows)
}

func (r scaleResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: HOP %d | %.2f B/HOP (%d bytes) | encode %.0f ns/row (incl. verify) | open %.1f ms | Scan %.0f ns/row (%.2fM rows/s) | Verify %.0f ns/row",
		r.name, r.rows, float64(r.bytes)/float64(r.rows), r.bytes, r.perRow(r.encode),
		float64(r.open.Microseconds())/1e3, r.perRow(r.scan), 1e3/r.perRow(r.scan), r.perRow(r.verify))
	names := make([]string, 0, len(r.sections))
	for k := range r.sections {
		names = append(names, k)
	}
	sort.Slice(names, func(i, j int) bool { return r.sections[names[i]] > r.sections[names[j]] })
	b.WriteString("\n    sections B/HOP:")
	for _, k := range names {
		fmt.Fprintf(&b, " %s=%.2f", k, float64(r.sections[k])/float64(r.rows))
	}
	return b.String()
}

func measure(tb testing.TB, sz Size) scaleResult {
	tb.Helper()
	rows := hopRows(tb, sz)
	res := scaleResult{name: "synthhop " + sz.Name, rows: len(rows)}
	runtime.GC()
	t0 := time.Now()
	data, err := Encode(hopSchema(), rows, Options{})
	if err != nil {
		tb.Fatalf("%s: encode: %v", res.name, err)
	}
	res.encode, res.bytes = time.Since(t0), len(data)
	rows = nil
	runtime.GC()
	t0 = time.Now()
	seg, err := Open(data)
	if err != nil {
		tb.Fatalf("%s: open: %v", res.name, err)
	}
	res.open = time.Since(t0)
	res.sections = seg.SectionBytes()
	n := 0
	t0 = time.Now()
	if err := seg.Scan(func(int, *Row) bool { n++; return true }); err != nil {
		tb.Fatal(err)
	}
	res.scan = time.Since(t0)
	t0 = time.Now()
	if err := seg.Verify(); err != nil {
		tb.Fatal(err)
	}
	res.verify = time.Since(t0)
	if n != res.rows {
		tb.Fatalf("%s: scanned %d of %d", res.name, n, res.rows)
	}
	return res
}

// TestSegmentBytesPerHOPSmall is the fast unit gate: a 5,000-HOP day of the
// same shape must stay under the v4 gate of 30 B/HOP and round-trip.
func TestSegmentBytesPerHOPSmall(t *testing.T) {
	sz := Size{Name: "tiny", HOP: 5000, Pairs: 2000, Hosts: 400, Actors: 50}
	res := measure(t, sz)
	if b := float64(res.bytes) / float64(res.rows); b > 30 {
		t.Fatalf("%.2f B/HOP exceeds the 30 B/HOP gate", b)
	}
}

// BenchmarkSegmentScale is the V3 byte ledger. Run at the release gate:
//
//	go test -run '^$' -bench SegmentScale -benchtime 1x ./internal/segment
//	RHO_V5_SEGMENT_SCALE=790k,3.15M,12.6M go test -run '^$' -bench SegmentScale -benchtime 1x -timeout 0 ./internal/segment
//
// It reports B/HOP and prints the per-section ledger (v4 measured 21.59 B/HOP
// at 790K on the same generator).
func BenchmarkSegmentScale(b *testing.B) {
	names := os.Getenv("RHO_V5_SEGMENT_SCALE")
	if names == "" {
		names = "790k"
	}
	for _, name := range strings.Split(names, ",") {
		sz, err := SizeByName(strings.TrimSpace(name))
		if err != nil {
			b.Fatal(err)
		}
		b.Run(sz.Name, func(b *testing.B) {
			var res scaleResult
			for i := 0; i < b.N; i++ {
				res = measure(b, sz)
			}
			b.ReportMetric(float64(res.bytes)/float64(res.rows), "B/HOP")
			b.ReportMetric(res.perRow(res.encode), "encode-ns/row")
			b.ReportMetric(res.perRow(res.scan), "scan-ns/row")
			fmt.Println("V3", res)
		})
	}
}
