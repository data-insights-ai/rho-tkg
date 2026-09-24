package segment_test

// ADR-0011 S1 scale gate: encoded bytes per HOP, encode and decode
// throughput, and the growth of both across the three synthday sizes.
//
//	RHO_TKG_SEGMENT_SCALE=790k,3.15M,12.6M go test -run TestSegmentScale -v -count=1 -timeout 0 ./pkg/graph/internal/segment
//
// Rows come from the real create doors of an in-memory graph (internal/synthhop,
// HOP after ai-soc P6). RHO_TKG_SEGMENT_HOPDUMP=<dir> additionally encodes
// dumped real synthday HOP rows (hop-<rows>.tsv.gz: id, version, type token,
// start, end, valid_from, valid_to, tx_from, hash, from hash, to hash,
// property count, then quoted key/value pairs), the rows the ADR's sizing
// prototype measured. Synthetic data only.

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/synthhop"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/segment"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type scaleResult struct {
	name                   string
	rows, bytes            int
	encode, open, dec, raw time.Duration
	cols                   time.Duration
	byType, forEach        float64 // row-path rows/s on the same graph (0 for dumps)
	sections               map[string]int
}

func (r scaleResult) perRow(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / float64(r.rows)
}

func (r scaleResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: HOP %d | %.2f B/HOP (%d bytes) | encode %.0f ns/row (%.2fM rows/s, incl. verify) | open %.1f ms | "+
		"Scan %.0f ns/row (%.2fM rows/s, with hash) | decode without hash %.0f ns/row (%.2fM rows/s) | columns only %.0f ns/row (%.2fM rows/s)",
		r.name, r.rows, float64(r.bytes)/float64(r.rows), r.bytes,
		r.perRow(r.encode), 1e3/r.perRow(r.encode), float64(r.open.Microseconds())/1e3,
		r.perRow(r.dec), 1e3/r.perRow(r.dec), r.perRow(r.raw), 1e3/r.perRow(r.raw), r.perRow(r.cols), 1e3/r.perRow(r.cols))
	if r.byType > 0 {
		fmt.Fprintf(&b, " | row path ByType %.2fM rows/s ForEachByType %.2fM rows/s", r.byType/1e6, r.forEach/1e6)
	}
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

// measure encodes rows, then calls release (which must drop every other
// reference to the rows and their graph, so decoding runs with the segment as
// the only large live object — the situation segments exist for) and times
// Open, Scan and a hash-free decode.
func measure(tb testing.TB, name string, schema segment.Schema, rows []*types.Relationship, release func()) scaleResult {
	tb.Helper()
	res := scaleResult{name: name, rows: len(rows)}
	runtime.GC()
	t0 := time.Now()
	data, err := segment.Encode(schema, rows, segment.Options{})
	if err != nil {
		tb.Fatalf("%s: encode: %v", name, err)
	}
	res.encode = time.Since(t0)
	res.bytes = len(data)
	release()
	runtime.GC()
	t0 = time.Now()
	seg, err := segment.Open(data)
	if err != nil {
		tb.Fatalf("%s: open: %v", name, err)
	}
	res.open = time.Since(t0)
	res.sections = segment.SectionSizes(seg)
	n := 0
	t0 = time.Now()
	if err := seg.Scan(func(int, *types.Relationship) bool { n++; return true }); err != nil {
		tb.Fatalf("%s: scan: %v", name, err)
	}
	res.dec = time.Since(t0)
	t0 = time.Now()
	if err := segment.ScanNoHash(seg, func(int, *types.Relationship) {}); err != nil {
		tb.Fatal(err)
	}
	res.raw = time.Since(t0)
	t0 = time.Now()
	_ = segment.DecodeColumns(seg)
	res.cols = time.Since(t0)
	if n != res.rows {
		tb.Fatalf("%s: scanned %d of %d", name, n, res.rows)
	}
	return res
}

func TestSegmentScale(t *testing.T) {
	names := os.Getenv("RHO_TKG_SEGMENT_SCALE")
	if names == "" {
		t.Skip("measurement: set RHO_TKG_SEGMENT_SCALE=790k,3.15M,12.6M to run")
	}
	var results []scaleResult
	for _, name := range strings.Split(names, ",") {
		sz, err := synthhop.SizeByName(strings.TrimSpace(name))
		if err != nil {
			t.Fatal(err)
		}
		g, rows := writeHOP(t, sz, synthhop.SchemaP6)
		// The row path's scan rate on the same rows, while they are resident.
		t0 := time.Now()
		rs, err := g.Rels().ByType("HOP", graph.QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		byType := float64(len(rs)) / time.Since(t0).Seconds()
		cnt := 0
		t0 = time.Now()
		if err := g.Rels().ForEachByType("HOP", graph.QueryOpts{}, func(*types.Relationship) bool { cnt++; return true }); err != nil {
			t.Fatal(err)
		}
		forEach := float64(cnt) / time.Since(t0).Seconds()
		token := rows[0].TypeToken().Value()
		res := measure(t, "synthhop "+sz.Name, hopSchema(token), rows, func() { rows = nil; _ = g.Close(); g = nil })
		res.byType, res.forEach = byType, forEach
		fmt.Println("S1", res)
		results = append(results, res)
		if dir := os.Getenv("RHO_TKG_SEGMENT_HOPDUMP"); dir != "" {
			path := filepath.Join(dir, fmt.Sprintf("hop-%d.tsv.gz", sz.Rows))
			drows, dtoken := readHOPDump(t, path)
			dres := measure(t, "real dump "+sz.Name, hopSchema(dtoken), drows, func() { drows = nil })
			fmt.Println("S1", dres)
		}
	}
	// Gates: at most 30 B/HOP at every size, and no super-linear term — the
	// per-row cost at the largest size stays within 2x of the smallest.
	for _, r := range results {
		if b := float64(r.bytes) / float64(r.rows); b > 30 {
			t.Errorf("%s: %.2f B/HOP exceeds the 30 B/HOP gate", r.name, b)
		}
	}
	if len(results) > 1 {
		first, last := results[0], results[len(results)-1]
		for _, m := range []struct {
			what string
			a, b float64
		}{
			{"encode", first.perRow(first.encode), last.perRow(last.encode)},
			{"scan", first.perRow(first.dec), last.perRow(last.dec)},
			{"decode", first.perRow(first.raw), last.perRow(last.raw)},
		} {
			fmt.Printf("S1 %s ns/row %s -> %s: %.0f -> %.0f (x%.2f)\n", m.what, first.name, last.name, m.a, m.b, m.b/m.a)
			if m.b > 2*m.a {
				t.Errorf("%s per-row cost grew x%.2f from %s to %s", m.what, m.b/m.a, first.name, last.name)
			}
		}
	}
}

// readHOPDump rebuilds relationships from a dump written from a real graph.
func readHOPDump(tb testing.TB, path string) ([]*types.Relationship, uint16) {
	tb.Helper()
	f, err := os.Open(path) // #nosec G304 -- test input named by the operator
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		tb.Fatal(err)
	}
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var rows []*types.Relationship
	var token uint16
	atoi := func(s string) int64 {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			tb.Fatalf("%s: %v", path, err)
		}
		return v
	}
	for sc.Scan() {
		fs := strings.Split(sc.Text(), "\t")
		token = uint16(atoi(fs[2])) // #nosec G115 -- a dumped uint16 token
		r := types.NewRelationship(types.RelID(atoi(fs[0])), token, types.NodeID(atoi(fs[3])), types.NodeID(atoi(fs[4])))
		props := map[string]any{}
		np := int(atoi(fs[11]))
		for k := 0; k < np; k++ {
			key, err1 := strconv.Unquote(fs[12+2*k])
			val, err2 := strconv.Unquote(fs[13+2*k])
			if err1 != nil || err2 != nil {
				tb.Fatalf("%s: bad property", path)
			}
			props[key] = val
		}
		ps, err := types.NewPropertySlice(props)
		if err != nil {
			tb.Fatal(err)
		}
		if err := r.SetProperties(ps); err != nil {
			tb.Fatal(err)
		}
		r.SetVersion(uint32(atoi(fs[1]))) // #nosec G115 -- a dumped version
		r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(atoi(fs[5])), ValidTo: types.Instant(atoi(fs[6])), TxFrom: types.Instant(atoi(fs[7]))})
		r.SetIntegrity(&types.RelIntegrity{Hash: fs[8], FromNodeHash: fs[9], ToNodeHash: fs[10]})
		rows = append(rows, r)
	}
	if err := sc.Err(); err != nil {
		tb.Fatal(err)
	}
	return rows, token
}
