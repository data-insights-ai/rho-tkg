package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"testing"
)

// IO/format guards only: the injected walk proves no SDK or graph semantics.
func TestGraphCostRawArtifactRefusesPartialEOFAndOverwrite(t *testing.T) {
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	header := graphCostArtifactHeader{Type: "header", Format: graphCostRawFormat, Stream: "history", Config: config{Limits: limits{Versions: 1}}, Policy: graphCostStreamPolicy{1, 4096}}
	stop := errors.New("consumer refused")
	walk := func(emit func(graphCostRawRow) error) (graphCostStreamEnd, error) {
		if err := emit(graphCostRawRow{Key: []byte{0, 255}, Value: []byte{255, 0}}); err != nil {
			return graphCostStreamEnd{}, err
		}
		return graphCostStreamEnd{}, stop
	}
	if end, err := writeGraphCostRawArtifact(root, header, walk); !errors.Is(err, stop) || end != (graphCostStreamEnd{}) {
		t.Fatal("partial walk published EOF success", end, err)
	}
	raw, err := root.ReadFile("fdb-cost-history.jsonl")
	if err != nil || bytes.Contains(raw, []byte(`"Type":"footer"`)) {
		t.Fatal("partial evidence has footer", err)
	}
	if _, err := writeGraphCostRawArtifact(root, header, walk); !errors.Is(err, fs.ErrExist) {
		t.Fatal("original evidence overwritten", err)
	}
	for _, name := range []string{"current", "cdc"} {
		header.Stream = name
		if end, err := writeGraphCostRawArtifact(root, header, func(func(graphCostRawRow) error) (graphCostStreamEnd, error) {
			return graphCostStreamEnd{Rows: [2]uint64{1, 0}}, nil
		}); !errors.Is(err, errMismatch) || end != (graphCostStreamEnd{}) {
			t.Fatal("unobserved row count published EOF", end, err)
		}
	}
	if _, err := writeGraphCostRawArtifact(root, header, nil); !errors.Is(err, errInvalid) {
		t.Fatal("missing walk admitted", err)
	}
	if err := publishGraphCostResult(root, graphCostResult{Version: 1}); !errors.Is(err, errInvalid) {
		t.Fatal("legacy format relabelled", err)
	}
	if _, err := root.Stat("fdb-cost-result.json"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("failed stream published result", err)
	}
	result := graphCostResult{Version: 2, OracleFormat: graphCostRawFormat}
	if err := publishGraphCostResult(root, result); err != nil {
		t.Fatal("closed exclusive result publication", err)
	}
	if err := publishGraphCostResult(root, result); !errors.Is(err, fs.ErrExist) {
		t.Fatal("original result overwritten", err)
	}
}

// Source-only authored composition: typed columns, cache and reclamation are
// genuine SDK calls. Execution/measurement belongs to the coordinated gate.
func TestGraphCostColumnsActualHeldReadCacheAndDerivedCollection(t *testing.T) {
	h := fixture(t, defaults())
	directory := t.TempDir()
	t.Setenv("FDB_COST_ARTIFACT", directory)
	t.Setenv("RHO_GRAPH_COST_LAYOUT", "columns")
	p := graphCostProfile{Version: 1, Facts: 2, CrossPercent: 100, Writers: 1}
	if err := runGraphCost(h.db, p); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	body, err := root.ReadFile("fdb-cost-result.json")
	if err != nil {
		t.Fatal(err)
	}
	result, err := decode[graphCostResult](body)
	if err != nil || result.Version != 3 || result.OracleFormat != graphCostColumnFormat || result.Config.Graph != "graph-cost-columns-v1" || result.Columns == nil || !result.Columns.complete() {
		t.Fatal("column lane was not genuinely selected/sealed", result, err)
	}
	artifact, err := root.ReadFile("fdb-cost-columns.jsonl")
	if err != nil || sha256.Sum256(artifact) != result.Columns.ArtifactSHA256 {
		t.Fatal("column artifact hash", err)
	}
	var points, refusals int
	var collected bool
	for _, line := range bytes.Split(bytes.TrimSuffix(artifact, []byte{'\n'}), []byte{'\n'}) {
		var kind struct{ Type string }
		if err := json.Unmarshal(line, &kind); err != nil {
			t.Fatal(err)
		}
		switch kind.Type {
		case "point":
			var point struct {
				Type, Generation string
				Owner            uint64
				Found            bool
				Row              graphColumnRow
			}
			if err := json.Unmarshal(line, &point); err != nil {
				t.Fatal(err)
			}
			want := int64(6)
			if point.Generation == "old" {
				want = 5
			}
			if point.Found != (point.Owner != 5) || point.Found && (point.Row.Value != want || point.Row.Owner != point.Owner) || !point.Found && !graphColumnSameRow(point.Row, graphColumnRow{}) {
				t.Fatal("old/final/phantom point", point)
			}
			points++
		case "refusal":
			refusals++
		case "collect":
			var collection struct {
				Type   string
				Budget graphColumnCollectBudget
				Result graphColumnCollectResult
				Absent []byte
				Guard  graphCostStoredRecord
			}
			if err := json.Unmarshal(line, &collection); err != nil {
				t.Fatal(err)
			}
			guard, err := decode[graphColumnGuard](collection.Guard.Value)
			if err != nil || !collection.Result.Done || guard.State != columnGuardCollected || guard.DeletedBytes != result.Columns.Old.Bytes || !bytes.HasSuffix(collection.Absent, []byte("/chunk/0000000000000000")) {
				t.Fatal("bounded actual derived collection", collection, err)
			}
			collected = true
		}
	}
	if points != 6 || refusals != 4 || !collected || result.Columns.Old.Rows != 5 || result.Columns.Final.Rows != 7 {
		t.Fatal("missing exact lifecycle", points, refusals, collected, result.Columns)
	}
	// These source EOFs were captured AFTER collection, not a pre-GC replay.
	if result.StreamEnds[0].Rows != [2]uint64{10, 5} || result.StreamEnds[2].Groups != [2]uint64{6, 1} {
		t.Fatal("derived collection lost retained source", result.StreamEnds)
	}
}

// This direct runner parent requires real native SDK transactions through
// fixture. It adds no fake FDB/model acceptance and is source-only until leased.
func TestGraphCostRawArtifactsActualCutsOriginalsAndEOF(t *testing.T) {
	p := graphCostProfile{Version: 1, Facts: 2, CrossPercent: 50, Writers: 1}
	h := fixture(t, graphCostConfig("graph-cost-v1", p))
	directory := t.TempDir()
	t.Setenv("FDB_COST_ARTIFACT", directory)
	if err := runGraphCost(h.db, p); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})
	raw, err := root.ReadFile("fdb-cost-result.json")
	if err != nil {
		t.Fatal(err)
	}
	var result graphCostResult
	if err := json.Unmarshal(raw, &result); err != nil || result.Version != 2 || result.OracleFormat != graphCostRawFormat || result.Config != h.config || result.CDCGroups != 7 || result.PhysicalGraphChanges != 15 {
		t.Fatal("new result lacks exact qualified format/config/counts", result, err)
	}
	for i, name := range []string{"history", "current", "cdc"} {
		raw, err := root.ReadFile("fdb-cost-" + name + ".jsonl") // Tiny direct test only.
		if err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(raw, []byte{'\n'})
		var header graphCostArtifactHeader
		if len(lines) < 3 || json.Unmarshal(lines[0], &header) != nil || header.Stream != name || header.Format != graphCostRawFormat || header.Config != h.config || !sameCertificate(header.InitialCut, result.InitialCut) || !sameCertificate(header.OldCut, result.OldCut) || !sameCertificate(header.FinalCut, result.FinalCut) {
			t.Fatal("foreign or missing actual header", name)
		}
		for g := range 2 {
			body, err := wire(header.Config)
			if err != nil || !bytes.Equal(header.ConfigRecords[g].Key, h.key(uint8(g), "meta", "config")) || !bytes.Equal(header.ConfigRecords[g].Value, body) {
				t.Fatal("native config raw binding", g, err)
			}
			body, err = wire(header.Metadata[g])
			if err != nil || !bytes.Equal(header.MetadataRecords[g].Key, h.key(uint8(g), "meta", "state")) || !bytes.Equal(header.MetadataRecords[g].Value, body) {
				t.Fatal("native metadata raw binding", g, err)
			}
		}
		for n, cert := range []certificate{result.InitialCut, result.OldCut, result.FinalCut} {
			body, err := wire(cert)
			if err != nil || !bytes.Equal(header.CutRecords[n].Key, h.key(0, "cut", hex.EncodeToString(body))) || !bytes.Equal(header.CutRecords[n].Value, body) {
				t.Fatal("native certificate raw binding", n, err)
			}
		}
		var footer graphCostArtifactFooter
		if json.Unmarshal(lines[len(lines)-2], &footer) != nil || footer.Type != "footer" || footer.Rows != result.StreamEnds[i].Rows || footer.Metadata != header.Metadata {
			t.Fatal("missing or mixed EOF", name)
		}
		// The payload hash includes the last row/header newline, not the footer.
		payload := append(bytes.Join(lines[:len(lines)-2], []byte{'\n'}), '\n')
		digest := sha256.Sum256(payload)
		if footer.PayloadSHA256 != hex.EncodeToString(digest[:]) {
			t.Fatal("payload/footer hash mismatch", name)
		}
		var counts [2]uint64
		for _, line := range lines[1 : len(lines)-2] {
			var row graphCostArtifactRow
			if json.Unmarshal(line, &row) != nil || row.Type != "row" || row.Row.Group > 1 || len(row.Row.Key) == 0 || len(row.Row.Value) == 0 {
				t.Fatal("missing raw native row", name)
			}
			counts[row.Row.Group]++
			if name == "cdc" {
				group, err := decode[changeGroup](row.Row.Value)
				if err != nil {
					t.Fatal(err)
				}
				original, err := wire(group.Outcome)
				if err != nil || !bytes.Equal(row.Row.IDKey, h.key(group.Outcome.Coordinator, "id", group.Outcome.ID)) || !bytes.Equal(row.Row.RequestKey, h.key(group.Outcome.Coordinator, "request", group.Outcome.Request)) || !bytes.Equal(row.Row.IDValue, original) || !bytes.Equal(row.Row.RequestValue, original) {
					t.Fatal("original ID/request raw record mismatch", err)
				}
			}
		}
		if counts != footer.Rows {
			t.Fatal("EOF cardinality mismatch", counts, footer.Rows)
		}
	}
	for _, name := range []string{"fdb-cost-result.pending.json", "fdb-cost-view-5.json", "fdb-cost-view-6.json"} {
		if _, err := root.Stat(name); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("pending result or legacy whole view remains", name, err)
		}
	}
	file, err := root.Open("fdb-cost-operation-groups.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	decoder, groups := json.NewDecoder(file), 0
	for {
		var group changeGroup
		err := decoder.Decode(&group)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || h.validateChange(group) != nil {
			t.Fatal("per-operation original encoding changed", err)
		}
		groups++
	}
	if err := file.Close(); err != nil || groups != 7 {
		t.Fatal("per-operation original count", groups, err)
	}
}
