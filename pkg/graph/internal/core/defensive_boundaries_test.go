package core

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// --- ImportGraph must not panic on malformed records ---
//
// ImportGraph reads from an arbitrary io.Reader (untrusted input). A corrupt or
// malicious export must NOT create invalid token-0 entities — it must surface a
// typed error before construction.

// writeImportRecord assembles one tagged length-prefixed record (matching
// writeExportRecord) into buf for use as ImportGraph input.
func writeImportRecord(buf *bytes.Buffer, tag byte, body []byte) {
	var header [5]byte
	header[0] = tag
	binary.BigEndian.PutUint32(header[1:5], uint32(len(body)))
	buf.Write(header[:])
	buf.Write(body)
}

// validImportHeader returns the bytes for a valid header+registry prelude that
// puts ImportGraph into a state where it will attempt to decode a node/rel
// record next.
func validImportPrelude(t *testing.T) []byte {
	t.Helper()
	return validImportPreludeWithCounts(t, 0, 0)
}

func validImportPreludeWithCounts(t *testing.T, nodeCount, relCount int64) []byte {
	t.Helper()
	hdr := exportHeader{
		Version:    exportFormatVersion,
		ExportedAt: 0,
		NodeCount:  nodeCount,
		RelCount:   relCount,
	}
	hdrBody, err := msgpack.Marshal(&hdr)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	// Index 0 is reserved (token 0 placeholder); ImportNames rejects a
	// non-empty names[0]. Real exports include the reserved slot — match
	// that shape so the prelude is admitted and the test exercises the
	// node/rel record validation, not the registry validation. Labels 2 and 3
	// let a valid record carry extra labels.
	reg := tiered.RegistryFileData{
		Labels:   []string{"", "L1", "L2", "L3"},
		RelTypes: []string{"", "R1"},
	}
	regBody, err := msgpack.Marshal(&reg)
	if err != nil {
		t.Fatalf("marshal registry: %v", err)
	}

	var buf bytes.Buffer
	writeImportRecord(&buf, exportTagHeader, hdrBody)
	writeImportRecord(&buf, exportTagRegistry, regBody)
	return buf.Bytes()
}

// runImportSafely invokes g.IO.Import(r, tkgio.ImportOptions{}) and converts a panic into a t.Fatal.
// The contract is: ImportGraph must surface malformed input as an error, never
// as a panic.
func runImportSafely(t *testing.T, g *Core, r io.Reader) error {
	t.Helper()
	var importErr error
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("ImportGraph panicked on malformed input: %v", rec)
			}
		}()
		importErr = g.IO.Import(r, tkgio.ImportOptions{})
	}()
	return importErr
}

func TestImportGraph_RejectsUnknownRecordTag(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	var buf bytes.Buffer
	buf.Write(validImportPrelude(t))
	writeImportRecord(&buf, 0x7f, []byte{0x81, 0xa1, 'x', 0x01})

	importErr := runImportSafely(t, g, bytes.NewReader(buf.Bytes()))
	if !errors.Is(importErr, ErrCorruptExport) {
		t.Fatalf("ImportGraph unknown tag: got %v, want ErrCorruptExport", importErr)
	}
}

func TestImportGraph_RejectsMalformedMsgpackRecordsWithCorruptSentinel(t *testing.T) {
	t.Parallel()

	malformed := []byte{0xc1} // msgpack "never used" byte; decoder rejects it.
	validHeader := validImportPrelude(t)
	var headerOnly bytes.Buffer
	hdrBody, err := msgpack.Marshal(&exportHeader{Version: exportFormatVersion})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	writeImportRecord(&headerOnly, exportTagHeader, hdrBody)

	tests := map[string][]byte{
		"header": func() []byte {
			var buf bytes.Buffer
			writeImportRecord(&buf, exportTagHeader, malformed)
			return buf.Bytes()
		}(),
		"registry": func() []byte {
			var buf bytes.Buffer
			buf.Write(headerOnly.Bytes())
			writeImportRecord(&buf, exportTagRegistry, malformed)
			return buf.Bytes()
		}(),
		"node": func() []byte {
			var buf bytes.Buffer
			buf.Write(validHeader)
			writeImportRecord(&buf, exportTagNode, malformed)
			return buf.Bytes()
		}(),
		"node-history": func() []byte {
			var buf bytes.Buffer
			buf.Write(validHeader)
			writeImportRecord(&buf, exportTagNodeHist, malformed)
			return buf.Bytes()
		}(),
		"rel": func() []byte {
			var buf bytes.Buffer
			buf.Write(validHeader)
			writeImportRecord(&buf, exportTagRel, malformed)
			return buf.Bytes()
		}(),
		"rel-history": func() []byte {
			var buf bytes.Buffer
			buf.Write(validHeader)
			writeImportRecord(&buf, exportTagRelHist, malformed)
			return buf.Bytes()
		}(),
	}

	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { g.Close() })

			importErr := runImportSafely(t, g, bytes.NewReader(input))
			if !errors.Is(importErr, ErrCorruptExport) {
				t.Fatalf("ImportGraph malformed %s: got %v, want ErrCorruptExport", name, importErr)
			}
			if !errors.Is(importErr, storepkg.ErrCorruptWire) { // the decode cause stays matchable (errors.md)
				t.Fatalf("ImportGraph malformed %s: got %v, want it to wrap store.ErrCorruptWire", name, importErr)
			}
		})
	}
}

func TestImportGraph_RejectsMissingRequiredRecords(t *testing.T) {
	t.Parallel()

	hdr := exportHeader{Version: exportFormatVersion}
	hdrBody, err := msgpack.Marshal(&hdr)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	var headerOnly bytes.Buffer
	writeImportRecord(&headerOnly, exportTagHeader, hdrBody)

	for name, input := range map[string][]byte{
		"empty":       nil,
		"header-only": headerOnly.Bytes(),
	} {
		t.Run(name, func(t *testing.T) {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { g.Close() })

			importErr := runImportSafely(t, g, bytes.NewReader(input))
			if !errors.Is(importErr, ErrCorruptExport) {
				t.Fatalf("ImportGraph %s stream: got %v, want ErrCorruptExport", name, importErr)
			}
		})
	}
}

func TestImportGraph_RejectsHeaderCountMismatch(t *testing.T) {
	t.Parallel()

	for name, input := range map[string][]byte{
		"missing-node": validImportPreludeWithCounts(t, 1, 0),
		"missing-rel":  validImportPreludeWithCounts(t, 0, 1),
	} {
		t.Run(name, func(t *testing.T) {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { g.Close() })

			importErr := runImportSafely(t, g, bytes.NewReader(input))
			if !errors.Is(importErr, ErrCorruptExport) {
				t.Fatalf("ImportGraph %s: got %v, want ErrCorruptExport", name, importErr)
			}
		})
	}
}

func TestImportGraph_RejectsInvalidHeaderCountsAndDuplicateSingletonRecords(t *testing.T) {
	t.Parallel()

	duplicateHeader := append([]byte{}, validImportPrelude(t)...)
	hdr := exportHeader{Version: exportFormatVersion}
	hdrBody, err := msgpack.Marshal(&hdr)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	var duplicateHeaderRecord bytes.Buffer
	writeImportRecord(&duplicateHeaderRecord, exportTagHeader, hdrBody)
	duplicateHeader = append(duplicateHeader, duplicateHeaderRecord.Bytes()...)

	duplicateRegistry := append([]byte{}, validImportPrelude(t)...)
	reg := tiered.RegistryFileData{
		Labels:   []string{"", "L1"},
		RelTypes: []string{"", "R1"},
	}
	regBody, err := msgpack.Marshal(&reg)
	if err != nil {
		t.Fatalf("marshal registry: %v", err)
	}
	var duplicateRegistryRecord bytes.Buffer
	writeImportRecord(&duplicateRegistryRecord, exportTagRegistry, regBody)
	duplicateRegistry = append(duplicateRegistry, duplicateRegistryRecord.Bytes()...)

	for name, input := range map[string][]byte{
		"negative-node-count": validImportPreludeWithCounts(t, -1, 0),
		"negative-rel-count":  validImportPreludeWithCounts(t, 0, -1),
		"duplicate-header":    duplicateHeader,
		"duplicate-registry":  duplicateRegistry,
	} {
		t.Run(name, func(t *testing.T) {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { g.Close() })

			importErr := runImportSafely(t, g, bytes.NewReader(input))
			if !errors.Is(importErr, ErrCorruptExport) {
				t.Fatalf("ImportGraph %s: got %v, want ErrCorruptExport", name, importErr)
			}
		})
	}
}

func TestImportGraph_RejectsDuplicateCurrentRecordsInStream(t *testing.T) {
	t.Parallel()

	nodeBody, err := msgpack.Marshal(&storeutil.NodeWire{
		ID:           snowflakeIDForTest(),
		PrimaryLabel: 1,
		Version:      0,
	})
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}

	relStartID := int64(1000001)
	relEndID := int64(1000002)
	relStartBody, err := msgpack.Marshal(&storeutil.NodeWire{
		ID:           relStartID,
		PrimaryLabel: 1,
		Version:      0,
	})
	if err != nil {
		t.Fatalf("marshal rel start node: %v", err)
	}
	relEndBody, err := msgpack.Marshal(&storeutil.NodeWire{
		ID:           relEndID,
		PrimaryLabel: 1,
		Version:      0,
	})
	if err != nil {
		t.Fatalf("marshal rel end node: %v", err)
	}
	relBody, err := msgpack.Marshal(&storeutil.RelWire{
		ID:      1000003,
		RelType: 1,
		StartID: relStartID,
		EndID:   relEndID,
		Version: 0,
	})
	if err != nil {
		t.Fatalf("marshal rel: %v", err)
	}

	for name, tc := range map[string]struct {
		prelude []byte
		tag     byte
		body    []byte
	}{
		"node": {prelude: validImportPreludeWithCounts(t, 2, 0), tag: exportTagNode, body: nodeBody},
		"rel":  {prelude: validImportPreludeWithCounts(t, 2, 2), tag: exportTagRel, body: relBody},
	} {
		t.Run(name, func(t *testing.T) {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { g.Close() })

			var buf bytes.Buffer
			buf.Write(tc.prelude)
			if name == "rel" {
				writeImportRecord(&buf, exportTagNode, relStartBody)
				writeImportRecord(&buf, exportTagNode, relEndBody)
			}
			writeImportRecord(&buf, tc.tag, tc.body)
			writeImportRecord(&buf, tc.tag, tc.body)

			importErr := runImportSafely(t, g, bytes.NewReader(buf.Bytes()))
			if !errors.Is(importErr, ErrCorruptExport) {
				t.Fatalf("ImportGraph duplicate %s record: got %v, want ErrCorruptExport", name, importErr)
			}
		})
	}
}

func TestImportGraph_RejectsDuplicateHistoryRecordsInStream(t *testing.T) {
	t.Parallel()

	nodeBody, err := msgpack.Marshal(&storeutil.NodeWire{
		ID:           snowflakeIDForTest(),
		PrimaryLabel: 1,
		Version:      1,
	})
	if err != nil {
		t.Fatalf("marshal node history: %v", err)
	}

	relBody, err := msgpack.Marshal(&storeutil.RelWire{
		ID:      snowflakeIDForTest(),
		RelType: 1,
		StartID: 1,
		EndID:   2,
		Version: 1,
	})
	if err != nil {
		t.Fatalf("marshal rel history: %v", err)
	}

	for name, tc := range map[string]struct {
		tag  byte
		body []byte
	}{
		"node-history": {tag: exportTagNodeHist, body: nodeBody},
		"rel-history":  {tag: exportTagRelHist, body: relBody},
	} {
		t.Run(name, func(t *testing.T) {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { g.Close() })

			var buf bytes.Buffer
			buf.Write(validImportPrelude(t))
			writeImportRecord(&buf, tc.tag, tc.body)
			writeImportRecord(&buf, tc.tag, tc.body)

			importErr := runImportSafely(t, g, bytes.NewReader(buf.Bytes()))
			if !errors.Is(importErr, ErrCorruptExport) {
				t.Fatalf("ImportGraph duplicate %s record: got %v, want ErrCorruptExport", name, importErr)
			}
		})
	}
}

// snowflakeIDForTest returns a stable non-zero snowflake-shaped ID for use
// in crafted wire records.
func snowflakeIDForTest() int64 {
	return 1000000
}

// wireRejectEndpointStart and wireRejectEndpointEnd are the node IDs the
// relationship rows of TestImportGraph_RejectsInvalidEntityWire point at; the
// stream carries both nodes so a valid relationship record imports.
const (
	wireRejectEndpointStart = int64(10)
	wireRejectEndpointEnd   = int64(11)
)

// TestImportGraph_RejectsInvalidEntityWire: a node, node-history, relationship
// or relationship-history record whose wire fields are corrupt (reserved token
// 0, a negative or out-of-uint16 token, a bad id, version or base id, temporal
// fields without the flag, an empty or reversed valid range, a repeated label,
// malformed properties) is refused with ErrCorruptExport before construction.
//
// Each stream is otherwise admissible: the header counts match the records,
// every token the valid rows use is in the registry, and integrity hashes are
// off so a record without a hash is accepted. The "valid" row of each record
// kind imports with no error, so a rejection row passes only because the
// field it names is refused. A primary or extra label of 70000 is refused by
// the wire check and again by the registry-size check (the registry cannot
// hold 70000 labels); either refusal is the behaviour under test.
func TestImportGraph_RejectsInvalidEntityWire(t *testing.T) {
	t.Parallel()

	id := snowflakeIDForTest()
	node := func(w storeutil.NodeWire) storeutil.NodeWire { return w }
	rel := func(w storeutil.RelWire) storeutil.RelWire {
		if w.StartID == 0 && w.EndID == 0 {
			w.StartID, w.EndID = wireRejectEndpointStart, wireRejectEndpointEnd
		}
		return w
	}
	int64Prop := func(k string, v int64) storeutil.PropertyWire {
		return storeutil.PropertyWire{Key: k, Value: v, Type: storeutil.PropertyTypeTag(v)}
	}

	tests := []struct {
		name  string
		tag   byte
		wire  any // storeutil.NodeWire or storeutil.RelWire
		valid bool
	}{
		// Current node records.
		{name: "node valid", tag: exportTagNode, valid: true,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, ExtraLabels: []int{2, 3}})},
		{name: "node zero primary label", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 0})},
		{name: "node negative primary label", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: -1})},
		{name: "node out-of-range primary label", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 70000})},
		{name: "node zero extra label", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, ExtraLabels: []int{2, 0, 3}})},
		{name: "node negative extra label", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, ExtraLabels: []int{-2}})},
		{name: "node out-of-range extra label", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, ExtraLabels: []int{70000}})},
		{name: "node extra duplicates primary", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, ExtraLabels: []int{1}})},
		{name: "node duplicate extras", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, ExtraLabels: []int{2, 2}})},
		{name: "node zero id", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: 0, PrimaryLabel: 1})},
		{name: "node negative id", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: -1, PrimaryLabel: 1})},
		{name: "node negative version", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, Version: -1})},
		{name: "node negative base entity id", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, HasTemporal: true, BaseEntityID: -1})},
		{name: "node temporal payload without flag", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, TxFrom: 10})},
		{name: "node empty temporal range", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, HasTemporal: true, ValidFrom: 20, ValidTo: 20})},
		{name: "node reversed temporal range", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, HasTemporal: true, ValidFrom: 30, ValidTo: 20})},
		{name: "node valid properties", tag: exportTagNode, valid: true,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1,
				Properties: []storeutil.PropertyWire{int64Prop("a", 1), int64Prop("z", 2)}})},
		{name: "node reserved shadow key", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, Properties: []storeutil.PropertyWire{{
				Key: types.ShadowHash, Value: "spoofed", Type: storeutil.PropertyTypeTag("spoofed"),
			}}})},
		{name: "node unsorted property keys", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1,
				Properties: []storeutil.PropertyWire{int64Prop("z", 1), int64Prop("a", 2)}})},
		{name: "node duplicate property keys", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1,
				Properties: []storeutil.PropertyWire{int64Prop("a", 1), int64Prop("a", 2)}})},
		{name: "node unknown property type tag", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, Properties: []storeutil.PropertyWire{{
				Key: "a", Value: "value", Type: 255,
			}}})},
		{name: "node lossy unsigned property type tag", tag: exportTagNode,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, Properties: []storeutil.PropertyWire{{
				Key: "a", Value: int64(-1), Type: storeutil.PropertyTypeTag(uint8(0)),
			}}})},

		// Node history records.
		{name: "node history valid", tag: exportTagNodeHist, valid: true,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 1, Version: 1})},
		{name: "node history zero primary label", tag: exportTagNodeHist,
			wire: node(storeutil.NodeWire{ID: id, PrimaryLabel: 0, Version: 1})},

		// Current relationship records.
		{name: "rel valid", tag: exportTagRel, valid: true,
			wire: rel(storeutil.RelWire{ID: id, RelType: 1})},
		{name: "rel zero type", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: id, RelType: 0})},
		{name: "rel negative type", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: id, RelType: -1})},
		{name: "rel out-of-range type", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: id, RelType: 70000})},
		{name: "rel zero id", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: 0, RelType: 1})},
		{name: "rel negative id", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: -1, RelType: 1})},
		{name: "rel zero start", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: id, RelType: 1, StartID: 0, EndID: wireRejectEndpointEnd})},
		{name: "rel zero end", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: id, RelType: 1, StartID: wireRejectEndpointStart, EndID: 0})},
		{name: "rel negative version", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: id, RelType: 1, Version: -1})},
		{name: "rel negative base entity id", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: id, RelType: 1, HasTemporal: true, BaseEntityID: -1})},
		{name: "rel temporal payload without flag", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: id, RelType: 1, TxFrom: 10})},
		{name: "rel empty temporal range", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: id, RelType: 1, HasTemporal: true, ValidFrom: 20, ValidTo: 20})},
		{name: "rel reversed temporal range", tag: exportTagRel,
			wire: rel(storeutil.RelWire{ID: id, RelType: 1, HasTemporal: true, ValidFrom: 30, ValidTo: 20})},

		// Relationship history records.
		{name: "rel history valid", tag: exportTagRelHist, valid: true,
			wire: rel(storeutil.RelWire{ID: id, RelType: 1, Version: 1})},
		{name: "rel history zero type", tag: exportTagRelHist,
			wire: rel(storeutil.RelWire{ID: id, RelType: 0, Version: 1})},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body, err := msgpack.Marshal(tc.wire)
			if err != nil {
				t.Fatalf("marshal record: %v", err)
			}

			var records bytes.Buffer
			var nodeCount, relCount int64
			if tc.tag == exportTagRel || tc.tag == exportTagRelHist {
				for _, endpoint := range []int64{wireRejectEndpointStart, wireRejectEndpointEnd} {
					nb, err := msgpack.Marshal(&storeutil.NodeWire{ID: endpoint, PrimaryLabel: 1})
					if err != nil {
						t.Fatalf("marshal endpoint node: %v", err)
					}
					writeImportRecord(&records, exportTagNode, nb)
					nodeCount++
				}
			}
			writeImportRecord(&records, tc.tag, body)
			switch tc.tag {
			case exportTagNode:
				nodeCount++
			case exportTagRel:
				relCount++
			}

			var buf bytes.Buffer
			buf.Write(validImportPreludeWithCounts(t, nodeCount, relCount))
			buf.Write(records.Bytes())

			g, err := New(Config{Store: memory.New(), DisableIntegrityHashes: true})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close() //nolint:errcheck

			importErr := runImportSafely(t, g, &buf)
			if tc.valid {
				if importErr != nil {
					t.Fatalf("ImportGraph of the valid record: %v", importErr)
				}
				return
			}
			if !errors.Is(importErr, ErrCorruptExport) {
				t.Fatalf("ImportGraph: got %v, want ErrCorruptExport", importErr)
			}
		})
	}
}

func TestImportGraph_RejectsPropertiesOverDestinationValidationLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tag  byte
		body func(t *testing.T) []byte
		want error
	}{
		{
			name: "node too many properties",
			tag:  exportTagNode,
			body: func(t *testing.T) []byte {
				t.Helper()
				b, err := msgpack.Marshal(&storeutil.NodeWire{
					ID:           snowflakeIDForTest(),
					PrimaryLabel: 1,
					Properties: []storeutil.PropertyWire{
						{Key: "a", Value: int64(1), Type: storeutil.PropertyTypeTag(int64(1))},
						{Key: "b", Value: int64(2), Type: storeutil.PropertyTypeTag(int64(2))},
					},
				})
				if err != nil {
					t.Fatalf("marshal node: %v", err)
				}
				return b
			},
			want: ErrTooManyProperties,
		},
		{
			name: "node nested oversized string",
			tag:  exportTagNode,
			body: func(t *testing.T) []byte {
				t.Helper()
				value := map[string]any{"nested": []any{"toolong"}}
				b, err := msgpack.Marshal(&storeutil.NodeWire{
					ID:           snowflakeIDForTest(),
					PrimaryLabel: 1,
					Properties: []storeutil.PropertyWire{{
						Key:   "a",
						Value: value,
						Type:  storeutil.PropertyTypeTag(value),
					}},
				})
				if err != nil {
					t.Fatalf("marshal node: %v", err)
				}
				return b
			},
			want: ErrValueTooLarge,
		},
		{
			name: "rel history nested oversized map key",
			tag:  exportTagRelHist,
			body: func(t *testing.T) []byte {
				t.Helper()
				value := map[string]any{"toolong": true}
				b, err := msgpack.Marshal(&storeutil.RelWire{
					ID:      snowflakeIDForTest(),
					RelType: 1,
					StartID: snowflakeIDForTest() + 1,
					EndID:   snowflakeIDForTest() + 2,
					Version: 1,
					Properties: []storeutil.PropertyWire{{
						Key:   "a",
						Value: value,
						Type:  storeutil.PropertyTypeTag(value),
					}},
				})
				if err != nil {
					t.Fatalf("marshal rel: %v", err)
				}
				return b
			},
			want: ErrValueTooLarge,
		},
		{
			name: "node custom property decoded oversized string",
			tag:  exportTagNode,
			body: func(t *testing.T) []byte {
				t.Helper()
				value := sizeLimitCustomProperty{Name: "xxxx"}
				if err := types.RegisterPropertyStructType(sizeLimitCustomProperty{}); err != nil {
					t.Fatalf("RegisterPropertyStructType: %v", err)
				}
				typeName, pointer, ok := types.RegisteredPropertyStructWireType(value)
				if !ok {
					t.Fatal("RegisteredPropertyStructWireType returned ok=false")
				}
				data, err := msgpack.Marshal(value)
				if err != nil {
					t.Fatalf("marshal custom property: %v", err)
				}
				b, err := msgpack.Marshal(&storeutil.NodeWire{
					ID:           snowflakeIDForTest(),
					PrimaryLabel: 1,
					Properties: []storeutil.PropertyWire{{
						Key:           "a",
						Value:         data,
						Type:          storeutil.PropertyTypeTag(value),
						CustomType:    typeName,
						CustomPointer: pointer,
					}},
				})
				if err != nil {
					t.Fatalf("marshal node: %v", err)
				}
				return b
			},
			want: ErrValueTooLarge,
		},
		{
			name: "rel custom property decoded oversized string",
			tag:  exportTagRel,
			body: func(t *testing.T) []byte {
				t.Helper()
				value := sizeLimitCustomProperty{Name: "xxxx"}
				if err := types.RegisterPropertyStructType(sizeLimitCustomProperty{}); err != nil {
					t.Fatalf("RegisterPropertyStructType: %v", err)
				}
				typeName, pointer, ok := types.RegisteredPropertyStructWireType(value)
				if !ok {
					t.Fatal("RegisteredPropertyStructWireType returned ok=false")
				}
				data, err := msgpack.Marshal(value)
				if err != nil {
					t.Fatalf("marshal custom property: %v", err)
				}
				b, err := msgpack.Marshal(&storeutil.RelWire{
					ID:      snowflakeIDForTest(),
					RelType: 1,
					StartID: snowflakeIDForTest() + 1,
					EndID:   snowflakeIDForTest() + 2,
					Properties: []storeutil.PropertyWire{{
						Key:           "a",
						Value:         data,
						Type:          storeutil.PropertyTypeTag(value),
						CustomType:    typeName,
						CustomPointer: pointer,
					}},
				})
				if err != nil {
					t.Fatalf("marshal rel: %v", err)
				}
				return b
			},
			want: ErrValueTooLarge,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			buf.Write(validImportPrelude(t))
			writeImportRecord(&buf, tc.tag, tc.body(t))

			g, err := New(Config{
				Store: memory.New(),
				Validation: ValidationLimits{
					MaxPropertiesPerEntity: 1,
					MaxPropertyValueSize:   3,
				},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer g.Close() //nolint:errcheck

			importErr := runImportSafely(t, g, &buf)
			if importErr == nil {
				t.Fatal("ImportGraph: expected validation error, got nil")
			}
			if !errors.Is(importErr, tc.want) {
				t.Errorf("ImportGraph: got %v, want %v", importErr, tc.want)
			}
		})
	}
}

// --- RunRepair Phase 2 must not silently swallow operational errors ---
//
// The bug: r, err := ns.store.GetRelationship(relID); if err != nil { continue }
// This conflates storepkg.ErrRelNotFound (legitimate skip) with operational errors
// (closed shard, I/O failure, routing failure) — leaving real corruption
// hiding behind a "Repair succeeded" return.
//
// Approach: propagate operational errors from RunRepair so callers can
// distinguish "repair clean" from "repair could not complete safely".

// TestRunRepair_PropagatesOperationalReadError: in Phase 2, RunRepair calls
// ns.store.GetRelationship(relID) for every rel. If that read returns a
// non-storepkg.ErrRelNotFound error (real I/O failure / closed shard / routing
// failure), the original code silently `continue`s — the repair returns
// success while genuinely needed in/-index repairs were missed.
//
// Expected behaviour after the fix: the operational error surfaces as the
// return error from RunRepair so the caller knows the scan was incomplete.
func TestRunRepair_PropagatesOperationalReadError(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()

	g, ts := newTestTieredGraph(t)

	// Build a cross-shard relationship whose ENTITY lives on the hot
	// event shard (the closeable target). Signal→Case routes:
	//   startShard = hotEventShard (Signal, event-classified)
	//   endShard   = refShard      (Case, reference-classified)
	// PutRelationship Section 12 ordering writes entity+out on startShard
	// (hot event), then in/ on endShard (ref). So the rel ENTITY lives on
	// the hot event shard — which is the shard we'll fault-inject below.
	signalNode, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	caseNode, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.Rels.Add(context.Background(), "LINK", signalNode, caseNode, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Inject an operational-class fault into the rel's entity shard so
	// Phase 2's GetRelationship surfaces a non-storepkg.ErrRelNotFound error.
	// Pre-fix code did `if err != nil { continue }` and silently swallowed
	// this — returning tiered.RepairResult success while genuinely needed in/-index
	// repairs were missed. Post-fix code must propagate.
	//
	// We cannot swap the badger.Store wholesale (RunRepair calls non-Store
	// methods on the concrete *badger.Store). The chosen fault: corrupt the
	// rel's stored msgpack bytes. After evicting it from the LRU cache,
	// GetRelationship cache-misses, finds the rel ID in the in-memory index,
	// reads garbage from Badger, and surfaces the unmarshal error — exactly
	// the "operational, non-storepkg.ErrRelNotFound" class the fix must propagate.
	ts.MuForTest().RLock()
	hot := ts.HotShardForTest()
	ts.MuForTest().RUnlock()
	if hot == nil || hot.Store() == nil {
		t.Fatal("hot shard store missing — cannot inject fault")
	}
	originalStore := hot.Store()
	if !originalStore.HasRelID(rel.ID().SnowflakeID()) {
		// Sanity: the rel entity must be on the hot shard for this test
		// to exercise the bug. If routing has changed, the test setup
		// needs updating, not the production code.
		t.Fatal("rel entity is not on hot event shard — fault would land elsewhere; revisit Section 12 routing or test setup")
	}
	corruptRelBytesOnDisk(t, originalStore, rel.ID())

	res, err := ts.RunRepair()
	if err == nil {
		t.Fatalf("RunRepair: got nil error, want operational error to be propagated. Result: %+v", res)
	}
	// Must NOT be an storepkg.ErrRelNotFound — that's the legitimate-skip sentinel
	// the original code conflated with operational errors. The fix must
	// surface real failures distinctly.
	if errors.Is(err, storepkg.ErrRelNotFound) {
		t.Errorf("RunRepair returned storepkg.ErrRelNotFound; the fix must surface operational errors as themselves, not as the legitimate-skip sentinel")
	}
}

// corruptRelBytesOnDisk forces a non-storepkg.ErrRelNotFound failure path on a
// subsequent GetRelationship for relID against bs:
//  1. Flush pending writes so the rel value is durable.
//  2. Evict the rel from the relCache so the read falls through to Badger.
//  3. Overwrite the rel's Badger value with non-msgpack bytes — the next
//     read will surface a msgpack unmarshal error (operational class).
//
// The test cannot use storepkg.ErrRelNotFound (legitimate-skip sentinel); it
// specifically needs an operational-class error to exercise the propagation.
func corruptRelBytesOnDisk(t *testing.T, bs *badger.Store, relID types.RelID) {
	t.Helper()

	// 1. Flush pending so the value is in Badger, not just the WriteBatch.
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush before corruption: %v", err)
	}

	// 2. Evict the rel from the LRU so cacheHit cannot short-circuit the
	//    read. The cache holds a clean copy after PutRelationship/flush;
	//    GetRelationship's cacheHit path returns success even if Badger
	//    is corrupted. Use EvictForTest instead of reaching into LRU
	//    internals.
	id := relID.SnowflakeID()
	bs.RelCacheForTest().EvictForTest(id)

	// 3. Overwrite the Badger value with non-msgpack bytes.
	err := bs.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.RelKey(id), []byte{0xFF, 0xFE, 0xFD, 0xFC})
	})
	if err != nil {
		t.Fatalf("corrupt rel bytes: %v", err)
	}
}

// staleRelIDInAllRelIDs creates the divergent state RunRepair Phase 2
// observes when a rel is deleted between AllRelIDs and GetRelationship:
//   - bs.relIDs still contains the rel (so AllRelIDs surfaces it)
//   - relCache no longer holds it (so cacheHit cannot satisfy the read)
//   - the Badger value is gone (so the disk read returns key-not-found)
//
// Without this divergence, Phase 2 would see a healthy rel and never
// hit the storepkg.ErrRelNotFound `continue` branch — the test would then pass
// regardless of whether the fix's errors.Is gate is correct.
func staleRelIDInAllRelIDs(t *testing.T, bs *badger.Store, relID types.RelID) {
	t.Helper()

	// Flush so the rel is persisted to Badger.
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush before stale-id setup: %v", err)
	}

	// Delete the Badger key and leave a delete tombstone in the cache, as a
	// delete that ran after AllRelIDs leaves them: GetRelationship answers
	// storepkg.ErrRelNotFound from the tombstone. (A missing row with no
	// tombstone and the ID still listed reads as a damaged table, v4.84.0.)
	id := relID.SnowflakeID()
	err := bs.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Delete(storeutil.RelKey(id))
	})
	if err != nil {
		t.Fatalf("delete rel key for stale-id setup: %v", err)
	}
	bs.RelCacheForTest().MarkDeleted(id)
	if _, err := bs.GetRelationship(relID); !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("stale-id setup: GetRelationship = %v, want ErrRelNotFound", err)
	}
	// Note: bs.relIDs still has the entry. AllRelIDs reads from that
	// in-memory set, so the rel will still be enumerated. This is the
	// divergence that simulates the AllRelIDs-then-delete race.
}

// TestRunRepair_SkipsLegitimateRelNotFound: a Phase 2 read that returns
// storepkg.ErrRelNotFound (rel deleted between AllRelIDs and GetRelationship — a
// legitimate TOCTOU race) must NOT be propagated. RunRepair should still
// complete successfully. The `staleRelIDInAllRelIDs` helper engineers
// the exact divergence — without it we'd be testing the happy path,
// not the errors.Is(err, storepkg.ErrRelNotFound) gate.
func TestRunRepair_SkipsLegitimateRelNotFound(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	g, ts := newTestTieredGraph(t)

	// Build a cross-shard rel so Phase 2 enters the GetRelationship loop.
	// (Same-shard rels short-circuit before the read.)
	caseRef, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}
	signalEvt, err := g.Nodes.Add(context.Background(), []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode signal: %v", err)
	}
	rel, err := g.Rels.Add(context.Background(), "TOUCHES", caseRef, signalEvt, nil)
	if err != nil {
		t.Fatalf("AddRelationship cross-shard: %v", err)
	}

	// Engineer the divergence: rel still in AllRelIDs, but
	// GetRelationship will return storepkg.ErrRelNotFound. The owner shard for a
	// cross-shard ref→event rel is refShard.
	staleRelIDInAllRelIDs(t, ts.RefShardForTest(), rel.InternalID())

	// RunRepair must NOT propagate storepkg.ErrRelNotFound — that's the fix's
	// legitimate-skip class.
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair must skip storepkg.ErrRelNotFound silently; got error %v", err)
	}
	if res == nil {
		t.Fatal("RunRepair returned nil result")
	}
}
