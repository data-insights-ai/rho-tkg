package graphapply

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

// Representation fixtures exercise codecs only; they are not native read,
// placement, quorum, or completed handoff evidence.
func rangeMoveCodecIntent() rangeMoveIntent {
	source := allocationScope{graph: [16]byte{1}, partition: 3, group: [16]byte{4}, ownership: 5, topology: 6, declaration: [32]byte{7}, semantic: rangeMoveSemanticContractID()}
	target := source
	target.partition, target.group = 8, [16]byte{9}
	return rangeMoveIntent{
		id: requestID{10}, source: source, target: target,
		publication: graphstore.RangePublication{Graph: graphstate.GraphID(source.graph), RangeID: 17, First: 17, Last: 32, InitialPartition: 3, InitialEpoch: 2, Configuration: [32]byte{11}, SourcePartition: 3, SourceIndex: 12},
		epoch:       2, maxPages: 16, pageRows: 8, pageBytes: 4096,
	}
}

func rangeMoveCodecStatus(t *testing.T) rangeMoveStatus {
	t.Helper()
	intent := rangeMoveCodecIntent()
	wire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	ns := namespace{intent.source.graph, intent.source.partition}
	return rangeMoveStatus{ns: ns, move: rangeMoveID(ns, intent.id), intent: sha256.Sum256(wire), phase: rangeMoveCapture, index: 20, floor: 13, round: 14, sequence: 2, rows: 3, size: 128, digest: [32]byte{15}, result: rangeMoveApplied}
}

func rangeMoveCodecReseal(wire []byte) []byte {
	return seal(bytes.Clone(wire[:len(wire)-32]))
}

func TestRangeMoveIdentityQualifiesOriginalSource(t *testing.T) {
	intent := rangeMoveCodecIntent()
	ns := namespace{intent.source.graph, intent.source.partition}
	id := rangeMoveID(ns, intent.id)
	// Independent literal framing pins source graph, partition, and retry key.
	b := append([]byte("rho-tkg:range-move-id:v1\x00"), ns.graph[:]...)
	b = binary.BigEndian.AppendUint64(b, ns.partition)
	b = append(b, intent.id[:]...)
	want := sha256.Sum256(b)
	if id != [16]byte(want[:16]) || id == ([16]byte{}) {
		t.Fatal("move identity changed qualified original framing")
	}
	for _, other := range []namespace{{ns.graph, ns.partition + 1}, {[16]byte{2}, ns.partition}} {
		if rangeMoveID(other, intent.id) == id {
			t.Fatal("distinct source namespaces collided", other)
		}
	}
	changed := intent
	changed.target.partition++
	changed.publication.Last++
	changed.pageBytes++
	if rangeMoveID(namespace{changed.source.graph, changed.source.partition}, changed.id) != id {
		t.Fatal("retry payload changed original identity")
	}
	if rangeMoveID(ns, requestID{11}) == id {
		t.Fatal("different retry key reused original identity")
	}
	key := rangeMoveReplayKey(ns, 3, id, 1)
	for _, other := range [][]byte{rangeMoveReplayKey(ns, 8, id, 1), rangeMoveReplayKey(ns, 3, id, 2), rangeMoveReplayKey(namespace{ns.graph, 8}, 3, id, 1)} {
		if bytes.Equal(key, other) {
			t.Fatal("replay namespace dropped source, recipient or record kind")
		}
	}
}

func TestRangeMoveCodecCanonicalRoundTrips(t *testing.T) {
	intent := rangeMoveCodecIntent()
	intentWire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	gotIntent, err := decodeRangeMoveIntent(intentWire)
	if err != nil || gotIntent != intent {
		t.Fatal("intent dropped reservation or admission fields", gotIntent, err)
	}
	status := rangeMoveCodecStatus(t)
	for phase := rangeMoveFence; phase <= rangeMoveAbort; phase++ {
		status.phase = phase
		wire, err := encodeRangeMoveStatus(status)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeRangeMoveStatus(wire)
		if err != nil || got != status {
			t.Fatal("phase status changed capture/progress coordinates", phase, got, err)
		}
	}
	for result := rangeMoveApplied; result <= rangeMoveLimit; result++ {
		status.result = result
		wire, err := encodeRangeMoveStatus(status)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := decodeRangeMoveStatus(wire); err != nil || got != status {
			t.Fatal("native outcome changed", result, got, err)
		}
	}
	command := rangeMoveCommand{scope: intent.target, phase: rangeMoveImport, intent: intentWire, sequence: 2, data: []byte{1, 2, 3}, proofs: []rangeMoveStatus{rangeMoveCodecStatus(t)}}
	wire, err := encodeRangeMoveCommand(command)
	if err != nil || !isRangeMoveWire(wire) || isRangeMoveWire(nil) || isRangeMoveWire([]byte("GMQ\x02")) {
		t.Fatal("command wire discriminator", err)
	}
	got, err := decodeRangeMoveCommand(wire)
	if err != nil || !reflect.DeepEqual(got, command) {
		t.Fatal("command lost imported data or proof coordinates", got, err)
	}
	canonical, err := encodeRangeMoveCommand(got)
	if err != nil || !bytes.Equal(wire, canonical) {
		t.Fatal("command has multiple encodings", err)
	}
}

func TestRangeMoveIntentRejectsChangedScopeAndUnboundedAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*rangeMoveIntent)
	}{
		{"missing-retry", func(r *rangeMoveIntent) { r.id = requestID{} }},
		{"same-owner", func(r *rangeMoveIntent) { r.target = r.source }},
		{"another-graph", func(r *rangeMoveIntent) { r.target.graph[0] ^= 1 }},
		{"another-declaration", func(r *rangeMoveIntent) { r.target.declaration[0] ^= 1 }},
		{"another-topology", func(r *rangeMoveIntent) { r.target.topology++ }},
		{"old-semantic-binding", func(r *rangeMoveIntent) {
			r.source.semantic = certifiedGraphSemanticContractID()
			r.target.semantic = r.source.semantic
		}},
		{"foreign-publication", func(r *rangeMoveIntent) { r.publication.Graph[0] ^= 1 }},
		{"invalid-reservation", func(r *rangeMoveIntent) { r.publication.Last = r.publication.First - 1 }},
		{"past-placement-epoch", func(r *rangeMoveIntent) { r.epoch = r.publication.InitialEpoch - 1 }},
		{"overflow-placement-epoch", func(r *rangeMoveIntent) { r.epoch = math.MaxUint64 }},
		{"zero-pages", func(r *rangeMoveIntent) { r.maxPages = 0 }},
		{"excess-pages", func(r *rangeMoveIntent) { r.maxPages = 1025 }},
		{"negative-rows", func(r *rangeMoveIntent) { r.pageRows = -1 }},
		{"excess-rows", func(r *rangeMoveIntent) { r.pageRows = 257 }},
		{"zero-bytes", func(r *rangeMoveIntent) { r.pageBytes = 0 }},
		{"excess-bytes", func(r *rangeMoveIntent) { r.pageBytes = (1 << 20) + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := rangeMoveCodecIntent()
			tc.edit(&bad)
			if bad.valid() {
				t.Fatal("invalid intent passed validator")
			}
			if wire, err := encodeRangeMoveIntent(bad); wire != nil || !errors.Is(err, errInvalid) {
				t.Fatal("invalid intent encoded", err)
			}
		})
	}
	boundary := rangeMoveCodecIntent()
	boundary.epoch, boundary.maxPages, boundary.pageRows, boundary.pageBytes = math.MaxUint64-1, 1024, 256, 1<<20
	wire, err := encodeRangeMoveIntent(boundary)
	if err != nil {
		t.Fatal("exact finite boundary refused", err)
	}
	if got, err := decodeRangeMoveIntent(wire); err != nil || got != boundary {
		t.Fatal("finite boundary changed", err)
	}
}

func TestRangeMoveCodecRefusesMalformedFramesWithoutPartialValues(t *testing.T) {
	intent, err := encodeRangeMoveIntent(rangeMoveCodecIntent())
	if err != nil {
		t.Fatal(err)
	}
	status, err := encodeRangeMoveStatus(rangeMoveCodecStatus(t))
	if err != nil {
		t.Fatal(err)
	}
	command, err := encodeRangeMoveCommand(rangeMoveCommand{scope: rangeMoveCodecIntent().target, phase: rangeMoveImport, intent: intent, sequence: 1, data: []byte{3}, proofs: []rangeMoveStatus{rangeMoveCodecStatus(t)}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		wire   []byte
		max    int
		decode func([]byte) (bool, error)
	}{
		{"intent", intent, 1024, func(b []byte) (bool, error) { v, err := decodeRangeMoveIntent(b); return v == (rangeMoveIntent{}), err }},
		{"status", status, 256, func(b []byte) (bool, error) { v, err := decodeRangeMoveStatus(b); return v == (rangeMoveStatus{}), err }},
		{"command", command, 1 << 20, func(b []byte) (bool, error) {
			v, err := decodeRangeMoveCommand(b)
			return reflect.DeepEqual(v, rangeMoveCommand{}), err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for n := range len(tc.wire) {
				if zero, err := tc.decode(tc.wire[:n]); !zero || !errors.Is(err, errCorrupt) {
					t.Fatal("truncation returned partial fields", n, err)
				}
			}
			badSeal := bytes.Clone(tc.wire)
			badSeal[len(badSeal)-1] ^= 1
			wrongVersion := bytes.Clone(tc.wire)
			wrongVersion[3]++
			trailing := seal(append(bytes.Clone(tc.wire[:len(tc.wire)-32]), 0))
			for _, bad := range [][]byte{badSeal, rangeMoveCodecReseal(wrongVersion), trailing} {
				if zero, err := tc.decode(bad); !zero || !errors.Is(err, errCorrupt) {
					t.Fatal("noncanonical frame returned fields", err)
				}
			}
			if zero, err := tc.decode(make([]byte, tc.max+1)); !zero || !errors.Is(err, errLimit) {
				t.Fatal("outer bound did not refuse before decode", err)
			}
		})
	}
	// Resealing prevents the checksum from hiding parser/semantic failures.
	for _, tc := range []struct {
		name   string
		wire   []byte
		offset int
		value  uint32
		decode func([]byte) error
	}{
		{"publication-length", intent, 260, math.MaxUint32, func(b []byte) error { _, err := decodeRangeMoveIntent(b); return err }},
		{"command-intent-length", command, 133, math.MaxUint32, func(b []byte) error { _, err := decodeRangeMoveCommand(b); return err }},
		{"proof-count", command, 141 + len(intent) + 1, 3, func(b []byte) error { _, err := decodeRangeMoveCommand(b); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := bytes.Clone(tc.wire)
			binary.BigEndian.PutUint32(bad[tc.offset:tc.offset+4], tc.value)
			if err := tc.decode(rangeMoveCodecReseal(bad)); !errors.Is(err, errLimit) {
				t.Fatal("untrusted field length/count admitted", err)
			}
		})
	}
	for _, tc := range []struct {
		offset int
		value  byte
	}{{76, 0}, {76, byte(rangeMoveAbort + 1)}, {77, 0}, {77, rangeMoveLimit + 1}} {
		bad := bytes.Clone(status)
		bad[tc.offset] = tc.value
		if got, err := decodeRangeMoveStatus(rangeMoveCodecReseal(bad)); got != (rangeMoveStatus{}) || !errors.Is(err, errCorrupt) {
			t.Fatal("unknown phase/result became native evidence", tc, err)
		}
	}
}

func TestRangeMoveCommandRejectsForeignScopeAndCaps(t *testing.T) {
	intent := rangeMoveCodecIntent()
	wire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*rangeMoveCommand)
	}{
		{"foreign-recipient", func(c *rangeMoveCommand) { c.scope.partition = 99 }},
		{"foreign-binding", func(c *rangeMoveCommand) { c.scope.semantic = certifiedGraphSemanticContractID() }},
		{"unknown-phase", func(c *rangeMoveCommand) { c.phase = rangeMoveAbort + 1 }},
		{"page-outside-intent", func(c *rangeMoveCommand) { c.sequence = uint64(intent.maxPages) + 1 }},
		{"oversized-data", func(c *rangeMoveCommand) { c.data = make([]byte, (1<<20)-2047) }},
		{"excess-evidence", func(c *rangeMoveCommand) { c.proofs = make([]rangeMoveStatus, 3) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := rangeMoveCommand{scope: intent.source, phase: rangeMoveFence, intent: wire}
			tc.edit(&command)
			if out, err := encodeRangeMoveCommand(command); out != nil || !errors.Is(err, errInvalid) {
				t.Fatal("invalid command returned wire", err)
			}
		})
	}
}
