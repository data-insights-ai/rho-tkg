package storeutil

import (
	"bytes"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 43 (retraction): the marker a Retract door writes on its tombstone
// (types.TemporalMetadata.Retracted, wire key "rx") must survive every wire
// door a row passes through — the full checked decode (badger rows, export /
// import, change-feed bodies, replica apply), the selection-scope partial
// decode and its scanner (badger point doors and timelines read skeletons),
// and the history delta (badger HistoryDeltaEncoding). A door that drops it
// degrades a retraction to a plain Delete (the past stays readable at pins
// after the retraction). Every case names that faulty implementation.

func retractedNodeTombstone(t *testing.T) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(4242)), 1, []uint16{2})
	n.SetVersion(3)
	n.SetProperties(mustPropertySlice(t, map[string]any{"name": "wrong record", "n": int64(7)}))
	n.SetTemporal(&types.TemporalMetadata{
		ValidFrom: 100, ValidTo: 900, TxFrom: 300, TxTo: 900,
		UpdatedAt: 300, DeletedAt: 900, Retracted: true,
	})
	n.SetIntegrity(&types.NodeIntegrity{Hash: "abc123", PrevHash: "def456"})
	return n
}

func retractedRelTombstone(t *testing.T) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(types.RelID(snowflake.ID(4243)), 1, types.NodeID(snowflake.ID(10)), types.NodeID(snowflake.ID(12)))
	r.SetVersion(2)
	r.SetProperties(mustPropertySlice(t, map[string]any{"w": int64(1)}))
	r.SetTemporal(&types.TemporalMetadata{
		ValidFrom: 100, ValidTo: 900, TxFrom: 300, TxTo: 900,
		DeletedAt: 900, Retracted: true,
	})
	r.SetIntegrity(&types.RelIntegrity{Hash: "abc123"})
	return r
}

// TestRetractedMarker_FullWireRoundTrip catches an encoder or checked decoder
// that drops the marker ("rx" not emitted, or not read back), node and rel.
func TestRetractedMarker_FullWireRoundTrip(t *testing.T) {
	t.Parallel()
	n := retractedNodeTombstone(t)
	buf, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatalf("MarshalNodeWire: %v", err)
	}
	var w NodeWire
	if err := SafeUnmarshal(buf, &w); err != nil {
		t.Fatalf("decode: %v", err)
	}
	back, err := WireToNodeChecked(w)
	if err != nil {
		t.Fatalf("WireToNodeChecked: %v", err)
	}
	if back.Temporal() == nil || !back.Temporal().Retracted {
		t.Fatalf("node marker lost in the full wire round trip: %+v", back.Temporal())
	}

	r := retractedRelTombstone(t)
	rbuf, err := MarshalRelWire(r)
	if err != nil {
		t.Fatalf("MarshalRelWire: %v", err)
	}
	var rw RelWire
	if err := SafeUnmarshal(rbuf, &rw); err != nil {
		t.Fatalf("decode rel: %v", err)
	}
	rback, err := WireToRelChecked(rw)
	if err != nil {
		t.Fatalf("WireToRelChecked: %v", err)
	}
	if rback.Temporal() == nil || !rback.Temporal().Retracted {
		t.Fatalf("relationship marker lost in the full wire round trip: %+v", rback.Temporal())
	}
}

// TestRetractedMarker_PlainRowBytesUnchanged is a GUARD (it holds before the
// marker exists): a row without the marker encodes exactly as before, so
// every stored row, golden vector and change-feed record is unaffected. It
// catches an encoder that always emits "rx" (a wire change for every row).
func TestRetractedMarker_PlainRowBytesUnchanged(t *testing.T) {
	t.Parallel()
	n := retractedNodeTombstone(t)
	n.Temporal().Retracted = false
	plain, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatalf("MarshalNodeWire: %v", err)
	}
	if bytes.Contains(plain, []byte{0xa2, 'r', 'x'}) {
		t.Fatalf("plain tombstone carries the rx key: %x", plain)
	}
	n.Temporal().Retracted = true
	marked, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatalf("MarshalNodeWire: %v", err)
	}
	if !bytes.Contains(marked, []byte{0xa2, 'r', 'x', 0xc3}) {
		t.Fatalf("retracted tombstone does not carry rx=true: %x", marked)
	}
	// The v2 fixed-width tail stays the last two entries (the ingest applier
	// patches it by offset from the end).
	tf, tt, ok := PeekWireTemporalTail(marked)
	if !ok || tf != 300 || tt != 900 {
		t.Fatalf("v2 tail after rx: tf=%d tt=%d ok=%v, want 300/900/true", tf, tt, ok)
	}
}

// TestRetractedMarker_PartialDecode catches a selection-scope decoder (the
// skeleton path of the badger point doors and timelines) that drops the
// marker: the scanner arm, the SafeUnmarshal arm, and the delta Meta arm.
func TestRetractedMarker_PartialDecode(t *testing.T) {
	t.Parallel()
	n := retractedNodeTombstone(t)
	buf, err := MarshalNodeWire(n)
	if err != nil {
		t.Fatalf("MarshalNodeWire: %v", err)
	}
	scanned, ok := scanWireTemporalMeta(buf)
	if !ok {
		t.Fatal("scanner declined a well-formed retracted row")
	}
	if !scanned.Retracted {
		t.Fatalf("scanner dropped the marker: %+v", scanned)
	}
	ref, err := referenceDecodeTemporalMeta(buf)
	if err != nil {
		t.Fatalf("reference decode: %v", err)
	}
	if !ref.Retracted {
		t.Fatalf("SafeUnmarshal partial decode dropped the marker: %+v", ref)
	}
	_, tm, err := DecodeWireTemporalMeta(buf)
	if err != nil {
		t.Fatalf("DecodeWireTemporalMeta: %v", err)
	}
	if tm == nil || !tm.Retracted {
		t.Fatalf("DecodeWireTemporalMeta dropped the marker: %+v", tm)
	}
	rbuf, err := MarshalRelWire(retractedRelTombstone(t))
	if err != nil {
		t.Fatalf("MarshalRelWire: %v", err)
	}
	_, rtm, err := DecodeWireTemporalMeta(rbuf)
	if err != nil {
		t.Fatalf("DecodeWireTemporalMeta(rel): %v", err)
	}
	if rtm == nil || !rtm.Retracted {
		t.Fatalf("DecodeWireTemporalMeta dropped the relationship marker: %+v", rtm)
	}
}

// TestRetractedMarker_HistoryDelta catches a delta encoding (badger
// HistoryDeltaEncoding) that drops the marker from a tombstone stored as a
// delta, on the full reconstruction and on the delta's selection Meta.
func TestRetractedMarker_HistoryDelta(t *testing.T) {
	t.Parallel()
	anchor := retractedNodeTombstone(t)
	anchor.Temporal().Retracted = false
	anchor.Temporal().DeletedAt, anchor.Temporal().TxTo, anchor.Temporal().ValidTo = 0, 0, 0
	aw, err := NodeToWireChecked(anchor)
	if err != nil {
		t.Fatalf("anchor wire: %v", err)
	}
	tw, err := NodeToWireChecked(retractedNodeTombstone(t))
	if err != nil {
		t.Fatalf("target wire: %v", err)
	}
	raw, err := EncodeNodeHistoryDelta(DiffNodeHistory(aw, tw))
	if err != nil {
		t.Fatalf("EncodeNodeHistoryDelta: %v", err)
	}
	d, err := DecodeNodeHistoryDelta(raw)
	if err != nil {
		t.Fatalf("DecodeNodeHistoryDelta: %v", err)
	}
	if tm := SelectionTemporalMetaOfNodeWire(d.Meta); tm == nil || !tm.Retracted {
		t.Fatalf("node delta Meta dropped the marker: %+v", tm)
	}
	full, err := WireToNodeChecked(ApplyNodeHistory(aw, d))
	if err != nil {
		t.Fatalf("WireToNodeChecked(applied): %v", err)
	}
	if !full.Temporal().Retracted {
		t.Fatal("node delta reconstruction dropped the marker")
	}

	ranchor := retractedRelTombstone(t)
	ranchor.Temporal().Retracted = false
	ranchor.Temporal().DeletedAt, ranchor.Temporal().TxTo, ranchor.Temporal().ValidTo = 0, 0, 0
	raw2, err := func() ([]byte, error) {
		a, err := RelToWireChecked(ranchor)
		if err != nil {
			return nil, err
		}
		tg, err := RelToWireChecked(retractedRelTombstone(t))
		if err != nil {
			return nil, err
		}
		return EncodeRelHistoryDelta(DiffRelHistory(a, tg))
	}()
	if err != nil {
		t.Fatalf("rel delta: %v", err)
	}
	rd, err := DecodeRelHistoryDelta(raw2)
	if err != nil {
		t.Fatalf("DecodeRelHistoryDelta: %v", err)
	}
	if tm := SelectionTemporalMetaOfRelWire(rd.Meta); tm == nil || !tm.Retracted {
		t.Fatalf("relationship delta Meta dropped the marker: %+v", tm)
	}
}

// TestRetractedMarker_CheckedDecodeRejectsOrphanMarker catches a checked
// decoder that admits a marker without a delete (rx on a row with no
// DeletedAt, or with no temporal block): such a row is no write door's output,
// and a reader would otherwise have to guess what it means.
func TestRetractedMarker_CheckedDecodeRejectsOrphanMarker(t *testing.T) {
	t.Parallel()
	w, err := NodeToWireChecked(retractedNodeTombstone(t))
	if err != nil {
		t.Fatalf("wire: %v", err)
	}
	noDelete := w
	noDelete.DeletedAt = 0
	if _, err := WireToNodeChecked(noDelete); err == nil {
		t.Fatal("node row with rx and no da was admitted")
	}
	if err := ValidateNodeWire(noDelete); err == nil {
		t.Fatal("ValidateNodeWire admitted rx without da")
	}
	noTemporal := NodeWire{FormatVersion: CurrentWireFormatVersion, ID: 4242, PrimaryLabel: 1, Retracted: true}
	if _, err := WireToNodeChecked(noTemporal); err == nil {
		t.Fatal("node row with rx and no temporal block was admitted")
	}
	if _, err := WireToNodeChecked(w); err != nil {
		t.Fatalf("counterpart: a retracted tombstone must decode: %v", err)
	}

	rw, err := RelToWireChecked(retractedRelTombstone(t))
	if err != nil {
		t.Fatalf("rel wire: %v", err)
	}
	rNoDelete := rw
	rNoDelete.DeletedAt = 0
	if _, err := WireToRelChecked(rNoDelete); err == nil {
		t.Fatal("relationship row with rx and no da was admitted")
	}
	rNoTemporal := RelWire{FormatVersion: CurrentWireFormatVersion, ID: 4243, RelType: 1, StartID: 10, EndID: 12, Retracted: true}
	if _, err := WireToRelChecked(rNoTemporal); err == nil {
		t.Fatal("relationship row with rx and no temporal block was admitted")
	}
	if _, err := WireToRelChecked(rw); err != nil {
		t.Fatalf("counterpart: a retracted relationship tombstone must decode: %v", err)
	}
}
