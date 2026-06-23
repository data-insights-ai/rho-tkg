package storeutil

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

func TestChangeLogKeyRoundTrip(t *testing.T) {
	for _, lsn := range []uint64{0, 1, 2, 255, 256, 1 << 32, math.MaxUint64} {
		key := ChangeLogKey(lsn)
		if len(key) != SizeChangeLog {
			t.Fatalf("lsn %d: key len %d, want %d", lsn, len(key), SizeChangeLog)
		}
		if key[0] != KeyChangeLog {
			t.Fatalf("lsn %d: key prefix 0x%02x, want 0x%02x", lsn, key[0], KeyChangeLog)
		}
		got, ok := ChangeLogLSNFromKey(key)
		if !ok || got != lsn {
			t.Fatalf("lsn %d: round-trip got (%d, %v)", lsn, got, ok)
		}
	}
}

// Big-endian LSN encoding must make a lexicographic key scan yield ascending
// LSN (commit) order — the property a ForEachChange prefix iterator relies on.
func TestChangeLogKeyOrdering(t *testing.T) {
	lsns := []uint64{1, 2, 255, 256, 65535, 65536, 1 << 40}
	for i := 1; i < len(lsns); i++ {
		lo := ChangeLogKey(lsns[i-1])
		hi := ChangeLogKey(lsns[i])
		if bytes.Compare(lo, hi) >= 0 {
			t.Fatalf("key(%d) >= key(%d): bytes not ascending", lsns[i-1], lsns[i])
		}
	}
}

func TestChangeLogLSNFromKey_Rejects(t *testing.T) {
	cases := map[string][]byte{
		"wrong prefix": append([]byte{KeyMeta}, make([]byte, 8)...),
		"too short":    {KeyChangeLog, 0x00},
		"too long":     append([]byte{KeyChangeLog}, make([]byte, 16)...),
		"empty":        {},
	}
	for name, key := range cases {
		t.Run(name, func(t *testing.T) {
			if lsn, ok := ChangeLogLSNFromKey(key); ok || lsn != 0 {
				t.Fatalf("expected (0,false), got (%d,%v)", lsn, ok)
			}
		})
	}
}

func TestEncodeSplitChangeValue_RoundTrip(t *testing.T) {
	tags := []storepkg.ChangeTag{
		storepkg.ChangeNodePut, storepkg.ChangeRelPut,
		storepkg.ChangeNodeDelete, storepkg.ChangeRelDelete,
		storepkg.ChangeNodeHistoryVersion, storepkg.ChangeRelHistoryVersion,
		storepkg.ChangeNodeHistoryTruncate, storepkg.ChangeRelHistoryTruncate,
		storepkg.ChangeMeta, storepkg.ChangeClear,
	}
	for _, tag := range tags {
		t.Run(tag.String(), func(t *testing.T) {
			payload := []byte{0xde, 0xad, 0xbe, 0xef}
			if tag == storepkg.ChangeClear {
				payload = nil // Clear carries no body
			}
			val := EncodeChangeValue(tag, payload)
			gotTag, gotPayload, err := SplitChangeValue(val)
			if err != nil {
				t.Fatalf("split: %v", err)
			}
			if gotTag != tag {
				t.Fatalf("tag: got %v want %v", gotTag, tag)
			}
			if !bytes.Equal(gotPayload, payload) {
				t.Fatalf("payload: got %x want %x", gotPayload, payload)
			}
		})
	}
}

func TestSplitChangeValue_FailsClosed(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		_, _, err := SplitChangeValue(nil)
		if !errors.Is(err, storepkg.ErrCorruptWire) {
			t.Fatalf("want ErrCorruptWire, got %v", err)
		}
	})
	t.Run("unknown tag", func(t *testing.T) {
		_, _, err := SplitChangeValue([]byte{0xFF, 0x00})
		if !errors.Is(err, storepkg.ErrCorruptWire) {
			t.Fatalf("want ErrCorruptWire, got %v", err)
		}
	})
	t.Run("tag zero", func(t *testing.T) {
		_, _, err := SplitChangeValue([]byte{0x00})
		if !errors.Is(err, storepkg.ErrCorruptWire) {
			t.Fatalf("want ErrCorruptWire, got %v", err)
		}
	})
}

func sampleNodeWire() NodeWire {
	return NodeWire{FormatVersion: CurrentWireFormatVersion, ID: 1001, PrimaryLabel: 3, Version: 7}
}

func sampleRelWire() RelWire {
	return RelWire{FormatVersion: CurrentWireFormatVersion, ID: 2002, RelType: 4, StartID: 1001, EndID: 1002, Version: 2}
}

func TestChangeBody_RoundTrip(t *testing.T) {
	t.Run("NodePut", func(t *testing.T) {
		w := sampleNodeWire()
		p, err := MarshalChangeBody(w)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeNodePut(p)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, w) {
			t.Fatalf("got %+v want %+v", got, w)
		}
	})

	t.Run("RelPut", func(t *testing.T) {
		w := sampleRelWire()
		p, _ := MarshalChangeBody(w)
		got, err := DecodeRelPut(p)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, w) {
			t.Fatalf("got %+v want %+v", got, w)
		}
	})

	t.Run("NodeDelete hard cascade", func(t *testing.T) {
		b := NodeDeleteBody{ID: 1001, WithHistory: false, CascadedRelIDs: []int64{2002, 2003}}
		p, _ := MarshalChangeBody(b)
		got, err := DecodeNodeDelete(p)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, b) {
			t.Fatalf("got %+v want %+v", got, b)
		}
	})

	t.Run("NodeDelete with history", func(t *testing.T) {
		tomb := sampleNodeWire()
		relTomb := sampleRelWire()
		b := NodeDeleteBody{ID: 1001, WithHistory: true, Tombstone: &tomb, RelTombstones: []RelWire{relTomb}}
		p, _ := MarshalChangeBody(b)
		got, err := DecodeNodeDelete(p)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, b) {
			t.Fatalf("got %+v want %+v", got, b)
		}
	})

	t.Run("RelDelete with history", func(t *testing.T) {
		tomb := sampleRelWire()
		b := RelDeleteBody{ID: 2002, WithHistory: true, Tombstone: &tomb}
		p, _ := MarshalChangeBody(b)
		got, err := DecodeRelDelete(p)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, b) {
			t.Fatalf("got %+v want %+v", got, b)
		}
	})

	t.Run("NodeHistoryVersion", func(t *testing.T) {
		b := HistoryVersionNodeBody{Version: 5, Wire: sampleNodeWire()}
		p, _ := MarshalChangeBody(b)
		got, err := DecodeHistoryVersionNode(p)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, b) {
			t.Fatalf("got %+v want %+v", got, b)
		}
	})

	t.Run("RelHistoryVersion", func(t *testing.T) {
		b := HistoryVersionRelBody{Version: 9, Wire: sampleRelWire()}
		p, _ := MarshalChangeBody(b)
		got, err := DecodeHistoryVersionRel(p)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, b) {
			t.Fatalf("got %+v want %+v", got, b)
		}
	})

	t.Run("HistoryTruncate keep-count and trim", func(t *testing.T) {
		for _, b := range []HistoryTruncateBody{
			{ID: 1001, IsTrim: false, Bound: 3},
			{ID: 2002, IsTrim: true, Bound: 4},
		} {
			p, _ := MarshalChangeBody(b)
			got, err := DecodeHistoryTruncate(p)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, b) {
				t.Fatalf("got %+v want %+v", got, b)
			}
		}
	})

	t.Run("Meta", func(t *testing.T) {
		b := MetaBody{Key: "schema_version", Value: []byte{0x00, 0x05}}
		p, _ := MarshalChangeBody(b)
		got, err := DecodeMeta(p)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, b) {
			t.Fatalf("got %+v want %+v", got, b)
		}
	})
}

// Every decoder must fail with an error (never panic) on truncated/garbage
// bytes — the trust-boundary contract for persisted change-log values.
func TestChangeBodyDecoders_FailClosedOnGarbage(t *testing.T) {
	garbage := []byte{0xc1, 0xff, 0x00, 0xde, 0xad} // 0xc1 is an invalid msgpack type byte
	decoders := map[string]func([]byte) error{
		"NodePut":            func(p []byte) error { _, e := DecodeNodePut(p); return e },
		"RelPut":             func(p []byte) error { _, e := DecodeRelPut(p); return e },
		"NodeDelete":         func(p []byte) error { _, e := DecodeNodeDelete(p); return e },
		"RelDelete":          func(p []byte) error { _, e := DecodeRelDelete(p); return e },
		"NodeHistoryVersion": func(p []byte) error { _, e := DecodeHistoryVersionNode(p); return e },
		"RelHistoryVersion":  func(p []byte) error { _, e := DecodeHistoryVersionRel(p); return e },
		"HistoryTruncate":    func(p []byte) error { _, e := DecodeHistoryTruncate(p); return e },
		"Meta":               func(p []byte) error { _, e := DecodeMeta(p); return e },
	}
	for name, dec := range decoders {
		t.Run(name, func(t *testing.T) {
			if err := dec(garbage); err == nil {
				t.Fatalf("expected error on garbage payload, got nil")
			}
		})
	}
}
