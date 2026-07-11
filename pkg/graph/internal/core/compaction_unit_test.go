package core

import (
	"errors"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// mkInfos builds an ascending-by-version chain with txFrom = 100*(version+1)
// and a per-version hash string so planTrim's boundary picks are checkable.
func mkInfos(n int) []versionInfo {
	out := make([]versionInfo, n)
	for i := 0; i < n; i++ {
		out[i] = versionInfo{
			version: uint32(i),
			txFrom:  types.Instant(100 * (i + 1)),
			hash:    string(rune('a' + i)),
		}
	}
	return out
}

func TestPlanTrim_Table(t *testing.T) {
	t.Parallel()
	// 5 history versions v0..v4: txFrom 100,200,300,400,500; hashes a,b,c,d,e.
	tests := []struct {
		name       string
		versions   int
		policy     RetentionPolicy
		wantTrim   int
		wantBound  uint32 // TrimmedThroughVersion (highest trimmed)
		wantTxTo   types.Instant
		wantOldest string // oldest kept hash
	}{
		{"keep-all-by-count", 5, RetentionPolicy{KeepVersions: 5}, 0, 0, 0, ""},
		{"keep-all-oversized", 5, RetentionPolicy{KeepVersions: 99}, 0, 0, 0, ""},
		{"keep2-by-count", 5, RetentionPolicy{KeepVersions: 2}, 3, 2, 400, "d"},
		{"keep1-by-count-mandatory-newest", 5, RetentionPolicy{KeepVersions: 1}, 4, 3, 500, "e"},
		{"age-keep-from-300", 5, RetentionPolicy{KeepSince: 300}, 2, 1, 300, "c"},
		{"age-keeps-nothing-old-but-newest-kept", 5, RetentionPolicy{KeepSince: 9999}, 4, 3, 500, "e"},
		{"union-count2-age400", 5, RetentionPolicy{KeepVersions: 2, KeepSince: 400}, 3, 2, 400, "d"},
		{"union-count1-age200-age-wins", 5, RetentionPolicy{KeepVersions: 1, KeepSince: 200}, 1, 0, 200, "b"},
		{"single-history-never-trims", 1, RetentionPolicy{KeepVersions: 0, KeepSince: 9999}, 0, 0, 0, ""},
		{"two-history-trim-oldest", 2, RetentionPolicy{KeepVersions: 1}, 1, 0, 200, "b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			infos := mkInfos(tc.versions)
			trim, boundary, oldest := planTrim(infos, tc.policy)
			if trim != tc.wantTrim {
				t.Fatalf("trim=%d, want %d", trim, tc.wantTrim)
			}
			if trim == 0 {
				return
			}
			if boundary.version != tc.wantBound {
				t.Fatalf("boundary.version=%d, want %d", boundary.version, tc.wantBound)
			}
			if oldest.txFrom != tc.wantTxTo {
				t.Fatalf("oldestKept.txFrom=%d, want %d", oldest.txFrom, tc.wantTxTo)
			}
			if oldest.hash != tc.wantOldest {
				t.Fatalf("oldestKept.hash=%q, want %q", oldest.hash, tc.wantOldest)
			}
		})
	}
}

func TestValidateRetentionPolicy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		p  RetentionPolicy
		ok bool
	}{
		{RetentionPolicy{KeepVersions: 5}, true},
		{RetentionPolicy{KeepSince: 100}, true},
		{RetentionPolicy{KeepVersions: 2, KeepSince: 100}, true},
		{RetentionPolicy{}, false},
		{RetentionPolicy{KeepVersions: -1}, false},
		{RetentionPolicy{KeepSince: -1}, false},
	}
	for _, c := range cases {
		err := validateRetentionPolicy(c.p)
		if c.ok && err != nil {
			t.Fatalf("policy %+v: unexpected err %v", c.p, err)
		}
		if !c.ok && !errors.Is(err, ErrInvalidRetentionPolicy) {
			t.Fatalf("policy %+v: err=%v, want ErrInvalidRetentionPolicy", c.p, err)
		}
	}
}

func TestCompactionStub_SelfHashRoundTrip(t *testing.T) {
	t.Parallel()
	stub := compactionStub{
		EntityID:              42,
		TrimmedThroughVersion: 3,
		LastTrimmedHash:       "deadbeef",
		LastTrimmedTxTo:       500,
		CompactedAtTx:         900,
	}.sealed()

	blob, err := stub.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeCompactionStub(42, blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != stub {
		t.Fatalf("round-trip mismatch: %+v vs %+v", got, stub)
	}
}

func TestCompactionStub_TamperFailsClosed(t *testing.T) {
	t.Parallel()
	stub := compactionStub{
		EntityID:              42,
		TrimmedThroughVersion: 3,
		LastTrimmedHash:       "deadbeef",
		LastTrimmedTxTo:       500,
		CompactedAtTx:         900,
	}.sealed()

	// Tamper with a field WITHOUT recomputing StubHash: self-hash mismatch.
	forged := stub
	forged.LastTrimmedHash = "0000face"
	blob, err := forged.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := decodeCompactionStub(42, blob); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("forged stub decode err=%v, want ErrCorruptWire", err)
	}

	// Entity-id mismatch (stub moved to another entity) fails closed.
	good, _ := stub.encode()
	if _, err := decodeCompactionStub(43, good); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("id-mismatch decode err=%v, want ErrCorruptWire", err)
	}

	// A raw bit-flip of a valid blob: SafeUnmarshal / self-hash fails closed.
	flip := append([]byte(nil), good...)
	flip[len(flip)-1] ^= 0xFF
	if _, err := decodeCompactionStub(42, flip); err == nil {
		t.Fatalf("bit-flipped blob decoded without error")
	}

	// Garbage that is not msgpack at all.
	if _, err := decodeCompactionStub(42, []byte{0xff, 0xff, 0xff}); err == nil {
		t.Fatalf("garbage blob decoded without error")
	}

	// A msgpack blob with the right shape but a zero StubHash fails.
	var bare compactionStub
	bare.EntityID = 42
	bareBytes, _ := msgpack.Marshal(bare)
	if _, err := decodeCompactionStub(42, bareBytes); !errors.Is(err, storepkg.ErrCorruptWire) {
		t.Fatalf("unsealed stub decode err=%v, want ErrCorruptWire", err)
	}
}
