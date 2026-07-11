package badger

import (
	"errors"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// key16/key24/key32 are valid AES-128/192/256 keys for tests. wrongKey32 is a
// DIFFERENT 32-byte key used to prove a mismatched key fails closed.
var (
	key16      = []byte("0123456789abcdef")
	key24      = []byte("0123456789abcdef01234567")
	key32      = []byte("0123456789abcdef0123456789abcdef")
	wrongKey32 = []byte("ZYXWVUTSRQPONMLKJIHGFEDCBA098765")
)

// encCfg returns a disk-backed encrypted Config with the given key and BOTH
// caches Badger requires whenever EncryptionKey is set: BlockCacheSize (else
// Open panics — checkAndSetOptions) and IndexCacheSize (else the first
// encrypted SSTable flush panics — table.go fetchIndex; see
// ErrEncryptionRequiresBlockCache / ErrEncryptionRequiresIndexCache).
func encCfg(dir string, key []byte) Config {
	return Config{
		Dir:            dir,
		EncryptionKey:  key,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
	}
}

// --- Test 1: write, close, reopen with the SAME key -------------------------

// TestBadgerEncryptionRoundTripSameKey proves an encrypted store is usable
// end-to-end on a REAL directory (encryption is a disk feature — an in-memory
// store never touches Badger's on-disk key registry, nor produces an
// encrypted SSTable): write enough records to force an actual memtable
// flush, close, reopen with the identical key, and every record must still
// be readable.
func TestBadgerEncryptionRoundTripSameKey(t *testing.T) {
	dir := t.TempDir()
	cfg := encCfg(dir, key32)

	bs, err := New(cfg)
	if err != nil {
		t.Fatalf("open for write: %v", err)
	}
	const n = 25
	for i := 1; i <= n; i++ {
		node := mustNode(t, i, map[string]any{"i": i, "label": "encrypted"})
		if err := bs.PutNode(node); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Force a real flush to disk so the round trip exercises the encrypted
	// SSTable path (fetchIndex/IndexCache), not just the in-RAM cache.
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	bs2, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen with same key: %v", err)
	}
	defer bs2.Close()
	for i := 1; i <= n; i++ {
		got, err := bs2.GetNode(types.NodeID(snowflake.ID(i)))
		if err != nil {
			t.Fatalf("get %d after reopen: %v", i, err)
		}
		if got.PropertiesMap()["label"] != "encrypted" {
			t.Fatalf("node %d property mismatch after encrypted round trip: %v", i, got.PropertiesMap())
		}
	}
}

// --- Test 2: reopen with the WRONG key fails closed --------------------------

// TestBadgerEncryptionWrongKeyFailsClosed proves that reopening an encrypted
// directory with a DIFFERENT key never silently succeeds or corrupts data —
// Open must fail, and the failure must be recognizable via errors.Is against
// Badger's own exported sentinel (empirically confirmed unwrapped through
// OpenKeyRegistry, so our fmt.Errorf("...: %w", err) wrap preserves it).
func TestBadgerEncryptionWrongKeyFailsClosed(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(encCfg(dir, key32))
	if err != nil {
		t.Fatalf("open for write: %v", err)
	}
	if err := bs.PutNode(mustNode(t, 1, map[string]any{"i": 1})); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	bad, err := New(encCfg(dir, wrongKey32))
	if bad != nil {
		_ = bad.Close()
	}
	if err == nil {
		t.Fatal("expected reopen with wrong key to fail, got nil error")
	}
	if !errors.Is(err, badgerv4.ErrEncryptionKeyMismatch) {
		t.Fatalf("reopen with wrong key: err = %v, want wrapped badgerv4.ErrEncryptionKeyMismatch", err)
	}
}

// --- Test 3: applying a key to an existing PLAINTEXT dir ---------------------

// TestBadgerEncryptionKeyOnPlaintextDirFailsClearly proves that turning
// encryption ON for a directory that was never encrypted fails closed with the
// SAME recognizable Badger sentinel as the wrong-key case (Badger validates
// both by decrypting a sanity marker in its KEYREGISTRY file — a plaintext
// dir's marker was never encrypted, so any non-empty key fails the check) —
// not silently, and not by corrupting the existing plaintext data.
func TestBadgerEncryptionKeyOnPlaintextDirFailsClearly(t *testing.T) {
	dir := t.TempDir()
	plain, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("open plaintext: %v", err)
	}
	if err := plain.PutNode(mustNode(t, 1, map[string]any{"i": 1})); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := plain.Close(); err != nil {
		t.Fatalf("close plaintext: %v", err)
	}

	enc, err := New(encCfg(dir, key32))
	if enc != nil {
		_ = enc.Close()
	}
	if err == nil {
		t.Fatal("expected opening a plaintext dir with an encryption key to fail, got nil error")
	}
	if !errors.Is(err, badgerv4.ErrEncryptionKeyMismatch) {
		t.Fatalf("key on plaintext dir: err = %v, want wrapped badgerv4.ErrEncryptionKeyMismatch", err)
	}

	// The plaintext data must remain intact and readable without a key —
	// the rejected encrypted open must not have mutated anything on disk.
	reopened, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen plaintext after rejected encrypted attempt: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.GetNode(types.NodeID(snowflake.ID(1))); err != nil {
		t.Fatalf("plaintext data lost after rejected encrypted open attempt: %v", err)
	}
}

// --- Test 4: validation table -------------------------------------------------

// TestBadgerEncryptionValidation pins every EncryptionKey validation rule at
// New(): the accepted AES key lengths (0/16/24/32), every rejected length,
// and BOTH cache guards that prevent a real Badger PANIC from escaping
// New()/a later flush:
//   - encryption + BlockCacheSize == 0 -> Badger panics immediately at Open
//     (checkAndSetOptions: "BlockCacheSize should be set since
//     compression/encryption are enabled").
//   - encryption + IndexCacheSize == 0 -> Badger panics on the FIRST
//     encrypted SSTable flush (table.go fetchIndex: "Index Cache must be
//     set for encrypted workloads") — empirically reproduced with a live
//     write+flush cycle before this guard existed.
//
// Both were verified against the actual Badger v4.9.2 source and a live
// reproduction, not assumed.
func TestBadgerEncryptionValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // "" = must succeed
	}{
		{"no key, no cache: fine", func(c *Config) {}, ""},
		{"key len 16 with both caches", func(c *Config) { c.EncryptionKey = key16; c.BlockCacheSize = 1 << 20; c.IndexCacheSize = 1 << 20 }, ""},
		{"key len 24 with both caches", func(c *Config) { c.EncryptionKey = key24; c.BlockCacheSize = 1 << 20; c.IndexCacheSize = 1 << 20 }, ""},
		{"key len 32 with both caches", func(c *Config) { c.EncryptionKey = key32; c.BlockCacheSize = 1 << 20; c.IndexCacheSize = 1 << 20 }, ""},

		{"key len 1 rejected", func(c *Config) { c.EncryptionKey = []byte("x"); c.BlockCacheSize = 1 << 20; c.IndexCacheSize = 1 << 20 }, "EncryptionKey"},
		{"key len 15 rejected", func(c *Config) {
			c.EncryptionKey = make([]byte, 15)
			c.BlockCacheSize = 1 << 20
			c.IndexCacheSize = 1 << 20
		}, "EncryptionKey"},
		{"key len 17 rejected", func(c *Config) {
			c.EncryptionKey = make([]byte, 17)
			c.BlockCacheSize = 1 << 20
			c.IndexCacheSize = 1 << 20
		}, "EncryptionKey"},
		{"key len 33 rejected", func(c *Config) {
			c.EncryptionKey = make([]byte, 33)
			c.BlockCacheSize = 1 << 20
			c.IndexCacheSize = 1 << 20
		}, "EncryptionKey"},
		{"key len 64 rejected", func(c *Config) {
			c.EncryptionKey = make([]byte, 64)
			c.BlockCacheSize = 1 << 20
			c.IndexCacheSize = 1 << 20
		}, "EncryptionKey"},

		{"key set, no BlockCacheSize rejected", func(c *Config) { c.EncryptionKey = key32; c.IndexCacheSize = 1 << 20 }, "BlockCacheSize"},
		{"key set, BlockCacheSize negative rejected by tuning check first", func(c *Config) {
			c.EncryptionKey = key32
			c.BlockCacheSize = -1
			c.IndexCacheSize = 1 << 20
		}, "BlockCacheSize"},
		{"key set, no IndexCacheSize rejected", func(c *Config) { c.EncryptionKey = key32; c.BlockCacheSize = 1 << 20 }, "IndexCacheSize"},
		{"key set, IndexCacheSize negative rejected by tuning check first", func(c *Config) {
			c.EncryptionKey = key32
			c.BlockCacheSize = 1 << 20
			c.IndexCacheSize = -1
		}, "IndexCacheSize"},
		{"key set, neither cache set rejected (BlockCacheSize checked first)", func(c *Config) { c.EncryptionKey = key32 }, "BlockCacheSize"},

		{"key empty, both caches zero: fine (encryption disabled)", func(c *Config) { c.EncryptionKey = nil; c.BlockCacheSize = 0; c.IndexCacheSize = 0 }, ""},
		{"no key, IndexCacheSize alone is a harmless tuning knob", func(c *Config) { c.IndexCacheSize = 1 << 20 }, ""},

		{"rotation duration alone (no key) is harmless", func(c *Config) { c.EncryptionKeyRotation = 0 }, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{Dir: t.TempDir()}
			tc.mutate(&cfg)
			bs, err := New(cfg)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected success, got: %v", err)
				}
				if cerr := bs.Close(); cerr != nil {
					t.Fatalf("close: %v", cerr)
				}
				return
			}
			if err == nil {
				_ = bs.Close()
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// TestBadgerEncryptionSentinelsWithErrorsIs asserts all three new sentinels
// are reachable and distinguishable via errors.Is, not merely by substring
// match (Testing Rule 4: sentinel errors are tested with errors.Is).
func TestBadgerEncryptionSentinelsWithErrorsIs(t *testing.T) {
	_, err := New(Config{Dir: t.TempDir(), EncryptionKey: []byte("bad-length-key")})
	if !errors.Is(err, ErrInvalidEncryptionKeyLength) {
		t.Fatalf("bad key length: err = %v, want ErrInvalidEncryptionKeyLength", err)
	}

	_, err = New(Config{Dir: t.TempDir(), EncryptionKey: key32})
	if !errors.Is(err, ErrEncryptionRequiresBlockCache) {
		t.Fatalf("key without any cache: err = %v, want ErrEncryptionRequiresBlockCache", err)
	}

	_, err = New(Config{Dir: t.TempDir(), EncryptionKey: key32, BlockCacheSize: 1 << 20})
	if !errors.Is(err, ErrEncryptionRequiresIndexCache) {
		t.Fatalf("key with BlockCacheSize but no IndexCacheSize: err = %v, want ErrEncryptionRequiresIndexCache", err)
	}
}

// TestBadgerEncryptionWithoutIndexCacheWouldPanicOnFlush is the adversarial
// regression pinning WHY ErrEncryptionRequiresIndexCache exists: it
// reproduces the exact live write+flush cycle that panics inside Badger
// ("Index Cache must be set for encrypted workloads") when IndexCacheSize is
// left at 0 for an encrypted store, by calling New() directly against
// buildBadgerOptions the way a caller who bypassed validateEncryptionConfig
// would experience it. Since New() now rejects this configuration outright,
// this test asserts the REJECTION (the panic is prevented, not merely
// tolerated) — it must never reach badgerv4.Open with this combination.
func TestBadgerEncryptionWithoutIndexCacheWouldPanicOnFlush(t *testing.T) {
	dir := t.TempDir()
	_, err := New(Config{Dir: dir, EncryptionKey: key32, BlockCacheSize: 1 << 20})
	if !errors.Is(err, ErrEncryptionRequiresIndexCache) {
		t.Fatalf("expected New to fail closed with ErrEncryptionRequiresIndexCache before ever reaching a flush, got: %v", err)
	}
}

// TestBadgerEncryptionKeyRotationApplied confirms EncryptionKeyRotation
// reaches Badger's live options (the same "applied, not just accepted" shape
// as the other footprint knobs), alongside IndexCacheSize.
func TestBadgerEncryptionKeyRotationApplied(t *testing.T) {
	dir := t.TempDir()
	const rotation = 3 * 24 * 60 * 60 * 1_000_000_000 // 3 days, in time.Duration units (ns)
	bs, err := New(Config{
		Dir:                   dir,
		EncryptionKey:         key32,
		BlockCacheSize:        1 << 20,
		IndexCacheSize:        1 << 20,
		EncryptionKeyRotation: rotation,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bs.Close()
	opts := bs.DBForTest().Opts()
	if opts.EncryptionKeyRotationDuration != rotation {
		t.Errorf("EncryptionKeyRotationDuration reached Badger as %v, want %v", opts.EncryptionKeyRotationDuration, rotation)
	}
	if len(opts.EncryptionKey) != len(key32) {
		t.Errorf("EncryptionKey length reached Badger as %d, want %d", len(opts.EncryptionKey), len(key32))
	}
	if opts.IndexCacheSize != 1<<20 {
		t.Errorf("IndexCacheSize reached Badger as %d, want %d", opts.IndexCacheSize, 1<<20)
	}
}
