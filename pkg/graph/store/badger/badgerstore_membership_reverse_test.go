package badger

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"os/exec"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func reverseMigrationSeed(t *testing.T, dir string) Config {
	t.Helper()
	cfg := membershipCrashConfig(dir)
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []types.NodeID{1, 2} {
		n := types.NewNode(id, 1, nil)
		n.SetVersion(1)
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
		if err := n.SetProperty("payload", "exact metadata selector"); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(11, 1, 1, 2)
	r.SetVersion(1)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
	if err := r.SetProperty("payload", "relationship selector"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	for {
		p, err := bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 10, MaxBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if p.Complete {
			break
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func finishReverseMigration(t *testing.T, cfg Config) HistoricalReverseMigrationProgress {
	t.Helper()
	m, err := OpenHistoricalReverseMigration(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for {
		p, err := m.Advance(t.Context(), HistoricalMembershipLimits{MaxRows: 1, MaxBytes: 1000})
		if err != nil {
			t.Fatal(err)
		}
		if p.Complete {
			return p
		}
	}
}

func TestCompactReverseCodecStructuralOwnerAndCorruption(t *testing.T) {
	for _, kind := range []byte{1, 2, 3, 4} {
		for _, id := range []types.EntityID{1, math.MaxInt64} {
			for _, tx := range []types.Instant{-1, 0, math.MaxInt64} {
				forward := diskMembershipOp(diskMembershipScope(kind, math.MaxUint16, "payload", "s:unchanged"), id, tx).key
				op := compactReverseMembershipOp(forward)
				got, err := compactReverseMembershipForward(op.key, op.value, kind, id)
				if err != nil || !bytes.Equal(got, forward) {
					t.Fatal(got, err)
				}
				old := diskMembershipPair(diskMembershipScope(kind, math.MaxUint16, "payload", "s:unchanged"), id, tx)[1]
				got, err = legacyReverseMembershipForward(old.key, old.value, kind, id)
				if err != nil || !bytes.Equal(got, forward) {
					t.Fatal(got, err)
				}
				for _, wrong := range []struct {
					k  byte
					id types.EntityID
				}{{kind, 2}, {kind%4 + 1, id}, {kind, 0}, {kind, -1}} {
					if _, err := compactReverseMembershipForward(op.key, op.value, wrong.k, wrong.id); !errors.Is(err, ErrInvalidStoreMutation) {
						t.Fatal("foreign owner/kind accepted", err)
					}
				}
				for i := range op.key {
					k := bytes.Clone(op.key)
					k[i] ^= 1
					if _, err := compactReverseMembershipForward(k, op.value, kind, id); !errors.Is(err, ErrInvalidStoreMutation) {
						t.Fatal("corruption accepted", i, err)
					}
				}
				// Forged valid-CRC selectors remain structurally confined.
				for _, mutate := range []func([]byte){func(k []byte) { k[1] = kind%4 + 1 }, func(k []byte) { binary.BigEndian.PutUint64(k[2:10], 2) }, func(k []byte) { clear(k[10:12]) }, func(k []byte) { k[len(k)-8] = 0xff }} {
					k := bytes.Clone(op.key)
					mutate(k)
					v := make([]byte, 4)
					binary.BigEndian.PutUint32(v, crc32.Checksum(k, diskMembershipReverseCRC))
					if _, err := compactReverseMembershipForward(k, v, kind, id); !errors.Is(err, ErrInvalidStoreMutation) {
						t.Fatal("forged selector accepted", err)
					}
				}
			}
		}
	}
	for _, k := range [][]byte{nil, {1}, make([]byte, 20), make([]byte, 52)} {
		if _, err := compactReverseMembershipForward(k, nil, 1, 1); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatal(err)
		}
	}
	old := diskMembershipPair(diskMembershipScope(1, 1, "", ""), 2, 0)[1]
	for _, tc := range []struct {
		key, value []byte
		kind       byte
		owner      types.EntityID
	}{
		{nil, nil, 1, 1}, {old.key, old.value, 2, 2}, {old.key, old.value, 1, 1},
		{old.key, []byte("corrupt"), 1, 2},
	} {
		if _, err := legacyReverseMembershipForward(tc.key, tc.value, tc.kind, tc.owner); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatal("legacy malformed selector accepted", err)
		}
	}
	forged := bytes.Clone(old.key)
	binary.BigEndian.PutUint64(forged[2:10], 1)
	if _, err := legacyReverseMembershipForward(forged, old.value, 1, 1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatal("legacy valid-SHA foreign pointer accepted", err)
	}
}

func TestCompactReverseExplicitMigrationResumeAndDefaultFence(t *testing.T) {
	cfg := reverseMigrationSeed(t, t.TempDir())
	compact := cfg
	compact.CompactHistoricalReverseOnDisk = true
	if bs, err := New(compact); !errors.Is(err, ErrHistoricalReverseMigrationRequired) {
		if bs != nil {
			bs.Close()
		}
		t.Fatal("implicit5 upgrade", err)
	}
	// Refusal did not upgrade the existing default writer.
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	v, err := readHistoricalWriterVersion(bs.db)
	if err != nil || v != 5 {
		t.Fatal(v, err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := OpenHistoricalReverseMigration(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := OpenHistoricalReverseMigration(t.Context(), cfg); err == nil {
		second.Close()
		t.Fatal("second directory owner accepted")
	}
	p, err := m.Status()
	if err != nil || p.Phase != "copy" || p.Copied != 0 {
		t.Fatal(p, err)
	}
	p, err = m.Advance(t.Context(), HistoricalMembershipLimits{MaxRows: 1, MaxBytes: 1000})
	if err != nil || p.Complete || p.Copied != 1 {
		t.Fatal(p, err)
	}
	if len(p.After) > 0 {
		p.After[0] = 0
	}
	p, err = m.Status()
	if err != nil || p.After[0] != diskMembershipReversePrefix {
		t.Fatal("progress aliases", p, err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Status(); !errors.Is(err, ErrStoreClosed) {
		t.Fatal(err)
	}
	if _, err := m.Advance(t.Context(), HistoricalMembershipLimits{MaxRows: 1, MaxBytes: 1000}); !errors.Is(err, ErrStoreClosed) {
		t.Fatal(err)
	}
	if bs, err := New(compact); !errors.Is(err, ErrHistoricalReverseMigrationIncomplete) {
		if bs != nil {
			bs.Close()
		}
		t.Fatal("incomplete ordinary open accepted", err)
	}
	for _, ro := range []bool{false, true} {
		old := cfg
		old.ReadOnly = ro
		if bs, err := New(old); !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
			if bs != nil {
				bs.Close()
			}
			t.Fatal("old format5 engine accepted6", err)
		}
	}
	p = finishReverseMigration(t, cfg)
	if p.Copied != 6 || p.Removed != 6 {
		t.Fatal(p)
	}
	bs, err = New(compact)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	for _, kind := range []byte{1, 2, 3, 4} {
		ids := map[types.EntityID]types.Instant{}
		scope := diskMembershipScope(kind, 1, "", "")
		if kind >= 3 {
			value := "exact metadata selector"
			if kind == 4 {
				value = "relationship selector"
			}
			scope = diskMembershipScope(kind, 1, "payload", types.IndexablePropertyValueKey(value))
		}
		if err := bs.forEachDiskMembership(scope, 0, func(id types.EntityID, tx types.Instant) bool { ids[id] = tx; return true }); err != nil {
			t.Fatal(err)
		}
		if kind == 1 || kind == 3 {
			if len(ids) != 2 || ids[1] != 10 || ids[2] != 10 {
				t.Fatal(ids)
			}
		} else if len(ids) != 1 || ids[11] != 10 {
			t.Fatal(ids)
		}
	}
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.DefaultIteratorOptions)
		defer it.Close()
		it.Seek([]byte{diskMembershipReversePrefix})
		if it.ValidForPrefix([]byte{diskMembershipReversePrefix}) {
			t.Fatal("legacy residue")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCompactReverseFreshClearAndErase(t *testing.T) {
	for _, logged := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "logged"}[logged], func(t *testing.T) {
			cfg := membershipCrashConfig(t.TempDir())
			cfg.CompactHistoricalReverseOnDisk = true
			cfg.ChangeLog = logged
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []types.NodeID{1, 2} {
				n := types.NewNode(id, 1, nil)
				if err := n.SetProperty("payload", "value"); err != nil {
					t.Fatal(err)
				}
				if err := bs.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			if !logged {
				// Mixed legacy+compact entries for the same owner deduplicate the
				// exact forward delete rather than expanding erase to any neighbor.
				ops := diskMembershipPair(diskMembershipScope(1, 1, "", ""), 1, 0)
				bs.appendOps(ops[1])
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				got, err := bs.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{1}, Bounds: badgerExactErasureBounds})
				if err != nil || got.NodesRemoved != 1 {
					t.Fatal(got, err)
				}
				if _, err := bs.GetNode(2); err != nil {
					t.Fatal("neighbor removed", err)
				}
				for _, prefix := range []byte{diskMembershipReversePrefix, diskMembershipCompactReversePrefix} {
					if err := bs.db.View(func(txn *badgerv4.Txn) error {
						p := []byte{prefix, 1, 0, 0, 0, 0, 0, 0, 0, 1}
						it := txn.NewIterator(badgerv4.DefaultIteratorOptions)
						defer it.Close()
						it.Seek(p)
						if it.ValidForPrefix(p) {
							t.Fatal("erase residue", prefix)
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
			p, err := readHistoricalReverseProgress(bs.db)
			if err != nil || !p.Complete {
				t.Fatal(p, err)
			}
			v, err := readHistoricalWriterVersion(bs.db)
			if err != nil || v != 6 {
				t.Fatal(v, err)
			}
			if bs.historicalMembershipReady.Load() {
				t.Fatal("Clear retained readiness")
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			bs, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			if bs.historicalMembershipReady.Load() {
				t.Fatal("reopen retained readiness")
			}
			if count, err := bs.NodeCount(); err != nil || count != 0 {
				t.Fatal("Clear retained rows")
			}
		})
	}
}

func TestCompactReverseClearAfterLoggedSessionKeepsLastLSN(t *testing.T) {
	cfg := membershipCrashConfig(t.TempDir())
	cfg.CompactHistoricalReverseOnDisk = true
	cfg.ChangeLog = true
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(types.NewNode(1, 1, nil)); err != nil {
		t.Fatal(err)
	}
	before, err := bs.MetaGet("last_lsn")
	if err != nil || len(before) != 8 {
		t.Fatal(before, err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.ChangeLog = false
	bs, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	after, err := bs.MetaGet("last_lsn")
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("watermark changed", before, after, err)
	}
	p, err := readHistoricalReverseProgress(bs.db)
	if err != nil || !p.Complete {
		t.Fatal(p, err)
	}
	v, err := readHistoricalWriterVersion(bs.db)
	if err != nil || v != 6 {
		t.Fatal(v, err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompactReverseMigrationGuardsAndCorruptProgress(t *testing.T) {
	var nilMigration *HistoricalReverseMigration
	if _, err := nilMigration.Status(); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	if _, err := nilMigration.Advance(t.Context(), HistoricalMembershipLimits{}); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	if err := nilMigration.Close(); !errors.Is(err, ErrNilStore) {
		t.Fatal(err)
	}
	var zero HistoricalReverseMigration
	if _, err := zero.Status(); !errors.Is(err, ErrStoreClosed) {
		t.Fatal(err)
	}
	if err := zero.Close(); !errors.Is(err, ErrStoreClosed) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := OpenHistoricalReverseMigration(ctx, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := OpenHistoricalReverseMigration(t.Context(), Config{}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatal(err)
	}
	cfg := reverseMigrationSeed(t, t.TempDir())
	m, err := OpenHistoricalReverseMigration(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, limits := range []HistoricalMembershipLimits{{}, {MaxRows: 10001, MaxBytes: 1}, {MaxRows: 1, MaxBytes: 65 << 20}, {MaxRows: 1, MaxBytes: 1}} {
		if _, err := m.Advance(t.Context(), limits); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatal(limits, err)
		}
	}
	if _, err := m.Advance(ctx, HistoricalMembershipLimits{MaxRows: 1, MaxBytes: 1000}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	p, err := m.Status()
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{}`, `{"version":2,"codec":"unknown","phase":"copy"}`, `{"version":2,"codec":"direct_crc32c","phase":"wrong"}`, `{"version":2,"codec":"direct_crc32c","phase":"complete","complete":true,"copied":1}`, `{"version":2,"codec":"direct_crc32c","phase":"copy","after":"AA=="}`, `{"version":2,"codec":"direct_crc32c","phase":"copy"} {}`, `{"version":2,"codec":"direct_crc32c","phase":"copy","unknown":1}`} {
		if err := m.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(historicalReverseProgressKeyBytes(), []byte(raw)) }); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Status(); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatal(raw, err)
		}
	}
	if err := publishHistoricalReverseProgress(m.db, p, false); err != nil {
		t.Fatal(err)
	}
}

func TestCompactReverseMigrationConstructorRefusesInvalidStorageAndDoesNotFence(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.MemTableSize = 1 },
		func(c *Config) { c.EncryptionKey = []byte("bad") },
		func(c *Config) { c.ReadOnly = true },
		func(c *Config) { c.SyncWrites = false },
	} {
		cfg := reverseMigrationSeed(t, t.TempDir())
		bad := cfg
		mutate(&bad)
		if m, err := OpenHistoricalReverseMigration(t.Context(), bad); err == nil {
			m.Close()
			t.Fatal("invalid configuration accepted")
		}
		bs, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		v, err := readHistoricalWriterVersion(bs.db)
		if err != nil || v != 5 {
			t.Fatal("refusal changed writer fence", v, err)
		}
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// An incomplete forward bootstrap must remain format5 after refusal.
	cfg := membershipCrashConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if m, err := OpenHistoricalReverseMigration(t.Context(), cfg); !errors.Is(err, ErrIndexNotFound) {
		if m != nil {
			m.Close()
		}
		t.Fatal(err)
	}
	bs, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	v, err := readHistoricalWriterVersion(bs.db)
	if err != nil || v != 5 {
		t.Fatal(v, err)
	}
	if err := bs.MetaSet(historicalMembershipProgressKey, []byte(`{"version":99}`)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if m, err := OpenHistoricalReverseMigration(t.Context(), cfg); !errors.Is(err, ErrInvalidStoreMutation) {
		if m != nil {
			m.Close()
		}
		t.Fatal(err)
	}
	// Unrelated row formats are never silently converted.
	plain := Config{Dir: t.TempDir(), SyncWrites: true}
	bs, err = New(plain)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	plain.HistoricalMembershipOnDisk = true
	if m, err := OpenHistoricalReverseMigration(t.Context(), plain); !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
		if m != nil {
			m.Close()
		}
		t.Fatal(err)
	}
	// Corrupt format6 descriptor rejects both ordinary and migration opens.
	good := reverseMigrationSeed(t, t.TempDir())
	m, err := OpenHistoricalReverseMigration(t.Context(), good)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.db.Update(func(txn *badgerv4.Txn) error {
		return txn.Set(historicalReverseProgressKeyBytes(), []byte(`{"version":99}`))
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if again, err := OpenHistoricalReverseMigration(t.Context(), good); !errors.Is(err, ErrInvalidStoreMutation) {
		if again != nil {
			again.Close()
		}
		t.Fatal(err)
	}
	good.CompactHistoricalReverseOnDisk = true
	if bs, err := New(good); !errors.Is(err, ErrInvalidStoreMutation) {
		if bs != nil {
			bs.Close()
		}
		t.Fatal(err)
	}
}

func TestCompactReverseMigrationCrashChild(t *testing.T) {
	dir := os.Getenv("RHO_COMPACT_REVERSE_CRASH_DIR")
	if dir == "" {
		t.Skip("subprocess only")
	}
	stage := os.Getenv("RHO_COMPACT_REVERSE_CRASH_STAGE")
	if stage == "reset-meta" || stage == "reset-data" {
		cfg := membershipCrashConfig(dir)
		cfg.CompactHistoricalReverseOnDisk = true
		cfg.ChangeLog = os.Getenv("RHO_COMPACT_REVERSE_CRASH_LOGGED") == "1"
		bs, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		bs.historicalReverseResetTestHook = func(at string) {
			if at == stage {
				os.Exit(17)
			}
		}
		if err := bs.Clear(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("reset crash stage not reached")
	}
	m, err := OpenHistoricalReverseMigration(t.Context(), membershipCrashConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	if stage == "fence" {
		os.Exit(17)
	}
	maxRows := 1
	if stage == "copy-large-data" {
		stage = "copy-data"
		maxRows = 10000
		t.Logf("max copy page%d; native max batch count%d", maxRows, m.db.MaxBatchCount())
	}
	m.testHook = func(at string) {
		if at == stage {
			os.Exit(17)
		}
	}
	for {
		p, err := m.Advance(t.Context(), HistoricalMembershipLimits{MaxRows: maxRows, MaxBytes: 64 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if p.Complete {
			t.Fatal("crash stage not reached", stage)
		}
	}
}

func TestCompactReverseMigrationLargeCopyCrashReplaysWholePage(t *testing.T) {
	cfg := membershipCrashConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Auxiliary-only conservative candidates are sufficient to force the
	// maximum public copy page without a large graph import.
	for i := range 10000 {
		ops := diskMembershipPair(diskMembershipScope(diskMemberNodeProperty, 1, "payload", "s:selector"), types.EntityID(i+1), 10)
		bs.appendOps(ops...)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	for {
		p, err := bs.BuildHistoricalMembership(t.Context(), HistoricalMembershipLimits{MaxRows: 1000, MaxBytes: 8 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if p.Complete {
			break
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestCompactReverseMigrationCrashChild$")
	cmd.Env = append(os.Environ(), "RHO_COMPACT_REVERSE_CRASH_DIR="+cfg.Dir, "RHO_COMPACT_REVERSE_CRASH_STAGE=copy-large-data")
	out, err := cmd.CombinedOutput()
	exit, ok := errors.AsType[*exec.ExitError](err)
	if !ok || exit.ExitCode() != 17 {
		t.Fatalf("child %v %s", err, out)
	}
	m, err := OpenHistoricalReverseMigration(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	p, err := m.Status()
	if err != nil || p.Copied != 0 || p.Phase != "copy" {
		t.Fatal("cursor led durable additions", p, err)
	}
	for {
		p, err = m.Advance(t.Context(), HistoricalMembershipLimits{MaxRows: 10000, MaxBytes: 64 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if p.Complete {
			break
		}
	}
	if p.Copied != 10000 || p.Removed != 10000 {
		t.Fatal("replay lost/duplicated progress", p)
	}
}

func TestCompactReverseProgressWritePreservesClosedDBError(t *testing.T) {
	db, err := badgerv4.Open(badgerv4.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := publishHistoricalReverseProgress(db, historicalReverseCompleteProgress(), true); !errors.Is(err, badgerv4.ErrDBClosed) {
		t.Fatal("write error flattened", err)
	}
	if _, err := readHistoricalWriterVersion(db); !errors.Is(err, badgerv4.ErrDBClosed) {
		t.Fatal("version read error flattened", err)
	}
	if _, err := readHistoricalReverseProgress(db); !errors.Is(err, badgerv4.ErrDBClosed) {
		t.Fatal("progress read error flattened", err)
	}
}

func TestCompactReverseBoundsMalformedMetadataAndLegacyValues(t *testing.T) {
	cfg := membershipCrashConfig(t.TempDir())
	cfg.CompactHistoricalReverseOnDisk = true
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(storepkg.WireFormatVersionKey, make([]byte, 8192)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := readHistoricalWriterVersion(bs.db); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatal(err)
	}
	if err := publishHistoricalReverseProgress(bs.db, historicalReverseCompleteProgress(), true); err != nil {
		t.Fatal(err)
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(historicalReverseProgressKeyBytes(), make([]byte, 8192)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := readHistoricalReverseProgress(bs.db); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	old := reverseMigrationSeed(t, t.TempDir())
	m, err := OpenHistoricalReverseMigration(t.Context(), old)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	op := diskMembershipPair(diskMembershipScope(1, 1, "", ""), 1, 10)[1]
	if err := m.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(op.key, make([]byte, 8192)) }); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Advance(t.Context(), HistoricalMembershipLimits{MaxRows: 1, MaxBytes: 1000}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatal(err)
	}
	p, err := m.Status()
	if err != nil || p.Copied != 0 {
		t.Fatal("malformed value advanced cursor", p, err)
	}
}

func TestCompactReverseResetCrashKeepsFenceCodecAndInvalidatesForwardReadiness(t *testing.T) {
	for _, logged := range []bool{false, true} {
		for _, stage := range []string{"reset-meta", "reset-data"} {
			t.Run(stage+map[bool]string{false: "-plain", true: "-logged"}[logged], func(t *testing.T) {
				cfg := reverseMigrationSeed(t, t.TempDir())
				finishReverseMigration(t, cfg)
				exe, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestCompactReverseMigrationCrashChild$")
				logValue := "0"
				if logged {
					logValue = "1"
				}
				cmd.Env = append(os.Environ(), "RHO_COMPACT_REVERSE_CRASH_DIR="+cfg.Dir, "RHO_COMPACT_REVERSE_CRASH_STAGE="+stage, "RHO_COMPACT_REVERSE_CRASH_LOGGED="+logValue)
				out, err := cmd.CombinedOutput()
				exit, ok := errors.AsType[*exec.ExitError](err)
				if !ok || exit.ExitCode() != 17 {
					t.Fatalf("child %v %s", err, out)
				}
				cfg.CompactHistoricalReverseOnDisk = true
				bs, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer bs.Close()
				v, err := readHistoricalWriterVersion(bs.db)
				if err != nil || v != 6 {
					t.Fatal(v, err)
				}
				p, err := readHistoricalReverseProgress(bs.db)
				if err != nil || !p.Complete {
					t.Fatal(p, err)
				}
				if bs.historicalMembershipReady.Load() {
					t.Fatal("crash retained forward readiness")
				}
				if err := bs.ForEachNodeHistoricalCandidate(1, "", "", 0, func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, ErrIndexNotFound) {
					t.Fatal("partial forward index trusted", err)
				}
				if stage == "reset-data" {
					if count, err := bs.NodeCount(); err != nil || count != 0 {
						t.Fatal(count, err)
					}
				}
			})
		}
	}
}

func TestCompactReverseRetentionRemovesBothKindsAndKeepsSurvivor(t *testing.T) {
	cfg := membershipCrashConfig(t.TempDir())
	cfg.CompactHistoricalReverseOnDisk = true
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	for _, id := range []types.NodeID{1, 2} {
		n := types.NewNode(id, 1, nil)
		end := types.Instant(30)
		if id == 2 {
			end = 0
		}
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 10, ValidFrom: 10, ValidTo: end})
		if err := n.SetProperty("payload", "metadata"); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(11, 1, 1, 2)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
	if err := r.SetProperty("payload", "metadata"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	got, err := bs.PurgeNodesByLabelValidToBefore(1, 40, 10)
	if err != nil || got.NodesPurged != 1 {
		t.Fatal(got, err)
	}
	if _, err := bs.GetNode(2); err != nil {
		t.Fatal("survivor removed", err)
	}
	for _, tc := range []struct {
		node bool
		id   types.EntityID
	}{{true, 1}, {false, 11}} {
		ops, err := bs.historicalMembershipEraseOps(tc.node, tc.id)
		if err != nil || len(ops) != 0 {
			t.Fatal("purged reverse residue", ops, err)
		}
	}
}

func TestCompactReverseMigrationProcessCrashResume(t *testing.T) {
	for _, stage := range []string{"fence", "copy-data", "copy-cursor", "cleanup-before-commit", "cleanup-data", "complete-cursor"} {
		t.Run(stage, func(t *testing.T) {
			cfg := reverseMigrationSeed(t, t.TempDir())
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestCompactReverseMigrationCrashChild$")
			cmd.Env = append(os.Environ(), "RHO_COMPACT_REVERSE_CRASH_DIR="+cfg.Dir, "RHO_COMPACT_REVERSE_CRASH_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok || exit.ExitCode() != 17 {
				t.Fatalf("child %v %s", err, out)
			}
			compact := cfg
			compact.CompactHistoricalReverseOnDisk = true
			if stage != "complete-cursor" {
				if bs, err := New(compact); !errors.Is(err, ErrHistoricalReverseMigrationIncomplete) {
					if bs != nil {
						bs.Close()
					}
					t.Fatal(err)
				}
			}
			p := finishReverseMigration(t, cfg)
			if !p.Complete || p.Copied != 6 || p.Removed != 6 {
				t.Fatal(p)
			}
			bs, err := New(compact)
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			if _, err := bs.GetNode(1); err != nil {
				t.Fatal("source node lost", err)
			}
			if _, err := bs.GetRelationship(11); err != nil {
				t.Fatal("source relation lost", err)
			}
			if v, err := bs.MetaGet(historicalReverseProgressKey); err != nil {
				t.Fatal(err)
			} else {
				var check HistoricalReverseMigrationProgress
				if err := json.Unmarshal(v, &check); err != nil || !check.Complete {
					t.Fatal(check, err)
				}
			}
		})
	}
}

func TestCompactReverseErasureRefusesForgedForeignAndCrossKind(t *testing.T) {
	for _, kind := range []byte{diskMemberNodeLabel, diskMemberNodeProperty} {
		t.Run(string(rune('0'+kind)), func(t *testing.T) {
			cfg := membershipCrashConfig(t.TempDir())
			cfg.CompactHistoricalReverseOnDisk = true
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			for _, id := range []types.NodeID{1, 2} {
				if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
					t.Fatal(err)
				}
			}
			forward := diskMembershipOp(diskMembershipScope(kind, 1, "payload", "s:x"), 2, 0).key
			op := compactReverseMembershipOp(forward)
			binary.BigEndian.PutUint64(op.key[2:10], 1)
			// Structurally one owner's selector can only reconstruct that owner.
			binary.BigEndian.PutUint32(op.value, crc32.Checksum(op.key, diskMembershipReverseCRC))
			reconstructed, err := compactReverseMembershipForward(op.key, op.value, kind, 1)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(reconstructed, forward) {
				t.Fatal("redirected to foreign forward key")
			}
			// A cross-kind record with a valid CRC but inconsistent shape must fail
			// preflight and leave both original nodes untouched.
			op.key[1] = diskMemberNodeLabel
			binary.BigEndian.PutUint32(op.value, crc32.Checksum(op.key, diskMembershipReverseCRC))
			if kind == diskMemberNodeLabel {
				op.key = append(op.key, make([]byte, 32)...)
				binary.BigEndian.PutUint32(op.value, crc32.Checksum(op.key, diskMembershipReverseCRC))
			}
			if err := bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(op.key, op.value) }); err != nil {
				t.Fatal(err)
			}
			if _, err := bs.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{1}, Bounds: badgerExactErasureBounds}); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatal("forged cross-kind erase accepted", err)
			}
			for _, id := range []types.NodeID{1, 2} {
				if _, err := bs.GetNode(id); err != nil {
					t.Fatal("refusal mutated nodes", id, err)
				}
			}
		})
	}
}
