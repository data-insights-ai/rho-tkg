package badger

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// TestTemporalIndexOnDisk_MintKeyedKeyspaceIsRebuilt (backlog 62): before
// v4.54.0 the persisted 0x0B keyspace keyed an untimed row by its ID's mint
// instant; the row now starts at its TxFrom, which a tx backfill puts BEFORE
// the mint. A keyspace written under the old marker must not be trusted (its
// envelope would start after the row and prune it at a valid instant): the open
// rebuilds it — one row per node, keyed by TxFrom, the old marker gone — and
// the temporal prune keeps the node at its TxFrom. keepV2 is the downgrade
// case: an older release rebuilt the keyspace mint-keyed and wrote the old
// marker, but left the new marker in place.
func TestTemporalIndexOnDisk_MintKeyedKeyspaceIsRebuilt(t *testing.T) {
	t.Parallel()
	for _, keepV2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("keepV2=%v", keepV2), func(t *testing.T) {
			mintKeyedKeyspaceIsRebuilt(t, keepV2)
		})
	}
}

func mintKeyedKeyspaceIsRebuilt(t *testing.T, keepV2 bool) {
	dir := t.TempDir()
	const label = uint16(3)
	id := types.NodeID(snowflake.ID(1) << 22) // mint instant ≈ the layout epoch
	const txFrom = types.Instant(7_000)       // backfilled: long before the mint
	if id.MintInstant() <= txFrom {
		t.Fatalf("setup: mint %d must follow TxFrom %d", id.MintInstant(), txFrom)
	}

	bs, err := New(Config{Dir: dir, TemporalIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateTemporalIndex(label); err != nil {
		t.Fatal(err)
	}
	nd := types.NewNode(id, label, nil)
	nd.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom})
	if err := bs.PutNode(nd); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	// Rewrite the keyspace as an older release left it: the row keyed by the
	// mint start, the old marker, no new marker.
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		if err := txn.Delete(storepkg.TemporalIndexEntryKey(label, txFrom, id.SnowflakeID())); err != nil {
			return err
		}
		if !keepV2 {
			if err := txn.Delete(storepkg.TemporalIndexOnDiskBuiltKey); err != nil {
				return err
			}
		}
		if err := txn.Set(storepkg.TemporalIndexEntryKey(label, id.MintInstant(), id.SnowflakeID()), storepkg.TemporalIndexEntryValue(0)); err != nil {
			return err
		}
		return txn.Set(storepkg.TemporalIndexOnDiskBuiltV1Key, []byte{1})
	}); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	bs2, err := New(Config{Dir: dir, TemporalIndexOnDisk: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bs2.Close() })
	if got := bs2.TemporalIndexDiskEntryCountForTest(label); got != 1 {
		t.Errorf("0x0B rows = %d after the rebuild, want 1 (the mint-keyed row deleted)", got)
	}
	if err := bs2.db.View(func(txn *badgerv4.Txn) error {
		if _, err := txn.Get(storepkg.TemporalIndexEntryKey(label, txFrom, id.SnowflakeID())); err != nil {
			t.Errorf("row keyed by TxFrom: %v", err)
		}
		if _, err := txn.Get(storepkg.TemporalIndexOnDiskBuiltV1Key); err == nil {
			t.Error("the old marker survived the rebuild")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	kept, ok := bs2.PruneTemporalCandidates(label, []types.NodeID{id}, QueryOpts{ValidAt: txFrom})
	if !ok || len(kept) != 1 {
		t.Errorf("PruneTemporalCandidates(ValidAt = TxFrom) = %v (ok %v), want the node kept", kept, ok)
	}
}
