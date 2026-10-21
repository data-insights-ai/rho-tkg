package badger

import (
	"fmt"

	badgerv4 "github.com/dgraph-io/badger/v4"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Index-key repair at a full rebuild (backlog 54, Fable review of v4.51.3).
//
// A crash inside a flush that badger split into several transactions can leave
// label, type, outgoing or incoming keys that disagree with the rows: a key of
// a change whose row did not land, or a row whose key did not. The full rebuild
// takes the rows as the truth and skips such keys (or adds the row's entry
// without its key). A clean reopen, however, trusts every key of a live
// entity, so an unrepaired key would turn into an answer after the next clean
// Close. A writable rebuild therefore rewrites the keys to match the rows:
// it deletes every key it skipped because the row disagrees or does not exist,
// and writes every key a decoded row needs and the keyspace lacks. A read-only
// rebuild cannot write and never writes an image; a writable one whose repair
// fails writes no image either (openImageSuppressed), so the next open
// rebuilds and repairs again.
//
// Keys of rows that fail to decode stay as they are (the corruption contract:
// skipped, counted, never invented), and so do incoming keys without a local
// relationship row (a tiered cross-shard leg) and outgoing keys whose start
// node lives on another shard.

// keyRepair collects the repair writes of one full rebuild.
type keyRepair struct {
	ops []writeOp
	// accepted counts the keys that matched a decoded row, per keyspace;
	// expected counts the keys the decoded rows need. A shortfall means some
	// key is missing and triggers one more pass over that keyspace.
	acceptedLabels, expectedLabels int
	acceptedTypes, acceptedOut     int
	acceptedIn, expectedIn         int
}

func (kr *keyRepair) drop(key []byte) {
	kr.ops = append(kr.ops, writeOp{opType: writeOpDelete, key: append([]byte(nil), key...)})
}

// addMissing writes every key of want that the prefix keyspace lacks.
func (kr *keyRepair) addMissing(txn *badgerv4.Txn, prefix byte, want map[string]struct{}) {
	opts := badgerv4.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	p := []byte{prefix}
	for it.Seek(p); it.ValidForPrefix(p) && len(want) > 0; it.Next() {
		delete(want, string(it.Item().Key()))
	}
	for k := range want {
		kr.ops = append(kr.ops, writeOp{opType: writeOpSet, key: []byte(k)})
	}
}

// addMissingKeys runs the extra passes for the keyspaces with a shortfall.
// Caller is inside the rebuild's View, after the four key scans.
func (kr *keyRepair) addMissingKeys(txn *badgerv4.Txn, labels map[types.NodeID]map[uint16]struct{},
	rels map[types.RelID]RelDeleteInfo, inKeyHere func(RelDeleteInfo) bool) {
	if kr.acceptedLabels < kr.expectedLabels {
		want := make(map[string]struct{})
		for nid, toks := range labels {
			for tok := range toks {
				want[string(storepkg.LabelIndexKey(tok, nid.SnowflakeID()))] = struct{}{}
			}
		}
		kr.addMissing(txn, storepkg.KeyLabel, want)
	}
	if kr.acceptedTypes < len(rels) {
		want := make(map[string]struct{}, len(rels))
		for _, info := range rels {
			want[string(storepkg.RelTypeIndexKey(info.RelType, info.ID))] = struct{}{}
		}
		kr.addMissing(txn, storepkg.KeyRelType, want)
	}
	if kr.acceptedOut < len(rels) {
		want := make(map[string]struct{}, len(rels))
		for _, info := range rels {
			want[string(storepkg.OutKey(info.StartID, info.RelType, info.EndID, info.ID))] = struct{}{}
		}
		kr.addMissing(txn, storepkg.KeyOut, want)
	}
	if kr.acceptedIn < kr.expectedIn {
		want := make(map[string]struct{})
		for _, info := range rels {
			if inKeyHere(info) { // a local end, or a co-located rel (tiered mint shard)
				want[string(storepkg.InKey(info.EndID, info.RelType, info.StartID, info.ID))] = struct{}{}
			}
		}
		kr.addMissing(txn, storepkg.KeyIn, want)
	}
}

// commit writes the repair. A read-only store cannot: it has rebuilt its RAM
// state from the rows and writes no image, so nothing trusts the keys.
func (bs *Store) commitKeyRepair(kr *keyRepair) {
	if len(kr.ops) == 0 || bs.readOnly || bs.inMemory {
		return
	}
	if err := bs.writeKeyRepair(kr.ops); err != nil {
		bs.openImageSuppressed.Store(true) // the next open must rebuild and repair again
		if bs.logger != nil {
			bs.logger.Warningf("graph: index-key repair at open failed, no open image this session: %v", err)
		}
		return
	}
	if bs.logger != nil {
		bs.logger.Warningf("graph: open repaired %d index keys that disagreed with their rows", len(kr.ops))
	}
}

func (bs *Store) writeKeyRepair(ops []writeOp) error {
	wb := bs.db.NewWriteBatch()
	defer wb.Cancel()
	for _, op := range ops {
		var err error
		if op.opType == writeOpDelete {
			err = wb.Delete(op.key)
		} else {
			err = wb.Set(op.key, op.value)
		}
		if err != nil {
			return fmt.Errorf("graph: index-key repair: %w", err)
		}
	}
	if keyRepairFaultForTest != nil {
		return keyRepairFaultForTest()
	}
	return wb.Flush()
}

// keyRepairFaultForTest, when set, fails the repair commit (tests only).
var keyRepairFaultForTest func() error
