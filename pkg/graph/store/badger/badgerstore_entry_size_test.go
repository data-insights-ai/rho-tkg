package badger

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// KEYSIZE / VALUESIZE (tasks/evidence/USAGE/size_test.go.txt): before the write
// door checked entry sizes, an entry badger refuses (a row above
// ValueLogFileSize, an on-disk property index key above 65,000 bytes) was
// acknowledged, then failed every flush, and nothing written after it
// persisted. Every door that hands badger a row, a change-log record or an
// index key now refuses it with ErrEntryTooLarge before any mutation, and the
// store keeps persisting. Mutation: drop the check from one builder or door.

func esBigProps() map[string]string {
	p := map[string]string{}
	for i := 0; i < 24; i++ { // 24 x 60 KB: a 1.4 MB row, every value under rho's 64 KB limit
		p[fmt.Sprintf("p%02d", i)] = strings.Repeat("y", 60_000)
	}
	return p
}

func esNode(id int, label uint16, props map[string]string) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(id)), label, nil)
	for k, v := range props {
		_ = n.SetProperty(k, v)
	}
	return n
}

func esRel(id, from, to int, props map[string]string) *types.Relationship {
	r := types.NewRelationship(types.RelID(snowflake.ID(id)), 3, types.NodeID(snowflake.ID(from)), types.NodeID(snowflake.ID(to)))
	for k, v := range props {
		_ = r.SetProperty(k, v)
	}
	return r
}

var esNext int // ids of the ordinary writes after each refusal

// esAfter: the refused write changed nothing, and an ordinary write after it
// persists.
func esAfter(t *testing.T, bs *Store, door string, err error, check func() bool) {
	t.Helper()
	if !errors.Is(err, ErrEntryTooLarge) {
		t.Errorf("%s: err %v, want ErrEntryTooLarge", door, err)
	}
	if check != nil && !check() {
		t.Errorf("%s: the refused write changed the store", door)
	}
	esNext++
	if err := bs.PutNode(esNode(900_000+esNext, 7, map[string]string{"t": "ok"})); err != nil {
		t.Fatalf("%s: ordinary write after the refusal: %v", door, err)
	}
	if err := bs.Flush(); err != nil {
		t.Errorf("%s: the store stopped persisting after the refusal: %v", door, err)
	}
}

func TestEntryTooLargeRefusedAtEveryDoor(t *testing.T) {
	for _, mode := range []struct {
		name string
		cfg  func(dir string) Config
	}{
		{"value-log file 1 MB, change log off", func(dir string) Config {
			return Config{Dir: dir, ValueLogFileSize: 1 << 20}
		}},
		{"value-log file 1 MB, change log on", func(dir string) Config {
			return Config{Dir: dir, ValueLogFileSize: 1 << 20, ChangeLog: true}
		}},
		{"in memory", func(string) Config { return Config{InMemory: true, ChangeLog: true} }},
	} {
		t.Run(mode.name, func(t *testing.T) {
			bs, err := New(mode.cfg(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			small := map[string]string{"t": "small"}
			for _, id := range []int{1, 2} {
				if err := bs.PutNode(esNode(id, 7, small)); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.PutRelationship(esRel(50, 1, 2, small)); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			big := esBigProps()
			absent := func(id int) func() bool {
				return func() bool { _, err := bs.GetNode(types.NodeID(snowflake.ID(id))); return err != nil }
			}
			unchanged := func(id int) func() bool {
				return func() bool {
					n, err := bs.GetNode(types.NodeID(snowflake.ID(id)))
					if err != nil {
						return false
					}
					v, _ := n.GetProperty("t")
					return v == "small" && n.LabelTokenCount() == 1
				}
			}
			relAbsent := func(id int) func() bool {
				return func() bool { _, err := bs.GetRelationship(types.RelID(snowflake.ID(id))); return err != nil }
			}
			relUnchanged := func() bool {
				r, err := bs.GetRelationship(types.RelID(snowflake.ID(50)))
				if err != nil {
					return false
				}
				v, _ := r.GetProperty("t")
				return v == "small"
			}
			cur, _ := bs.GetNode(types.NodeID(snowflake.ID(1)))
			curRel, _ := bs.GetRelationship(types.RelID(snowflake.ID(50)))
			bigNode := func(id int) *types.Node { return esNode(id, 7, big) }
			nid1 := types.NodeID(snowflake.ID(1))

			esAfter(t, bs, "PutNode", bs.PutNode(bigNode(10)), absent(10))
			esAfter(t, bs, "ReplaceNode", bs.ReplaceNode(bigNode(1)), unchanged(1))
			addLabel := bigNode(1)
			addLabel.AddLabelTokenRaw(8)
			esAfter(t, bs, "AddNodeLabelToken", bs.AddNodeLabelToken(nid1, 8, addLabel), unchanged(1))
			esAfter(t, bs, "RemoveNodeLabelToken", bs.RemoveNodeLabelToken(nid1, 7, esNode(1, 9, big)), unchanged(1))
			esAfter(t, bs, "ReplaceNodeWithHistory", bs.ReplaceNodeWithHistory(bigNode(1), 0, cur), unchanged(1))
			esAfter(t, bs, "AddNodeLabelTokenWithHistory", bs.AddNodeLabelTokenWithHistory(nid1, 8, addLabel, 0, cur), unchanged(1))
			esAfter(t, bs, "RemoveNodeLabelTokenWithHistory", bs.RemoveNodeLabelTokenWithHistory(nid1, 7, esNode(1, 9, big), 0, cur), unchanged(1))
			esAfter(t, bs, "PutNodeVersion", bs.PutNodeVersion(types.NodeID(snowflake.ID(2)), 0, esNode(2, 7, big)), unchanged(2))
			esAfter(t, bs, "PutNodesBatch", bs.PutNodesBatch([]*types.Node{esNode(20, 7, small), bigNode(21)}), func() bool { return absent(20)() && absent(21)() })
			wire, err := storepkg.MarshalNodeWireWithKeys(bigNode(22), bs.propKeyReg.Load())
			if err != nil {
				t.Fatal(err)
			}
			esAfter(t, bs, "PutNodesBatchPreEncoded", bs.PutNodesBatchPreEncoded([]*types.Node{bigNode(22)}, [][]byte{wire}), absent(22))
			esAfter(t, bs, "PutRelationship", bs.PutRelationship(esRel(60, 1, 2, big)), relAbsent(60))
			esAfter(t, bs, "ReplaceRelationship", bs.ReplaceRelationship(esRel(50, 1, 2, big)), relUnchanged)
			esAfter(t, bs, "PutRelEntityAndOut", bs.PutRelEntityAndOut(esRel(61, 1, 2, big)), relAbsent(61))
			esAfter(t, bs, "PutRelationshipsBatch", bs.PutRelationshipsBatch([]*types.Relationship{esRel(62, 1, 2, small), esRel(63, 1, 2, big)}), func() bool { return relAbsent(62)() && relAbsent(63)() })
			esAfter(t, bs, "ReplaceRelWithHistory", bs.ReplaceRelWithHistory(esRel(50, 1, 2, big), 0, curRel), relUnchanged)
			esAfter(t, bs, "PutRelVersion", bs.PutRelVersion(types.RelID(snowflake.ID(50)), 0, esRel(50, 1, 2, big)), relUnchanged)
		})
	}
}

// KEYSIZE: an indexed string whose on-disk property index key would exceed
// badger's key limit (less the staging overhead) is refused at every node door
// that writes index keys; a string just under it is stored and found.
func TestIndexKeyTooLargeRefusedAtEveryNodeDoor(t *testing.T) {
	bs, err := New(Config{Dir: t.TempDir(), PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if err := bs.CreatePropertyIndex(7, "body"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(esNode(1, 7, map[string]string{"t": "small"})); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	long := map[string]string{"t": "small", "body": strings.Repeat("x", 65_100)}
	cur, _ := bs.GetNode(types.NodeID(snowflake.ID(1)))
	nid1 := types.NodeID(snowflake.ID(1))
	unchanged := func() bool {
		n, err := bs.GetNode(nid1)
		if err != nil {
			return false
		}
		_, has := n.GetProperty("body")
		return !has
	}
	absent := func(id int) func() bool {
		return func() bool { _, err := bs.GetNode(types.NodeID(snowflake.ID(id))); return err != nil }
	}
	addLabel := esNode(1, 9, long)
	addLabel.AddLabelTokenRaw(7)
	esAfter(t, bs, "PutNode", bs.PutNode(esNode(10, 7, long)), absent(10))
	esAfter(t, bs, "ReplaceNode", bs.ReplaceNode(esNode(1, 7, long)), unchanged)
	esAfter(t, bs, "ReplaceNodeWithHistory", bs.ReplaceNodeWithHistory(esNode(1, 7, long), 0, cur), unchanged)
	esAfter(t, bs, "PutNodesBatch", bs.PutNodesBatch([]*types.Node{esNode(20, 7, map[string]string{"t": "s"}), esNode(21, 7, long)}), func() bool { return absent(20)() && absent(21)() })
	esAfter(t, bs, "AddNodeLabelToken", bs.AddNodeLabelToken(nid1, 7, addLabel), unchanged)
	esAfter(t, bs, "AddNodeLabelTokenWithHistory", bs.AddNodeLabelTokenWithHistory(nid1, 7, addLabel, 0, cur), unchanged)

	// Exactly at the limit: stored, flushed, found. The key's overhead is
	// measured with rho's own key builders on a one-byte string.
	payload1, _ := storepkg.PropertyIndexValueBytes(types.IndexablePropertyValueKey("z"))
	overhead := len(storepkg.PropertyIndexEntryKey(0, payload1, snowflake.ID(30))) - 1
	fits := strings.Repeat("z", bs.maxEntryKey-overhead)
	esAfter(t, bs, "PutNode one byte over", bs.PutNode(esNode(31, 7, map[string]string{"body": fits + "z"})), absent(31))
	if err := bs.PutNode(esNode(30, 7, map[string]string{"body": fits})); err != nil {
		t.Fatalf("a string just under the key limit: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("flush of a key just under the limit: %v", err)
	}
	got, err := bs.NodesByLabelAndProperty(7, "body", fits, QueryOpts{})
	if err != nil || len(got) != 1 {
		t.Fatalf("lookup of a key just under the limit: %d nodes, %v", len(got), err)
	}
}

// TestCascadeDeleteRecordTooLargeRefused: a cascade delete's change-log record
// names every cascaded relationship and is built after the mutation, so its
// size is bounded before it (checkCascadeRecordLocked): in memory (1 MB) a
// node with 120,000 relationships is refused with nothing deleted, and the
// store keeps persisting. Mutation: no bound (the record is refused only at
// flush, after the delete, and every later flush fails).
func TestCascadeDeleteRecordTooLargeRefused(t *testing.T) {
	bs, err := New(Config{InMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	for _, id := range []int{1, 2} {
		if err := bs.PutNode(esNode(id, 7, nil)); err != nil {
			t.Fatal(err)
		}
	}
	const rels = 120_000
	batch := make([]*types.Relationship, 0, 10_000)
	for i := 0; i < rels; i++ {
		batch = append(batch, esRel(1_000_000+i, 1, 2, nil))
		if len(batch) == cap(batch) {
			if err := bs.PutRelationshipsBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	err = bs.DeleteNodeCascade(types.NodeID(snowflake.ID(1)))
	esAfter(t, bs, "DeleteNodeCascade", err, func() bool {
		_, nodeErr := bs.GetNode(types.NodeID(snowflake.ID(1)))
		_, relErr := bs.GetRelationship(types.RelID(snowflake.ID(1_000_000)))
		return nodeErr == nil && relErr == nil
	})
}

// TestMetaSetDurableRaisingTooLargeRefused: MetaSetDurableRaising buffers its
// entry and change-log record like a row (store.MetaFormatMarkerCapability), so
// it refuses a value the flush could not commit. Mutation: no check (the entry
// is buffered and every later flush fails).
func TestMetaSetDurableRaisingTooLargeRefused(t *testing.T) {
	bs, err := New(Config{InMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	err = bs.MetaSetDurableRaising("zz_big", make([]byte, 1<<20), 0)
	esAfter(t, bs, "MetaSetDurableRaising", err, func() bool {
		v, err := bs.MetaGet("zz_big")
		return err == nil && v == nil
	})
}
