package graph

import (
	"encoding/binary"
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// BadgerStoreConfig configures a BadgerStore instance.
type BadgerStoreConfig struct {
	// Dir is the Badger data directory. Required unless InMemory is true.
	Dir string
	// InMemory enables memory-only mode (no disk I/O). Useful for testing.
	InMemory bool
	// Logger is the Badger logger. Nil uses Badger's default logger.
	Logger badger.Logger
}

// BadgerStore implements the Store interface using Badger as the storage backend.
// All entity data is serialized using msgpack; keys use fixed-width binary encoding
// for correct sort order.
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore opens a Badger database with the given configuration.
func NewBadgerStore(cfg BadgerStoreConfig) (*BadgerStore, error) {
	opts := badger.DefaultOptions(cfg.Dir)
	if cfg.InMemory {
		opts = opts.WithInMemory(true)
	}
	if cfg.Logger != nil {
		opts = opts.WithLogger(cfg.Logger)
	} else {
		opts = opts.WithLogger(nil) // suppress default Badger logs
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("graph: badger open: %w", err)
	}
	bs := &BadgerStore{db: db}
	if err := bs.initCounters(); err != nil {
		_ = db.Close() // best-effort cleanup
		return nil, fmt.Errorf("graph: init counters: %w", err)
	}
	return bs, nil
}

// Close closes the Badger database.
func (bs *BadgerStore) Close() error {
	return bs.db.Close()
}

// --- Atomic counters ---
//
// Node and relationship counts are maintained as metadata keys for O(1) reads.
// Each mutating transaction increments/decrements the counter atomically.

var (
	counterNodeCountKey = metaKey("node_count")
	counterRelCountKey  = metaKey("rel_count")
)

// getCounter reads a big-endian int64 counter from the given key within txn.
// Returns 0 if the key does not exist.
func getCounter(txn *badger.Txn, key []byte) (int64, error) {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var val int64
	err = item.Value(func(v []byte) error {
		if len(v) != 8 {
			return fmt.Errorf("graph: counter value size %d, want 8", len(v))
		}
		val = int64(binary.BigEndian.Uint64(v)) // #nosec G115 — inverse of setCounter encoding
		return nil
	})
	return val, err
}

// setCounter writes a big-endian int64 counter to the given key within txn.
func setCounter(txn *badger.Txn, key []byte, val int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(val)) // #nosec G115 — intentional int64→uint64 for binary encoding
	return txn.Set(key, buf)
}

// incrCounter atomically reads, modifies, and writes a counter within txn.
func incrCounter(txn *badger.Txn, key []byte, delta int64) error {
	current, err := getCounter(txn, key)
	if err != nil {
		return err
	}
	return setCounter(txn, key, current+delta)
}

// initCounters initializes counter metadata keys if they don't exist.
// For fresh databases or migration from older versions without counters,
// counts entities by scanning then persists the result.
func (bs *BadgerStore) initCounters() error {
	return bs.db.Update(func(txn *badger.Txn) error {
		// If node counter already exists, counters are initialized.
		if _, err := txn.Get(counterNodeCountKey); err == nil {
			return nil
		} else if err != badger.ErrKeyNotFound {
			return err
		}

		// Count nodes by prefix scan.
		nodeCount := int64(0)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		prefix := []byte{keyNode}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			nodeCount++
		}
		it.Close()

		// Count relationships by prefix scan.
		relCount := int64(0)
		it = txn.NewIterator(opts)
		prefix = []byte{keyRel}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			relCount++
		}
		it.Close()

		if err := setCounter(txn, counterNodeCountKey, nodeCount); err != nil {
			return err
		}
		return setCounter(txn, counterRelCountKey, relCount)
	})
}

// --- Node operations ---

// PutNode stores a node with its label index entries.
// Returns ErrNodeExists if a node with the same ID already exists.
func (bs *BadgerStore) PutNode(n *types.Node) error {
	id := int64(n.InternalID().SnowflakeID())

	w := nodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	return bs.db.Update(func(txn *badger.Txn) error {
		nk := nodeKey(id)

		// Check for duplicate.
		if _, err := txn.Get(nk); err == nil {
			return ErrNodeExists
		}

		// Store node entity.
		if err := txn.Set(nk, data); err != nil {
			return err
		}

		// Label index entries.
		for _, tok := range n.AllLabelTokens() {
			lik := labelIndexKey(tok.Value(), id)
			if err := txn.Set(lik, nil); err != nil {
				return err
			}
		}

		return incrCounter(txn, counterNodeCountKey, 1)
	})
}

// GetNode retrieves a node by its snowflake ID.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) GetNode(id snowflake.ID) (*types.Node, error) {
	var n *types.Node
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(int64(id)))
		if err == badger.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w nodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			n = wireToNode(w)
			return nil
		})
	})
	return n, err
}

// DeleteNode removes a node and its label index entries.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) DeleteNode(id snowflake.ID) error {
	return bs.db.Update(func(txn *badger.Txn) error {
		nk := nodeKey(int64(id))
		item, err := txn.Get(nk)
		if err == badger.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}

		// Unmarshal to get label tokens for index cleanup.
		var w nodeWire
		if err := item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &w)
		}); err != nil {
			return fmt.Errorf("graph: unmarshal node for delete: %w", err)
		}

		// Delete label index entries.
		allTokens := []int{w.PrimaryLabel}
		allTokens = append(allTokens, w.ExtraLabels...)
		for _, tok := range allTokens {
			lik := labelIndexKey(uint16(tok), int64(id)) // #nosec G115 — token from our own serialization, always in uint16 range
			if err := txn.Delete(lik); err != nil {
				return err
			}
		}

		// Delete node entity.
		if err := txn.Delete(nk); err != nil {
			return err
		}

		return incrCounter(txn, counterNodeCountKey, -1)
	})
}

// --- Relationship operations ---

// PutRelationship stores a relationship with type index and adjacency entries.
// Returns ErrNodeNotFound if the start or end node does not exist.
// Returns ErrRelExists if a relationship with the same ID already exists.
func (bs *BadgerStore) PutRelationship(r *types.Relationship) error {
	id := int64(r.InternalID().SnowflakeID())
	startID := int64(r.StartNodeID().SnowflakeID())
	endID := int64(r.EndNodeID().SnowflakeID())
	relType := r.TypeToken().Value()

	w := relToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	return bs.db.Update(func(txn *badger.Txn) error {
		// Verify endpoints exist.
		if _, err := txn.Get(nodeKey(startID)); err == badger.ErrKeyNotFound {
			return ErrNodeNotFound
		} else if err != nil {
			return err
		}
		if _, err := txn.Get(nodeKey(endID)); err == badger.ErrKeyNotFound {
			return ErrNodeNotFound
		} else if err != nil {
			return err
		}

		rk := relKey(id)
		if _, err := txn.Get(rk); err == nil {
			return ErrRelExists
		}

		// Store relationship entity.
		if err := txn.Set(rk, data); err != nil {
			return err
		}

		// Type index.
		if err := txn.Set(relTypeIndexKey(relType, id), nil); err != nil {
			return err
		}

		// Adjacency: outgoing.
		if err := txn.Set(outKey(startID, relType, endID, id), nil); err != nil {
			return err
		}

		// Adjacency: incoming.
		if err := txn.Set(inKey(endID, relType, startID, id), nil); err != nil {
			return err
		}

		return incrCounter(txn, counterRelCountKey, 1)
	})
}

// GetRelationship retrieves a relationship by its snowflake ID.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *BadgerStore) GetRelationship(id snowflake.ID) (*types.Relationship, error) {
	var r *types.Relationship
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(relKey(int64(id)))
		if err == badger.ErrKeyNotFound {
			return ErrRelNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w relWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal relationship: %w", err)
			}
			r = wireToRel(w)
			return nil
		})
	})
	return r, err
}

// DeleteRelationship removes a relationship and cleans up type + adjacency indexes.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *BadgerStore) DeleteRelationship(id snowflake.ID) error {
	return bs.db.Update(func(txn *badger.Txn) error {
		rk := relKey(int64(id))
		item, err := txn.Get(rk)
		if err == badger.ErrKeyNotFound {
			return ErrRelNotFound
		}
		if err != nil {
			return err
		}

		// Unmarshal to get type/start/end for index cleanup.
		var w relWire
		if err := item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &w)
		}); err != nil {
			return fmt.Errorf("graph: unmarshal relationship for delete: %w", err)
		}

		relType := uint16(w.RelType) // #nosec G115 — token from our own serialization, always in uint16 range

		// Delete type index.
		if err := txn.Delete(relTypeIndexKey(relType, int64(id))); err != nil {
			return err
		}

		// Delete adjacency: outgoing.
		if err := txn.Delete(outKey(w.StartID, relType, w.EndID, int64(id))); err != nil {
			return err
		}

		// Delete adjacency: incoming.
		if err := txn.Delete(inKey(w.EndID, relType, w.StartID, int64(id))); err != nil {
			return err
		}

		// Delete relationship entity.
		if err := txn.Delete(rk); err != nil {
			return err
		}

		return incrCounter(txn, counterRelCountKey, -1)
	})
}

// --- Index queries ---

// NodesByLabel returns all nodes with the given label token.
// Results are sorted by snowflake.ID (chronological) due to big-endian key encoding.
func (bs *BadgerStore) NodesByLabel(token uint16) ([]*types.Node, error) {
	var nodes []*types.Node
	prefix := labelIndexPrefix(token)

	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // index keys have no values
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			nodeID := parseNodeIDFromLabelIdx(it.Item().Key())
			item, err := txn.Get(nodeKey(nodeID))
			if err == badger.ErrKeyNotFound {
				continue // index orphan; skip silently
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(val []byte) error {
				var w nodeWire
				if err := msgpack.Unmarshal(val, &w); err != nil {
					return fmt.Errorf("graph: unmarshal node %d: %w", nodeID, err)
				}
				nodes = append(nodes, wireToNode(w))
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})

	return nodes, err
}

// RelationshipsByType returns all relationships with the given type token.
// Results are sorted by snowflake.ID (chronological) due to big-endian key encoding.
func (bs *BadgerStore) RelationshipsByType(token uint16) ([]*types.Relationship, error) {
	var rels []*types.Relationship
	prefix := relTypeIndexPrefix(token)

	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			relID := parseRelIDFromTypeIdx(it.Item().Key())
			item, err := txn.Get(relKey(relID))
			if err == badger.ErrKeyNotFound {
				continue // index orphan
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(val []byte) error {
				var w relWire
				if err := msgpack.Unmarshal(val, &w); err != nil {
					return fmt.Errorf("graph: unmarshal rel %d: %w", relID, err)
				}
				rels = append(rels, wireToRel(w))
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})

	return rels, err
}

// --- Adjacency queries ---

// OutgoingRelationships returns relationships starting from the given node.
// If typeToken is 0, returns all outgoing; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) OutgoingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error) {
	var prefix []byte
	if typeToken == 0 {
		prefix = outPrefix(int64(nodeID))
	} else {
		prefix = outTypedPrefix(int64(nodeID), typeToken)
	}

	return bs.scanAdjacency(prefix)
}

// IncomingRelationships returns relationships ending at the given node.
// If typeToken is 0, returns all incoming; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) IncomingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error) {
	var prefix []byte
	if typeToken == 0 {
		prefix = inPrefix(int64(nodeID))
	} else {
		prefix = inTypedPrefix(int64(nodeID), typeToken)
	}

	return bs.scanAdjacency(prefix)
}

// scanAdjacency scans adjacency keys with the given prefix, fetches each
// relationship, and returns them sorted by snowflake.ID.
func (bs *BadgerStore) scanAdjacency(prefix []byte) ([]*types.Relationship, error) {
	var rels []*types.Relationship

	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			relID := parseRelIDFromAdjKey(it.Item().Key())
			item, err := txn.Get(relKey(relID))
			if err == badger.ErrKeyNotFound {
				continue // orphan adjacency entry
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(val []byte) error {
				var w relWire
				if err := msgpack.Unmarshal(val, &w); err != nil {
					return fmt.Errorf("graph: unmarshal rel %d: %w", relID, err)
				}
				rels = append(rels, wireToRel(w))
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})

	// Adjacency keys are grouped by (relType, endID/startID, relID), not by relID
	// alone. Sort by relID for deterministic, chronological output.
	sort.Slice(rels, func(i, j int) bool {
		return rels[i].InternalID().SnowflakeID() < rels[j].InternalID().SnowflakeID()
	})

	return rels, err
}

// --- Cascade operations ---

// DeleteNodeCascade atomically removes a node and all connected relationships
// in a single Badger Update() transaction — no TOCTOU window.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) DeleteNodeCascade(id snowflake.ID) error {
	nodeID := int64(id)

	return bs.db.Update(func(txn *badger.Txn) error {
		// 1. Get node entity for label index cleanup.
		nk := nodeKey(nodeID)
		item, err := txn.Get(nk)
		if err == badger.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		var w nodeWire
		if err := item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &w)
		}); err != nil {
			return fmt.Errorf("graph: unmarshal node for cascade: %w", err)
		}

		// 2. Collect all connected relIDs from adjacency indexes.
		// Each entry holds the data needed to delete all index keys for that rel.
		type relInfo struct {
			relType uint16
			startID int64
			endID   int64
		}
		relInfos := make(map[int64]relInfo)

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		// Scan outgoing adjacency: outKey = keyOut(1B) + startID(8B) + relType(2B) + endID(8B) + relID(8B)
		outPfx := outPrefix(nodeID)
		it := txn.NewIterator(opts)
		for it.Seek(outPfx); it.ValidForPrefix(outPfx); it.Next() {
			key := it.Item().KeyCopy(nil)
			relID := parseRelIDFromAdjKey(key)
			relType := binary.BigEndian.Uint16(key[9:])
			endID := parseIDFromKey(key, 11)
			relInfos[relID] = relInfo{relType: relType, startID: nodeID, endID: endID}
		}
		it.Close()

		// Scan incoming adjacency: inKey = keyIn(1B) + endID(8B) + relType(2B) + startID(8B) + relID(8B)
		inPfx := inPrefix(nodeID)
		it = txn.NewIterator(opts)
		for it.Seek(inPfx); it.ValidForPrefix(inPfx); it.Next() {
			key := it.Item().KeyCopy(nil)
			relID := parseRelIDFromAdjKey(key)
			if _, exists := relInfos[relID]; exists {
				continue // dedup self-loops
			}
			relType := binary.BigEndian.Uint16(key[9:])
			startID := parseIDFromKey(key, 11)
			relInfos[relID] = relInfo{relType: relType, startID: startID, endID: nodeID}
		}
		it.Close()

		// 3. Delete each relationship and all its index entries.
		for relID, ri := range relInfos {
			// Relationship entity.
			if err := txn.Delete(relKey(relID)); err != nil {
				return err
			}
			// Type index.
			if err := txn.Delete(relTypeIndexKey(ri.relType, relID)); err != nil {
				return err
			}
			// Outgoing adjacency.
			if err := txn.Delete(outKey(ri.startID, ri.relType, ri.endID, relID)); err != nil {
				return err
			}
			// Incoming adjacency.
			if err := txn.Delete(inKey(ri.endID, ri.relType, ri.startID, relID)); err != nil {
				return err
			}
		}

		// 4. Delete label index entries.
		allTokens := []int{w.PrimaryLabel}
		allTokens = append(allTokens, w.ExtraLabels...)
		for _, tok := range allTokens {
			if err := txn.Delete(labelIndexKey(uint16(tok), nodeID)); err != nil { // #nosec G115 — token from our own serialization
				return err
			}
		}

		// 5. Delete node entity.
		if err := txn.Delete(nk); err != nil {
			return err
		}

		// 6. Update counters: node -1, rels -N.
		if err := incrCounter(txn, counterNodeCountKey, -1); err != nil {
			return err
		}
		relCount := int64(len(relInfos))
		if relCount > 0 {
			if err := incrCounter(txn, counterRelCountKey, -relCount); err != nil {
				return err
			}
		}

		return nil
	})
}

// --- Counts (O(1) via atomic counters) ---

// NodeCount returns the number of stored nodes.
// Reads the counter metadata key — O(1) regardless of graph size.
func (bs *BadgerStore) NodeCount() (int, error) {
	var count int64
	err := bs.db.View(func(txn *badger.Txn) error {
		var err error
		count, err = getCounter(txn, counterNodeCountKey)
		return err
	})
	return int(count), err // #nosec G115 — count is always non-negative and within int range
}

// RelationshipCount returns the number of stored relationships.
// Reads the counter metadata key — O(1) regardless of graph size.
func (bs *BadgerStore) RelationshipCount() (int, error) {
	var count int64
	err := bs.db.View(func(txn *badger.Txn) error {
		var err error
		count, err = getCounter(txn, counterRelCountKey)
		return err
	})
	return int(count), err // #nosec G115 — count is always non-negative and within int range
}

// --- Registry persistence ---

// SaveLabelRegistry persists the label registry to the Badger store.
func (bs *BadgerStore) SaveLabelRegistry(reg *labelRegistry) error {
	names := reg.ExportNames()
	data, err := msgpack.Marshal(names)
	if err != nil {
		return fmt.Errorf("graph: marshal label registry: %w", err)
	}
	return bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey("label_tokens"), data)
	})
}

// LoadLabelRegistry loads the label registry from the Badger store.
// Returns false if no saved data exists (fresh database).
func (bs *BadgerStore) LoadLabelRegistry(reg *labelRegistry) (bool, error) {
	var names []string
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey("label_tokens"))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &names)
		})
	})
	if err != nil {
		return false, fmt.Errorf("graph: load label registry: %w", err)
	}
	if names == nil {
		return false, nil
	}
	return true, reg.ImportNames(names)
}

// SaveRelTypeRegistry persists the relationship type registry to the Badger store.
func (bs *BadgerStore) SaveRelTypeRegistry(reg *relTypeRegistry) error {
	names := reg.ExportNames()
	data, err := msgpack.Marshal(names)
	if err != nil {
		return fmt.Errorf("graph: marshal reltype registry: %w", err)
	}
	return bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey("reltype_tokens"), data)
	})
}

// LoadRelTypeRegistry loads the relationship type registry from the Badger store.
// Returns false if no saved data exists (fresh database).
func (bs *BadgerStore) LoadRelTypeRegistry(reg *relTypeRegistry) (bool, error) {
	var names []string
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey("reltype_tokens"))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &names)
		})
	})
	if err != nil {
		return false, fmt.Errorf("graph: load reltype registry: %w", err)
	}
	if names == nil {
		return false, nil
	}
	return true, reg.ImportNames(names)
}
