package graph

import (
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
	return &BadgerStore{db: db}, nil
}

// Close closes the Badger database.
func (bs *BadgerStore) Close() error {
	return bs.db.Close()
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

		return nil
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
		return txn.Delete(nk)
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

		return nil
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
		return txn.Delete(rk)
	})
}

// --- Index queries ---

// NodesByLabel returns all nodes with the given label token.
// Results are sorted by snowflake.ID (chronological) due to big-endian key encoding.
func (bs *BadgerStore) NodesByLabel(token uint16) []*types.Node {
	var nodes []*types.Node
	prefix := labelIndexPrefix(token)

	_ = bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // index keys have no values
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			nodeID := parseNodeIDFromLabelIdx(it.Item().Key())
			item, err := txn.Get(nodeKey(nodeID))
			if err != nil {
				continue // index orphan; skip silently
			}
			_ = item.Value(func(val []byte) error {
				var w nodeWire
				if err := msgpack.Unmarshal(val, &w); err != nil {
					return err
				}
				nodes = append(nodes, wireToNode(w))
				return nil
			})
		}
		return nil
	})

	return nodes
}

// RelationshipsByType returns all relationships with the given type token.
// Results are sorted by snowflake.ID (chronological) due to big-endian key encoding.
func (bs *BadgerStore) RelationshipsByType(token uint16) []*types.Relationship {
	var rels []*types.Relationship
	prefix := relTypeIndexPrefix(token)

	_ = bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			relID := parseRelIDFromTypeIdx(it.Item().Key())
			item, err := txn.Get(relKey(relID))
			if err != nil {
				continue
			}
			_ = item.Value(func(val []byte) error {
				var w relWire
				if err := msgpack.Unmarshal(val, &w); err != nil {
					return err
				}
				rels = append(rels, wireToRel(w))
				return nil
			})
		}
		return nil
	})

	return rels
}

// --- Adjacency queries ---

// OutgoingRelationships returns relationships starting from the given node.
// If typeToken is 0, returns all outgoing; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) OutgoingRelationships(nodeID snowflake.ID, typeToken uint16) []*types.Relationship {
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
func (bs *BadgerStore) IncomingRelationships(nodeID snowflake.ID, typeToken uint16) []*types.Relationship {
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
func (bs *BadgerStore) scanAdjacency(prefix []byte) []*types.Relationship {
	var rels []*types.Relationship

	_ = bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			relID := parseRelIDFromAdjKey(it.Item().Key())
			item, err := txn.Get(relKey(relID))
			if err != nil {
				continue
			}
			_ = item.Value(func(val []byte) error {
				var w relWire
				if err := msgpack.Unmarshal(val, &w); err != nil {
					return err
				}
				rels = append(rels, wireToRel(w))
				return nil
			})
		}
		return nil
	})

	// Adjacency keys are grouped by (relType, endID/startID, relID), not by relID
	// alone. Sort by relID for deterministic, chronological output.
	sort.Slice(rels, func(i, j int) bool {
		return rels[i].InternalID().SnowflakeID() < rels[j].InternalID().SnowflakeID()
	})

	return rels
}

// --- Counts ---

// NodeCount returns the number of stored nodes.
func (bs *BadgerStore) NodeCount() int {
	count := 0
	prefix := []byte{keyNode}

	_ = bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})

	return count
}

// RelationshipCount returns the number of stored relationships.
func (bs *BadgerStore) RelationshipCount() int {
	count := 0
	prefix := []byte{keyRel}

	_ = bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})

	return count
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
