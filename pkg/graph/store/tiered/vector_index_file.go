package tiered

import (
	"errors"
	"fmt"
	"os"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// vectorIdxDef is the store-level serialization format for TieredStore vector
// indexes. The entries themselves are rebuilt from node properties on open.
type vectorIdxDef struct {
	LabelToken  uint16         `msgpack:"l"`
	PropertyKey string         `msgpack:"p"`
	Dims        int            `msgpack:"d"`
	Metric      DistanceMetric `msgpack:"m"`
}

func saveVectorIndexFile(path string, defs []vectorIdxDef) error {
	if err := validateVectorIndexFileDefs(defs); err != nil {
		return err
	}
	if len(defs) == 0 {
		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("vector index file: remove: %w", err)
		}
		if err := syncParentDir(path, "vector index file"); err != nil {
			return err
		}
		return nil
	}
	data, err := msgpack.Marshal(defs)
	if err != nil {
		return fmt.Errorf("vector index file: marshal: %w", err)
	}
	return atomicWriteFile(path, data, "vector index file")
}

func validateVectorIndexFileDefs(defs []vectorIdxDef) error {
	seenDefs := make(map[indexpkg.VectorIndexKey]vectorIdxDef, len(defs))
	for _, def := range defs {
		if err := storecontract.ValidateLabelToken(def.LabelToken); err != nil {
			return fmt.Errorf("vector index file: invalid definition label %d property %q: %w",
				def.LabelToken, def.PropertyKey, err)
		}
		if err := storecontract.ValidateIndexPropertyKey(def.PropertyKey); err != nil {
			return fmt.Errorf("vector index file: invalid definition label %d property %q: %w",
				def.LabelToken, def.PropertyKey, err)
		}
		if err := indexpkg.ValidateVectorIndexConfig(def.Dims, def.Metric); err != nil {
			return fmt.Errorf("vector index file: invalid definition label %d property %q: %w",
				def.LabelToken, def.PropertyKey, err)
		}
		key := indexpkg.VectorIndexKey{LabelToken: def.LabelToken, PropertyKey: def.PropertyKey}
		if existing, exists := seenDefs[key]; exists {
			if existing.Dims != def.Dims || existing.Metric != def.Metric {
				return fmt.Errorf("vector index file: label %d property %q has conflicting definitions: %w",
					def.LabelToken, def.PropertyKey, ErrVectorIndexExists)
			}
			return fmt.Errorf("vector index file: duplicate definition label %d property %q: %w",
				def.LabelToken, def.PropertyKey, ErrVectorIndexExists)
		}
		seenDefs[key] = def
	}
	return nil
}

type vectorIndexFileSnapshot struct {
	path   string
	data   []byte
	exists bool
}

func snapshotVectorIndexFile(path string) (vectorIndexFileSnapshot, error) {
	if path == "" {
		return vectorIndexFileSnapshot{}, nil
	}
	data, err := os.ReadFile(path) // #nosec G304 — path derived from trusted Store config
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return vectorIndexFileSnapshot{path: path}, nil
		}
		return vectorIndexFileSnapshot{}, fmt.Errorf("vector index file: snapshot: %w", err)
	}
	return vectorIndexFileSnapshot{path: path, data: data, exists: true}, nil
}

func restoreVectorIndexFile(snapshot vectorIndexFileSnapshot) error {
	if snapshot.path == "" {
		return nil
	}
	if snapshot.exists {
		return atomicWriteFile(snapshot.path, snapshot.data, "vector index rollback")
	}
	if err := os.Remove(snapshot.path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("vector index rollback: remove: %w", err)
	}
	if err := syncParentDir(snapshot.path, "vector index rollback"); err != nil {
		return err
	}
	return nil
}

func loadVectorIndexFile(path string) ([]vectorIdxDef, error) {
	data, err := os.ReadFile(path) // #nosec G304 — path derived from caller-provided Config.DataDir (trusted config, not end-user input)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("vector index file: read: %w", err)
	}
	var defs []vectorIdxDef
	if err := msgpack.Unmarshal(data, &defs); err != nil {
		return nil, fmt.Errorf("vector index file: unmarshal: %w", err)
	}
	if err := validateVectorIndexFileDefs(defs); err != nil {
		return nil, err
	}
	return defs, nil
}

func (ts *Store) loadVectorIndexDefs() error {
	defs, err := loadVectorIndexFile(ts.vectorIdxFile)
	if err != nil || len(defs) == 0 {
		return err
	}

	validDefs := make([]vectorIdxDef, 0, len(defs))
	seenDefs := make(map[indexpkg.VectorIndexKey]vectorIdxDef, len(defs))
	for _, def := range defs {
		if err := storecontract.ValidateLabelToken(def.LabelToken); err != nil {
			return fmt.Errorf("vector index file: invalid definition label %d property %q: %w",
				def.LabelToken, def.PropertyKey, err)
		}
		if err := storecontract.ValidateIndexPropertyKey(def.PropertyKey); err != nil {
			return fmt.Errorf("vector index file: invalid definition label %d property %q: %w",
				def.LabelToken, def.PropertyKey, err)
		}
		if err := indexpkg.ValidateVectorIndexConfig(def.Dims, def.Metric); err != nil {
			return fmt.Errorf("vector index file: invalid definition label %d property %q: %w",
				def.LabelToken, def.PropertyKey, err)
		}
		key := indexpkg.VectorIndexKey{LabelToken: def.LabelToken, PropertyKey: def.PropertyKey}
		if existing, exists := seenDefs[key]; exists {
			if existing.Dims != def.Dims || existing.Metric != def.Metric {
				return fmt.Errorf("vector index file: label %d property %q has conflicting definitions: %w",
					def.LabelToken, def.PropertyKey, ErrVectorIndexExists)
			}
			continue
		}
		seenDefs[key] = def
		validDefs = append(validDefs, def)
	}

	ids := make([]types.NodeID, 0)
	if err := ts.ForEachNodeID(func(id types.NodeID) bool {
		ids = append(ids, id)
		return true
	}); err != nil {
		return err
	}

	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	for _, def := range validDefs {
		key := indexpkg.VectorIndexKey{LabelToken: def.LabelToken, PropertyKey: def.PropertyKey}
		vi := &indexpkg.VectorIndex{Dims: def.Dims, Metric: def.Metric}
		for _, id := range ids {
			n, getErr := ts.GetNode(id)
			if getErr != nil {
				if errors.Is(getErr, ErrNodeNotFound) {
					continue
				}
				return fmt.Errorf("vector index file: rebuild node %d label %d property %q: %w",
					id.SnowflakeID(), def.LabelToken, def.PropertyKey, getErr)
			}
			vec, ok := vectorForDefinition(n, def)
			if !ok {
				continue
			}
			if addErr := vi.AddOwned(id.SnowflakeID(), vec); addErr != nil {
				return fmt.Errorf("vector index file: rebuild node %d label %d property %q: %w",
					id.SnowflakeID(), def.LabelToken, def.PropertyKey, addErr)
			}
		}
		ts.vectorIndexes[key] = vi
	}
	return nil
}

func vectorForDefinition(n *types.Node, def vectorIdxDef) ([]float32, bool) {
	if !n.HasLabelTokenRaw(def.LabelToken) {
		return nil, false
	}
	return n.Float32SlicePropertyCopy(def.PropertyKey)
}

// persistVectorIndexDefsLocked writes the current store-level vector index
// definitions. Caller must hold ts.vectorIdxMu.
func (ts *Store) persistVectorIndexDefsLocked() error {
	if ts.inMemory {
		return nil
	}
	defs := make([]vectorIdxDef, 0, len(ts.vectorIndexes))
	for key, idx := range ts.vectorIndexes {
		if idx == nil || idx.Mutated != nil {
			continue
		}
		defs = append(defs, vectorIdxDef{
			LabelToken:  key.LabelToken,
			PropertyKey: key.PropertyKey,
			Dims:        idx.Dims,
			Metric:      idx.Metric,
		})
	}
	return saveVectorIndexFile(ts.vectorIdxFile, defs)
}
