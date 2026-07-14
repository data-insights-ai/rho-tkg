package sharded

import (
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
)

// --- MetaKV + registry seams (anchor-shard scoped) ---
//
// The anchor shard (slot base) owns store-global metadata: graph-layer meta
// markers, the label/reltype registries, and the property-key registry. These
// delegate to the anchor. SetPropertyKeyRegistry installs the shared registry on
// EVERY shard (the wire encoders/decoders dictionary-encode property keys with
// it), mirroring tiered.Store.

func (s *Store) MetaGet(key string) ([]byte, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	return s.anchor().MetaGet(key)
}

func (s *Store) MetaSet(key string, value []byte) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.anchor().MetaSet(key, value)
}

func (s *Store) SaveRegistries(labelReg *registrypkg.LabelRegistry, relTypeReg *registrypkg.RelTypeRegistry) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.anchor().SaveRegistries(labelReg, relTypeReg)
}

func (s *Store) LoadLabelRegistry(reg *registrypkg.LabelRegistry) (bool, error) {
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	return s.anchor().LoadLabelRegistry(reg)
}

func (s *Store) LoadRelTypeRegistry(reg *registrypkg.RelTypeRegistry) (bool, error) {
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	return s.anchor().LoadRelTypeRegistry(reg)
}

func (s *Store) SavePropertyKeyRegistry(reg *registrypkg.PropertyKeyRegistry) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	return s.anchor().SavePropertyKeyRegistry(reg)
}

func (s *Store) LoadPropertyKeyRegistry(reg *registrypkg.PropertyKeyRegistry) (bool, error) {
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	return s.anchor().LoadPropertyKeyRegistry(reg)
}

// SetPropertyKeyRegistry installs reg as the canonical property-key registry on
// every shard so property-key tokenization is uniform across the store.
func (s *Store) SetPropertyKeyRegistry(reg *registrypkg.PropertyKeyRegistry) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.propKeyReg = reg
	shards := s.shards
	s.mu.Unlock()
	for _, shard := range shards {
		if shard != nil {
			shard.SetPropertyKeyRegistry(reg)
		}
	}
}
