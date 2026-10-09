package sharded

import (
	"errors"
	"fmt"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// DurableFlushSupported implements store.DurableFlushCapability: false for an
// InMemory sharded store (its slots have no write-ahead log).
func (s *Store) DurableFlushSupported() bool {
	return s != nil && !s.inMemory
}

// DurableFlush implements store.DurableFlushCapability (Config.DurableCommit):
// BadgerStore.DurableFlush on every slot (all slots are always open). A slot
// with nothing written since its last sync is not fsynced again. Every slot is
// attempted; the errors are joined, and a failed slot keeps its pending
// operations for the next flush.
func (s *Store) DurableFlush() error {
	if s == nil {
		return ErrNilStore
	}
	if s.inMemory {
		return fmt.Errorf("graph: sharded: durable flush of an in-memory store: %w", storecontract.ErrCapabilityNotSupported)
	}
	// Hold s.mu (read) for the whole fold: Close takes it exclusively before
	// closing the shards, so no shard is closed under a flush.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrStoreClosed
	}
	var errs []error
	for i, shard := range s.shards {
		if err := shard.DurableFlush(); err != nil {
			errs = append(errs, fmt.Errorf("graph: sharded: durable flush shard %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}
