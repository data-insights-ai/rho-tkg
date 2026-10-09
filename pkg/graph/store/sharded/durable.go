package sharded

// DurableFlushSupported implements store.DurableFlushCapability.
// RED STUB: replaced by the implementation commit.
func (s *Store) DurableFlushSupported() bool { return true }

// DurableFlush implements store.DurableFlushCapability.
// RED STUB: replaced by the implementation commit.
func (s *Store) DurableFlush() error { return nil }
