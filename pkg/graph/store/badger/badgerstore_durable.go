package badger

// DurableFlushSupported implements store.DurableFlushCapability.
// RED STUB: replaced by the implementation commit.
func (bs *Store) DurableFlushSupported() bool { return true }

// DurableFlush implements store.DurableFlushCapability.
// RED STUB: replaced by the implementation commit.
func (bs *Store) DurableFlush() error { return nil }
