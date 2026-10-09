package raftlog

// ApplicationIdentity returns the immutable durable transfer identity by value.
// Nil stores and stores without transfer identity return zero. Like
// ApplicationLimits, it may report configuration after close; it does not check
// store validity or grant a lease, generation fence, coverage or activation.
func (s *Store) ApplicationIdentity() ApplicationIdentity {
	if s == nil {
		return ApplicationIdentity{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta.Transfer.Identity
}
