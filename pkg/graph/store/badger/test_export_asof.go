package badger

// Test seams, no promise: the as-of index's read modes, for tests that
// compare as-of answers with the index used, declined and invalidated.

// SetAsOfIndexDeclinedForTest makes every as-of read decline the index and
// take the admitted scan (as a read-only handle after an unclean close does),
// or use it again.
func (bs *Store) SetAsOfIndexDeclinedForTest(declined bool) {
	bs.wbMu.Lock()
	bs.asOfUntrusted = declined
	bs.wbMu.Unlock()
}

// InvalidateAsOfGenerationForTest moves the recovery generation as an
// unclean open does: every proof written before it stops certifying. As
// after that open, the write hook reads the new generation.
func (bs *Store) InvalidateAsOfGenerationForTest() error {
	if err := bs.update(invalidateAsOfGeneration); err != nil {
		return err
	}
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()
	bs.refreshAsOfSnap(true)
	return nil
}

// SetAsOfViewPinHookForTest installs fn on every indexed as-of read of this
// store: before the read's lock section (false) and after it pinned the
// committed view (true), with no lock held. nil removes it.
func (bs *Store) SetAsOfViewPinHookForTest(fn func(pinned bool)) { bs.asOfViewPinTestHook = fn }
