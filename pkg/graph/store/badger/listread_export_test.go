package badger

// NodeRowPointReadsForTest reports the node rows read by a point read of
// their own (LISTREAD).
func (bs *Store) NodeRowPointReadsForTest() int64 { return bs.nodeRowPointReads.Load() }
