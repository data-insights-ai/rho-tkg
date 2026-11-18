//go:build !measure

package badger

// asOfIndexOff is the as-of index's A/B switch of the measure build
// (badgerstore_asof_index_switch_measure.go). Here it is a false constant, so
// the branch in noteAsOfHistoryLocked and the empty counters compile away.
const asOfIndexOff = false

func noteAsOfHookTxn()  {}
func noteAsOfHookGet()  {}
func noteAsOfHookSeek() {}
