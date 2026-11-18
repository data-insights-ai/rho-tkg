//go:build measure

package badger

import "sync/atomic"

// asOfIndexOff turns the as-of index's write hook off in the measure build
// only, so one binary measures history appends with and without the index on
// one disk (badgerstore_asof_index_cost_measure_test.go). No consumer build
// has it: production compiles badgerstore_asof_index_switch.go.
var asOfIndexOff bool

// The hook's own native work: read transactions it opens, its point reads
// that leave both write buffers, and its first-row iterator seeks.
var asOfHookTxns, asOfHookGets, asOfHookSeeks atomic.Int64

func noteAsOfHookTxn()  { asOfHookTxns.Add(1) }
func noteAsOfHookGet()  { asOfHookGets.Add(1) }
func noteAsOfHookSeek() { asOfHookSeeks.Add(1) }
