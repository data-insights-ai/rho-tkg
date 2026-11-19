//go:build measure

package badger

// asOfIndexOff turns the as-of index's write hook off in the measure build
// only, so one binary measures history appends with and without the index on
// one disk (badgerstore_asof_index_cost_measure_test.go). No consumer build
// has it: production compiles badgerstore_asof_index_switch.go.
var asOfIndexOff bool
