package raftlog

import "testing"

// Control and history work counters are the one ReadBudget {Rows, Bytes}
// type, not twins (compile-time check).
func TestReadWorkIsReadBudget(t *testing.T) {
	var v *ApplicationView
	_ = func() {
		_, _, w, _ := v.GetControl(nil, nil, ReadBudget{})
		var _ ReadBudget = w
		_, hw, _ := v.ApplicationRecordBounded(nil, 0, false, 0, ReadBudget{})
		var _ ReadBudget = hw
	}
}
