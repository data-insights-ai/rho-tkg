package index

import "sync/atomic"

var windowMemberBuildCounter atomic.Pointer[atomic.Int64]

func noteWindowMemberBuilt() {
	if c := windowMemberBuildCounter.Load(); c != nil {
		c.Add(1)
	}
}

// CountWindowMembersBuiltForTest counts entries actually copied into first
// window views, independently of their read meter. These tests run alone.
func CountWindowMembersBuiltForTest() (visited func() int64, stop func()) {
	c := new(atomic.Int64)
	windowMemberBuildCounter.Store(c)
	return c.Load, func() { windowMemberBuildCounter.Store(nil) }
}
