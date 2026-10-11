package graphapply

import "testing"

// The capability RED originally exercised the accepted local door and proved
// the missing same-Graph cross-edge path. Its breaking input now enters the
// actual Host prepare/decision/resolve door; local remote-refusal guards remain.
func TestCrossPartitionGraphPrepareResolveMissingCompleteEndpointEffects(t *testing.T) {
	testCrossPartitionHostTypedCrossEdgeCommitsBothEndpointPostings(t)
}
