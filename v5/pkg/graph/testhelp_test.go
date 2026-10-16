package graph

// testCut and testReceipt build valid opaque tokens; only the future engine
// mints real ones, so tests use the package-private constructors.
func testCut() Cut         { return Cut{tok: []byte{1}} }
func testReceipt() Receipt { return Receipt{tok: []byte{1}} }
