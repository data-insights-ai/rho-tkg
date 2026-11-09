package badger

import (
	"bytes"
	"slices"
	"testing"

	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
)

// TestPropPrefixLookupIteratesTokenRangeOnly (PROPPREFIX): an equality
// lookup on the on-disk property index restricts its iterator to the
// property token's key range, so badger skips every table outside it (node
// rows, other keyspaces), and its answers are unchanged.
func TestPropPrefixLookupIteratesTokenRangeOnly(t *testing.T) {
	bs := propSeekStore(t, 2000)
	tok, ok := bs.propKeyTokenFor("k")
	if !ok {
		t.Fatal("no token for k")
	}
	want := []byte{storepkg.KeyPropertyIndex, byte(tok >> 8), byte(tok)}
	var seen [][]byte
	propertyDiskIterOptsTestHook = func(o badgerv4.IteratorOptions) { seen = append(seen, o.Prefix) }
	t.Cleanup(func() { propertyDiskIterOptsTestHook = nil })
	keys := []string{indexpkg.PropertyValueKey("b000007"), indexpkg.PropertyValueKey("a000001"), indexpkg.PropertyValueKey("b001999")}
	ids, _, err := bs.NodeIDsByLabelAndPropertyKeys(1, "k", keys, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(ids)
	if len(ids) != 2 || int64(ids[0]) != existMapBase+7 || int64(ids[1]) != existMapBase+1999 {
		t.Fatalf("ids %v, want the nodes of b000007 and b001999", ids)
	}
	if len(seen) == 0 {
		t.Fatal("the lookup made no iterator")
	}
	for _, p := range seen {
		if !bytes.Equal(p, want) {
			t.Fatalf("iterator prefix %x, want the token range %x", p, want)
		}
	}
}
