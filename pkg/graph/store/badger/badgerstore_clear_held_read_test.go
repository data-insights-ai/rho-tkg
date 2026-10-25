package badger

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestClearUnderHeldReadKeepsTheValue pins DROPALL: a read that found the key
// of a 1.4 MB row (its value in the value log) before Clear still reads the
// row's bytes after Clear, on a store that never had the change log. badger's
// DropAll deleted the value-log files under it, and badger returns an empty
// value for a value-log read it cannot do (iterator.go:187-188), which rho
// decoded as "corrupt wire data". After Clear the row is gone for new reads.
func TestClearUnderHeldReadKeepsTheValue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	id := types.NodeID(snowflake.ID(77))
	n := types.NewNode(id, 7, nil)
	for i := 0; i < 24; i++ {
		_ = n.SetProperty(fmt.Sprintf("p%02d", i), strings.Repeat("v", 60_000))
	}
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	txn := bs.db.NewTransaction(false)
	item, err := txn.Get(storepkg.NodeKey(snowflake.ID(id)))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	before, err := item.ValueCopy(nil)
	if err != nil || len(before) < 1_000_000 {
		t.Fatalf("value read before Clear: %d bytes (%v)", len(before), err)
	}
	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	val, err := item.ValueCopy(nil)
	txn.Discard()
	if err != nil || !bytes.Equal(val, before) {
		t.Fatalf("value read after Clear: %d of %d bytes (%v)", len(val), len(before), err)
	}
	if _, err := bs.GetNode(id); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after Clear = %v, want ErrNodeNotFound", err)
	}
}
