package store_test

import (
	"errors"
	"fmt"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 69: the paged form of the property membership sidecars
// (RelPropertyTxMemberPageCapability / NodePropertyTxMemberPageCapability) on
// every backend that offers it. Faulty implementations caught: an unordered
// or cursor-ignoring page, a limit not honoured, a member recorded after the
// first page missing (the ordered view not maintained), another value's
// members, an early stop ignored, an undeclared index not refused with
// ErrIndexNotFound, a nil callback accepted.

type propTxPageCaps interface {
	storecontract.Store
	storecontract.RelPropertyIndexCapability
	storecontract.RelPropertyTxMemberPageCapability
	storecontract.NodePropertyTxMemberPageCapability
}

func TestPropertyTxMemberPage(t *testing.T) {
	t.Parallel()
	const typ, label = uint16(3), uint16(4)
	one, two := types.IndexablePropertyValueKey(int64(1)), types.IndexablePropertyValueKey(int64(2))
	for _, be := range propTxBackends() {
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			st := be.open(t)
			defer st.Close()
			c, ok := st.(propTxPageCaps)
			if !ok {
				t.Fatalf("%T does not implement the paged membership capabilities", st)
			}
			for _, id := range []types.NodeID{1, 2} {
				if err := c.PutNode(types.NewNode(id, 9, nil)); err != nil {
					t.Fatal(err)
				}
			}
			put := func(id int64, v int64, tx types.Instant) {
				r := types.NewRelationship(types.RelID(id), typ, 1, 2)
				_ = r.SetProperty("seat", v)
				r.SetTemporal(&types.TemporalMetadata{TxFrom: tx})
				if err := c.PutRelationship(r); err != nil {
					t.Fatal(err)
				}
				n := types.NewNode(types.NodeID(id), label, nil)
				_ = n.SetProperty("seat", v)
				n.SetTemporal(&types.TemporalMetadata{TxFrom: tx})
				if err := c.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			page := func(rel bool, vk string, after int64, limit int) (string, error) {
				var out []string
				var err error
				if rel {
					err = c.ForEachRelPropertyTxMemberAfter(typ, "seat", vk, types.RelID(after), limit, func(id types.RelID, tx types.Instant) bool {
						out = append(out, fmt.Sprintf("%d:%d", id, tx))
						return true
					})
				} else {
					err = c.ForEachNodePropertyTxMemberAfter(label, "seat", vk, types.NodeID(after), limit, func(id types.NodeID, tx types.Instant) bool {
						out = append(out, fmt.Sprintf("%d:%d", id, tx))
						return true
					})
				}
				return fmt.Sprint(out), err
			}
			put(10, 1, 100)
			for _, rel := range []bool{true, false} {
				if _, err := page(rel, one, 0, 5); !errors.Is(err, storecontract.ErrIndexNotFound) {
					t.Fatalf("rel=%v undeclared: %v, want ErrIndexNotFound", rel, err)
				}
			}
			if err := c.CreateRelPropertyIndex(typ, "seat"); err != nil {
				t.Fatal(err)
			}
			if err := c.CreatePropertyIndex(label, "seat"); err != nil {
				t.Fatal(err)
			}
			put(40, 1, 400)
			put(30, 1, 300)
			put(20, 2, 200)
			for _, rel := range []bool{true, false} {
				check := func(vk string, after int64, limit int, want string) {
					t.Helper()
					got, err := page(rel, vk, after, limit)
					if err != nil || got != want {
						t.Fatalf("rel=%v %q after %d limit %d = %s, %v; want %s", rel, vk, after, limit, got, err, want)
					}
				}
				check(one, 0, 2, "[10:100 30:300]")
				check(one, 10, 5, "[30:300 40:400]")
				check(one, 40, 5, "[]")
				check(two, 0, 5, "[20:200]")
				check("absent", 0, 5, "[]")
				check(one, 0, 0, "[]")
			}
			put(25, 1, 250) // below the last member: the open view keeps it
			put(50, 1, 500)
			for _, rel := range []bool{true, false} {
				got, err := page(rel, one, 10, 3)
				if want := "[25:250 30:300 40:400]"; err != nil || got != want {
					t.Fatalf("rel=%v after writes = %s, %v; want %s", rel, got, err, want)
				}
				calls := 0
				_ = c.ForEachRelPropertyTxMemberAfter(typ, "seat", one, 0, 10, func(types.RelID, types.Instant) bool { calls++; return false })
				_ = c.ForEachNodePropertyTxMemberAfter(label, "seat", one, 0, 10, func(types.NodeID, types.Instant) bool { calls++; return false })
				if calls != 2 {
					t.Fatalf("early stop: %d calls, want 2", calls)
				}
			}
			if err := c.ForEachRelPropertyTxMemberAfter(typ, "seat", one, 0, 1, nil); err == nil {
				t.Fatal("nil rel callback accepted")
			}
			if err := c.ForEachNodePropertyTxMemberAfter(label, "seat", one, 0, 1, nil); err == nil {
				t.Fatal("nil node callback accepted")
			}
			keep := func(types.RelID, types.Instant) bool { return true }
			keepN := func(types.NodeID, types.Instant) bool { return true }
			for name, err := range map[string]error{
				"rel token 0":      c.ForEachRelPropertyTxMemberAfter(0, "seat", one, 0, 1, keep),
				"node token 0":     c.ForEachNodePropertyTxMemberAfter(0, "seat", one, 0, 1, keepN),
				"rel shadow key":   c.ForEachRelPropertyTxMemberAfter(typ, "tkg_valid_from", one, 0, 1, keep),
				"node shadow key":  c.ForEachNodePropertyTxMemberAfter(label, "tkg_valid_from", one, 0, 1, keepN),
				"rel other key":    c.ForEachRelPropertyTxMemberAfter(typ, "other", one, 0, 1, keep),
				"node other label": c.ForEachNodePropertyTxMemberAfter(label+1, "seat", one, 0, 1, keepN),
			} {
				if err == nil {
					t.Fatalf("%s accepted", name)
				}
			}
			_ = c.Close()
			if err := c.ForEachRelPropertyTxMemberAfter(typ, "seat", one, 0, 1, keep); err == nil {
				t.Fatal("closed store answered a rel page")
			}
			if err := c.ForEachNodePropertyTxMemberAfter(label, "seat", one, 0, 1, keepN); err == nil {
				t.Fatal("closed store answered a node page")
			}
		})
	}
}
