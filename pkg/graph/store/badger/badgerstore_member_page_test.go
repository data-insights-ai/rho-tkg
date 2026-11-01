package badger

import (
	"fmt"
	"slices"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// memberPageStore holds n relationships on value "v" with a history row each
// (two member entries per ID), every third on "w", flushed.
func memberPageStore(t *testing.T, n int64) (*Store, uint16) {
	t.Helper()
	relTypes := registrypkg.NewRelTypeRegistry()
	r, _ := relTypes.GetOrCreate("R")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: relTypes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	for _, id := range []types.NodeID{1, 2} {
		if err := bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.CreateRelPropertyIndex(r, "k"); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= n; id++ {
		if err := bs.PutRelationship(memberRel(id, r, memberValue(id))); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutRelVersion(types.RelID(id), 0, memberRel(id, r, memberValue(id))); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	return bs, r
}

func memberValue(id int64) string {
	if id%3 == 0 {
		return "w"
	}
	return "v"
}

func memberRel(id int64, r uint16, v string) *types.Relationship {
	x := types.NewRelationship(types.RelID(id), r, types.NodeID(1), types.NodeID(2))
	_ = x.SetProperty("k", v)
	return x
}

func memberPage(t *testing.T, bs *Store, r uint16, vk string, after types.RelID, limit int) []types.RelID {
	t.Helper()
	var ids []types.RelID
	if err := bs.ForEachRelPropertyTxMemberAfter(r, "k", vk, after, limit, func(id types.RelID, _ types.Instant) bool {
		ids = append(ids, id)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return ids
}

// A page costs about the page: the steps of its walk follow limit, not the
// value's members, wherever the cursor stands.
func TestRelMemberPageWalkFollowsTheLimit(t *testing.T) {
	t.Parallel()
	bs, r := memberPageStore(t, 3000)
	vk := types.IndexablePropertyValueKey("v")
	steps := 0
	bs.entityIndexWalkTestHook = func() { steps++ }
	const limit = 10
	for _, after := range []types.RelID{0, 1500, 2900} {
		steps = 0
		if got := memberPage(t, bs, r, vk, after, limit); len(got) != limit {
			t.Fatalf("after %d: %d members, want %d", after, len(got), limit)
		}
		if steps > 3*limit+3 {
			t.Errorf("after %d: page of %d walked %d steps of a value with 2000 members", after, limit, steps)
		}
	}
}

// Every cursor and limit answers what the whole walk answers, with the write
// buffer resolved: a buffered new member, a moved one and a new version.
func TestRelMemberPageEqualsTheWholeWalk(t *testing.T) {
	t.Parallel()
	bs, r := memberPageStore(t, 40)
	// Buffered, not flushed: 41 new on v, 5 moved to w, 7 re-written on v, 10
	// purged.
	if err := bs.PurgeRelationshipByInfo(storecontract.PurgedRel{ID: 10, TypeToken: r, StartID: 1, EndID: 2}); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelationship(memberRel(41, r, "v")); err != nil {
		t.Fatal(err)
	}
	for _, x := range []*types.Relationship{memberRel(5, r, "w"), memberRel(7, r, "v")} {
		if err := bs.ReplaceRelationship(x); err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range []string{"v", "w", "absent"} {
		vk := types.IndexablePropertyValueKey(v)
		var whole []string
		if err := bs.ForEachRelPropertyTxMember(r, "k", vk, func(id types.RelID, tx types.Instant) bool {
			whole = append(whole, fmt.Sprint(id, "@", tx))
			return true
		}); err != nil {
			t.Fatal(err)
		}
		for _, limit := range []int{1, 2, 3, 7, 100} {
			var got []string
			after := types.RelID(0)
			for {
				n := 0
				if err := bs.ForEachRelPropertyTxMemberAfter(r, "k", vk, after, limit, func(id types.RelID, tx types.Instant) bool {
					got = append(got, fmt.Sprint(id, "@", tx))
					after = id
					n++
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if n < limit {
					break
				}
			}
			a, b := slices.Clone(got), slices.Clone(whole)
			slices.Sort(a)
			slices.Sort(b)
			if !slices.Equal(a, b) {
				t.Fatalf("value %s limit %d: paged %v, whole %v", v, limit, got, whole)
			}
			if v == "v" && slices.Contains(a, "10@0") {
				t.Fatalf("purged 10 still a member: %v", a)
			}
		}
	}
}

// The node door walks the same way.
func TestNodeMemberPageWalkFollowsTheLimit(t *testing.T) {
	t.Parallel()
	labels := registrypkg.NewLabelRegistry()
	d, _ := labels.GetOrCreate("D")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		PropertyIndexOnDiskLabels: []string{"D"}, LabelNames: labels})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.CreatePropertyIndex(d, "k"); err != nil {
		t.Fatal(err)
	}
	for id := types.NodeID(1); id <= 1000; id++ {
		n := types.NewNode(id, d, nil)
		_ = n.SetProperty("k", "v")
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	steps := 0
	bs.entityIndexWalkTestHook = func() { steps++ }
	const limit = 10
	var got []types.NodeID
	if err := bs.ForEachNodePropertyTxMemberAfter(d, "k", types.IndexablePropertyValueKey("v"), 500, limit, func(id types.NodeID, _ types.Instant) bool {
		got = append(got, id)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != limit || got[0] != 501 {
		t.Fatalf("page %v, want %d from 501", got, limit)
	}
	if steps > 2*limit+2 {
		t.Errorf("page of %d walked %d steps of a value with 1000 members", limit, steps)
	}
}
