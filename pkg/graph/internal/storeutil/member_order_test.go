package storeutil

import (
	"slices"
	"testing"
)

func TestMemberOrder(t *testing.T) {
	var o MemberOrder[int64]
	if _, gen, ok := o.Ordered(1, 0); ok || gen != 0 {
		t.Fatal("an unbuilt list must not be served")
	}
	o.Add(1, 30)
	o.Add(1, 10)
	o.Add(1, 20)
	_, gen, ok := o.Ordered(1, 3)
	if ok {
		t.Fatal("no list was installed")
	}
	members := []int64{30, 10, 20}
	SortMembers(members)
	o.Install(1, gen, members)
	ids, _, ok := o.Ordered(1, 3)
	if !ok || !slices.Equal(ids, []int64{10, 20, 30}) {
		t.Fatalf("installed list = %v, %v", ids, ok)
	}

	// An insert above the last member appends; the old header is unchanged.
	o.Add(1, 40)
	got, _, ok := o.Ordered(1, 4)
	if !ok || !slices.Equal(got, []int64{10, 20, 30, 40}) || !slices.Equal(ids, []int64{10, 20, 30}) {
		t.Fatalf("after append: %v (old header %v)", got, ids)
	}

	// An older ID drops the list.
	o.Add(1, 15)
	if _, _, ok := o.Ordered(1, 5); ok {
		t.Fatal("an out-of-order insert must drop the list")
	}

	// A list collected at an older generation is not installed.
	_, gen, _ = o.Ordered(1, 5)
	o.Add(1, 50)
	o.Install(1, gen, []int64{10, 15, 20, 30, 40})
	if _, _, ok := o.Ordered(1, 6); ok {
		t.Fatal("a list collected before an insert must not be installed")
	}

	// The count check: a set whose size differs from the live entries is not
	// trusted (a change that bypassed Add / Remove).
	_, gen, _ = o.Ordered(1, 3)
	o.Install(1, gen, []int64{1, 2, 3})
	if _, _, ok := o.Ordered(1, 2); ok {
		t.Fatal("a list with more live entries than the set has members must be dropped")
	}

	// Remove counts stale entries; the list is dropped past half stale (and
	// more than 64).
	_, gen, _ = o.Ordered(2, 0)
	big := make([]int64, 200)
	for i := range big {
		big[i] = int64(i + 1)
	}
	o.Install(2, gen, big)
	for i := 0; i < 100; i++ {
		o.Remove(2)
	}
	if _, _, ok := o.Ordered(2, 100); !ok {
		t.Fatal("100 of 200 stale must still be served")
	}
	o.Remove(2)
	if _, _, ok := o.Ordered(2, 99); ok {
		t.Fatal("101 of 200 stale must drop the list")
	}
	o.Remove(3) // no list: no-op

	o.Reset()
	if _, gen, ok := o.Ordered(1, 0); ok || gen != 0 {
		t.Fatal("Reset must drop lists and generations")
	}
	o.Add(4, 1) // Add after Reset initializes again
	if _, gen, _ := o.Ordered(4, 1); gen != 1 {
		t.Fatalf("gen after one Add = %d", gen)
	}
}
