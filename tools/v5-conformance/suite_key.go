package conformance

import (
	"sync"
	"testing"
)

func init() {
	register("GetOrCreateByKey",
		both("CreatesThenReturnsTheSameIdentity", keyIdempotent),
		both("DistinctValuesAndLabelsAreDistinctIdentities", keyDistinct),
		both("ConcurrentCallsCreateOnce", keyConcurrent),
		both("InvalidInputsCreateNothing", keyInvalid),
		both("DeletedIdentityIsMintedAgain", keyAfterDelete),
		both("IdentitySurvivesReopen", keyReopen),
	)
}

// keyIdempotent: the second call is a hit that returns the same node, reports
// created=false, applies no extra properties and appends no version.
func keyIdempotent(t *testing.T, a Adapter) {
	g := open(t, a, true)
	first, created, err := g.GetOrCreateByKey(ctx, "U", "id", "x1", map[string]any{"z": 1})
	ok(t, err)
	if !created || first.ID == 0 {
		t.Fatalf("first call: created=%v id=%d, want created with an ID", created, first.ID)
	}
	wantProps(t, "first props", first.Props, map[string]any{"id": "x1", "z": 1})
	for i := 0; i < 3; i++ {
		again, created, err := g.GetOrCreateByKey(ctx, "U", "id", "x1", map[string]any{"z": 2, "extra": true})
		ok(t, err)
		if created || again.ID != first.ID {
			t.Fatalf("hit %d: created=%v id=%d, want the existing node %d", i, created, again.ID, first.ID)
		}
		wantProps(t, "hit props", again.Props, map[string]any{"id": "x1", "z": 1})
	}
	cur := mustGet(t, g, first.ID)
	if cur.Version != 0 {
		t.Fatalf("hits appended versions: version %d", cur.Version)
	}
	h, err := g.NodeHistory(first.ID)
	ok(t, err)
	if len(h) != 0 {
		t.Fatalf("hits appended history: %+v", h)
	}
	ns, err := g.NodesByLabel("U", Query{})
	ok(t, err)
	if len(ns) != 1 {
		t.Fatalf("%d U nodes, want 1", len(ns))
	}
}

func keyDistinct(t *testing.T, a Adapter) {
	g := open(t, a, true)
	x, _, err := g.GetOrCreateByKey(ctx, "U", "id", "x", nil)
	ok(t, err)
	y, created, err := g.GetOrCreateByKey(ctx, "U", "id", "y", nil)
	ok(t, err)
	if !created || y.ID == x.ID {
		t.Fatalf("a different value must mint a different node")
	}
	w, created, err := g.GetOrCreateByKey(ctx, "W", "id", "x", nil)
	ok(t, err)
	if !created || w.ID == x.ID {
		t.Fatalf("the same value under another label must mint a different node")
	}
	other, created, err := g.GetOrCreateByKey(ctx, "U", "other", "x", nil)
	ok(t, err)
	if !created || other.ID == x.ID {
		t.Fatalf("the same value under another key must mint a different node")
	}
	// A string "1" and an integer 1 are different keys.
	s, _, err := g.GetOrCreateByKey(ctx, "N", "k", "1", nil)
	ok(t, err)
	i, created, err := g.GetOrCreateByKey(ctx, "N", "k", 1, nil)
	ok(t, err)
	if !created || i.ID == s.ID {
		t.Fatalf("integer 1 and string \"1\" must not share an identity")
	}
}

// keyConcurrent: 32 callers race for one key; exactly one creates.
func keyConcurrent(t *testing.T, a Adapter) {
	g := open(t, a, true)
	const callers = 32
	got := make([]int64, callers)
	created := make([]bool, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			n, c, err := g.GetOrCreateByKey(ctx, "C", "id", "same", nil)
			got[i], created[i], errs[i] = n.ID, c, err
		}(i)
	}
	close(start)
	wg.Wait()
	creates := 0
	for i := range got {
		ok(t, errs[i])
		if created[i] {
			creates++
		}
		if got[i] != got[0] || got[i] == 0 {
			t.Fatalf("caller %d got node %d, caller 0 got %d", i, got[i], got[0])
		}
	}
	if creates != 1 {
		t.Fatalf("%d callers created the node, want exactly 1", creates)
	}
	ns, err := g.NodesByLabel("C", Query{})
	ok(t, err)
	if len(ns) != 1 {
		t.Fatalf("%d C nodes after the race, want 1", len(ns))
	}
}

func keyInvalid(t *testing.T, a Adapter) {
	g := open(t, a, true)
	for _, c := range []struct {
		name       string
		label, key string
		value      any
	}{
		{"empty label", "", "id", "x"},
		{"empty key", "U", "", "x"},
		{"nil value", "U", "id", nil},
		{"list value", "U", "id", []int{1}},
		{"map value", "U", "id", map[string]any{"a": 1}},
	} {
		n, created, err := g.GetOrCreateByKey(ctx, c.label, c.key, c.value, nil)
		if err == nil {
			t.Fatalf("%s: accepted (node %d created=%v)", c.name, n.ID, created)
		}
		is(t, c.name, err, ErrInvalidArgument)
		if created || n.ID != 0 {
			t.Fatalf("%s: refused call returned created=%v id=%d", c.name, created, n.ID)
		}
	}
	for _, label := range []string{"U"} {
		ns, err := g.NodesByLabel(label, Query{})
		ok(t, err)
		if len(ns) != 0 {
			t.Fatalf("refused calls created %d node(s)", len(ns))
		}
	}
}

// keyAfterDelete: once the identity's node is deleted, the key mints a new node
// and the old one stays in history.
func keyAfterDelete(t *testing.T, a Adapter) {
	g := open(t, a, true)
	first, _, err := g.GetOrCreateByKey(ctx, "U", "id", "k", map[string]any{"gen": 1})
	ok(t, err)
	pin, err := g.CommittedTx()
	ok(t, err)
	ok(t, g.DeleteNode(ctx, first.ID, 0))
	second, created, err := g.GetOrCreateByKey(ctx, "U", "id", "k", map[string]any{"gen": 2})
	ok(t, err)
	if !created || second.ID == first.ID {
		t.Fatalf("after delete: created=%v id=%d (old %d), want a new node", created, second.ID, first.ID)
	}
	wantProps(t, "new node", second.Props, map[string]any{"id": "k", "gen": 2})
	// The old life is still readable at the earlier pin.
	wantNodeAsOf(t, g, first.ID, pin, map[string]any{"id": "k", "gen": 1}, 0)
	ns, err := g.NodesByLabelAndProperty("U", "id", "k", Query{})
	ok(t, err)
	wantIDs(t, "current holders of the key", ids(ns), []int64{second.ID})
}

func keyReopen(t *testing.T, a Adapter) {
	g, _ := openDir(t, a)
	first, created, err := g.GetOrCreateByKey(ctx, "U", "id", "k", nil)
	ok(t, err)
	if !created {
		t.Fatalf("first call did not create")
	}
	ok(t, g.Reopen())
	again, created, err := g.GetOrCreateByKey(ctx, "U", "id", "k", nil)
	ok(t, err)
	if created || again.ID != first.ID {
		t.Fatalf("after reopen: created=%v id=%d, want the existing node %d", created, again.ID, first.ID)
	}
}
