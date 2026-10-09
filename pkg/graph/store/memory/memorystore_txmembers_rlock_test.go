package memory

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A lookup on a BUILT membership sidecar (property: backlog 8; K1 label and
// rel type) is a read: it must take ms.mu shared, so concurrent lookups and
// other readers do not serialize on it. Faulty implementation caught: taking
// ms.mu.Lock on every call. The test holds a read lock (as a concurrent reader
// would) and requires each lookup to complete while it is held; with the
// write lock the lookup waits for the reader and the test fails at the
// deadline instead of hanging.
func TestBuiltMembershipLookupsTakeTheReadLock(t *testing.T) {
	ms := New()
	defer ms.Close()
	const typ, label = uint16(3), uint16(4)
	for _, n := range []*types.Node{types.NewNode(1, label, nil), types.NewNode(2, label, nil)} {
		_ = n.SetProperty("seat", int64(1))
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(10, typ, 1, 2)
	_ = r.SetProperty("seat", int64(1))
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateRelPropertyIndex(typ, "seat"); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreatePropertyIndex(label, "seat"); err != nil {
		t.Fatal(err)
	}
	vk := types.IndexablePropertyValueKey(int64(1))
	lookups := map[string]func() error{
		"rel property": func() error {
			return ms.ForEachRelPropertyTxMember(typ, "seat", vk, func(types.RelID, types.Instant) bool { return true })
		},
		"node property": func() error {
			return ms.ForEachNodePropertyTxMember(label, "seat", vk, func(types.NodeID, types.Instant) bool { return true })
		},
		"K1 label": func() error {
			return ms.ForEachLabelTxMember(label, func(types.NodeID, types.Instant) bool { return true })
		},
		"K1 rel type": func() error {
			return ms.ForEachRelTypeTxMember(typ, func(types.RelID, types.Instant) bool { return true })
		},
	}
	for name, lookup := range lookups {
		if err := lookup(); err != nil { // builds the sidecar
			t.Fatalf("%s build: %v", name, err)
		}
		ms.mu.RLock()
		done := make(chan error, 1)
		go func() { done <- lookup() }()
		select {
		case err := <-done:
			ms.mu.RUnlock()
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(5 * time.Second):
			ms.mu.RUnlock()
			<-done
			t.Fatalf("%s: a lookup on a built sidecar waited for a concurrent reader (write lock)", name)
		}
	}
}
