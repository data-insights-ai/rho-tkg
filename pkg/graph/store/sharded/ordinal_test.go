package sharded

import "testing"

func TestShardedStoreOrdinals(t *testing.T) {
	s, err := New(Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !s.HasOrdinals() || s.MaxNodeOrdinal() != 0 || s.MaxRelOrdinal() != 0 {
		t.Fatal("a new sharded store has ordinals and has handed out none")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.MaxNodeOrdinal() != 0 || s.MaxRelOrdinal() != 0 {
		t.Fatal("a closed store reports 0")
	}
	var nilStore *Store
	if nilStore.HasOrdinals() || nilStore.MaxNodeOrdinal() != 0 || nilStore.MaxRelOrdinal() != 0 {
		t.Fatal("nil store")
	}
}
