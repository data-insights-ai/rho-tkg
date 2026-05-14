package events

import "testing"

func TestEventBusZeroValueReadyToUse(t *testing.T) {
	var bus EventBus

	var got []EventType
	unsub := bus.Subscribe(func(e Event) {
		got = append(got, e.Type)
	})

	bus.Publish(Event{Type: EventNodeCreate})
	if len(got) != 1 || got[0] != EventNodeCreate {
		t.Fatalf("zero-value EventBus Publish delivered %v, want [EventNodeCreate]", got)
	}

	unsub()
	unsub()
	bus.PublishBatch(Event{Type: EventNodeDelete})
	if len(got) != 1 {
		t.Fatalf("zero-value EventBus unsubscribe failed: got %d events, want 1", len(got))
	}
}

func TestEventBusSubscribeNilIsNoop(t *testing.T) {
	var bus EventBus

	unsub := bus.Subscribe(nil)
	unsub()
	unsub()

	bus.mu.RLock()
	handlerCount := len(bus.handlers)
	bus.mu.RUnlock()
	if handlerCount != 0 {
		t.Fatalf("Subscribe(nil) installed %d handlers, want 0", handlerCount)
	}

	bus.Publish(Event{Type: EventNodeCreate})
	bus.PublishBatch(Event{Type: EventRelDelete})
}

func TestEventBusNilReceiverMethodsAreNoop(t *testing.T) {
	var bus *EventBus

	unsub := bus.Subscribe(func(Event) {
		t.Fatal("nil EventBus should not install handlers")
	})
	unsub()
	unsub()

	bus.Publish(Event{Type: EventNodeCreate})
	bus.PublishBatch(Event{Type: EventRelDelete})
}

func TestEventBusPublishBatchUsesStableHandlerSnapshot(t *testing.T) {
	var bus EventBus

	var got []EventType
	var newGot []EventType
	var unsub func()
	unsub = bus.Subscribe(func(e Event) {
		got = append(got, e.Type)
		if e.Type == EventNodeCreate {
			unsub()
			bus.Subscribe(func(e Event) {
				newGot = append(newGot, e.Type)
			})
		}
	})

	bus.PublishBatch(
		Event{Type: EventNodeCreate},
		Event{Type: EventRelCreate},
	)

	want := []EventType{EventNodeCreate, EventRelCreate}
	if !sameEventTypes(got, want) {
		t.Fatalf("original handler saw %v, want %v", got, want)
	}
	if len(newGot) != 0 {
		t.Fatalf("new handler saw same-batch events %v, want none", newGot)
	}

	bus.Publish(Event{Type: EventNodeDelete})
	if !sameEventTypes(newGot, []EventType{EventNodeDelete}) {
		t.Fatalf("new handler after batch saw %v, want [EventNodeDelete]", newGot)
	}
	if !sameEventTypes(got, want) {
		t.Fatalf("unsubscribed handler after batch saw %v, want %v", got, want)
	}
}

func sameEventTypes(got, want []EventType) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
