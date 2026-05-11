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
