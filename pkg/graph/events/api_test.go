package events

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/grapherr"
)

func TestAPINilReceiversReturnErrNilGraphOrNil(t *testing.T) {
	t.Parallel()

	var nilAPI *API
	if err := nilAPI.SetSync(nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil SetSync = %v, want ErrNilGraph", err)
	}
	if err := nilAPI.SetAsync(nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("nil SetAsync = %v, want ErrNilGraph", err)
	}
	if got := nilAPI.GetSync(); got != nil {
		t.Fatalf("nil GetSync = %v, want nil", got)
	}
	if got := nilAPI.GetAsync(); got != nil {
		t.Fatalf("nil GetAsync = %v, want nil", got)
	}

	api := New((*eventsOpsSpy)(nil))
	if err := api.SetSync(nil); !errors.Is(err, grapherr.ErrNilGraph) {
		t.Fatalf("typed-nil SetSync = %v, want ErrNilGraph", err)
	}
	if got := api.GetSync(); got != nil {
		t.Fatalf("typed-nil GetSync = %v, want nil", got)
	}
	if got := api.GetAsync(); got != nil {
		t.Fatalf("typed-nil GetAsync = %v, want nil", got)
	}
}

func TestAPIForwardsMethodsAndErrors(t *testing.T) {
	t.Parallel()

	syncErr := errors.New("sync failed")
	asyncErr := errors.New("async failed")
	syncBus := NewEventBus()
	asyncBus := &AsyncEventBus{}
	ops := &eventsOpsSpy{
		syncErr:  syncErr,
		asyncErr: asyncErr,
		getSync:  syncBus,
		getAsync: asyncBus,
	}
	api := New(ops)

	if err := api.SetSync(syncBus); !errors.Is(err, syncErr) {
		t.Fatalf("SetSync error = %v, want %v", err, syncErr)
	}
	if err := api.SetAsync(asyncBus); !errors.Is(err, asyncErr) {
		t.Fatalf("SetAsync error = %v, want %v", err, asyncErr)
	}
	if got := api.GetSync(); got != syncBus {
		t.Fatalf("GetSync = %p, want %p", got, syncBus)
	}
	if got := api.GetAsync(); got != asyncBus {
		t.Fatalf("GetAsync = %p, want %p", got, asyncBus)
	}

	if ops.syncArg != syncBus || ops.asyncArg != asyncBus {
		t.Fatalf("forwarded buses = sync %p async %p", ops.syncArg, ops.asyncArg)
	}
	if ops.setSyncCalls != 1 || ops.setAsyncCalls != 1 || ops.getSyncCalls != 1 || ops.getAsyncCalls != 1 {
		t.Fatalf("call counts = sync %d async %d getSync %d getAsync %d, want 1/1/1/1", ops.setSyncCalls, ops.setAsyncCalls, ops.getSyncCalls, ops.getAsyncCalls)
	}
}

type eventsOpsSpy struct {
	syncArg  *EventBus
	asyncArg *AsyncEventBus
	getSync  *EventBus
	getAsync *AsyncEventBus

	syncErr  error
	asyncErr error

	setSyncCalls  int
	setAsyncCalls int
	getSyncCalls  int
	getAsyncCalls int
}

func (s *eventsOpsSpy) SetSync(bus *EventBus) error {
	s.setSyncCalls++
	s.syncArg = bus
	return s.syncErr
}

func (s *eventsOpsSpy) SetAsync(bus *AsyncEventBus) error {
	s.setAsyncCalls++
	s.asyncArg = bus
	return s.asyncErr
}

func (s *eventsOpsSpy) GetSync() *EventBus {
	s.getSyncCalls++
	return s.getSync
}

func (s *eventsOpsSpy) GetAsync() *AsyncEventBus {
	s.getAsyncCalls++
	return s.getAsync
}
