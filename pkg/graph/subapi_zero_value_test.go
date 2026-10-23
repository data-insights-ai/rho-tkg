package graph_test

import (
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	hashpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/hash"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestSubAPINewWithTypedNilOpsReturnsErrNilGraph(t *testing.T) {
	t.Parallel()

	var hashOps *typedNilHashOps
	hashAPI := hashpkg.New(hashOps)
	if got, err := hashAPI.VerifyNodeChain(0); !errors.Is(err, graphpkg.ErrNilGraph) || got {
		t.Fatalf("typed nil Hash.VerifyNodeChain = (%v, %v), want (false, ErrNilGraph)", got, err)
	}

	var eventOps *typedNilEventOps
	eventsAPI := eventspkg.New(eventOps)
	if err := eventsAPI.SetSync(nil); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("typed nil Events.SetSync = %v, want ErrNilGraph", err)
	}
	if got := eventsAPI.GetSync(); got != nil {
		t.Fatalf("typed nil Events.GetSync = %v, want nil", got)
	}
	if got := eventsAPI.GetAsync(); got != nil {
		t.Fatalf("typed nil Events.GetAsync = %v, want nil", got)
	}
}

type typedNilHashOps struct{}

func (*typedNilHashOps) VerifyNodeChain(types.NodeID) (bool, error) {
	panic("typed nil hash ops should not be invoked")
}

func (*typedNilHashOps) VerifyRelChain(types.RelID) (bool, error) {
	panic("typed nil hash ops should not be invoked")
}

type typedNilEventOps struct{}

func (*typedNilEventOps) SetSync(*eventspkg.EventBus) error {
	panic("typed nil event ops should not be invoked")
}

func (*typedNilEventOps) SetAsync(*eventspkg.AsyncEventBus) error {
	panic("typed nil event ops should not be invoked")
}

func (*typedNilEventOps) GetSync() *eventspkg.EventBus {
	panic("typed nil event ops should not be invoked")
}

func (*typedNilEventOps) GetAsync() *eventspkg.AsyncEventBus {
	panic("typed nil event ops should not be invoked")
}
