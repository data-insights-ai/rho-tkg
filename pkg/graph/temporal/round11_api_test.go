package temporal

import (
	"errors"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// spyLastOpts is the QueryOpts the round 11 per-entity doors forwarded.
var spyLastOpts storepkg.QueryOpts

func (s *temporalOpsSpy) NodeAtOpts(id types.NodeID, opts storepkg.QueryOpts) (*types.Node, error) {
	s.record("NodeAtOpts")
	s.lastNodeID, spyLastOpts = id, opts
	return nil, s.err
}

func (s *temporalOpsSpy) RelAtOpts(id types.RelID, opts storepkg.QueryOpts) (*types.Relationship, error) {
	s.record("RelAtOpts")
	s.lastRelID, spyLastOpts = id, opts
	return nil, s.err
}

func (s *temporalOpsSpy) NodesAtOptsByIDs(ids []types.NodeID, opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.record("NodesAtOptsByIDs")
	s.lastNodeIDs, spyLastOpts = ids, opts
	return nil, s.err
}

func (s *temporalOpsSpy) RelsAtOptsByIDs(ids []types.RelID, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	s.record("RelsAtOptsByIDs")
	s.lastRelIDs, spyLastOpts = ids, opts
	return nil, s.err
}

// TestRound11AtOptsWrappers — rule 1 for the per-entity opts doors: a nil API
// and a typed-nil Ops return ErrNilGraph; a ready API forwards the ID(s) and
// the opts (presence flags included) and returns the Ops' error.
func TestRound11AtOptsWrappers(t *testing.T) {
	opts := storepkg.QueryOpts{TxAt: 0, TxAtSet: true, ValidAt: -3, ValidAtSet: true}
	wantErr := errors.New("ops failed")
	calls := map[string]func(a *API) error{
		"NodeAtOpts":       func(a *API) error { _, err := a.NodeAtOpts(7, opts); return err },
		"RelAtOpts":        func(a *API) error { _, err := a.RelAtOpts(8, opts); return err },
		"NodesAtOptsByIDs": func(a *API) error { _, err := a.NodesAtOptsByIDs([]types.NodeID{7, 9}, opts); return err },
		"RelsAtOptsByIDs":  func(a *API) error { _, err := a.RelsAtOptsByIDs([]types.RelID{8, 10}, opts); return err },
	}
	for name, call := range calls {
		for label, api := range map[string]*API{"nil API": nil, "typed-nil ops": New((*temporalOpsSpy)(nil))} {
			if err := call(api); !errors.Is(err, grapherr.ErrNilGraph) {
				t.Fatalf("%s %s = %v, want ErrNilGraph", name, label, err)
			}
		}
		spy := &temporalOpsSpy{err: wantErr}
		spyLastOpts = storepkg.QueryOpts{}
		if err := call(New(spy)); !errors.Is(err, wantErr) || !slices.Equal(spy.calls, []string{name}) || spyLastOpts != opts {
			t.Fatalf("%s: err %v, calls %v, opts %+v", name, err, spy.calls, spyLastOpts)
		}
		switch name {
		case "NodeAtOpts":
			if spy.lastNodeID != 7 {
				t.Fatalf("NodeAtOpts id %d", spy.lastNodeID)
			}
		case "RelAtOpts":
			if spy.lastRelID != 8 {
				t.Fatalf("RelAtOpts id %d", spy.lastRelID)
			}
		case "NodesAtOptsByIDs":
			if !slices.Equal(spy.lastNodeIDs, []types.NodeID{7, 9}) {
				t.Fatalf("NodesAtOptsByIDs ids %v", spy.lastNodeIDs)
			}
		case "RelsAtOptsByIDs":
			if !slices.Equal(spy.lastRelIDs, []types.RelID{8, 10}) {
				t.Fatalf("RelsAtOptsByIDs ids %v", spy.lastRelIDs)
			}
		}
	}
}
