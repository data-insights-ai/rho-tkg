package core

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// pdrMoveOps are the writes that move a current row into history.
var pdrMoveOps = []string{"Update", "CloseVersion", "Delete", "SetVersionInterval"}

func pdrMove(e *ccEnt, id int64, op string, closeAt types.Instant) error {
	switch op {
	case "Update":
		return e.update(id, map[string]any{"x": int64(42)})
	case "CloseVersion":
		return e.closeAt(id, closeAt)
	case "Delete":
		return e.del(id)
	default:
		return e.cascade(id, 2000, 3000, map[string]any{"x": int64(43)})
	}
}

// pdrEntityDoors are the per-entity chain resolvers, each through a public
// door: the point resolver (NodeAtTx), the as-of resolver (NodeAsOf), the
// interval resolver behind the During scans and the Allen resolver behind the
// Relating scans. A door renders the entity's row ("" = absent) or an error.
func pdrEntityDoors(e *ccEnt, pin types.Instant) map[string]func(id int64) (string, error) {
	g := e.g
	contains := types.Contains.Set()
	if e.rel {
		pick := func(id int64, rs []*types.Relationship, err error) (string, error) {
			if err != nil {
				return "", err
			}
			for _, r := range rs {
				if int64(r.ID()) == id {
					return pdrRelRow(r), nil
				}
			}
			return "", nil
		}
		return map[string]func(id int64) (string, error){
			"RelAtTx": func(id int64) (string, error) {
				r, err := g.Temporal.RelAtTx(types.RelID(id), pdrValidAt, pin)
				if err != nil {
					return "", err
				}
				return pdrRelRow(r), nil
			},
			"RelAsOf": func(id int64) (string, error) {
				r, err := g.Temporal.RelAsOf(types.RelID(id), pin)
				if err != nil {
					return "", err
				}
				return pdrRelRow(r), nil
			},
			"RelsDuringTx": func(id int64) (string, error) {
				rs, err := g.Temporal.RelsDuringTx(1200, 1800, pin)
				return pick(id, rs, err)
			},
			"RelsRelating": func(id int64) (string, error) {
				rs, err := g.Temporal.RelsRelating(1200, 1800, contains)
				row, err := pick(id, rs, err)
				if row != "" {
					row = "present" // current knowledge: the row may change, the membership not
				}
				return row, err
			},
		}
	}
	pick := func(id int64, ns []*types.Node, err error) (string, error) {
		if err != nil {
			return "", err
		}
		for _, n := range ns {
			if int64(n.ID()) == id {
				return pdrNodeRow(n), nil
			}
		}
		return "", nil
	}
	return map[string]func(id int64) (string, error){
		"NodeAtTx": func(id int64) (string, error) {
			n, err := g.Temporal.NodeAtTx(types.NodeID(id), pdrValidAt, pin)
			if err != nil {
				return "", err
			}
			return pdrNodeRow(n), nil
		},
		"NodeAsOf": func(id int64) (string, error) {
			n, err := g.Temporal.NodeAsOf(types.NodeID(id), pin)
			if err != nil {
				return "", err
			}
			return pdrNodeRow(n), nil
		},
		"NodesDuringTx": func(id int64) (string, error) {
			ns, err := g.Temporal.NodesDuringTx(1200, 1800, pin)
			return pick(id, ns, err)
		},
		"NodesRelating": func(id int64) (string, error) {
			ns, err := g.Temporal.NodesRelating(1200, 1800, contains)
			row, err := pick(id, ns, err)
			if row != "" {
				row = "present"
			}
			return row, err
		},
	}
}

// TestPointDoorRace_MoveBetweenChainReads lands a COMPLETE move (Update,
// CloseVersion, Delete, SetVersionInterval) of the entity between a chain
// assembly's current-row read and its history read, deterministically
// (chainReadHook), on every backend and every per-entity resolver: the door
// must answer the pre-move row. Breaks: a resolver that reads history before
// the current row (history before the move + current after it = the moved row
// in neither read). A resolver whose backend answers in one store call (the
// badger native as-of door) has no such window; the test asserts the hook
// fired everywhere else.
func TestPointDoorRace_MoveBetweenChainReads(t *testing.T) {
	pdrRun(t, func(t *testing.T, e *ccEnt) {
		closeAt := e.pin() + 1_000_000
		doors := pdrEntityDoors(e, 0)
		names := make([]string, 0, len(doors))
		for name := range doors {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, door := range names {
			for _, op := range pdrMoveOps {
				for _, prior := range []bool{false, true} {
					id := e.add(fmt.Sprintf("%s-%s-%v", door, op, prior), 1000, nil)
					if prior {
						e.mustUpdate(id, map[string]any{"x": int64(-1)})
					}
					pin := e.pin()
					eval := pdrEntityDoors(e, pin)[door]
					want, err := eval(id)
					if err != nil || want == "" {
						t.Fatalf("%s before the move: %q, %v", door, want, err)
					}
					fired := false
					var werr error
					e.g.chainReadHook = func(hid int64) {
						if hid != id || fired {
							return
						}
						fired = true
						done := make(chan error)
						go func() { done <- pdrMove(e, id, op, closeAt) }()
						werr = <-done
					}
					got, err := eval(id)
					e.g.chainReadHook = nil
					if werr != nil {
						t.Fatalf("%s %s: %v", door, op, werr)
					}
					native := e.g.txTimeQuery != nil && strings.HasSuffix(door, "AsOf")
					switch {
					case !fired && !native:
						t.Errorf("%s/%s/prior=%v: the move hook never fired", door, op, prior)
					case fired && native:
						t.Errorf("%s/%s/prior=%v: the native as-of door assembled a chain in core", door, op, prior)
					}
					if err != nil || got != want {
						t.Errorf("%s/%s/prior=%v with the move between the chain reads:\ngot  %q (%v)\nwant %q", door, op, prior, got, err, want)
					}
				}
			}
		}
	})
}
