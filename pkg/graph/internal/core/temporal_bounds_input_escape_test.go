package core

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestLegacyTemporalInputDoesNotAllocateMapContainer(t *testing.T) {
	allocations := testing.AllocsPerRun(100, func() {
		vf, vt, ca, tx, filtered, presence, err := extractTemporalWithBounds(map[string]any{"seq": int64(1), "p": int64(0)})
		if err != nil || vf != 0 || vt != 0 || ca != 0 || tx != 0 || presence != 0 || filtered["seq"] != int64(1) || filtered["p"] != int64(0) {
			panic("legacy temporal extraction changed")
		}
	})
	if allocations != 0 {
		t.Fatalf("ordinary input allocated map containers: %g allocs/op", allocations)
	}
}

func TestTypedTemporalInputPreservesCallerAndExactBounds(t *testing.T) {
	cases := []types.TemporalRange{
		{From: types.TemporalEndpoint{Value: 0, Finite: true}, To: types.TemporalEndpoint{Value: 1, Finite: true}},
		{From: types.TemporalEndpoint{Value: -10, Finite: true}, To: types.TemporalEndpoint{Value: 0, Finite: true}},
		{From: types.TemporalEndpoint{Value: 0, Finite: true}},
		{To: types.TemporalEndpoint{Value: 0, Finite: true}},
	}
	for _, bounds := range cases {
		nested := map[string]any{"literal": "preserved"}
		input := map[string]any{"tkg_valid_bounds": bounds, "tkg_created_at": int64(13), "payload": nested, "zero": int64(0)}
		vf, vt, created, _, filtered, presence, err := extractTemporalWithBounds(input)
		if err != nil {
			t.Fatal(err)
		}
		var tm types.TemporalMetadata
		if err := tm.SetValidBounds(bounds); err != nil {
			t.Fatal(err)
		}
		if vf != tm.ValidFrom || vt != tm.ValidTo || presence != tm.ValidEndpoints || created != 13 {
			t.Fatalf("incorrect bounds: %v/%v presence=%v", vf, vt, presence)
		}
		if !reflect.DeepEqual(filtered, map[string]any{"payload": nested, "zero": int64(0)}) {
			t.Fatalf("lost ordinary properties: %#v", filtered)
		}
		if !reflect.DeepEqual(input, map[string]any{"tkg_valid_bounds": bounds, "tkg_created_at": int64(13), "payload": nested, "zero": int64(0)}) {
			t.Fatalf("mutated caller: %#v", input)
		}
		filtered["zero"] = int64(9)
		if input["zero"] != int64(0) {
			t.Fatal("filtered map aliases input")
		}
		filtered["payload"].(map[string]any)["literal"] = "shallow copy"
		if nested["literal"] != "shallow copy" {
			t.Fatal("copy changed value identity")
		}
	}
	for _, input := range []map[string]any{
		{"tkg_valid_bounds": "bad"},
		{"tkg_valid_bounds": cases[0], "tkg_valid_from": int64(0)},
		{"tkg_valid_bounds": cases[0], "tkg_valid_to": int64(0)},
		{"tkg_valid_bounds": types.TemporalRange{From: types.TemporalEndpoint{Value: 2, Finite: true}, To: types.TemporalEndpoint{Value: 1, Finite: true}}},
	} {
		if _, _, _, _, _, _, err := extractTemporalWithBounds(input); !errors.Is(err, ErrInvalidTimeRange) {
			t.Fatalf("invalid input was accepted: %#v: %v", input, err)
		}
	}
	if _, _, _, _, filtered, _, err := extractTemporalWithBounds(nil); err != nil || filtered != nil {
		t.Fatalf("nil compatibility: %#v: %v", filtered, err)
	}
}

func TestTemporalInputPipelinePreservesCompleteAnswers(t *testing.T) {
	for _, producers := range []int{1, 8} {
		t.Run(fmt.Sprintf("p%d", producers), func(t *testing.T) {
			c := newIngestGraph(t)
			const count = 10000
			ids := make([][]types.NodeID, producers)
			var wg sync.WaitGroup
			for p := range producers {
				wg.Go(func() {
					session, err := c.Ingest.NewSession(IngestOptions{DeclareLabels: []string{"Input"}})
					if err != nil {
						t.Error(err)
						return
					}
					var last SubmitToken
					bounds := types.TemporalRange{From: types.TemporalEndpoint{Value: 0, Finite: true}, To: types.TemporalEndpoint{Value: 1, Finite: true}}
					for seq := range count / producers {
						input := map[string]any{"p": int64(p), "seq": int64(seq)}
						if seq%3 == 0 {
							input["tkg_valid_bounds"] = bounds
						}
						node, err := session.AddNode([]string{"Input"}, input)
						if err != nil {
							t.Error(err)
							return
						}
						if seq%3 == 0 && input["tkg_valid_bounds"] != bounds {
							t.Error("mutated caller bounds")
							return
						}
						ids[p] = append(ids[p], node.ID())
						if seq%512 == 511 {
							last, err = session.Submit()
							if err != nil {
								t.Error(err)
								return
							}
						}
					}
					token, err := session.Submit()
					if err != nil {
						t.Error(err)
						return
					}
					if token.Seq != 0 {
						last = token
					}
					if err := c.Ingest.WaitApplied(last); err != nil {
						t.Error(err)
					}
				})
			}
			wg.Wait()
			actual, err := c.Nodes.Count()
			if err != nil || actual != count {
				t.Fatalf("lost/duplicated pipeline rows: %d: %v", actual, err)
			}
			seen := make(map[types.NodeID]bool, count)
			for p, rows := range ids {
				if len(rows) != count/producers {
					t.Fatalf("incomplete producer %d: %d", p, len(rows))
				}
				for seq, id := range rows {
					if seen[id] {
						t.Fatalf("duplicate identity %v", id)
					}
					seen[id] = true
					node, err := c.Nodes.Get(t.Context(), id)
					if err != nil {
						t.Fatal(err)
					}
					gotP, _ := node.GetProperty("p")
					gotSeq, _ := node.GetProperty("seq")
					if gotP != int64(p) || gotSeq != int64(seq) {
						t.Fatalf("wrong row p%d seq%d: %v/%v", p, seq, gotP, gotSeq)
					}
					if _, stored := node.GetProperty("tkg_valid_bounds"); stored {
						t.Fatal("reserved input was stored as property")
					}
					if seq%3 == 0 {
						tm := node.Temporal()
						if tm.ValidFrom != 0 || tm.ValidTo != 1 || tm.ValidEndpoints != types.EndpointsExplicit|types.EndpointFromFinite|types.EndpointToFinite {
							t.Fatalf("lost exact epoch bounds: %+v", tm)
						}
					}
				}
			}
		})
	}
}
