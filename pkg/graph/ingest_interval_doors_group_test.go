package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ivOutcome is the apply outcome of one Submit: the Submit error (strong sync
// and concurrent sessions report it there) or, for an async session, the
// WaitApplied result of the token.
func ivOutcome(g *graphpkg.Graph, tok ingest.SubmitToken, submitErr error) error {
	if submitErr != nil {
		return submitErr
	}
	return g.Ingest().WaitApplied(tok)
}

// Failed-group shape. Four groups are submitted back to back BEFORE any
// outcome is read (so in the async mode they are in flight at once and the
// strong applier may coalesce them into one batch):
//
//	mixed-node: a missing node and a good node in ONE group
//	mixed-rel:  a missing rel and a good rel in ONE group
//	bad-only:   a missing rel
//	good:       a good rel and a good node
//
// Each outcome must be its own: the mixed groups and the bad-only group carry
// the real not-found sentinel, the good group is nil. The good entity inside a
// mixed group still gets its history row (partial success, as the Batch door),
// and nothing exists under the missing ids.
//
// Catches: an applier that drops a whole group when one of its ops fails (the
// good entity of a mixed group has no history row), that attributes a failure
// to a sibling group, that swallows the failure (nil for a missing id), or that
// creates the missing entity.
func TestSessionSetVersionInterval_FailedGroupShape(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		for _, m := range ivModes {
			t.Run(b.name+"/"+m.name, func(t *testing.T) {
				g := b.open(t)
				mixedRel, goodRel := ivNewRel(t, g), ivNewRel(t, g)
				mixedNode, goodNode := ivNewNode(t, g), ivNewNode(t, g)
				missNode1 := types.NodeID(1 << 40)
				missRel1, missRel2 := types.RelID(1<<41), types.RelID(1<<41+1) // distinct from the node id: the applier keys groups by the numeric id
				patch := map[string]any{"cnt": int64(2)}

				s, err := g.Ingest().NewSession(m.opts)
				if err != nil {
					t.Fatalf("NewSession: %v", err)
				}
				defer s.Close()

				type group struct {
					name  string
					queue func() error
					want  error
				}
				groups := []group{
					{"mixed-node", func() error {
						if err := s.SetNodeVersionInterval(missNode1, ivT, ivT+5000, patch); err != nil {
							return err
						}
						return s.SetNodeVersionInterval(mixedNode.ID(), ivT, ivT+5000, patch)
					}, graphpkg.ErrNodeNotFound},
					{"mixed-rel", func() error {
						if err := s.SetRelVersionInterval(missRel1, ivT, ivT+5000, patch); err != nil {
							return err
						}
						return s.SetRelVersionInterval(mixedRel.ID(), ivT, ivT+5000, patch)
					}, graphpkg.ErrRelNotFound},
					{"bad-only", func() error {
						return s.SetRelVersionInterval(missRel2, ivT, ivT+5000, patch)
					}, graphpkg.ErrRelNotFound},
					{"good", func() error {
						if err := s.SetRelVersionInterval(goodRel.ID(), ivT, ivT+5000, patch); err != nil {
							return err
						}
						return s.SetNodeVersionInterval(goodNode.ID(), ivT, ivT+5000, patch)
					}, nil},
				}

				toks := make([]ingest.SubmitToken, len(groups))
				subErrs := make([]error, len(groups))
				for i, gr := range groups {
					if err := gr.queue(); err != nil {
						t.Fatalf("%s: queue: %v", gr.name, err)
					}
					toks[i], subErrs[i] = s.Submit()
				}
				for i, gr := range groups {
					got := ivOutcome(g, toks[i], subErrs[i])
					if gr.want == nil && got != nil {
						t.Errorf("%s outcome = %v, want nil (a sibling's failure must not leak)", gr.name, got)
					}
					if gr.want != nil && !errors.Is(got, gr.want) {
						t.Errorf("%s outcome = %v, want %v", gr.name, got, gr.want)
					}
				}

				for _, e := range []struct {
					name string
					n    int
				}{
					{"mixed rel (good half)", histLenRel(g, mixedRel.ID())},
					{"good rel", histLenRel(g, goodRel.ID())},
					{"mixed node (good half)", histLenNode(g, mixedNode.ID())},
					{"good node", histLenNode(g, goodNode.ID())},
				} {
					if e.n < 1 {
						t.Errorf("%s has %d appended history rows, want >= 1", e.name, e.n)
					}
				}
				ctx := context.Background()
				if _, err := g.Nodes().Get(ctx, missNode1); !errors.Is(err, graphpkg.ErrNodeNotFound) {
					t.Errorf("Nodes().Get(missing) = %v, want ErrNodeNotFound", err)
				}
				for _, id := range []types.RelID{missRel1, missRel2} {
					if _, err := g.Rels().Get(ctx, id); !errors.Is(err, graphpkg.ErrRelNotFound) {
						t.Errorf("Rels().Get(missing %d) = %v, want ErrRelNotFound", id, err)
					}
				}
			})
		}
	}
}

func histLenRel(g *graphpkg.Graph, id types.RelID) int {
	h, err := g.Rels().History(id)
	if err != nil {
		return -1
	}
	return len(h)
}

func histLenNode(g *graphpkg.Graph, id types.NodeID) int {
	h, err := g.Nodes().History(id)
	if err != nil {
		return -1
	}
	return len(h)
}

// ivEntity is the node/relationship-agnostic handle the sibling and chained
// test drives, so the two kinds share one body (parity).
type ivEntity struct {
	set   func(s *ingest.Session, ve types.Instant, cnt int64) error
	atTx  func(valid, tx types.Instant) (cnt int64, found bool, err error)
	pin   func() types.Instant // highest TxFrom among the head row and History
	hist  func() int
	notFn error
}

func ivRelEntity(t *testing.T, g *graphpkg.Graph, r *types.Relationship) ivEntity {
	return ivEntity{
		set: func(s *ingest.Session, ve types.Instant, cnt int64) error {
			return s.SetRelVersionInterval(r.ID(), ivT, ve, map[string]any{"cnt": cnt})
		},
		atTx: func(valid, tx types.Instant) (int64, bool, error) {
			got, err := g.Temporal().RelAtTx(r.ID(), valid, tx)
			if err != nil || got == nil {
				return 0, false, err
			}
			return ivCnt(t, got, "RelAtTx"), true, nil
		},
		pin: func() types.Instant {
			cur, _ := g.Rels().Get(context.Background(), r.ID())
			p := cur.Temporal().TxFrom
			h, _ := g.Rels().History(r.ID())
			for _, x := range h {
				if x.Temporal().TxFrom > p {
					p = x.Temporal().TxFrom
				}
			}
			return p
		},
		hist:  func() int { return histLenRel(g, r.ID()) },
		notFn: storepkg.ErrNoVersionValidAt,
	}
}

func ivNodeEntity(t *testing.T, g *graphpkg.Graph, n *types.Node) ivEntity {
	return ivEntity{
		set: func(s *ingest.Session, ve types.Instant, cnt int64) error {
			return s.SetNodeVersionInterval(n.ID(), ivT, ve, map[string]any{"cnt": cnt})
		},
		atTx: func(valid, tx types.Instant) (int64, bool, error) {
			got, err := g.Temporal().NodeAtTx(n.ID(), valid, tx)
			if err != nil || got == nil {
				return 0, false, err
			}
			return ivCnt(t, got, "NodeAtTx"), true, nil
		},
		pin: func() types.Instant {
			cur, _ := g.Nodes().Get(context.Background(), n.ID())
			p := cur.Temporal().TxFrom
			h, _ := g.Nodes().History(n.ID())
			for _, x := range h {
				if x.Temporal().TxFrom > p {
					p = x.Temporal().TxFrom
				}
			}
			return p
		},
		hist:  func() int { return histLenNode(g, n.ID()) },
		notFn: storepkg.ErrNoVersionValidAt,
	}
}

// Two corrections in one session on the entity under test while an untouched
// sibling of the same kind shares its interval (rule 16: diverging lifecycles,
// exact reads, negative assertions):
//
//	create [T,T+1000) cnt 1 (target) and cnt 7 (sibling, never corrected)
//	correction 1: [T,T+5000) cnt 2         -> pin1
//	correction 2: [T,T+9000) cnt 3         -> pin2   (same session, later Submit)
//
// Catches: a door that rewrites rows in place (pinBefore or pin1 would see a
// later value), a chained correction that is lost or applied only to the head
// row (pin2 at T+7000 stays 2 or absent), a second correction that leaks into
// the first belief (pin1 at T+7000 found), and a correction that touches the
// sibling (its pinned reads or history change).
func TestSessionSetVersionInterval_SiblingAndChained(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		for _, m := range ivModes {
			for _, kind := range []string{"rel", "node"} {
				t.Run(b.name+"/"+m.name+"/"+kind, func(t *testing.T) {
					g := b.open(t)
					var target, sibling ivEntity
					if kind == "rel" {
						tr, sr := ivNewRel(t, g), ivNewRelCnt(t, g, 7)
						target, sibling = ivRelEntity(t, g, tr), ivRelEntity(t, g, sr)
					} else {
						tn, sn := ivNewNode(t, g), ivNewNodeCnt(t, g, 7)
						target, sibling = ivNodeEntity(t, g, tn), ivNodeEntity(t, g, sn)
					}
					pinBefore := target.pin()
					sibPinBefore := sibling.pin()
					time.Sleep(5 * time.Millisecond)

					s, err := g.Ingest().NewSession(m.opts)
					if err != nil {
						t.Fatalf("NewSession: %v", err)
					}
					defer s.Close()
					step := func(ve types.Instant, cnt int64) types.Instant {
						t.Helper()
						if err := target.set(s, ve, cnt); err != nil {
							t.Fatalf("set(ve=%d): %v", ve, err)
						}
						tok, subErr := s.Submit()
						if err := ivOutcome(g, tok, subErr); err != nil {
							t.Fatalf("apply(ve=%d): %v", ve, err)
						}
						return target.pin()
					}
					pin1 := step(ivT+5000, 2)
					time.Sleep(5 * time.Millisecond)
					pin2 := step(ivT+9000, 3)
					if pinBefore >= pin1 || pin1 >= pin2 {
						t.Fatalf("pins not increasing: before %d, 1 %d, 2 %d", pinBefore, pin1, pin2)
					}

					type read struct {
						valid, tx types.Instant
						want      int64
						found     bool
					}
					for _, r := range []read{
						{ivT + 500, pinBefore, 1, true},
						{ivT + 3000, pinBefore, 0, false},
						{ivT + 3000, pin1, 2, true},
						{ivT + 7000, pin1, 0, false},
						{ivT + 3000, pin2, 3, true},
						{ivT + 7000, pin2, 3, true},
						{ivT + 9500, pin2, 0, false},
					} {
						cnt, found, err := target.atTx(r.valid, r.tx)
						if err != nil && !errors.Is(err, target.notFn) {
							t.Fatalf("AtTx(valid %d, tx %d): %v", r.valid, r.tx, err)
						}
						if found != r.found || (found && cnt != r.want) {
							t.Errorf("AtTx(valid T+%d, tx %d) = (cnt %d, found %v), want (cnt %d, found %v)",
								r.valid-ivT, r.tx, cnt, found, r.want, r.found)
						}
					}

					// The sibling is exactly as created.
					if n := sibling.hist(); n != 0 {
						t.Errorf("sibling has %d appended history rows, want 0", n)
					}
					if p := sibling.pin(); p != sibPinBefore {
						t.Errorf("sibling TxFrom moved %d -> %d", sibPinBefore, p)
					}
					for _, tx := range []types.Instant{sibPinBefore, pin1, pin2} {
						if cnt, found, err := sibling.atTx(ivT+500, tx); err != nil || !found || cnt != 7 {
							t.Errorf("sibling AtTx(T+500, %d) = (%d, %v, %v)", tx, cnt, found, err)
						}
						if cnt, found, _ := sibling.atTx(ivT+3000, tx); found {
							t.Errorf("sibling AtTx(T+3000, %d) = cnt %d, want absent (its interval was not grown)", tx, cnt)
						}
					}
				})
			}
		}
	}
}
