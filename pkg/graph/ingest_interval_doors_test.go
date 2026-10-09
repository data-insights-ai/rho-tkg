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

// Session.SetNodeVersionInterval / SetRelVersionInterval on every backend and
// every session mode (strong sync, strong async + WaitApplied, concurrent).
//
// Requested by ai-soc: a burst fact [vs, ve) grows to [vs, ve') through the
// ingest session as an append-only correction, the same cascade the standalone
// Temporal().SetRelVersionInterval and the Batch door run.
//
// Catches: a door that forwards to a plain UpdateRelationship (cnt changes but
// the valid interval does not grow: RelAtTx(T+3000) stays cnt 1), a door that
// rewrites the stored row in place (lesson 46: RelAtTx(T+500, pinBefore) would
// report cnt 2 or nothing), and a door that is queued but never applied
// (Pending drops, nothing changes).

const ivT = types.Instant(1_000_000)

type ivMode struct {
	name string
	opts ingest.IngestOptions
}

var ivModes = []ivMode{
	{"strong-sync", ingest.IngestOptions{Sync: true}},
	{"strong-async", ingest.IngestOptions{}},
	{"concurrent", ingest.IngestOptions{Concurrent: true}},
}

// ivSubmit submits and waits for the apply outcome in every mode.
func ivSubmit(g *graphpkg.Graph, s *ingest.Session) error {
	tok, err := s.Submit()
	if err != nil {
		return err
	}
	return g.Ingest().WaitApplied(tok)
}

func ivCnt(t *testing.T, e interface {
	GetProperty(string) (any, bool)
}, ctx string) int64 {
	t.Helper()
	v, ok := e.GetProperty("cnt")
	if !ok {
		t.Fatalf("%s: no cnt property", ctx)
	}
	n, ok := v.(int64)
	if !ok {
		t.Fatalf("%s: cnt = %#v, want int64", ctx, v)
	}
	return n
}

func ivNewRel(t *testing.T, g *graphpkg.Graph) *types.Relationship {
	t.Helper()
	ctx := context.Background()
	a, err := g.Nodes().Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Ev"}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	r, err := g.Rels().Add(ctx, "LINK", a, b, map[string]any{
		"tkg_valid_from": ivT, "tkg_valid_to": ivT + 1000, "cnt": int64(1),
	})
	if err != nil {
		t.Fatalf("Rels.Add: %v", err)
	}
	return r
}

func ivNewNode(t *testing.T, g *graphpkg.Graph) *types.Node {
	t.Helper()
	n, err := g.Nodes().Add(context.Background(), []string{"Ev"}, map[string]any{
		"tkg_valid_from": ivT, "tkg_valid_to": ivT + 1000, "cnt": int64(1),
	})
	if err != nil {
		t.Fatalf("Nodes.Add: %v", err)
	}
	return n
}

func TestSessionSetRelVersionInterval_TwoPhase(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		for _, m := range ivModes {
			t.Run(b.name+"/"+m.name, func(t *testing.T) {
				g := b.open(t)
				r := ivNewRel(t, g)
				pinBefore := r.Temporal().TxFrom
				time.Sleep(5 * time.Millisecond)

				s, err := g.Ingest().NewSession(m.opts)
				if err != nil {
					t.Fatalf("NewSession: %v", err)
				}
				if err := s.SetRelVersionInterval(r.ID(), ivT, ivT+5000, map[string]any{"cnt": int64(2)}); err != nil {
					t.Fatalf("SetRelVersionInterval: %v", err)
				}
				if got := s.Pending(); got != 1 {
					t.Fatalf("Pending = %d, want 1 (queued, not applied)", got)
				}
				if err := ivSubmit(g, s); err != nil {
					t.Fatalf("Submit/WaitApplied: %v", err)
				}
				if err := s.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				hist, err := g.Rels().History(r.ID())
				if err != nil || len(hist) < 1 {
					t.Fatalf("History = %d rows, %v; want >= 1 appended row (append-only correction)", len(hist), err)
				}
				pinAfter := pinBefore
				for _, h := range hist {
					if h.Temporal().TxFrom > pinAfter {
						pinAfter = h.Temporal().TxFrom
					}
				}
				if pinAfter <= pinBefore {
					t.Fatalf("pinAfter %d <= pinBefore %d: the correction was not stamped later", pinAfter, pinBefore)
				}

				got, err := g.Temporal().RelAtTx(r.ID(), ivT+3000, pinAfter)
				if err != nil {
					t.Fatalf("RelAtTx(T+3000, pinAfter): %v", err)
				}
				if c := ivCnt(t, got, "RelAtTx(T+3000, pinAfter)"); c != 2 {
					t.Fatalf("RelAtTx(T+3000, pinAfter) cnt = %d, want 2 (grown interval)", c)
				}
				got, err = g.Temporal().RelAtTx(r.ID(), ivT+500, pinBefore)
				if err != nil {
					t.Fatalf("RelAtTx(T+500, pinBefore): %v", err)
				}
				if c := ivCnt(t, got, "RelAtTx(T+500, pinBefore)"); c != 1 {
					t.Fatalf("RelAtTx(T+500, pinBefore) cnt = %d, want 1 (the old belief survives)", c)
				}
				// The old belief must not cover the grown tail.
				if old, err := g.Temporal().RelAtTx(r.ID(), ivT+3000, pinBefore); err == nil && old != nil {
					t.Fatalf("RelAtTx(T+3000, pinBefore) = %v; the extension leaked into the old belief", old)
				}
				// Documented behaviour: the correction rows are appended to the
				// history; the head row (Get) is not rewritten, and RelAsOf at the
				// pin still answers that head row, not the appended correction.
				asOf, err := g.Temporal().RelAsOf(r.ID(), pinAfter)
				if err != nil {
					t.Fatalf("RelAsOf(pinAfter): %v", err)
				}
				cur, err := g.Rels().Get(context.Background(), r.ID())
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if asOf.Version() != cur.Version() || ivCnt(t, asOf, "RelAsOf(pinAfter)") != 1 {
					t.Fatalf("RelAsOf(pinAfter) version %d != head row %d (cnt 1)", asOf.Version(), cur.Version())
				}
			})
		}
	}
}

func TestSessionSetNodeVersionInterval_TwoPhase(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		for _, m := range ivModes {
			t.Run(b.name+"/"+m.name, func(t *testing.T) {
				g := b.open(t)
				n := ivNewNode(t, g)
				pinBefore := n.Temporal().TxFrom
				time.Sleep(5 * time.Millisecond)

				s, err := g.Ingest().NewSession(m.opts)
				if err != nil {
					t.Fatalf("NewSession: %v", err)
				}
				if err := s.SetNodeVersionInterval(n.ID(), ivT, ivT+5000, map[string]any{"cnt": int64(2)}); err != nil {
					t.Fatalf("SetNodeVersionInterval: %v", err)
				}
				if got := s.Pending(); got != 1 {
					t.Fatalf("Pending = %d, want 1 (queued, not applied)", got)
				}
				if err := ivSubmit(g, s); err != nil {
					t.Fatalf("Submit/WaitApplied: %v", err)
				}
				if err := s.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				hist, err := g.Nodes().History(n.ID())
				if err != nil || len(hist) < 1 {
					t.Fatalf("History = %d rows, %v; want >= 1 appended row (append-only correction)", len(hist), err)
				}
				pinAfter := pinBefore
				for _, h := range hist {
					if h.Temporal().TxFrom > pinAfter {
						pinAfter = h.Temporal().TxFrom
					}
				}
				if pinAfter <= pinBefore {
					t.Fatalf("pinAfter %d <= pinBefore %d: the correction was not stamped later", pinAfter, pinBefore)
				}
				got, err := g.Temporal().NodeAtTx(n.ID(), ivT+3000, pinAfter)
				if err != nil {
					t.Fatalf("NodeAtTx(T+3000, pinAfter): %v", err)
				}
				if c := ivCnt(t, got, "NodeAtTx(T+3000, pinAfter)"); c != 2 {
					t.Fatalf("NodeAtTx(T+3000, pinAfter) cnt = %d, want 2 (grown interval)", c)
				}
				got, err = g.Temporal().NodeAtTx(n.ID(), ivT+500, pinBefore)
				if err != nil {
					t.Fatalf("NodeAtTx(T+500, pinBefore): %v", err)
				}
				if c := ivCnt(t, got, "NodeAtTx(T+500, pinBefore)"); c != 1 {
					t.Fatalf("NodeAtTx(T+500, pinBefore) cnt = %d, want 1 (the old belief survives)", c)
				}
				if old, err := g.Temporal().NodeAtTx(n.ID(), ivT+3000, pinBefore); err == nil && old != nil {
					t.Fatalf("NodeAtTx(T+3000, pinBefore) = %v; the extension leaked into the old belief", old)
				}
				// Node mirror of the RelAsOf note above.
				asOf, err := g.Temporal().NodeAsOf(n.ID(), pinAfter)
				if err != nil {
					t.Fatalf("NodeAsOf(pinAfter): %v", err)
				}
				cur, err := g.Nodes().Get(context.Background(), n.ID())
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if asOf.Version() != cur.Version() || ivCnt(t, asOf, "NodeAsOf(pinAfter)") != 1 {
					t.Fatalf("NodeAsOf(pinAfter) version %d != head row %d (cnt 1)", asOf.Version(), cur.Version())
				}
			})
		}
	}
}

// Queue-time refusals: nothing is queued, nothing is written, and the sentinel
// survives the facade (errors.Is). Node and relationship twins in one table.
//
// Catches: an interval check that lets validFrom >= validTo through to the
// applier (the group then fails late instead of at the door), validFrom == 0
// accepted (the cascade would anchor at the epoch), a zero or negative ID that
// reaches the store, a refused call that still leaves Pending > 0, and a
// returned error that is a bare string (errors.Is fails).
func TestSessionSetVersionInterval_QueueRefusals(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		from, to types.Instant
		node     types.NodeID
		rel      types.RelID
		want     error
	}{
		{"from==to", ivT, ivT, 1, 1, graphpkg.ErrInvalidTimeRange},
		{"from>to", ivT + 10, ivT, 1, 1, graphpkg.ErrInvalidTimeRange},
		{"from zero", 0, ivT, 1, 1, graphpkg.ErrInvalidTimeRange},
		{"zero id", ivT, ivT + 10, 0, 0, storepkg.ErrInvalidStoreMutation},
		{"negative id", ivT, ivT + 10, -5, -5, storepkg.ErrInvalidStoreMutation},
	}
	for _, b := range allStoreBackends() {
		for _, tc := range cases {
			t.Run(b.name+"/"+tc.name, func(t *testing.T) {
				g := b.open(t)
				s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
				if err != nil {
					t.Fatalf("NewSession: %v", err)
				}
				defer s.Close()
				if err := s.SetNodeVersionInterval(tc.node, tc.from, tc.to, map[string]any{"cnt": int64(2)}); !errors.Is(err, tc.want) {
					t.Fatalf("SetNodeVersionInterval err = %v, want %v", err, tc.want)
				}
				if err := s.SetRelVersionInterval(tc.rel, tc.from, tc.to, map[string]any{"cnt": int64(2)}); !errors.Is(err, tc.want) {
					t.Fatalf("SetRelVersionInterval err = %v, want %v", err, tc.want)
				}
				if got := s.Pending(); got != 0 {
					t.Fatalf("Pending = %d after refused calls, want 0", got)
				}
			})
		}
	}
}

// A closed or nil session refuses; a refused door writes nothing.
//
// Catches: a door that skips lockOpen (writes through a closed session or
// panics on a nil receiver).
func TestSessionSetVersionInterval_ClosedAndNilSession(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			r := ivNewRel(t, g)
			n := ivNewNode(t, g)
			s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := s.SetRelVersionInterval(r.ID(), ivT, ivT+5000, nil); !errors.Is(err, graphpkg.ErrIngestClosed) {
				t.Fatalf("closed SetRelVersionInterval = %v, want ErrIngestClosed", err)
			}
			if err := s.SetNodeVersionInterval(n.ID(), ivT, ivT+5000, nil); !errors.Is(err, graphpkg.ErrIngestClosed) {
				t.Fatalf("closed SetNodeVersionInterval = %v, want ErrIngestClosed", err)
			}
			var nilS *ingest.Session
			if err := nilS.SetRelVersionInterval(r.ID(), ivT, ivT+5000, nil); !errors.Is(err, graphpkg.ErrNilSession) {
				t.Fatalf("nil SetRelVersionInterval = %v, want ErrNilSession", err)
			}
			if err := nilS.SetNodeVersionInterval(n.ID(), ivT, ivT+5000, nil); !errors.Is(err, graphpkg.ErrNilSession) {
				t.Fatalf("nil SetNodeVersionInterval = %v, want ErrNilSession", err)
			}
			if h, err := g.Rels().History(r.ID()); err != nil || len(h) != 0 {
				t.Fatalf("rel history after refused doors = %d, %v; want 0 (History holds appended rows only)", len(h), err)
			}
			if h, err := g.Nodes().History(n.ID()); err != nil || len(h) != 0 {
				t.Fatalf("node history after refused doors = %d, %v; want 0 (History holds appended rows only)", len(h), err)
			}
		})
	}
}

// Failed-group shape: an apply-time failure (unknown id) fails ITS group's
// outcome with the real sentinel, a sibling group in the same session applies,
// and the failing group leaves no row behind.
//
// Catches: an applier that attributes the failure to every group (the sibling
// gets the error), swallows it (WaitApplied nil for the unknown id), or applies
// the failing group's other effects.
func TestSessionSetVersionInterval_FailedGroupShape(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		for _, m := range ivModes {
			t.Run(b.name+"/"+m.name, func(t *testing.T) {
				g := b.open(t)
				r := ivNewRel(t, g)
				n := ivNewNode(t, g)
				missingRel, missingNode := types.RelID(1<<40), types.NodeID(1<<40)

				s, err := g.Ingest().NewSession(m.opts)
				if err != nil {
					t.Fatalf("NewSession: %v", err)
				}
				defer s.Close()
				if err := s.SetRelVersionInterval(missingRel, ivT, ivT+5000, map[string]any{"cnt": int64(2)}); err != nil {
					t.Fatalf("queue unknown rel: %v", err)
				}
				badRelErr := ivSubmit(g, s)
				if !errors.Is(badRelErr, graphpkg.ErrRelNotFound) {
					t.Fatalf("unknown rel outcome = %v, want ErrRelNotFound", badRelErr)
				}
				if err := s.SetNodeVersionInterval(missingNode, ivT, ivT+5000, nil); err != nil {
					t.Fatalf("queue unknown node: %v", err)
				}
				badNodeErr := ivSubmit(g, s)
				if !errors.Is(badNodeErr, graphpkg.ErrNodeNotFound) {
					t.Fatalf("unknown node outcome = %v, want ErrNodeNotFound", badNodeErr)
				}
				if err := s.SetRelVersionInterval(r.ID(), ivT, ivT+5000, map[string]any{"cnt": int64(2)}); err != nil {
					t.Fatalf("queue good rel: %v", err)
				}
				if err := s.SetNodeVersionInterval(n.ID(), ivT, ivT+5000, map[string]any{"cnt": int64(2)}); err != nil {
					t.Fatalf("queue good node: %v", err)
				}
				if err := ivSubmit(g, s); err != nil {
					t.Fatalf("good group outcome = %v, want nil (earlier failure must not leak)", err)
				}
				if h, err := g.Rels().History(r.ID()); err != nil || len(h) < 1 {
					t.Fatalf("good rel history = %d, %v; want >= 1", len(h), err)
				}
				if h, err := g.Nodes().History(n.ID()); err != nil || len(h) < 1 {
					t.Fatalf("good node history = %d, %v; want >= 1", len(h), err)
				}
				if _, err := g.Rels().Get(context.Background(), missingRel); !errors.Is(err, graphpkg.ErrRelNotFound) {
					t.Fatalf("the failing group created a rel: %v", err)
				}
			})
		}
	}
}

// nil props is a valid correction (the cascade keeps the covering version's
// properties); the interval still grows and the value is the old one.
//
// Catches: a nil map dereferenced at queue or apply, and a nil-props cascade
// that drops the properties of the row it extends.
func TestSessionSetVersionInterval_NilProps(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			r := ivNewRel(t, g)
			n := ivNewNode(t, g)
			s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			if err := s.SetRelVersionInterval(r.ID(), ivT, ivT+5000, nil); err != nil {
				t.Fatalf("SetRelVersionInterval(nil props): %v", err)
			}
			if err := s.SetNodeVersionInterval(n.ID(), ivT, ivT+5000, nil); err != nil {
				t.Fatalf("SetNodeVersionInterval(nil props): %v", err)
			}
			if err := ivSubmit(g, s); err != nil {
				t.Fatalf("Submit: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			gr, err := g.Temporal().RelAt(r.ID(), ivT+3000)
			if err != nil {
				t.Fatalf("RelAt(T+3000): %v", err)
			}
			if c := ivCnt(t, gr, "RelAt(T+3000)"); c != 1 {
				t.Fatalf("RelAt(T+3000) cnt = %d, want 1 (nil props keep the old value)", c)
			}
			gn, err := g.Temporal().NodeAt(n.ID(), ivT+3000)
			if err != nil {
				t.Fatalf("NodeAt(T+3000): %v", err)
			}
			if c := ivCnt(t, gn, "NodeAt(T+3000)"); c != 1 {
				t.Fatalf("NodeAt(T+3000) cnt = %d, want 1 (nil props keep the old value)", c)
			}
		})
	}
}

// The caller's props map is copied at queue time.
//
// Catches: a door that queues the caller's map by reference (mutating it
// after the call changes the applied value).
func TestSessionSetVersionInterval_PropsCopiedAtQueue(t *testing.T) {
	t.Parallel()
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			r := ivNewRel(t, g)
			n := ivNewNode(t, g)
			s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			props := map[string]any{"cnt": int64(2)}
			if err := s.SetRelVersionInterval(r.ID(), ivT, ivT+5000, props); err != nil {
				t.Fatalf("SetRelVersionInterval: %v", err)
			}
			if err := s.SetNodeVersionInterval(n.ID(), ivT, ivT+5000, props); err != nil {
				t.Fatalf("SetNodeVersionInterval: %v", err)
			}
			props["cnt"] = int64(99)
			if err := ivSubmit(g, s); err != nil {
				t.Fatalf("Submit: %v", err)
			}
			_ = s.Close()
			gr, err := g.Temporal().RelAt(r.ID(), ivT+3000)
			if err != nil {
				t.Fatalf("RelAt: %v", err)
			}
			if c := ivCnt(t, gr, "RelAt"); c != 2 {
				t.Fatalf("rel cnt = %d, want 2 (queue-time copy)", c)
			}
			gn, err := g.Temporal().NodeAt(n.ID(), ivT+3000)
			if err != nil {
				t.Fatalf("NodeAt: %v", err)
			}
			if c := ivCnt(t, gn, "NodeAt"); c != 2 {
				t.Fatalf("node cnt = %d, want 2 (queue-time copy)", c)
			}
		})
	}
}
