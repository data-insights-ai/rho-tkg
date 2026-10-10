package core

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// rtDeleteQueue is the delete surface a Batch and an ingest Session share.
type rtDeleteQueue interface {
	DeleteNode(types.NodeID) error
	RetractNode(types.NodeID) error
	DeleteRelationship(types.RelID) error
	RetractRelationship(types.RelID) error
}

// TestRetraction_UnitKeepsQueueOrder (review finding F2): a Retract and a
// plain Delete of the same entity in one unit apply in queue order, so the
// FIRST one writes the tombstone and the second fails on the deleted entity.
// It catches a unit that runs every plain relationship delete before the
// retracting ones (Retract; Delete left a plain tombstone), node and rel,
// Batch and both ingest modes, both orders.
func TestRetraction_UnitKeepsQueueOrder(t *testing.T) {
	t.Parallel()
	units := []struct {
		name string
		do   func(t *testing.T, g *Core, q func(rtDeleteQueue) error) error
	}{
		{"batch", func(t *testing.T, g *Core, q func(rtDeleteQueue) error) error {
			return txbBatchDo(t, g, func(b *BatchBuilder) error { return q(b) })
		}},
		{"ingestSync", func(t *testing.T, g *Core, q func(rtDeleteQueue) error) error {
			return txbIngestDo(t, g, IngestOptions{Sync: true}, func(s *Session) error { return q(s) })
		}},
		{"ingestConcurrent", func(t *testing.T, g *Core, q func(rtDeleteQueue) error) error {
			return txbIngestDo(t, g, IngestOptions{Concurrent: true}, func(s *Session) error { return q(s) })
		}},
	}
	for _, u := range units {
		for _, kind := range []string{"node", "rel"} {
			for _, retractFirst := range []bool{true, false} {
				g := txbBackends()[0].open(t, true)
				w := buildRetractWorld(t, g)
				q := func(d rtDeleteQueue) error {
					ops := []func() error{
						func() error { return d.RetractRelationship(w.rxy) },
						func() error { return d.DeleteRelationship(w.rxy) },
					}
					if kind == "node" {
						ops = []func() error{
							func() error { return d.RetractNode(w.y) },
							func() error { return d.DeleteNode(w.y) },
						}
					}
					if !retractFirst {
						ops[0], ops[1] = ops[1], ops[0]
					}
					for _, op := range ops {
						if err := op(); err != nil {
							return err
						}
					}
					return nil
				}
				err := u.do(t, g, q)
				tm := newestRelRow(t, g, w.rxy).Temporal()
				if kind == "node" {
					tm = newestNodeRow(t, g, w.y).Temporal()
				}
				if tm == nil || tm.DeletedAt == 0 || tm.Retracted != retractFirst || err == nil {
					t.Errorf("%s/%s retractFirst=%v: tombstone %+v, unit err=%v; want Retracted=%v and the second op refused",
						u.name, kind, retractFirst, tm, err, retractFirst)
				}
			}
		}
	}
}
