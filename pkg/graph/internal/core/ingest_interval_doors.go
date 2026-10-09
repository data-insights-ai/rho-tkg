package core

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// Session interval doors: the ingest-session twins of
// Temporal().SetNodeVersionInterval / SetRelVersionInterval and of the
// BatchBuilder queue doors they forward to. A burst fact that grows from
// [vs, ve) to [vs, ve') is expressed through the session as an append-only
// correction — fresh rows stamped TxFrom = now, existing rows untouched
// (lesson 46) — so reads pinned before the apply (*AtTx, *AsOf) still see the
// old belief.
//
// The session queues the cascade on its BatchBuilder; the applier runs it
// through cascadeNodeVersionInterval / cascadeRelVersionInterval, the same
// kernel the standalone and GraphTx doors use: after the creates and updates
// of the group, before its deletes, in strong mode (Batch.Execute) and in
// concurrent mode (applyIngestGroupConcurrent) alike. Each cascade is its own
// entity-lock-bounded write; an apply-time failure (unknown id, a refused
// interval) fails the group that queued it with the real sentinel and leaves
// that group's other ops to commit (partial success, as the Batch door).
//
// Unique constraints: the kernel judges the props patch before any row is
// built (enforceUniqueForCascade, unique_cascade.go), so a refused patch fails
// its own op with ErrUniqueViolation, appends nothing, and the group's other
// ops commit, as an UpdateNode violation does.

// SetNodeVersionInterval accumulates a valid-time correction for node id over
// [validFrom, validTo) (validTo == 0 means open-ended). props is a PATCH over
// the state believed valid at each instant of the interval (nil keeps every
// property; a nil value deletes a key); see TempOps.SetNodeVersionInterval for
// the full semantics. The map is copied at queue time. Queue-time refusals,
// with nothing queued: ErrInvalidTimeRange (validFrom == 0, or validTo != 0
// and validFrom >= validTo) and an ErrInvalidStoreMutation-family id error
// (zero or negative id). An unknown id surfaces at apply (ErrNodeNotFound).
// Note that Get keeps returning the head row: the appended correction rows
// appear in History and in *AtTx reads.
func (s *Session) SetNodeVersionInterval(id types.NodeID, validFrom, validTo types.Instant, props map[string]any) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.SetNodeVersionInterval(id, validFrom, validTo, props)
}

// SetRelVersionInterval is the relationship counterpart of
// Session.SetNodeVersionInterval (unknown id: ErrRelNotFound at apply).
func (s *Session) SetRelVersionInterval(id types.RelID, validFrom, validTo types.Instant, props map[string]any) error {
	if err := s.lockOpen(); err != nil {
		return err
	}
	defer s.mu.Unlock()
	return s.b.SetRelVersionInterval(id, validFrom, validTo, props)
}
