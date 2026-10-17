package core

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The per-entity point doors with explicit presence (round 11 item 3):
// NodeAtTx / RelAtTx and their batched twins read a txAt of 0 as "no pin",
// so a pin at the instant 0 could not be named. NodeAtOpts, RelAtOpts,
// NodesAtOptsByIDs and RelsAtOptsByIDs take the state point form of
// store.QueryOpts instead, read through its Has* helpers like every generic
// door: the valid instant (ValidAt, else now, as the generic doors' TxAt-only
// form) and the transaction pin (TxAt: a present pin at 0 is the instant 0,
// absent is none). They run the named doors' gates and bodies, which are the
// generic doors' per-entity point form (findNodeVersionForOpts), so for one
// entity they answer what ByLabel / ByType with the same opts answers.

// atOpts is the state point form of opts: its valid instant and its pin as
// the named doors' body reads it (txAtOf: a present 0 is -1, which reads the
// same rows as the instant 0; absent is 0, no pin). Any other filter (a TxPin,
// a window) or a page (Limit, After), a depth or IncludeEclipsed is
// ErrConflictingTemporalOpts: these doors are the point form only (the record
// door is NodeAsOf / RelAsOf, windows are the generic doors').
func (c *Core) atOpts(opts storepkg.QueryOpts) (validAt, txAt types.Instant, err error) {
	if err := validateTemporalQueryOpts(opts); err != nil {
		return 0, 0, err
	}
	if opts.HasTxPin() || opts.HasValidInterval() || (opts.HasValidBound() && !opts.HasValidAt()) ||
		opts.Limit != 0 || opts.After != 0 || opts.Depth != storepkg.DepthAll || opts.IncludeEclipsed {
		return 0, 0, fmt.Errorf("%w: a per-entity point door takes ValidAt and TxAt only", ErrConflictingTemporalOpts)
	}
	validAt = c.readNow()
	if opts.HasValidAt() {
		validAt = opts.ValidAt
	}
	return validAt, txAtOf(opts), nil
}

// NodeAtOpts returns node id's version valid at opts' valid instant as known
// at opts' pin, with explicit presence (see the file comment).
func (t *TempOps) NodeAtOpts(id types.NodeID, opts storepkg.QueryOpts) (*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	validAt, txAt, err := c.atOpts(opts)
	if err != nil {
		return nil, err
	}
	return t.NodeAtTx(id, validAt, txAt)
}

// RelAtOpts is NodeAtOpts for relationships (RelAtTx's gates and body).
func (t *TempOps) RelAtOpts(id types.RelID, opts storepkg.QueryOpts) (*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	validAt, txAt, err := c.atOpts(opts)
	if err != nil {
		return nil, err
	}
	return t.RelAtTx(id, validAt, txAt)
}

// NodesAtOptsByIDs is NodeAtOpts for a set of IDs, laid out as
// NodesAtTxByIDs (aligned to ids, nil where no version is valid); a pin above
// the commit clock is ErrTxPinTooNew.
func (t *TempOps) NodesAtOptsByIDs(ids []types.NodeID, opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	validAt, txAt, err := c.atOpts(opts)
	if err != nil {
		return nil, err
	}
	if err := c.checkPinNotTooNew(txAt); err != nil {
		return nil, err
	}
	return c.nodesAtTxByIDs(ids, validAt, txAt)
}

// RelsAtOptsByIDs is NodesAtOptsByIDs for relationships.
func (t *TempOps) RelsAtOptsByIDs(ids []types.RelID, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
	}
	validAt, txAt, err := c.atOpts(opts)
	if err != nil {
		return nil, err
	}
	if err := c.checkPinNotTooNew(txAt); err != nil {
		return nil, err
	}
	return c.relsAtTxByIDs(ids, validAt, txAt)
}

// checkPinNotTooNew refuses a transaction-time pin above the commit clock
// (ErrTxPinTooNew), the batched doors' rule; 0 (no pin) and pins at or below
// 0 pass.
func (c *Core) checkPinNotTooNew(pin types.Instant) error {
	if now := c.peekNow(); pin > now {
		return fmt.Errorf("%w: pin %d, commit clock %d", ErrTxPinTooNew, pin, now)
	}
	return nil
}
