package core

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// changeFeedOrErr returns the store's change-feed capability or a wrapped
// ErrCapabilityNotSupported when the backend does not provide one (e.g. tiered,
// or a store opened without the change-log enabled does provide the methods but
// returns an empty feed; only backends lacking the capability entirely error).
func (r *ReplOps) changeFeedOrErr() (storepkg.ChangeFeedCapability, error) {
	if r == nil || r.c == nil || r.c.changeFeed == nil {
		return nil, fmt.Errorf("graph: change feed: %w", storepkg.ErrCapabilityNotSupported)
	}
	return r.c.changeFeed, nil
}

// ChangeFeed returns up to limit committed change-log records with LSN >
// afterLSN, in ascending LSN order (limit <= 0 = all).
func (r *ReplOps) ChangeFeed(afterLSN uint64, limit int) ([]storepkg.ChangeRecord, error) {
	cf, err := r.changeFeedOrErr()
	if err != nil {
		return nil, err
	}
	return cf.ChangeFeed(afterLSN, limit)
}

// ForEachChange streams committed change-log records with LSN > afterLSN in
// ascending order, stopping early when fn returns false.
func (r *ReplOps) ForEachChange(afterLSN uint64, fn func(storepkg.ChangeRecord) bool) error {
	cf, err := r.changeFeedOrErr()
	if err != nil {
		return err
	}
	return cf.ForEachChange(afterLSN, fn)
}

// LastCommittedLSN returns the highest durably-committed change-log LSN, or 0
// when the log is empty. It is the watermark a session reads after a write to
// drive read-your-writes routing against a read replica.
func (r *ReplOps) LastCommittedLSN() (uint64, error) {
	cf, err := r.changeFeedOrErr()
	if err != nil {
		return 0, err
	}
	return cf.LastCommittedLSN()
}
