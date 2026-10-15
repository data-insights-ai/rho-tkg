package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// MemberTimeStats is the answer of Stats().LabelTimeStats /
// RelTypeTimeStats (round 8 request 3): what a reader needs to know before
// treating a label's (type's) rows as eternal, without scanning them.
type MemberTimeStats struct {
	// Members is the label's (type's) current member count.
	Members int64
	// ExplicitValid counts the current members whose row carries an explicit
	// valid time: a tkg_valid_from or a tkg_valid_to (a closed version
	// included). A row written without either has none.
	ExplicitValid int64
	// LastEnded is the latest transaction instant at which a row of an entity
	// whose rows ever carried the label (type) was ended: superseded, closed,
	// retracted or deleted (the largest TxTo or DeletedAt over the history
	// rows of those entities, whichever label the row carried). 0: none.
	LastEnded types.Instant
}

// HistoryAfter reports whether a row of a member entity was ended after pin,
// so a read pinned at pin may see a version that is not the entity's current
// row. False means every member's row visible at pin is its current row (or
// none, for an entity first written after pin).
func (s MemberTimeStats) HistoryAfter(pin types.Instant) bool { return s.LastEnded > pin }

// Eternal reports that no current member carries explicit valid time and no
// member row was ever ended: every member is its one current row, without a
// valid-time bound of its own.
func (s MemberTimeStats) Eternal() bool { return s.ExplicitValid == 0 && s.LastEnded == 0 }
