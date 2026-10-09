package temporal

import "errors"

// Scope errors identify malformed bounds, missing placement and improper intervals.
var (
	ErrInvalidBound     = errors.New("temporal: invalid bound")
	ErrInvalidScope     = errors.New("temporal: invalid scope")
	ErrUnplacedScope    = errors.New("temporal: unplaced scope has no set support")
	ErrImproperInterval = errors.New("temporal: Allen classification requires finite proper intervals")
)
