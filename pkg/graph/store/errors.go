package store

import "errors"

// Sentinel errors for store operations.
var (
	ErrNodeNotFound          = errors.New("graph: node not found")
	ErrRelNotFound           = errors.New("graph: relationship not found")
	ErrNodeExists            = errors.New("graph: node already exists")
	ErrRelExists             = errors.New("graph: relationship already exists")
	ErrVersionNotFound       = errors.New("graph: version not found")
	ErrNoVersionValidAt      = errors.New("graph: no version valid at the given time")
	ErrIndexExists           = errors.New("graph: property index already exists")
	ErrIndexNotFound         = errors.New("graph: property index not found")
	ErrTemporalIndexExists   = errors.New("graph: temporal index already exists")
	ErrTemporalIndexNotFound = errors.New("graph: temporal index not found")
	ErrTxDone                = errors.New("graph: transaction already committed or rolled back")
	ErrStoreClosed           = errors.New("graph: store already closed")
)
