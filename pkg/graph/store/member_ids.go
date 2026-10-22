package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// MemberIDsCapability is OPTIONAL: the current members of a label or a
// relationship type, listed from the store's index without reading their rows
// (backlog 33: the candidate gather of a temporal label/type read, which only
// needs the IDs, decoded every current row before each candidate's own read
// decoded it again). NodeIDsByLabel(tok) / RelIDsByType(tok) return, ascending,
// the IDs NodesByLabel(tok, QueryOpts{}) / RelationshipsByType(tok,
// QueryOpts{}) return, pending writes included; because no row is read, an
// index entry whose row is unreadable is listed (the row read that follows
// reports it). Token 0 fails with ErrInvalidStoreMutation, a closed store
// with ErrStoreClosed. Badger reads its RAM label/type index (the on-disk
// label index in label-on-disk mode), sharded merges its slots, tiered merges
// the reference shard, the archive and every event shard (the DepthAll
// members, each once). Memory holds decoded rows and declines, and a consumer
// reads the rows. A store whose label and type reads honour QueryOpts.Depth
// (one offering DepthHistoryIterationCapability) answers for DepthAll: the
// core asks it only for DepthAll reads and reads the rows of a depth-scoped
// one.
type MemberIDsCapability interface {
	NodeIDsByLabel(labelToken uint16) ([]types.NodeID, error)
	RelIDsByType(typeToken uint16) ([]types.RelID, error)
}
