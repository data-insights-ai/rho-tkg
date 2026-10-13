package core

import (
	"reflect"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Undoing a relationship create (a failed create's partial write, a rolled-back
// transaction's or a failed import's created relationship) removes a
// relationship no reader may find, so it goes through the store's
// UndoRelationshipCreate doors, which record no ex-adjacency (backlog 50); a
// store without them is undone by DeleteRelationship (sound: an extra
// candidate per endpoint).

// undoRelationshipCreate removes id whose create is undone; token routes the
// change-log record as deleteRelationshipScopedAware does.
func (c *Core) undoRelationshipCreate(token uint64, id types.RelID) error {
	if token != 0 && c.scopedChangeLog != nil {
		if c.scopedRelCreateUndo != nil {
			return c.scopedRelCreateUndo.UndoRelationshipCreateScoped(id, token)
		}
		return c.scopedChangeLog.DeleteRelationshipScoped(id, token)
	}
	if c.relCreateUndo != nil {
		return c.relCreateUndo.UndoRelationshipCreate(id)
	}
	return c.store.DeleteRelationship(id)
}

// relCreateUndoCapabilities resolves both undo doors. A wrapper that only
// inherits them from an embedded native store does not get them: its own
// DeleteRelationship (a fault or policy hook) stays the undo door.
func relCreateUndoCapabilities(store storepkg.MandatoryStore) (storepkg.RelCreateUndoCapability, storepkg.ScopedRelCreateUndoCapability) {
	plain, _ := store.(storepkg.RelCreateUndoCapability)
	scoped, _ := store.(storepkg.ScopedRelCreateUndoCapability)
	if isExactNativeStore(store) {
		return plain, scoped
	}
	if plain != nil && embedsNativeCapability(store, reflect.TypeOf((*storepkg.RelCreateUndoCapability)(nil)).Elem(), "UndoRelationshipCreate") {
		plain = nil
	}
	if scoped != nil && embedsNativeCapability(store, reflect.TypeOf((*storepkg.ScopedRelCreateUndoCapability)(nil)).Elem(), "UndoRelationshipCreateScoped") {
		scoped = nil
	}
	return plain, scoped
}
