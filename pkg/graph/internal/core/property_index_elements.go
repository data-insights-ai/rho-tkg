package core

import (
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Elements property indexes (backlog 63, storepkg.PropertyIndexOptions.Elements):
// a lookup on such an index means "the list contains the value". Every door
// that answers an equality lookup on (label|type, key) asks these once and
// passes the flag to types.Node/Relationship.HasPropertyIndexKey, so the
// current read, the row validation, the scan fallback and the history re-check
// of the temporal doors all apply the same match; the candidate ids come from
// the store's index and its per-element membership sidecar.

// nodeIndexElements reports whether the node property index on (tok, key) is
// an Elements index. A store without PropertyIndexOptionsCapability has none.
func (c *Core) nodeIndexElements(tok uint16, key string) (bool, error) {
	oc, ok := c.store.(storepkg.PropertyIndexOptionsCapability)
	if !ok {
		return false, nil
	}
	o, found, err := oc.PropertyIndexOptions(tok, key)
	if err != nil {
		return false, err
	}
	return found && o.Elements, nil
}

// relIndexElements is nodeIndexElements for a relationship property index.
func (c *Core) relIndexElements(tok uint16, key string) (bool, error) {
	oc, ok := c.store.(storepkg.RelPropertyIndexOptionsCapability)
	if !ok {
		return false, nil
	}
	o, found, err := oc.RelPropertyIndexOptions(tok, key)
	if err != nil {
		return false, err
	}
	return found && o.Elements, nil
}

// refuseNodeElementsIndex returns ErrElementsIndexUnsupported when (tok, key)
// has an Elements index: what names the operation that needs value equality
// (a unique constraint, GetOrCreateByKey, a multi-key lookup).
func (c *Core) refuseNodeElementsIndex(tok uint16, key, what string) error {
	elements, err := c.nodeIndexElements(tok, key)
	if err != nil {
		return err
	}
	if elements {
		return fmt.Errorf("%w: %s on key %q", storepkg.ErrElementsIndexUnsupported, what, key)
	}
	return nil
}
