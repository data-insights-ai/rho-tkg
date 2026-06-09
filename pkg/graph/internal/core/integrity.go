package core

import (
	"errors"
	"reflect"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const hashVerifyHistoryVersionBatchSize = 4096

// VerifyNodeChain verifies the full hash chain for a node.
// Returns (true, nil) if the chain is valid. Returns (false, nil) if a hash
// mismatch or broken PrevHash link is detected. Returns (false, err) on I/O
// failure or if the node never existed (no current entity AND no history).
//
// VerifyNodeChain deleted entities: if the current node is gone (storepkg.ErrNodeNotFound) but
// history exists, verifies the history chain alone. Labels are extracted from
// each history entry's internal tokens.
func (h *HashOps) VerifyNodeChain(id types.NodeID) (bool, error) {
	c := h.c
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return false, err
	}
	valid := false
	err := c.readUnderRLock(func() error {
		var err error
		valid, err = c.verifyNodeChainLocked(id)
		return err
	})
	return valid, err
}

func (c *Core) verifyNodeChainLocked(id types.NodeID) (bool, error) {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return false, err
	}
	current, err := c.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return false, err
	}
	// current may be nil for deleted entities.

	if pager := hashVerifyHistoryVersionPageCapability(c.store); pager != nil {
		return c.verifyNodeChainPagedLocked(id, current, pager)
	}

	history, err := c.getNodeHistory(id)
	if err != nil {
		return false, err
	}

	return c.verifyNodeChainRowsLocked(current, history)
}

func (c *Core) verifyNodeChainRowsLocked(current *types.Node, history []*types.Node) (bool, error) {
	if current == nil && len(history) == 0 {
		return false, storepkg.ErrNodeNotFound
	}

	var prev *types.Node
	for _, entry := range history {
		valid, err := c.verifyNodeChainEntryLocked(entry, prev)
		if err != nil || !valid {
			return valid, err
		}
		prev = entry
	}
	if current != nil {
		return c.verifyNodeChainEntryLocked(current, prev)
	}
	return true, nil
}

func (c *Core) verifyNodeChainPagedLocked(id types.NodeID, current *types.Node, pager storepkg.HistoryVersionPageCapability) (bool, error) {
	var (
		prev        *types.Node
		seenHistory bool
		start       uint32
	)
	for {
		history, err := c.nodeHistoryVersionsFrom(pager, id, start, hashVerifyHistoryVersionBatchSize)
		if err != nil {
			return false, err
		}
		if len(history) == 0 {
			break
		}
		for _, entry := range history {
			valid, err := c.verifyNodeChainEntryLocked(entry, prev)
			if err != nil || !valid {
				return valid, err
			}
			prev = entry
			seenHistory = true
		}
		if len(history) < hashVerifyHistoryVersionBatchSize {
			break
		}
		lastVersion := history[len(history)-1].Version()
		if lastVersion == ^uint32(0) {
			break
		}
		start = lastVersion + 1
	}
	if current == nil && !seenHistory {
		return false, storepkg.ErrNodeNotFound
	}
	if current != nil {
		return c.verifyNodeChainEntryLocked(current, prev)
	}
	return true, nil
}

func (c *Core) verifyNodeChainEntryLocked(entry, prev *types.Node) (bool, error) {
	if entry == nil {
		return false, ErrNilNode
	}
	ig := entry.Integrity()
	if ig == nil {
		return false, nil
	}

	if entry.Version() == 0 {
		// Genesis: PrevHash must be empty.
		if ig.PrevHash != "" {
			return false, nil
		}
	} else if prev != nil {
		// Non-genesis with predecessor in chain: verify PrevHash link.
		prevIG := prev.Integrity()
		if prevIG == nil {
			return false, nil
		}
		if ig.PrevHash != prevIG.Hash {
			return false, nil
		}
	}
	// else: first visible entry has version != 0, so history was truncated;
	// skip the missing predecessor link and still verify content integrity.

	labels := c.nodeLabelsUnlocked(entry)
	computed, err := integrity.ComputeNodeHashChecked(entry, labels)
	if err != nil {
		return false, err
	}
	if ig.Hash != computed {
		return false, nil
	}
	return true, nil
}

// VerifyRelChain verifies the full hash chain for a relationship.
// Returns (true, nil) if the chain is valid. Returns (false, nil) if a hash
// mismatch or broken PrevHash link is detected. Returns (false, err) on I/O
// failure or if the relationship never existed (no current AND no history).
//
// VerifyRelChain deleted entities: if the current relationship is gone (storepkg.ErrRelNotFound)
// but history exists, verifies the history chain alone.
func (h *HashOps) VerifyRelChain(id types.RelID) (bool, error) {
	c := h.c
	if err := c.checkOpen(); err != nil {
		return false, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return false, err
	}
	valid := false
	err := c.readUnderRLock(func() error {
		var err error
		valid, err = c.verifyRelChainLocked(id)
		return err
	})
	return valid, err
}

func (c *Core) verifyRelChainLocked(id types.RelID) (bool, error) {
	if err := storepkg.ValidateRelID(id); err != nil {
		return false, err
	}
	current, err := c.getCurrentRelationship(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return false, err
	}
	// current may be nil for deleted entities.

	if pager := hashVerifyHistoryVersionPageCapability(c.store); pager != nil {
		return c.verifyRelChainPagedLocked(id, current, pager)
	}

	history, err := c.getRelHistory(id)
	if err != nil {
		return false, err
	}

	return c.verifyRelChainRowsLocked(current, history)
}

func (c *Core) verifyRelChainRowsLocked(current *types.Relationship, history []*types.Relationship) (bool, error) {
	if current == nil && len(history) == 0 {
		return false, storepkg.ErrRelNotFound
	}

	// Extract type name from the best available source.
	typeSource := current
	if typeSource == nil {
		typeSource = history[len(history)-1]
	}
	if typeSource == nil {
		return false, ErrNilRelationship
	}
	typeName := c.relTypeUnlocked(typeSource)

	var prev *types.Relationship
	for _, entry := range history {
		valid, err := verifyRelChainEntry(entry, prev, typeName)
		if err != nil || !valid {
			return valid, err
		}
		prev = entry
	}
	if current != nil {
		return verifyRelChainEntry(current, prev, typeName)
	}
	return true, nil
}

func (c *Core) verifyRelChainPagedLocked(id types.RelID, current *types.Relationship, pager storepkg.HistoryVersionPageCapability) (bool, error) {
	var (
		prev          *types.Relationship
		seenHistory   bool
		start         uint32
		typeName      string
		typeNameReady bool
	)
	if current != nil {
		typeName = c.relTypeUnlocked(current)
		typeNameReady = true
	}

	for {
		history, err := c.relHistoryVersionsFrom(pager, id, start, hashVerifyHistoryVersionBatchSize)
		if err != nil {
			return false, err
		}
		if len(history) == 0 {
			break
		}
		if !typeNameReady {
			if history[0] == nil {
				return false, ErrNilRelationship
			}
			typeName = c.relTypeUnlocked(history[0])
			typeNameReady = true
		}
		for _, entry := range history {
			valid, err := verifyRelChainEntry(entry, prev, typeName)
			if err != nil || !valid {
				return valid, err
			}
			prev = entry
			seenHistory = true
		}
		if len(history) < hashVerifyHistoryVersionBatchSize {
			break
		}
		lastVersion := history[len(history)-1].Version()
		if lastVersion == ^uint32(0) {
			break
		}
		start = lastVersion + 1
	}
	if current == nil && !seenHistory {
		return false, storepkg.ErrRelNotFound
	}
	if current != nil {
		return verifyRelChainEntry(current, prev, typeName)
	}
	return true, nil
}

func verifyRelChainEntry(entry, prev *types.Relationship, typeName string) (bool, error) {
	if entry == nil {
		return false, ErrNilRelationship
	}
	ig := entry.Integrity()
	if ig == nil {
		return false, nil
	}

	if entry.Version() == 0 {
		// Genesis: PrevHash must be empty.
		if ig.PrevHash != "" {
			return false, nil
		}
	} else if prev != nil {
		// Non-genesis with predecessor in chain: verify PrevHash link.
		prevIG := prev.Integrity()
		if prevIG == nil {
			return false, nil
		}
		if ig.PrevHash != prevIG.Hash {
			return false, nil
		}
	}
	// else: first visible entry has version != 0, so history was truncated.

	computed, err := integrity.ComputeRelHashChecked(entry, typeName)
	if err != nil {
		return false, err
	}
	if ig.Hash != computed {
		return false, nil
	}
	return true, nil
}

func hashVerifyHistoryVersionPageCapability(store storepkg.MandatoryStore) storepkg.HistoryVersionPageCapability {
	pager := historyVersionPageCapability(store)
	if pager == nil || isExactNativeStore(store) {
		return pager
	}
	typ := reflect.TypeOf(store)
	if typeDeclaresMethod(typ, "GetNodeHistory") || typeDeclaresMethod(typ, "GetRelHistory") {
		return nil
	}
	return pager
}
