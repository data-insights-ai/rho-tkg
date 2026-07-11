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

	// Load the compaction stub (if any). A corrupt/forged stub fails closed
	// here — Verify* is a tamper-evidence gate.
	stub, err := c.loadNodeCompactionStubPtr(id)
	if err != nil {
		return false, err
	}

	if pager := hashVerifyHistoryVersionPageCapability(c.store); pager != nil {
		return c.verifyNodeChainPagedLocked(id, current, pager, stub)
	}

	history, err := c.getNodeHistory(id)
	if err != nil {
		return false, err
	}

	return c.verifyNodeChainRowsLocked(current, history, stub)
}

// loadNodeCompactionStubPtr adapts loadNodeCompactionStub to a nil-able pointer
// for the verify seam.
func (c *Core) loadNodeCompactionStubPtr(id types.NodeID) (*compactionStub, error) {
	s, ok, err := c.loadNodeCompactionStub(id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return &s, nil
}

// loadRelCompactionStubPtr is the relationship mirror of
// loadNodeCompactionStubPtr.
func (c *Core) loadRelCompactionStubPtr(id types.RelID) (*compactionStub, error) {
	s, ok, err := c.loadRelCompactionStub(id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return &s, nil
}

// chainEntryMeta is the per-version integrity summary the linkage check operates
// on, decoupled from the node/relationship row type.
type chainEntryMeta struct {
	version  uint32
	hash     string
	prevHash string
}

// verifyChainLinkage validates the integrity hash linkage across a version set.
//
// The chain is a DAG, not a linear list. A bitemporal correction
// (SetNodeVersionInterval / SetRelVersionInterval) appends a version whose
// PrevHash points to whichever version it supersedes ON THE VALID-TIME AXIS, not
// to the immediately higher version number (see temporal_cascade.go: "joins the
// chain via PrevHash from whichever row it directly supersedes on the VT axis").
// So linkage is "every non-genesis version's PrevHash equals the Hash of SOME
// version in the set" — a real predecessor exists — rather than the stricter (and
// for corrected data, WRONG) "== the version-order predecessor". The genesis
// version (version 0) must carry an empty PrevHash; the lowest retained version
// may have a dangling PrevHash (its predecessor was truncated away), tolerated to
// match the pre-DAG behavior. Per-version CONTENT hashes are verified separately
// by the caller — that is the tamper-evidence on content; this checks structure.
//
// stub-aware (ADR-0001): when a compaction stub exists, the lowest retained
// version is NOT allowed a dangling PrevHash — it must link to the stub's
// LastTrimmedHash (the stub is a VIRTUAL PREDECESSOR). This turns the boundary
// back into tamper-evidence after a trim: a forged truncation that drops the
// oldest versions without a matching stub, or with a stub whose LastTrimmedHash
// does not equal the oldest kept row's PrevHash, fails closed. A stub covering a
// chain that still holds its genesis (version 0) is contradictory (nothing below
// genesis can have been trimmed) and also fails.
func verifyChainLinkage(metas []chainEntryMeta, stub *compactionStub) bool {
	hashSet := make(map[string]struct{}, len(metas))
	minVersion := ^uint32(0)
	for _, m := range metas {
		hashSet[m.hash] = struct{}{}
		if m.version < minVersion {
			minVersion = m.version
		}
	}
	for _, m := range metas {
		if m.version == 0 {
			// Genesis must anchor with an empty PrevHash. A stub claims a trimmed
			// predecessor, which cannot coexist with a retained genesis.
			if stub != nil {
				return false
			}
			if m.prevHash != "" {
				return false
			}
			continue
		}
		if m.version == minVersion {
			if stub != nil {
				// Virtual predecessor: the oldest kept version must link to the
				// stub's recorded LastTrimmedHash.
				if m.prevHash != stub.LastTrimmedHash {
					return false
				}
				continue
			}
			// No stub: predecessor may have been truncated (legacy leniency).
			continue
		}
		if m.prevHash == "" {
			return false
		}
		if _, ok := hashSet[m.prevHash]; !ok {
			return false
		}
	}
	return true
}

func (c *Core) verifyNodeChainRowsLocked(current *types.Node, history []*types.Node, stub *compactionStub) (bool, error) {
	if current == nil && len(history) == 0 {
		return false, storepkg.ErrNodeNotFound
	}

	metas := make([]chainEntryMeta, 0, len(history)+1)
	for _, entry := range history {
		m, ok, err := c.verifyNodeChainEntryContentLocked(entry)
		if err != nil || !ok {
			return ok, err
		}
		metas = append(metas, m)
	}
	if current != nil {
		m, ok, err := c.verifyNodeChainEntryContentLocked(current)
		if err != nil || !ok {
			return ok, err
		}
		metas = append(metas, m)
	}
	return verifyChainLinkage(metas, stub), nil
}

func (c *Core) verifyNodeChainPagedLocked(id types.NodeID, current *types.Node, pager storepkg.HistoryVersionPageCapability, stub *compactionStub) (bool, error) {
	var (
		metas       = make([]chainEntryMeta, 0)
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
			m, ok, err := c.verifyNodeChainEntryContentLocked(entry)
			if err != nil || !ok {
				return ok, err
			}
			metas = append(metas, m)
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
		m, ok, err := c.verifyNodeChainEntryContentLocked(current)
		if err != nil || !ok {
			return ok, err
		}
		metas = append(metas, m)
	}
	return verifyChainLinkage(metas, stub), nil
}

// verifyNodeChainEntryContentLocked recomputes a node version's content hash and
// compares it with the stored hash (the tamper-evidence check), returning the
// entry's integrity meta for the separate linkage pass.
func (c *Core) verifyNodeChainEntryContentLocked(entry *types.Node) (chainEntryMeta, bool, error) {
	if entry == nil {
		return chainEntryMeta{}, false, ErrNilNode
	}
	ig := entry.Integrity()
	if ig == nil {
		return chainEntryMeta{}, false, nil
	}
	labels := c.nodeLabelsUnlocked(entry)
	computed, err := integrity.ComputeNodeHashChecked(entry, labels)
	if err != nil {
		return chainEntryMeta{}, false, err
	}
	if ig.Hash != computed {
		return chainEntryMeta{}, false, nil
	}
	return chainEntryMeta{version: entry.Version(), hash: ig.Hash, prevHash: ig.PrevHash}, true, nil
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

	stub, err := c.loadRelCompactionStubPtr(id)
	if err != nil {
		return false, err
	}

	if pager := hashVerifyHistoryVersionPageCapability(c.store); pager != nil {
		return c.verifyRelChainPagedLocked(id, current, pager, stub)
	}

	history, err := c.getRelHistory(id)
	if err != nil {
		return false, err
	}

	return c.verifyRelChainRowsLocked(current, history, stub)
}

func (c *Core) verifyRelChainRowsLocked(current *types.Relationship, history []*types.Relationship, stub *compactionStub) (bool, error) {
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

	metas := make([]chainEntryMeta, 0, len(history)+1)
	for _, entry := range history {
		m, ok, err := verifyRelChainEntryContent(entry, typeName)
		if err != nil || !ok {
			return ok, err
		}
		metas = append(metas, m)
	}
	if current != nil {
		m, ok, err := verifyRelChainEntryContent(current, typeName)
		if err != nil || !ok {
			return ok, err
		}
		metas = append(metas, m)
	}
	return verifyChainLinkage(metas, stub), nil
}

func (c *Core) verifyRelChainPagedLocked(id types.RelID, current *types.Relationship, pager storepkg.HistoryVersionPageCapability, stub *compactionStub) (bool, error) {
	var (
		metas         = make([]chainEntryMeta, 0)
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
			m, ok, err := verifyRelChainEntryContent(entry, typeName)
			if err != nil || !ok {
				return ok, err
			}
			metas = append(metas, m)
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
		m, ok, err := verifyRelChainEntryContent(current, typeName)
		if err != nil || !ok {
			return ok, err
		}
		metas = append(metas, m)
	}
	return verifyChainLinkage(metas, stub), nil
}

// verifyRelChainEntryContent is the relationship counterpart of
// verifyNodeChainEntryContentLocked.
func verifyRelChainEntryContent(entry *types.Relationship, typeName string) (chainEntryMeta, bool, error) {
	if entry == nil {
		return chainEntryMeta{}, false, ErrNilRelationship
	}
	ig := entry.Integrity()
	if ig == nil {
		return chainEntryMeta{}, false, nil
	}
	computed, err := integrity.ComputeRelHashChecked(entry, typeName)
	if err != nil {
		return chainEntryMeta{}, false, err
	}
	if ig.Hash != computed {
		return chainEntryMeta{}, false, nil
	}
	return chainEntryMeta{version: entry.Version(), hash: ig.Hash, prevHash: ig.PrevHash}, true, nil
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
