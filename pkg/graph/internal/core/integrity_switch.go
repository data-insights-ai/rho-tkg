package core

import (
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Config.DisableIntegrityHashes (backlog 61). Every write path hashes through
// nodeContentHash / relContentHash, so the switch is one branch here: off
// computes nothing and the rows carry empty hashes, which no encoder writes
// (wire_encode.go omits an empty hash; the compact frozen form holds none).

// nodeContentHash is the content hash a node row carries: "" with hashes off.
func (c *Core) nodeContentHash(n *types.Node, labels []string) (string, error) {
	if c.hashesOff {
		return "", nil
	}
	return integrity.ComputeNodeHashChecked(n, labels)
}

// relContentHash is the content hash a relationship row carries: "" with
// hashes off.
func (c *Core) relContentHash(r *types.Relationship, typeName string) (string, error) {
	if c.hashesOff {
		return "", nil
	}
	return integrity.ComputeRelHashChecked(r, typeName)
}

// nodeHashSuffix is integrity.PrecomputeNodeHashSuffixChecked behind the
// switch (nil with hashes off); nodeHashFromSuffix completes it ("" off).
func (c *Core) nodeHashSuffix(labels []string, props types.PropertySlice) ([]byte, error) {
	if c.hashesOff {
		return nil, nil
	}
	return integrity.PrecomputeNodeHashSuffixChecked(labels, props)
}

func (c *Core) nodeHashFromSuffix(id types.NodeID, version uint32, suffix []byte) string {
	if c.hashesOff {
		return ""
	}
	return integrity.ComputeNodeHashFromSuffix(id, version, suffix)
}

// integrityHashesMeta is the MetaKV key persisting the setting a store was
// written under: 1 = hashes on, 0 = off.
const integrityHashesMeta = "integrity_hashes"

// resolveIntegrityMode checks the configured setting against the one the
// store was written under; persistIntegrityMode writes it once New has passed
// every other check, so a failed New persists nothing. Rule: a store with
// rows keeps its setting — the configured one must equal it, otherwise New
// fails with ErrIntegrityModeMismatch (fail closed: a store never holds hashed
// and unhashed rows side by side). The setting of a store with rows is its
// key, or, without the key (rows written before the setting existed, or a key
// lost in a power cut), what its rows show: a row with a hash is an on-store,
// a row without one an off-store. A store without rows takes the configured
// setting, also over a key, and so does a store without stable storage (never
// reopened). A store without MetaKV (a custom backend) has
// nothing to persist the setting in and follows the configuration.
func (c *Core) resolveIntegrityMode() error {
	mk := c.metaKV
	if mk == nil {
		return nil
	}
	v, err := mk.MetaGet(integrityHashesMeta)
	if err != nil {
		return fmt.Errorf("graph: read integrity-hash setting: %w", err)
	}
	wantOn := !c.hashesOff
	switch {
	case len(v) == 1 && v[0] <= 1:
		if v[0] == 1 == wantOn {
			return nil
		}
	case len(v) != 0:
		return fmt.Errorf("graph: integrity-hash setting is %d bytes: %w", len(v), storepkg.ErrCorruptWire)
	}
	// No key, or a key naming the other setting: the rows decide. A store
	// without stable storage (memory, BadgerInMemory) is never reopened, so it
	// has no earlier setting: it takes the configured one without a probe.
	if df, ok := c.store.(storepkg.DurableFlushCapability); !ok || !df.DurableFlushSupported() {
		c.integrityModeStamp = true
		return nil
	}
	hashed, found, err := c.sampleRowHashed()
	if err != nil {
		return fmt.Errorf("graph: integrity-hash setting: %w", err)
	}
	switch {
	case !found:
		c.integrityModeStamp = true // an empty store takes the configured setting
	case len(v) == 1:
		return integrityModeMismatch(v[0] == 1)
	case hashed != wantOn:
		return integrityModeMismatch(hashed)
	default:
		c.integrityModeStamp = true
	}
	return nil
}

// persistIntegrityMode writes the setting resolveIntegrityMode decided to
// stamp, durably (store.MetaSetDurable where the backend has it): rows of a
// tiered or sharded store land on other shards than the key, so a power cut
// must not keep the rows and lose the key. A read-only store cannot stamp;
// nothing will be written under the setting.
func (c *Core) persistIntegrityMode() error {
	if !c.integrityModeStamp {
		return nil
	}
	if err := c.writeIntegrityMode(); err != nil {
		if errors.Is(err, storepkg.ErrInvalidStoreMutation) {
			return nil
		}
		return fmt.Errorf("graph: persist integrity-hash setting: %w", err)
	}
	return nil
}

// writeIntegrityMode persists this graph's setting.
func (c *Core) writeIntegrityMode() error {
	b := []byte{1}
	if c.hashesOff {
		b[0] = 0
	}
	if d, ok := c.metaKV.(interface {
		MetaSetDurable(key string, value []byte) error
	}); ok {
		return d.MetaSetDurable(integrityHashesMeta, b)
	}
	return c.metaKV.MetaSet(integrityHashesMeta, b)
}

// restoreIntegrityModeAfterReset re-persists the setting after Admin.Reset()
// or a replica's ChangeClear apply wiped the MetaKV keyspace —
// integrityHashesMeta's reapPolicyPreserve half (BACKLOG 13l). The graph keeps
// writing under its setting after the Clear; without the key, a reopen would
// read its rows as rows written before the setting existed (hashes on). Like
// restoreInstantFloorAfterReset it needs no capture: the authoritative value
// is c.hashesOff. Caller must hold c.mu.Lock.
func (c *Core) restoreIntegrityModeAfterReset() error {
	if c.metaKV == nil {
		return nil
	}
	if err := c.writeIntegrityMode(); err != nil {
		return fmt.Errorf("graph: restore integrity-hash setting after reset: %w", err)
	}
	return nil
}

func integrityModeMismatch(storedOn bool) error {
	if storedOn {
		return fmt.Errorf("graph: the store holds integrity hashes, Config.DisableIntegrityHashes is set: %w", ErrIntegrityModeMismatch)
	}
	return fmt.Errorf("graph: the store was written with Config.DisableIntegrityHashes, it is not set now: %w", ErrIntegrityModeMismatch)
}

// rowHashEvidence / relHashEvidence read a sampled row: with a hash, an
// on-store; without one (no integrity metadata at all on a row decoded from an
// off-store), an off-store.
func rowHashEvidence(ig *types.NodeIntegrity) (hashed, found bool, err error) {
	return ig != nil && ig.Hash != "", true, nil
}

func relHashEvidence(ig *types.RelIntegrity) (hashed, found bool, err error) {
	return ig != nil && ig.Hash != "", true, nil
}

// sampleRowHashed reads the first row the store holds (a current node, a
// current relationship, then a history row) and reports whether it carries a
// content hash; found is false for a store without rows. Every row of a store
// is written under one setting, so one row decides.
func (c *Core) sampleRowHashed() (hashed, found bool, err error) {
	var nid types.NodeID
	if err := c.store.ForEachNodeID(func(id types.NodeID) bool { nid = id; return false }); err != nil {
		return false, false, err
	}
	if nid != 0 {
		n, err := c.store.GetNode(nid)
		if err != nil {
			return false, false, err
		}
		return rowHashEvidence(n.Integrity())
	}
	var rid types.RelID
	if err := c.store.ForEachRelID(func(id types.RelID) bool { rid = id; return false }); err != nil {
		return false, false, err
	}
	if rid != 0 {
		r, err := c.store.GetRelationship(rid)
		if err != nil {
			return false, false, err
		}
		return relHashEvidence(r.Integrity())
	}
	if err := c.store.ForEachNodeHistoryID(func(id types.NodeID) bool { nid = id; return false }); err != nil {
		return false, false, err
	}
	if nid != 0 {
		h, err := c.store.GetNodeHistory(nid)
		if err != nil || len(h) == 0 {
			return false, err == nil && len(h) > 0, err
		}
		return rowHashEvidence(h[0].Integrity())
	}
	if err := c.store.ForEachRelHistoryID(func(id types.RelID) bool { rid = id; return false }); err != nil {
		return false, false, err
	}
	if rid != 0 {
		h, err := c.store.GetRelHistory(rid)
		if err != nil || len(h) == 0 {
			return false, err == nil && len(h) > 0, err
		}
		return relHashEvidence(h[0].Integrity())
	}
	return false, false, nil
}

// admitImportedNodeHash is the hash gate of every import and replica-apply
// row. Hashes on: the row must carry its content hash and the hash must match
// (BACKLOG 12d, verifyImportedNodeHash). Hashes off: a hash the row carries is
// still verified (a corrupt record is refused either way), then every hash is
// dropped so the off-store holds none.
//
// The record's wire is stripped with the row, so an idempotent redelivery
// (nodeWireMatches / relWireMatches) compares like with like.
func (c *Core) admitImportedNodeHash(n *types.Node, w *storeutil.NodeWire, kind string) error {
	id := w.ID
	if !c.hashesOff {
		return c.verifyImportedNodeHash(n, id, kind)
	}
	w.Hash, w.PrevHash = "", ""
	ig := n.Integrity()
	if ig == nil {
		return nil
	}
	if ig.Hash != "" {
		if err := c.verifyImportedNodeHash(n, id, kind); err != nil {
			return err
		}
	}
	if ig.Hash != "" || ig.PrevHash != "" {
		ig.Hash, ig.PrevHash = "", ""
		n.SetIntegrity(ig)
	}
	return nil
}

// admitImportedRelHash is admitImportedNodeHash for relationships.
func (c *Core) admitImportedRelHash(r *types.Relationship, w *storeutil.RelWire, kind string) error {
	id := w.ID
	if !c.hashesOff {
		return c.verifyImportedRelHash(r, id, kind)
	}
	w.Hash, w.PrevHash, w.FromNodeHash, w.ToNodeHash = "", "", "", ""
	ig := r.Integrity()
	if ig == nil {
		return nil
	}
	if ig.Hash != "" {
		if err := c.verifyImportedRelHash(r, id, kind); err != nil {
			return err
		}
	}
	if ig.Hash != "" || ig.PrevHash != "" || ig.FromNodeHash != "" || ig.ToNodeHash != "" {
		ig.Hash, ig.PrevHash, ig.FromNodeHash, ig.ToNodeHash = "", "", "", ""
		r.SetIntegrity(ig)
	}
	return nil
}
