package core

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/vmihailenco/msgpack/v5"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const exactErasureDigestDomain = "rho-tkg/exact-erasure/v1"

// ExactErasureBounds makes relationship-closure resolution finite.
type ExactErasureBounds = storepkg.ExactErasureBounds

// ExactErasureRequest is the complete caller-declared graph scope. The graph
// canonicalizes ordering and duplicates. ResolveExactErasure expands
// RelationshipIDs with identities found in current or temporal relationship
// versions; ExactErase then independently rechecks the declared closure.
type ExactErasureRequest struct {
	NodeIDs         []types.NodeID
	RelationshipIDs []types.RelID
	Bounds          ExactErasureBounds
}

// ExactErasureRelationshipBinding is one current or historical relationship
// version touching the request. It exposes only structural identity needed by
// the semantic owner to classify an endpoint before sealing a deletion plan.
type ExactErasureRelationshipBinding struct {
	RelationshipID types.RelID
	Type           string
	StartNodeID    types.NodeID
	EndNodeID      types.NodeID
	Version        uint32
	IntegrityHash  string
}

// ExactErasureResolution separates the canonical deletion request from the
// historical endpoints it discovered. EndpointNodeIDs are evidence for
// ownership classification, never an implicit request to delete those nodes.
type ExactErasureResolution struct {
	Request              ExactErasureRequest
	EndpointNodeIDs      []types.NodeID
	RelationshipBindings []ExactErasureRelationshipBinding
}

// ExactErasureReceipt is content-addressed by the canonical request. It is
// intentionally independent of live-row removal counts, so the first run and
// every idempotent retry return the same receipt.
type ExactErasureReceipt struct {
	Digest            string
	NodeCount         int
	RelationshipCount int
}

// ResolveExactErasure returns the canonical, reference-closed plan for one
// exact erasure. It performs no mutation. Callers must persist the returned
// request and pass it unchanged to ExactErase so retries keep a stable receipt.
func (a *AdminOps) ResolveExactErasure(
	ctx context.Context,
	request ExactErasureRequest,
) (ExactErasureResolution, error) {
	var zero ExactErasureResolution
	c := a.c
	if err := c.checkOpen(); err != nil {
		return zero, err
	}
	if ctx == nil {
		return zero, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if !c.allowExactErasure {
		return zero, ErrExactErasureDisabled
	}
	if c.exactErasure == nil {
		return zero, fmt.Errorf("graph: exact erasure: %w", storepkg.ErrCapabilityNotSupported)
	}

	nodes, rels, _, err := canonicalExactErasureRequest(request)
	if err != nil {
		return zero, err
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return zero, ErrGraphClosed
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if len(nodes) != 0 {
		closure, closureErr := c.exactErasure.ExactErasureRelationshipClosure(
			storepkg.ExactErasureClosureRequest{
				NodeIDs: nodes,
				Bounds:  request.Bounds,
			},
		)
		if closureErr != nil {
			return zero, closureErr
		}
		rels = append(rels, closure.RelationshipIDs...)
		sort.Slice(rels, func(i, j int) bool { return rels[i] < rels[j] })
		rels = dedupRelIDs(rels)
		if len(rels) > request.Bounds.MaxRelationshipIdentities {
			return zero, storepkg.ErrExactErasureClosureLimit
		}
		bindings := make([]ExactErasureRelationshipBinding, 0, len(closure.Bindings))
		for _, binding := range closure.Bindings {
			typeName := c.relTypes.Resolve(binding.TypeToken)
			if typeName == "" {
				return zero, fmt.Errorf(
					"graph: exact erasure relationship %d has unresolved type token %d: %w",
					binding.RelationshipID, binding.TypeToken,
					storepkg.ErrInvalidStoreMutation,
				)
			}
			bindings = append(bindings, ExactErasureRelationshipBinding{
				RelationshipID: binding.RelationshipID,
				Type:           typeName,
				StartNodeID:    binding.StartNodeID,
				EndNodeID:      binding.EndNodeID,
				Version:        binding.Version,
				IntegrityHash:  binding.IntegrityHash,
			})
		}
		return ExactErasureResolution{
			Request: ExactErasureRequest{
				NodeIDs: nodes, RelationshipIDs: rels, Bounds: request.Bounds,
			},
			EndpointNodeIDs:      append([]types.NodeID(nil), closure.EndpointNodeIDs...),
			RelationshipBindings: bindings,
		}, nil
	}
	return ExactErasureResolution{
		Request: ExactErasureRequest{
			NodeIDs: nodes, RelationshipIDs: rels, Bounds: request.Bounds,
		},
	}, nil
}

// ExactErase performs one bounded legal-erasure operation. It takes the same
// graph-wide exclusion lock as Reset and transactions, while the backend
// capability provides its own atomicity against direct Store callers.
func (a *AdminOps) ExactErase(ctx context.Context, request ExactErasureRequest) (ExactErasureReceipt, error) {
	var zero ExactErasureReceipt
	c := a.c
	if err := c.checkOpen(); err != nil {
		return zero, err
	}
	if ctx == nil {
		return zero, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if err := c.checkWritable(); err != nil {
		return zero, err
	}
	if !c.allowExactErasure {
		return zero, ErrExactErasureDisabled
	}

	nodes, rels, receipt, err := canonicalExactErasureRequest(request)
	if err != nil {
		return zero, err
	}
	if c.exactErasure == nil {
		return zero, fmt.Errorf("graph: exact erasure: %w", storepkg.ErrCapabilityNotSupported)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return zero, ErrGraphClosed
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	// UniqueForever values and compaction stubs retain erased IDs and, for
	// UniqueForever, the original indexed value key. Prepare their replacement
	// while holding uniqueMu and send it through the backend's SAME atomic batch.
	c.uniqueMu.Lock()
	defer c.uniqueMu.Unlock()
	nextOwners := make(map[string]types.NodeID, len(c.uniqueOwners))
	erasedNodes := make(map[types.NodeID]struct{}, len(nodes))
	for _, id := range nodes {
		erasedNodes[id] = struct{}{}
	}
	ownersChanged := false
	for key, owner := range c.uniqueOwners {
		if _, erased := erasedNodes[owner]; erased {
			ownersChanged = true
			continue
		}
		nextOwners[key] = owner
	}

	metaWrites := make([]storepkg.MetaWrite, 0, len(nodes)+len(rels)+1)
	if ownersChanged {
		encoded, encodeErr := c.encodeForeverOwners(nextOwners)
		if encodeErr != nil {
			return zero, encodeErr
		}
		metaWrites = append(metaWrites, storepkg.MetaWrite{Key: uniqueForeverOwnersMeta, Value: encoded})
	}
	for _, id := range nodes {
		metaWrites = append(metaWrites, storepkg.MetaWrite{Key: compactStubNodeKey(id)})
	}
	for _, id := range rels {
		metaWrites = append(metaWrites, storepkg.MetaWrite{Key: compactStubRelKey(id)})
	}

	_, err = c.exactErasure.ExactErase(storepkg.ExactErasureRequest{
		NodeIDs:    nodes,
		RelIDs:     rels,
		Bounds:     request.Bounds,
		MetaWrites: metaWrites,
	})
	if err != nil {
		return zero, err
	}
	if ownersChanged {
		c.uniqueOwners = nextOwners
	}
	c.asOfColumns.clear()
	return receipt, nil
}

func canonicalExactErasureRequest(request ExactErasureRequest) ([]types.NodeID, []types.RelID, ExactErasureReceipt, error) {
	nodes := append([]types.NodeID(nil), request.NodeIDs...)
	rels := append([]types.RelID(nil), request.RelationshipIDs...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	sort.Slice(rels, func(i, j int) bool { return rels[i] < rels[j] })
	nodes = dedupNodeIDs(nodes)
	rels = dedupRelIDs(rels)
	if len(nodes) == 0 && len(rels) == 0 {
		return nil, nil, ExactErasureReceipt{}, ErrInvalidExactErasureRequest
	}
	for _, id := range nodes {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, nil, ExactErasureReceipt{}, fmt.Errorf("%w: %v", ErrInvalidExactErasureRequest, err)
		}
	}
	for _, id := range rels {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, nil, ExactErasureReceipt{}, fmt.Errorf("%w: %v", ErrInvalidExactErasureRequest, err)
		}
	}
	if len(nodes) != 0 &&
		(request.Bounds.MaxRelationshipIdentities <= 0 ||
			request.Bounds.MaxRelationshipVersions <= 0 ||
			request.Bounds.MaxEndpointNodeIdentities <= 0 ||
			len(rels) > request.Bounds.MaxRelationshipIdentities) {
		return nil, nil, ExactErasureReceipt{}, ErrInvalidExactErasureRequest
	}

	h := sha256.New()
	_, _ = h.Write([]byte(exactErasureDigestDomain))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(len(nodes)))
	_, _ = h.Write(buf[:])
	for _, id := range nodes {
		binary.BigEndian.PutUint64(buf[:], uint64(id.SnowflakeID()))
		_, _ = h.Write(buf[:])
	}
	binary.BigEndian.PutUint64(buf[:], uint64(len(rels)))
	_, _ = h.Write(buf[:])
	for _, id := range rels {
		binary.BigEndian.PutUint64(buf[:], uint64(id.SnowflakeID()))
		_, _ = h.Write(buf[:])
	}
	return nodes, rels, ExactErasureReceipt{
		Digest:            hex.EncodeToString(h.Sum(nil)),
		NodeCount:         len(nodes),
		RelationshipCount: len(rels),
	}, nil
}

func dedupNodeIDs(ids []types.NodeID) []types.NodeID {
	out := ids[:0]
	for _, id := range ids {
		if len(out) == 0 || out[len(out)-1] != id {
			out = append(out, id)
		}
	}
	return out
}

func dedupRelIDs(ids []types.RelID) []types.RelID {
	out := ids[:0]
	for _, id := range ids {
		if len(out) == 0 || out[len(out)-1] != id {
			out = append(out, id)
		}
	}
	return out
}

func (c *Core) encodeForeverOwners(owners map[string]types.NodeID) ([]byte, error) {
	recs := make([]foreverOwnerRecord, 0, len(owners))
	for key, owner := range owners {
		labelTok, propKey, valueKey, ok := parseForeverOwnerKey(key)
		if !ok {
			continue
		}
		recs = append(recs, foreverOwnerRecord{
			Label:    c.labels.Resolve(labelTok),
			PropKey:  propKey,
			ValueKey: valueKey,
			Owner:    int64(owner),
		})
	}
	blob := foreverOwnersBlob{SelfHash: hashForeverOwners(recs), Owners: recs}
	b, err := msgpack.Marshal(blob)
	if err != nil {
		return nil, fmt.Errorf("graph: encode unique-forever owners: %w", err)
	}
	return b, nil
}
