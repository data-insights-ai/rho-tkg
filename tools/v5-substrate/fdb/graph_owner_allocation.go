package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

var errAllocationConsumed = errors.New("fdb-owner: installation already consumed; recover receipt, not a new cursor")

type graphOwnerAllocationState struct {
	Next, Grants uint64
	Exhausted    bool
}
type graphOwnerAllocationGrant struct {
	Version               uint8
	Graph, Request, Range string
	SecretHash            [32]byte
	First, Last, Count    uint64
	Bucket                uint8
	Home, Recipient       graphOwnerRoute
}
type graphOwnerAllocationSession struct {
	Version        uint8
	Grant, Request string
	Secret         [32]byte
	Route          graphOwnerRoute
}
type graphOwnerAllocationSlot struct {
	Grant    string
	Session  *graphOwnerAllocationSession `json:",omitempty"`
	State    uint8                        // 0 reserved; 1 installed; 2 closed/burned, never reusable.
	Consumed uint64
}
type graphOwnerAllocatedID struct {
	ID, Offset     uint64
	Grant, Session string
}
type graphOwnerIssuedID struct {
	Claim                graphOwnerAllocatedID
	Coordinator          uint8
	Transaction, Request string
	Digest               [32]byte
}

func graphOwnerAllocatedInput(input []graphOwnerAllocatedID) ([]graphOwnerAllocatedID, error) {
	if len(input) > 8 {
		return nil, errLimit
	}
	if input == nil {
		return nil, nil
	}
	for i, id := range input {
		if id.ID == 0 || !validName(id.Grant) || !validName(id.Session) || i > 0 && input[i-1].ID >= id.ID {
			return nil, errInvalid
		}
	}
	owned := slices.Clone(input)
	for i := range owned {
		owned[i].Grant = strings.Clone(owned[i].Grant)
		owned[i].Session = strings.Clone(owned[i].Session)
	}
	return owned, nil
}

func ownerCommandIDs(x transaction) ([]uint64, error) {
	if x.GraphChange == nil || x.Lifecycle == nil {
		return nil, errInvalid
	}
	g := x.GraphChange
	ids := []uint64{}
	if g.Kind == "node-create" {
		ids = append(ids, g.Node.Node, g.Node.Life)
	} else {
		if g.Kind != "edge-create" && g.Kind != "edge-update" || x.Lifecycle.Definition == nil {
			return nil, errInvalid
		}
		ids = append(ids, g.Edge.ID, x.Lifecycle.EdgeLife, g.Edge.Source.Node, g.Edge.Source.Life, g.Edge.Target.Node, g.Edge.Target.Life, x.Lifecycle.Definition.ID)
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	if len(ids) > 8 || ids[0] == 0 {
		return nil, errInvalid
	}
	return ids, nil
}

func (c *graphOwnerCatalog) allocatedIn(tr fdb.ReadTransaction, x transaction, intent graphOwnerIntent, state graphOwnerState) error {
	if c.binding.Transfer == nil {
		if len(intent.Allocated) != 0 {
			return errInvalid
		}
		return nil
	}
	ids, err := ownerCommandIDs(x)
	if err != nil {
		return err
	}
	if len(ids) != len(intent.Allocated) {
		return errMismatch
	}
	for i, claim := range intent.Allocated {
		if claim.ID != ids[i] {
			return errMismatch
		}
		g, slot, err := c.grantIn(tr, claim.Grant)
		if err != nil {
			return err
		}
		if claim.Offset >= g.Count || claim.ID != g.First+claim.Offset || slot.Session == nil || slot.Session.Request != claim.Session {
			return errMismatch
		}
		// Already committed identities retain their ORIGINAL issuance lineage.
		// Unknown closed suffixes have no such authority and remain burned.
		prior, found, err := get[graphOwnerIssuedID](tr, c.key("allocation-issued", fmt.Sprintf("%016x", claim.ID)))
		if err != nil {
			return err
		}
		if found {
			if prior.Claim != claim || prior.Coordinator > 1 {
				return errMismatch
			}
			o, ok, err := get[outcome](tr, c.h.key(prior.Coordinator, "request", prior.Request))
			if err != nil {
				return err
			}
			id, matched, err := get[outcome](tr, c.h.key(prior.Coordinator, "id", prior.Transaction))
			if err != nil {
				return err
			}
			if !ok || !matched || o != id || !o.Commit || o.ID != prior.Transaction || o.Request != prior.Request || o.Digest != prior.Digest {
				return errUnavailable
			}
			original, exists, err := get[graphOwnerIntent](tr, c.key("intent-request", prior.Request))
			if err != nil {
				return err
			}
			if !exists || original.Digest != prior.Digest || !slices.Contains(original.Allocated, claim) {
				return errMismatch
			}
			tx, err := decode[transaction](original.Body)
			if err != nil {
				return err
			}
			proof, err := c.h.graphCostParallelOriginalIn(tr, tx, original.Body)
			if err != nil {
				return err
			}
			if proof.Outcome != o {
				return errMismatch
			}
			continue
		}
		if slot.State == 1 {
			if g.Recipient != state.Routes[g.Bucket] {
				return errStale
			}
		} else if slot.State != 2 || claim.Offset >= slot.Consumed {
			return errAllocationConsumed
		}
	}
	return nil
}

// The existing business commit publishes first-use provenance atomically with
// its real originals. This is not a separate per-ID allocator RPC/counter.
func (c *graphOwnerCatalog) recordIssuedIn(tr fdb.Transaction, x transaction, intent graphOwnerIntent) error {
	if c.binding.Transfer == nil {
		return nil
	}
	for _, claim := range intent.Allocated {
		key := c.key("allocation-issued", fmt.Sprintf("%016x", claim.ID))
		prior, found, err := get[graphOwnerIssuedID](tr, key)
		if err != nil {
			return err
		}
		if found {
			if prior.Claim != claim {
				return errMismatch
			}
			continue
		}
		if err := c.createIn(tr, key, graphOwnerIssuedID{claim, x.Coordinator, x.ID, x.Request, intent.Digest}); err != nil {
			return err
		}
	}
	return nil
}

// Bounded native range lookup, for constructing a later factory from retained
// issued identities without an O(facts) map. Lower bound is checked explicitly.
func (c *graphOwnerClient) AllocatedID(ctx context.Context, id uint64) (graphOwnerAllocatedID, error) {
	if c == nil || c.catalog == nil || id == 0 {
		return graphOwnerAllocatedID{}, errInvalid
	}
	ctx, cancel, err := c.catalog.operationContext(ctx)
	if err != nil {
		return graphOwnerAllocatedID{}, err
	}
	defer cancel()
	r, _, err := graphCostParallelRead(ctx, c.catalog.h.db, func(tr fdb.ReadTransaction) (graphOwnerAllocatedID, error) {
		actual, err := c.catalog.h.ownerCatalogIn(tr)
		if err != nil {
			return graphOwnerAllocatedID{}, err
		}
		if actual.binding.Transfer == nil {
			return graphOwnerAllocatedID{}, errInvalid
		}
		if err := actual.authenticate(c.authority); err != nil {
			return graphOwnerAllocatedID{}, err
		}
		prefix := actual.key("allocation-index", "")
		end := bytes.Clone(prefix)
		end[len(end)-1]++
		rows, err := tr.GetRange(fdb.KeyRange{Begin: actual.key("allocation-index", fmt.Sprintf("%016x", id)), End: fdb.Key(end)}, fdb.RangeOptions{Limit: 1}).GetSliceWithError()
		if err != nil {
			return graphOwnerAllocatedID{}, err
		}
		if len(rows) != 1 {
			return graphOwnerAllocatedID{}, errUnavailable
		}
		g, err := decode[graphOwnerAllocationGrant](rows[0].Value)
		if err != nil {
			return graphOwnerAllocatedID{}, err
		}
		if !ownerGrantValid(g, actual) || id < g.First || id > g.Last || !bytes.Equal(rows[0].Key, actual.key("allocation-index", fmt.Sprintf("%016x", g.Last))) {
			return graphOwnerAllocatedID{}, errUnavailable
		}
		original, slot, err := actual.grantIn(tr, g.Request)
		if err != nil {
			return graphOwnerAllocatedID{}, err
		}
		if original != g || slot.Session == nil {
			return graphOwnerAllocatedID{}, errMismatch
		}
		claim := graphOwnerAllocatedID{id, id - g.First, g.Request, slot.Session.Request}
		if slot.State != 1 && (slot.State != 2 || claim.Offset >= slot.Consumed) {
			issued, found, err := get[graphOwnerIssuedID](tr, actual.key("allocation-issued", fmt.Sprintf("%016x", id)))
			if err != nil {
				return claim, err
			}
			if !found || issued.Claim != claim {
				return claim, errAllocationConsumed
			}
		}
		return claim, nil
	})
	return r, err
}

type graphOwnerAllocationReceipt struct {
	Grant   graphOwnerAllocationGrant
	Slot    graphOwnerAllocationSlot
	Records [2]graphCostStoredRecord
}

// This is an owned local iterator, NOT a current-owner authorization. Native
// factory/commit gates authenticate the returned provenance. Close stops local
// Next synchronously before attempting its genuine durable terminal update.
type graphOwnerAllocationCursor struct {
	mu      sync.Mutex
	client  *graphOwnerClient
	grant   graphOwnerAllocationGrant
	session graphOwnerAllocationSession
	used    uint64
	closed  bool
}

func (c *graphOwnerAllocationCursor) Next() (graphOwnerAllocatedID, error) {
	ids, err := c.take(1)
	return ids[0], err
}
func (c *graphOwnerAllocationCursor) take(count uint64) ([2]graphOwnerAllocatedID, error) {
	var ids [2]graphOwnerAllocatedID
	if c == nil || count < 1 || count > 2 {
		return ids, errInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ids, errStale
	}
	if c.used > c.grant.Count || count > c.grant.Count-c.used || c.used > math.MaxUint64-c.grant.First || count-1 > math.MaxUint64-(c.grant.First+c.used) {
		return ids, errLimit
	}
	for i := uint64(0); i < count; i++ {
		ids[i] = graphOwnerAllocatedID{c.grant.First + c.used + i, c.used + i, c.grant.Request, c.session.Request}
	}
	c.used += count
	return ids, nil
}
func (c *graphOwnerAllocationCursor) Close(ctx context.Context) (graphOwnerAllocationReceipt, error) {
	if c == nil {
		return graphOwnerAllocationReceipt{}, errInvalid
	}
	c.mu.Lock()
	c.closed = true
	used := c.used
	c.mu.Unlock()
	return c.client.closeAllocationSession(ctx, c.session, &used)
}

// Actual factory outputs use Next results; stable logical bucket IDs come from
// the HOME publication, never from numeric ID bits or processing-owner labels.
func (c *graphOwnerAllocationCursor) Node() (endpoint, [2]graphOwnerAllocatedID, error) {
	if c == nil {
		return endpoint{}, [2]graphOwnerAllocatedID{}, errInvalid
	}
	ids, err := c.take(2)
	if err != nil {
		return endpoint{}, ids, err
	}
	return endpoint{c.grant.Bucket, ids[0].ID, ids[1].ID}, ids, nil
}
func (c *graphOwnerAllocationCursor) Edge(source, target endpoint, v int64) (graphEdge, uint64, [2]graphOwnerAllocatedID, error) {
	if c == nil || c.grant.Bucket != 0 {
		return graphEdge{}, 0, [2]graphOwnerAllocatedID{}, errInvalid
	}
	ids, err := c.take(2)
	if err != nil {
		return graphEdge{}, 0, ids, err
	}
	return graphEdge{ids[0].ID, source, target, v}, ids[1].ID, ids, nil
}

func (c *graphOwnerClient) allocationIn(tr fdb.ReadTransaction, bucket uint8) (*graphOwnerCatalog, graphOwnerState, error) {
	if c == nil || c.catalog == nil || bucket > 1 {
		return nil, graphOwnerState{}, errInvalid
	}
	actual, err := c.catalog.h.ownerCatalogIn(tr)
	if err != nil {
		return nil, graphOwnerState{}, err
	}
	if actual.binding.Transfer == nil {
		return nil, graphOwnerState{}, errInvalid
	}
	if err := actual.authenticate(c.authority); err != nil {
		return nil, graphOwnerState{}, err
	}
	state, err := actual.stateIn(tr)
	if err != nil {
		return nil, state, err
	}
	if err := graphOwnerCurrent(state, c.authority, bucket, c.routes[bucket]); err != nil {
		return nil, state, err
	}
	return actual, state, nil
}
func ownerGrantValid(g graphOwnerAllocationGrant, c *graphOwnerCatalog) bool {
	return g.Version == 1 && g.Graph == c.h.config.Graph && validName(g.Request) && g.Range == "allocation-range-"+g.Request && validName(g.Range) && g.SecretHash != [32]byte{} && g.Bucket <= 1 && g.First > 0 && g.Last >= g.First && g.Last-g.First < c.binding.Transfer.MaxBlock && g.Count == g.Last-g.First+1 && validName(g.Home.Owner) && g.Home.Epoch > 0 && validName(g.Recipient.Owner) && g.Recipient.Epoch > 0
}
func (c *graphOwnerCatalog) grantIn(tr fdb.ReadTransaction, request string) (graphOwnerAllocationGrant, graphOwnerAllocationSlot, error) {
	g, found, err := get[graphOwnerAllocationGrant](tr, c.key("allocation-grant", request))
	if err != nil {
		return g, graphOwnerAllocationSlot{}, err
	}
	if !found || c.binding.Transfer == nil || !ownerGrantValid(g, c) {
		return g, graphOwnerAllocationSlot{}, errUnavailable
	}
	s, found, err := get[graphOwnerAllocationSlot](tr, c.key("allocation-slot", request))
	if err != nil {
		return g, s, err
	}
	if !found || s.Grant != request || s.State > 2 || s.Consumed > g.Count || s.State != 2 && s.Consumed != 0 || s.State == 1 && s.Session == nil || s.Session == nil && s.Consumed != 0 {
		return g, s, errMismatch
	}
	if s.Session != nil && (s.Session.Version != 1 || s.Session.Grant != request || !validName(s.Session.Request) || s.Session.Secret == [32]byte{} || s.Session.Route != g.Recipient) {
		return g, s, errMismatch
	}
	return g, s, nil
}
func (c *graphOwnerCatalog) allocationReceiptIn(tr fdb.ReadTransaction, request string) (graphOwnerAllocationReceipt, error) {
	g, s, err := c.grantIn(tr, request)
	if err != nil {
		return graphOwnerAllocationReceipt{}, err
	}
	r := graphOwnerAllocationReceipt{Grant: g, Slot: s}
	for i, key := range []fdb.Key{c.key("allocation-grant", request), c.key("allocation-slot", request)} {
		r.Records[i], err = snapshotStored(tr, key, graphOwnerRecordBytes)
		if err != nil {
			return graphOwnerAllocationReceipt{}, err
		}
	}
	return r, nil
}

// At most64 grant descriptors: complete native continuity/high-water proof,
// independent of a caller's reported counter. No per-object state is gathered.
func (c *graphOwnerCatalog) allocationStateIn(tr fdb.ReadTransaction) (graphOwnerAllocationState, error) {
	a, found, err := get[graphOwnerAllocationState](tr, c.key("allocation-state", "current"))
	if err != nil {
		return a, err
	}
	if !found || c.binding.Transfer == nil || a.Next == 0 || a.Grants > c.binding.Transfer.MaxGrants {
		return a, errUnavailable
	}
	prefix := c.key("allocation-index", "")
	end := bytes.Clone(prefix)
	end[len(end)-1]++
	rows, err := tr.GetRange(fdb.KeyRange{Begin: prefix, End: fdb.Key(end)}, fdb.RangeOptions{Limit: int(c.binding.Transfer.MaxGrants) + 1}).GetSliceWithError()
	if err != nil {
		return a, err
	}
	if uint64(len(rows)) != a.Grants {
		return a, errUnavailable
	}
	next, exhausted := uint64(1), false
	for _, row := range rows {
		g, err := decode[graphOwnerAllocationGrant](row.Value)
		if err != nil {
			return a, err
		}
		if exhausted || !ownerGrantValid(g, c) || g.First != next || !bytes.Equal(row.Key, c.key("allocation-index", fmt.Sprintf("%016x", g.Last))) {
			return a, errMismatch
		}
		original, _, err := c.grantIn(tr, g.Request)
		if err != nil {
			return a, err
		}
		if original != g {
			return a, errMismatch
		}
		if g.Last == math.MaxUint64 {
			next, exhausted = math.MaxUint64, true
		} else {
			next = g.Last + 1
		}
	}
	if next != a.Next || exhausted != a.Exhausted {
		return a, errMismatch
	}
	return a, nil
}

// One HOME commit reserves an inclusive graph-qualified range and its initial
// stable bucket placement. The native catalog gates even SAME reservation
// recovery. High-water never decreases; unknown unused ranges are not recycled.
func (c *graphOwnerClient) ReserveIDs(ctx context.Context, request string, secret [32]byte, count uint64, bucket uint8) (graphOwnerAllocationReceipt, error) {
	if c == nil || c.catalog == nil || !validName(request) || secret == [32]byte{} || bucket > 1 || count == 0 {
		return graphOwnerAllocationReceipt{}, errInvalid
	}
	ctx, cancel, err := c.catalog.operationContext(ctx)
	if err != nil {
		return graphOwnerAllocationReceipt{}, err
	}
	defer cancel()
	_, _, err = historyTransactContext(ctx, c.catalog.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		catalog, state, err := c.allocationIn(tr, 0)
		if err != nil {
			return false, err
		}
		if count > catalog.binding.Transfer.MaxBlock {
			return false, errLimit
		}
		old, found, err := get[graphOwnerAllocationGrant](tr, catalog.key("allocation-grant", request))
		if err != nil {
			return false, err
		}
		if found {
			if !ownerGrantValid(old, catalog) || old.Count != count || old.Bucket != bucket || !historySecret(old.SecretHash, sha256.Sum256(secret[:])) {
				return false, errMismatch
			}
			_, _, err := catalog.grantIn(tr, request)
			return true, err
		}
		if state.Moving != "" {
			return false, errOwnerBusy
		}
		a, err := catalog.allocationStateIn(tr)
		if err != nil {
			return false, err
		}
		if a.Exhausted || a.Grants == catalog.binding.Transfer.MaxGrants || count-1 > math.MaxUint64-a.Next {
			return false, errLimit
		}
		g := graphOwnerAllocationGrant{1, catalog.h.config.Graph, request, "allocation-range-" + request, sha256.Sum256(secret[:]), a.Next, a.Next + count - 1, count, bucket, state.Routes[0], state.Routes[bucket]}
		if !ownerGrantValid(g, catalog) {
			return false, errInvalid
		}
		a.Grants++
		if g.Last == math.MaxUint64 {
			a.Exhausted = true
		} else {
			a.Next = g.Last + 1
		}
		if err := catalog.createIn(tr, catalog.key("allocation-grant", request), g); err != nil {
			return false, err
		}
		if err := catalog.createIn(tr, catalog.key("allocation-slot", request), graphOwnerAllocationSlot{Grant: request}); err != nil {
			return false, err
		}
		if err := catalog.reserveCleanupIn(tr, "grant/"+request); err != nil {
			return false, err
		}
		// Ordered inclusive upper bound; lookup also checks the actual lower bound.
		if err := catalog.createIn(tr, catalog.key("allocation-index", fmt.Sprintf("%016x", g.Last)), g); err != nil {
			return false, err
		}
		return true, put(tr, catalog.key("allocation-state", "current"), a)
	})
	if err != nil {
		return graphOwnerAllocationReceipt{}, err
	}
	r, _, err := graphCostParallelRead(ctx, c.catalog.h.db, func(tr fdb.ReadTransaction) (graphOwnerAllocationReceipt, error) {
		catalog, _, err := c.allocationIn(tr, 0)
		if err != nil {
			return graphOwnerAllocationReceipt{}, err
		}
		return catalog.allocationReceiptIn(tr, request)
	})
	return r, err
}

// Session identity is persisted and its mutable terminal slot funded BEFORE
// installation. A replacement can authenticate Close without inventing a new
// cursor or relying on a PID, timeout or absence of an SDK response.
func (c *graphOwnerClient) AllocationSession(ctx context.Context, grant, request string, grantSecret, secret [32]byte) (graphOwnerAllocationSession, error) {
	if c == nil || c.catalog == nil || !validName(grant) || !validName(request) || secret == [32]byte{} {
		return graphOwnerAllocationSession{}, errInvalid
	}
	ctx, cancel, err := c.catalog.operationContext(ctx)
	if err != nil {
		return graphOwnerAllocationSession{}, err
	}
	defer cancel()
	r, _, err := historyTransactContext(ctx, c.catalog.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphOwnerAllocationSession, error) {
		actual, err := c.catalog.h.ownerCatalogIn(tr)
		if err != nil {
			return graphOwnerAllocationSession{}, err
		}
		g, slot, err := actual.grantIn(tr, grant)
		if err != nil {
			return graphOwnerAllocationSession{}, err
		}
		_, state, err := c.allocationIn(tr, g.Bucket)
		if err != nil {
			return graphOwnerAllocationSession{}, err
		}
		if !historySecret(g.SecretHash, sha256.Sum256(grantSecret[:])) || g.Recipient != c.routes[g.Bucket] {
			return graphOwnerAllocationSession{}, errStale
		}
		o := graphOwnerAllocationSession{1, grant, request, secret, g.Recipient}
		if slot.Session != nil {
			if *slot.Session != o {
				return o, errMismatch
			}
			return o, nil
		}
		if state.Moving != "" {
			return o, errOwnerBusy
		}
		if slot.State != 0 {
			return o, errAllocationConsumed
		}
		slot.Session = &o
		return o, put(tr, actual.key("allocation-slot", grant), slot)
	})
	return r, err
}

func (c *graphOwnerClient) InstallIDs(ctx context.Context, original graphOwnerAllocationSession) (*graphOwnerAllocationCursor, graphOwnerAllocationReceipt, error) {
	if c == nil || c.catalog == nil || original.Version != 1 || !validName(original.Grant) || !validName(original.Request) || original.Secret == [32]byte{} {
		return nil, graphOwnerAllocationReceipt{}, errInvalid
	}
	ctx, cancel, err := c.catalog.operationContext(ctx)
	if err != nil {
		return nil, graphOwnerAllocationReceipt{}, err
	}
	defer cancel()
	installedNow := false
	_, _, err = historyTransactContext(ctx, c.catalog.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		installedNow = false
		actual, err := c.catalog.h.ownerCatalogIn(tr)
		if err != nil {
			return false, err
		}
		g, slot, err := actual.grantIn(tr, original.Grant)
		if err != nil {
			return false, err
		}
		_, state, err := c.allocationIn(tr, g.Bucket)
		if err != nil {
			return false, err
		}
		if slot.Session == nil || *slot.Session != original || original.Route != state.Routes[g.Bucket] {
			return false, errStale
		}
		if slot.State != 0 {
			return false, nil
		} // Genuine receipt only, NEVER reset First.
		if state.Moving != "" {
			return false, errOwnerBusy
		}
		slot.State = 1
		installedNow = true
		return true, put(tr, actual.key("allocation-slot", original.Grant), slot)
	})
	if err != nil {
		return nil, graphOwnerAllocationReceipt{}, err
	}
	r, _, err := graphCostParallelRead(ctx, c.catalog.h.db, func(tr fdb.ReadTransaction) (graphOwnerAllocationReceipt, error) {
		actual, err := c.catalog.h.ownerCatalogIn(tr)
		if err != nil {
			return graphOwnerAllocationReceipt{}, err
		}
		g, _, err := actual.grantIn(tr, original.Grant)
		if err != nil {
			return graphOwnerAllocationReceipt{}, err
		}
		if _, _, err := c.allocationIn(tr, g.Bucket); err != nil {
			return graphOwnerAllocationReceipt{}, err
		}
		return actual.allocationReceiptIn(tr, original.Grant)
	})
	if err != nil {
		return nil, r, err
	}
	if !installedNow || r.Slot.State != 1 || r.Slot.Session == nil || *r.Slot.Session != original {
		return nil, r, errAllocationConsumed
	}
	return &graphOwnerAllocationCursor{client: c, grant: r.Grant, session: original}, r, nil
}

func (c *graphOwnerClient) CloseAllocationSession(ctx context.Context, original graphOwnerAllocationSession) (graphOwnerAllocationReceipt, error) {
	return c.closeAllocationSession(ctx, original, nil)
}
func (c *graphOwnerClient) closeAllocationSession(ctx context.Context, original graphOwnerAllocationSession, consumed *uint64) (graphOwnerAllocationReceipt, error) {
	if c == nil || c.catalog == nil || original.Version != 1 || !validName(original.Grant) || !validName(original.Request) || original.Secret == [32]byte{} {
		return graphOwnerAllocationReceipt{}, errInvalid
	}
	ctx, cancel, err := c.catalog.terminalContext(ctx, "grant/"+original.Grant, func(tr fdb.ReadTransaction) error {
		actual, err := c.catalog.h.ownerCatalogIn(tr)
		if err != nil {
			return err
		}
		if err := actual.authenticate(c.authority); err != nil {
			return err
		}
		_, slot, err := actual.grantIn(tr, original.Grant)
		if err != nil {
			return err
		}
		if slot.Session == nil || *slot.Session != original || original.Route.Owner != c.authority.ID {
			return errMismatch
		}
		return nil
	})
	if err != nil {
		return graphOwnerAllocationReceipt{}, err
	}
	defer cancel()
	_, _, err = historyTransactContext(ctx, c.catalog.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		actual, err := c.catalog.h.ownerCatalogIn(tr)
		if err != nil {
			return false, err
		}
		if err := actual.authenticate(c.authority); err != nil {
			return false, err
		}
		g, slot, err := actual.grantIn(tr, original.Grant)
		if err != nil {
			return false, err
		}
		if slot.Session == nil || *slot.Session != original || original.Route.Owner != c.authority.ID {
			return false, errMismatch
		}
		if slot.State == 2 {
			return true, nil
		}
		if consumed != nil {
			if *consumed > g.Count || slot.State != 1 {
				return false, errMismatch
			}
			slot.Consumed = *consumed
		} // Unknown/restarted cursor: zero claimed consumption, whole suffix burned.
		slot.State = 2
		return true, put(tr, actual.key("allocation-slot", original.Grant), slot)
	})
	if err != nil {
		return graphOwnerAllocationReceipt{}, err
	}
	r, _, err := graphCostParallelRead(ctx, c.catalog.h.db, func(tr fdb.ReadTransaction) (graphOwnerAllocationReceipt, error) {
		return c.catalog.allocationReceiptIn(tr, original.Grant)
	})
	return r, err
}

// Native freeze checks actual durable terminal sessions, not cooperative local
// process liveness. SAME original Close can fence a lost/crashed cursor first.
func (c *graphOwnerCatalog) allocationDrainIn(tr fdb.ReadTransaction, bucket uint8, route graphOwnerRoute) error {
	if c.binding.Transfer == nil {
		return nil
	}
	state, err := c.allocationStateIn(tr)
	if err != nil {
		return err
	}
	// Catalog key names are hex of kind/request; use their exact byte prefix.
	prefix := c.key("allocation-grant", "")
	end := bytes.Clone(prefix)
	end[len(end)-1]++
	rows, err := tr.GetRange(fdb.KeyRange{Begin: prefix, End: fdb.Key(end)}, fdb.RangeOptions{Limit: int(c.binding.Transfer.MaxGrants) + 1}).GetSliceWithError()
	if err != nil {
		return err
	}
	if uint64(len(rows)) > c.binding.Transfer.MaxGrants {
		return errLimit
	}
	if state.Grants != uint64(len(rows)) {
		return errUnavailable
	}
	for _, row := range rows {
		g, err := decode[graphOwnerAllocationGrant](row.Value)
		if err != nil {
			return err
		}
		if !bytes.Equal(row.Key, c.key("allocation-grant", g.Request)) {
			return errMismatch
		}
		_, slot, err := c.grantIn(tr, g.Request)
		if err != nil {
			return err
		}
		if g.Bucket == bucket && g.Recipient == route && slot.State != 2 {
			return errHistoryInUse
		}
	}
	return nil
}

// Recovery of a reserved but never installed block closes its ALREADY FUNDED
// slot. A live/prepared session requires its own exact original cleanup door.
func (c *graphOwnerClient) CloseReservedIDs(ctx context.Context, request string, secret [32]byte) (graphOwnerAllocationReceipt, error) {
	if c == nil || c.catalog == nil || !validName(request) || secret == [32]byte{} {
		return graphOwnerAllocationReceipt{}, errInvalid
	}
	ctx, cancel, err := c.catalog.terminalContext(ctx, "grant/"+request, func(tr fdb.ReadTransaction) error {
		actual, err := c.catalog.h.ownerCatalogIn(tr)
		if err != nil {
			return err
		}
		if err := actual.authenticate(c.authority); err != nil {
			return err
		}
		g, slot, err := actual.grantIn(tr, request)
		if err != nil {
			return err
		}
		if g.Recipient.Owner != c.authority.ID || !historySecret(g.SecretHash, sha256.Sum256(secret[:])) {
			return errMismatch
		}
		if slot.Session != nil {
			return errHistoryInUse
		}
		return nil
	})
	if err != nil {
		return graphOwnerAllocationReceipt{}, err
	}
	defer cancel()
	_, _, err = historyTransactContext(ctx, c.catalog.h.db, func() error { return nil }, func(tr fdb.Transaction) (bool, error) {
		actual, err := c.catalog.h.ownerCatalogIn(tr)
		if err != nil {
			return false, err
		}
		if err := actual.authenticate(c.authority); err != nil {
			return false, err
		}
		g, slot, err := actual.grantIn(tr, request)
		if err != nil {
			return false, err
		}
		if g.Recipient.Owner != c.authority.ID || !historySecret(g.SecretHash, sha256.Sum256(secret[:])) {
			return false, errMismatch
		}
		if slot.Session != nil {
			return false, errHistoryInUse
		}
		slot.State = 2
		return true, put(tr, actual.key("allocation-slot", request), slot)
	})
	if err != nil {
		return graphOwnerAllocationReceipt{}, err
	}
	r, _, err := graphCostParallelRead(ctx, c.catalog.h.db, func(tr fdb.ReadTransaction) (graphOwnerAllocationReceipt, error) {
		return c.catalog.allocationReceiptIn(tr, request)
	})
	return r, err
}
