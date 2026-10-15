package main

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphOwnerIntent struct {
	Body   []byte
	Digest [32]byte
	Routes [2]graphOwnerRoute
}
type graphOwnerPermit struct {
	Authority graphOwnerAuthority
	Intent    graphOwnerIntent
	Route     graphOwnerRoute
}
type graphOwnerClient struct {
	catalog   *graphOwnerCatalog
	authority graphOwnerAuthority
	routes    [2]graphOwnerRoute
}

// The native catalog issues this client its current route epochs. A delayed
// client does not become current again merely because a later move returns the
// same owner ID to a bucket (A1→B2→A3). Historical intent bytes remain unchanged.
func (c *graphOwnerCatalog) Client(ctx context.Context, a graphOwnerAuthority) (*graphOwnerClient, error) {
	if c == nil || c.h == nil || ctx == nil || c.binding.validate() != nil {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, _, err := graphCostParallelRead(ctx, c.h.db, func(tr fdb.ReadTransaction) (*graphOwnerClient, error) {
		actual, err := c.h.ownerCatalogIn(tr)
		if err != nil {
			return nil, err
		}
		if !graphColumnSame(actual.binding, c.binding) {
			return nil, errMismatch
		}
		if err := actual.authenticate(a); err != nil {
			return nil, err
		}
		state, err := actual.stateIn(tr)
		if err != nil {
			return nil, err
		}
		return &graphOwnerClient{actual, a, state.Routes}, nil
	})
	return client, err
}

// Input owns the actual canonical proposal before any SDK retry or callback.
// A fresh intent's envelope cap is explicit, checked BEFORE decode or cloning.
func (c *graphOwnerClient) input(body []byte) (transaction, []byte, error) {
	if c == nil || c.catalog == nil || c.catalog.h == nil || len(body) == 0 || len(body) > 32<<10 {
		return transaction{}, nil, errInvalid
	}
	x, err := decode[transaction](body)
	if err != nil {
		return x, nil, err
	}
	if err := c.catalog.h.validate(x); err != nil {
		return x, nil, err
	}
	return x, bytes.Clone(body), nil
}
func graphOwnerCurrent(state graphOwnerState, a graphOwnerAuthority, coordinator uint8, route graphOwnerRoute) error {
	if coordinator > 1 || route.Owner != a.ID || route.Epoch == 0 || state.Routes[coordinator] != route {
		return errStale
	}
	return nil
}
func graphOwnerNew(state graphOwnerState, x transaction, intent graphOwnerIntent) error {
	if state.Moving != "" {
		return errOwnerBusy // Not an SDK predicate, unknown decision or ABORT.
	}
	for _, p := range x.Participants {
		if p.Group > 1 || state.Routes[p.Group] != intent.Routes[p.Group] {
			return errStale
		}
	}
	return nil
}

// Prepare durably binds both request AND transaction ID to exactly one body and
// route capture. It grants no business completion. Retrying after uncertainty
// uses these SAME bytes; capture never silently refreshes under an old request.
func (c *graphOwnerClient) Prepare(ctx context.Context, body []byte) (graphOwnerIntent, error) {
	if ctx == nil {
		return graphOwnerIntent{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphOwnerIntent{}, err
	}
	x, owned, err := c.input(body)
	if err != nil {
		return graphOwnerIntent{}, err
	}
	intent, _, err := historyTransactContext(ctx, c.catalog.h.db, func() error { return nil }, func(tr fdb.Transaction) (graphOwnerIntent, error) {
		actual, err := c.catalog.h.ownerCatalogIn(tr)
		if err != nil {
			return graphOwnerIntent{}, err
		}
		if err := actual.authenticate(c.authority); err != nil {
			return graphOwnerIntent{}, err
		}
		state, err := actual.stateIn(tr)
		if err != nil {
			return graphOwnerIntent{}, err
		}
		if err := graphOwnerCurrent(state, c.authority, x.Coordinator, c.routes[x.Coordinator]); err != nil {
			return graphOwnerIntent{}, err
		}
		key := actual.key("intent-request", x.Request)
		idKey := actual.key("intent-id", x.ID)
		prior, found, err := get[graphOwnerIntent](tr, key)
		if err != nil {
			return prior, err
		}
		id, idFound, err := get[graphOwnerIntent](tr, idKey)
		if err != nil {
			return prior, err
		}
		if found {
			if !idFound || !graphColumnSame(id, prior) || !bytes.Equal(prior.Body, owned) || prior.Digest != sha256.Sum256(owned) {
				return prior, errMismatch
			}
			return prior, nil // Never recapture, even under successor ownership.
		}
		if idFound {
			return prior, errMismatch
		}
		intent := graphOwnerIntent{owned, sha256.Sum256(owned), state.Routes}
		if err := graphOwnerNew(state, x, intent); err != nil {
			return intent, err
		}
		// An absent intent cannot be retrofitted around an existing original.
		for _, k := range []fdb.Key{actual.h.key(x.Coordinator, "id", x.ID), actual.h.key(x.Coordinator, "request", x.Request)} {
			raw, err := tr.Get(k).Get()
			if err != nil {
				return intent, err
			}
			if raw != nil {
				return intent, errMismatch
			}
		}
		if err := actual.createIn(tr, key, intent); err != nil {
			return intent, err
		}
		return intent, actual.createIn(tr, idKey, intent)
	})
	return intent, err
}

// Every execute door calls this BEFORE retained replay. Fresh graph selection
// is not optional capability detection: nil/unowned callers are always refused.
// Historical validation remains independent and can still authenticate old CDC.
func (h *harness) ownerGateIn(tr fdb.ReadTransaction, x transaction, digest [32]byte, permit *graphOwnerPermit) (*graphOwnerCatalog, graphOwnerState, error) {
	if !graphOwnerSelected(h.config) {
		if permit != nil {
			return nil, graphOwnerState{}, errInvalid
		}
		return nil, graphOwnerState{}, nil
	}
	if permit == nil {
		return nil, graphOwnerState{}, errStale
	}
	c, err := h.ownerCatalogIn(tr)
	if err != nil {
		return nil, graphOwnerState{}, err
	}
	if err := c.authenticate(permit.Authority); err != nil {
		return nil, graphOwnerState{}, err
	}
	state, err := c.stateIn(tr)
	if err != nil {
		return nil, state, err
	}
	if err := graphOwnerCurrent(state, permit.Authority, x.Coordinator, permit.Route); err != nil {
		return nil, state, err
	}
	actual, found, err := get[graphOwnerIntent](tr, c.key("intent-request", x.Request))
	if err != nil {
		return nil, state, err
	}
	id, idFound, err := get[graphOwnerIntent](tr, c.key("intent-id", x.ID))
	if err != nil {
		return nil, state, err
	}
	body, err := wire(x)
	if err != nil {
		return nil, state, err
	}
	if !found || !idFound || !graphColumnSame(actual, id) || !graphColumnSame(actual, permit.Intent) || !bytes.Equal(body, actual.Body) || actual.Digest != digest || sha256.Sum256(actual.Body) != digest {
		return nil, state, errMismatch
	}
	return c, state, nil
}

func (c *graphOwnerClient) Submit(ctx context.Context, intent graphOwnerIntent) (outcome, int, error) {
	if ctx == nil {
		return outcome{}, 0, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return outcome{}, 0, err
	}
	x, owned, err := c.input(intent.Body)
	if err != nil {
		return outcome{}, 0, err
	}
	intent.Body = owned
	if intent.Digest != sha256.Sum256(owned) {
		return outcome{}, 0, errMismatch
	}
	permit := graphOwnerPermit{c.authority, intent, c.routes[x.Coordinator]}
	return historyTransactContext(ctx, c.catalog.h.db, func() error { return nil }, func(tr fdb.Transaction) (outcome, error) {
		return c.catalog.h.executeOwned(tr, x, intent.Digest, &permit)
	})
}

// Original is an authenticated successor read, not a prepare, replan or new
// execution. Missing remains unknown. Both ID/request bodies and genuine CDC
// are joined by the SAME existing canonical original reducer.
func (c *graphOwnerClient) Original(ctx context.Context, intent graphOwnerIntent) (graphCostParallelProof, int, error) {
	if ctx == nil {
		return graphCostParallelProof{}, 0, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return graphCostParallelProof{}, 0, err
	}
	x, owned, err := c.input(intent.Body)
	if err != nil {
		return graphCostParallelProof{}, 0, err
	}
	intent.Body = owned
	if intent.Digest != sha256.Sum256(owned) {
		return graphCostParallelProof{}, 0, errMismatch
	}
	permit := graphOwnerPermit{c.authority, intent, c.routes[x.Coordinator]}
	return graphCostParallelRead(ctx, c.catalog.h.db, func(tr fdb.ReadTransaction) (graphCostParallelProof, error) {
		if _, _, err := c.catalog.h.ownerGateIn(tr, x, intent.Digest, &permit); err != nil {
			return graphCostParallelProof{}, err
		}
		proof, err := c.catalog.h.graphCostParallelOriginalIn(tr, x, owned)
		if err != nil {
			return proof, err
		}
		return proof, nil
	})
}
