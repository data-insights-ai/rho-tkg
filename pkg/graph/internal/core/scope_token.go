package core

import "context"

// scopeTokenKey is an unexported context key type — only this package can
// construct a context.Context that carries a scoped change-log token (see
// store.ScopedTxChangeLog and BACKLOG 11f). No caller outside this package
// can observe or forge a scope token: context.WithValue requires the exact
// unexported key value to retrieve it, and Go's type system gives every
// package-external key literal (even one shaped like scopeTokenKey{}) a
// distinct dynamic type from any OTHER package. This is what lets the token
// ride ctx through shared internal helpers (addNodeInternal,
// createRelWithTypeRollback, …) that ALSO serve the standalone (non-tx) path
// without adding a formal parameter to every one of them: a standalone caller
// never constructs a ctx carrying this key, so scopeTokenFrom always returns
// (0, false) for it — zero behavior change, zero blast radius.
//
// FOUNDATION ONLY (Batch A): nothing in this codebase calls withScopeToken
// yet. GraphTx still drives the legacy single-scope TxChangeLogScope
// mechanism (tx.go's lockActiveCoreWrite / SetLogDivert) unchanged. A later
// BACKLOG 11f batch wires GraphTx to open a store.ScopedTxChangeLog scope and
// call withScopeToken so its mutations can take the same shared read-lock a
// standalone mutation takes instead of the current full exclusive lock.
type scopeTokenKey struct{}

// withScopeToken returns a context carrying scoped change-log token tok (see
// store.ScopedTxChangeLog). token == 0 is reserved for "no scope" — callers
// should not construct a context carrying 0; scopeTokenFrom on a context that
// never called withScopeToken already reports (0, false), which every scoped
// store door treats identically to token == 0.
func withScopeToken(ctx context.Context, token uint64) context.Context {
	return context.WithValue(ctx, scopeTokenKey{}, token)
}

// scopeTokenFrom extracts a scoped change-log token previously attached via
// withScopeToken. Returns (0, false) when ctx carries none — the overwhelming
// common case today, since nothing constructs one yet (see the package doc
// above). Callers that only need "the token, or 0 if absent" can ignore the
// second return value; scopeTokenFrom(ctx) never panics on a nil ctx.
func scopeTokenFrom(ctx context.Context) (uint64, bool) {
	if ctx == nil {
		return 0, false
	}
	v := ctx.Value(scopeTokenKey{})
	if v == nil {
		return 0, false
	}
	token, ok := v.(uint64)
	if !ok {
		return 0, false
	}
	return token, true
}
