package core

import (
	"context"
	"testing"
)

func TestScopeToken_RoundTrip(t *testing.T) {
	ctx := withScopeToken(context.Background(), 42)
	token, ok := scopeTokenFrom(ctx)
	if !ok {
		t.Fatal("scopeTokenFrom: ok = false, want true")
	}
	if token != 42 {
		t.Fatalf("scopeTokenFrom: token = %d, want 42", token)
	}
}

func TestScopeToken_AbsentOnPlainContext(t *testing.T) {
	token, ok := scopeTokenFrom(context.Background())
	if ok {
		t.Fatal("scopeTokenFrom(context.Background()): ok = true, want false")
	}
	if token != 0 {
		t.Fatalf("scopeTokenFrom(context.Background()): token = %d, want 0", token)
	}
}

func TestScopeToken_NilContextDoesNotPanic(t *testing.T) {
	//nolint:staticcheck // deliberately probing nil-safety
	token, ok := scopeTokenFrom(nil)
	if ok || token != 0 {
		t.Fatalf("scopeTokenFrom(nil) = (%d, %v), want (0, false)", token, ok)
	}
}

// A plain context.Context caller (standalone path — the overwhelming common
// case, since nothing in this codebase constructs a token-carrying context
// yet) must never observe a scope token. Guards against a future accidental
// key collision with another package's context value.
func TestScopeToken_ForeignContextNeverObserved(t *testing.T) {
	type otherKey struct{}
	ctx := context.WithValue(context.Background(), otherKey{}, uint64(99))
	token, ok := scopeTokenFrom(ctx)
	if ok || token != 0 {
		t.Fatalf("scopeTokenFrom(foreign key) = (%d, %v), want (0, false)", token, ok)
	}
}

func TestScopeToken_ZeroTokenRoundTrips(t *testing.T) {
	// withScopeToken(ctx, 0) is discouraged by its own doc comment, but must
	// not panic or misbehave — scopeTokenFrom should report ok=true with
	// token 0, which every scoped door already treats identically to "no
	// token attached at all" (ok=false).
	ctx := withScopeToken(context.Background(), 0)
	token, ok := scopeTokenFrom(ctx)
	if !ok || token != 0 {
		t.Fatalf("scopeTokenFrom(withScopeToken(ctx, 0)) = (%d, %v), want (0, true)", token, ok)
	}
}

func TestScopeToken_NestedContextOverride(t *testing.T) {
	outer := withScopeToken(context.Background(), 1)
	inner := withScopeToken(outer, 2)
	token, ok := scopeTokenFrom(inner)
	if !ok || token != 2 {
		t.Fatalf("scopeTokenFrom(inner) = (%d, %v), want (2, true) — the most recently attached token wins", token, ok)
	}
	// The outer context is unaffected (context.WithValue never mutates its
	// parent) — re-deriving from outer must still see 1.
	outerToken, ok := scopeTokenFrom(outer)
	if !ok || outerToken != 1 {
		t.Fatalf("scopeTokenFrom(outer) = (%d, %v), want (1, true)", outerToken, ok)
	}
}
