package constraints

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// BACKLOG 8c: DryRunValidate and uniqueReady() (feeding CreateUnique /
// CreateUniqueForever / ReleaseOwnership / DropUnique) returned
// grapherr.ErrNilGraph when the underlying Ops implementation didn't
// support DryRunOps/UniqueOps — the WRONG sentinel: ErrNilGraph means "the
// graph/API wrapper is nil or unwired" (a.ready() already returns it for
// that case, one line above), not "this capability isn't implemented."
// constraintsOpsSpy (api_test.go) implements only the base Ops interface
// (Set/Add/Get), not DryRunOps or UniqueOps, so it's the exact fixture
// needed to trigger the previously-dead branch.
func TestDryRunValidate_UnsupportedOpsReturnsCapabilityNotSupported(t *testing.T) {
	api := New(&constraintsOpsSpy{})
	_, err := api.DryRunValidate(context.Background(), DryRunFacts{})
	if !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("DryRunValidate with unsupporting Ops = %v, want ErrCapabilityNotSupported — BACKLOG 8c regression", err)
	}
}

func TestUniqueMethods_UnsupportedOpsReturnCapabilityNotSupported(t *testing.T) {
	api := New(&constraintsOpsSpy{})
	ctx := context.Background()

	if err := api.CreateUnique(ctx, "Label", "key"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("CreateUnique with unsupporting Ops = %v, want ErrCapabilityNotSupported — BACKLOG 8c regression", err)
	}
	if err := api.CreateUniqueForever(ctx, "Label", "key"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("CreateUniqueForever with unsupporting Ops = %v, want ErrCapabilityNotSupported — BACKLOG 8c regression", err)
	}
	if err := api.ReleaseOwnership(ctx, "Label", "key", "v"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("ReleaseOwnership with unsupporting Ops = %v, want ErrCapabilityNotSupported — BACKLOG 8c regression", err)
	}
	if err := api.DropUnique(ctx, "Label", "key"); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("DropUnique with unsupporting Ops = %v, want ErrCapabilityNotSupported — BACKLOG 8c regression", err)
	}
}
