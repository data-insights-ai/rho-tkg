package core

import (
	"bytes"
	"context"
	"math"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// ImportMerge reads the SAME untrusted export stream as Import, but reaches the
// commit-clock floor by a different route — and that route is unbounded.
//
// Import calls advanceImportedStamp, which bounds the wire's stamp and rejects
// an implausible one as ErrCorruptExport. ImportMerge instead hands each record
// to applyChangeRecordLocked, the REPLICA-APPLY door, which advances the floor
// unconditionally and is right to: replica apply reproduces a trusted primary's
// already-committed row, where refusing to advance would strand the row above
// the pin.
//
// So one function serves two callers at different trust levels, and the trust
// level is not a property of applyChangeRecordLocked — it is a property of who
// called it. import_merge.go says so itself ("the stream is the untrusted trust
// boundary", captureMergeRecord's doc). A delta stream carrying
// TxFrom = MaxInt64 therefore installs a MaxInt64 floor by walking through the
// door built for trusted input.
//
// The damage is not scoped to the imported row: every later write in the
// process takes its stamp from that floor, and because TxFrom is outside the
// integrity hash, VerifyNodeChain / VerifyRelChain keep reporting the store as
// healthy the whole time.
func TestInstantFloor_CorruptDeltaMergeCannotPoisonTheCommitClock(t *testing.T) {
	ctx := context.Background()
	base := newPlainGraph(t, 1)
	if _, err := base.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var buf bytes.Buffer
	hdr := exportHeader{Version: exportFormatVersionDelta, ExportedAt: 1, IsDelta: true, FromLSN: 1, FromEpoch: 7, ToLSN: 5, ToEpoch: 7}
	if err := marshalAndWrite(&buf, exportTagHeader, &hdr); err != nil {
		t.Fatalf("header: %v", err)
	}
	reg := tiered.RegistryFileData{Labels: base.labels.ExportNames(), RelTypes: base.relTypes.ExportNames()}
	if err := marshalAndWrite(&buf, exportTagRegistry, &reg); err != nil {
		t.Fatalf("registry: %v", err)
	}
	// A structurally valid node put whose ONLY defect is an absurd transaction
	// stamp — so it clears decode, the trust-boundary checks, and Strict.
	body, err := storeutil.MarshalChangeBody(storeutil.NodePutBody{
		Wire: mustHashedNodeWire(t, storeutil.NodeWire{
			ID:           4242,
			PrimaryLabel: 1,
			Version:      1,
			HasTemporal:  true,
			TxFrom:       math.MaxInt64,
		}, []string{"Person"}),
	})
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	crw := changeRecordWire{LSN: 2, Tag: uint8(storepkg.ChangeNodePut), Payload: body}
	if err := marshalAndWrite(&buf, exportTagChange, &crw); err != nil {
		t.Fatalf("change: %v", err)
	}

	// Accepting or rejecting the delta is a policy choice; poisoning the clock
	// is not a permitted outcome either way.
	_ = base.IO.ImportMerge(&buf, tkgio.MergeOptions{})

	pin, err := base.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	wall := int64(nowInstant())
	if int64(pin) > wall+maxClockAdvanceSkewMillis {
		t.Fatalf("NowTx = %d, implausibly far past wall %d — a corrupt DELTA stream poisoned the "+
			"commit clock. Import bounds its stamps via advanceImportedStamp; ImportMerge reaches "+
			"the floor through applyChangeRecordLocked, the replica-apply door, which is unbounded "+
			"by design. Trust is a property of the CALLER, not of that function.", pin, wall)
	}
}
