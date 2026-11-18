package core

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ImportMerge reads the SAME untrusted export stream as Import, but reaches the
// commit-clock floor by a different route, which must be bounded separately.
//
// Import calls checkImportedStamp, which bounds the wire's stamp and rejects
// an implausible one as ErrCorruptExport. ImportMerge instead hands each record
// to applyChangeRecordLocked, the REPLICA-APPLY door, which advances the floor
// unconditionally and is right to: replica apply reproduces a trusted primary's
// already-committed row, where refusing to advance would strand the row above
// the pin. The merge door therefore bounds the stamp itself, with
// checkImportedRecordStamp, before the record reaches that door.
//
// So one function serves two callers at different trust levels, and the trust
// level is not a property of applyChangeRecordLocked — it is a property of who
// called it. import_merge.go says so itself ("the stream is the untrusted trust
// boundary", captureMergeRecord's doc). Without the bound, a delta stream
// carrying TxFrom = MaxInt64 would install a MaxInt64 floor by walking through
// the door built for trusted input.
//
// The damage is not scoped to the imported row: every later write in the
// process takes its stamp from that floor, and because TxFrom is outside the
// integrity hash, VerifyNodeChain / VerifyRelChain keep reporting the store as
// healthy the whole time.
func TestInstantFloor_CorruptDeltaMergeCannotPoisonTheCommitClock(t *testing.T) {
	t.Parallel()
	// sane is the control: the same stream with a plausible stamp merges, so
	// the refusal below is the stamp bound, not a defect in the fixture.
	for _, tc := range []struct {
		name   string
		txFrom int64
		refuse bool
	}{
		{"sane", time.Now().UnixMilli(), false},
		{"MaxInt64", math.MaxInt64, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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
			// A structurally valid node put whose ONLY possible defect is its
			// transaction stamp — so it clears decode, the trust-boundary
			// checks, and Strict.
			body, err := storeutil.MarshalChangeBody(storeutil.NodePutBody{
				Wire: mustHashedNodeWire(t, storeutil.NodeWire{
					ID:           4242,
					PrimaryLabel: 1,
					Version:      1,
					HasTemporal:  true,
					TxFrom:       tc.txFrom,
				}, []string{"Person"}),
			})
			if err != nil {
				t.Fatalf("marshal body: %v", err)
			}
			crw := changeRecordWire{LSN: 2, Tag: uint8(storepkg.ChangeNodePut), Payload: body}
			if err := marshalAndWrite(&buf, exportTagChange, &crw); err != nil {
				t.Fatalf("change: %v", err)
			}

			err = base.IO.ImportMerge(&buf, tkgio.MergeOptions{})
			_, getErr := base.store.GetNode(types.NodeID(4242))
			if !tc.refuse {
				if err != nil {
					t.Fatalf("ImportMerge(sane stamp) = %v, want nil", err)
				}
				if getErr != nil {
					t.Fatalf("GetNode(4242) after the sane merge = %v, want the node", getErr)
				}
				return
			}
			if !errors.Is(err, ErrCorruptExport) || !strings.Contains(err.Error(), "transaction stamp") {
				t.Fatalf("ImportMerge(TxFrom=MaxInt64) = %v, want ErrCorruptExport from the stamp bound", err)
			}
			if !errors.Is(getErr, storepkg.ErrNodeNotFound) {
				t.Fatalf("GetNode(4242) after the refused merge = %v, want ErrNodeNotFound", getErr)
			}

			pin, err := base.Temporal.NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			wall := int64(nowInstant())
			if pin <= 0 || int64(pin) > wall+maxClockAdvanceSkewMillis {
				t.Fatalf("NowTx = %d, implausibly far from wall %d — a corrupt DELTA stream poisoned the "+
					"commit clock. ImportMerge reaches the floor through applyChangeRecordLocked, the "+
					"replica-apply door, which is unbounded by design. Trust is a property of the CALLER, "+
					"not of that function.", pin, wall)
			}
		})
	}
}
