package core

import (
	"bytes"
	"math"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// An EXPORT FILE is untrusted input, exactly like the durable watermark.
//
// The import door advances the commit-clock floor to each imported wire's
// TxFrom so a bootstrap-only replica's NowTx() still covers rows imported from
// a bursting or clock-skewed primary (lesson 71). But the wire arrives from a
// FILE — a truncated copy, a bit-rotted archive, a snapshot from a host with a
// broken RTC, or an attacker-supplied stream. Nothing about crossing the import
// boundary makes its stamps trustworthy.
//
// If an absurd stamp is accepted, it installs a floor near MaxInt64 and the
// damage is not scoped to the bad row: EVERY subsequent write in the process is
// stamped from that floor, and TxFrom is not covered by the integrity hash, so
// VerifyNodeChain/VerifyRelChain keep reporting the store as healthy while its
// transaction time is meaningless.
//
// The plausibility bound therefore belongs on this door as much as on the seed.
func TestInstantFloor_CorruptExportCannotPoisonTheCommitClock(t *testing.T) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	var stream bytes.Buffer
	writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{Version: exportFormatVersion})
	writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{
		Labels:   []string{"", "Person"},
		RelTypes: []string{""},
	})
	// A well-formed, correctly-hashed wire whose only defect is an absurd
	// transaction stamp — so it passes every existing validation gate.
	writeImportMsgpackRecord(t, &stream, exportTagNode, mustHashedNodeWire(t, storeutil.NodeWire{
		ID:           100,
		PrimaryLabel: 1,
		Version:      1,
		HasTemporal:  true,
		TxFrom:       math.MaxInt64,
	}, []string{"Person"}))

	// Whether the import is rejected or accepted is a policy choice; what must
	// NOT happen either way is the clock being dragged to the moon.
	_ = g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{})

	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	wall := int64(nowInstant())
	if int64(pin) > wall+maxClockAdvanceSkewMillis {
		t.Fatalf("NowTx = %d, implausibly far past wall %d — a corrupt EXPORT FILE poisoned the "+
			"commit clock. Every later write in this process now carries a garbage TxFrom, and "+
			"because TxFrom is outside the integrity hash the chain verifiers still report the "+
			"store as healthy. An import wire is untrusted input and must be bounded like the seed.",
			pin, wall)
	}
}
