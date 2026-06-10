package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/index"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Anti-drift guard for the sentinel-error architecture: every sentinel has
// exactly ONE canonical declaration; every other exported surface is a pure
// alias of it. If anyone replaces an alias with a fresh errors.New (same
// message, different identity), errors.Is silently stops matching across
// packages — this test turns that silent break into a build-time failure.
func TestSentinelAliasesShareIdentity(t *testing.T) {
	t.Parallel()

	identical := map[string][2]error{
		// pkg/graph aliases of store-canonical sentinels.
		"NodeNotFound/graph=store":        {graphpkg.ErrNodeNotFound, storepkg.ErrNodeNotFound},
		"RelNotFound/graph=store":         {graphpkg.ErrRelNotFound, storepkg.ErrRelNotFound},
		"NodeExists/graph=store":          {graphpkg.ErrNodeExists, storepkg.ErrNodeExists},
		"RelExists/graph=store":           {graphpkg.ErrRelExists, storepkg.ErrRelExists},
		"IndexExists/graph=store":         {graphpkg.ErrIndexExists, storepkg.ErrIndexExists},
		"IndexNotFound/graph=store":       {graphpkg.ErrIndexNotFound, storepkg.ErrIndexNotFound},
		"DimensionMismatch/graph=store":   {graphpkg.ErrDimensionMismatch, storepkg.ErrDimensionMismatch},
		"InvalidQueryLimit/graph=store":   {graphpkg.ErrInvalidQueryLimit, storepkg.ErrInvalidQueryLimit},
		"InvalidQueryCursor/graph=store":  {graphpkg.ErrInvalidQueryCursor, storepkg.ErrInvalidQueryCursor},
		"CapabilityNotSupported":          {graphpkg.ErrCapabilityNotSupported, storepkg.ErrCapabilityNotSupported},
		"TxDone/graph=store":              {graphpkg.ErrTxDone, storepkg.ErrTxDone},
		"WireFormatVersion/graph=store":   {graphpkg.ErrWireFormatVersionUnsupported, storepkg.ErrWireFormatVersionUnsupported},
		"InvalidTimeRange/graph=store":    {graphpkg.ErrInvalidTimeRange, storepkg.ErrInvalidTimeRange},
		"InvalidShardDepth/graph=store":   {graphpkg.ErrInvalidShardDepth, storepkg.ErrInvalidShardDepth},
		"InvalidVectorValue/graph=store":  {graphpkg.ErrInvalidVectorValue, storepkg.ErrInvalidVectorValue},
		"TemporalIdxExists/graph=store":   {graphpkg.ErrTemporalIndexExists, storepkg.ErrTemporalIndexExists},
		"TemporalIdxNotFound/graph=store": {graphpkg.ErrTemporalIndexNotFound, storepkg.ErrTemporalIndexNotFound},
		"VectorIdxExists/graph=store":     {graphpkg.ErrVectorIndexExists, storepkg.ErrVectorIndexExists},
		"VectorIdxNotFound/graph=store":   {graphpkg.ErrVectorIndexNotFound, storepkg.ErrVectorIndexNotFound},

		// tiered re-exports of the same store sentinels — a third surface
		// that must share the same identity, not merely the same message.
		"NodeNotFound/tiered=store": {tieredpkg.ErrNodeNotFound, storepkg.ErrNodeNotFound},
		"RelNotFound/tiered=store":  {tieredpkg.ErrRelNotFound, storepkg.ErrRelNotFound},
		"StoreClosed/tiered=store":  {tieredpkg.ErrStoreClosed, storepkg.ErrStoreClosed},
		"NilStore/tiered=store":     {tieredpkg.ErrNilStore, storepkg.ErrNilStore},

		// pkg/graph aliases of io-canonical sentinels (pkg/graph/io owns the
		// declarations; internal/core aliases FROM io).
		"NilReader/graph=io":            {graphpkg.ErrNilReader, tkgio.ErrNilReader},
		"NilWriter/graph=io":            {graphpkg.ErrNilWriter, tkgio.ErrNilWriter},
		"ImportSizeLimit/graph=io":      {graphpkg.ErrImportSizeLimit, tkgio.ErrImportSizeLimit},
		"IncompatibleExport/graph=io":   {graphpkg.ErrIncompatibleExport, tkgio.ErrIncompatibleExport},
		"IncompatibleRegistry/graph=io": {graphpkg.ErrIncompatibleRegistry, tkgio.ErrIncompatibleRegistry},
		"CorruptExport/graph=io":        {graphpkg.ErrCorruptExport, tkgio.ErrCorruptExport},

		// pkg/graph aliases of index-provider sentinels.
		"ProviderExists/graph=index":    {graphpkg.ErrIndexProviderExists, indexpkg.ErrIndexProviderExists},
		"ProviderNotFound/graph=index":  {graphpkg.ErrIndexProviderNotFound, indexpkg.ErrIndexProviderNotFound},
		"ProviderEmptyName/graph=index": {graphpkg.ErrIndexProviderEmptyName, indexpkg.ErrIndexProviderEmptyName},
	}

	for name, pair := range identical {
		if pair[0] != pair[1] {
			t.Errorf("%s: alias and canonical sentinel are DIFFERENT error values (messages may match, errors.Is will not)", name)
		}
		if pair[0] == nil {
			t.Errorf("%s: nil sentinel", name)
		}
	}
}

// Behavioral half of the guard: errors actually produced by the engine must
// match through EVERY exported qualifier a consumer might reasonably hold.
func TestSentinelsMatchThroughEveryQualifier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer g.Close()

	// Missing node → must match via graph, store, AND tiered qualifiers.
	_, getErr := g.Nodes().Get(ctx, types.NodeID(1234567))
	if getErr == nil {
		t.Fatalf("Get(nonexistent) succeeded")
	}
	for q, sentinel := range map[string]error{
		"graph":  graphpkg.ErrNodeNotFound,
		"store":  storepkg.ErrNodeNotFound,
		"tiered": tieredpkg.ErrNodeNotFound,
	} {
		if !errors.Is(getErr, sentinel) {
			t.Errorf("missing-node error %v does not match %s.ErrNodeNotFound", getErr, q)
		}
	}

	// Self-loop rejection → core-canonical sentinel via the graph alias.
	n, err := g.Nodes().Add(ctx, []string{"N"}, nil)
	if err != nil {
		t.Fatalf("add node: %v", err)
	}
	if _, err := g.Rels().Add(ctx, "SELF", n, n, nil); !errors.Is(err, graphpkg.ErrSelfLoop) {
		t.Errorf("self-loop error %v does not match graph.ErrSelfLoop", err)
	}

	// Nil export writer → io-canonical sentinel via both qualifiers.
	exportErr := g.IO().Export(nil)
	if !errors.Is(exportErr, graphpkg.ErrNilWriter) || !errors.Is(exportErr, tkgio.ErrNilWriter) {
		t.Errorf("nil-writer error %v does not match through both graph and io qualifiers", exportErr)
	}
}
