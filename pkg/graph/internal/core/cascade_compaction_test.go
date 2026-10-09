package core

import (
	"bytes"
	"context"
	"errors"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestCompactionKeepsPrevHashAnchors (backlog 24, same root as 18): create,
// apply a cascade shape, update, compact with KeepVersions 1, verify the hash
// chain, export, import into a fresh graph of the same backend, verify again.
// KeepVersions keeps the newest history VERSIONS; after a bounded cascade the
// newest history rows can be cascade rows whose PrevHash points at an older
// base row, and the current row's PrevHash at a row below them — trimming
// that anchor left a chain that does not verify, and an export of it failed
// import ("imported hash chain does not verify"). Catches a keep rule that
// counts versions only. Counterpart: the policy still trims (the number of
// history rows after compaction is below the number before) where the
// anchors allow it — a plain update chain trims to KeepVersions.
func TestCompactionKeepsPrevHashAnchors(t *testing.T) {
	t.Parallel()
	shapes := append([]ccShape{{"plain-updates", func(e *ccEnt, id int64) {
		e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(1500), "x": int64(1)})
		e.mustUpdate(id, map[string]any{"tkg_valid_from": types.Instant(1600), "x": int64(2)})
	}}}, ccShapes()...)
	for _, be := range txbBackends() {
		for _, rel := range []bool{false, true} {
			kind := map[bool]string{false: "node", true: "rel"}[rel]
			for _, sh := range shapes {
				t.Run(be.name+"/"+kind+"/"+sh.name, func(t *testing.T) {
					g := be.open(t, false)
					useTestClock(t, g)
					e := newCCEnt(t, g, rel)
					id := e.add("T", 1000, nil)
					sh.apply(e, id)
					if err := e.update(id, map[string]any{"tkg_valid_from": types.Instant(9000), "x": int64(7)}); err != nil && !errors.Is(err, ErrAlreadyClosed) {
						t.Fatalf("update: %v", err)
					}
					before := len(e.chain(id))
					var err error
					if rel {
						_, err = g.Admin.CompactHistoryRels(context.Background(), RetentionPolicy{KeepVersions: 1})
					} else {
						_, err = g.Admin.CompactHistoryNodes(context.Background(), RetentionPolicy{KeepVersions: 1})
					}
					if errors.Is(err, storepkg.ErrCapabilityNotSupported) {
						t.Skipf("%s: compaction not supported (%v)", be.name, err)
					}
					if err != nil {
						t.Fatalf("compact: %v", err)
					}
					after := len(e.chain(id))
					if sh.name == "plain-updates" && after != 2 {
						t.Fatalf("plain update chain kept %d rows; want 2 (current + KeepVersions 1):%s", after, e.chainString(id))
					}
					verify := func(phase string, g *Core) {
						t.Helper()
						var ok bool
						var err error
						if rel {
							ok, err = g.Hash.VerifyRelChain(types.RelID(id))
						} else {
							ok, err = g.Hash.VerifyNodeChain(types.NodeID(id))
						}
						if err != nil || !ok {
							t.Fatalf("[%s] chain verify = %v, %v (rows %d -> %d):%s", phase, ok, err, before, after, e.chainString(id))
						}
					}
					verify("after compaction", g)
					var buf bytes.Buffer
					if err := g.IO.Export(&buf); err != nil {
						t.Fatalf("Export: %v", err)
					}
					g2 := be.open(t, false)
					if err := g2.IO.Import(bytes.NewReader(buf.Bytes()), tkgio.ImportOptions{}); err != nil {
						t.Fatalf("Import: %v (rows %d -> %d):%s", err, before, after, e.chainString(id))
					}
					verify("after import", g2)
				})
			}
		}
	}
}
