package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/replication"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// TestRelRetentionPurgeFeed (backlog 21): a relationship purge emits ONE
// ChangeRelRangePurge record (tag 14, the predicate) and no per-entity
// record, byte-identical on badger, sharded and tiered; a replica re-runs it
// to the primary's state and refuses the low pin; a replica bootstrapped
// from an export taken AFTER the purge refuses it too (the export header
// carries the relationship watermarks). Faulty implementations caught: a
// purge that ships per-entity deletes, reuses tag 13 (an old replica would
// purge nodes), or a bootstrap that drops the watermark.
func TestRelRetentionPurgeFeed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	primaries := []struct {
		name string
		cfg  func(t *testing.T) graphpkg.Config
	}{
		{"badger", func(*testing.T) graphpkg.Config {
			return graphpkg.Config{BadgerInMemory: true, ChangeLog: true}
		}},
		{"sharded", func(t *testing.T) graphpkg.Config {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, ChangeLog: true})
			if err != nil {
				t.Fatal(err)
			}
			return graphpkg.Config{Store: st}
		}},
		{"tiered", func(t *testing.T) graphpkg.Config {
			st, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Machine", "Overview"},
				ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1, ChangeLog: true})
			if err != nil {
				t.Fatal(err)
			}
			return graphpkg.Config{Store: st}
		}},
	}
	var payloads [][]byte
	for _, p := range primaries {
		t.Run(p.name, func(t *testing.T) {
			cfg := p.cfg(t)
			cfg.SnowflakeNodeID = 0
			cfg.AllowRetentionPurge = true
			cfg.AllowTxBackfill = true
			primary, err := graphpkg.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer primary.Close()
			f := buildRelRetentionFixture(t, primary)

			var snap bytes.Buffer
			if err := primary.IO().Export(&snap); err != nil {
				t.Fatal(err)
			}
			lsn0, _ := primary.Replication().LastCommittedLSN()
			replica, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 7, BadgerInMemory: true, ReadOnlyReplica: true})
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close()
			if err := replica.IO().Import(bytes.NewReader(snap.Bytes()), tkgio.ImportOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
				t.Fatal(err)
			}

			before := f.before
			if _, err := primary.Admin().PurgeExpiredRels(ctx, adminpkg.RelPurgePolicy{Type: "EVENT", Before: before}); err != nil {
				t.Fatal(err)
			}
			var recs []store.ChangeRecord
			var purges, perEntity int
			if err := primary.Replication().ForEachChange(lsn0, func(rec store.ChangeRecord) bool {
				recs = append(recs, rec)
				switch rec.Tag {
				case store.ChangeRelRangePurge:
					purges++
					if _, err := replication.ChangeOpOf(rec); !errors.Is(err, replication.ErrNoEntityIdentity) {
						t.Errorf("ChangeOpOf(tag 14) err = %v, want ErrNoEntityIdentity", err)
					}
					if _, _, err := replication.DecodeChangeIdentity(rec); !errors.Is(err, replication.ErrNoEntityIdentity) {
						t.Errorf("DecodeChangeIdentity(tag 14) err = %v, want ErrNoEntityIdentity", err)
					}
				case store.ChangeRangePurge, store.ChangeRelDelete, store.ChangeRelHistoryTruncate, store.ChangeNodeDelete:
					perEntity++
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if purges != 1 || perEntity != 0 {
				t.Fatalf("feed: %d ChangeRelRangePurge, %d other purge/delete records; want 1 and 0", purges, perEntity)
			}
			if !store.ChangeRelRangePurge.Valid() || store.ChangeRelRangePurge.String() != "RelRangePurge" {
				t.Fatalf("tag 14: Valid=%v String=%q", store.ChangeRelRangePurge.Valid(), store.ChangeRelRangePurge.String())
			}

			if _, err := replica.Replication().ApplyChanges(recs); err != nil {
				t.Fatalf("replica apply: %v", err)
			}
			// Idempotent re-apply.
			if _, err := replica.Replication().ApplyChanges(recs); err != nil {
				t.Fatalf("replica re-apply: %v", err)
			}
			for _, g := range []*graphpkg.Graph{primary, replica} {
				assertRelIDs(t, "EVENT after purge", relSetOf(t)(g.Rels().ByType("EVENT", store.QueryOpts{})), f.boundary, f.late)
				assertRelIDs(t, "OV_PAIR after purge", relSetOf(t)(g.Rels().ByType("OV_PAIR", store.QueryOpts{})), f.ovPair)
			}
			assertRelGateMatrix(t, replica, f)

			// A replica bootstrapped after the purge refuses the low pin too.
			var late bytes.Buffer
			if err := primary.IO().Export(&late); err != nil {
				t.Fatal(err)
			}
			fresh, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 8, BadgerInMemory: true, ReadOnlyReplica: true})
			if err != nil {
				t.Fatal(err)
			}
			defer fresh.Close()
			if err := fresh.IO().Import(bytes.NewReader(late.Bytes()), tkgio.ImportOptions{}); err != nil {
				t.Fatal(err)
			}
			assertRelGateMatrix(t, fresh, f)

			// Byte-identical predicate record across backends: same token,
			// same boundary, same mode.
			lsn1, _ := primary.Replication().LastCommittedLSN()
			if _, err := primary.Admin().PurgeExpiredRels(ctx, adminpkg.RelPurgePolicy{Type: "EVENT", Before: 12345}); err != nil {
				t.Fatal(err)
			}
			if err := primary.Replication().ForEachChange(lsn1, func(rec store.ChangeRecord) bool {
				if rec.Tag == store.ChangeRelRangePurge {
					payloads = append(payloads, append([]byte(nil), rec.Payload...))
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
	if len(payloads) != len(primaries) {
		t.Fatalf("collected %d tag-14 payloads, want %d", len(payloads), len(primaries))
	}
	for i := 1; i < len(payloads); i++ {
		if !bytes.Equal(payloads[0], payloads[i]) {
			t.Fatalf("tag-14 payload differs between backends: %x vs %x", payloads[0], payloads[i])
		}
	}
}

// TestImportApplyNodeRetention (backlog 21): an export taken after a node
// purge carries the node watermark; Import ignores it by default (as before)
// and applies it with ImportOptions.ApplyNodeRetention.
func TestImportApplyNodeRetention(t *testing.T) {
	ctx := context.Background()
	src, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err := src.Nodes().Add(ctx, []string{"Event"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Nodes().Add(ctx, []string{"Overview"}, nil); err != nil {
		t.Fatal(err)
	}
	pin, _ := src.Temporal().NowTx()
	if _, err := src.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: pin}); err != nil {
		t.Fatal(err)
	}
	var snap bytes.Buffer
	if err := src.IO().Export(&snap); err != nil {
		t.Fatal(err)
	}
	for _, apply := range []bool{false, true} {
		dst, err := graphpkg.New(graphpkg.Config{})
		if err != nil {
			t.Fatal(err)
		}
		if err := dst.IO().Import(bytes.NewReader(snap.Bytes()), tkgio.ImportOptions{ApplyNodeRetention: apply}); err != nil {
			t.Fatal(err)
		}
		_, err = dst.Nodes().ByLabel("Overview", store.QueryOpts{TxPin: pin - 1})
		if apply != errors.Is(err, graphpkg.ErrRetentionExpired) {
			t.Fatalf("ApplyNodeRetention=%v: ByLabel below the node watermark err = %v", apply, err)
		}
		_ = dst.Close()
	}
}
