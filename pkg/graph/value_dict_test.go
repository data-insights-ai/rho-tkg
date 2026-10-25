package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// vdAisocDicts codes LOGON.action and LOGON.remote_ip, and Device.key (a
// unique key: GetOrCreateByKey probes its index with the plain value).
func vdAisocDicts() []store.ValueDictionary {
	return []store.ValueDictionary{
		{Name: "action", Cap: 100, Members: []store.ValueDictionaryMember{{RelType: "LOGON", Property: "action"}}},
		{Name: "ip", Cap: 100, Members: []store.ValueDictionaryMember{{RelType: "LOGON", Property: "remote_ip"}}},
		{Name: "device", Cap: 1000, Members: []store.ValueDictionaryMember{{Label: "Device", Property: "key"}}},
	}
}

// TestValueDict_GraphInstallsNames (DICT step 3): a graph over a badger store
// with value dictionaries installs its label and type registries, so the
// members written through the graph are coded, keyed upserts on a coded key
// find their node again, and the values read back plain. Catches a graph
// that does not install the registries (nothing coded).
func TestValueDict_GraphInstallsNames(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs, err := badger.New(badger.Config{Dir: dir, ValueDictionaries: vdAisocDicts()})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range []string{"Device", "Account"} {
		if err := g.Constraints().CreateUnique(context.Background(), l, "key"); err != nil {
			t.Fatal(err)
		}
	}
	writeAisocRecords(t, g, 0, 300, 20, 30)
	writeAisocRecords(t, g, 300, 300, 20, 30) // the same 20 devices: keyed upserts on coded keys
	stats := bs.ValueDictionaryStats()
	if len(stats) != 3 || stats[0].Coded < 600 || stats[0].NewEntries != 1 || stats[1].Coded < 600 || stats[2].NewEntries != 20 {
		t.Fatalf("stats %+v; want action and remote_ip coded on 600 rels, 20 device keys", stats)
	}
	devices, err := g.Nodes().ByLabel("Device", store.QueryOpts{})
	if err != nil || len(devices) != 20 {
		t.Fatalf("devices = %d, %v; want 20 (a keyed upsert missed its coded key)", len(devices), err)
	}
	logons, err := g.Rels().ByType("LOGON", store.QueryOpts{})
	if err != nil || len(logons) != 600 {
		t.Fatalf("LOGON rels = %d, %v", len(logons), err)
	}
	for _, r := range logons {
		a, _ := r.GetProperty("action")
		ip, _ := r.GetProperty("remote_ip")
		if a != "LogonSuccess" || ip != "10.2.0.2" {
			t.Fatalf("rel %v: action %#v remote_ip %#v", r.ID(), a, ip)
		}
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
}
