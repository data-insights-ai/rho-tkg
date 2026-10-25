package graph_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	badgerv4 "github.com/dgraph-io/badger/v4"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// vdAcceptDicts is ai-soc's kind of list on a LOGON shape: one shared action
// dictionary, the ip, sid and command-line columns, and two node members
// (a keyed upsert's key and a long folder).
func vdAcceptDicts() []store.ValueDictionary {
	rel := func(p string) []store.ValueDictionaryMember {
		return []store.ValueDictionaryMember{{RelType: "LOGON", Property: p}}
	}
	return []store.ValueDictionary{
		{Name: "action", Cap: 100, Members: rel("action")},
		{Name: "remote_ip", Cap: 1000, Members: rel("remote_ip")},
		{Name: "account_sid", Cap: 1000, Members: rel("account_sid")},
		{Name: "cmdline", Cap: 1000, Members: rel("cmdline")},
		{Name: "device", Cap: 1000, Members: []store.ValueDictionaryMember{{Label: "Device", Property: "key"}}},
		{Name: "folder", Cap: 1000, Members: []store.ValueDictionaryMember{{Label: "Device", Property: "folder"}}},
	}
}

const vdRecords = 1200

var vdActions = []string{"LogonSuccess", "LogonFailed", "LogonAttempted"}

func vdIP(i int) string { return fmt.Sprintf("10.2.%d.%d", i%7, i%30) }

// vdPin is a transaction-time pin after a write phase: the newest stamp a
// LOGON row carries (the store's stamps run ahead of the wall clock).
func vdPin(t *testing.T, g *graphpkg.Graph) types.Instant {
	t.Helper()
	rels, err := g.Rels().ByType("LOGON", store.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	var pin types.Instant
	for _, r := range rels {
		if tm := r.Temporal(); tm != nil && tm.TxFrom > pin {
			pin = tm.TxFrom
		}
	}
	return pin
}

// vdWorkload writes the same records, keyed upserts and two update rounds
// into g and returns a pin after each phase (the graphs' pins differ, the
// states between them do not).
func vdWorkload(t *testing.T, g *graphpkg.Graph) []types.Instant {
	t.Helper()
	ctx := context.Background()
	for _, l := range []string{"Device", "Account"} {
		if err := g.Constraints().CreateUnique(ctx, l, "key"); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{"remote_ip", "action"} {
		if err := g.Index().CreateRelProperty("LOGON", p); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Index().CreateProperty("Device", "folder"); err != nil {
		t.Fatal(err)
	}
	for c := 0; c < vdRecords; c += 100 {
		err := g.Tx().RunContext(ctx, func(tx *graphpkg.GraphTx) error {
			for i := c; i < c+100; i++ {
				dev, _, err := tx.GetOrCreateByKey("Device", "key", fmt.Sprintf("dev-%03d", i%40),
					map[string]any{"estate": "BA", "folder": fmt.Sprintf(`C:\Program Files\Vendor%02d\bin`, i%40%9)})
				if err != nil {
					return err
				}
				acct, _, err := tx.GetOrCreateByKey("Account", "key", fmt.Sprintf("acct-%03d", i%55), map[string]any{"estate": "BA"})
				if err != nil {
					return err
				}
				vf := int64(1_790_700_000_000 + i*1000)
				_, err = tx.AddRelationshipByID("LOGON", acct.ID(), dev.ID(), map[string]any{
					"action": vdActions[i%3], "remote_ip": vdIP(i), "account_sid": fmt.Sprintf("S-1-5-21-%04d", i%15),
					"cmdline": fmt.Sprintf(`"C:\Windows\system32\svchost.exe" -k netsvcs -p -s Schedule%d`, i%400),
					"logon_type": "Network", "tkg_valid_from": vf, "record": fmt.Sprintf("r%05d", i)})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	pins := []types.Instant{vdPin(t, g)}
	rels, err := g.Rels().ByType("LOGON", store.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(rels, func(a, b *types.Relationship) int { return vdCmp(vdRecord(a), vdRecord(b)) })
	for i, r := range rels {
		if i%10 == 0 {
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"action": "LogonRevoked", "remote_ip": vdIP(i + 1)}); err != nil {
				t.Fatalf("update: %v", err)
			}
		}
	}
	pins = append(pins, vdPin(t, g))
	for i, r := range rels {
		if i%20 == 0 {
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"action": "LogonSuccess", "cmdline": "cmd.exe /c whoami"}); err != nil {
				t.Fatalf("update: %v", err)
			}
		}
	}
	return append(pins, vdPin(t, g))
}

func vdRecord(r *types.Relationship) string { v, _ := r.GetProperty("record"); s, _ := v.(string); return s }

func vdCmp(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// vdRelsCanon is the rows as sorted "properties" strings (IDs differ between
// graphs; the record property identifies a row).
func vdRelsCanon(rels []*types.Relationship) []string {
	out := make([]string, 0, len(rels))
	for _, r := range rels {
		out = append(out, fmt.Sprintf("%v", r.PropertiesMap()))
	}
	slices.Sort(out)
	return out
}

func vdNodesCanon(nodes []*types.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, fmt.Sprintf("%v", n.PropertiesMap()))
	}
	slices.Sort(out)
	return out
}

// vdAnswers asks g every question the acceptance compares.
func vdAnswers(t *testing.T, g *graphpkg.Graph, pins []types.Instant) map[string][]string {
	t.Helper()
	ans := map[string][]string{}
	must := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	rels, err := g.Rels().ByType("LOGON", store.QueryOpts{})
	must("ByType", err)
	ans["ByType LOGON"] = vdRelsCanon(rels)
	nodes, err := g.Nodes().ByLabel("Device", store.QueryOpts{})
	must("ByLabel", err)
	ans["ByLabel Device"] = vdNodesCanon(nodes)
	probes := append(append([]string{"LogonRevoked"}, vdActions...), "LogonUnknown")
	for _, a := range probes {
		got, err := g.Rels().ByTypeAndProperty("LOGON", "action", a, store.QueryOpts{})
		must("ByTypeAndProperty action", err)
		ans["action="+a] = vdRelsCanon(got)
	}
	for i := 0; i < 31; i++ {
		got, err := g.Rels().ByTypeAndProperty("LOGON", "remote_ip", vdIP(i), store.QueryOpts{})
		must("ByTypeAndProperty remote_ip", err)
		ans["remote_ip="+vdIP(i)] = vdRelsCanon(got)
	}
	for _, k := range []string{"dev-003", "dev-039", "dev-040"} {
		got, err := g.Nodes().ByLabelAndProperty("Device", "key", k, store.QueryOpts{})
		must("ByLabelAndProperty key", err)
		ans["key="+k] = vdNodesCanon(got)
	}
	got, err := g.Nodes().ByLabelAndProperty("Device", "folder", `C:\Program Files\Vendor04\bin`, store.QueryOpts{})
	must("ByLabelAndProperty folder", err)
	ans["folder"] = vdNodesCanon(got)
	for i, pin := range pins {
		asOf, err := g.Temporal().RelsAsOf(pin)
		must("RelsAsOf", err)
		ans[fmt.Sprintf("RelsAsOf pin %d", i)] = vdRelsCanon(asOf)
	}
	at, err := g.Temporal().RelsByTypeAt("LOGON", 1_790_700_000_000+500*1000)
	must("RelsByTypeAt", err)
	ans["RelsByTypeAt"] = vdRelsCanon(at)
	byPropAt, err := g.Temporal().RelsByTypePropertyAt("LOGON", "remote_ip", vdIP(0), 1_790_700_000_000+300*1000)
	must("RelsByTypePropertyAt", err)
	ans["RelsByTypePropertyAt"] = vdRelsCanon(byPropAt)
	return ans
}

// vdStoredBytes sums every key and value badger holds under dir (all shards).
func vdStoredBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "MANIFEST" {
			return err
		}
		db, err := badgerv4.Open(badgerv4.DefaultOptions(filepath.Dir(p)).WithReadOnly(true).WithLogger(nil))
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()
		return db.View(func(txn *badgerv4.Txn) error {
			it := txn.NewIterator(badgerv4.DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				total += int64(len(it.Item().Key())) + it.Item().ValueSize()
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return total
}

func vdOpenTiered(t *testing.T, dir string, dicts []store.ValueDictionary) *graphpkg.Graph {
	t.Helper()
	ts, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"Device", "Account"}, RelsByMint: true, ValueDictionaries: dicts})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// TestValueDict_Acceptance (DICT step 5): the same writes into a tiered
// store with ai-soc's kind of dictionary list and into one without give the
// same answers on every read door, index probe and history read, before and
// after a reopen, and the coded store holds fewer bytes. Catches any read
// path that sees a token, a lost or wrong value, and an index keyed on
// something other than the plain value.
func TestValueDict_Acceptance(t *testing.T) {
	t.Parallel()
	dirs := map[string]string{"plain": t.TempDir(), "coded": t.TempDir()}
	answers := map[string]map[string][]string{}
	for _, name := range []string{"plain", "coded"} {
		var dicts []store.ValueDictionary
		if name == "coded" {
			dicts = vdAcceptDicts()
		}
		g := vdOpenTiered(t, dirs[name], dicts)
		pins := vdWorkload(t, g)
		answers[name] = vdAnswers(t, g, pins)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		g = vdOpenTiered(t, dirs[name], dicts)
		reopened := vdAnswers(t, g, pins)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		for q, a := range answers[name] {
			if !slices.Equal(a, reopened[q]) {
				t.Fatalf("%s: %q changed across a reopen", name, q)
			}
		}
	}
	ipRows := 0
	for q, want := range answers["plain"] {
		got := answers["coded"][q]
		if strings.HasPrefix(q, "remote_ip=") {
			ipRows += len(want) // some ips were all updated away
		} else if len(want) == 0 && q != "action=LogonUnknown" && q != "key=dev-040" {
			t.Fatalf("plain answers %q with nothing: the question tests nothing", q)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%q: coded store answers %d rows, plain %d (first differing: %v)", q, len(got), len(want), vdFirstDiff(got, want))
		}
	}
	if ipRows < 100 {
		t.Fatalf("the remote_ip probes found %d rows: they test little", ipRows)
	}
	plain, coded := vdStoredBytes(t, dirs["plain"]), vdStoredBytes(t, dirs["coded"])
	t.Logf("stored bytes: plain %d, coded %d (%.1f %% less)", plain, coded, 100*float64(plain-coded)/float64(plain))
	if coded >= plain {
		t.Fatalf("coded store %d B, plain %d B: no saving", coded, plain)
	}
	if os.Getenv("RHO_DICT_BYTES") != "" { // evidence runs print per-dir sizes
		t.Logf("dirs: %v", dirs)
	}
}

func vdFirstDiff(a, b []string) string {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return fmt.Sprintf("%q vs %q", a[i], b[i])
		}
	}
	return "length"
}
