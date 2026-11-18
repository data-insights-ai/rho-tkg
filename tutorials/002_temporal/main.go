// Tutorial 002: Temporal Graph (MemoryStore)
//
// A temporal knowledge graph keeps two time axes for every node and
// relationship:
//
//   - valid time: when a fact is true in the world. A create or update sets it
//     with the tkg_valid_from / tkg_valid_to keys in its property map; without
//     them a version is valid from the instant the graph recorded it.
//   - transaction time: when the graph recorded (and later superseded) a fact.
//     The graph's clock stamps it on every write (TxFrom, TxTo).
//
// Every Update writes a new version; the old one stays readable in History and
// is linked to the new one by a hash chain the graph computes. This tutorial
// creates an employee, changes it twice, and reads it back on both axes.
//
// Run: go run ./tutorials/002_temporal/
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// commas formats an integer with thousand separators: 1234567 -> "1,234,567".
func commas(n int64) string {
	if n < 0 {
		return "-" + commas(-n)
	}
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var buf []byte
	pre := len(s) % 3
	if pre > 0 {
		buf = append(buf, s[:pre]...)
	}
	for i := pre; i < len(s); i += 3 {
		if len(buf) > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, s[i:i+3]...)
	}
	return string(buf)
}

// ts prints an instant as UTC with milliseconds; the open end (0) prints as
// "open".
func ts(t types.Instant) string {
	if t == 0 {
		return "open"
	}
	return t.Time().Format("2006-01-02T15:04:05.000Z")
}

// short returns the first 12 hex digits of a hash, enough to tell versions
// apart in the output.
func short(h any) string {
	s, _ := h.(string)
	if len(s) > 12 {
		return s[:12]
	}
	if s == "" {
		return "(none)"
	}
	return s
}

func main() {
	ctx := context.Background()

	// AllowTxBackfill is off by default; section 9 turns it on to load facts
	// at a past transaction instant.
	g, err := graph.New(graph.Config{AllowTxBackfill: true})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := g.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	fmt.Println("=== 1. Create with a Valid Time ===")

	// Alice joined Engineering on 15 January 2024. The graph records the fact
	// now; tkg_valid_from says since when it is true.
	joined := types.InstantFromTime(time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC))
	alice, err := g.Nodes().Add(ctx, []string{"Employee"}, map[string]any{
		"name":           "Alice",
		"department":     "Engineering",
		"tkg_valid_from": joined,
	})
	if err != nil {
		log.Fatal(err)
	}
	from, to, _ := alice.ValidRange()
	txFrom, _, _ := alice.TxStamps()
	fmt.Printf("Alice ID %s, version %d\n", commas(int64(alice.ID())), alice.Version())
	fmt.Printf("  valid       [%s, %s)\n", ts(from), ts(to))
	fmt.Printf("  recorded at %s (TxFrom, stamped by the graph clock)\n", ts(txFrom))
	_, stored := alice.GetProperty("tkg_valid_from")
	fmt.Printf("  tkg_valid_from stored as a property: %v (it is metadata)\n", stored)

	// The pin "everything recorded so far". Take read coordinates from the
	// graph clock (NowTx), not from time.Now(): the clock can run ahead of
	// wall time.
	pinCreated, err := g.Temporal().NowTx()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n=== 2. Update Writes New Versions ===")

	// No tkg_valid_from: each new version is valid from the instant it is
	// recorded. The previous version gets a TxTo (superseded in transaction
	// time) and, on the effective timeline (section 6), ends where the new one
	// starts.
	if _, err := g.Nodes().Update(ctx, alice.ID(), map[string]any{"department": "Platform"}); err != nil {
		log.Fatal(err)
	}
	pinMoved, err := g.Temporal().NowTx()
	if err != nil {
		log.Fatal(err)
	}
	current, err := g.Nodes().Update(ctx, alice.ID(), map[string]any{"title": "Staff Engineer"})
	if err != nil {
		log.Fatal(err)
	}
	dept, _ := current.GetProperty("department")
	title, _ := current.GetProperty("title")
	fmt.Printf("Current: version %d, department=%s, title=%s\n", current.Version(), dept, title)

	// Reserved keys other than the temporal and provenance ones are refused.
	_, err = g.Nodes().Update(ctx, alice.ID(), map[string]any{"tkg_custom": "x"})
	fmt.Printf("Update with tkg_custom refused: %v\n", errors.Is(err, types.ErrReservedPrefix))

	fmt.Println("\n=== 3. History and the Hash Chain ===")

	// History returns the superseded versions, oldest first; the current one
	// is what Get (or Update) returns. Each version's PrevHash is its
	// predecessor's Hash; the graph computes both on every write. A raw row's
	// valid start is read through the store's valid axis (a version written
	// without tkg_valid_from stores no start and is valid from its TxFrom).
	history, err := g.Nodes().History(alice.ID())
	if err != nil {
		log.Fatal(err)
	}
	axis, err := g.Admin().ValidAxis()
	if err != nil {
		log.Fatal(err)
	}
	var prevHash any
	linked := true
	for _, v := range append(history, current) {
		txF, txT, _ := v.TxStamps()
		d, _ := v.GetProperty("department")
		hash, _ := g.Resolve().NodeProperty(v, types.ShadowHash)
		prev, _ := g.Resolve().NodeProperty(v, types.ShadowPrevHash)
		if prevHash != nil && prev != prevHash {
			linked = false
		}
		prevHash = hash
		fmt.Printf("  v%d %-11s valid from %s  tx [%s, %s)\n",
			v.Version(), d, ts(axis.NodeStart(v)), ts(txF), ts(txT))
		fmt.Printf("     hash %s  prev %s\n", short(hash), short(prev))
	}
	fmt.Printf("History rows: %d; every PrevHash names the version before: %v\n", len(history), linked)

	ok, err := g.Hash().VerifyNodeChain(alice.ID())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("VerifyNodeChain: %v (each hash checked against its version's content, each PrevHash link followed)\n", ok)

	fmt.Println("\n=== 4. Valid-Time Reads (what was true when) ===")

	// NodeAt answers the version valid at an instant, as the graph knows it now.
	midYear := types.InstantFromTime(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	now, err := g.Temporal().NowTx()
	if err != nil {
		log.Fatal(err)
	}
	for _, at := range []types.Instant{midYear, now} {
		n, err := g.Temporal().NodeAt(alice.ID(), at)
		if err != nil {
			log.Fatal(err)
		}
		d, _ := n.GetProperty("department")
		fmt.Printf("  valid at %s: v%d %s\n", ts(at), n.Version(), d)
	}
	before := types.InstantFromTime(time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC))
	_, err = g.Temporal().NodeAt(alice.ID(), before)
	fmt.Printf("  valid at %s: ErrNoVersionValidAt=%v (before she joined)\n",
		ts(before), errors.Is(err, graph.ErrNoVersionValidAt))

	fmt.Println("\n=== 5. Transaction-Time Reads (what the graph believed when) ===")

	// NodeAsOf is a record door: the newest version recorded by the pin.
	asOf, err := g.Temporal().NodeAsOf(alice.ID(), pinCreated)
	if err != nil {
		log.Fatal(err)
	}
	d, _ := asOf.GetProperty("department")
	fmt.Printf("  NodeAsOf(pin after create): v%d %s\n", asOf.Version(), d)

	// NodeAtTx is a state door on both axes: valid at one instant, as recorded
	// at a pin. "Where does Alice work now?" asked of the graph's belief at
	// each pin.
	for _, p := range []struct {
		name string
		pin  types.Instant
	}{{"after create", pinCreated}, {"after the move", pinMoved}, {"now", now}} {
		n, err := g.Temporal().NodeAtTx(alice.ID(), now, p.pin)
		if err != nil {
			log.Fatal(err)
		}
		d, _ := n.GetProperty("department")
		t, _ := n.GetProperty("title")
		fmt.Printf("  NodeAtTx(valid now, pin %s): v%d %s, title=%v\n", p.name, n.Version(), d, t)
	}

	fmt.Println("\n=== 6. Effective Timeline ===")

	// The effective timeline at a pin tiles valid time with the version that
	// holds at each instant: half-open segments, ascending, no overlaps.
	segs, err := g.Temporal().NodeEffectiveTimeline(alice.ID(), now)
	if err != nil {
		log.Fatal(err)
	}
	for _, s := range segs {
		d, _ := s.Node.GetProperty("department")
		fmt.Printf("  [%s, %s)  v%d %s\n", ts(s.ValidFrom), ts(s.ValidTo), s.Node.Version(), d)
	}
	// The same timeline as believed right after the create has one open segment.
	segs, err = g.Temporal().NodeEffectiveTimeline(alice.ID(), pinCreated)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  at the pin after create: %d segment(s)\n", len(segs))

	fmt.Println("\n=== 7. Shadow Properties ===")

	// The metadata reads as 22 virtual tkg_* properties through g.Resolve().
	// The relationship-only keys (tkg_type, tkg_from_hash, tkg_to_hash) do not
	// resolve on a node; unset ones resolve to their zero value (0, "", nil).
	// tkg_valid_from 0 is "no claim": this version is valid from tkg_tx_from.
	shadowKeys := []string{
		types.ShadowID, types.ShadowLabels, types.ShadowType,
		types.ShadowValidFrom, types.ShadowValidTo, types.ShadowTxFrom, types.ShadowTxTo,
		types.ShadowCreatedAt, types.ShadowUpdatedAt, types.ShadowDeletedAt,
		types.ShadowCreatedBy, types.ShadowUpdatedBy, types.ShadowVersion,
		types.ShadowHash, types.ShadowPrevHash, types.ShadowFromHash, types.ShadowToHash,
		types.ShadowAuthorID, types.ShadowSignature, types.ShadowAuthorizedBy, types.ShadowAuthLevel,
		types.ShadowBaseEntity,
	}
	resolved := 0
	for _, key := range shadowKeys {
		if v, ok := g.Resolve().NodeProperty(current, key); ok {
			resolved++
			if key == types.ShadowHash || key == types.ShadowPrevHash {
				v = short(v) + "..."
			}
			fmt.Printf("  %-18s = %v\n", key, v)
		}
	}
	fmt.Printf("  %d of %d keys resolved for this node\n", resolved, len(shadowKeys))

	fmt.Println("\n=== 8. Relationships Are Versioned Too ===")

	team, err := g.Nodes().Add(ctx, []string{"Team"}, map[string]any{"name": "Platform"})
	if err != nil {
		log.Fatal(err)
	}
	member, err := g.Rels().Add(ctx, "MEMBER_OF", current, team, map[string]any{"role": "engineer"})
	if err != nil {
		log.Fatal(err)
	}
	member, err = g.Rels().Update(ctx, member.ID(), map[string]any{"role": "tech lead"})
	if err != nil {
		log.Fatal(err)
	}
	relHistory, err := g.Rels().History(member.ID())
	if err != nil {
		log.Fatal(err)
	}
	relOK, err := g.Hash().VerifyRelChain(member.ID())
	if err != nil {
		log.Fatal(err)
	}
	role, _ := member.GetProperty("role")
	fmt.Printf("MEMBER_OF: version %d role=%s, %d superseded row(s) in History, VerifyRelChain=%v\n",
		member.Version(), role, len(relHistory), relOK)

	fmt.Println("\n=== 9. Transaction-Time Backfill ===")

	// Loading a record from another system that recorded it on 1 June 2025:
	// AddWithTx stamps that instant as TxFrom (needs Config.AllowTxBackfill,
	// else ErrTxBackfillDisabled). The fact was valid from 1 May 2025.
	recorded := types.InstantFromTime(time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC))
	contract, err := g.Nodes().AddWithTx(ctx, []string{"Contract"}, map[string]any{
		"client":         "Acme",
		"tkg_valid_from": types.InstantFromTime(time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)),
	}, recorded)
	if err != nil {
		log.Fatal(err)
	}
	cFrom, _, _ := contract.ValidRange()
	cTx, _, _ := contract.TxStamps()
	fmt.Printf("Contract valid from %s, recorded at %s\n", ts(cFrom), ts(cTx))
	_, err = g.Temporal().NodeAsOf(contract.ID(), recorded-1)
	fmt.Printf("  NodeAsOf(1 ms before the record): ErrNoVersionAsOf=%v\n", errors.Is(err, graph.ErrNoVersionAsOf))
	if _, err := g.Temporal().NodeAsOf(contract.ID(), recorded); err != nil {
		log.Fatal(err)
	}
	fmt.Println("  NodeAsOf(the record instant): found")

	fmt.Println("\n=== Done ===")
}
