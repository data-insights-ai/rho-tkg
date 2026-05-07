// Tutorial 004: Full Feature Showcase
//
// Comprehensive exercise combining all tkg/v3 features: multi-label nodes,
// rich properties, temporal metadata, integrity chains, shadow properties,
// adjacency queries, deep copy, cascade delete, and error handling.
//
// Scenario: Knowledge graph of a tech company.
//
// Run: go run ./tutorials/004_full_features/
package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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

func main() {
	// BadgerInMemory gives BadgerStore behavior without disk I/O.
	g, err := graph.New(graph.Config{BadgerInMemory: true})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := g.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// ----------------------------------------------------------------
	fmt.Println("=== 1. Multi-label Nodes ===")
	// ----------------------------------------------------------------

	eng, err := g.AddNode([]string{"Department"}, map[string]any{
		"name":   "Engineering",
		"budget": float64(2_500_000),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Department: %v\n", g.NodeLabels(eng))

	atlas, err := g.AddNode([]string{"Project", "Internal"}, map[string]any{
		"name":     "Atlas",
		"priority": int64(1),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Project: %v (2 labels)\n", g.NodeLabels(atlas))

	alice, err := g.AddNode([]string{"Person", "Employee", "Engineer"}, map[string]any{
		"name":  "Alice",
		"email": "alice@example.com",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Person: %v (3 labels)\n", g.NodeLabels(alice))

	bob, err := g.AddNode([]string{"Person", "Employee"}, map[string]any{
		"name": "Bob",
	})
	if err != nil {
		log.Fatal(err)
	}

	// ----------------------------------------------------------------
	fmt.Println("\n=== 2. Rich Properties ===")
	// ----------------------------------------------------------------

	richNode, err := g.AddNode([]string{"Config"}, map[string]any{
		"name":    "app-config",
		"enabled": true,
		"count":   int64(42),
		"ratio":   float64(3.14),
		"tags":    []any{"prod", "v2", "stable"},
		"limits":  map[string]any{"cpu": int64(4), "memory": int64(8192)},
		"nested":  []any{int64(1), int64(2), []any{int64(3), int64(4)}},
	})
	if err != nil {
		log.Fatal(err)
	}

	propsMap := richNode.PropertiesMap()
	fmt.Printf("Config properties (%d keys):\n", len(propsMap))
	for k, v := range propsMap {
		fmt.Printf("  %-10s = %v (%T)\n", k, v, v)
	}

	// ----------------------------------------------------------------
	fmt.Println("\n=== 3. Relationships ===")
	// ----------------------------------------------------------------

	worksIn, err := g.AddRelationship("WORKS_IN", alice, eng, map[string]any{
		"since": int64(2022),
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = g.AddRelationship("WORKS_IN", bob, eng, nil)
	if err != nil {
		log.Fatal(err)
	}

	assignedTo, err := g.AddRelationship("ASSIGNED_TO", alice, atlas, map[string]any{
		"role": "lead",
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = g.AddRelationship("ASSIGNED_TO", bob, atlas, map[string]any{
		"role": "contributor",
	})
	if err != nil {
		log.Fatal(err)
	}

	manages, err := g.AddRelationship("MANAGES", alice, bob, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Self-loop: Alice reviews her own code.
	selfLoop, err := g.AddRelationship("REVIEWS", alice, alice, map[string]any{
		"type": "self-review",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Self-loop: Alice -[REVIEWS]-> Alice (ID: %s)\n",
		commas(int64(selfLoop.ID())))

	nc, err := g.NodeCount()
	if err != nil {
		log.Fatal(err)
	}
	rc, err := g.RelationshipCount()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total: %d nodes, %d relationships\n", nc, rc)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 4. Temporal Metadata ===")
	// ----------------------------------------------------------------

	now := types.Instant(time.Now().UnixMilli())
	yearEnd := types.Instant(
		time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli())

	alice.SetTemporal(&types.TemporalMetadata{
		ValidFrom: now,
		ValidTo:   yearEnd,
		TxFrom:    now,
		CreatedAt: now,
		UpdatedAt: now,
		CreatedBy: "admin",
		UpdatedBy: "admin",
	})
	alice.SetVersion(1)

	// Simulate an update.
	alice.Temporal().UpdatedAt = now + 1000
	alice.Temporal().UpdatedBy = "hr-bot"
	alice.SetVersion(2)

	fmt.Printf("Alice version: %d, updatedBy: %s\n",
		alice.Version(), alice.Temporal().UpdatedBy)

	// Soft-delete a relationship.
	worksIn.SetTemporal(&types.TemporalMetadata{
		ValidFrom: now,
		TxFrom:    now,
		CreatedAt: now,
		DeletedAt: now + 5000,
		CreatedBy: "system",
	})

	deletedAt, _ := g.ResolveRelProperty(worksIn, types.ShadowDeletedAt)
	fmt.Printf("worksIn soft-deleted at: %v\n", deletedAt)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 5. Integrity Chain ===")
	// ----------------------------------------------------------------

	alice.SetIntegrity(&types.NodeIntegrity{
		Hash:     "sha256:v1-abc123",
		PrevHash: "",
	})
	hash, _ := g.ResolveNodeProperty(alice, types.ShadowHash)
	prev, _ := g.ResolveNodeProperty(alice, types.ShadowPrevHash)
	fmt.Printf("Alice v1: hash=%s, prevHash=%q\n", hash, prev)

	// Version 2 hash chain.
	alice.SetIntegrity(&types.NodeIntegrity{
		Hash:     "sha256:v2-def456",
		PrevHash: "sha256:v1-abc123",
	})
	hash2, _ := g.ResolveNodeProperty(alice, types.ShadowHash)
	prev2, _ := g.ResolveNodeProperty(alice, types.ShadowPrevHash)
	fmt.Printf("Alice v2: hash=%s, prevHash=%s\n", hash2, prev2)

	manages.SetIntegrity(&types.RelIntegrity{
		Hash: "sha256:rel-001",
	})
	rh, _ := g.ResolveRelProperty(manages, types.ShadowHash)
	fmt.Printf("MANAGES hash: %s\n", rh)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 6. All 15 Shadow Properties ===")
	// ----------------------------------------------------------------

	shadowKeys := []string{
		types.ShadowLabels, types.ShadowType,
		types.ShadowValidFrom, types.ShadowValidTo,
		types.ShadowTxFrom, types.ShadowTxTo,
		types.ShadowCreatedAt, types.ShadowUpdatedAt, types.ShadowDeletedAt,
		types.ShadowCreatedBy, types.ShadowUpdatedBy,
		types.ShadowVersion,
		types.ShadowHash, types.ShadowPrevHash,
		types.ShadowBaseEntity,
	}

	fmt.Println("Node (Alice):")
	for _, key := range shadowKeys {
		val, ok := g.ResolveNodeProperty(alice, key)
		if ok {
			fmt.Printf("  %-20s = %v\n", key, val)
		} else {
			fmt.Printf("  %-20s = (n/a)\n", key)
		}
	}

	fmt.Println("Relationship (ASSIGNED_TO):")
	assignedTo.SetTemporal(&types.TemporalMetadata{
		CreatedAt: now,
		CreatedBy: "pm-tool",
	})
	assignedTo.SetVersion(1)
	for _, key := range shadowKeys {
		val, ok := g.ResolveRelProperty(assignedTo, key)
		if ok {
			fmt.Printf("  %-20s = %v\n", key, val)
		} else {
			fmt.Printf("  %-20s = (n/a)\n", key)
		}
	}

	// ----------------------------------------------------------------
	fmt.Println("\n=== 7. Adjacency Queries ===")
	// ----------------------------------------------------------------

	// All outgoing from Alice.
	outAll, err := g.OutgoingRelationships(alice.ID(), "")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Alice outgoing (all types): %d relationships\n", len(outAll))
	for _, r := range outAll {
		fmt.Printf("  -[%s]-> %s\n",
			g.RelationshipType(r), commas(int64(r.EndNodeID().SnowflakeID())))
	}

	// All incoming to Bob.
	inBob, err := g.IncomingRelationships(bob.ID(), "")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Bob incoming (all types): %d relationships\n", len(inBob))

	// Filtered by type.
	assignedOnly, err := g.OutgoingRelationships(
		alice.ID(), "ASSIGNED_TO")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Alice outgoing ASSIGNED_TO: %d\n", len(assignedOnly))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 8. Property Operations ===")
	// ----------------------------------------------------------------

	if err := alice.SetProperty("phone", "+1-555-0100"); err != nil {
		log.Fatal(err)
	}
	phone, ok := alice.GetProperty("phone")
	fmt.Printf("Set phone: %s (found: %v)\n", phone, ok)

	deleted, err := alice.DeleteProperty("phone")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Delete phone: removed=%v\n", deleted)

	_, ok = alice.GetProperty("phone")
	fmt.Printf("After delete: found=%v\n", ok)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 9. Deep Copy ===")
	// ----------------------------------------------------------------

	aliceCopy := alice.DeepCopy()
	if err := aliceCopy.SetProperty("copy_marker", "this is a copy"); err != nil {
		log.Fatal(err)
	}
	_, hasCopyMarker := alice.GetProperty("copy_marker")
	fmt.Printf("Original has 'copy_marker': %v (should be false)\n", hasCopyMarker)
	copyMarker, _ := aliceCopy.GetProperty("copy_marker")
	fmt.Printf("Copy has 'copy_marker': %s\n", copyMarker)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 10. Cascade Delete ===")
	// ----------------------------------------------------------------

	nc, _ = g.NodeCount()
	rc, _ = g.RelationshipCount()
	fmt.Printf("Before delete: %d nodes, %d rels\n", nc, rc)

	// Delete Alice — cascade removes all her relationships.
	aliceID := alice.ID()
	if err := g.DeleteNode(aliceID); err != nil {
		log.Fatal(err)
	}

	nc, _ = g.NodeCount()
	rc, _ = g.RelationshipCount()
	fmt.Printf("After deleting Alice: %d nodes, %d rels\n", nc, rc)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 11. Error Handling ===")
	// ----------------------------------------------------------------

	_, err = g.GetNode(aliceID)
	if errors.Is(err, store.ErrNodeNotFound) {
		fmt.Println("GetNode(deleted Alice): ErrNodeNotFound")
	}

	_, err = g.GetRelationship(worksIn.ID())
	if errors.Is(err, store.ErrRelNotFound) {
		fmt.Println("GetRelationship(cascaded rel): ErrRelNotFound")
	}

	// ----------------------------------------------------------------
	fmt.Println("\n=== 12. Version Tracking ===")
	// ----------------------------------------------------------------

	bob.SetVersion(3)
	ver, _ := g.ResolveNodeProperty(bob, types.ShadowVersion)
	fmt.Printf("Bob version via shadow: %v\n", ver)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 13. Property Validation ===")
	// ----------------------------------------------------------------

	// Reserved prefix rejected.
	err = bob.SetProperty("tkg_custom", "fail")
	if errors.Is(err, types.ErrReservedPrefix) {
		fmt.Printf("SetProperty('tkg_custom'): rejected (%v)\n", err)
	}

	// Unsupported type (channel) rejected.
	ch := make(chan int)
	err = bob.SetProperty("bad", ch)
	if errors.Is(err, types.ErrUnsupportedValueType) {
		fmt.Printf("SetProperty(chan): rejected (%v)\n", err)
	}

	fmt.Println("\n=== Done ===")
}
