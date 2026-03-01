// Tutorial 002: Temporal Features (MemoryStore)
//
// Demonstrates temporal metadata, shadow properties, integrity chains,
// version tracking, and the reserved tkg_ prefix protection.
//
// Run: go run ./tutorials/002_temporal/
package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

func main() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := g.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	fmt.Println("=== 1. Auto-derived tkg_created_at ===")

	// Every entity gets an accurate creation timestamp from its snowflake ID,
	// even without calling SetTemporal.
	emp, err := g.AddNode([]string{"Employee"}, map[string]any{
		"name":       "Alice",
		"department": "Engineering",
	})
	if err != nil {
		log.Fatal(err)
	}

	createdAt, ok := g.ResolveNodeProperty(emp, types.ShadowCreatedAt)
	if !ok {
		log.Fatal("tkg_created_at not resolved")
	}
	ts := time.UnixMilli(int64(createdAt.(types.Instant)))
	fmt.Printf("Employee created at: %s (auto-derived from snowflake ID)\n",
		ts.UTC().Format(time.RFC3339))

	fmt.Println("\n=== 2. Explicit TemporalMetadata ===")

	now := types.Instant(time.Now().UnixMilli())
	validTo := types.Instant(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())

	emp.SetTemporal(&types.TemporalMetadata{
		ValidFrom: now,
		ValidTo:   validTo,
		TxFrom:    now,
		CreatedAt: now,
		UpdatedAt: now,
		CreatedBy: "hr-system",
		UpdatedBy: "hr-system",
	})
	emp.SetVersion(1)

	fmt.Printf("ValidFrom: %s\n", time.UnixMilli(int64(now)).UTC().Format(time.RFC3339))
	fmt.Printf("ValidTo:   %s\n",
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339))

	fmt.Println("\n=== 3. Relationship with Temporal Data ===")

	mgr, err := g.AddNode([]string{"Employee", "Manager"}, map[string]any{
		"name": "Bob",
	})
	if err != nil {
		log.Fatal(err)
	}

	reports, err := g.AddRelationship("REPORTS_TO", emp, mgr, map[string]any{
		"role": "direct",
	})
	if err != nil {
		log.Fatal(err)
	}

	reports.SetTemporal(&types.TemporalMetadata{
		ValidFrom: now,
		TxFrom:    now,
		CreatedAt: now,
		CreatedBy: "hr-system",
	})
	reports.SetVersion(1)

	fmt.Println("\n=== 4. All 15 Shadow Properties (Node) ===")

	shadowKeys := []string{
		types.ShadowLabels,
		types.ShadowType,
		types.ShadowValidFrom,
		types.ShadowValidTo,
		types.ShadowTxFrom,
		types.ShadowTxTo,
		types.ShadowCreatedAt,
		types.ShadowUpdatedAt,
		types.ShadowDeletedAt,
		types.ShadowCreatedBy,
		types.ShadowUpdatedBy,
		types.ShadowVersion,
		types.ShadowHash,
		types.ShadowPrevHash,
		types.ShadowBaseEntity,
	}

	fmt.Println("Node shadow properties:")
	for _, key := range shadowKeys {
		val, ok := g.ResolveNodeProperty(emp, key)
		if ok {
			fmt.Printf("  %-20s = %v\n", key, val)
		} else {
			fmt.Printf("  %-20s = (not set / not applicable)\n", key)
		}
	}

	fmt.Println("\n=== 5. Shadow Properties (Relationship) ===")

	fmt.Println("Relationship shadow properties:")
	for _, key := range shadowKeys {
		val, ok := g.ResolveRelProperty(reports, key)
		if ok {
			fmt.Printf("  %-20s = %v\n", key, val)
		} else {
			fmt.Printf("  %-20s = (not set / not applicable)\n", key)
		}
	}

	fmt.Println("\n=== 6. Integrity Chain ===")

	emp.SetIntegrity(&types.NodeIntegrity{
		Hash:     "sha256:abc123def456",
		PrevHash: "",
	})

	hash, _ := g.ResolveNodeProperty(emp, types.ShadowHash)
	prevHash, _ := g.ResolveNodeProperty(emp, types.ShadowPrevHash)
	fmt.Printf("Node hash:      %s\n", hash)
	fmt.Printf("Node prev_hash: %q (empty = first version)\n", prevHash)

	reports.SetIntegrity(&types.RelIntegrity{
		Hash:     "sha256:789abc",
		PrevHash: "sha256:abc123def456",
	})

	rHash, _ := g.ResolveRelProperty(reports, types.ShadowHash)
	rPrevHash, _ := g.ResolveRelProperty(reports, types.ShadowPrevHash)
	fmt.Printf("Rel hash:       %s\n", rHash)
	fmt.Printf("Rel prev_hash:  %s\n", rPrevHash)

	fmt.Println("\n=== 7. Version Tracking ===")

	fmt.Printf("Employee version: %d\n", emp.Version())
	ver, _ := g.ResolveNodeProperty(emp, types.ShadowVersion)
	fmt.Printf("Via shadow: tkg_version = %v\n", ver)

	emp.SetVersion(2)
	ver2, _ := g.ResolveNodeProperty(emp, types.ShadowVersion)
	fmt.Printf("After update: tkg_version = %v\n", ver2)

	fmt.Println("\n=== 8. Base Entity ID (Version Chain) ===")

	// Create a new version linked back to the original.
	empV2, err := g.AddNode([]string{"Employee"}, map[string]any{
		"name":       "Alice",
		"department": "Platform",
	})
	if err != nil {
		log.Fatal(err)
	}

	empV2.SetTemporal(&types.TemporalMetadata{
		ValidFrom: now,
		CreatedAt: now,
		CreatedBy: "hr-system",
	})
	empV2.Temporal().SetBaseEntityID(emp.InternalID().SnowflakeID())

	baseID, ok := g.ResolveNodeProperty(empV2, types.ShadowBaseEntity)
	if ok {
		fmt.Printf("empV2 base entity: %d (points to original %d)\n",
			baseID, emp.InternalID().SnowflakeID())
	}

	fmt.Println("\n=== 9. Reserved Prefix Protection ===")

	err = emp.SetProperty("tkg_custom", "should fail")
	if errors.Is(err, types.ErrReservedPrefix) {
		fmt.Printf("SetProperty('tkg_custom'): correctly rejected\n")
	}

	fmt.Println("\n=== Done ===")
}
