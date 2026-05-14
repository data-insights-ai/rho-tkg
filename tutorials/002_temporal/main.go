// Tutorial 002: Temporal Features (MemoryStore)
//
// Demonstrates temporal metadata, shadow properties, integrity chains,
// version tracking, and the reserved tkg_ prefix protection.
//
// Two ways to set temporal metadata on creation:
//   - Props-based (recommended): pass tkg_valid_from, tkg_valid_to, tkg_created_at
//     in the properties map — extracted before validation, merged with auto-set TxFrom.
//   - Direct: call node.SetTemporal() after creation (mutates in-place, not persisted
//     unless followed by an update).
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

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// commas formats an integer with thousand separators: 1234567 -> "1,234,567".
func commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	if n < 0 {
		return "-" + commas(-n)
	}
	return insertCommas(s)
}

// commasFmt formats any %d-compatible value with thousand separators.
func commasFmt(v any) string {
	return insertCommas(fmt.Sprintf("%d", v))
}

func insertCommas(s string) string {
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
	emp, err := g.Nodes.Add(context.Background(), []string{"Employee"}, map[string]any{
		"name":       "Alice",
		"department": "Engineering",
	})
	if err != nil {
		log.Fatal(err)
	}

	createdAt, ok := g.Resolve.NodeProperty(emp, types.ShadowCreatedAt)
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

	fmt.Println("\n=== 3. Temporal via Props (Recommended for Creation) ===")

	// Pass tkg_valid_from / tkg_valid_to / tkg_created_at in the props map.
	// They are extracted before property validation (tkg_ prefix is reserved),
	// merged into TemporalMetadata alongside the auto-set TxFrom, and never
	// stored as regular properties. This is the recommended approach for
	// setting temporal at creation time — it works through AddNode,
	// AddNodeWithContext, BatchBuilder.AddNode, and GraphTx.AddNode.
	eventTime := types.Instant(time.Date(2026, 3, 9, 14, 30, 0, 0, time.UTC).UnixMilli())
	farFuture := types.Instant(time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli())

	sensor, err := g.Nodes.Add(context.Background(), []string{"Sensor"}, map[string]any{
		"name":           "temperature-east-wing",
		"location":       "building-A",
		"tkg_valid_from": int64(eventTime), // when this fact becomes valid
		"tkg_valid_to":   int64(farFuture), // open-ended (far future sentinel)
		"tkg_created_at": int64(eventTime), // domain creation time
	})
	if err != nil {
		log.Fatal(err)
	}

	stm := sensor.Temporal()
	fmt.Printf("Sensor ValidFrom:  %s\n",
		time.UnixMilli(int64(stm.ValidFrom)).UTC().Format(time.RFC3339))
	fmt.Printf("Sensor ValidTo:    %s\n",
		time.UnixMilli(int64(stm.ValidTo)).UTC().Format(time.RFC3339))
	fmt.Printf("Sensor CreatedAt:  %s\n",
		time.UnixMilli(int64(stm.CreatedAt)).UTC().Format(time.RFC3339))
	fmt.Printf("Sensor TxFrom:     %s (auto-set by tkg)\n",
		time.UnixMilli(int64(stm.TxFrom)).UTC().Format(time.RFC3339))

	// Verify tkg_ keys are NOT stored as regular properties.
	if v, _ := sensor.GetProperty("tkg_valid_from"); v != nil {
		log.Fatal("tkg_valid_from should not be a regular property")
	}
	fmt.Println("tkg_valid_from NOT in regular properties (correctly extracted)")

	// Works with relationships too:
	reading, err := g.Rels.Add(context.Background(), "HAS_READING", sensor, emp, map[string]any{
		"value":          float64(22.5),
		"tkg_valid_from": int64(eventTime),
		"tkg_created_at": int64(eventTime),
	})
	if err != nil {
		log.Fatal(err)
	}
	rtmReading := reading.Temporal()
	fmt.Printf("Reading rel ValidFrom: %s\n",
		time.UnixMilli(int64(rtmReading.ValidFrom)).UTC().Format(time.RFC3339))

	fmt.Println("\n=== 4. Relationship with Temporal Data (Direct SetTemporal) ===")

	mgr, err := g.Nodes.Add(context.Background(), []string{"Employee", "Manager"}, map[string]any{
		"name": "Bob",
	})
	if err != nil {
		log.Fatal(err)
	}

	reports, err := g.Rels.Add(context.Background(), "REPORTS_TO", emp, mgr, map[string]any{
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

	fmt.Println("\n=== 5. All 15 Shadow Properties (Node) ===")

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
		val, ok := g.Resolve.NodeProperty(emp, key)
		if ok {
			fmt.Printf("  %-20s = %v\n", key, val)
		} else {
			fmt.Printf("  %-20s = (not set / not applicable)\n", key)
		}
	}

	fmt.Println("\n=== 6. Shadow Properties (Relationship) ===")

	fmt.Println("Relationship shadow properties:")
	for _, key := range shadowKeys {
		val, ok := g.Resolve.RelProperty(reports, key)
		if ok {
			fmt.Printf("  %-20s = %v\n", key, val)
		} else {
			fmt.Printf("  %-20s = (not set / not applicable)\n", key)
		}
	}

	fmt.Println("\n=== 7. Integrity Chain ===")

	emp.SetIntegrity(&types.NodeIntegrity{
		Hash:     "sha256:abc123def456",
		PrevHash: "",
	})

	hash, _ := g.Resolve.NodeProperty(emp, types.ShadowHash)
	prevHash, _ := g.Resolve.NodeProperty(emp, types.ShadowPrevHash)
	fmt.Printf("Node hash:      %s\n", hash)
	fmt.Printf("Node prev_hash: %q (empty = first version)\n", prevHash)

	reports.SetIntegrity(&types.RelIntegrity{
		Hash:     "sha256:789abc",
		PrevHash: "sha256:abc123def456",
	})

	rHash, _ := g.Resolve.RelProperty(reports, types.ShadowHash)
	rPrevHash, _ := g.Resolve.RelProperty(reports, types.ShadowPrevHash)
	fmt.Printf("Rel hash:       %s\n", rHash)
	fmt.Printf("Rel prev_hash:  %s\n", rPrevHash)

	fmt.Println("\n=== 8. Version Tracking ===")

	fmt.Printf("Employee version: %d\n", emp.Version())
	ver, _ := g.Resolve.NodeProperty(emp, types.ShadowVersion)
	fmt.Printf("Via shadow: tkg_version = %v\n", ver)

	emp.SetVersion(2)
	ver2, _ := g.Resolve.NodeProperty(emp, types.ShadowVersion)
	fmt.Printf("After update: tkg_version = %v\n", ver2)

	fmt.Println("\n=== 9. Base Entity ID (Version Chain) ===")

	// Create a new version linked back to the original.
	empV2, err := g.Nodes.Add(context.Background(), []string{"Employee"}, map[string]any{
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
	empV2.Temporal().SetBaseEntityID(types.EntityID(emp.ID()))

	baseID, ok := g.Resolve.NodeProperty(empV2, types.ShadowBaseEntity)
	if ok {
		fmt.Printf("empV2 base entity: %s (points to original %s)\n",
			commasFmt(baseID), commas(int64(emp.ID())))
	}

	fmt.Println("\n=== 10. Reserved Prefix Protection ===")

	err = emp.SetProperty("tkg_custom", "should fail")
	if errors.Is(err, types.ErrReservedPrefix) {
		fmt.Printf("SetProperty('tkg_custom'): correctly rejected\n")
	}

	fmt.Println("\n=== Done ===")
}
