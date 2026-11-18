// Tutorial 004: Properties, Copies, Transactions and Constraints
//
// What tutorials 001 and 002 do not show: rich and nested property values,
// frozen rows and DeepCopy, the value types a property refuses, and three
// write doors beside the single-call ones: a transaction (g.Tx), a batch
// (g.Batch) and a unique constraint (g.Constraints).
//
// Scenario: the configuration and staff of a small tech company, on an
// in-memory Badger store (graph.Config{BadgerInMemory}).
//
// Run: go run ./tutorials/004_full_features/
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"

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

func main() {
	ctx := context.Background()

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
	fmt.Println("=== 1. Rich and Nested Properties ===")
	// ----------------------------------------------------------------

	// Scalars keep their Go type (an int64 stays an int64), lists and maps
	// nest up to 32 levels, and every value is part of the version's hash.
	cfg, err := g.Nodes().Add(ctx, []string{"Config"}, map[string]any{
		"name":    "app-config",
		"enabled": true,
		"count":   int64(42),
		"ratio":   3.14,
		"blob":    []byte{0xca, 0xfe},
		"tags":    []any{"prod", "v2", "stable"},
		"limits":  map[string]any{"cpu": int64(4), "memory": int64(8192)},
		"nested":  []any{int64(1), int64(2), []any{int64(3), int64(4)}},
	})
	if err != nil {
		log.Fatal(err)
	}

	stored, err := g.Nodes().Get(ctx, cfg.ID())
	if err != nil {
		log.Fatal(err)
	}
	props := stored.PropertiesMap()
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	fmt.Printf("Config as stored (%d keys):\n", len(keys))
	for _, k := range keys {
		fmt.Printf("  %-8s = %v (%T)\n", k, props[k], props[k])
	}

	// ----------------------------------------------------------------
	fmt.Println("\n=== 2. Frozen Rows and DeepCopy ===")
	// ----------------------------------------------------------------

	// Plural and scan reads (ByLabel, ForEach...) hand out the store's own
	// frozen rows, shared with other readers: a mutator refuses them.
	rows, err := g.Nodes().ByLabel("Config", graph.QueryOpts{})
	if err != nil || len(rows) != 1 {
		log.Fatalf("ByLabel(Config): %d rows, %v", len(rows), err)
	}
	row := rows[0]
	err = row.SetProperty("count", int64(43))
	fmt.Printf("ByLabel row frozen: %v, SetProperty refused with ErrFrozenNode: %v\n",
		row.IsFrozen(), errors.Is(err, types.ErrFrozenNode))

	// DeepCopy thaws: a private copy, nested values included.
	cp := row.DeepCopy()
	if err := cp.SetProperty("count", int64(43)); err != nil {
		log.Fatal(err)
	}
	limits, _ := cp.GetProperty("limits") // GetProperty returns a copy of a map
	limits.(map[string]any)["cpu"] = int64(16)
	if err := cp.SetProperty("limits", limits); err != nil {
		log.Fatal(err)
	}
	cpCount, _ := cp.GetProperty("count")
	cpLimits, _ := cp.GetProperty("limits")
	rowCount, _ := row.GetProperty("count")
	rowLimits, _ := row.GetProperty("limits")
	fmt.Printf("Copy:     count=%v limits=%v\n", cpCount, cpLimits)
	fmt.Printf("Original: count=%v limits=%v\n", rowCount, rowLimits)

	// A changed copy is a local value: only a write door reaches the graph.
	current, err := g.Nodes().Get(ctx, cfg.ID())
	if err != nil {
		log.Fatal(err)
	}
	graphCount, _ := current.GetProperty("count")
	fmt.Printf("Graph before Update: count=%v\n", graphCount)
	current, err = g.Nodes().Update(ctx, cfg.ID(), map[string]any{"count": cpCount, "limits": cpLimits})
	if err != nil {
		log.Fatal(err)
	}
	graphCount, _ = current.GetProperty("count")
	fmt.Printf("Graph after Update:  count=%v (version %d)\n", graphCount, current.Version())

	// ----------------------------------------------------------------
	fmt.Println("\n=== 3. Refused Value Types ===")
	// ----------------------------------------------------------------

	// Property values are limited to what the graph can hash, copy and store:
	// a channel, a function or an unregistered struct is refused before
	// anything is written.
	before, _ := g.Nodes().Count()
	for _, bad := range []struct {
		name  string
		value any
	}{
		{"chan", make(chan int)},
		{"func", func() {}},
		{"struct", struct{ X int }{1}},
		{"nested chan", []any{"ok", make(chan int)}},
	} {
		_, err := g.Nodes().Add(ctx, []string{"Config"}, map[string]any{"bad": bad.value})
		fmt.Printf("  Add with a %-11s value: ErrUnsupportedValueType=%v\n",
			bad.name, errors.Is(err, types.ErrUnsupportedValueType))
	}
	after, _ := g.Nodes().Count()
	fmt.Printf("Node count unchanged: %v (%d)\n", before == after, after)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 4. Transactions (g.Tx) ===")
	// ----------------------------------------------------------------

	// Run commits when fn returns nil and rolls every write back when it
	// returns an error.
	var alice *types.Node
	err = g.Tx().Run(func(tx *graph.GraphTx) error {
		eng, err := tx.AddNode([]string{"Department"}, map[string]any{"name": "Engineering"})
		if err != nil {
			return err
		}
		alice, err = tx.AddNode([]string{"Person", "Employee"}, map[string]any{
			"name": "Alice", "email": "alice@example.com",
		})
		if err != nil {
			return err
		}
		_, err = tx.AddRelationship("WORKS_IN", alice, eng, nil)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	nc, _ := g.Nodes().Count()
	rc, _ := g.Rels().Count()
	fmt.Printf("Committed: %d nodes, %d relationships\n", nc, rc)

	errAbort := errors.New("budget not approved")
	err = g.Tx().Run(func(tx *graph.GraphTx) error {
		if _, err := tx.AddNode([]string{"Department"}, map[string]any{"name": "Research"}); err != nil {
			return err
		}
		return errAbort
	})
	nc, _ = g.Nodes().Count()
	fmt.Printf("Rolled back: Run returned our error=%v, nodes still %d\n", errors.Is(err, errAbort), nc)

	// ----------------------------------------------------------------
	fmt.Println("\n=== 5. Batches (g.Batch) ===")
	// ----------------------------------------------------------------

	// A batch queues writes and applies them in one step, atomic against
	// readers. Unlike a transaction it reports per-operation results.
	res, err := g.Batch().Run(func(bb *graph.BatchBuilder) error {
		return bb.AddNodes([]string{"Laptop"}, map[string]any{"model": "T14"}, 250)
	})
	if err != nil {
		log.Fatal(err)
	}
	laptops, _ := g.Nodes().CountByLabel("Laptop")
	fmt.Printf("Batch: created %s, failed %d; Laptop nodes: %s\n",
		commas(int64(res.Created)), res.Failed, commas(int64(laptops)))

	// ----------------------------------------------------------------
	fmt.Println("\n=== 6. Unique Constraint ===")
	// ----------------------------------------------------------------

	// No two current Employee nodes may share an email. Creating the
	// constraint also creates the property index and checks existing data.
	if err := g.Constraints().CreateUnique(ctx, "Employee", "email"); err != nil {
		log.Fatal(err)
	}
	_, err = g.Nodes().Add(ctx, []string{"Employee"}, map[string]any{
		"name": "Impostor", "email": "alice@example.com",
	})
	fmt.Printf("Second alice@example.com: ErrUniqueViolation=%v\n", errors.Is(err, graph.ErrUniqueViolation))

	// GetOrCreateByKey returns the holder of a value or creates it, under one
	// value lock.
	got, created, err := g.Nodes().GetOrCreateByKey(ctx, "Employee", "email", "alice@example.com", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("GetOrCreateByKey(alice@example.com): created=%v, same node=%v\n", created, got.ID() == alice.ID())
	bob, created, err := g.Nodes().GetOrCreateByKey(ctx, "Employee", "email", "bob@example.com",
		map[string]any{"name": "Bob"})
	if err != nil {
		log.Fatal(err)
	}
	bobName, _ := bob.GetProperty("name")
	fmt.Printf("GetOrCreateByKey(bob@example.com):   created=%v, name=%s\n", created, bobName)

	fmt.Println("\n=== Done ===")
}
