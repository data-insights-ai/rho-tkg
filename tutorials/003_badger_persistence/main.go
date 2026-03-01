// Tutorial 003: Badger Persistence
//
// Demonstrates writing data to BadgerStore, closing the graph, reopening it,
// and verifying that all data (entities, registries, counts) survives.
//
// Run: go run ./tutorials/003_badger_persistence/
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/graph"
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
	dir, err := os.MkdirTemp("", "tkg-tutorial-003-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	fmt.Printf("Badger directory: %s\n", dir)

	// Save IDs as int64 to pass between graph instances.
	var aliceID, bobID, knowsID int64

	// --- Phase 1: Write data ---

	fmt.Println("\n=== Phase 1: Write Data ===")

	func() {
		g, err := graph.New(graph.Config{BadgerDir: dir})
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := g.Close(); err != nil {
				log.Fatal(err)
			}
		}()

		alice, err := g.AddNode([]string{"Person"}, map[string]any{
			"name": "Alice",
			"age":  int64(30),
		})
		if err != nil {
			log.Fatal(err)
		}
		aliceID = int64(alice.InternalID().SnowflakeID())

		bob, err := g.AddNode([]string{"Person", "Developer"}, map[string]any{
			"name": "Bob",
			"age":  int64(28),
		})
		if err != nil {
			log.Fatal(err)
		}
		bobID = int64(bob.InternalID().SnowflakeID())

		charlie, err := g.AddNode([]string{"Person"}, map[string]any{
			"name": "Charlie",
		})
		if err != nil {
			log.Fatal(err)
		}

		knows, err := g.AddRelationship("KNOWS", alice, bob, map[string]any{
			"since": int64(2023),
		})
		if err != nil {
			log.Fatal(err)
		}
		knowsID = int64(knows.InternalID().SnowflakeID())

		_, err = g.AddRelationship("WORKS_WITH", bob, charlie, nil)
		if err != nil {
			log.Fatal(err)
		}

		nc, err := g.NodeCount()
		if err != nil {
			log.Fatal(err)
		}
		rc, err := g.RelationshipCount()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Written: %d nodes, %d relationships\n", nc, rc)
		fmt.Printf("Alice ID: %s, Bob ID: %s\n", commas(aliceID), commas(bobID))

		fmt.Println("Closing graph (flush + save registries)...")
	}()

	// --- Phase 2: Reopen and verify ---

	fmt.Println("\n=== Phase 2: Reopen and Verify ===")

	func() {
		g, err := graph.New(graph.Config{BadgerDir: dir})
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := g.Close(); err != nil {
				log.Fatal(err)
			}
		}()

		nc, err := g.NodeCount()
		if err != nil {
			log.Fatal(err)
		}
		rc, err := g.RelationshipCount()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Counts after reopen: %d nodes, %d relationships\n", nc, rc)

		// Retrieve by ID.
		alice, err := g.GetNode(snowflake.ID(aliceID))
		if err != nil {
			log.Fatal(err)
		}
		name, _ := alice.GetProperty("name")
		age, _ := alice.GetProperty("age")
		fmt.Printf("GetNode(Alice): name=%s, age=%d\n", name, age)

		bob, err := g.GetNode(snowflake.ID(bobID))
		if err != nil {
			log.Fatal(err)
		}
		bobName, _ := bob.GetProperty("name")
		fmt.Printf("GetNode(Bob): name=%s\n", bobName)

		// Label resolution — registries are reloaded from Badger.
		fmt.Printf("Alice labels: %v\n", g.NodeLabels(alice))
		fmt.Printf("Bob labels:   %v\n", g.NodeLabels(bob))
		fmt.Printf("Alice primary: %s\n", g.NodePrimaryLabel(alice))

		// Query by label.
		persons, err := g.NodesByLabel("Person")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Person nodes: %d\n", len(persons))

		developers, err := g.NodesByLabel("Developer")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Developer nodes: %d\n", len(developers))

		// Query by relationship type.
		knowsRels, err := g.RelationshipsByType("KNOWS")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("KNOWS relationships: %d\n", len(knowsRels))

		// Retrieve relationship by ID.
		rel, err := g.GetRelationship(snowflake.ID(knowsID))
		if err != nil {
			log.Fatal(err)
		}
		since, _ := rel.GetProperty("since")
		fmt.Printf("GetRelationship: type=%s, since=%d\n",
			g.RelationshipType(rel), since)

		// Add more data to the reopened graph.
		fmt.Println("\nAdding more data to reopened graph...")
		dave, err := g.AddNode([]string{"Person"}, map[string]any{
			"name": "Dave",
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Dave ID: %s\n", commas(int64(dave.InternalID().SnowflakeID())))

		_, err = g.AddRelationship("KNOWS", alice, dave, nil)
		if err != nil {
			log.Fatal(err)
		}

		nc, err = g.NodeCount()
		if err != nil {
			log.Fatal(err)
		}
		rc, err = g.RelationshipCount()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Final counts: %d nodes, %d relationships\n", nc, rc)

		fmt.Println("Closing graph...")
	}()

	fmt.Println("\n=== Done ===")
}
