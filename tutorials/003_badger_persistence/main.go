// Tutorial 003: Badger Persistence
//
// Demonstrates writing to an on-disk Badger store (graph.Config{BadgerDir}),
// closing the graph, reopening it, and checking that entities, label and type
// registries, counts and version history survive. The store lives in a
// temporary directory this program creates and removes.
//
// Run: go run ./tutorials/003_badger_persistence/
package main

import (
	"context"
	"fmt"
	"log"
	"os"
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
	dir, err := os.MkdirTemp("", "tkg-tutorial-003-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

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

		alice, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{
			"name": "Alice",
			"age":  int64(30),
		})
		if err != nil {
			log.Fatal(err)
		}
		aliceID = int64(alice.ID())

		// A versioned update: version 1 is current, version 0 moves to history.
		if _, err := g.Nodes().Update(context.Background(), alice.ID(), map[string]any{"age": int64(31)}); err != nil {
			log.Fatal(err)
		}

		bob, err := g.Nodes().Add(context.Background(), []string{"Person", "Developer"}, map[string]any{
			"name": "Bob",
			"age":  int64(28),
		})
		if err != nil {
			log.Fatal(err)
		}
		bobID = int64(bob.ID())

		charlie, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{
			"name": "Charlie",
		})
		if err != nil {
			log.Fatal(err)
		}

		knows, err := g.Rels().Add(context.Background(), "KNOWS", alice, bob, map[string]any{
			"since": int64(2023),
		})
		if err != nil {
			log.Fatal(err)
		}
		knowsID = int64(knows.ID())

		_, err = g.Rels().Add(context.Background(), "WORKS_WITH", bob, charlie, nil)
		if err != nil {
			log.Fatal(err)
		}

		nc, err := g.Nodes().Count()
		if err != nil {
			log.Fatal(err)
		}
		rc, err := g.Rels().Count()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Written: %d nodes, %d relationships\n", nc, rc)
		fmt.Printf("Alice ID: %s, Bob ID: %s\n", commas(aliceID), commas(bobID))

		// Close flushes the store and saves the label and type registries.
		if err := g.Close(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Graph closed")
	}()

	// --- Phase 2: Reopen and verify ---

	fmt.Println("\n=== Phase 2: Reopen and Verify ===")

	func() {
		g, err := graph.New(graph.Config{BadgerDir: dir})
		if err != nil {
			log.Fatal(err)
		}

		nc, err := g.Nodes().Count()
		if err != nil {
			log.Fatal(err)
		}
		rc, err := g.Rels().Count()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Counts after reopen: %d nodes, %d relationships\n", nc, rc)

		// Retrieve by ID.
		alice, err := g.Nodes().Get(context.Background(), types.NodeID(aliceID))
		if err != nil {
			log.Fatal(err)
		}
		name, _ := alice.GetProperty("name")
		age, _ := alice.GetProperty("age")
		fmt.Printf("Nodes().Get(Alice): name=%s, age=%d, version %d\n", name, age, alice.Version())

		// The version history and its hash chain are on disk too.
		history, err := g.Nodes().History(alice.ID())
		if err != nil {
			log.Fatal(err)
		}
		chainOK, err := g.Hash().VerifyNodeChain(alice.ID())
		if err != nil {
			log.Fatal(err)
		}
		if len(history) == 0 {
			log.Fatal("history lost on reopen")
		}
		oldAge, _ := history[0].GetProperty("age")
		fmt.Printf("Alice history: %d superseded version (age=%d), VerifyNodeChain=%v\n",
			len(history), oldAge, chainOK)

		bob, err := g.Nodes().Get(context.Background(), types.NodeID(bobID))
		if err != nil {
			log.Fatal(err)
		}
		bobName, _ := bob.GetProperty("name")
		fmt.Printf("Nodes().Get(Bob): name=%s\n", bobName)

		// Label resolution — registries are reloaded from Badger.
		fmt.Printf("Alice labels: %v\n", g.Nodes().Labels(alice))
		fmt.Printf("Bob labels:   %v\n", g.Nodes().Labels(bob))
		fmt.Printf("Alice primary: %s\n", g.Nodes().PrimaryLabel(alice))

		// Query by label.
		persons, err := g.Nodes().ByLabel("Person", graph.QueryOpts{})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Person nodes: %d\n", len(persons))

		developers, err := g.Nodes().ByLabel("Developer", graph.QueryOpts{})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Developer nodes: %d\n", len(developers))

		// Query by relationship type.
		knowsRels, err := g.Rels().ByType("KNOWS", graph.QueryOpts{})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("KNOWS relationships: %d\n", len(knowsRels))

		// Retrieve relationship by ID.
		rel, err := g.Rels().Get(context.Background(), types.RelID(knowsID))
		if err != nil {
			log.Fatal(err)
		}
		since, _ := rel.GetProperty("since")
		fmt.Printf("Rels().Get(KNOWS): type=%s, since=%d\n",
			g.Rels().Type(rel), since)

		// Add more data to the reopened graph.
		fmt.Println("\nAdding more data to reopened graph...")
		dave, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{
			"name": "Dave",
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Dave ID: %s\n", commas(int64(dave.ID())))

		_, err = g.Rels().Add(context.Background(), "KNOWS", alice, dave, nil)
		if err != nil {
			log.Fatal(err)
		}

		nc, err = g.Nodes().Count()
		if err != nil {
			log.Fatal(err)
		}
		rc, err = g.Rels().Count()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Final counts: %d nodes, %d relationships\n", nc, rc)

		if err := g.Close(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Graph closed")
	}()

	fmt.Println("\n=== Done ===")
}
