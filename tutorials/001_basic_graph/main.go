// Tutorial 001: Basic Graph Operations (MemoryStore)
//
// Demonstrates creating nodes, relationships, querying, and deleting entities
// using the in-memory store. Scenario: a social network with Person nodes.
//
// Run: go run ./tutorials/001_basic_graph/
package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
)

// commas formats an integer with thousand separators: 1234567 -> "1,234,567".
func commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	if n < 0 {
		return "-" + commas(-n)
	}
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
	// Create graph with explicit configuration.
	g, err := graph.New(graph.Config{
		// SnowflakeNodeID identifies this graph instance (0-511).
		// It's used to guarantee uniqueness in a distributed environment.
		SnowflakeNodeID: 0,

		// Persistence options (optional):
		// 1. Leave all nil/empty for MemoryStore (default).
		// 2. Set BadgerDir for a persistent BadgerStore.
		// 3. Set BadgerInMemory for an in-memory BadgerStore.
		// 4. Or provide a custom Store implementation via the Store field.
		BadgerDir:      "",
		BadgerInMemory: false,
		Store:          nil,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := g.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	fmt.Println("=== 1. Adding Nodes ===")

	// Single-label node with properties.
	alice, err := g.AddNode([]string{"Person"}, map[string]any{
		"name": "Alice",
		"age":  int64(30),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Alice ID: %s, labels: %v\n",
		commas(int64(alice.InternalID().SnowflakeID())), g.NodeLabels(alice))

	// Multi-label node.
	bob, err := g.AddNode([]string{"Person", "Employee"}, map[string]any{
		"name": "Bob",
		"age":  int64(25),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Bob   ID: %s, labels: %v\n",
		commas(int64(bob.InternalID().SnowflakeID())), g.NodeLabels(bob))

	charlie, err := g.AddNode([]string{"Person"}, map[string]any{
		"name":   "Charlie",
		"age":    int64(35),
		"active": true,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Charlie ID: %s, labels: %v\n",
		commas(int64(charlie.InternalID().SnowflakeID())), g.NodeLabels(charlie))

	fmt.Println("\n=== 2. Adding Relationships ===")

	knows, err := g.AddRelationship("KNOWS", alice, bob, map[string]any{
		"since": int64(2020),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Alice -[%s]-> Bob  (ID: %s)\n",
		g.RelationshipType(knows), commas(int64(knows.InternalID().SnowflakeID())))

	worksWith, err := g.AddRelationship("WORKS_WITH", bob, charlie, map[string]any{
		"project": "Atlas",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Bob -[%s]-> Charlie  (ID: %s)\n",
		g.RelationshipType(worksWith), commas(int64(worksWith.InternalID().SnowflakeID())))

	knows2, err := g.AddRelationship("KNOWS", alice, charlie, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Alice -[%s]-> Charlie  (ID: %s)\n",
		g.RelationshipType(knows2), commas(int64(knows2.InternalID().SnowflakeID())))

	fmt.Println("\n=== 3. Entity Counts ===")

	nc, err := g.NodeCount()
	if err != nil {
		log.Fatal(err)
	}
	rc, err := g.RelationshipCount()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Nodes: %d, Relationships: %d\n", nc, rc)

	fmt.Println("\n=== 4. Query Nodes by Label ===")

	persons, err := g.NodesByLabel("Person", graph.QueryOpts{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Person nodes: %d\n", len(persons))
	for _, n := range persons {
		name, _ := n.GetProperty("name")
		fmt.Printf("  - %s (primary: %s)\n", name, g.NodePrimaryLabel(n))
	}

	employees, err := g.NodesByLabel("Employee", graph.QueryOpts{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Employee nodes: %d\n", len(employees))
	for _, n := range employees {
		name, _ := n.GetProperty("name")
		fmt.Printf("  - %s\n", name)
	}

	fmt.Println("\n=== 5. Query Relationships by Type ===")

	knowsRels, err := g.RelationshipsByType("KNOWS", graph.QueryOpts{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("KNOWS relationships: %d\n", len(knowsRels))

	worksRels, err := g.RelationshipsByType("WORKS_WITH", graph.QueryOpts{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("WORKS_WITH relationships: %d\n", len(worksRels))

	fmt.Println("\n=== 6. Retrieve by ID ===")

	fetched, err := g.GetNode(alice.InternalID().SnowflakeID())
	if err != nil {
		log.Fatal(err)
	}
	name, _ := fetched.GetProperty("name")
	fmt.Printf("GetNode(Alice): name=%s\n", name)

	fetchedRel, err := g.GetRelationship(knows.InternalID().SnowflakeID())
	if err != nil {
		log.Fatal(err)
	}
	since, _ := fetchedRel.GetProperty("since")
	fmt.Printf("GetRelationship(KNOWS): since=%d\n", since)

	fmt.Println("\n=== 7. Label and Type Checks ===")

	fmt.Printf("Alice has 'Person': %v\n", g.NodeHasLabel(alice, "Person"))
	fmt.Printf("Alice has 'Employee': %v\n", g.NodeHasLabel(alice, "Employee"))
	fmt.Printf("Bob has 'Employee': %v\n", g.NodeHasLabel(bob, "Employee"))
	fmt.Printf("knows is 'KNOWS': %v\n", g.RelationshipHasType(knows, "KNOWS"))
	fmt.Printf("knows is 'WORKS_WITH': %v\n", g.RelationshipHasType(knows, "WORKS_WITH"))

	fmt.Println("\n=== 8. Properties ===")

	age, ok := alice.GetProperty("age")
	fmt.Printf("Alice age: %d (found: %v)\n", age, ok)
	_, ok = alice.GetProperty("nonexistent")
	fmt.Printf("Alice 'nonexistent': found=%v\n", ok)
	fmt.Printf("Charlie properties: %v\n", charlie.PropertiesMap())

	fmt.Println("\n=== 9. Adjacency Queries ===")

	// All outgoing from Alice.
	outAll, err := g.OutgoingRelationships(alice.InternalID().SnowflakeID(), "")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Alice outgoing (all): %d\n", len(outAll))
	for _, r := range outAll {
		fmt.Printf("  -[%s]-> %s\n", g.RelationshipType(r), commas(int64(r.EndNodeID().SnowflakeID())))
	}

	// Filtered: only KNOWS.
	outKnows, err := g.OutgoingRelationships(alice.InternalID().SnowflakeID(), "KNOWS")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Alice outgoing KNOWS: %d\n", len(outKnows))

	// Incoming to Charlie.
	inCharlie, err := g.IncomingRelationships(charlie.InternalID().SnowflakeID(), "")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Charlie incoming (all): %d\n", len(inCharlie))

	fmt.Println("\n=== 10. Delete a Relationship ===")

	if err := g.DeleteRelationship(worksWith.InternalID().SnowflakeID()); err != nil {
		log.Fatal(err)
	}
	rc, err = g.RelationshipCount()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("After deleting WORKS_WITH: relationships=%d\n", rc)

	_, err = g.GetRelationship(worksWith.InternalID().SnowflakeID())
	if errors.Is(err, graph.ErrRelNotFound) {
		fmt.Println("WORKS_WITH correctly not found after deletion")
	}

	fmt.Println("\n=== 11. Delete a Node (Cascade) ===")

	// Alice has two KNOWS relationships. Deleting Alice removes them.
	if err := g.DeleteNode(alice.InternalID().SnowflakeID()); err != nil {
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
	fmt.Printf("After deleting Alice: nodes=%d, relationships=%d\n", nc, rc)

	_, err = g.GetNode(alice.InternalID().SnowflakeID())
	if errors.Is(err, graph.ErrNodeNotFound) {
		fmt.Println("Alice correctly not found after deletion")
	}

	fmt.Println("\n=== Done ===")
}
