package fixtures

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/vmihailenco/msgpack/v5"
)

// Export record tags that carry an entity row (internal/core/export.go):
// 0x03 node, 0x04 node history, 0x05 relationship, 0x06 relationship history.
const (
	tagNode    = 0x03
	tagNodeHis = 0x04
	tagRel     = 0x05
	tagRelHis  = 0x06
)

// Source is what an importer reads from an export: every node and
// relationship with all its stored rows (History, then Current), as v4 returns
// them after importing the stream under cfg. IDs are sorted.
type Source struct {
	Config Config
	Nodes  []Entity
	Rels   []Entity
}

// ReadExport lists the entity IDs the export holds, imports it into a fresh v4
// store under cfg (the untimed mode and hash setting must match the source
// store) and reads every entity's rows back. It is the importer's one door to
// a v4 export; it does not use the oracle file.
func ReadExport(cfg Config, export []byte) (*Source, error) {
	nodeIDs, relIDs, err := entityIDs(export)
	if err != nil {
		return nil, err
	}
	s := Scenario{Unbounded: cfg.UntimedValidStart == "unbounded", Hashes: cfg.IntegrityHashes}
	if cfg.UntimedValidStart != "unbounded" && cfg.UntimedValidStart != "tx_from" {
		return nil, fmt.Errorf("unknown untimed_valid_start %q", cfg.UntimedValidStart)
	}
	g, err := importGraph(s, export)
	if err != nil {
		return nil, fmt.Errorf("v4 import: %w", err)
	}
	defer g.Close()
	e, err := answer(context.Background(), g, "", cfg, query{nodeIDs: nodeIDs, relIDs: relIDs})
	if err != nil {
		return nil, err
	}
	return &Source{Config: cfg, Nodes: e.Nodes, Rels: e.Rels}, nil
}

func entityIDs(export []byte) (nodes, rels []int64, err error) {
	tags, offs, lens, err := records(export)
	if err != nil {
		return nil, nil, err
	}
	seenN, seenR := map[int64]bool{}, map[int64]bool{}
	for i, t := range tags {
		if t < tagNode || t > tagRelHis {
			continue
		}
		var rec struct {
			ID int64 `msgpack:"id"`
		}
		dec := msgpack.NewDecoder(bytes.NewReader(export[offs[i] : offs[i]+lens[i]]))
		dec.SetCustomStructTag("msgpack")
		if err := dec.Decode(&rec); err != nil {
			return nil, nil, fmt.Errorf("record %d (tag 0x%02x): %w", i, t, err)
		}
		if t == tagNode || t == tagNodeHis {
			seenN[rec.ID] = true
		} else {
			seenR[rec.ID] = true
		}
	}
	for id := range seenN {
		nodes = append(nodes, id)
	}
	for id := range seenR {
		rels = append(rels, id)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	sort.Slice(rels, func(i, j int) bool { return rels[i] < rels[j] })
	return nodes, rels, nil
}
