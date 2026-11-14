package core

import (
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestEntityReadsUnsupportedHistoryRefusesBeforeCopies(t *testing.T) {
	ms := memory.New()
	defer ms.Close()
	n := types.NewNode(10, 1, nil)
	if err := ms.PutNodeVersion(10, 0, n); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(20, 1, 1, 2)
	if err := ms.PutRelVersion(20, 0, r); err != nil {
		t.Fatal(err)
	}
	// This store declares only the mandatory surface, hiding native admission.
	c := &Core{store: struct{ storepkg.MandatoryStore }{ms}, temporalMetaHistory: ms, storeRowsTrust: true}
	for _, door := range []struct {
		name string
		read func(entityReads) (int, error)
	}{
		{"nodes", func(rd entityReads) (int, error) { rows, err := rd.nodeHistory(10); return len(rows), err }},
		{"rels", func(rd entityReads) (int, error) { rows, err := rd.relHistory(20); return len(rows), err }},
		{"node metas", func(rd entityReads) (int, error) { rows, err := rd.nodeMeta(10); return len(rows), err }},
		{"rel metas", func(rd entityReads) (int, error) { rows, err := rd.relMeta(20); return len(rows), err }},
	} {
		t.Run(door.name, func(t *testing.T) {
			if rows, err := door.read(entityReads{c: c}); err != nil || rows != 1 {
				t.Fatalf("plain control: rows %d, err %v", rows, err)
			}
			_, meter := storepkg.WithReadBudget(t.Context(), storepkg.ReadBudget{})
			beforeRows, beforeMetas := ms.HistoryRowsReadForTest(), ms.HistoryMetasListedForTest()
			rows, err := door.read(entityReads{c: c, tally: meter.Tally("history")})
			var refusal *storepkg.ReadBudgetError
			if !errors.As(err, &refusal) || refusal.Limit != "unsupported" || rows != 0 || meter.Exceeded() == nil || ms.HistoryRowsReadForTest() != beforeRows || ms.HistoryMetasListedForTest() != beforeMetas {
				t.Fatalf("metered: rows %d, native row copies %d/metas %d, err %v", rows, ms.HistoryRowsReadForTest()-beforeRows, ms.HistoryMetasListedForTest()-beforeMetas, err)
			}
		})
	}
}
