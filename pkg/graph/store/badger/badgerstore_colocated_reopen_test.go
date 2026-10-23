package badger

import (
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestCoLocatedLegsSurviveReopen (row 94): a co-located rel whose endpoints
// live on another shard (a tiered mint shard) keeps both adjacency legs
// across a reopen, clean (open image) and rebuilt, as the write path holds
// them; OutgoingRelIDsOfType filters them by type like IncomingRelIDs.
func TestCoLocatedLegsSurviveReopen(t *testing.T) {
	t.Parallel()
	for _, rebuild := range []bool{false, true} {
		name := "clean"
		if rebuild {
			name = "rebuild"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			gen, relGen := newTestGen(t, 0), newTestGen(t, 1)
			a, b := gen.Generate(), gen.Generate() // never stored here
			r1, r2 := relGen.Generate(), relGen.Generate()
			for _, r := range []*types.Relationship{
				types.NewRelationship(types.RelID(r1), 3, types.NodeID(a), types.NodeID(b)),
				types.NewRelationship(types.RelID(r2), 4, types.NodeID(a), types.NodeID(b)),
			} {
				if err := bs.PutRelationshipCoLocated(r); err != nil {
					t.Fatal(err)
				}
			}
			check := func(phase string) {
				t.Helper()
				cases := []struct {
					what string
					got  []snowflake.ID
					want []snowflake.ID
				}{
					{"out all", bs.OutgoingRelIDsOfType(a, 0), []snowflake.ID{r1, r2}},
					{"out type 3", bs.OutgoingRelIDsOfType(a, 3), []snowflake.ID{r1}},
					{"out type 9", bs.OutgoingRelIDsOfType(a, 9), nil},
					{"out of end", bs.OutgoingRelIDsOfType(b, 0), nil},
					{"in type 4", bs.IncomingRelIDs(b, 4), []snowflake.ID{r2}},
					{"in all", bs.IncomingRelIDs(b, 0), []snowflake.ID{r1, r2}},
				}
				for _, c := range cases {
					if !slices.Equal(c.got, c.want) {
						t.Fatalf("%s: %s = %v, want %v", phase, c.what, c.got, c.want)
					}
				}
			}
			check("before close")
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if rebuild {
				updateRawBadgerDir(t, dir, DeleteOpenStateForTest)
			}
			bs, err = New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			check("after reopen")
		})
	}
}
