package core

import (
	"context"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A fetch nothing passes reads on in a larger one, up to pageChunkMax: a filter
// meeting few members costs about log(members) fetches up to the cap, not
// members / pageChunkMin.
func TestPageSourceFillGrowsTheFetchWhileNothingPasses(t *testing.T) {
	t.Parallel()
	var asked []int
	s := &pageSource[types.RelID]{fetch: func(after types.RelID, n int) ([]types.RelID, types.RelID, bool, error) {
		asked = append(asked, n)
		if len(asked) == 9 {
			return []types.RelID{after + 1}, after + 1, false, nil
		}
		return nil, after + types.RelID(n), false, nil
	}}
	if err := s.fill(context.Background(), pageChunkMin); err != nil {
		t.Fatal(err)
	}
	want := []int{64, 128, 256, 512, 1024, 2048, 4096, 4096, 4096}
	if !slices.Equal(asked, want) {
		t.Fatalf("fetch sizes %v, want %v", asked, want)
	}
	if len(s.buf) != 1 {
		t.Fatalf("buf %v, want the one that passed", s.buf)
	}
}
