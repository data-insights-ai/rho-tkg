package badger

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 41: both packed sidecars over thousands of IDs (many table blocks),
// against the history door. The build races writers that add IDs between the
// seeded ones; then random steps insert IDs (block splits), raise versions
// (in place), overwrite a row with lower stamps, trim all rows (delete, then
// a probe removes the ID), truncate, re-import (trim all, rewrite from
// version 0 with older stamps), flush, and reopen (a clean Close writes the
// open image, the reopened store trusts it and rebuilds the sidecars lazily).
// After each check every ID agrees: HasHistory == (rows > 0), HistoryStamps
// == fold(rows), and the history-ID walk (read from the presence table) ==
// the IDs with rows.
func TestHistorySidecars_ManyBlocksDifferential(t *testing.T) {
	pks, iks := presenceKinds(), idOverlayKinds()
	for ki, sk := range stampKinds() {
		pk, ik := pks[ki], iks[ki]
		t.Run(sk.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(41)) // #nosec G404 -- deterministic test sequence
			dir := t.TempDir()
			open := func() *Store {
				bs, err := New(Config{Dir: dir, FlushInterval: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				return bs
			}
			bs := open()
			t.Cleanup(func() { _ = bs.Close() })
			const seeded, gap = 3000, 10
			ver := map[int64]uint32{}
			var mu sync.Mutex // guards ver while the build races writers
			clock := types.Instant(1_000)
			put := func(id int64, tf types.Instant) error {
				mu.Lock()
				v := ver[id]
				ver[id] = v + 1
				mu.Unlock()
				return sk.put(bs, id, v, tf, tf+1, 0)
			}
			for i := int64(1); i <= seeded; i++ {
				if err := put(i*gap, clock+types.Instant(i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			// Build both sidecars while two writers add IDs between the seeded ones.
			var wg sync.WaitGroup
			errs := make(chan error, 2)
			for w := int64(0); w < 2; w++ {
				wg.Add(1)
				go func(w int64) {
					defer wg.Done()
					for i := int64(1); i <= 300; i++ {
						if err := put(i*7*gap+1+w, clock+types.Instant(i)); err != nil {
							errs <- err
							return
						}
					}
				}(w)
			}
			if _, err := pk.has(bs, gap); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := sk.stamps(bs, gap); err != nil {
				t.Fatal(err)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			check := func(step string) {
				t.Helper()
				walked, err := ik.historyIDs(bs)
				if err != nil {
					t.Fatal(err)
				}
				var want []int64
				for id := int64(1); id <= seeded*gap+gap; id++ {
					if _, written := ver[id]; !written && id%97 != 0 {
						continue // never written: sampled
					}
					rows, err := pk.rows(bs, id)
					if err != nil {
						t.Fatal(err)
					}
					if has, err := pk.has(bs, id); err != nil || has != (rows > 0) {
						t.Fatalf("%s: has(%d) = %v, %v; history has %d rows", step, id, has, err, rows)
					}
					expectStampsAgree(t, sk, bs, step, id)
					if rows > 0 {
						want = append(want, id)
					}
				}
				if !slices.Equal(walked, want) {
					t.Fatalf("%s: history-ID walk has %d IDs, %d IDs have rows", step, len(walked), len(want))
				}
			}
			check("built during writes")
			ids := func() int64 { return gap * (1 + r.Int63n(seeded)) }
			for step := 1; step <= 2500; step++ {
				clock++
				var err error
				switch op := r.Intn(100); {
				case op < 30:
					err = put(ids()+1+r.Int63n(gap-1), clock) // a new ID between: insert
				case op < 50:
					err = put(ids(), clock) // a higher version: in place
				case op < 58:
					id := ids()
					if v := ver[id]; v > 0 {
						err = sk.put(bs, id, v-1, 1, 2, 0) // overwrite the top row with older stamps
					}
				case op < 70:
					err = sk.trimFrom(bs, ids(), 0)
				case op < 78:
					err = sk.truncate(bs, ids(), 1)
				case op < 86:
					id := ids() // re-import: rows gone, then rewritten from version 0
					if err = sk.trimFrom(bs, id, 0); err == nil {
						ver[id] = 0
						err = put(id, 5)
					}
				case op < 96:
					err = bs.Flush()
				default:
					if err = bs.Close(); err == nil {
						bs = open()
					}
				}
				if err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
				if step%500 == 0 {
					check(fmt.Sprintf("step %d", step))
				}
			}
			check("final")
			if built, _ := pk.built(bs.HistoryPresenceStats()); !built {
				t.Fatal("the presence set is not built after the final check")
			}
		})
	}
}
