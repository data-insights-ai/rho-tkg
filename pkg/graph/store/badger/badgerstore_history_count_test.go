package badger

import "testing"

// A HistoryCounts read in the window where a history key is in the write
// buffer but not yet in the presence set (the hook runs there, under wbMu)
// must not cache its count under the epoch that key's write produced: the
// next read after the write has to count the key. The epoch advanced before
// the presence note, so the window read cached n-1 under the final epoch and
// every later read returned it (TestHistoryCountsConcurrentWriters/badger).
func TestHistoryCount_ReadBeforePresenceNoteIsNotCachedAsCurrent(t *testing.T) {
	counts := map[string]func(*Store) (int, error){
		"node": (*Store).NodeHistoryCount,
		"rel":  (*Store).RelHistoryCount,
	}
	for _, k := range presenceKinds() {
		t.Run(k.name, func(t *testing.T) {
			count := counts[k.name]
			bs := newFlushParkStore(t, nil)
			if err := k.put(bs, 100, 0); err != nil {
				t.Fatal(err)
			}
			expectPresence(t, k, bs, "built", 100, true) // the count walks the presence set
			if n, err := count(bs); err != nil || n != 1 {
				t.Fatalf("before: count = %d, %v; want 1", n, err)
			}
			fired := 0
			bs.historyNoteTestHook = func() {
				fired++
				if n, err := count(bs); err != nil || n != 1 {
					t.Errorf("in the window: count = %d, %v; want 1 (key not yet noted)", n, err)
				}
			}
			if err := k.put(bs, 200, 0); err != nil {
				t.Fatal(err)
			}
			bs.historyNoteTestHook = nil
			if fired != 1 {
				t.Fatalf("hook fired %d times, want 1", fired)
			}
			if n, err := count(bs); err != nil || n != 2 {
				t.Fatalf("after the write: count = %d, %v; want 2", n, err)
			}
		})
	}
}
