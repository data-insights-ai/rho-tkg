package tiered

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// openParallelCfg builds a store whose event shards rotate quickly, so a test
// can produce several warm shards without waiting a week.
func openParallelCfg(dir string) Config {
	return Config{
		DataDir:     dir,
		RefLabels:   []string{"Case"},
		ShardWindow: time.Minute,
	}
}

// warmShards rotates until at least n warm shards exist on disk.
func warmShards(t *testing.T, dir string, n int) {
	t.Helper()
	ts, err := New(openParallelCfg(dir))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 0; i < n; i++ {
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatalf("rotate %d: %v", i, err)
		}
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// WARM SHARDS OPEN CONCURRENTLY, AND THE RESULT MUST NOT DEPEND ON THAT.
//
// Every shard the catalog lists has to be mounted, whichever worker happens to
// finish first — a shard silently missing from the map is a slice of the graph
// that queries would simply not see.
func TestParallelOpen_MountsEveryShard(t *testing.T) {
	dir := t.TempDir()
	warmShards(t, dir, 4)

	// Count what is on disk, then what the store mounted.
	entries, err := os.ReadDir(filepath.Join(dir, "events"))
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	onDisk := 0
	for _, e := range entries {
		if e.IsDir() {
			onDisk++
		}
	}
	if onDisk < 4 {
		t.Skipf("rotation produced only %d shard dirs", onDisk)
	}

	for attempt := 0; attempt < 3; attempt++ {
		ts, err := New(openParallelCfg(dir))
		if err != nil {
			t.Fatalf("reopen %d: %v", attempt, err)
		}
		infos, err := ts.ListShards()
		if err != nil {
			t.Fatalf("ListShards: %v", err)
		}
		events := 0
		for _, s := range infos {
			if s.Kind == ShardEvent {
				events++
			}
		}
		if events != onDisk {
			t.Errorf("attempt %d mounted %d event shards, want the %d on disk",
				attempt, events, onDisk)
		}
		if err := ts.Close(); err != nil {
			t.Fatalf("close %d: %v", attempt, err)
		}
	}
}

// A SHARD THAT WILL NOT OPEN MUST NOT LEAK THE ONES THAT DID.
//
// Opening concurrently means other workers keep finishing after the failure is
// noticed, so the cleanup has to cover shards that did not exist yet when the
// error was recorded. A leaked Badger handle holds its directory lock, and the
// next open of that directory fails — which is what this checks, because a
// leaked handle is otherwise invisible until something else breaks.
func TestParallelOpen_FailureLeaksNoHandles(t *testing.T) {
	dir := t.TempDir()
	warmShards(t, dir, 4)

	eventsDir := filepath.Join(dir, "events")
	entries, err := os.ReadDir(eventsDir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	var victim string
	for _, e := range entries {
		if e.IsDir() {
			victim = e.Name() // corrupt the first shard listed
			break
		}
	}
	if victim == "" {
		t.Skip("no event shard to corrupt")
	}
	manifest := filepath.Join(eventsDir, victim, "MANIFEST")
	saved, err := os.ReadFile(manifest)
	if err != nil {
		t.Skipf("shard %s has no MANIFEST to corrupt: %v", victim, err)
	}
	if err := os.WriteFile(manifest, []byte("corrupt"), 0o644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}

	if _, err := New(openParallelCfg(dir)); err == nil {
		t.Fatal("open succeeded with a corrupt shard manifest")
	} else if !strings.Contains(err.Error(), victim) {
		t.Errorf("error does not name the failing shard %s: %v", victim, err)
	}

	// Repair and reopen. If the failed open leaked a handle on any OTHER shard,
	// its directory lock is still held and this fails.
	if err := os.WriteFile(manifest, saved, 0o644); err != nil {
		t.Fatalf("restore: %v", err)
	}
	ts, err := New(openParallelCfg(dir))
	if err != nil {
		t.Fatalf("reopen after a failed open leaked handles: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
