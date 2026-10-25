//go:build !windows && !plan9 && !js && !wasip1 && !aix

package tiered

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// offlineFixture is a closed store with a warm and a cold volume whose
// reference shard holds one node. It returns the config, the node and the
// label registry.
func offlineFixture(t *testing.T) (Config, *types.Node, *registrypkg.LabelRegistry) {
	t.Helper()
	cfg := volumeCfg(t.TempDir(), t.TempDir(), t.TempDir())
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Case")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return cfg, n, reg
}

// rootCopies lists every copy of the shard name (the reference shard or the
// archive) at each volume's root, and every copy being made.
func rootCopies(t *testing.T, cfg Config, name string) []string {
	t.Helper()
	var out []string
	for _, root := range []string{cfg.DataDir, cfg.WarmDir, cfg.ColdDir} {
		ents, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			if e.Name() == name || strings.HasPrefix(e.Name(), name+".moving-") {
				out = append(out, filepath.Join(root, e.Name()))
			}
		}
	}
	return out
}

// TestMoveShardOffline_CrashMatrix pins SHARD step 7: the reference shard of
// a closed store moves to the warm volume; a crash after the copy, the
// rename, the catalog save or the removal leaves, after New, exactly one copy
// on the volume the catalog names, with its row readable. Catches a copy left
// at a volume root (orphans looked for under events/ only) and a reference
// shard opened from the wrong volume; a directory at a volume root that is
// not a shard's is left alone.
func TestMoveShardOffline_CrashMatrix(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		crashAfter string
		want       string
	}{
		{"copied", VolumeHot}, {"renamed", VolumeHot}, {"saved", VolumeWarm}, {"removed", VolumeWarm}, {"", VolumeWarm},
	} {
		t.Run("crash after "+c.crashAfter, func(t *testing.T) {
			t.Parallel()
			cfg, n, reg := offlineFixture(t)
			crash := errors.New("crash")
			err := moveShardOffline(context.Background(), cfg, "reference", VolumeWarm, func(step string) error {
				if step == c.crashAfter {
					return crash
				}
				return nil
			})
			if c.crashAfter == "" && err != nil || c.crashAfter != "" && !errors.Is(err, crash) {
				t.Fatalf("MoveShardOffline = %v", err)
			}
			foreign := filepath.Join(cfg.WarmDir, "backup.moving-1")
			if err := os.Mkdir(foreign, 0o750); err != nil {
				t.Fatal(err)
			}
			ts, err := New(cfg)
			if err != nil {
				t.Fatalf("New after the move: %v", err)
			}
			if _, err := os.Stat(foreign); err != nil {
				t.Fatalf("New removed a directory at the volume root that is not a shard's: %v", err)
			}
			defer func() { _ = ts.Close() }()
			ts.SetLabelRegistry(reg)
			if _, err := ts.GetNode(n.ID()); err != nil {
				t.Fatalf("GetNode of the reference row: %v", err)
			}
			root := cfg.DataDir
			if c.want == VolumeWarm {
				root = cfg.WarmDir
			}
			want := filepath.Join(root, "reference")
			if got := rootCopies(t, cfg, "reference"); len(got) != 1 || got[0] != want {
				t.Fatalf("reference copies %v, want only %s", got, want)
			}
		})
	}
}

// TestMoveShardOfflineRefusals: MoveShardOffline refuses an open store (its
// reference shard is locked), an event shard, the reference shard to the
// cold volume, an unmapped or unknown volume (also the one the catalog names)
// and a store without the shard;
// a move to the volume the shard is on does nothing, and a second offline
// move is refused while one runs. Each refusal comes before a copy and
// leaves the shard where it was.
func TestMoveShardOfflineRefusals(t *testing.T) {
	t.Parallel()
	cfg, _, _ := offlineFixture(t)
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	event := ts.HotShardForTest().Name()
	ctx := context.Background()
	if err := MoveShardOffline(ctx, cfg, "reference", VolumeWarm); err == nil || !strings.Contains(err.Error(), "open") {
		t.Fatalf("MoveShardOffline of an open store = %v, want an error naming it open", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	noWarm := cfg
	noWarm.WarmDir = ""
	for _, c := range []struct {
		cfg          Config
		name, volume string
		want         string
	}{
		{cfg, event, VolumeWarm, "MoveShard"},
		{cfg, "reference", VolumeCold, "cold"},
		{noWarm, "reference", VolumeWarm, "not map"},
		{cfg, "reference", "ssd", "not a volume"},
		{cfg, "nothing", VolumeWarm, "neither"},
	} {
		copied := false
		err := moveShardOffline(ctx, c.cfg, c.name, c.volume, func(string) error { copied = true; return nil })
		if err == nil || !strings.Contains(err.Error(), c.want) || copied {
			t.Errorf("MoveShardOffline(%s, %s) = %v (copied: %v), want an error containing %q before a copy", c.name, c.volume, err, copied, c.want)
		}
	}
	if err := MoveShardOffline(ctx, cfg, "reference", VolumeHot); err != nil {
		t.Fatalf("MoveShardOffline to the volume it is on = %v", err)
	}
	if err := MoveShardOffline(ctx, cfg, "reference", VolumeWarm); err != nil {
		t.Fatalf("MoveShardOffline: %v", err)
	}
	if err := MoveShardOffline(ctx, noWarm, "reference", VolumeHot); err == nil || !strings.Contains(err.Error(), "not map") {
		t.Fatalf("MoveShardOffline off a volume the config does not map = %v", err)
	}
	if err := MoveShardOffline(ctx, cfg, "reference", VolumeHot); err != nil {
		t.Fatalf("MoveShardOffline back to hot: %v", err)
	}
	release := make(chan struct{})
	held := make(chan struct{})
	moved := make(chan error, 1)
	go func() {
		moved <- moveShardOffline(ctx, cfg, "reference", VolumeWarm, func(step string) error {
			if step == "copied" {
				close(held)
				<-release
			}
			return nil
		})
	}()
	<-held
	if err := MoveShardOffline(ctx, cfg, "reference", VolumeWarm); err == nil || !strings.Contains(err.Error(), "open") {
		t.Errorf("a second MoveShardOffline during one = %v, want refused", err)
	}
	close(release)
	if err := <-moved; err != nil {
		t.Fatalf("MoveShardOffline: %v", err)
	}
	if err := MoveShardOffline(ctx, cfg, "reference", VolumeHot); err != nil {
		t.Fatalf("MoveShardOffline back to hot: %v", err)
	}
	empty := volumeCfg(t.TempDir(), t.TempDir(), "")
	if err := MoveShardOffline(ctx, empty, "archive", VolumeWarm); err == nil || !strings.Contains(err.Error(), "no archive") {
		t.Fatalf("MoveShardOffline without a store = %v, want an error naming the missing archive", err)
	}
	if got := rootCopies(t, cfg, "reference"); len(got) != 1 || got[0] != filepath.Join(cfg.DataDir, "reference") {
		t.Fatalf("reference copies %v after the refusals, want only the hot one", got)
	}
}
