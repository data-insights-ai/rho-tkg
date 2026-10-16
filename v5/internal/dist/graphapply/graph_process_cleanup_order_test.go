package graphapply

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// A restart re-attaches a hub after the network's reaping cleanup was already
// registered, so the hub's cleanup runs first while its children still live.
// It must still return (its read loops end only at child EOF) and close each
// child gracefully. Cases: one live child, three live children, and children
// the network already reaped before the hub cleanup (no second reap).
func TestGraphProcessHubCleanupReapsLiveChildrenInAnyOrder(t *testing.T) {
	for _, tc := range []struct {
		name        string
		children    int
		reapedFirst bool
	}{{"one live child", 1, false}, {"three live children", 3, false}, {"already reaped child", 2, true}} {
		done := make(chan struct{})
		all := make(map[uint64]*graphProcess)
		dir := t.TempDir()
		go func() {
			defer close(done)
			t.Run(tc.name, func(t *testing.T) {
				for id := uint64(1); id <= uint64(tc.children); id++ {
					all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1")
				}
				// Network cleanup registered BEFORE the (re)attached hub: LIFO runs the hub first.
				t.Cleanup(func() {
					for _, p := range all {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						if err := p.shutdown(ctx, false); err != nil {
							t.Error("graceful close", err)
						}
						cancel()
					}
				})
				attachGraphProcessMuxes(t, all)
				if tc.reapedFirst {
					for _, p := range all {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						if err := p.shutdown(ctx, false); err != nil {
							t.Error("graceful close", err)
						}
						cancel()
					}
				}
			})
		}()
		select {
		case <-done:
		case <-time.After(60 * time.Second):
			t.Fatal("hub cleanup deadlocked on live children", tc.name)
		}
		for id, p := range all {
			if !p.closed || p.closeErr != nil || p.cmd.ProcessState == nil || !p.cmd.ProcessState.Success() {
				t.Fatal("child not gracefully reaped", tc.name, id, p.closeErr)
			}
		}
	}
}
