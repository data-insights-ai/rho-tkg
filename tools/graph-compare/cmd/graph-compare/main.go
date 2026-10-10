package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	slice "github.com/data-insights-ai/rho-tkg/tools/graph-compare"
)

func run(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 1 && args[0] == "capabilities" {
		if err := json.NewEncoder(out).Encode(map[string]any{"adapter": "native-rho-v4.43.0", "current_answers": 92, "history_feature": "pending-separate-lane", "native_v5": false, "vendors": "not installed or run", "performance_acceptance": false}); err != nil {
			return 1
		}
		return 0
	}
	if len(args) < 1 || args[0] != "run" {
		_, _ = fmt.Fprintln(errOut, "usage: graph-compare capabilities | run --backend memory|badger --dataset ... --reference ... --pin ... --out ...")
		return 2
	}
	f := flag.NewFlagSet("graph-compare run", flag.ContinueOnError)
	f.SetOutput(errOut)
	backend := f.String("backend", "memory", "native v4 backend")
	dataset := f.String("dataset", "../reference/fixtures/basic-small", "exact pinned fixture")
	reference := f.String("reference", "../reference", "frozen oracle/normalizer source")
	pin := f.String("pin", "../reference-pin.json", "trusted reference inventory")
	output := f.String("out", "", "new atomic publication directory")
	rows := f.Int("max-rows", 5_000_000, "complete answer row limit")
	bytes := f.Int("max-bytes", 64<<20, "complete answer encoded byte limit")
	work := f.Int("max-visited", 5_000_000, "adapter callback work limit")
	if err := f.Parse(args[1:]); err != nil || f.NArg() != 0 {
		return 2
	}
	r, err := slice.Run(ctx, slice.RunConfig{Dataset: *dataset, Reference: *reference, ReferencePin: *pin, Backend: *backend, Output: *output, Limits: slice.Limits{MaxRows: *rows, MaxBytes: *bytes, MaxVisited: *work}})
	if err != nil {
		_, _ = fmt.Fprintln(errOut, err)
		return 1
	}
	if err := json.NewEncoder(out).Encode(map[string]any{"validated_current": r.ValidatedCurrent, "pending_history": r.PendingHistory, "reopened_current": r.ReopenedCurrent, "source_commit": r.SourceCommit, "backend": r.Backend, "output": *output, "performance_acceptance": false}); err != nil {
		return 1
	}
	return 0
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
