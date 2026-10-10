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
	"time"

	vendor "github.com/data-insights-ai/rho-tkg/tools/vendor-compare"
)

func run(ctx context.Context, args []string, out, errOut io.Writer) int {
	if ctx == nil {
		return 2
	}
	if len(args) == 1 && args[0] == "capabilities" {
		if err := json.NewEncoder(out).Encode(map[string]any{"adapter": "TigerGraph 4.2.5 basic-current functional candidate", "image": vendor.ImageReference, "current_answers_required": 92, "reopened_answers_required": 23, "history": "unsupported/pending", "platform": "linux/amd64; ARM emulation explicitly labeled", "container_creation_or_image_pull": false, "complete_cost_acceptance": false, "performance_acceptance": false, "V7_complete": false}); err != nil {
			return 1
		}
		return 0
	}
	if len(args) < 1 || args[0] != "run" {
		_, _ = fmt.Fprintln(errOut, "usage: vendor-compare capabilities | run --reference <frozen-reference> --out <new-directory>")
		return 2
	}
	flags := flag.NewFlagSet("vendor-compare run", flag.ContinueOnError)
	flags.SetOutput(errOut)
	reference := flags.String("reference", "", "unchanged 114-file reference directory")
	output := flags.String("out", "", "local output directory; outside-Git guarding is unfinished")
	timeout := flags.Duration("timeout", 20*time.Minute, "bounded adapter run")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 || *timeout <= 0 || *timeout > 30*time.Minute {
		return 2
	}
	backend, err := vendor.NewTigerGraph(vendor.Endpoint, nil)
	if err != nil {
		_, _ = fmt.Fprintln(errOut, err)
		return 1
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	report, err := vendor.Run(ctx, vendor.Config{Reference: *reference, Output: *output, Limits: vendor.Limits{MaxRows: 100000, MaxBytes: 64 << 20, MaxVisited: 100000}}, backend, vendor.NewDockerController(), vendor.PythonValidator{})
	if err != nil {
		_, _ = fmt.Fprintln(errOut, "adapter run refused:", err)
		return 1
	}
	if err := json.NewEncoder(out).Encode(map[string]any{"status": report.Status, "current_answers": report.CurrentAnswers, "reopened_answers": report.ReopenedAnswers, "costs_complete": report.Costs.Complete, "execution_mode": report.Identity.ExecutionMode, "performance_acceptance": false}); err != nil {
		return 1
	}
	return 0
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
