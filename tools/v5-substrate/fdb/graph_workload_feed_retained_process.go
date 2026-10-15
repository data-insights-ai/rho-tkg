package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type graphFeedRetainedChildInput struct {
	Catalog      graphOwnerBinding
	Authorities  [2]graphOwnerAuthority
	Policy       graphFeedPolicy
	Token        graphFeedToken
	Boundary     certificate
	Reader, Page string
}
type graphFeedRetainedChildResult struct {
	PID                            int
	Identity                       graphFeedRetainedIdentity
	Delivery                       graphFeedDelivery
	Ack                            *graphFeedAcknowledgement `json:",omitempty"`
	Work                           graphSnapshotWork
	OpenNanos, PageNanos, AckNanos int64
}

func runGraphFeedRetainedChild(db fdb.Database, resume bool) (err error) {
	admission, policy, err := graphFeedRetainedEnvironment()
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(os.Getenv("FDB_COST_ARTIFACT"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	input, err := feedChildRead[graphFeedRetainedChildInput](root, "fdb-retained-child-input.json")
	if err != nil {
		return err
	}
	if input.Catalog.validate() != nil || input.Catalog.Transfer == nil || !graphColumnSame(*input.Catalog.Transfer, policy) || input.Policy != graphFeedPolicyFor(policy.Config.Cost) || input.Token.DeadlineNanos != policy.DeadlineUnixNanos || input.Reader != "retained-process-reader" || input.Page != "retained-process-page" {
		return errMismatch
	}
	ctx, cancel := graphTransportContext(&admission)
	defer cancel()
	h, err := newHarnessContext(ctx, db, policy.Config)
	if err != nil {
		return err
	}
	source, err := newGraphHistoryRetentionContext(ctx, h, cut{input.Catalog.Source.Initial}, input.Catalog.Source.Policy)
	if err != nil {
		return err
	}
	catalog, err := newGraphOwnerCatalogTransfer(ctx, source, input.Authorities, policy)
	if err != nil {
		return err
	}
	if !graphColumnSame(catalog.binding, input.Catalog) {
		return errMismatch
	}
	store, err := newGraphRetainedFeedStore(ctx, catalog, input.Policy)
	if err != nil {
		return err
	}
	defer store.stopNormal()
	begin := time.Now()
	f, err := store.OpenRetained(ctx, input.Token, input.Reader)
	if err != nil {
		return err
	}
	out := graphFeedRetainedChildResult{PID: os.Getpid(), Identity: f.retained.id, OpenNanos: time.Since(begin).Nanoseconds()}
	begin = time.Now()
	out.Delivery, err = f.Page(ctx, input.Page, input.Boundary)
	if err != nil {
		return errors.Join(err, f.Close())
	}
	out.PageNanos = time.Since(begin).Nanoseconds()
	if !resume {
		if err = feedChildWrite(root, "fdb-retained-child-ready.json", out); err != nil {
			return errors.Join(err, f.Close())
		}
		_, err = io.ReadFull(os.Stdin, make([]byte, 1))
		return errors.Join(errUnavailable, err, f.Close())
	}
	if !out.Delivery.Replay {
		return errors.Join(errMismatch, f.Close())
	}
	begin = time.Now()
	ack, err := f.Ack(ctx, input.Page, sha256.Sum256(out.Delivery.Record.Value))
	if err != nil {
		return errors.Join(err, f.Close())
	}
	out.Ack = new(ack)
	out.AckNanos = time.Since(begin).Nanoseconds()
	if err = f.Close(); err != nil {
		return err
	}
	out.Work = f.Work()
	return feedChildWrite(root, "fdb-retained-child-resumed.json", out)
}
func (a *graphFeedRetainedArtifacts) replace(input graphFeedRetainedChildInput) (result graphFeedRetainedChildResult, err error) {
	if err = feedChildWrite(a.root, "fdb-retained-child-input.json", input); err != nil {
		return result, err
	}
	executable, err := os.Executable()
	if err != nil {
		return result, err
	}
	launch := func(mode string) (*exec.Cmd, *graphFeedBoundedOutput, io.WriteCloser, error) {
		file, _, err := openGraphCostArtifact(a.root, "fdb-retained-"+mode+".output")
		if err != nil {
			return nil, nil, nil, err
		}
		output := &graphFeedBoundedOutput{file: file}
		command := exec.CommandContext(a.ctx, executable, mode)
		command.WaitDelay = 3 * time.Second
		command.Stdout, command.Stderr = output, output
		stdin, err := command.StdinPipe()
		if err != nil {
			return nil, nil, nil, errors.Join(err, file.Close())
		}
		if err = command.Start(); err != nil {
			return nil, nil, nil, errors.Join(err, stdin.Close(), file.Close())
		}
		return command, output, stdin, nil
	}
	first, output, stdin, err := launch("feed-retained-hold")
	if err != nil {
		return result, err
	}
	waited := false
	defer func() {
		if !waited {
			_ = first.Process.Kill()
			err = errors.Join(err, first.Wait())
		}
		err = errors.Join(err, stdin.Close())
		if output.file != nil {
			err = errors.Join(err, output.file.Close())
		}
	}()
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var ready graphFeedRetainedChildResult
	for {
		ready, err = feedChildRead[graphFeedRetainedChildResult](a.root, "fdb-retained-child-ready.json")
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return result, err
		}
		select {
		case <-a.ctx.Done():
			return result, a.ctx.Err()
		case <-timer.C:
			return result, context.DeadlineExceeded
		case <-tick.C:
		}
	}
	if ready.PID != first.Process.Pid || ready.Delivery.Replay || ready.Ack != nil || len(ready.Delivery.Page.Rows) == 0 {
		return result, errMismatch
	}
	if err = first.Process.Signal(syscall.SIGKILL); err != nil {
		return result, err
	}
	waitErr := first.Wait()
	waited = true
	exit, ok := errors.AsType[*exec.ExitError](waitErr)
	if !ok {
		return result, errors.Join(errMismatch, waitErr)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		return result, errMismatch
	}
	if err = errors.Join(output.Err(), output.file.Sync(), output.file.Close()); err != nil {
		return result, err
	}
	output.file = nil
	a.end.Processes[0], a.end.KilledReaped = ready.PID, true
	if err = a.event(struct {
		Type   string
		Input  graphFeedRetainedChildInput
		Result graphFeedRetainedChildResult
		Signal string
		Reaped bool
	}{"child-killed", input, ready, "SIGKILL", true}); err != nil {
		return result, err
	}
	second, replacement, replacementInput, err := launch("feed-retained-resume")
	if err != nil {
		return result, err
	}
	closeInputErr := replacementInput.Close()
	if closeInputErr != nil {
		_ = second.Process.Kill()
	}
	waitErr = second.Wait()
	if err = errors.Join(closeInputErr, waitErr, replacement.Err(), replacement.file.Sync(), replacement.file.Close()); err != nil {
		return result, err
	}
	result, err = feedChildRead[graphFeedRetainedChildResult](a.root, "fdb-retained-child-resumed.json")
	if err != nil {
		return result, err
	}
	if result.PID != second.Process.Pid || result.PID == ready.PID || result.Identity != ready.Identity || result.Ack == nil || !result.Delivery.Replay || !bytes.Equal(result.Delivery.Record.Value, ready.Delivery.Record.Value) || result.Ack.Ack.Page != sha256.Sum256(ready.Delivery.Record.Value) {
		return result, errMismatch
	}
	a.end.Processes[1], a.end.ReplacementReaped = result.PID, true
	if err = a.event(struct {
		Type   string
		Result graphFeedRetainedChildResult
		Reaped bool
	}{"child-replaced", result, true}); err != nil {
		return result, err
	}
	return result, nil
}
