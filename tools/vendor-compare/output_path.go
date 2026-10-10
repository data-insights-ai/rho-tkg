package vendorcompare

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type gitCheck func(context.Context, string) error

func outsideGit(ctx context.Context, parent string) error {
	if ctx == nil {
		return ErrContract
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for dir := parent; ; dir = filepath.Dir(dir) {
		if _, err := os.Lstat(filepath.Join(dir, ".git")); err == nil {
			return fmt.Errorf("%w: output parent is in a Git worktree", ErrContract)
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.Join(ErrContract, err)
		}
		if filepath.Dir(dir) == dir {
			break
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// #nosec G204 -- Fixed Git subcommand; canonical parent is an argument, not shell text.
	command := exec.CommandContext(ctx, "git", "-C", parent, "rev-parse", "--is-inside-work-tree", "--is-inside-git-dir")
	for _, item := range os.Environ() {
		key, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(key, "GIT_") || key == "LC_ALL" {
			continue
		}
		command.Env = append(command.Env, item)
	}
	command.Env = append(command.Env, "LC_ALL=C")
	output, err := command.Output()
	if err != nil {
		if ctx.Err() != nil {
			return errors.Join(ErrContract, ctx.Err())
		}
		if exit, ok := errors.AsType[*exec.ExitError](err); ok && exit.ExitCode() == 128 && strings.Contains(string(exit.Stderr), "not a git repository") {
			return nil
		}
		return errors.Join(ErrContract, err)
	}
	if string(output) != "false\nfalse\n" {
		return fmt.Errorf("%w: Git output parent classification is unsafe or unknown", ErrContract)
	}
	return nil
}

type outputGuard struct {
	path, parent, leaf string
	root               *os.Root
	parentInfo         os.FileInfo
	destination        *os.Root
	destinationInfo    os.FileInfo
	defaultParent      string
	defaultInfo        os.FileInfo
	check              gitCheck
}

func canonicalParent(ctx context.Context, path string, check gitCheck) (string, os.FileInfo, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", nil, errors.Join(ErrContract, err)
	}
	parent, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", nil, errors.Join(ErrContract, err)
	}
	info, err := os.Lstat(parent)
	if err != nil || !info.IsDir() {
		return "", nil, errors.Join(ErrContract, err)
	}
	if err := check(ctx, parent); err != nil {
		return "", nil, err
	}
	return parent, info, nil
}
func removeEmptyOwned(path string, original os.FileInfo) {
	current, err := os.Lstat(path)
	if err == nil && original != nil && os.SameFile(current, original) {
		_ = os.Remove(path)
	}
}
func guardOutput(ctx context.Context, requested string, check gitCheck) (*outputGuard, error) {
	if ctx == nil || check == nil {
		return nil, ErrContract
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defaultParent := ""
	var defaultInfo os.FileInfo
	if requested == "" {
		base, _, err := canonicalParent(ctx, os.TempDir(), check)
		if err != nil {
			return nil, err
		}
		defaultParent, err = os.MkdirTemp(base, "vendor-compare-output-")
		if err != nil {
			return nil, err
		}
		defaultInfo, err = os.Lstat(defaultParent)
		if err != nil {
			return nil, err
		}
		requested = filepath.Join(defaultParent, "artifacts")
	}
	failDefault := func() {
		if defaultParent != "" {
			removeEmptyOwned(defaultParent, defaultInfo)
		}
	}
	absolute, err := filepath.Abs(requested)
	if err != nil {
		failDefault()
		return nil, errors.Join(ErrContract, err)
	}
	parent, info, err := canonicalParent(ctx, filepath.Dir(absolute), check)
	if err != nil {
		failDefault()
		return nil, err
	}
	leaf := filepath.Base(absolute)
	if leaf == "." || leaf == ".." || leaf == string(filepath.Separator) {
		failDefault()
		return nil, ErrContract
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		failDefault()
		return nil, err
	}
	guard := &outputGuard{path: filepath.Join(parent, leaf), parent: parent, leaf: leaf, root: root, parentInfo: info, defaultParent: defaultParent, defaultInfo: defaultInfo, check: check}
	if err := guard.recheck(ctx); err != nil {
		_ = guard.close()
		return nil, err
	}
	if _, err := root.Lstat(leaf); !errors.Is(err, os.ErrNotExist) {
		_ = guard.close()
		return nil, errors.Join(ErrContract, err)
	}
	return guard, nil
}
func (g *outputGuard) recheck(ctx context.Context) error {
	if g == nil || g.root == nil || ctx == nil {
		return ErrContract
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := os.Lstat(g.parent)
	if err != nil || !current.IsDir() || !os.SameFile(g.parentInfo, current) {
		return errors.Join(ErrContract, err)
	}
	anchored, err := g.root.Stat(".")
	if err != nil || !os.SameFile(g.parentInfo, anchored) {
		return errors.Join(ErrContract, err)
	}
	if err := g.check(ctx, g.parent); err != nil {
		return err
	}
	if g.destination == nil {
		if _, err := g.root.Lstat(g.leaf); !errors.Is(err, os.ErrNotExist) {
			return errors.Join(ErrContract, err)
		}
	}
	if g.destination != nil {
		current, err := g.root.Lstat(g.leaf)
		if err != nil || !current.IsDir() || !os.SameFile(g.destinationInfo, current) {
			return errors.Join(ErrContract, err)
		}
		anchored, err := g.destination.Stat(".")
		if err != nil || !os.SameFile(g.destinationInfo, anchored) {
			return errors.Join(ErrContract, err)
		}
		if err := g.check(ctx, g.path); err != nil {
			return err
		}
	}
	return nil
}
func (g *outputGuard) create(ctx context.Context) error {
	if err := g.recheck(ctx); err != nil {
		return err
	}
	if err := g.root.Mkdir(g.leaf, 0700); err != nil {
		return err
	}
	destination, err := g.root.OpenRoot(g.leaf)
	if err != nil {
		return err
	}
	info, err := destination.Stat(".")
	if err != nil {
		_ = destination.Close()
		return err
	}
	g.destination = destination
	g.destinationInfo = info
	return g.recheck(ctx)
}
func (g *outputGuard) copyFile(ctx context.Context, source *os.Root, name string) error {
	if source == nil || g == nil || g.destination == nil || name == "" || filepath.Base(name) != name {
		return ErrContract
	}
	if err := g.recheck(ctx); err != nil {
		return err
	}
	info, err := source.Lstat(name)
	if err != nil || !info.Mode().IsRegular() {
		return errors.Join(ErrContract, err)
	}
	input, err := source.Open(name)
	if err != nil {
		return err
	}
	opened, err := input.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = input.Close()
		return errors.Join(ErrContract, err)
	}
	part := ".part-" + name
	output, err := g.destination.OpenFile(part, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.Join(err, input.Close())
	}
	created, statErr := output.Stat()
	if statErr != nil {
		return errors.Join(statErr, output.Close(), input.Close())
	}
	_, copyErr := io.Copy(output, input)
	if err := errors.Join(copyErr, output.Sync(), output.Close(), input.Close()); err != nil {
		return err
	}
	if err := g.recheck(ctx); err != nil {
		return err
	}
	current, err := g.destination.Lstat(part)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(created, current) {
		return errors.Join(ErrContract, err)
	}
	if err := g.destination.Link(part, name); err != nil {
		return err
	} // Final names, including completion, appear only when complete.
	_ = g.destination.Remove(part)
	return nil
}
func (g *outputGuard) close() error {
	if g == nil {
		return nil
	}
	var err error
	if g.destination != nil {
		err = g.destination.Close()
	}
	if g.root != nil {
		err = errors.Join(err, g.root.Close())
	}
	if g.defaultParent != "" {
		removeEmptyOwned(g.defaultParent, g.defaultInfo)
	}
	return err
}
