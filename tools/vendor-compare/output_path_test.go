package vendorcompare

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type untouchedVendor struct{ calls int }

func (s *untouchedVendor) Load(context.Context, Entity) error { s.calls++; return ErrContract }
func (s *untouchedVendor) Apply(context.Context, Event) error { s.calls++; return ErrContract }
func (s *untouchedVendor) Query(context.Context, Request, func(any) error) error {
	s.calls++
	return ErrContract
}
func (s *untouchedVendor) Traffic() (Traffic, error) { s.calls++; return Traffic{}, ErrContract }
func (s *untouchedVendor) PhysicalInventory(context.Context) (Inventory, error) {
	s.calls++
	return Inventory{}, ErrContract
}
func (s *untouchedVendor) Prepare(context.Context) (Identity, error) {
	s.calls++
	return Identity{}, ErrContract
}
func (s *untouchedVendor) Reopen(context.Context) error { s.calls++; return ErrContract }
func (s *untouchedVendor) Observe(context.Context) (Costs, error) {
	s.calls++
	return Costs{}, ErrContract
}

func fixtureGit(t *testing.T, args ...string) {
	t.Helper()
	command := exec.CommandContext(t.Context(), "git", args...)
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "LC_ALL=C", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("fixture Git %v: %v %s", args, err, output)
	}
}
func refuseBeforeVendor(t *testing.T, output string) {
	t.Helper()
	spy := &untouchedVendor{}
	result, err := Run(t.Context(), Config{Reference: protectedReference(t), Output: output, Limits: defaultLimits()}, spy, spy, exactWireValidator{})
	if !errors.Is(err, ErrContract) || result.Status != "" || spy.calls != 0 {
		t.Fatalf("refusal=%v status=%s vendor calls=%d", err, result.Status, spy.calls)
	}
}

func TestOutputGuardExternalControlReachesPrepareExactlyOnce(t *testing.T) {
	spy := &untouchedVendor{}
	parent := t.TempDir()
	result, err := Run(t.Context(), Config{Reference: protectedReference(t), Output: filepath.Join(parent, "output"), Limits: defaultLimits()}, spy, spy, exactWireValidator{})
	if !errors.Is(err, ErrContract) || result.Status != "" || spy.calls != 1 {
		t.Fatal("positive external control", result, err, spy.calls)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatal("control left staging/output data", entries, err)
	}
}

type repositoryBeforeExport struct {
	parent         string
	loads, queries int
}

func (s *repositoryBeforeExport) Load(_ context.Context, _ Entity) error {
	s.loads++
	if s.loads == 96 {
		return os.Mkdir(filepath.Join(s.parent, ".git"), 0700)
	}
	return nil
}
func (s *repositoryBeforeExport) Apply(context.Context, Event) error { return ErrContract }
func (s *repositoryBeforeExport) Query(context.Context, Request, func(any) error) error {
	s.queries++
	return ErrContract
}
func (s *repositoryBeforeExport) Traffic() (Traffic, error) { return Traffic{}, ErrContract }
func (s *repositoryBeforeExport) PhysicalInventory(context.Context) (Inventory, error) {
	return Inventory{}, ErrContract
}
func TestOutputGuardNewRepositoryBeforeExportWritesNoAnswerOrCompletion(t *testing.T) {
	parent := t.TempDir()
	output := filepath.Join(parent, "output")
	backend := &repositoryBeforeExport{parent: parent}
	lifecycle := &fakeLifecycle{}
	report, err := Run(t.Context(), Config{Reference: protectedReference(t), Output: output, Limits: defaultLimits()}, backend, lifecycle, exactWireValidator{})
	if !errors.Is(err, ErrContract) || report.Status != "" || backend.loads != 96 || backend.queries != 0 {
		t.Fatal(report, err, backend.loads, backend.queries)
	}
	if _, err := os.Lstat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("output artifacts created", err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 1 || entries[0].Name() != ".git" {
		t.Fatal("answer/completion/staging data retained", entries, err)
	}
}
func TestOutputGuardMainNestedLinkedWorktreesAndNoVendorCalls(t *testing.T) {
	parent := t.TempDir()
	repo := filepath.Join(parent, "repo")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	fixtureGit(t, "init", repo)
	fixtureGit(t, "-C", repo, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "fixture")
	nested := filepath.Join(repo, "nested", "deeper")
	if err := os.MkdirAll(nested, 0700); err != nil {
		t.Fatal(err)
	}
	linked := filepath.Join(parent, "linked")
	fixtureGit(t, "-C", repo, "worktree", "add", "--detach", linked, "HEAD")
	for _, dir := range []string{repo, nested, linked} {
		refuseBeforeVendor(t, filepath.Join(dir, "output"))
	}
	alias := filepath.Join(parent, "repo-alias")
	if err := os.Symlink(nested, alias); err != nil {
		t.Fatal(err)
	}
	refuseBeforeVendor(t, filepath.Join(alias, "output"))
	if err := os.WriteFile(filepath.Join(repo, "caller-data"), []byte("retain"), 0600); err != nil {
		t.Fatal(err)
	}
	refuseBeforeVendor(t, filepath.Join(repo, "caller-data"))
	if data, err := os.ReadFile(filepath.Join(repo, "caller-data")); err != nil || string(data) != "retain" {
		t.Fatal(string(data), err)
	}
}
func TestOutputGuardExistingDanglingMissingAndUnknownRefuse(t *testing.T) {
	parent := t.TempDir()
	file := filepath.Join(parent, "file")
	if err := os.WriteFile(file, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(parent, "directory")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	dangling := filepath.Join(parent, "dangling")
	if err := os.Symlink(filepath.Join(parent, "not-there"), dangling); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{file, directory, dangling, filepath.Join(parent, "missing-parent", "output")} {
		refuseBeforeVendor(t, path)
	}
	if data, err := os.ReadFile(file); err != nil || string(data) != "original" {
		t.Fatal(string(data), err)
	}
	t.Run("unavailable Git", func(t *testing.T) { t.Setenv("PATH", ""); refuseBeforeVendor(t, filepath.Join(parent, "unclassified")) })
	unknown := func(context.Context, string) error { return ErrContract }
	if _, err := guardOutput(t.Context(), filepath.Join(parent, "unknown"), unknown); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	spy := &untouchedVendor{}
	if _, err := Run(ctx, Config{Reference: "unused", Output: filepath.Join(parent, "cancel"), Limits: defaultLimits()}, spy, spy, exactWireValidator{}); !errors.Is(err, context.Canceled) || spy.calls != 0 {
		t.Fatal(err, spy.calls)
	}
}
func TestOutputGuardCanonicalExternalAliasAndSanitizedGitEnvironment(t *testing.T) {
	parent := t.TempDir()
	external := filepath.Join(parent, "external")
	if err := os.Mkdir(external, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(external, alias); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_DIR", filepath.Join(parent, "bogus-metadata"))
	t.Setenv("GIT_WORK_TREE", parent)
	guard, err := guardOutput(t.Context(), filepath.Join(alias, "output"), outsideGit)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = guard.close() }()
	canonical, err := filepath.EvalSymlinks(external)
	if err != nil {
		t.Fatal(err)
	}
	if guard.path != filepath.Join(canonical, "output") {
		t.Fatal(guard.path)
	}
	if err := guard.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	sourcePath := t.TempDir()
	if err := os.WriteFile(filepath.Join(sourcePath, "answer.jsonl"), []byte("answer\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := os.OpenRoot(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	if err := guard.copyFile(t.Context(), source, "answer.jsonl"); err != nil {
		t.Fatal(err)
	}
	if err := guard.copyFile(t.Context(), source, "answer.jsonl"); !errors.Is(err, os.ErrExist) {
		t.Fatal("overwrite", err)
	}
	if data, err := os.ReadFile(filepath.Join(guard.path, "answer.jsonl")); err != nil || string(data) != "answer\n" {
		t.Fatal(string(data), err)
	}
}
func TestOutputGuardDefaultTempAndRepositoryTMPDIR(t *testing.T) {
	base := t.TempDir()
	t.Setenv("TMPDIR", base)
	guard, err := guardOutput(t.Context(), "", outsideGit)
	if err != nil {
		t.Fatal(err)
	}
	realBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(guard.path) || !strings.HasPrefix(guard.path, realBase+string(filepath.Separator)) {
		t.Fatal(guard.path)
	}
	ownedParent := guard.parent
	if err := guard.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(ownedParent); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("empty owned default parent retained", err)
	}
	repo := filepath.Join(base, "repo")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".git"), []byte("gitdir: linked-metadata\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", repo)
	refuseBeforeVendor(t, "")
	entries, err := os.ReadDir(repo)
	if err != nil || len(entries) != 1 {
		t.Fatal("default refusal wrote files", entries, err)
	}
}
func TestOutputGuardParentReplacementAndNewRepositoryRefuse(t *testing.T) {
	base := t.TempDir()
	parent := filepath.Join(base, "parent")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	guard, err := guardOutput(t.Context(), filepath.Join(parent, "output"), outsideGit)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = guard.close() }()
	moved := filepath.Join(base, "original-parent")
	if err := os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(parent, "caller-data")
	if err := os.WriteFile(marker, []byte("retain"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := guard.create(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "retain" {
		t.Fatal(string(data), err)
	}
	other := filepath.Join(base, "other")
	if err := os.Mkdir(other, 0700); err != nil {
		t.Fatal(err)
	}
	newGuard, err := guardOutput(t.Context(), filepath.Join(other, "output"), outsideGit)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = newGuard.close() }()
	if err := os.Mkdir(filepath.Join(other, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := newGuard.create(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
}
func TestOutputGuardDestinationRepositoryRefusesPublication(t *testing.T) {
	parent := t.TempDir()
	guard, err := guardOutput(t.Context(), filepath.Join(parent, "output"), outsideGit)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = guard.close() }()
	if err := guard.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	sourcePath := t.TempDir()
	if err := os.WriteFile(filepath.Join(sourcePath, "answer.jsonl"), []byte("answer\n"), 0600); err != nil {
		t.Fatal(err)
	}
	source, err := os.OpenRoot(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	if err := os.Mkdir(filepath.Join(guard.path, ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := guard.copyFile(t.Context(), source, "answer.jsonl"); !errors.Is(err, ErrContract) {
		t.Error("published inside newly-created destination repository", err)
	}
	entries, err := os.ReadDir(guard.path)
	if err != nil || len(entries) != 1 || entries[0].Name() != ".git" {
		t.Error("refusal left artifacts", entries, err)
	}
}
func TestOutputGuardExclusiveNamesSourceTypesAndDestinationReplacement(t *testing.T) {
	for _, fault := range []string{"nil-source", "empty-name", "nested-name", "directory", "symlink", "existing-final", "existing-part", "replacement", "repository-during-copy"} {
		t.Run(fault, func(t *testing.T) {
			parent := t.TempDir()
			path := filepath.Join(parent, "output")
			triggered := false
			check := outsideGit
			if fault == "repository-during-copy" {
				check = func(ctx context.Context, parent string) error {
					if !triggered {
						if _, err := os.Stat(filepath.Join(path, ".part-answer.jsonl")); err == nil {
							triggered = true
							if err := os.Mkdir(filepath.Join(path, ".git"), 0700); err != nil {
								return err
							}
						}
					}
					return outsideGit(ctx, parent)
				}
			}
			guard, err := guardOutput(t.Context(), path, check)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = guard.close() }()
			if err := guard.create(t.Context()); err != nil {
				t.Fatal(err)
			}
			sourcePath := t.TempDir()
			name := "answer.jsonl"
			if err := os.WriteFile(filepath.Join(sourcePath, name), []byte("answer\n"), 0600); err != nil {
				t.Fatal(err)
			}
			source, err := os.OpenRoot(sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			defer func(root *os.Root) { _ = root.Close() }(source)
			var marker string
			var original os.FileInfo
			switch fault {
			case "nil-source":
				source = nil
			case "empty-name":
				name = ""
			case "nested-name":
				name = "nested/answer.jsonl"
			case "directory":
				if err := os.Remove(filepath.Join(sourcePath, name)); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(sourcePath, name), 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Remove(filepath.Join(sourcePath, name)); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(sourcePath, "caller-data"), filepath.Join(sourcePath, name)); err != nil {
					t.Fatal(err)
				}
			case "existing-final":
				marker = filepath.Join(path, name)
			case "existing-part":
				marker = filepath.Join(path, ".part-"+name)
			case "replacement":
				if err := os.Rename(path, filepath.Join(parent, "old-output")); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				marker = filepath.Join(path, "caller-data")
			}
			if marker != "" {
				if err := os.WriteFile(marker, []byte("caller original"), 0600); err != nil {
					t.Fatal(err)
				}
				original, err = os.Lstat(marker)
				if err != nil {
					t.Fatal(err)
				}
			}
			err = guard.copyFile(t.Context(), source, name)
			if fault == "existing-final" || fault == "existing-part" {
				if !errors.Is(err, os.ErrExist) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrContract) {
				t.Fatal(err)
			}
			if marker != "" {
				data, err := os.ReadFile(marker)
				current, statErr := os.Lstat(marker)
				if err != nil || statErr != nil || string(data) != "caller original" || !os.SameFile(original, current) {
					t.Fatal("caller bytes/identity changed", string(data), err, statErr)
				}
			}
			if fault != "existing-final" {
				if _, err := os.Lstat(filepath.Join(path, "answer.jsonl")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("completed output appeared", err)
				}
			}
			if fault == "repository-during-copy" && !triggered {
				t.Fatal("copy-boundary hook not reached")
			}
		})
	}
	var guard *outputGuard
	if err := guard.recheck(t.Context()); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	if err := guard.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := guardOutput(nil, "", outsideGit); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
	if _, err := guardOutput(t.Context(), "", nil); !errors.Is(err, ErrContract) {
		t.Fatal(err)
	}
}
