package replica

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Every successful cleanup below releases a genuine registered public view.
// Lock observation uses TryLock only; it introduces no IO/error/blocking hook.
type applicationViewCleanup struct {
	view    *raftlog.ApplicationView
	driver  *Driver
	closed  bool
	outside bool
}

func (c *applicationViewCleanup) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.outside = c.driver == nil || c.driver.mu.TryLock()
	if c.driver != nil && c.outside {
		c.driver.mu.Unlock()
	}
	err := c.view.Close()
	if !c.outside {
		return errors.Join(ErrInvalid, err)
	}
	return err
}

type cleanupApplication struct {
	applicationFixture
	store      *raftlog.Store
	driver     *Driver
	pending    *applicationViewCleanup
	owners     [16]*applicationViewCleanup
	ownerCount int
}

func (m *cleanupApplication) own(index uint64) error {
	if m.pending != nil || m.ownerCount >= len(m.owners) {
		return ErrInvalid
	}
	v, err := m.store.ApplicationView(index)
	if err != nil {
		return err
	}
	owner := &applicationViewCleanup{view: v, driver: m.driver}
	m.pending = owner
	m.owners[m.ownerCount] = owner
	m.ownerCount++
	return nil
}
func (m *cleanupApplication) Restore(index uint64, image []byte) error {
	if err := m.own(index); err != nil {
		return err
	}
	return m.applicationFixture.Restore(index, image)
}
func (m *cleanupApplication) Stage(e Entry, b raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if err := m.own(m.index); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return m.applicationFixture.Stage(e, b)
}
func (m *cleanupApplication) TakeApplicationCleanup() ApplicationCleanup {
	owner := m.pending
	m.pending = nil
	return owner
}
func cleanupDriver(t *testing.T, wantClose error) (*Driver, *cleanupApplication) {
	t.Helper()
	p := raftlog.DefaultApplicationPolicy(1)
	s, err := raftlog.Open(raftlog.Config{Dir: "callback-cleanup", FS: vfs.NewMem(), Create: true, Application: p})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Initialize([]uint64{1}, nil); err != nil {
		t.Fatal(errors.Join(err, s.Close()))
	}
	m := &cleanupApplication{store: s}
	d, err := Open(Config{ID: 1, Store: s, ApplicationMachine: m})
	if err != nil {
		t.Fatal(errors.Join(err, s.Close()))
	}
	m.driver = d
	t.Cleanup(func() {
		err := d.Close()
		if wantClose == nil && err != nil || wantClose != nil && !errors.Is(err, wantClose) {
			t.Error(err)
		}
	})
	return d, m
}
func assertCallbackOwners(t *testing.T, d *Driver, m *cleanupApplication) {
	t.Helper()
	if m.pending != nil {
		t.Fatal("undetached callback owner")
	}
	for _, owner := range m.owners {
		if owner == nil {
			continue
		}
		if !owner.closed || !owner.outside {
			t.Fatal("cleanup lock/ownership", owner)
		}
	}
	usage, err := d.store.ApplicationUsage()
	if err != nil || usage.Views != 0 || usage.ViewBytes != 0 {
		t.Fatal("callback views", usage, err)
	}
}
func TestApplicationMachineCleanupOpenStageRestoreAndError(t *testing.T) {
	d, m := cleanupDriver(t, nil)
	assertCallbackOwners(t, d, m)
	if _, err := d.Campaign(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Propose(command(7)); err != nil {
		t.Fatal(err)
	}
	assertCallbackOwners(t, d, m)
	if m.value != 7 || m.index != 3 {
		t.Fatal("real adjacent install", m.value, m.index)
	}
	before := d.Applied()
	out, err := d.Propose([]byte{1}) // Ordinary malformed command, not an injected failure.
	if !errors.Is(err, ErrInvalid) || !zeroPendingOutput(out) || d.Applied() != before {
		t.Fatal("failed Stage", out, err)
	}
	assertCallbackOwners(t, d, m)
	if out, err := d.Tick(); !errors.Is(err, ErrStopped) || !errors.Is(err, ErrInvalid) || !zeroPendingOutput(out) {
		t.Fatal("cached real input cause", out, err)
	}
}
func TestApplicationMachineCleanupInvalidOwnerStopsBeforeInstall(t *testing.T) {
	// An unregistered handle's real Close contract returns ErrInvalid. The
	// driver must not treat that capability error as successful cleanup.
	d, m := cleanupDriver(t, raftlog.ErrInvalid)
	if _, err := d.Campaign(); err != nil {
		t.Fatal(err)
	}
	before := d.Applied()
	bad := &unregisteredCleanupApplication{cleanupApplication: m}
	d.applicationMachine = bad // Refusal-only unit input; no successful authority.
	out, err := d.Propose(command(7))
	if !errors.Is(err, raftlog.ErrInvalid) || !zeroPendingOutput(out) || d.Applied() != before {
		t.Fatal("bad cleanup installed", out, err)
	}
	assertCallbackOwners(t, d, m)
	if out, err := d.Tick(); !errors.Is(err, ErrStopped) || !errors.Is(err, raftlog.ErrInvalid) || !zeroPendingOutput(out) {
		t.Fatal("lost cleanup cause", out, err)
	}
	firstClose, repeatedClose := d.Close(), d.Close()
	if !errors.Is(firstClose, raftlog.ErrInvalid) || !errors.Is(repeatedClose, raftlog.ErrInvalid) {
		t.Fatal("first cleanup cause on Close")
	}
}

type unregisteredCleanupApplication struct{ *cleanupApplication }

func (m *unregisteredCleanupApplication) TakeApplicationCleanup() ApplicationCleanup {
	real := m.cleanupApplication.TakeApplicationCleanup()
	if isNilMachine(real) {
		return nil
	}
	return &invalidApplicationCleanup{real: real}
}

type invalidApplicationCleanup struct{ real ApplicationCleanup }

func (c *invalidApplicationCleanup) Close() error {
	invalid := new(raftlog.ApplicationView)
	return errors.Join(invalid.Close(), c.real.Close())
}

func (m *cleanupApplication) ApplicationCallbackLimits() ApplicationCallbackLimits {
	return ApplicationCallbackLimits{StageWork: 1 << 20, StageOwned: 1 << 20, RestoreWork: 1 << 20, RestoreOwned: 1 << 20, CleanupWork: 1 << 20, CleanupOwned: 1 << 20}
}

type callbackProfileFixture struct {
	*cleanupApplication
	limits ApplicationCallbackLimits
}

func (m *callbackProfileFixture) ApplicationCallbackLimits() ApplicationCallbackLimits {
	return m.limits
}

type missingCallbackProfile struct{ ApplicationMachine }

func (m *missingCallbackProfile) TakeApplicationCleanup() ApplicationCleanup {
	return takeApplicationCleanup(m.ApplicationMachine)
}
func TestApplicationCallbackProfileValidationBeforeRestore(t *testing.T) {
	good := (&cleanupApplication{}).ApplicationCallbackLimits()
	for _, bad := range []ApplicationCallbackLimits{{}, {StageWork: ^uint64(0), StageOwned: 1, RestoreWork: 1, RestoreOwned: 1, CleanupWork: 1, CleanupOwned: 1}} {
		if _, err := applicationCallbackProfile(&callbackProfileFixture{cleanupApplication: &cleanupApplication{}, limits: bad}); bad == (ApplicationCallbackLimits{}) && !errors.Is(err, ErrInvalid) || bad != (ApplicationCallbackLimits{}) && !errors.Is(err, ErrLimit) {
			t.Fatal(bad, err)
		}
	}
	for field := range 6 {
		bad := good
		switch field {
		case 0:
			bad.StageWork = 0
		case 1:
			bad.StageOwned = 0
		case 2:
			bad.RestoreWork = 0
		case 3:
			bad.RestoreOwned = 0
		case 4:
			bad.CleanupWork = 0
		case 5:
			bad.CleanupOwned = 0
		}
		if got, err := applicationCallbackProfile(&callbackProfileFixture{cleanupApplication: &cleanupApplication{}, limits: bad}); got != (ApplicationCallbackLimits{}) || !errors.Is(err, ErrInvalid) {
			t.Fatal(field, got, err)
		}
	}
	if got, err := applicationCallbackProfile(&cleanupApplication{}); err != nil || got != good {
		t.Fatal(got, err)
	}
	if _, err := applicationCallbackProfile(&missingCallbackProfile{ApplicationMachine: &applicationFixture{}}); !errors.Is(err, ErrInvalid) {
		t.Fatal("missing source-work profile", err)
	}
	if got, err := applicationCallbackProfile(&applicationFixture{}); err != nil || got != (ApplicationCallbackLimits{}) {
		t.Fatal("legacy profile changed", got, err)
	}
}

func TestApplicationCommitCallbackMultiplicationOverflow(t *testing.T) {
	d, _ := cleanupDriver(t, nil)
	c := d.callbackLimits
	c.CleanupWork = ^uint64(0)/2 + 1
	inert := &Driver{config: d.config, store: d.store, callbackLimits: c}
	w, o, err := inert.applicationCommitRequirements(4096, 4096, raftlog.ApplicationContractForPolicy(d.store.ApplicationLimits()))
	if w != 0 || o != 0 || !errors.Is(err, ErrLimit) {
		t.Fatal("overflow must refuse zero before any actual event", w, o, err)
	}
}

// Invalid source/owned profiles must refuse the genuine Open door before Restore.
func TestApplicationOpenInvalidProfileBeforeRestore(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limits  ApplicationCallbackLimits
		missing bool
		want    error
	}{{name: "zero", want: ErrInvalid}, {name: "missing", missing: true, want: ErrInvalid}, {name: "overflow", limits: ApplicationCallbackLimits{StageWork: ^uint64(0), StageOwned: 1, RestoreWork: 1, RestoreOwned: 1, CleanupWork: 1, CleanupOwned: 1}, want: ErrLimit}} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := raftlog.Open(raftlog.Config{Dir: "profile-before-restore", FS: vfs.NewMem(), Create: true, Application: raftlog.DefaultApplicationPolicy(1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := s.Initialize([]uint64{1}, nil); err != nil {
				t.Fatal(err)
			}
			before, err := s.ApplicationUsage()
			if err != nil {
				t.Fatal(err)
			}
			m := &cleanupApplication{store: s}
			var machine ApplicationMachine = &callbackProfileFixture{cleanupApplication: m, limits: tc.limits}
			if tc.missing {
				machine = &missingCallbackProfile{ApplicationMachine: m}
			}
			d, err := Open(Config{ID: 1, Store: s, ApplicationMachine: machine})
			if d != nil || !errors.Is(err, tc.want) {
				t.Fatal("Open profile admission", d, err)
			}
			after, err := s.ApplicationUsage()
			if err != nil || after != before || m.ownerCount != 0 || m.pending != nil || m.index != 0 {
				t.Fatal("refused Open invoked Restore/created owner", after, before, m.ownerCount, m.index, err)
			}
		})
	}
}

// Refusal-only DTOs retain real immutable Store/Config limits and never enter RawNode.
func TestApplicationCommitActualLimitArithmeticOverflow(t *testing.T) {
	d, _ := cleanupDriver(t, nil)
	good := d.callbackLimits
	for _, tc := range []struct {
		name   string
		limits ApplicationCallbackLimits
	}{
		{name: "tail sum", limits: ApplicationCallbackLimits{StageWork: ^uint64(0)/2 - 100, StageOwned: 1, RestoreWork: 10, RestoreOwned: 1, CleanupWork: ^uint64(0) / 2, CleanupOwned: 1}},
		{name: "tail product", limits: ApplicationCallbackLimits{StageWork: ^uint64(0)/d.store.ApplicationLimits().MaxTailEntries + 1, StageOwned: 1, RestoreWork: 1, RestoreOwned: 1, CleanupWork: 1, CleanupOwned: 1}},
		{name: "owner sum", limits: ApplicationCallbackLimits{StageWork: 1, StageOwned: 1, RestoreWork: 1, RestoreOwned: ^uint64(0) - 5, CleanupWork: 1, CleanupOwned: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := tc.limits
			if _, err := applicationCallbackProfile(&callbackProfileFixture{cleanupApplication: &cleanupApplication{}, limits: fields}); err != nil {
				t.Fatal("valid immutable profile prerequisite", err)
			}
			inert := &Driver{config: d.config, store: d.store, callbackLimits: fields}
			w, o, err := inert.applicationCommitRequirements(4096, 4096, raftlog.ApplicationContractForPolicy(d.store.ApplicationLimits()))
			if w != 0 || o != 0 || !errors.Is(err, ErrLimit) {
				t.Fatal("zero refusal before any event", w, o, err)
			}
		})
	}
	if d.callbackLimits != good {
		t.Fatal("real Driver profile changed")
	}
}
