package graphapply

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strings"
	"syscall"
)

// These selectors stop an actual opted-in OS child. They do not establish that
// any graph proposal committed. An after-WAL-write receipt is deliberately not
// a durability receipt; only a successful synchronization provides that claim.
// Existing arm ownership admits at most16 receipts, Nth<=128, each<=4096 bytes.
type nativeTransitionIO struct {
	Operation   string
	Side        string
	Returned    bool
	Guaranteed  bool
	ReturnError string `json:",omitempty"`
}

func nativeTransitionSelector(kind string) (operation, side string, ok bool) {
	for _, suffix := range []string{"-before", "-after"} {
		if !strings.HasSuffix(kind, suffix) {
			continue
		}
		operation = strings.TrimSuffix(kind, suffix)
		switch operation {
		case "wal-write", "wal-sync", "wal-sync-data", "wal-sync-to", "directory-sync", "options-rename", "manifest-create", "manifest-close":
			return operation, strings.TrimPrefix(suffix, "-"), true
		}
	}
	return "", "", false
}

func nativeTransitionMatches(kind, path string, directory bool, operation string) bool {
	selector, _, ok := nativeTransitionSelector(kind)
	if !ok {
		return false
	}
	name := filepath.Base(path)
	switch selector {
	case "wal-write":
		return !directory && strings.HasSuffix(name, ".log") && operation == "write"
	case "wal-sync":
		return !directory && strings.HasSuffix(name, ".log") && operation == "sync"
	case "wal-sync-data":
		return !directory && strings.HasSuffix(name, ".log") && operation == "sync-data"
	case "wal-sync-to":
		return !directory && strings.HasSuffix(name, ".log") && operation == "sync-to"
	case "directory-sync":
		return directory && operation == "sync"
	case "options-rename":
		return !directory && operation == "rename"
	case "manifest-create", "manifest-close":
		return !directory && strings.HasPrefix(name, "marker.manifest.") && operation == strings.TrimPrefix(selector, "manifest-")
	}
	return false
}

func (f *nativeFaultFS) transitionStop(kind string, written []byte, returned, guaranteed bool, actual error) error {
	operation, side, ok := nativeTransitionSelector(kind)
	if !ok || returned != (side == "after") || guaranteed && (!returned || actual != nil) {
		return errInvalid
	}
	point := &nativeTransitionIO{Operation: operation, Side: side, Returned: returned, Guaranteed: guaranteed}
	if actual != nil {
		point.ReturnError = actual.Error()
		if len(point.ReturnError) > 1024 {
			return errLimit
		}
	}
	f.mu.Lock()
	f.state.IO.Transition = point
	f.mu.Unlock()
	stop := errors.New("declared native storage transition SIGKILL")
	if err := f.complete(written, errors.Join(stop, actual)); err != nil {
		return err
	}
	// Diagnostic persistence is outside the injected FS. A receipt cannot
	// survive only in a pipe buffer, and no parent fallback kill qualifies it.
	return errors.Join(stop, actual, syscall.Kill(syscall.Getpid(), syscall.SIGKILL))
}

func (f *nativeFaultFS) transitionCall(path string, directory bool, operation string, call func() (bool, error)) (bool, bool, error) {
	kind, matched, err := f.match(path, directory, operation, nil)
	if err != nil || !matched {
		return false, false, err
	}
	_, side, ok := nativeTransitionSelector(kind)
	if !ok {
		return false, false, errInvalid
	}
	if side == "before" {
		return true, false, f.transitionStop(kind, nil, false, false, nil)
	}
	guaranteed, actual := call()
	return true, guaranteed, f.transitionStop(kind, nil, true, guaranteed && actual == nil, actual)
}

func nativeTransitionReceiptValid(receipt nativeFaultReceipt) bool {
	operation, side, ok := nativeTransitionSelector(receipt.Kind)
	point := receipt.IO.Transition
	if receipt.Version != 1 || receipt.Profile != nativeFaultProfile || receipt.Cookie == ([16]byte{}) || receipt.Member < 1 || receipt.Member > 6 || receipt.PID == 0 || receipt.Sequence == 0 || receipt.Arms < receipt.Sequence || receipt.Arms > 16 || !ok || point == nil || point.Operation != operation || point.Side != side || point.Returned != (side == "after") || !receipt.Matched || receipt.Armed || receipt.Seen != receipt.Nth || receipt.Nth < 1 || receipt.Nth > 128 || point.ReturnError != "" {
		return false
	}
	path := receipt.IO.Path
	if path == "" || len(path) > 1024 || filepath.IsAbs(path) || filepath.Clean(path) != path || path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)) || strings.Contains(path, "\\") {
		return false
	}
	if !nativeTransitionMatches(receipt.Kind, receipt.IO.Path, receipt.IO.Directory, receipt.IO.Kind) {
		return false
	}
	sync := operation == "wal-sync" || operation == "wal-sync-data" || operation == "wal-sync-to" || operation == "directory-sync"
	// An actual queued SyncTo is authentic evidence, but does not complete a
	// requested guaranteed-after-sync cell. Its controller reports unavailable.
	if point.Guaranteed != (sync && side == "after") {
		return false
	}
	if side == "before" && receipt.IO.Written != 0 {
		return false
	}
	if receipt.IO.Requested > processFrameBytes || receipt.IO.Written > receipt.IO.Requested {
		return false
	}
	empty := sha256.Sum256(nil)
	if operation != "wal-write" && (receipt.IO.Requested != 0 || receipt.IO.Written != 0 || receipt.IO.InputSHA256 != empty || receipt.IO.WrittenSHA256 != empty) {
		return false
	}
	if operation == "wal-write" && side == "after" && (receipt.IO.Requested == 0 || receipt.IO.Written != receipt.IO.Requested || receipt.IO.InputSHA256 != receipt.IO.WrittenSHA256) {
		return false
	}
	if operation == "options-rename" && (!strings.HasPrefix(receipt.IO.Destination, "OPTIONS-") || filepath.Base(receipt.IO.Destination) != receipt.IO.Destination) {
		return false
	}
	return receipt.IO.Error == "declared native storage transition SIGKILL"
}
