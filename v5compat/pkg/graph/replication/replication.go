// Package replication mirrors the v4 import path pkg/graph/replication: the change feed, LSN follower and ID slot lease. Every method keeps its
// v4 signature and reports ErrNotSupportedInV5 (declined or not in the first
// batch of WP-11); the types are placeholders so consumers compile.
package replication

import (
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// ErrNotSupportedInV5 is the class of every declined door.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5

func declined(door string) error { return fmt.Errorf("%w: "+door, ErrNotSupportedInV5) }

// API is the sub-API handle returned by the graph accessor.
type API struct{}

// Watcher is a placeholder for the v4 type of the same name.
type Watcher struct{}

// ChangeFeed is declined.
func (a *API) ChangeFeed(afterLSN uint64, limit int) (r0 []store.ChangeRecord, err error) {
	err = declined("replication.ChangeFeed")
	return
}

// ForEachChange is declined.
func (a *API) ForEachChange(afterLSN uint64, fn func(store.ChangeRecord) bool) error {
	return declined("replication.ForEachChange")
}

// LastCommittedLSN is declined.
func (a *API) LastCommittedLSN() (r0 uint64, err error) {
	err = declined("replication.LastCommittedLSN")
	return
}

// ApplyChange is declined.
func (a *API) ApplyChange(rec store.ChangeRecord) error {
	return declined("replication.ApplyChange")
}

// ApplyChanges is declined.
func (a *API) ApplyChanges(recs []store.ChangeRecord) (r0 uint64, err error) {
	err = declined("replication.ApplyChanges")
	return
}

// AppliedLSN is declined.
func (a *API) AppliedLSN() (r0 uint64, err error) {
	err = declined("replication.AppliedLSN")
	return
}

// SetAppliedLSN is declined.
func (a *API) SetAppliedLSN(lsn uint64) error {
	return declined("replication.SetAppliedLSN")
}

// RegistrySnapshot is declined.
func (a *API) RegistrySnapshot() (r0 *store.RegistrySnapshot, err error) {
	err = declined("replication.RegistrySnapshot")
	return
}

// IDSlotLease is declined.
func (a *API) IDSlotLease() (r0 *store.IDSlotLeaseRecord, err error) {
	err = declined("replication.IDSlotLease")
	return
}

// SetIDSlotLease is declined.
func (a *API) SetIDSlotLease(rec *store.IDSlotLeaseRecord) error {
	return declined("replication.SetIDSlotLease")
}

// Watch is declined.
func (a *API) Watch(afterLSN uint64) (r0 *Watcher, err error) {
	err = declined("replication.Watch")
	return
}
