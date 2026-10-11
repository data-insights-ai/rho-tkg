package badger

import (
	"sync"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

type identityCloseLease struct {
	bs       *Store
	mu       sync.Mutex
	released bool
	sealed   bool
}

var _ storecontract.CloseAdmissionCapability = (*Store)(nil)

// CloseAdmissionRequired reports whether graph close needs the opted two-phase
// reservation. Default stores preserve their existing close behavior.
func (bs *Store) CloseAdmissionRequired() bool { return bs != nil && bs.entityIdentityOnDisk }

// AdmitClose reserves graph close without disabling predicate reads needed by
// accepted ingest. The caller drains ingest, seals readers, then closes the lease.
func (bs *Store) AdmitClose() (storecontract.CloseAdmissionLease, error) {
	if bs == nil {
		return nil, ErrNilStore
	}
	if !bs.entityIdentityOnDisk {
		return nil, nil
	}
	bs.identityReadMu.Lock()
	defer bs.identityReadMu.Unlock()
	if bs.identityCallbacks > 0 || bs.identityResetting || bs.identityCloseReservation != nil {
		return nil, IdentityCallbackMaintenanceError{Operation: "graph Close"}
	}
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	lease := &identityCloseLease{bs: bs}
	bs.identityCloseReservation = lease
	return lease, nil
}

func (lease *identityCloseLease) Seal() error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return IdentityCallbackMaintenanceError{Operation: "Seal released close lease"}
	}
	if lease.sealed {
		return nil
	}
	bs := lease.bs
	bs.identityReadMu.Lock()
	if err := bs.checkOpen(); err != nil {
		bs.identityReadMu.Unlock()
		return err
	}
	bs.identityClosePreparing = true
	bs.identityReadMu.Unlock()
	bs.identityReaders.Wait()
	lease.sealed = true
	return nil
}

func (lease *identityCloseLease) Close() error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released || !lease.sealed {
		return IdentityCallbackMaintenanceError{Operation: "Close unsealed/released lease"}
	}
	return lease.bs.closeWithAdmission(lease)
}

func (lease *identityCloseLease) Release() {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return
	}
	bs := lease.bs
	bs.identityReadMu.Lock()
	if bs.identityCloseReservation == lease {
		bs.identityClosePreparing = false
		bs.identityCloseReservation = nil
	}
	bs.identityReadMu.Unlock()
	lease.released = true
}
