package replica

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func (d *Driver) admitApplicationData(data []byte) error {
	if d.store.ApplicationRecovery().Version == 0 {
		return d.store.AdmitApplication(len(data))
	}
	_, err := d.store.AdmitApplicationRecovery(context.Background(), data)
	return err
}

func (d *Driver) bindApplicationRecoveryBatch(index uint64, b *raftlog.ApplicationBatch) error {
	if d.store.ApplicationRecovery().Version == 0 {
		return nil
	}
	claim, err := d.store.RecoveryAdmissionForCommittedEntry(context.Background(), index)
	if err != nil {
		return err
	}
	if b.RecoveryReserve != nil || b.RecoveryUse != nil {
		b.RecoveryAdmission = claim
	}
	return nil
}
