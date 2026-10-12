package replica

import (
	"bytes"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Called only for bound normal-entry mode, under the unchanged Driver event.
// Original C is actual RawNode committed authority, not packet Commit or a cut.
// One original Ready remains pending until ALL its appended entries are durable.
func (d *Driver) progressApplicationReady(rd raft.Ready, current **raftlog.ApplicationReadyProgress) error {
	if *current != nil {
		// Advance may reveal additional actual committed pages or commit-only work.
		// Bound fixed-membership Advance cannot create a new appended command here.
		if len(rd.Entries) != 0 || !raft.IsEmptySnap(rd.Snapshot) {
			return ErrInvalid
		}
		return d.store.Persist(rd)
	}
	status := d.raw.BasicStatus()
	progress, err := d.store.BeginApplicationReadyProgress(d.binding, rd, status.GetCommit(), status.Applied)
	if err != nil {
		return err
	}
	*current = progress
	full, err := progress.PersistFull()
	if err != nil {
		return err
	}
	if full {
		return nil
	} // Apply already-offered entries through the original path.
	if !full {
		predecessor, err := progress.CommitPredecessor()
		if err != nil {
			return err
		}
		if err := d.applyDurableApplicationThrough(predecessor); err != nil {
			return err
		}
		for steps := 0; steps < len(rd.Entries); steps++ {
			_, done, err := progress.AppliedLimit()
			if err != nil {
				return err
			}
			if done {
				break
			}
			if _, err := progress.PersistPrefix(); err != nil {
				return err
			}
			through, _, err := progress.AppliedLimit()
			if err != nil {
				return err
			}
			if err := d.applyDurableApplicationThrough(through); err != nil {
				return err
			}
		}
	}
	through, done, err := progress.AppliedLimit()
	if err != nil {
		return err
	}
	if !done {
		return ErrLimit
	}
	return d.applyDurableApplicationThrough(through)
}

// Read only genuine durable canonical log through ORIGINAL consensus commit.
// The bounded Store page/entry hash chain remains the source oracle. Never
// compact while physical application leads actual offered RawNode bookkeeping.
func (d *Driver) applyDurableApplicationThrough(through uint64) error {
	if through < d.applied {
		return ErrInvalid
	}
	for d.applied < through {
		page, err := d.store.Entries(d.applied+1, through+1, uint64(d.config.Limits.MaxReadBytes)) // #nosec G115 -- Driver.Open validates MaxReadBytes positive and <=32 MiB before retaining config.
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return raftlog.ErrCorrupt
		}
		for _, e := range page {
			if e == nil || e.GetIndex() != d.applied+1 || e.GetIndex() > through || e.GetType() != pb.EntryNormal {
				return raftlog.ErrCorrupt
			}
			entry := Entry{Index: e.GetIndex(), Term: e.GetTerm(), Generation: d.store.ApplicationGeneration(), Data: bytes.Clone(e.GetData())}
			if err := d.applyApplicationEntry(entry); err != nil {
				return err
			}
			d.applied = e.GetIndex()
		}
	}
	return nil
}

// Every ordinary application entry, including an ahead-of-offered durable one,
// follows exactly the existing Stage/credit/install/Restore/cleanup sequence.
func (d *Driver) applyApplicationEntry(entry Entry) error {
	b, stageErr := d.applicationMachine.Stage(entry, d.store.ApplicationBudget())
	if err := errors.Join(stageErr, d.releaseApplicationCallback()); err != nil {
		return err
	}
	if err := d.bindApplicationRecoveryBatch(entry.Index, &b); err != nil {
		return err
	}
	if err := d.store.InstallApplication(entry.Index, b); err != nil {
		return err
	}
	restoreErr := d.applicationMachine.Restore(entry.Index, bytes.Clone(b.Image))
	return errors.Join(restoreErr, d.releaseApplicationCallback())
}
