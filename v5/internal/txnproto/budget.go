package txnproto

import (
	"encoding/json"
	"math"
)

// recoveryBudgets preflights EVERY required future envelope, including the two
// copies of Tx in second-participant prepare. Index/floor/round widths are the
// maximum uint64 width. Digests are already immutable and therefore exact.
// NO reasons use the longest reason this reducer can generate. This reserves
// future command framing, not only retained journal capacity.
func recoveryBudgets(t Tx, limit int) (coordinatorBytes, intentBytes int, err error) {
	reg := View{Graph: t.Graph, Topology: t.Topology, Group: t.Coordinator, Index: math.MaxUint64, Kind: Registration, Tx: &t}
	cp, _ := part(t, t.Coordinator)
	reg.Epoch = cp.Epoch
	shapes := []command{}
	yes := make([]Vote, len(t.Participants))
	no := make([]Vote, len(t.Participants))
	for j, p := range t.Participants {
		yes[j] = Vote{Group: p.Group, Epoch: p.Epoch, Digest: digest(t), Effects: digest(p), Floor: math.MaxUint64, Yes: true}
		no[j] = yes[j]
		no[j].Yes = false
		no[j].Reason = ErrRetry.Error()
		v := reg
		v.Group = p.Group
		v.Epoch = p.Epoch
		v.Kind = Prepared
		v.Vote = &yes[j]
		prepare := command{Version: 1, Kind: "prepare", Remote: &reg}
		if j > 0 {
			prior := reg
			prior.Group = t.Participants[j-1].Group
			prior.Epoch = t.Participants[j-1].Epoch
			prior.Kind = Prepared
			prior.Vote = &yes[j-1]
			prepare.Prior = &prior
		}
		shapes = append(shapes, prepare)
		voteYes := command{Version: 1, Kind: "vote", Remote: &v}
		vn := v
		vn.Vote = &no[j]
		voteNo := command{Version: 1, Kind: "vote", Remote: &vn}
		by, _ := json.Marshal(voteYes)
		bn, _ := json.Marshal(voteNo)
		coordinatorBytes += max(len(by), len(bn)) + 32
		shapes = append(shapes, voteYes, voteNo)
	}
	decision := command{Version: 1, Kind: "decide", Remote: &reg, Commit: false}
	b, _ := json.Marshal(decision)
	coordinatorBytes += len(b) + 32
	shapes = append(shapes, decision)
	commitView := reg
	commitView.Kind = Decided
	commitView.Decision = &Decision{Commit: true, Round: math.MaxUint64, Digest: digest(t), Votes: yes}
	abortView := reg
	abortView.Kind = Decided
	abortView.Decision = &Decision{Digest: digest(t), Votes: no}
	for _, v := range []View{commitView, abortView} {
		resolve := command{Version: 1, Kind: "resolve", Remote: &v}
		b, _ := json.Marshal(resolve)
		intentBytes = max(intentBytes, len(b)+32)
		shapes = append(shapes, resolve)
	}
	for _, c := range shapes {
		b, _ := json.Marshal(c)
		if len(b) > limit {
			return 0, 0, ErrLimit
		}
	}
	return coordinatorBytes, intentBytes, nil
}
