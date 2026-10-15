#!/usr/bin/env python3
"""Authored direct guards only; not an executed engine or matrix result."""
import copy
import unittest
import fdb_create_update_check as check


class CreateUpdateGuards(unittest.TestCase):
    def test_business_locality_excludes_metadata_and_empty_participants(self):
        spec={"version":1,"family":check.FAMILY,"facts":40,"crossPercent":50,"writers":8,"layout":"kv"}
        for fact in (0,1):
            command,_,effects,reads=check.source.codec.fdb_contract(spec,4+fact)
            tx={"GraphChange":command,"Participants":[{"Group":g,"Reads":[{"Key":k,"Version":1} for k in reads[g]],"Effects":effects[g]} for g in sorted(effects)]}
            observed=check.footprint(tx,spec)
            self.assertEqual(observed,([1,int(fact==1)],[1,int(fact==1)]))
            bad=copy.deepcopy(tx)
            if fact==0: bad["Participants"].append({"Group":1,"Reads":[],"Effects":[]})
            else: bad["Participants"].pop()
            with self.assertRaises(ValueError):check.footprint(bad,spec)
        self.assertEqual(check.row_semantics(0),"strict-partition-local-create-update")
        self.assertEqual(check.row_semantics(100),"mixed-or-cross-partition-create-update")

    def test_phase_wall_and_actual_ratio_cannot_be_reported_nominally(self):
        p={"Phase":"create","Attempts":42,"Aborted":2,"Committed":40,"SingleParticipant":20,"MultiParticipant":20,"ReadParticipants":[40,20],"WriteParticipants":[40,20],"StartedUnixNanos":1,"FinishedUnixNanos":101,"WallNanos":100}
        check.phase(p,40,50)
        for change in ({"MultiParticipant":0},{"ReadParticipants":[40,0]},{"WallNanos":0},{"Attempts":40},{"StartedUnixNanos":0}):
            with self.assertRaises(ValueError):check.phase(p|change,40,50)


if __name__=="__main__":unittest.main()
