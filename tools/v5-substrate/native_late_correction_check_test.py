#!/usr/bin/env python3
"""Authored half-cohort/whole-index boundary guards; never runtime evidence."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import native_late_correction_check as check
import native_late_correction_cell as cell
from comparison_spool import Policy,Spool


class NativeLateCorrectionGuards(unittest.TestCase):
    def test_half_cohort_and_pre_effect_transport_refusal(self):
        s={"version":1,"family":check.FAMILY,"facts":80,"crossPercent":50,"writers":32,"layout":"columns"}
        for transport in check.TRANSPORTS:cell.preflight(s,transport,1800,4096)
        self.assertEqual(check.scalar._scalar_updates(s),40)
        self.assertEqual([check.scalar._scalar_value(s,6,f) for f in (0,39,40,79)],[6,6,5,5])
        for change in ({"family":"immutable-columns"},{"facts":40},{"version":True}):
            with self.assertRaises(ValueError):check.shape(s|change)
        for transport in ("rtt-1ms","rtt-20ms","rtt-100ms"):
            with self.assertRaises(ValueError):cell.preflight(s,transport,1800,4096,"paired")
        phase={"Phase":"update","StartNanos":10,"StopNanos":110,"WallNanos":100,"SingleParticipant":20,"MultiParticipant":20,"ReadParticipants":[40,20],"WriteParticipants":[40,0]}
        check.common.phase(phase,40,50,1000)
        with self.assertRaises(ValueError):check.common.phase(phase,80,50,1000)
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/"absent"
            with self.assertRaises(ValueError):cell.execute(s|{"family":"snapshot-selectors"},"local",output,"missing","missing",1800,4096,Policy(4<<20,65536,4096,check.LINE))
            self.assertFalse(output.exists())

    def test_endpoint_eof_is_not_whole_index_and_duplicate_or_reset_refuses(self):
        s={"version":1,"family":check.FAMILY,"facts":80,"crossPercent":50,"writers":32,"layout":"kv"}
        end={"spec":s,"Admission":{"BudgetSeconds":180},"Correction":{"Version":1,"IndexObservations":2,"SHA256":[0]*32,"Access":check.ACCESS}}
        work=dict.fromkeys(check.WORK,0)
        owner={"eof":True,"token":"genuine-owner-in-the-real-oracle","sequence":1,"wire":"actual-old-cut"}
        observation={"points":True,"timing":{},"pointWork":work}
        point={"id":7,"source":1,"target":3,"property":{"i64":5}}
        r={"Sequence":2,"Handle":1,"Index":{"Endpoint":1,"Kind":"out","Equals":[5,6,7],"Matches":[1,0,0],"EndpointEOF":False,"Complete":False},"EOF":False,"Points":[point],"Candidates":[7],"Starts":1,"Calls":1,"Work":work|{"Records":1},"Charged":dict(zip(check.column.WORK,(16384,16<<20,0,32<<20,1024,4<<30)))}
        trace={"Stage":check.INDEX_STAGES[0],"Input":{"Action":"index","Sequence":2,"Handle":1,"IDs":None},"Reply":r,"OperationNanos":10}
        with tempfile.TemporaryDirectory() as directory:
            policy=Policy(4<<20,65536,4096,check.LINE)
            with Spool(directory,policy) as spool,patch.object(check.source,"point"):
                for change in ({"Complete":True},{"Endpoint":3},{"Matches":[1,0,1]}):
                    with self.assertRaises(ValueError):check.index_page(trace|{"Reply":r|{"Index":r["Index"]|change}},end,spool,copy.deepcopy(owner),copy.deepcopy(observation))
                check.index_page(trace,end,spool,owner,observation)
                self.assertEqual(observation["index"]["endpoint"],0)
                with self.assertRaises(ValueError):check.index_page(trace,end,spool,owner,observation)
                reset=trace|{"Input":trace["Input"]|{"Sequence":3},"Reply":r|{"Sequence":3,"Work":work}}
                with self.assertRaises(ValueError):check.index_page(reset,end,spool,owner,observation)
            root=Path(directory);(root/"indexes.jsonl").write_bytes(b"")
            with Spool(directory,policy) as spool,self.assertRaises(ValueError):check.indexes(root,end,spool)


if __name__=="__main__":unittest.main()
