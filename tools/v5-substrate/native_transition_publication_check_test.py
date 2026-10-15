#!/usr/bin/env python3
"""Inert publication codec/member guards, never successful native evidence."""
import copy
import importlib.util
import unittest
from pathlib import Path


def module(name,file):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(file))
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result


P=module("publication_guard","native_transition_publication_check.py")
G=module("state_inert_guard","native_transition_state_check_test.py")


class PublicationGuard(unittest.TestCase):
    def test_compaction_boundary_does_not_substitute_current_or_wrap(self):
        before=dict(FirstIndex=2,LastIndex=7,SnapshotOffers=0)
        after=dict(FirstIndex=8,LastIndex=7,SnapshotOffers=0)
        P.boundary(before,before,7,"before")
        P.boundary(before,after,7,"after")
        with self.assertRaises(ValueError):
            P.boundary(before,before,7,"after")
        with self.assertRaises(ValueError):
            P.boundary(before,after,7,"before")
        with self.assertRaises(ValueError):
            P.boundary(before,after,(1<<64)-1,"after")
        with self.assertRaises(ValueError):
            P.bounds({**before,"SnapshotOffers":3})

    def test_explicit_actual_member_preserves_import_default(self):
        arm,r=G.sample()
        for value in (arm,r):
            value["Member"]=1
            value["Control"]["Kind"]=2
            value["Control"]["Filter"].update(Applied=7,SourceIndex=7,Sequence=0,SourceCutID=[0]*32,ManifestID=[0]*32,MatchSequence=False)
        for e in r["Events"]:
            e["Kind"]=2
            e["Selection"]["Identity"].update(Applied=7,Sequence=0)
        P.S.pair(arm,r,bytes([8]*16),1000,member=1)
        with self.assertRaises(ValueError):
            P.S.pair(arm,r,bytes([8]*16),1000)
        changed=copy.deepcopy(r);changed["Events"][1]["Selection"]["Identity"]["Applied"]=8
        with self.assertRaises(ValueError):
            P.S.pair(arm,changed,bytes([8]*16),1000,member=1)
        changed=copy.deepcopy(r);changed["DeadlineUnixNanos"]=1001
        with self.assertRaises(ValueError):
            P.S.pair(arm,changed,bytes([8]*16),1000,member=1)

    def test_default_actual_import_member_stays_three(self):
        arm,r=G.sample()
        P.S.pair(arm,r,bytes([8]*16),1000)
        with self.assertRaises(ValueError):
            P.S.pair(arm,r,bytes([8]*16),1000,member=1)


if __name__=="__main__":
    unittest.main()
