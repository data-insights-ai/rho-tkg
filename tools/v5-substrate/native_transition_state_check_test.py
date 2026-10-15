#!/usr/bin/env python3
"""Compact inert diagnostic guards; no JSON fixture creates native authority."""
import copy
import importlib.util
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("state_owner_guard",Path(__file__).with_name("native_transition_state_check.py"))
S = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(S)


def sample():
    ident = {k:0 for k in S.IDENTITY}
    ident.update({k:[0]*32 for k in S.HASHES})
    ident.update(Applied=3,SourceIndex=7,ActiveGeneration=1,TargetGeneration=2,TargetBank=1,Sequence=1,Final=False,Verified=False,SourceCutID=[1]*32,ManifestID=[2]*32,ImageSHA256=[3]*32)
    f = dict(Applied=0,SourceIndex=7,Sequence=1,TargetGeneration=0,SourceCutID=[1]*32,ManifestID=[2]*32,PlanID=[0]*32,TargetBank=0,MatchSequence=True,MatchTarget=False)
    c = dict(Version=1,Action="arm",Cookie=[4]*16,Sequence=1,Kind=4,Side="after",Filter=f)
    zero_id = {k:False if k in ("Final","Verified") else [0]*32 if k in S.HASHES else 0 for k in S.IDENTITY}
    zero = {k:0 for k in S.EVENT}
    zero.update(Selection=dict(Cookie=[0]*16,Sequence=0,Identity=zero_id),Synced=False,BeforeMetadataSHA256=[0]*32,MetadataSHA256=[0]*32,BatchPayloadSHA256=[0]*32,Work=dict(Entries=0,Bytes=0,OutputBytes=0))
    e = dict(Selection=dict(Cookie=c["Cookie"],Sequence=1,Identity=ident),Kind=4,Stage=1,Synced=False,BeforeMetadataBytes=100,MetadataBytes=120,BeforeMetadataSHA256=[5]*32,MetadataSHA256=[6]*32,BatchBytes=500,BatchPayloadSHA256=[7]*32,Work=dict(Entries=3,Bytes=1<<20,OutputBytes=2048))
    arm = dict(Version=1,Profile=S.PROFILE,Cookie=c["Cookie"],Run=[8]*16,Member=3,PID=123,Sequence=1,Control=c,State="armed",DeadlineUnixNanos=1000,Attempts=0,WorkBytes=0,WorkEntries=0,ObservedOutput=0,SerializedOutput=65536,Count=0,Events=[copy.deepcopy(zero),copy.deepcopy(zero)])
    r = {**arm,"State":"observing","Attempts":1,"WorkBytes":1<<20,"WorkEntries":3,"ObservedOutput":2048,"Count":2,"Events":[e,copy.deepcopy({**e,"Stage":2,"Synced":True})]}
    return arm,r


class ActualStateGuard(unittest.TestCase):
    def test_exact_before_after_pair_and_separate_output_admission(self):
        arm,r = sample()
        S.pair(arm,r,bytes([8]*16),1000)
        for mutate in (lambda x:x["Events"][1].update(Synced=False),
                       lambda x:x["Events"][1].update(BeforeMetadataSHA256=[6]*32),
                       lambda x:x.update(ObservedOutput=0),
                       lambda x:x.update(WorkEntries=2),
                       lambda x:x.update(State="unavailable")):
            bad=copy.deepcopy(r);mutate(bad)
            with self.assertRaises(ValueError):
                S.pair(arm,bad,bytes([8]*16),1000)

    def test_native_source_and_target_are_whole_identity(self):
        arm,r=sample()
        for mutate in (lambda x:x["Events"][0]["Selection"]["Identity"].update(SourceIndex=8),
                       lambda x:x["Events"][1]["Selection"]["Identity"].update(TargetGeneration=3),
                       lambda x:x.update(PID=124),
                       lambda x:x.update(DeadlineUnixNanos=1001)):
            bad=copy.deepcopy(r);mutate(bad)
            with self.assertRaises(ValueError):
                S.pair(arm,bad,bytes([8]*16),1000)

    def test_empty_ignored_or_foreign_filter_refuses(self):
        arm,r=sample()
        c,ident=arm["Control"],r["Events"][0]["Selection"]["Identity"]
        S.selector(c,ident)
        for update in (dict(SourceCutID=[0]*32),dict(SourceIndex=8),dict(MatchSequence=False),dict(TargetBank=1)):
            bad=copy.deepcopy(c);bad["Filter"].update(update)
            with self.assertRaises(ValueError):
                S.selector(bad,ident)
        # Initialize is deliberately outside this import selector, including its
        # genuine possible Applied==0; no fabricated nonzero genesis admission.
        bad=copy.deepcopy(c);bad["Kind"]=11
        with self.assertRaises(ValueError):
            S.selector(bad,ident)


if __name__ == "__main__":
    unittest.main()
