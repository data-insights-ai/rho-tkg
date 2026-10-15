#!/usr/bin/env python3
"""Authored compact offline guards; no SDK/process/matrix acceptance evidence."""
import base64
import copy
import hashlib
import tempfile
import unittest
import fdb_transfer_check as check
from comparison_spool import Policy, Spool


def spec():
    return {"version":2,"family":check.FAMILY,"facts":40,"crossPercent":50,"writers":32,"layout":"columns"}


def grant():
    return {"MaxRecords":10000,"MaxControlBytes":1<<30,"MaxGrants":8,"MaxBlock":32,
            "Census":{"PageRows":32,"PageBytes":4<<20,"HeadRows":128,"HeadBytes":48<<10,"MaxRows":10000,"MaxBytes":1<<30,"MaxCalls":1024,"MaxWorkRows":1<<40,"MaxWorkBytes":1<<58}}


def record(key,body):
    return {"Key":base64.b64encode(key).decode(),"Value":base64.b64encode(body).decode()}


def attempt(end,ordinal,commit=True,number=1,previous=""):
    p,c=end["Spec"],end["Config"]
    phase,fact,_,_=check.codec.fixture(p,ordinal)
    op={"Dataset":list(hashlib.sha256(b"\x01"+check.u64(p["facts"])+check.u64(p["crossPercent"])+b"graph-v2").digest()),"Phase":phase,"Fact":fact,"Ordinal":ordinal}
    command,lifecycle,effects,reads=check.codec.fdb_contract(p,ordinal)
    name=f"transfer-{phase}-{fact}-c0-a{number}"
    owner=1 if phase=="setup" and fact==2 else 0
    tx={"Graph":c["Graph"],"Topology":1,"ID":name,"Request":"request/"+name,"Coordinator":owner,"Dependency":0,
        "Participants":[{"Group":g,"Epoch":c["Epochs"][g],"Generation":0,"Reads":[{"Key":k,"Version":0} for k in reads[g]],"Effects":effects[g]} for g in sorted(effects)],"GraphChange":command,"Lifecycle":lifecycle}
    body=check.go_json(tx);digest=list(hashlib.sha256(body).digest())
    intent={"Body":base64.b64encode(body).decode(),"Digest":digest,"Routes":[{"Owner":"A","Epoch":1},{"Owner":"B","Epoch":1}],"Allocated":[{"ID":1,"Offset":0,"Grant":"home-00","Session":"session-home-00"}]}
    o={"Commit":commit,"Round":2 if commit else 0,"Digest":digest,"ID":name,"Request":tx["Request"],"Reason":"" if commit else "owner-epoch","Coordinator":owner,"Sequence":1 if commit else 0}
    group={"Tx":tx,"Outcome":o,"Writes":[]}
    proof={"Pair":[record(check.key(c,owner,k,i),check.go_json(o)) for k,i in (("id",name),("request",tx["Request"]))],"Original":record(check.prefix(c,owner)+b"changes/0000000000000001/"+tx["Request"].encode().hex().encode(),check.go_json(group)) if commit else {"Key":None,"Value":None},"Group":group if commit else {},"Outcome":o}
    return {"Operation":op,"ClientID":0,"AttemptOrdinal":number,"PreviousRequest":previous,"Intent":intent,"Prepare":{"Intent":intent,"Error":""},"Submissions":[{"Error":"","Proof":proof,"Outcome":o}],"Proof":proof,"Classification":"committed" if commit else "aborted","Nanos":1}


class TransferGuards(unittest.TestCase):
    def test_actual_stable_source_footprint_changes_authority_without_fake_colocation(self):
        p=spec();before=[{"Owner":"A","Epoch":1},{"Owner":"B","Epoch":1}]
        after=[{"Owner":"B","Epoch":2},{"Owner":"B","Epoch":1}]
        end={"Spec":p,"Config":{"Graph":"graph-owned-v3-columns-"+"01"*16,"Topology":1,"Epochs":[7,11]},
             "Move":{"Before":{"Routes":before}},"Handoff":{"Seal":{"After":{"Routes":after}},"Original":{"Frozen":{"Cut":{"Round":50}}}}}
        a=attempt(end,4+p["facts"]+1)
        self.assertEqual(check.actual_footprint(a,end),("before-handoff",(0,1),(0,1),("A","B")))
        a["Intent"]["Routes"]=after;a["Proof"]["Outcome"]["Round"]=51
        self.assertEqual(check.actual_footprint(a,end),("after-handoff",(0,1),(0,1),("B",)))
        a["Proof"]["Outcome"]["Round"]=50
        with self.assertRaises(ValueError):check.actual_footprint(a,end)
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory,Policy(1<<20,64<<10,4096,check.LINE)) as spool:
                for role,call in ((b"A",7),(b"B",2)):
                    spool.insert(b"submit-issued",role+check.u64(call),check.go_json({"ID":call}))
                names=check.paired_artifacts(end,spool)
                self.assertIn("fdb-transfer-lost-A-0000000000000007.json",names)
                self.assertIn("fdb-transfer-lost-B-0000000000000002.json",names)
                self.assertIn("fdb-transfer-after-gc-history.jsonl",names)

    def test_shared_answer_requires_actual_old_current_postings_and_acknowledged_predecessor(self):
        p=spec();end={"Spec":p,"Config":{"Graph":"graph-owned-v3-columns-"+"01"*16}}
        def fill(spool,bad_predecessor=False):
            for fact in range(p["facts"]):
                before=None
                for updated in (False,True):
                    ordinal=4+fact+(p["facts"] if updated else 0)
                    command,lifecycle,effects,_=check.codec.fdb_contract(p,ordinal)
                    value={"Value":6 if updated else 5,"Version":2 if updated else 1,"Round":3 if updated else 2,"Deleted":False,"TxID":f"tx-{ordinal}"}
                    original={"Tx":{"GraphChange":command,"Lifecycle":lifecycle},"Outcome":{"Coordinator":0,"Sequence":ordinal,"Request":f"request/{ordinal}"},"Writes":[]}
                    for g,items in effects.items():
                        for effect in items:
                            native=check.key(end["Config"],g,"current",effect["Key"])
                            prior=before if updated else None
                            if bad_predecessor and updated and fact==0:prior=before|{"Value":6}
                            original["Writes"].append({"Group":g,"Key":effect["Key"],"Before":prior,"After":value})
                            spool.insert(b"fdb-current" if updated else b"fdb-old",native,check.go_json(value))
                            spool.insert(b"snapshot-seen-"+(b"current-before-gc" if updated else b"held-old-after-target-restart"),native,check.go_json(value))
                            if updated:spool.insert(b"feed-replica",native,check.go_json(value))
                    spool.insert(b"fdb-operation",check.u64(ordinal),check.go_json(original))
                    if updated:
                        o=original["Outcome"];native=check.prefix(end["Config"],0)+f'changes/{ordinal:016x}/'.encode()+o["Request"].encode().hex().encode()
                        spool.insert(b"feed-consumed",native,check.go_json(original))
                        spool.insert(b"transfer/update-authority",check.u64(fact),b'{"actual-native-ack":"already-joined"}')
                    else:before=value
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory,Policy(8<<20,64<<10,4096,check.LINE)) as spool:
                fill(spool);digest,count=check.logical_answers(end,spool)
                self.assertEqual(count,2*p["facts"]);self.assertEqual(len(digest),64)
                self.assertEqual(check.transfer_answers(p,spool),(digest,count))
                update=check.decoded(spool.lookup(b"transfer/updates",check.u64(0)))
                self.assertEqual((update["before"][7],update["after"][7]),(5,6))
            with Spool(directory,Policy(8<<20,64<<10,4096,check.LINE)) as spool:
                fill(spool,True)
                with self.assertRaises(ValueError):check.logical_answers(end,spool)

    def test_finite_native_grant_and_closed_mutable_classes(self):
        value=grant();p=spec()
        self.assertEqual(check.validate_grant(value,p),value)
        for change in ({"MaxGrants":4},{"MaxBlock":31},{"MaxRecords":100},{"Census":value["Census"]|{"HeadBytes":49<<10}},{"Census":value["Census"]|{"MaxWorkBytes":1<<30}},{"Census":value["Census"]|{"MaxCalls":8}}):
            with self.assertRaises(ValueError):check.validate_grant(value|change,p)
        c={"Graph":"graph-owned-v3-kv-"+"01"*16}
        self.assertTrue(check.census_class(c,check.key(c,0,"owner-catalog-v1","allocation-slot/block")))
        self.assertFalse(check.census_class(c,check.key(c,0,"owner-catalog-v1","allocation-issued/0000000000000001")))
        for native in (check.key(c,0,"owner-catalog-v1","unbound-mutable/new"),check.key(c,1,"source-retention-v1","state"),b"foreign/key"):
            with self.assertRaises(ValueError):check.census_class(c,native)
        move={"Request":"storage","Before":{"Version":2,"Routes":[{"Owner":"B","Epoch":2},{"Owner":"B","Epoch":1}],"Moving":""},"Owner":{"Request":"held"}}
        before=record(check.key(c,0,"owner-catalog-v1","state/current"),check.go_json(move["Before"]|{"Moving":"storage"}))
        after=record(check.key(c,0,"owner-catalog-v1","state/current"),check.go_json(move["Before"]|{"Version":3}))
        check.canceled_head(before,after,move,{"Config":c})
        with self.assertRaises(ValueError):check.canceled_head(before,before,move,{"Config":c})
        metadata=check.key(c,0,"meta","state")
        with self.assertRaises(ValueError):check.canceled_head(record(metadata,b'{"Generation":1}'),record(metadata,b'{"Generation":2}'),move,{"Config":c})

    def test_paired_original_body_and_authoritative_replan_linkage(self):
        p=spec();end={"Spec":p,"Config":{"Graph":"graph-owned-v3-columns-"+"01"*16,"Topology":1,"Epochs":[7,11]}}
        policy=Policy(8<<20,64<<10,4096,check.LINE)
        with tempfile.TemporaryDirectory() as directory:
            abort=attempt(end,4+p["facts"]+1,False)
            for mutate in (lambda a:a["Proof"]["Outcome"].update(Reason="timeout"),lambda a:a["Intent"].update(Digest=[0]*32),lambda a:a["Proof"]["Pair"][1].update(Value=base64.b64encode(b"{}").decode())):
                bad=copy.deepcopy(abort);mutate(bad)
                with Spool(directory,policy) as spool:
                    with self.assertRaises(ValueError):check.attempt(bad,end,spool)
            with Spool(directory,policy) as spool:
                check.attempt(abort,end,spool)
                fresh=attempt(end,4+p["facts"]+1,True,2,abort["Proof"]["Outcome"]["Request"])
                check.attempt(fresh,end,spool)
                with self.assertRaises(ValueError):check.attempt(fresh,end,spool)
            with Spool(directory,policy) as spool:
                wrong=attempt(end,4+p["facts"]+1,True,2,"request/unproved-abort")
                with self.assertRaises(ValueError):check.attempt(wrong,end,spool)


if __name__=="__main__":unittest.main()
