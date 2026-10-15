"""Authored inert codec/admission/oracle guards; no engine or SDK evidence."""
import base64
import copy
import hashlib
import unittest

import fdb_feed_retained_check as check


def spec():return {"version":3,"family":"snapshot-to-feed","facts":40,"crossPercent":50,"writers":32,"layout":"columns"}
def grant():return {"MaxRecords":1<<30,"MaxControlBytes":1<<45,"MaxGrants":64,"MaxBlock":1<<20,"Census":{"PageRows":128,"PageBytes":4<<20,"HeadRows":512,"HeadBytes":48<<10,"MaxRows":1<<31,"MaxBytes":1<<45,"MaxCalls":1<<28,"MaxWorkRows":1<<48,"MaxWorkBytes":1<<62}}
def b64(value):return base64.b64encode(value).decode()


class FixedSpool:
    """Only a fixed direct-guard fixture; never a runtime verification backend."""
    def __init__(self,rows):self.rows=rows
    def lookup(self,ns,key):
        if (ns,key) not in self.rows:raise ValueError("missing actual fixture original")
        return self.rows[ns,key]
    def insert(self,ns,key,value):
        if (ns,key) in self.rows:raise ValueError("duplicate fixture identity")
        self.rows[ns,key]=value


class RetainedFeedGuards(unittest.TestCase):
    def test_exact_dimensions_real_writer_census_and_finite_grants(self):
        for n in (20,40,10000,100000,1000000):
            for w in (1,8,32):
                if n<=w:continue
                s=spec()|{"facts":n,"writers":w};self.assertEqual(check.shape(s),s)
                self.assertEqual(check.validate_grant(grant(),s),grant())
                counters=[2*((n-1-i)//w+1) if i<w else 0 for i in range(32)]
                check.caller_census(s,counters)
                wrong=counters.copy();wrong[0]=True
                with self.assertRaises(ValueError):check.caller_census(s,wrong)
                if w>1:
                    wrong=[0]*32;wrong[0]=2*n
                    with self.assertRaises(ValueError):check.caller_census(s,wrong)
        for change in ({"version":True},{"facts":True},{"writers":True},{"crossPercent":False},{"version":2},{"facts":20}):
            with self.assertRaises(ValueError):check.shape(spec()|change)
        for change in ({"MaxRecords":1},{"MaxBlock":9},{"MaxGrants":True},{"MaxControlBytes":1<<51}):
            with self.assertRaises(ValueError):check.validate_grant(grant()|change,spec())

    def test_correct_scalar_with_wrong_historical_revision_or_presence_refuses(self):
        c={"Graph":"guard"};logical="edge/guard";nk=check.key(c,0,"current",logical)
        old={"Value":5,"Version":1,"Round":7,"Deleted":False,"TxID":"create"}
        current={"Value":6,"Version":2,"Round":9,"Deleted":False,"TxID":"update"}
        o={"Commit":True,"Round":7,"Digest":[1]*32,"ID":"create","Request":"request/create","Reason":"","Coordinator":0,"Sequence":1}
        group={"Tx":{},"Outcome":o,"Writes":[{"Group":0,"Key":logical,"After":old}]};body=check.go_json(group);ck=check.prefix(c,0)+b"changes/0000000000000001/"+b"request/create".hex().encode()
        raw={"Group":0,"Key":b64(ck),"Value":b64(body),"IDKey":b64(check.key(c,0,"id",o["ID"])),"IDValue":b64(check.go_json(o)),"RequestKey":b64(check.key(c,0,"request",o["Request"])),"RequestValue":b64(check.go_json(o))}
        hk=check.history_key(c,0,logical,old);row={"Group":0,"Key":logical,"Value":old,"Current":{"Key":b64(nk),"Value":b64(check.go_json(old))},"History":{"Key":b64(hk),"Value":b64(check.go_json(old))},"Chain":[raw]}
        def fixture():return FixedSpool({(check.ns("old"),nk):check.go_json(old),(check.ns("native/before-gc"),nk):check.go_json(old),(check.ns("history"),hk):check.go_json(old),(check.ns("cdc"),b"\0"+check.u64(1)):body})
        check.snapshot_row(row,{"Round":7,"Groups":[1,0]},{"Config":c},fixture(),"snapshot-after-update")
        for changed in (old|{"Version":current["Version"],"Round":current["Round"]},old|{"Deleted":True},old|{"TxID":"update"}):
            bad=copy.deepcopy(row);bad["Value"]=changed
            with self.assertRaises(ValueError):check.snapshot_row(bad,{"Round":7,"Groups":[1,0]},{"Config":c},fixture(),"snapshot-after-update")

    def test_unread_page_must_follow_real_collection_and_post_gc_eof(self):
        e={"Spec":{"facts":40},"BeforeGC":{"Round":7},"Transition":{"Key":"phase","Value":"actual"},"Retired":{"Deleted":[3,0],"DeletedBytes":[300,0]}}
        events=[{"Type":"feed-page","Delivery":{"Page":{"Boundary":e["BeforeGC"],"EOF":False,"Rows":[1]}}},
                {"Type":"native-eof","Stage":"before-gc","Group":1},
                {"Type":"retained-only","Record":e["Transition"]},
                {"Type":"source-collect","Progress":{"Phase":2,"WorkRows":3,"WorkBytes":1024,"Deleted":[3,0],"DeletedBytes":[300,0],"Done":True,"Seal":e["Retired"]}},
                {"Type":"native-eof","Stage":"after-gc","Group":1},
                {"Type":"prepared","Operation":{"Phase":"update","Fact":20}},
                {"Type":"native-eof","Stage":"checkpoint","Group":1},
                {"Type":"child-killed","Result":{"Delivery":{"Page":{"Rows":[1]}}}},
                {"Type":"child-replaced"}]
        self.assertEqual(check.chronology(iter(events),e)["calls"],1)
        for first,last in ((2,3),(3,4),(4,5),(6,7),(7,8)):
            changed=copy.deepcopy(events);changed[first],changed[last]=changed[last],changed[first]
            with self.assertRaises(ValueError):check.chronology(iter(changed),e)
        for change in ({"Deleted":[0,0]},{"Done":False},{"WorkRows":True}):
            changed=copy.deepcopy(events);changed[3]["Progress"].update(change)
            with self.assertRaises(ValueError):check.chronology(iter(changed),e)

    def test_interval_chunk_binds_initial_vector_not_just_total_rows(self):
        # A finite header mutation guard, not a manufactured valid engine row.
        generation={"Version":2,"ID":"interval","Binding":[1]*32,"Initial":{"Groups":[7,3]},"Cut":{"Groups":[8,3]}}
        body=b"RHOCLM03"+bytes([1])*32+hashlib.sha256(check.go_json(generation)).digest()+check.u64(0)+bytes(32)+b"\0\1"+bytes(98)
        changed=copy.deepcopy(generation);changed["Initial"]["Groups"][0]-=1
        with self.assertRaises(ValueError):list(check.chunk_rows(body,changed,bytes([1])*32,0,bytes(32),{"ChunkBytes":65536,"ChunkRows":16},{},FixedSpool({})))
        with self.assertRaises(ValueError):list(check.chunk_rows(b"RHOCLM01"+body[8:],generation,bytes([1])*32,0,bytes(32),{"ChunkBytes":65536,"ChunkRows":16},{},FixedSpool({})))


if __name__=="__main__":unittest.main()
