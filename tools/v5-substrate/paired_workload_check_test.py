"""Authored bounded source guards; mock pipeline units are not real pair evidence."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import paired_workload_check as check
import paired_workload_cell as cell
from comparison_spool import Policy,Spool,u64,SpoolError


def spec(family="snapshot-selectors",n=80,version=None):
    return {"version":(2 if family in ("snapshot-to-feed","snapshot-selectors") else 1) if version is None else version,"family":family,"facts":n,"crossPercent":50,"writers":32,"layout":"columns"}


def grants(seconds):
    count=(seconds*1000+59999)//60000+2
    return {"QueuePackets":128,"SampleMillis":60000,"MaxSamples":count,"MaxArtifactBytes":(16<<20)+count*(2<<20)}


def put(spool,ns,key,value):
    body=check.common.native.encoded(value);old=spool.find(ns,key)
    if old is None:spool.insert(ns,key,body)
    else:spool.replace(ns,key,old,body)


class OwnedPairSourceGuards(unittest.TestCase):
    def test_actual_family_registry_full_dimensions_and_declared_delays(self):
        for family in check.FAMILIES:
            for n in (10000,100000,1000000):
                for writers in (1,8,32):
                    for cross in (0,10,50,100):
                        for layout in ("kv","columns"):
                            s=spec(family,n)|{"writers":writers,"crossPercent":cross,"layout":layout}
                            check.registry(s);self.assertEqual(check.fdb_spec(s),s|{"version":2})
            for name,delay in check.TRANSPORTS.items():
                s=spec(family)
                self.assertEqual(cell.preflight(s,name,31536000,4096,Policy(1<<42,65536,4096,512<<10),grants(31536000))["targetRTTMillis"],delay)
        for s in (spec("transfer-retained-pins"),spec("snapshot-to-feed",version=1),spec("snapshot-selectors",version=1),spec()|{"writers":True}):
            with self.assertRaises(ValueError):check.registry(s)
        for change in ({"targetRTTMillis":True},{"seed":True},{"jitterMillis":True},{"kind":"label-only"}):
            with self.assertRaises(ValueError):check.validate_transport(check.declared_transport("rtt-1ms")|change)
        with self.assertRaises(ValueError):cell.preflight(spec(),"rtt-100ms",1,4096,Policy(1<<20,65536,4096,512<<10),grants(1))
        with tempfile.TemporaryDirectory() as path:
            output=Path(path)/"absent"
            with patch.object(cell.common,"digest",side_effect=AssertionError("before-effect admission was bypassed")):
                with self.assertRaises(ValueError):cell.execute(spec(),"rtt-100ms",output,None,None,1,4096,Policy(1<<20,65536,4096,512<<10),profile="small",owned_options=grants(1))
            self.assertFalse(output.exists())

    def test_original_transport_join_is_not_nominal_rtt_or_renewed_deadline(self):
        s=spec();selected=check.declared_transport("rtt-20ms");run="12"*16
        p={"Spec":check.fdb_spec(s),"Run":list(bytes.fromhex(run)),"Transport":selected,"BudgetSeconds":60,"AdmittedUnixNanos":100,"DeadlineUnixNanos":60000000100}
        proof={"policySHA256":hashlib.sha256(json.dumps(p,separators=(",",":"),ensure_ascii=False).encode()).hexdigest(),"configuredAddedOneWayMicros":10000,"drops":0}
        n={"transport":selected,"run":run};f={"transport":proof};end={"Transport":{"Policy":p}}
        self.assertEqual(check.transport_join(s,run,60,selected,n,f,end)["foundationdb"],proof)
        for name,value in (("Spec",p["Spec"]|{"writers":8}),("BudgetSeconds",61),("DeadlineUnixNanos",60000000101),("Run",[13]*16),("Transport",check.declared_transport("rtt-100ms"))):
            changed=copy.deepcopy(end);changed["Transport"]["Policy"][name]=value
            with self.assertRaises(ValueError):check.transport_join(s,run,60,selected,n,f,changed)
        for key,value in (("drops",1),("policySHA256","0"*64),("configuredAddedOneWayMicros",0)):
            with self.assertRaises(ValueError):check.transport_join(s,run,60,selected,n,{"transport":proof|{key:value}},end)

    def test_scalar_join_requires_every_actual_typed_life_and_presence_row(self):
        s=spec(n=40);n,_,f=check.registry(s)
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory,Policy(4<<20,65536,4096,512<<10)) as spool:
                left,right=check.common.NamespacedSpool(spool,b"n/"),check.common.NamespacedSpool(spool,b"f/")
                for updated in (False,True):
                    for fact in range(s["facts"]):
                        ordinal=4+fact+(s["facts"] if updated else 0);owner=7+2*fact;value=6 if updated else 5;target=3
                        row={"Owner":owner,"Life":owner+1,"Source":1,"SourceLife":2,"Target":target,"TargetLife":target+1,"Before":{"Present":updated,"Value":5 if updated else 0,"Provenance":4+fact if updated else 0},"After":{"Present":True,"Null":False,"Handle":value,"Value":value,"Provenance":ordinal}}
                        put(left,b"row",u64(ordinal)+u64(owner)+b"\3",row)
                        put(left,b"row",u64(ordinal)+u64(owner)+b"",{"Before":{"Present":updated,"Handle":owner+1 if updated else 0,"Provenance":4+fact if updated else 0},"After":{"Present":True,"Handle":owner+1,"Provenance":ordinal}})
                        tx={"GraphChange":{"Edge":{"ID":owner,"Source":{"Node":1,"Life":2},"Target":{"Node":target,"Life":target+1}}},"Lifecycle":{"EdgeLife":owner+1,"Definition":{"Kind":"i64","ID":value,"I64":value},"Provenance":ordinal}}
                        after={"Value":value,"Deleted":False};write={"Group":0,"Key":"fixture/"+str(fact),"After":after}
                        if updated:write["Before"]={"Value":5,"Deleted":False}
                        put(right,b"fdb-operation",u64(ordinal),{"Tx":tx,"Writes":[write]})
                        key=check.scalar_raw.key({"Graph":"fixture-graph"},0,"current",write["Key"])
                        put(right,b"fdb-current" if updated else b"fdb-old",key,after)
                        put(right,b"snapshot-seen-"+(b"fresh-current" if updated else b"old-fresh"),key,after)
                answer,count=check.scalar_answers(s,"fixture-graph",n,f,left,right);self.assertEqual(count,80);self.assertEqual(len(answer),64)
                bad=n.decoded(left.lookup(b"row",u64(4)+u64(7)+b"\3"));bad["TargetLife"]+=1
                put(left,b"row",u64(4)+u64(7)+b"\3",bad)
                with self.assertRaises(ValueError):check.scalar_answers(s,"fixture-graph",n,f,left,right)
                bad["TargetLife"]-=1;put(left,b"row",u64(4)+u64(7)+b"\3",bad)
                original=f.decoded(right.lookup(b"fdb-operation",u64(44)));original["Writes"][0]["Before"]["Value"]=6
                put(right,b"fdb-operation",u64(44),original)
                with self.assertRaises(ValueError):check.scalar_answers(s,"fixture-graph",n,f,left,right)


    def test_edge_independent_winners_keep_actual_old_and_new_endpoint_lives(self):
        s=spec("cross-edge-endpoint-uniqueness",n=20)|{"writers":1};n,_,f=check.registry(s)
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory,Policy(4<<20,65536,4096,512<<10)) as spool:
                left,right=check.common.NamespacedSpool(spool,b"n/"),check.common.NamespacedSpool(spool,b"f/")
                for fact in range(s["facts"]):
                    left.insert(b"winner",u64(fact),u64(0));right.insert(b"old-winner",u64(fact),u64(1))
                    for updated in (False,True):
                        ns,fs=(b"state",b"current") if updated else (b"old-state",b"old");owner=7+4*fact+2*int(updated)
                        e=f.edge(s,fact,0 if updated else 1,updated)
                        row={"Source":1,"SourceLife":2,"Target":e["Target"]["Node"],"TargetLife":e["Target"]["Life"],"After":{"Present":True,"Null":False,"Value":fact+5}}
                        put(left,ns,u64(owner)+u64(owner+1)+b"\3p",row);put(left,ns,u64(owner)+u64(0)+b"\1",{"After":{"Present":True,"Handle":owner+1}})
                        put(right,fs,u64(0)+f.edge_key("edge",e).encode(),{"Deleted":False,"Value":fact+5})
                self.assertEqual(check.edge_answers(s,n,f,left,right)[1],40)
                bad=n.decoded(left.lookup(b"state",u64(9)+u64(10)+b"\3p"));bad["TargetLife"]-=1
                put(left,b"state",u64(9)+u64(10)+b"\3p",bad)
                with self.assertRaises(ValueError):check.edge_answers(s,n,f,left,right)

    def test_native_inventory_uses_actual_family_wrapper(self):
        for family in ("snapshot-selectors","snapshot-to-feed","immutable-columns","late-correction-old-index"):
            s=spec(family);_,wrapper,_=check.registry(s)
            base=wrapper.feed_cell if family=="snapshot-to-feed" else wrapper.selectors_cell if family=="snapshot-selectors" else wrapper.common
            with patch.object(base,"hash_artifacts",return_value={"actual":"hash"}) as actual:
                self.assertEqual(check.native_inventory(s,wrapper,"owned"),{"actual":"hash"})
                expected={"family":family} if family in check.SCALARS else {}
                actual.assert_called_once_with("owned","columns",**expected)


if __name__=="__main__":unittest.main()
