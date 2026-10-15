#!/usr/bin/env python3
"""Authored admission/ownership guards only; these are not Linux/FDB evidence."""
import copy
import base64
import hashlib
import tempfile
from pathlib import Path
import unittest
from unittest import mock
import owned_transport as owned
import cost_monitor


def request():
    return {"Version":2,"Spec":{"version":2,"family":"snapshot-to-feed","facts":1_000_000,"crossPercent":50,"writers":1,"layout":"kv"},
            "Run":"01"*16,"Transport":{"kind":"injected-rtt","targetRTTMillis":100,"jitterMillis":0,"seed":20261009},
            "BudgetSeconds":31_536_000,"QueuePackets":1024,"SampleMillis":60000,"MaxSamples":525602,"MaxArtifactBytes":1<<44,"ToolsDirectory":"@pinned-prebuild"}


class OwnedTransportGuards(unittest.TestCase):
    def test_checked_finite_budget_and_exact_directed_ownership(self):
        value=request()
        self.assertEqual(owned.validate_input(value),value)
        self.assertEqual(owned.causal_minimum(value["Spec"],value["Transport"]),212500300)
        changes=[{"BudgetSeconds":1800},{"BudgetSeconds":31_536_001},{"MaxSamples":525601},{"MaxArtifactBytes":(16<<20)+525602*(2<<20)-1},{"QueuePackets":0},{"Spec":{**value["Spec"],"family":"missing-family"}},{"Transport":{**value["Transport"],"jitterMillis":1}}]
        for change in changes:
            with self.assertRaises(ValueError): owned.validate_input(value|change)
        peers=[{"Role":r,"Container":f"{i+1:064x}","IP":f"10.0.0.{i+10}","NetNS":f"net:[{i+100}]","Network":"aa"*32} for i,r in enumerate(owned.ROLES)]
        self.assertEqual(len(owned.directed(peers)),20)
        commands=list(owned.shaping_commands(peers,4,1024,50000))
        self.assertEqual(sum("flower" in c for c in commands),4)
        self.assertTrue(all("replace" not in c and "eth1" not in c for c in commands))
        self.assertEqual({c[c.index("src_ip")+1] for c in commands if "flower" in c},{p["IP"]+"/32" for p in peers[:4]})
        # JSON shape follows pinned iproute2 source. This is a direct validator
        # guard fixture, never an executed kernel/probe result.
        rows=[]
        for target,peer in enumerate(peers):
            q=[{"kind":"noqueue","dev":dev} for dev in ("lo","eth0")]+[{"kind":"clsact","dev":"eth0"}]
            filters=[];links=[{"ifname":"lo"},{"ifname":"eth0","stats64":{"rx":{"dropped":0},"tx":{"dropped":0}}}]
            for source in range(5):
                if source==target: continue
                dev=f"rhoi{source}"
                q.append({"kind":"netem","dev":dev,"root":True,"handle":f"{100+source}:","drops":0,"backlog":0,"options":{"limit":1024,"delay":{"delay":0.05,"jitter":0},"seed":20261009,"ecn":False,"gap":0}})
                links.append({"ifname":dev,"stats64":{"rx":{"dropped":0},"tx":{"dropped":0}}})
                filters.append({"kind":"flower","pref":100+source,"options":{"keys":{"eth_type":"ipv4","src_ip":peers[source]["IP"]+"/32","dst_ip":peer["IP"]+"/32"},"skip_hw":True,"actions":[{"kind":"mirred","to_dev":dev,"mirred_action":"redirect","direction":"egress"}]}})
            rows.append({"Peer":peer,"Qdisc":q,"Filters":filters,"Links":links})
        self.assertEqual(owned.validate_readback(rows,peers,value,True),0)
        bad=copy.deepcopy(rows);bad[0]["Filters"][0]["options"]["keys"]["dst_ip"]="10.0.0.255/32"
        with self.assertRaises(ValueError): owned.validate_readback(bad,peers,value,True)
        bad=copy.deepcopy(rows);bad[0]["Qdisc"][1]=copy.deepcopy(bad[0]["Qdisc"][3])
        with self.assertRaises(ValueError): owned.validate_readback(bad,peers,value,True)
        bad=copy.deepcopy(rows);bad[0]["Qdisc"][3]["drops"]=1
        self.assertEqual(owned.validate_readback(bad,peers,value,True),1)
        foreign=copy.deepcopy(peers);foreign[1]["NetNS"]=foreign[0]["NetNS"]
        with self.assertRaises(ValueError): owned.directed(foreign)
        with self.assertRaises(ValueError): owned.validate_readback([],peers,value,True)

    def test_all_actual_family_schedules_and_native_admission_binding(self):
        for family, minimum in (("contention-empty-range",150000300),("snapshot-selectors",200000300),("cross-edge-endpoint-uniqueness",600000700),("immutable-columns",200000300)):
            value=request();value["Spec"]["family"]=family
            self.assertEqual(owned.validate_input(value),value)
            self.assertEqual(owned.causal_minimum(value["Spec"],value["Transport"]),minimum)
            p={"Version":2,"Spec":value["Spec"],"Run":[1]*16,"Transport":value["Transport"],"BudgetSeconds":value["BudgetSeconds"],"CausalMinimumMillis":minimum,"AdmittedUnixNanos":1,"DeadlineUnixNanos":1+value["BudgetSeconds"]*1_000_000_000,
               **{k:value[k] for k in ("QueuePackets","SampleMillis","MaxSamples","MaxArtifactBytes")},"ProbeSamples":9,"ProbePayloadBytes":64,"ProbeTimeoutMillis":3000,
               "Tools":{"Version":"iproute2-6.12.0",**{k:[i+1]*32 for i,k in enumerate(("TC","IP","Probe","Source","Build"))}},
               "Peers":[{"Role":role,"Container":f"{i+1:064x}","Image":"sha256:"+f"{i+10:064x}","IP":f"10.0.0.{i+10}","NetNS":f"net:[{i+100}]","Network":"aa"*32,"PID":i+1} for i,role in enumerate(owned.ROLES)],"ArmedSHA256":[7]*32}
            p["Tools"]["Source"]=list(bytes.fromhex(owned.SOURCE_SHA))
            stem={"contention-empty-range":"contention","snapshot-selectors":"selectors","cross-edge-endpoint-uniqueness":"edge-unique","immutable-columns":"columns"}[family]
            c={"Graph":f"graph-workload-{stem}-v2-kv-"+"01"*16}
            key=b"g0/"+c["Graph"].encode().hex().encode()+b"/workload-transport-v2/"+b"binding".hex().encode()
            native={"Policy":p,"Record":{"Key":base64.b64encode(key).decode(),"Value":base64.b64encode(owned.canonical(p)).decode()}}
            self.assertEqual(owned.family_record(native,value["Spec"],c),list(hashlib.sha256(owned.canonical(p)).digest()))
            for bad in ({**native,"Record":{**native["Record"],"Value":base64.b64encode(b"{}").decode()}}, {**native,"Policy":{**p,"DeadlineUnixNanos":p["DeadlineUnixNanos"]+1}}):
                with self.assertRaises(ValueError):owned.family_record(bad,value["Spec"],c)
            with self.assertRaises(ValueError):owned.family_record(native,{**value["Spec"],"family":"snapshot-to-feed"},c)
            with self.assertRaises(ValueError):owned.validate_input(value|{"BudgetSeconds":minimum//1000})

    def test_stream_grant_partial_eof_and_missing_actual_assets(self):
        with tempfile.TemporaryDirectory() as directory:
            marker=Path(directory)/"transport-release"
            marker.touch()
            with mock.patch.object(cost_monitor.time,"sleep",side_effect=AssertionError("terminal observation slept60s")):
                cost_monitor.monitor_sleep(60,owned.time.monotonic()+120,marker)
            raw="memory.current\n4\nmemory.peak\n8\nmemory.stat\n"+"".join(name+" 0\n" for name in ("anon","file","kernel","kernel_stack","pagetables","sock","slab"))
            self.assertEqual(cost_monitor.parse_cgroup_raw(raw)[:2],(4,8))
            for bad in (raw+"anon 0\n",raw.replace("4\n","-1\n",1),raw.replace("sock 0\n","")):
                with self.assertRaises(ValueError): cost_monitor.parse_cgroup_raw(bad)
            e=owned.Evidence(directory,64)
            try:
                e.record({"one":1})
                with self.assertRaises(ValueError): e.write("not-created",{"too":"x"*64})
                self.assertFalse((Path(directory)/"not-created").exists())
            finally: e.close()
            partial=Path(directory)/"partial"
            partial.write_bytes(b'{"Ordinal":0}')
            with self.assertRaises(ValueError): list(owned.frames(partial,1,64))
            with self.assertRaises((ValueError,OSError)): owned.tools(directory)
            # A nominal policy is never enough to skip actual native artifacts.
            with self.assertRaises(ValueError): owned.validate_policy({"Version":2})


if __name__ == "__main__":
    unittest.main()
