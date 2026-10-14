"""Compact source dispatch/format guards; subprocesses are mocked, no engine."""
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("cost_column_controller",Path(__file__).with_name("cost-cell.py"))
CELL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CELL)


class NativeColumnController(unittest.TestCase):
    def test_explicit_fdb_retention_uses_fresh_enum_and_refuses_foreign_selection(self):
        for writers in (1,8,32):
            spec = CELL.specification(10000,100,writers)
            for layout in ("kv","columns"):
                self.assertEqual(CELL.fdb_format(spec,layout,"source-v1"),(6,"fdb-graph-cost-retained-v1"))
                self.assertEqual(CELL.fdb_format(spec,layout,"off")[0],(3 if layout=="columns" else 2) if writers==1 else (5 if layout=="columns" else 4))
        with self.assertRaises(ValueError): CELL.fdb_format(spec,"kv","foreign")
        with self.assertRaises(ValueError): CELL.fdb_format(spec,"relabeled","source-v1")

    def test_fdb_dispatch_always_overrides_inherited_source_profile(self):
        spec = CELL.specification(10000,0,1)
        for retention in ("off","source-v1"):
            with self.subTest(retention=retention), tempfile.TemporaryDirectory() as tmp:
                args = SimpleNamespace(layout="kv",fdb_retention=retention,output=Path(tmp),benchmark_seconds=60,probe=True)
                def refused(command,**kw):
                    self.assertEqual(kw["env"]["RHO_GRAPH_COST_RETENTION"],retention)
                    self.assertEqual(kw["env"]["RHO_GRAPH_COST_LAYOUT"],"kv")
                    return SimpleNamespace(returncode=1)
                with patch.dict(os.environ,{"RHO_GRAPH_COST_RETENTION":"foreign"}),patch.object(CELL.subprocess,"run",side_effect=refused) as run:
                    with self.assertRaisesRegex(ValueError,"artifact root"): CELL.fdb_cell(args,spec,1,{"synthetic-test-only":True})
                    self.assertEqual(run.call_count,1)

    def test_fresh_native_column_contract_refuses_kv_relabelling_and_partial_seals(self):
        kv = {"version":1,"runner":"independent-clients-request-bound-ack-v2"}
        CELL.native_format(kv,"kv")
        fresh = {"version":2,"runner":"independent-clients-request-bound-ack-columns-v1","layout":"columns",
                 "columnInitialCut":{"Certificates":["test-left","test-right"]},
                 "columnGenerations":[[{"Generation":{"ID":[n]+[0]*15,"Coverage":1<<g,"Sources":[{},{}]}} for g in range(2)] for n in (1,2)]}
        CELL.native_format(fresh,"columns") # Envelope only; complete checker authenticates the real seals/bytes.
        for bad in (kv, fresh | {"version":True}, fresh | {"version":1}, fresh | {"runner":kv["runner"]},
                    fresh | {"layout":"kv"}, fresh | {"columnInitialCut":None}, fresh | {"columnGenerations":[]}):
            with self.subTest(bad=bad), self.assertRaises(ValueError): CELL.native_format(bad,"columns")
        with self.assertRaises(ValueError): CELL.native_format(fresh,"kv")

    def test_native_dispatch_selects_actual_layout_and_retains_failed_receipt(self):
        spec = CELL.specification(10000,100,8)
        for layout in ("kv","columns"):
            with self.subTest(layout=layout), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                binary,watchdog = root/"fixture-binary",root/"fixture-watchdog"
                binary.write_bytes(b"not-an-executed-engine")
                watchdog.write_bytes(b"not-an-executed-watchdog")
                args = SimpleNamespace(layout=layout,output=root,native_binary=binary,watchdog=watchdog,benchmark_seconds=60,probe=True)
                def refused(command,**kw):
                    self.assertEqual(kw["env"]["RHO_GRAPH_COST_LAYOUT"],layout)
                    self.assertEqual(json.loads(kw["env"]["RHO_GRAPH_COST_SPEC"]),spec)
                    self.assertIn("-test.run=^TestNativeGraphCostCell$",command)
                    report = Path(command[command.index("--report")+1])
                    report.write_text(json.dumps({"exit_code":1}))
                    return SimpleNamespace(returncode=1)
                with patch.dict(os.environ,{"RHO_GRAPH_COST_LAYOUT":"foreign"}),patch.object(CELL.subprocess,"run",side_effect=refused) as run:
                    with self.assertRaisesRegex(ValueError,"refused/failed"): CELL.native_cell(args,spec,1,{"synthetic-test-only":True})
                    self.assertEqual(run.call_count,1)
                receipt = next(root.glob("native-*/receipt.json"))
                result = json.loads(receipt.read_text())
                self.assertFalse(result["completedCell"] or result["minimumFiveRunAcceptance"])
                self.assertEqual(result["layout"],layout)
                self.assertEqual(result["version"],2 if layout=="columns" else 1)
                self.assertNotIn("result",result)

    def test_native_columns_need_every_actual_hash_bound_source_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            native = {"completedCell":True,"spec":CELL.specification(10000,0,1),"layout":"columns",
                      "result":{"version":2,"layout":"columns","runner":"independent-clients-request-bound-ack-columns-v1",
                                "columnInitialCut":{"Certificates":["test-left","test-right"]},
                                "columnGenerations":[[{"Generation":{"ID":[n]+[0]*15,"Coverage":1<<g,"Sources":[{},{}]}} for g in range(2)] for n in (1,2)]},"artifacts":{}}
            fdb = {"completedCell":True,"spec":native["spec"],"layout":"columns"}
            left,right = root/"native.json",root/"fdb.json"
            left.write_text(json.dumps(native)); right.write_text(json.dumps(fdb))
            with self.assertRaisesRegex(ValueError,"hash-bound"): CELL.compare_cells(left,right)
            self.assertEqual(list(root.glob(".cost-spool-*")),[])


if __name__ == "__main__": unittest.main()
