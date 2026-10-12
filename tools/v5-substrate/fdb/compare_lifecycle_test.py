"""Small adversarial checks over supplied actual native/FDB trace artifacts."""
import argparse
import base64
import copy
import hashlib
import importlib.util
from pathlib import Path
import sys
import unittest

spec = importlib.util.spec_from_file_location("lifecycle_compare", Path(__file__).with_name("compare-lifecycle.py"))
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)
NATIVE = FDB = None


class ActualTraceChecks(unittest.TestCase):
    def test_actual_complete_traces_equal(self):
        self.assertTrue(compare.compare(NATIVE, FDB)["equal"])

    def test_missing_closed_property_or_current_as_old_refuses(self):
        for mutate in (
            lambda trace: trace["steps"][-1]["After"]["Entities"][-1].__setitem__("Property", trace["steps"][-1]["After"]["Entities"][0]["Property"]),
            lambda trace: trace["oldReads"][0].__setitem__("View", trace["steps"][-1]["After"]),
        ):
            bad = copy.deepcopy(FDB)
            mutate(bad)
            with self.assertRaises(compare.Refusal):
                compare.compare(NATIVE, bad)

    def test_missing_physical_posting_and_changed_metadata_refuse(self):
        for mutate in (
            lambda trace: trace["steps"][2]["RawObservation"]["Groups"][2]["Writes"].pop(),
            lambda trace: trace["steps"][3]["RawObservation"]["Groups"][3]["Tx"]["Lifecycle"].__setitem__("Provenance", 91),
            lambda trace: trace["steps"][5]["RawObservation"]["Groups"][5]["Writes"][-1]["After"].__setitem__("Deleted", False),
            lambda trace: trace["steps"][2]["LogicalCDC"][0].pop("AllScope"),
            lambda trace: trace["oldReads"][0]["Cut"].__setitem__("Round", trace["oldReads"][0]["Cut"]["Round"] + 1),
            lambda trace: trace["oldReads"].__setitem__(1, copy.deepcopy(trace["oldReads"][0])),
            lambda trace: trace["steps"][2]["RawRecords"][0].__setitem__("Value", base64.b64encode(b"{}").decode()),
        ):
            bad = copy.deepcopy(FDB)
            mutate(bad)
            with self.assertRaises((compare.Refusal, KeyError)):
                compare.compare(NATIVE, bad)

        # Coherent DTO/digest changes cannot turn a wrong physical key into a
        # valid edge effect. The declared GraphChange still names the real edge.
        bad = copy.deepcopy(FDB)
        group = bad["steps"][2]["RawObservation"]["Groups"][2]
        edge = group["Tx"]["GraphChange"]["Edge"]["ID"]
        for participant in group["Tx"]["Participants"]:
            for effect in participant["Effects"]:
                effect["Key"] = effect["Key"].replace(f"/{edge:016x}/", f"/{edge + 100:016x}/", 1)
        for write in group["Writes"]:
            write["Key"] = write["Key"].replace(f"/{edge:016x}/", f"/{edge + 100:016x}/", 1)
        group["Outcome"]["Digest"] = list(hashlib.sha256(compare.go_json(group["Tx"])).digest())
        with self.assertRaisesRegex(compare.Refusal, "FDB graph physical shape"):
            compare.compare(NATIVE, bad)

    def test_identity_alias_and_float_impersonating_i64_refuse(self):
        bad = copy.deepcopy(FDB)
        bad["identityMapping"][-1]["Raw"] = bad["identityMapping"][-2]["Raw"]
        with self.assertRaises(compare.Refusal):
            compare.compare(NATIVE, bad)
        bad = copy.deepcopy(NATIVE)
        bad["steps"][3]["After"]["Entities"][-1]["Property"]["I64"] = 6.0
        with self.assertRaises(compare.Refusal):
            compare.compare(bad, FDB)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("native")
    parser.add_argument("fdb")
    args = parser.parse_args()
    NATIVE, _ = compare.load(args.native)
    FDB, _ = compare.load(args.fdb)
    unittest.main(argv=[sys.argv[0]], verbosity=2)
