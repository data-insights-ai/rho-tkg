#!/usr/bin/env python3
"""Authored direct guards only; synthetic bytes are never SDK evidence."""
from pathlib import Path
import tempfile
import unittest

import fdb_columns_check as check
from comparison_spool import Policy, Spool, SpoolError


class ImmutableColumnsOracleGuards(unittest.TestCase):
    def test_exact_generation_timing_and_nonrenewable_spool_identity(self):
        old={"Graph":"fresh-columns","Round":7,"Groups":[22,1]}
        end={"Spec":{"family":check.FAMILY,"facts":20,"layout":"columns"},"Committed":43,"Old":old,"Final":old|{"Round":9,"Groups":[42,1]}}
        sample={"Type":"column-timing","Call":"Walk","Name":"old-cache","Access":"complete actual caller operation through EOF including retry/wait and artifact IO; scan-based points are not indexed latency","Cut":old,"Rows":23,"Nanos":10}
        policy=Policy(4<<20,64<<10,4096,512<<10)
        with tempfile.TemporaryDirectory() as directory:
            with Spool(directory,policy) as spool:
                check.timing(sample,end,spool)
                with self.assertRaises((ValueError,SpoolError)):check.timing(sample,end,spool)
            for change in ({"Cut":end["Final"]},{"Rows":43},{"Call":"IndexedPoint"},{"Name":"foreign"},{"Nanos":0}):
                with Spool(directory,policy) as spool,self.assertRaises(ValueError):check.timing(sample|change,end,spool)
            with Spool(directory,policy) as spool,self.assertRaises(ValueError):check.timing(sample,{**end,"Spec":{**end["Spec"],"layout":"kv"}},spool)
            self.assertEqual(list(Path(directory).iterdir()),[])

    def test_explicit_filename_keeps_old_oracle_defaults_and_partial_refusal(self):
        # A real file in the dedicated namespace must not become accepted KV
        # columns just because the historical selector filename is absent.
        end={"Spec":{"layout":"kv"}}
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/"fdb-columns-columns.jsonl").write_bytes(b'{"Type":"header"}\n')
            with Spool(directory,Policy(4<<20,64<<10,4096,512<<10)) as spool:
                self.assertIsNone(check.source.columns(root,end,spool))
                with self.assertRaises(ValueError):check.source.columns(root,end,spool,"fdb-columns-columns.jsonl")
            # Refuse missing family event EOF before manufacturing timing or
            # completion from a printable header/count.
            (root/"fdb-columns-events.jsonl").write_bytes(b"")
            partial={"Config":{},"Spec":{"facts":20,"layout":"kv"},"Attempts":0,"Committed":43}
            with Spool(directory,Policy(4<<20,64<<10,4096,512<<10)) as spool,self.assertRaises(ValueError):check.events(root,partial,spool,{})
        with self.assertRaises(ValueError):check.validate_result({"Version":1,"Format":"fdb-snapshot-selectors-v1"})


if __name__=="__main__":
    unittest.main()
