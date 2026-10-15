#!/usr/bin/env python3
"""Authored compact guards; fabricated fixtures are never SDK evidence."""
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import fdb_correction_check as check
from comparison_spool import Policy,Spool


class CorrectionOracleGuards(unittest.TestCase):
    def test_exact_predicate_duplicate_and_partial_eof(self):
        row={"Group":0,"Key":"out/actual","Value":{"Value":5}}
        frame={"Type":"match","Name":"old","Match":{"Selector":0,"Row":row}}
        end={"Config":{"Graph":"fresh"},"Spec":{"facts":20},"Old":{"Round":1},"Final":{"Round":2}}
        with tempfile.TemporaryDirectory() as directory:
            policy=Policy(4<<20,64<<10,4096,512<<10)
            with Spool(directory,policy) as spool,mock.patch.object(check.source,"snapshot_row") as native:
                counts=[[0,0] for _ in range(8)]
                self.assertEqual(check.index_match(frame,"old",end,spool,None,counts),(0,"out/actual"))
                self.assertEqual(counts[0],[1,0]);native.assert_called_once()
                with self.assertRaises(ValueError):check.index_match(frame,"old",end,spool,(0,"out/actual"),counts)
                with self.assertRaises(ValueError):check.index_match({**frame,"Match":{**frame["Match"],"Selector":1}},"old",end,spool,None,counts)
                with self.assertRaises(ValueError):check.index_match({**frame,"Name":"final"},"old",end,spool,None,counts)
            (Path(directory)/"fdb-correction-indexes.jsonl").write_bytes(b"")
            with Spool(directory,policy) as spool,self.assertRaises(ValueError):check.indexes(Path(directory),end,spool)
        with self.assertRaises(ValueError):check.shared.validate_result({"Version":1},"unknown-family")
        with self.assertRaises(ValueError):check.shared.verify("absent","absent",family="unimplemented")


if __name__=="__main__":unittest.main()
