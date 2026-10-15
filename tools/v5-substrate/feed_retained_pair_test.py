"""Authored paired minimum-contract guards; no runtime parity claim."""
import copy
import tempfile
from pathlib import Path
import unittest
from unittest import mock

import feed_retained_pair as pair
from comparison_spool import Policy
from fdb_feed_retained_check_test import spec,grant


class RetainedPairGuards(unittest.TestCase):
    def test_nonlocal_or_unfunded_pair_refuses_before_any_engine_or_directory(self):
        options={"QueuePackets":1024,"SampleMillis":1000,"MaxSamples":62,"MaxArtifactBytes":1<<30}
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"absent"
            with mock.patch.object(pair.native_cell,"execute",side_effect=AssertionError("native effects")):
                for transport in ("rtt-1ms","rtt-20ms","rtt-100ms"):
                    with self.assertRaises(ValueError):pair.execute(spec(),transport,path,None,None,60,4096,Policy(1<<30,8<<20,4096,512<<10),profile="small",owned_options=options,owner_grant=grant())
                with self.assertRaises(ValueError):pair.execute(spec(),"local",path,None,None,60,4096,Policy(1<<30,8<<20,4096,512<<10),profile="small",owned_options=options,owner_grant=grant()|{"MaxRecords":3})
                self.assertFalse(path.exists())

    def test_complete_typed_life_endpoints_and_value_are_compared_not_labels(self):
        row={"Owner":7,"Life":8,"Source":1,"Target":87,"SourceLife":2,"TargetLife":88,"After":{"Present":True,"Null":False,"Handle":6,"Value":6,"Provenance":44}}
        group={"Tx":{"GraphChange":{"Edge":{"ID":7,"Source":{"Node":1,"Life":2},"Target":{"Node":87,"Life":88}}},"Lifecycle":{"EdgeLife":8,"Provenance":44,"Definition":{"Kind":"i64","ID":6,"I64":6}}}}
        self.assertEqual(pair.normalized_native(row),pair.normalized_fdb(group))
        for field in ("Owner","Life","Source","Target","SourceLife","TargetLife"):
            bad=copy.deepcopy(row);bad[field]+=1;self.assertNotEqual(pair.normalized_native(bad),pair.normalized_fdb(group))
        for field in ("Handle","Value","Provenance"):
            bad=copy.deepcopy(row);bad["After"][field]+=1;self.assertNotEqual(pair.normalized_native(bad),pair.normalized_fdb(group))
        for bad in ({"Present":False},{"Null":True},{"Handle":True}):
            changed=copy.deepcopy(row);changed["After"].update(bad)
            with self.assertRaises(ValueError):pair.normalized_native(changed)


if __name__=="__main__":unittest.main()
