"""Authored fixed-size guards only; no executed cell or parity evidence."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import native_guarded_scalar_check as check
import native_guarded_scalar_cell as cell
import native_feed_check as feed
import native_selectors_check as selectors
import native_feed_cell as old_feed
import native_selectors_cell as old_selectors
from comparison_spool import Policy,Spool,u64


def spec(family="snapshot-to-feed"):
    return {"version":2,"family":family,"facts":40,"crossPercent":50,"writers":32,"layout":"columns"}


class GuardedScalarSourceGuards(unittest.TestCase):
    def test_full_dimensions_exact_profile_and_owned_native_delays(self):
        for family in check.FORMATS:
            for n in (40,10000,100000,1000000):
                for writers in (1,8,32):
                    for cross in (0,10,50,100):
                        for layout in ("kv","columns"):
                            s=spec(family)|{"facts":n,"writers":writers,"crossPercent":cross,"layout":layout};check.shape(s)
                            self.assertLess(check.frames(s)*260+65536,1<<39)
                            for transport,rtt in check.TRANSPORTS.items():self.assertEqual(cell.preflight(s,transport,31536000,4096)["targetRTTMillis"],rtt)
            cell.preflight(spec(family),"local",1800,4096,"paired")
            cell.preflight(spec(family),"local",31536000,4096,"paired",owned_fdb=True)
            with self.assertRaises(ValueError):cell.preflight(spec(family),"local",1801,4096,"paired")
            for transport in ("rtt-1ms","rtt-20ms","rtt-100ms"):
                with self.assertRaises(ValueError):cell.preflight(spec(family),transport,31536000,4096,"paired",owned_fdb=True)
        for change in ({"version":1},{"version":3},{"facts":True},{"writers":True},{"crossPercent":True},{"family":"immutable-columns"}):
            with self.assertRaises(ValueError):check.shape(spec()|change)
        with self.assertRaises(ValueError):feed.shape(spec())
        with self.assertRaises(ValueError):selectors.shape(spec("snapshot-selectors"))

    def test_old_controller_wrappers_preserve_literal_defaults(self):
        for module,family in ((old_feed,"snapshot-to-feed"),(old_selectors,"snapshot-selectors")):
            s=spec(family)|{"version":1}
            with patch.object(module,"_execute",return_value="old") as helper:
                self.assertEqual(module.execute(s,"local","owned","binary","watchdog",60,4096,"grant"),"old")
                helper.assert_called_once_with(s,"local","owned","binary","watchdog",60,4096,"grant",run=None)
        with patch.object(old_feed,"_execute",return_value="fresh") as helper:
            self.assertEqual(cell.execute(spec(),"local","owned","binary","watchdog",60,4096,"grant"),"fresh")
            self.assertEqual(helper.call_args.kwargs["_format"],check.FORMATS["snapshot-to-feed"])
            self.assertIs(helper.call_args.kwargs["_verify"],check.verify)
            self.assertEqual(helper.call_args.kwargs["_test"],"TestNativeGraphGuardedSnapshotFeedWorkloadCell")

    def test_owned_fdb_inventory_keeps_known_nested_tools_and_path_safety(self):
        with tempfile.TemporaryDirectory() as path:
            directory=Path(path);(directory/"owned-tools").mkdir()
            hashes={}
            for name in ("fdb-feed-result.json",*sorted(cell.OWNED_TOOLS)):
                (directory/name).write_bytes(b"actual bounded fixture")
                hashes[name]=cell.digest(directory/name)
            cell.fdb_inventory(directory,hashes)
            for name in ("../outside","owned-tools/../fdb-feed-result.json","owned-tools/foreign","receipt.json"):
                with self.assertRaises(ValueError):cell.fdb_inventory(directory,hashes|{name:"0"*64})
            incomplete=dict(hashes);incomplete.pop("owned-tools/tc")
            with self.assertRaises(ValueError):cell.fdb_inventory(directory,incomplete)
            (directory/"owned-tools/tc").unlink();(directory/"owned-tools/tc").symlink_to(directory/"fdb-feed-result.json")
            with self.assertRaises(ValueError):cell.fdb_inventory(directory,hashes)
        with patch.object(cell.common,"executables",return_value=("binary","watchdog")) as helper:
            self.assertEqual(cell.executables("binary",None),("binary","watchdog"))
            self.assertEqual(helper.call_args.args[1],Path(cell.__file__).with_name("process_watchdog.py"))

    def test_current_value_with_old_presence_or_revision_refuses(self):
        def v(handle,revision,provenance,kind):return {"present":True,"null":False,"handle":handle,"revision":revision,"provenance":provenance,"kind":kind,"i64":handle if kind=="i64" else 0}
        p={"Fact":0,"Entity":{"id":7,"found":True,"effective":True,"kind":2,"type":"edge","mode":1,"source":1,"target":3,"effectiveLife":8,"labels":[],"presence":v(8,23,44,"life"),"property":v(6,23,44,"i64")}}
        with tempfile.TemporaryDirectory() as path:
            with Spool(path,Policy(1<<20,65536,4096,check.LINE)) as spool:
                for component,name in ((1,"presence"),(3,"property")):
                    c=p["Entity"][name];after={"Present":True,"Null":False,"Handle":c["handle"],"Value":c["i64"],"Revision":c["revision"],"Provenance":c["provenance"]}
                    spool.insert(b"row",u64(44)+u64(7)+bytes([component]),feed.encoded({"After":after}))
                self.assertEqual(feed.point(spec(),p,6,spool,guarded_updates=True),0)
                for field,key,value in (("presence","provenance",4),("presence","revision",11),("property","revision",11),("presence","handle",9)):
                    bad=copy.deepcopy(p);bad["Entity"][field][key]=value
                    with self.assertRaises(ValueError):feed.point(spec(),bad,6,spool,guarded_updates=True)
                with self.assertRaises(ValueError):feed.point(spec(),p,6,spool)

    def test_pairing_compares_full_lives_dictionary_and_provenance(self):
        row={"Owner":7,"Life":8,"Source":1,"Target":87,"SourceLife":2,"TargetLife":88,"After":{"Present":True,"Null":False,"Handle":6,"Value":6,"Provenance":44}}
        group={"Tx":{"GraphChange":{"Edge":{"ID":7,"Source":{"Node":1,"Life":2},"Target":{"Node":87,"Life":88}}},"Lifecycle":{"EdgeLife":8,"Provenance":44,"Definition":{"Kind":"i64","ID":6,"I64":6}}}}
        self.assertEqual(cell.normalized_native(row),cell.normalized_fdb(group))
        for key in ("Owner","Life","Source","Target","SourceLife","TargetLife"):
            bad=copy.deepcopy(row);bad[key]+=1;self.assertNotEqual(cell.normalized_native(bad),cell.normalized_fdb(group))
        for key in ("Handle","Value","Provenance"):
            bad=copy.deepcopy(row);bad["After"][key]+=1;self.assertNotEqual(cell.normalized_native(bad),cell.normalized_fdb(group))
        for key in ("Owner","Life","Source"):
            bad=copy.deepcopy(row);bad[key]=True
            with self.assertRaises(ValueError):cell.normalized_native(bad)
        with tempfile.TemporaryDirectory() as path:
            output=Path(path)/"absent"
            with patch.object(old_feed,"_execute",side_effect=AssertionError("engine effects")):
                with self.assertRaises(ValueError):cell.preflight(spec(),"rtt-100ms",60,4096,"paired")
            self.assertFalse(output.exists())


if __name__=="__main__":unittest.main()
