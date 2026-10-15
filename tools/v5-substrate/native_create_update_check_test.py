#!/usr/bin/env python3
"""Authored fresh-profile/actual-original guards, no execution evidence."""
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
import native_create_update_check as check
import native_create_update_cell as cell
from comparison_spool import Policy


class NativeCreateUpdateGuards(unittest.TestCase):
    def test_fresh_shape_and_real_local_vs_cross_selection(self):
        s={"version":1,"family":check.FAMILY,"facts":40,"crossPercent":0,"writers":32,"layout":"columns"}
        for transport in check.TRANSPORTS:self.assertEqual(cell.preflight(s,transport,1800,4096)["targetRTTMillis"],check.TRANSPORTS[transport])
        for delay in ("rtt-1ms","rtt-20ms","rtt-100ms"):
            with self.assertRaises(ValueError):cell.preflight(s,delay,1800,4096,"paired")
        for change in ({"version":True},{"facts":20},{"family":"snapshot-selectors"},{"crossPercent":True}):
            with self.assertRaises(ValueError):check.shape(s|change)
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/"missing"
            with self.assertRaises(ValueError):cell.execute(s|{"family":"snapshot-to-feed"},"local",output,"/missing","/missing",180,4096,Policy(1<<20,65536,4096,check.LINE))
            self.assertFalse(output.exists())
            parent=Path(directory).resolve();target=parent/"foreign";target.mkdir()
            (target/"keep").write_text("unchanged")
            linked=parent/"linked";linked.symlink_to(target,target_is_directory=True)
            absent=parent/"absent";dangling=parent/"dangling";dangling.symlink_to(absent,target_is_directory=True)
            for refused in (linked,dangling,target):
                with self.assertRaises(ValueError):cell.owned_output(refused)
            self.assertFalse(absent.exists());self.assertEqual((target/"keep").read_text(),"unchanged")
            binary,watchdog=parent/"binary",parent/"watchdog";binary.write_bytes(b"supplied");watchdog.write_bytes(b"supplied")
            class BeforeNative(Exception):pass
            def launch(command,*,env,stdout,stderr,check):
                engine=Path(env["RHO_GRAPH_WORKLOAD_OUTPUT"])
                self.assertTrue(engine.is_absolute() and not engine.exists())
                self.assertIn("-test.run=^TestNativeGraphCreateUpdateWorkloadCell$",command)
                raise BeforeNative
            with patch.object(cell,"executables",return_value=(binary,watchdog)),patch.object(cell.subprocess,"run",side_effect=launch):
                with self.assertRaises(BeforeNative):cell.execute(s,"local",parent/"cell",binary,watchdog,1800,4096,Policy(1<<20,65536,4096,check.LINE),run="01"*16)

    def test_phase_wall_cannot_be_replaced_by_parallel_latency_sum(self):
        p={"Phase":"create","StartNanos":10,"StopNanos":110,"WallNanos":100,"SingleParticipant":20,"MultiParticipant":20,"ReadParticipants":[40,20],"WriteParticipants":[40,20]}
        check.phase(p,40,50,1000)
        for change in ({"WallNanos":99},{"MultiParticipant":0},{"SingleParticipant":40},{"StopNanos":2000}):
            with self.assertRaises(ValueError):check.phase(p|change,40,50,1000)
        check.phase(p|{"Phase":"update","WriteParticipants":[40,0]},40,50,1000)
        with self.assertRaises(ValueError):check.phase(p|{"Phase":"update"},40,50,1000)


if __name__=="__main__":unittest.main()
