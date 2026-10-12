"""Contract guards only; native graph tests and launcher establish actual faults."""
import copy
import hashlib
from pathlib import Path
import subprocess
import tempfile
import unittest

import launcher


class Contracts(unittest.TestCase):
    def test_cleanup_output_preserves_sealed_redirected_failure_log(self):
        # Reproduce Bash's EXIT trap inside a redirected failing function. The
        # original output FD must carry the final path after the log is sealed.
        source = Path(__file__).with_name("run-local.sh").read_text()
        setup = next(line for line in source.splitlines() if line.startswith("exec 3>&1"))
        cleanup = source.split("cleanup() {", 1)[1].split("\n}\n", 1)[0]
        report = next(line for line in cleanup.splitlines() if "printf" in line)
        with tempfile.TemporaryDirectory() as directory:
            script = r'''
set -euo pipefail
SETUP
out="$1"
cleanup() {
  result=$?
  shasum -a 256 "$out/log" >"$out/hash"
REPORT
  exit "$result"
}
trap cleanup EXIT
fail() { printf 'command log\n'; false; }
fail >"$out/log"
'''.replace("SETUP", setup).replace("REPORT", report)
            result = subprocess.run(["bash", "-c", script, "--", directory], capture_output=True, text=True, timeout=5, check=False)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stdout, directory + "\n")
            raw = (Path(directory) / "log").read_bytes()
            self.assertEqual(raw, b"command log\n")
            self.assertEqual(hashlib.sha256(raw).hexdigest(), (Path(directory) / "hash").read_text().split()[0])

    def test_aggregate_admission_refuses_large_or_invalid_profiles(self):
        p = launcher.profile("small", 4096, 2)
        self.assertEqual(p["cluster_phase_mib"], 3072)
        self.assertEqual(p["cluster_phase_cpus"], 2)
        for name, memory, cpu in [("large", 4096, 2), ("small", 3071, 2), ("small", 4096, 1.99), ("missing", 4096, 2)]:
            with self.assertRaises((KeyError, ValueError)):
                launcher.profile(name, memory, cpu)
        self.assertEqual(launcher.profile("large", 10240, 2)["cluster_phase_mib"], 10240)

    def test_exact_markers_refuse_wrong_phase_extra_keys_or_empty(self):
        launcher.marker({"ready": "before-native-commit"}, "before")
        launcher.marker({"ready": "after-native-commit-before-reply"}, "after")
        for value in [{}, {"ready": "after-native-commit-before-reply"}, {"ready": "before-native-commit", "extra": True}, {"ready": True}]:
            with self.assertRaises(AssertionError):
                launcher.marker(value, "before")

    def test_observed_resource_settings_refuse_oom_or_wrong_limits(self):
        p = launcher.profile("small", 4096, 2)
        record = {"HostConfig": {"Memory": 512 << 20, "NanoCpus": 375000000}, "State": {"OOMKilled": False}}
        launcher.resource(p, [record], "server")
        for records in ([], [record, record]):
            with self.assertRaises(AssertionError):
                launcher.resource(p, records, "server")
        for location, key, value in [("HostConfig", "Memory", 2 << 30), ("HostConfig", "NanoCpus", 0), ("State", "OOMKilled", True)]:
            bad = copy.deepcopy(record)
            bad[location][key] = value
            with self.assertRaises(AssertionError):
                launcher.resource(p, [bad], "server")

    def test_health_refuses_weaker_replication_or_missing_zones(self):
        state = {"name": "healthy", "min_replicas_remaining": 3}
        d = {"client": {"database_status": {"healthy": True}, "coordinators": {"coordinators": [1, 2, 3]}},
             "cluster": {"configuration": {"redundancy_mode": "triple"}, "full_replication": True,
                         "data": {"state": state}, "recovery_state": {"name": "fully_recovered"},
                         "processes": dict.fromkeys(range(4)), "machines": {i: {"locality": {"zoneid": f"zone{i}"}} for i in range(4)}}}
        launcher.healthy(d)
        for field, value in [("configuration", {"redundancy_mode": "double"}), ("full_replication", False), ("machines", {})]:
            bad = copy.deepcopy(d)
            bad["cluster"][field] = value
            with self.assertRaises(AssertionError):
                launcher.healthy(bad)


if __name__ == "__main__":
    unittest.main()
