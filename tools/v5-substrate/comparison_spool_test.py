"""Direct byte/state guards for disposable comparison state, never engine proof."""
import json
from pathlib import Path
import tempfile
import unittest

from comparison_spool import Policy, Spool, SpoolError, u64


class ComparisonSpool(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "caller-input").write_bytes(b"immutable input")
        self.policy = Policy(max_disk_bytes=64 << 10, cache_bytes=16 << 10,
                             max_key_bytes=64, max_value_bytes=1024)

    def tearDown(self):
        self.temp.cleanup()

    def clean(self):
        self.assertEqual(list(self.root.iterdir()), [self.root / "caller-input"])
        self.assertEqual((self.root / "caller-input").read_bytes(), b"immutable input")

    def test_raw_uint64_predecessor_walk_and_success_cleanup(self):
        maximum = (1 << 64) - 1
        self.assertEqual(u64(maximum), b"\xff" * 8)
        with Spool(self.root, self.policy) as state:
            raw = b"\0\xfforiginal bytes\n"
            state.insert(b"native", u64(maximum), raw)
            state.insert(b"native", u64(0), b"")
            self.assertEqual(state.lookup(b"native", u64(maximum)), raw)
            self.assertIsNone(state.find(b"fdb", u64(maximum)))
            state.replace(b"native", u64(maximum), raw, raw + b"after")
            self.assertEqual(list(state.walk(b"native")),
                             [(u64(0), b""), (u64(maximum), raw + b"after")])
            self.assertEqual(list(state.directory.iterdir()), [state.path])
            report = state.report()
            self.assertLessEqual(report["peak_file_bytes"], self.policy.max_disk_bytes)
            self.assertFalse(report["cache_is_process_rss_limit"])
            print(json.dumps(report, sort_keys=True))
        self.clean()
        state.close()  # Idempotent cleanup, never resume.
        with self.assertRaisesRegex(SpoolError, "closed"):
            state.find(b"native", u64(0))
        for invalid in (-1, 1 << 64, True, 1.0):
            with self.assertRaises(SpoolError):
                u64(invalid)

    def test_duplicate_original_and_transaction_refuse_and_cleanup(self):
        for namespace in (b"original", b"transaction"):
            with self.subTest(namespace=namespace), self.assertRaisesRegex(SpoolError, "duplicate"):
                with Spool(self.root, self.policy) as state:
                    key = u64((1 << 64) - 1) + b"\0opaque-id"
                    state.insert(namespace, key, b"original")
                    state.insert(namespace, key, b"changed")
            self.clean()

    def test_wrong_predecessor_and_missing_state_refuse_and_cleanup(self):
        with self.assertRaisesRegex(SpoolError, "predecessor"):
            with Spool(self.root, self.policy) as state:
                state.insert(b"physical", b"key", b"before\0")
                state.replace(b"physical", b"key", b"before", b"after")
        self.clean()
        with self.assertRaisesRegex(SpoolError, "missing"):
            with Spool(self.root, self.policy) as state:
                state.replace(b"physical", b"absent", b"", b"after")
        self.clean()

    def test_page_quota_and_blob_type_refuse_and_cleanup(self):
        with self.assertRaisesRegex(SpoolError, "quota"):
            Spool(self.root, Policy(4096, 4096, 64, 1024))
        self.clean()
        tiny = Policy(max_disk_bytes=8192, cache_bytes=4096,
                      max_key_bytes=64, max_value_bytes=4096)
        with self.assertRaisesRegex(SpoolError, "capacity|quota"):
            with Spool(self.root, tiny) as state:
                state.insert(b"physical", b"key", b"x" * 4096)
        self.clean()
        with self.assertRaisesRegex(SpoolError, "bytes"):
            with Spool(self.root, self.policy) as state:
                state.insert(b"physical", b"key", "not raw bytes")
        self.clean()
        for key, value in ((b"k" * 65, b""), (b"key", b"v" * 1025)):
            with self.subTest(lengths=(len(key), len(value))), self.assertRaisesRegex(SpoolError, "quota"):
                with Spool(self.root, self.policy) as state:
                    state.insert(b"physical", key, value)
            self.clean()

    def test_consumer_failure_and_active_walk_cleanup(self):
        with self.assertRaisesRegex(RuntimeError, "consumer"):
            with Spool(self.root, self.policy) as state:
                state.insert(b"physical", b"key", b"value")
                raise RuntimeError("consumer validation failure")
        self.clean()
        with self.assertRaisesRegex(SpoolError, "walk"):
            with Spool(self.root, self.policy) as state:
                state.insert(b"physical", b"key", b"value")
                iterator = state.walk(b"physical")
                self.assertEqual(next(iterator), (b"key", b"value"))
                state.insert(b"physical", b"other", b"value")
        iterator.close()
        self.clean()


if __name__ == "__main__":
    unittest.main()
