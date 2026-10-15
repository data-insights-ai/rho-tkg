#!/usr/bin/env python3
"""Compact inert codec/negative guards, never native source authority."""
import hashlib
import struct
import unittest
import native_transition_scope_check as scope


def sealed(body):
    return body + hashlib.sha256(body).digest()


def request(partitions=(3,), mode=1, tx=bytes(16), coordinator=0):
    return sealed(b"GEQ\2" + bytes([mode]) + tx + struct.pack(">QI", coordinator, 0)
                  + struct.pack(">I", len(partitions)) + b"".join(struct.pack(">Q", p) for p in partitions))


def facts(current=False, phantom=False):
    result = b"GSF1" + struct.pack(">I", 3)
    for owner in (1, 3, 7):
        present = current and owner == 1 or phantom and owner == 7
        body = bytes([3 if present else 0]) + struct.pack(">QQQII", 2 if present else 0, 0, 0, 0, 0)
        result += struct.pack(">QI", owner, len(body)) + body
    return result


class ScopedSourceGuards(unittest.TestCase):
    def test_explicit_scope_is_never_unrestricted_or_foreign(self):
        self.assertEqual(scope.request(request()), (1, bytes(16), []))
        self.assertEqual(scope.request(request(mode=4, tx=b"x"*16, coordinator=8)), (4, b"x"*16, []))
        for wire in (request(()), request((8,)), request((3, 8)), request((3, 3)), request((0,)),
                     request(mode=4), request(mode=1, tx=b"x"*16, coordinator=8), request(mode=3), request()+b"extra"):
            with self.subTest(wire=wire), self.assertRaises(ValueError):
                scope.request(wire)

    def test_old_exact_empty_cannot_be_replaced_by_current_or_phantom(self):
        self.assertEqual(scope.facts(facts(), False), {})
        self.assertEqual(scope.facts(facts(True), True), {"A/node/1": dict(life=2, labels=[], incident=[])})
        for wire, current in ((facts(True), False), (facts(), True), (facts(True, True), True), (facts()+b"extra", False)):
            with self.subTest(wire=wire, current=current), self.assertRaises(ValueError):
                scope.facts(wire, current)

    def test_unexpected_errors_and_partial_tokens_never_qualify(self):
        valid = dict(code=6, mask=0, aux=b"", image=b"", outcome=b"", changes=b"")
        scope.unavailable(valid, 1 << 10)
        for response, mask in (({**valid, "code": 0}, 1 << 10), ({**valid, "code": 1}, 1 << 10),
                               ({**valid, "code": 2}, 1 << 10), ({**valid, "code": 4}, 1 << 10),
                               (valid, 1 << 3), (valid, (1 << 10) | (1 << 2)),
                               ({**valid, "aux": b"partial-cut"}, 1 << 10), ({**valid, "mask": 1 << 4}, 1 << 10)):
            with self.subTest(response=response, mask=mask), self.assertRaises(ValueError):
                scope.unavailable(response, mask)
        for data in ({}, dict(Version=1, Profile=scope.PROFILE), dict.fromkeys(scope.HEADER)):
            with self.assertRaises(ValueError):
                scope.check(data)


if __name__ == "__main__":
    unittest.main()
