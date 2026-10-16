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
    def test_failure_reader_uses_authenticated_program_layout_not_held_reader(self):
        selected = []  # Presence alone grants no process/semantic authority.
        self.assertEqual(scope.scope_reader(), 3)
        self.assertEqual(scope.scope_reader(selected_processes=selected), 2)
        original = dict(leaders=(1, 4), target=3)
        rotated = dict(leaders=(2, 6), target=1)
        self.assertEqual(scope.scope_reader(selected_processes=selected, program_roles=original), 2)
        self.assertEqual(scope.scope_reader(selected_processes=selected, program_roles=rotated), 3)
        for roles in (dict(leaders=(2, 6), target=3), dict(leaders=(1, 4), target=2),
                      dict(leaders=(2, 6), target=2), dict(leaders=(1, 4), target=5),
                      dict(leaders=[2, 6], target=1), dict(leaders=(2, 6), target=True)):
            with self.subTest(roles=roles), self.assertRaises(ValueError):
                scope.scope_reader(selected_processes=selected, program_roles=roles)
        for member, processes, roles in ((2, None, None), (3, selected, None), (3, selected, original),
                                         (2, selected, rotated), (True, selected, original),
                                         (3, None, rotated), (3, selected, dict(leaders=(1, 6), target=3))):
            with self.subTest(member=member, roles=roles), self.assertRaises(ValueError):
                scope.check({}, failure_member=member, selected_processes=processes, program_roles=roles)

    def test_failure_incarnation_cannot_borrow_held_reader_or_replacement(self):
        # Canonical inert native GPR body, not a graph source/cut witness.
        incarnation = bytes([3])*16
        wire = incarnation+bytes(28*8+32+32+8)+bytes(4)+struct.pack(">I", 0)
        wire += bytes(1)+struct.pack(">QQBH", 7, 23, 6, 0)+struct.pack(">I", 0)
        wire += bytes(4+4)+struct.pack(">I", 0)*3+bytes(1+8+4)+struct.pack(">I", 0)
        stopped = {2: (dict(Instance="voter2/pid2/"+(bytes([2])*16).hex()), {}),
                   3: (dict(Instance="voter3/pid3/"+incarnation.hex()), {})}
        scope.response_incarnation(wire, 3, stopped)
        with self.assertRaises(ValueError):
            scope.response_incarnation(wire, 2, stopped)
        replacement = {**stopped, 3: (dict(Instance="voter3/pid4/"+(bytes([4])*16).hex()), {})}
        with self.assertRaises(ValueError):
            scope.response_incarnation(wire, 3, replacement)

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
