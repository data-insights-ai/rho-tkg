#!/usr/bin/env python3
"""Compact authored refusal/codec guards; no fabricated native source."""
import base64
import unittest
import native_transition_pin_check as pin


class PinTransitionGuards(unittest.TestCase):
    def test_invalid_limit_corrupt_or_unknown_cannot_qualify_interruption(self):
        for code in (3, 6): pin.interrupted("pending-or-unavailable", code, None)
        for code in (0, 1, 2, 4, 5, True):
            with self.subTest(code=code), self.assertRaises(ValueError): pin.interrupted("pending-or-unavailable", code, None)
            with self.subTest(terminal_code=code), self.assertRaises(ValueError): pin.terminal("released", code, None)
        for result in ("invalid", "limit", "corrupt", "unexpected-error", "applied", "released", "expired"):
            with self.subTest(result=result), self.assertRaises(ValueError): pin.interrupted(result, 6, None)
        for result in ("released", "expired"):
            pin.terminal(result, 6, None)
            with self.assertRaises(ValueError): pin.terminal(result, 6, {})
        with self.assertRaises(ValueError): pin.interrupted("pending-or-unavailable", 6, {})

    def test_feasible_pin_phase_and_publication_inventory(self):
        accepted = []
        for phase in range(1, 9):
            for partition in (3, 8):
                for side in ("before-submit", "after-applied"):
                    value = dict(Version=1, Phase=phase, Partition=partition, Side=side)
                    try: accepted.append(pin.recipe(value))
                    except ValueError: pass
        self.assertEqual(len(accepted), 30)
        self.assertIn((7, 3, "before-submit"), accepted)
        self.assertIn((1, 8, "after-applied"), accepted)
        for update in ({"Phase": 7}, {"Version": True}, {"Side": "after-submit"}, {"Partition": 4}, {"Page": 0}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                pin.recipe({**dict(Version=1, Phase=1, Partition=8, Side="before-submit"), **update})

    def test_issued_published_and_terminal_actions_cannot_be_interchanged(self):
        request, wire = bytes([1])+bytes(15), b"inert diagnostic bytes"
        actions = [pin.action(wire, request, phase) for phase in range(1, 9)]
        self.assertEqual(actions[0], request)
        self.assertEqual(len(set(actions)), 8)
        for raw in (b"", b"GPO\1"+bytes(146), b"GPM\1"+bytes(146)):
            with self.assertRaises(ValueError): pin.outcome(base64.b64encode(raw).decode())
        for data in ({}, dict(Version=1, Profile=pin.PROFILE), dict.fromkeys(pin.HEADER)):
            with self.assertRaises(ValueError): pin.check(data)
        with self.assertRaises(ValueError): pin.T.decoded('{"Original":null,"Original":null}')


if __name__ == "__main__": unittest.main()
