#!/usr/bin/env python3
"""Compact codec/selector refusals; these bytes never claim native authority."""
import base64
import hashlib
import unittest
import native_transition_certificate_check as cert


def cut_result(index, round_=7, phase=2, status=1):
    wire = b"GSO\1" + bytes((phase, status)) + round_.to_bytes(8, "big") + index.to_bytes(8, "big")
    return base64.b64encode(wire + hashlib.sha256(wire).digest()).decode()


class FirstCertificateGuards(unittest.TestCase):
    def test_four_actual_first_certify_sides_exclude_fence_and_foreign_participant(self):
        accepted = {cert.recipe(dict(Version=1, Phase=2, Partition=p, Side=s))
                    for p in (3, 8) for s in ("before-submit", "after-applied")}
        self.assertEqual(len(accepted), 4)
        valid = dict(Version=1, Phase=2, Partition=3, Side="before-submit")
        for update in ({"Phase": 1}, {"Partition": 4}, {"Side": "after-submit"}, {"Version": True}, {"Page": 0}):
            with self.subTest(update=update), self.assertRaises(ValueError):
                cert.recipe({**valid, **update})

    def test_repeated_reply_index_does_not_replace_missing_or_earlier_publication(self):
        records = {(3, n): dict(Outcome=cut_result(n), CDC=None) for n in (17, 21)}
        self.assertEqual(cert.first_publication(records, 3, 7), (17, [17, 21]))
        self.assertEqual(cert.stored_publication(dict(partition=3, round=7, index=17), records), [17, 21])
        with self.assertRaises(ValueError):
            cert.stored_publication(dict(partition=3, round=7, index=21), records)
        for participant, round_ in ((8, 7), (3, 8)):
            with self.assertRaises(ValueError):
                cert.first_publication(records, participant, round_)
        records[3, 17]["Outcome"] = cut_result(21)
        with self.assertRaises(ValueError):
            cert.first_publication(records, 3, 7)
        for status in (0, 2, 3, 4):
            with self.subTest(status=status), self.assertRaises(ValueError):
                cert.control(cut_result(17, status=status))

    def test_unknown_generic_or_invalid_failure_cannot_qualify_interruption(self):
        valid = dict(code=3, mask=1 << 7, aux=b"", image=b"", outcome=b"", changes=b"")
        cert.interrupted(valid, 1 << 3)
        for response, mask in (({**valid, "code": 1}, 1 << 3), ({**valid, "code": 2}, 1 << 3),
                               ({**valid, "code": 4}, 1 << 3), ({**valid, "code": 6}, 0),
                               ({**valid, "mask": 1 << 4}, 1 << 3), (valid, (1 << 3) | (1 << 2)),
                               ({**valid, "aux": b"caller-created-cut"}, 1 << 3)):
            with self.subTest(response=response, mask=mask), self.assertRaises(ValueError):
                cert.interrupted(response, mask)
        for data in ({}, dict(Version=1, Profile=cert.PROFILE), dict.fromkeys(cert.HEADER)):
            with self.assertRaises(ValueError):
                cert.check(data)


if __name__ == "__main__":
    unittest.main()
