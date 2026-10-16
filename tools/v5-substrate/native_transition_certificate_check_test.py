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
    def test_actual_p01_declared_root_and_complete_native_field_validation(self):
        # Actual Old GSC from the complete rotated/reorder P01 process run
        # p01-mode1-phase2-p3-k2-after (artifact SHA256 b594edb4296eb01beeded189
        # b1215adde0bc00d5a6ef7111ac564c0b2dfa7723). This codec guard alone
        # grants no stored-certificate/process authority; the full oracle does.
        wire = base64.b64decode(
            "R1NDAQEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAwQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAJ5YC6nkH1VeCJ9gWUrGypZl0GVgWG9BK0C2EVZGWuqsJBIlGt6z/F1VDg6BAEAJP0UI1Pgtd0/tLFH8VjXQSDxbmxsd6FADuA9DY875OJZY71rgP6GlHHOCs6i03bF9QoAAAAAAAAAAQAAAAAAAAAFAAAAAAAAAAEAAAAAAAAABJHUgMFAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAIAAAAAAAAABAAAAAAAAAAGzosFnQBbJLcl5Skqijat7fVTcJQU6gONy3MLKE0DDmAAAAAAAAAACQAAAAAAAAABAAAAAAAAAAPlgLqeQfVV4In2BZSsbKlmXQZWBYb0ErQLYRVkZa6qwmEwkX1V6CHgKjhSUn0xvu6OE1RIhE6Ah5+atOm0s+N3o9iwADwXo8+OlqfDZpw5H4Slclz8EXcnESAmOhh+C+g=",
            validate=True)
        encode = lambda value: base64.b64encode(value).decode()
        actual = cert.certificate(encode(wire))
        self.assertEqual((actual["partition"], actual["image"][:4]), (3, b"GR\3\5"))

        def changed(offset, value, *, seal_root=True, seal_certificate=True):
            body = bytearray(wire[:-32])
            body[offset:offset+len(value)] = value
            if seal_root:
                body[-32:] = hashlib.sha256(body[188:-32]).digest()
            return encode(bytes(body) + (hashlib.sha256(body).digest() if seal_certificate else wire[-32:]))

        def root_changed(offset, value, **kwargs):
            return changed(188+offset, value, **kwargs)

        # Root.validate: declared pending/published have no graph index;
        # initialized has index3. Primitive/single modes are different layouts.
        for mode in (3, 4):
            body = bytearray(wire[:-32])
            body[191], body[288:296] = mode, bytes(8)
            body[-32:] = hashlib.sha256(body[188:-32]).digest()
            cert.certificate(encode(bytes(body)+hashlib.sha256(body).digest()))
        invalid = [root_changed(3, bytes((mode,))) for mode in (0, 1, 2, 6, 255)]
        invalid += [root_changed(0, b"GR\1"), root_changed(0, b"GR\2"),
                    root_changed(4, b"\2"), root_changed(20, (8).to_bytes(8, "big")),
                    root_changed(28, bytes(8)), root_changed(28, (3).to_bytes(8, "big")),
                    root_changed(44, bytes(8)), root_changed(52, bytes(32)),
                    root_changed(84, bytes(8)), root_changed(84, (10).to_bytes(8, "big")),
                    root_changed(92, bytes(8)), root_changed(92, (2).to_bytes(8, "big")),
                    root_changed(100, bytes(8)), root_changed(108, bytes(32)),
                    root_changed(108, b"\1"*32), root_changed(3, b"\3"),
                    root_changed(44, (7).to_bytes(8, "big"), seal_root=False),
                    root_changed(44, (7).to_bytes(8, "big"), seal_certificate=False),
                    encode(wire[:-1]), encode(wire+b"\0")]
        # An otherwise valid root does not excuse a malformed native scope.
        invalid += [changed(offset, bytes(size)) for offset, size in
                    ((28, 16), (44, 8), (52, 8), (60, 32), (92, 32))]
        for encoded in invalid:
            with self.subTest(encoded=encoded), self.assertRaises(ValueError):
                cert.certificate(encoded)

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
