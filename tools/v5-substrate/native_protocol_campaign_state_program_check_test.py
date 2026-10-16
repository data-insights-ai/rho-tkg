#!/usr/bin/env python3
"""Inert codec and routing guards, not native election/process evidence."""
import base64
import copy
import hashlib
import struct
import unittest
from unittest import mock

import native_protocol_campaign_state_program_check as W
from native_protocol_campaign_state_check_test import catchup_sample, native_response


def encoded(value):
    return base64.b64encode(value).decode()


def sealed(value):
    return value+hashlib.sha256(value).digest()


def field(value):
    return struct.pack(">I", len(value))+value


class StateProgramChecks(unittest.TestCase):
    def refused(self, fn, *args, **kwargs):
        with self.assertRaises(ValueError):
            fn(*args, **kwargs)

    def test_native_catchup_reuses_common_join_with_actual_program_cohort(self):
        a,restored,raw = catchup_sample()
        leader = W.R.response(raw["Catchup"]["LeaderState"])
        election = native_response(b"",leader["image"],incarnation=leader["incarnation"],stats=leader["stats"],sequence=8,applied=7)
        a["Program"] = dict(Cohort=[dict(Member=m,Instance=f"voter{m}/pid{100+m}/"+(bytes([m])*16).hex()) for m in range(1,7)],
                            Program=dict(Elections=[dict(State=election),{}]))
        roles = dict(leaders=(1,4),target=3)
        self.assertTrue(W.C.replacement_catchup(a,restored,roles))
        for change in (lambda x:x["Program"]["Cohort"][0].update(Instance="foreign-process"),
                       lambda x:x["Program"]["Program"]["Elections"][0].update(State=native_response(b"",leader["image"],stats=leader["stats"],applied=7)),
                       lambda x:x["Program"]["Program"]["Elections"][0].update(State=raw["Catchup"]["LeaderState"])):
            bad = copy.deepcopy(a);change(bad)
            self.refused(W.C.replacement_catchup,bad,restored,roles)

    def test_actual_role_shape_never_renumbers_native_members(self):
        for leaders, targets in (((1, 4), (3, 6)), ((2, 6), (1, 4))):
            for part, target in zip((3, 8), targets):
                recipe = dict(Family="move", Partition=part)
                roles = dict(leaders=leaders, target=target)
                self.assertEqual(W.C.program_state_roles(roles, recipe), (leaders, target))
                for invalid in ({**roles, "leaders": (True, 4)}, {**roles, "leaders": (2, 6.0)},
                                {**roles, "leaders": list(leaders)}, {**roles, "target": True},
                                {**roles, "target": leaders[0 if part == 3 else 1]},
                                {**roles, "renamedMember": target}):
                    self.refused(W.C.program_state_roles, invalid, recipe)
                self.refused(W.C.program_state_roles, roles, dict(Family="allocation", Partition=part))

    def test_fresh_profile_needs_separately_authenticated_context(self):
        header = {name: None for name in W.C.HEADER}
        header.update(Version=3, Profile=W.C.PROGRAM_PROFILE, Recipe=dict(Family="move", Partition=3), Pooled={}, Program={})
        self.refused(W.C.state_profile, header)
        self.assertEqual(W.C.state_profile(header, dict(leaders=(2, 6), target=1)), (W.C.POOLED_PROFILE, W.C.POOLED_SEMANTIC))
        self.refused(W.authenticate, header)  # Shape does not create actual owner/election evidence.
        self.refused(W.C.state_profile, {**header, "Version": 2}, dict(leaders=(2, 6), target=1))
        self.refused(W.C.state_profile, {**header, "Profile": W.C.POOLED_PROFILE}, dict(leaders=(2, 6), target=1))

    def test_collector_limit_is_exact_process_encoding(self):
        for mask in (1 << 2, 1 << 5, (1 << 2) | (1 << 5)):
            W.collector_limit(dict(code=2, mask=mask))
        for response in (dict(code=0, mask=1 << 2), dict(code=2, mask=0), dict(code=2, mask=1 << 3),
                         dict(code=2, mask=1 << 7), dict(code=2, mask=(1 << 2) | (1 << 14)),
                         dict(code=2, mask=(1 << 5) | (1 << 4)), dict(code=True, mask=1 << 2)):
            self.refused(W.collector_limit, response)
        for code in (1, 3, 4, 5, 6):
            self.refused(W.collector_limit, dict(code=code, mask=1 << 2))

    def test_native_packet_keeps_actual_scope_and_envelope(self):
        scope = bytes(range(120))
        message = bytes((16, 3, 24, 2, 32, 7))
        wire = b"RP\1\0"+scope[:40]+scope[88:]+struct.pack(">I", len(message))+message
        row = dict(From=2, To=3, Snapshot=False, Payload=encoded(wire))
        self.assertEqual(W.K.frame(row, {3: scope})[0], wire)
        self.refused(W.K.frame, {**row, "From": 1}, {3: scope})
        self.refused(W.K.frame, row, {3: scope[:-1]+b"\0"})

    def test_publication_predecessor_requires_exact_stored_original_and_cdc(self):
        # Minimal inert format vectors test the owning join only. No call to
        # authenticate/check can accept these as native process evidence.
        u = lambda n: struct.pack(">Q", n)
        request, scope = b"r"*16, b"s"*120
        prefix = b"\3"+W.P.GRAPH+u(3)+request
        command = sealed(b"PGQ\2"+prefix+b"c"*32+b"d"*32+b"t"*32+field(sealed(b"PGO\1"+prefix)))
        index = 9
        outcome = sealed(b"GRO\4"+prefix+hashlib.sha256(command).digest()+u(index)+b"\1\0\0")
        evidence = dict(Window="new-root-predecessor", Transport="local-propose", BusinessOriginal=None,
                        MessageCommand=encoded(command), MessageRequest=encoded(sealed(b"GSC\1"+field(sealed(b"APS\1"+scope))+field(command))),
                        MessageReply="reply", LocalRecord="record")
        reply = dict(code=0, mask=0, incarnation=b"i"*16, sequence=4)
        record = dict(reply, sequence=6, outcome=outcome, changes=b"real-cdc-vector")
        rows = {(3, index): dict(Outcome=encoded(outcome), CDC=encoded(record["changes"]))}
        def response(value):
            return reply if value == "reply" else record
        with mock.patch.object(W.R, "response", side_effect=response):
            self.assertEqual(W.source_window(dict(Recipe=dict(Family="publication-race")), evidence, {3: scope}, (2, 6), rows,
                                             [dict(incarnation=b"i"*16, sequence=2)]), command)
            foreign = copy.deepcopy(rows); foreign[3, index]["Outcome"] = encoded(b"foreign-original")
            self.refused(W.source_window, dict(Recipe=dict(Family="publication-race")), evidence, {3: scope}, (2, 6), foreign,
                         [dict(incarnation=b"i"*16, sequence=2)])
            self.refused(W.source_window, dict(Recipe=dict(Family="publication-race")), {**evidence, "Transport": "native-batch"},
                         {3: scope}, (2, 6), rows, [dict(incarnation=b"i"*16, sequence=2)])
            record["changes"] = b""
            self.refused(W.source_window, dict(Recipe=dict(Family="publication-race")), evidence, {3: scope}, (2, 6), rows,
                         [dict(incarnation=b"i"*16, sequence=2)])

    def test_local_gc_requires_actual_same_k_prepare_source_and_original(self):
        # Inert sealed codec vectors exercise the exact source/raw join. They
        # contain no elected process or native Sync receipt and cannot qualify
        # authenticate(), which is the only caller allowed to route C.check.
        u = lambda n: struct.pack(">Q", n)
        scopes = {3: b"a"*120, 8: b"b"*120}
        prefix = b"\3"+W.P.GRAPH+u(8)+b"r"*16
        original = sealed(b"PGQ\2"+prefix+b"c"*32+b"d"*32+b"t"*32+field(sealed(b"PGO\1"+prefix+b"body-tail")))
        tx = b"x"*16
        manifest = sealed(b"GCM\1"+tx+u(8)+field(original))
        digest = hashlib.sha256(manifest).digest()
        def exchange(part, phase, voter, index, flags):
            command = sealed(b"GCT\1"+bytes((phase,))+W.P.GRAPH+u(part)+field(manifest)+struct.pack(">I", 0))
            proposal = sealed(b"GSC\1"+field(sealed(b"APS\1"+scopes[part]))+field(command))
            request = sealed(b"GSQ\1"+b"\10"+u(1)+b"n"*16+bytes(48)+field(b"")*3+field(proposal)+bytes(17))
            status = sealed(b"GCS\1"+W.P.GRAPH+u(part)+tx+digest+bytes((phase,))+u(index)+u(1)+u(1)+bytes((flags, 0)))
            reply = sealed(b"GSR\1"+field(sealed(b"APS\1"+scopes[part]))+u(voter)+b"i"*16+b"\10"+u(1)+b"n"*16+bytes(2)+field(status))
            return command, request, reply, status
        command, request, reply, status = exchange(3, 2, 2, 9, 1)
        _, lost_request, lost_reply, _ = exchange(8, 3, 6, 10, 3)
        a = dict(Recipe=dict(Family="gc"), LostRequest=encoded(lost_request), LostReply=encoded(lost_reply))
        evidence = dict(Window="committed-K-predecessor", Transport="source-submit", MessageCommand=encoded(command),
                        MessageRequest=encoded(request), MessageReply=encoded(reply), BusinessOriginal=encoded(original), LocalRecord=None)
        rows = {(3, 9): dict(Outcome=encoded(status), CDC="")}
        self.assertEqual(W.source_window(a, evidence, scopes, (2, 6), rows, []), command)
        self.refused(W.source_window, a, evidence, scopes, (1, 4), rows, [])
        self.refused(W.source_window, a, {**evidence, "MessageCommand": encoded(b"foreign-command")}, scopes, (2, 6), rows, [])
        self.refused(W.source_window, a, evidence, scopes, (2, 6), {(3, 9): dict(Outcome=encoded(status), CDC=encoded(b"unexpected-cdc"))}, [])


if __name__ == "__main__":
    unittest.main()
