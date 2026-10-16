#!/usr/bin/env python3
"""Compact inert grammar/routing refusals; no process evidence is invented."""
import base64
import copy
import json
import struct
import unittest

import native_protocol_campaign_program_check as K


def conditional_restart_rows(member):
    # Inert native/process codec fixtures, not a source or election witness.
    old_incarnation, incarnation = b"o"*16, b"n"*16
    old_instance = f"voter{member}/pid101/{old_incarnation.hex()}"
    instance = f"voter{member}/pid202/{incarnation.hex()}"
    volume = f"/inert/cohort/{member}"
    def stopped(event, pid, identity):
        raw = dict(Member=member, PID=pid, Reaped=True, Success=True, Signaled=False,
                   ExitCode=0, Instance=identity, Volume=volume, Signal="clean-exit")
        row = dict(EventID=event, Member=member, Action="stop", Reaped=True,
                   Instance=identity, Volume=volume,
                   Raw=base64.b64encode(json.dumps(raw).encode()).decode())
        return row, raw
    stats = [0]*28
    stats[1:4] = [32, 4, 1 if member <= 3 else 4]
    field = lambda value: struct.pack(">I", len(value))+value
    wire = incarnation+struct.pack(">28Q", *stats)+bytes(32+32+8+4)+field(b"")
    wire += struct.pack(">BQQBH", int(member in (1, 4)), 7, 32, 0, 0)+field(b"")
    wire += bytes(4+4)+field(b"")*3+bytes(1+8+4)+field(b"")
    old, _ = stopped(1, 101, old_instance)
    terminal = stopped(3, 202, instance)
    restored = dict(EventID=2, Member=member, Action="reopen", Ready=True,
                    PreviousInstance=old_instance, Instance=instance, Volume=volume,
                    RecoveryRaw=base64.b64encode(wire).decode())
    return old, restored, terminal


class ProgramChecks(unittest.TestCase):
    def refused(self, fn, *args, **kwargs):
        with self.assertRaises(ValueError):
            fn(*args, **kwargs)

    def test_exact_programs_and_no_nominal_member_mapping(self):
        for layout, expected in (("original", (1, 4)), ("rotated", (2, 6))):
            for message in ("literal", "delay", "reorder", "duplicate"):
                self.assertEqual(K.spec(dict(Version=1, Layout=layout, Message=message)), expected)
        for value in ({}, dict(Version=True, Layout="original", Message="literal"),
                      dict(Version=1, Layout="relabelled", Message="literal"),
                      dict(Version=1, Layout="rotated", Message="pretend-delay")):
            self.refused(K.spec, value)
        self.refused(K.authenticate, dict(Evidence=dict(P01={})))

    def test_conditional_restart_binds_all_incarnations_without_follower_source_authority(self):
        for member in range(1, 7):
            old, restored, terminal = conditional_restart_rows(member)
            K.conditional_restart(member, old, restored, terminal)
            for name, value in (("Member", 7-member), ("PreviousInstance", "foreign"),
                                ("Instance", old["Instance"]), ("Volume", "/foreign"),
                                ("EventID", 3), ("Ready", False)):
                self.refused(K.conditional_restart, member, old, {**restored, name:value}, terminal)
            raw = bytearray(K.P.raw(restored["RecoveryRaw"]))
            for offset, value in ((0, 0), (16+28*8+32+32+8+4+4+1+8+8, 2)):
                changed = raw.copy(); changed[offset] = value
                self.refused(K.conditional_restart, member, old,
                             {**restored, "RecoveryRaw":base64.b64encode(changed).decode()}, terminal)
            if member in (1, 4):
                changed = raw.copy(); changed[16+28*8+32+32+8+4+4] = 0
                self.refused(K.conditional_restart, member, old,
                             {**restored, "RecoveryRaw":base64.b64encode(changed).decode()}, terminal)
            wrong = copy.deepcopy(terminal)
            wrong[1]["PID"] = 101
            self.refused(K.conditional_restart, member, old, restored, wrong)
            old_bad = copy.deepcopy(old)
            raw_stop = json.loads(K.P.raw(old_bad["Raw"])); raw_stop["Success"] = False
            old_bad["Raw"] = base64.b64encode(json.dumps(raw_stop).encode()).decode()
            self.refused(K.conditional_restart, member, old_bad, restored, terminal)

    def test_explicit_scope_context_keeps_actual_member_ids(self):
        # Only shape diagnostics: this does not invoke check/authenticate or
        # create any source, native receipt, election or allowed authority.
        for leaders, targets in (((1, 4), (3, 6)), ((2, 6), (1, 4))):
            for target in targets:
                self.assertEqual(K.Q.program_scope_roles(dict(leaders=leaders, target=target)), (leaders, target))
        for context in (dict(leaders=(2, 6), target=2), dict(leaders=(2, 6), target=5),
                        dict(leaders=(2, 6), target=6), dict(leaders=(1, 6), target=3),
                        dict(leaders=[2, 6], target=1), dict(leaders=(2, 6), target=True),
                        dict(leaders=(2, 6), target=1, renamedMember=3)):
            self.refused(K.Q.program_scope_roles, context)

    def test_real_packet_body_must_match_its_actual_envelope(self):
        # Inert codec bytes, explicitly not a native process/term witness.
        scope = bytes(range(120))
        message = bytes([16, 3, 24, 1, 32, 2])
        payload = b"RP\1\0"+scope[:40]+scope[88:120]+struct.pack(">I", len(message))+message
        row = dict(From=1, To=3, Snapshot=False, Payload=base64.b64encode(payload).decode())
        self.assertEqual(K.frame(row, {3: scope})[0], payload)
        self.refused(K.frame, {**row, "To": 2}, {3: scope})
        self.refused(K.frame, {**row, "From": 4}, {3: scope})
        self.refused(K.frame, {**row, "Snapshot": True}, {3: scope})
        self.refused(K.frame, row, {3: scope[:-1]+bytes([scope[-1] ^ 1])})

    def test_persisted_fence_uses_actual_successor_not_initial_source(self):
        for initial, leaders in (((1, 4), (2, 4)), ((2, 6), (1, 6))):
            for part, target in ((3, initial[0]), (8, next(v for v in range(4, 7) if v != leaders[1]))):
                recipe = dict(Schedule="persisted-fence-before-application", Partition=part)
                roles = dict(initial_leaders=initial, leaders=leaders, target=target)
                self.assertEqual(K.H.program_fence_roles(roles, recipe), (initial, leaders, target))
                self.refused(K.H.program_fence_roles, {**roles, "leaders": initial}, recipe)
                self.refused(K.H.program_fence_roles, {**roles, "target": leaders[0] if part == 3 else leaders[1]}, recipe)

    def test_first_live_roles_keep_both_real_leader_losses_and_caller(self):
        for initial, leaders, target, successor in (((1, 4), (2, 4), 4, 6), ((2, 6), (1, 6), 6, 4)):
            roles = dict(initial_leaders=initial, leaders=leaders, target=target, successor=successor)
            self.assertEqual(K.H.program_live_roles(roles), (initial, leaders, target, successor))
            for bad in ({**roles, "leaders": initial}, {**roles, "target": successor},
                        {**roles, "successor": 5}, {**roles, "initial_leaders": list(initial)},
                        {**roles, "renamedSource": target}):
                self.refused(K.H.program_live_roles, bad)

    def test_durable_decision_roles_preserve_real_caller_and_recoverer(self):
        for initial in ((1, 4), (2, 6)):
            for co in (3, 8):
                leader = initial[0 if co == 3 else 1]
                caller, actor = ((2, 3) if initial == (1, 4) else (3, 1)) if co == 3 else ((5, 6) if initial == (1, 4) else (5, 4))
                for phase in (1, 2, 3, 4):
                    for part in ((co,) if phase in (1, 3) else (3, 8)):
                        current = list(initial)
                        if phase == 4:
                            current[0 if co == 3 else 1] = caller
                        target = caller if phase == 4 and part == co else next(v for v in range(1 if part == 3 else 4, 4 if part == 3 else 7) if v != initial[0 if part == 3 else 1] and v != caller)
                        recipe = dict(Coordinator=co, Phase=phase, Partition=part)
                        roles = dict(initial_leaders=initial, leaders=tuple(current), target=target)
                        recovery = caller if phase != 4 and target == actor else actor
                        self.assertEqual(K.H.program_coordinator_roles(roles, recipe), (initial, leader, caller, recovery, target))
                        self.refused(K.H.program_coordinator_roles, {**roles, "target": actor if phase == 4 and part == co else initial[0 if part == 3 else 1]}, recipe)

    def test_partial_roles_keep_native_leader_and_live_owner_distinct(self):
        for initial, actor in (((1, 4), 6), ((2, 6), 4)):
            self.assertEqual(K.H.C.C.program_leaders(dict(leaders=initial)), initial)
            for phase, part in ((1, 8), (2, 3), (2, 8), (3, 8), (4, 3), (4, 8)):
                current = (initial[0], 5 if phase == 4 and part == 8 else initial[1])
                target = 3 if part == 3 else 5
                recipe = dict(Phase=phase, Partition=part)
                roles = dict(initial_leaders=initial, leaders=current, target=target)
                self.assertEqual(K.H.program_partial_roles(roles, recipe), (initial, actor, target))
                self.refused(K.H.program_partial_roles, {**roles, "target": actor}, recipe)
        for roles in (dict(leaders=[2, 6]), dict(leaders=(1, 6)), dict(leaders=(2, 6), nominalOnly=True)):
            self.refused(K.H.C.C.program_leaders, roles)

    def test_frozen_roles_keep_private_queue_on_actual_current_b_leader(self):
        for leaders in ((1, 4), (2, 6)):
            for original in ("A", "B"):
                for part in (3, 8):
                    actor = 3 if original == "A" else leaders[1]
                    target = (2 if part == 3 else 6) if leaders == (1, 4) else (1 if part == 3 else 4)
                    recipe = dict(Original=original, Partition=part)
                    roles = dict(leaders=leaders, target=target)
                    self.assertEqual(K.H.program_frozen_roles(roles, recipe), (leaders, actor, target))
                    self.refused(K.H.program_frozen_roles, {**roles, "target": leaders[0 if part == 3 else 1]}, recipe)
                    self.refused(K.H.program_frozen_roles, {**roles, "virtualQueueMember": 4}, recipe)

    def test_conditional_sources_switch_only_at_actual_cumulative_boundary(self):
        # Inert selector checks: actual cohort/process receipts are required by
        # authenticate before these optional routes enter the complete oracle.
        for leaders in ((1, 4), (2, 6)):
            for schedule in ("write-skew-A-first", "write-skew-B-first", "empty-range-and-absent-key-insertion", "temporal-life-value-ABA", "authority-fence-before-prepare"):
                boundary = 2 if schedule == "write-skew-A-first" else 0
                for part in (3, 8):
                    actor = 4 if schedule == "write-skew-B-first" else 1
                    target = 3 if part == 3 else 6 if leaders == (1, 4) else 5
                    recipe = dict(Schedule=schedule, Partition=part)
                    roles = dict(leaders=leaders, target=target, reopen_after=boundary)
                    _, actual_actor, _, sources = K.H.program_conditional_roles(roles, recipe)
                    self.assertEqual(actual_actor, actor)
                    for ordinal in range(4):
                        expected = (1, 4) if boundary and ordinal >= boundary else leaders
                        self.assertEqual(K.H.N.program_source_voter(sources, part, ordinal, 4), expected[0 if part == 3 else 1])
                    self.refused(K.H.program_conditional_roles, {**roles, "target": leaders[0 if part == 3 else 1]}, recipe)
        for sources in (dict(leaders=(2, 6), reopen_after=True), dict(leaders=(2, 6), reopen_after=4),
                        dict(leaders=(1, 6), reopen_after=2), dict(leaders=(2, 6), reopen_after=-1)):
            self.refused(K.H.N.program_source_voter, sources, 3, 0, 4)


if __name__ == "__main__":
    unittest.main()
