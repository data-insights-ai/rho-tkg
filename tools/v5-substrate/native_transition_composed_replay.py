#!/usr/bin/env python3
"""Finite recorded two-conflict/fence/real-transfer replay on fresh native cohorts.

No binary is built. Every trial creates actual child owners and original
factories; optional-action deletion cannot erase the mandatory causal body.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("native_composed_runner", Path(__file__).with_name("native_transition_replay.py"))
R = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(R)
C = R.components("composed")


def seeded_recipe(seed):
    C.integer(seed, minimum=1)
    state = seed
    def next_():
        nonlocal state
        state ^= (state << 13) & ((1 << 64)-1)
        state ^= state >> 7
        state ^= (state << 17) & ((1 << 64)-1)
        return state
    choices = [(first, phase, fence) for first in ("A", "B") for phase in ("registered", "prepared")
               for fence in (False, True) if first == "A" or fence]
    first, phase, fence = choices[next_() % len(choices)]
    stages = ("held", "settled", "transferred")
    actions = [dict(At=stages[next_()%3], Kind="tick", Original="", Member=1+next_()%6, Ticks=1+next_()%3) for _ in range(2)]
    actions += [dict(At=stage, Kind="duplicate", Original=("A", "B")[next_()%2], Member=0, Ticks=0) for stage in ("settled", "transferred")]
    actions.sort(key=lambda action: C.STAGES[action["At"]])
    value = dict(Version=1, Seed=seed, First=first, Phase=phase, FenceFirst=fence, Actions=actions)
    C.recipe(value)
    return value


def campaign(seeds):
    R.require(type(seeds) is list and 1<=len(seeds)<=2,"one or two explicit recorded campaign seeds")
    recorded=[seeded_recipe(seed) for seed in seeds]
    enumeration=[dict(Version=1,Seed=seeds[0],First=first,Phase=phase,FenceFirst=fence,Actions=[])
                 for first in ("A","B") for phase in ("registered","prepared") for fence in (True,False)
                 if first=="A" or fence]
    return R.campaign_admission(dict(Version=1,Enumeration=enumeration,Recorded=recorded),C)


def soak_policy(seeds, max_trials, min_rounds, min_active_seconds):
    """Record every deterministic recipe before releasing any actual cohort."""
    campaign(seeds)
    R.require(type(max_trials) is int and 1<=max_trials<=R.MAX_SOAK_TRIALS,"finite explicit soak trial ceiling")
    recorded=[]
    for ordinal in range(max(0,(max_trials-6-R.SOAK_FAILURE_TRIALS)//2)):
        seed=seeds[ordinal%len(seeds)]
        generation=ordinal//len(seeds)
        if generation:
            wire=f"native-composed-soak-v1/{seed}/{generation}".encode("ascii")
            seed=int.from_bytes(hashlib.sha256(wire).digest()[:8],"big") or 1
        recorded.append(seeded_recipe(seed))
    return R.soak_admission(dict(Version=1,MaxTrials=max_trials,MinRounds=min_rounds,
                                MinActiveSeconds=min_active_seconds,Recorded=recorded),C)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--watchdog", type=Path, default=R.WATCHDOG)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--seconds", required=True, type=int)
    parser.add_argument("--memory-mib", required=True, type=int)
    selection=parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--seed",type=int,help="unchanged single recorded replay")
    selection.add_argument("--seeds",type=int,nargs="+",help="six genuine causal orders, then one or two recorded seeds under ONE trial/deadline pool")
    parser.add_argument("--deadline-unix-nanos", type=int)
    parser.add_argument("--soak",action="store_true",help="explicit sustained fresh-cohort component; no long-lived-store or whole-D claim")
    parser.add_argument("--max-trials",type=int,help="soak-only cumulative trial grant, at most64 including enumeration/replay/deletion")
    parser.add_argument("--min-rounds",type=int,help="soak-only required successful recorded recipe + exact fresh replay pairs")
    parser.add_argument("--min-active-seconds",type=int,help="soak-only required active recorded execution; no idle padding")
    args = parser.parse_args()
    limits=(args.max_trials,args.min_rounds,args.min_active_seconds)
    if args.soak and (args.seeds is None or any(value is None for value in limits)):
        parser.error("--soak requires --seeds and all three explicit soak resource/minimum options")
    if not args.soak and any(value is not None for value in limits):
        parser.error("soak-only resource/minimum options require --soak")
    exploration=None if args.seeds is None else campaign(args.seeds)
    soak=None if not args.soak else soak_policy(args.seeds,*limits)
    value=seeded_recipe(args.seed) if exploration is None else exploration["Recorded"][0]
    result = R.execute(args.output, args.binary, args.watchdog, args.seconds, args.memory_mib, value,
                       profile="composed", deadline_unix_nanos=args.deadline_unix_nanos,
                       **({"campaign":exploration} if exploration is not None else {}),
                       **({"soak":soak} if soak is not None else {}))
    print(json.dumps(result, indent=2))
    if result["classification"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
