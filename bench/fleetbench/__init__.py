"""Reproduction benchmarks for the Kiro CLI subscription-pool fleet.

Only M1 (the per-subscription throttling curve) lives here today, because it is the
one measurement that needs a single key and no deployed fleet. M2~M5 need the stack
and >=5 subscriptions and are tracked separately; when they land they extend this
same ramp core rather than replacing it.
"""

from __future__ import annotations

from fleetbench.profiles import HEAVY, LIGHT, PROFILES, Profile, resolve
from fleetbench.ramp import (
    LevelResult,
    RampResult,
    RunOnce,
    StreamOutcome,
    StreamResult,
    default_schedule,
    run_level,
    run_ramp,
)

__all__ = [
    "HEAVY",
    "LIGHT",
    "PROFILES",
    "LevelResult",
    "Profile",
    "RampResult",
    "RunOnce",
    "StreamOutcome",
    "StreamResult",
    "default_schedule",
    "resolve",
    "run_level",
    "run_ramp",
]
