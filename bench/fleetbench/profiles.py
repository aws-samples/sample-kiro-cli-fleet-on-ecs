"""The two stream shapes M1 contrasts: heavy and light.

The measurement plan asks for both because the throttling limit is about streaming
connections held open, and the two shapes hold them differently. A heavy stream
asks for a long generation and keeps its connection open for the duration; a light
stream asks for a one-line answer and releases almost at once. Comparing where each
one starts throttling shows whether the cap the fleet enforces (5 in-flight per
subscription) is a safe margin under the demanding shape as well as the easy one.

The prompts are deliberately generic and self-contained -- no repository, no
customer workload, nothing to generalise away before publication. They exist to
occupy a connection for a characteristic length of time, not to produce a useful
artifact, so M1 does not trust a tool and does not require output.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Profile:
    name: str
    prompt: str
    trust_tools: str = ""
    """Passed straight to `--trust-tools`. Empty trusts nothing, which is right for
    M1: a connection that opens, streams, and closes is the whole experiment, and a
    tool call would only release the connection early and muddy what is being
    measured."""


HEAVY = Profile(
    name="heavy",
    prompt=(
        "Write a thorough, self-contained explanation of how a work queue with "
        "visibility timeouts prevents two consumers from processing the same message. "
        "Cover the receive-make-invisible step, the delete-on-success step, the "
        "redelivery-on-timeout step, and the dead-letter path. Explain each step in "
        "full prose with a worked example, and do not stop early."
    ),
    trust_tools="",
)
"""A long generation. Holds its streaming connection open for the bulk of the run,
which is the condition that reaches a concurrent-connection limit."""


LIGHT = Profile(
    name="light",
    prompt="Reply with exactly the word: ok",
    trust_tools="",
)
"""A near-instant answer. Opens and releases its connection quickly, so a given
concurrency keeps fewer connections open at any instant than the heavy shape does."""


PROFILES: dict[str, Profile] = {HEAVY.name: HEAVY, LIGHT.name: LIGHT}


def resolve(names: list[str]) -> list[Profile]:
    """Map profile names to profiles, failing loudly on an unknown one.

    A typo should stop the run before it spends credits, not silently measure fewer
    shapes than asked for.
    """
    unknown = [n for n in names if n not in PROFILES]
    if unknown:
        raise ValueError(
            f"unknown profile(s): {', '.join(unknown)}; known: {', '.join(PROFILES)}"
        )
    return [PROFILES[n] for n in names]
