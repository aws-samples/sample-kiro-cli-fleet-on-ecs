"""Confining a workflow's writes to where they belong.

The contract says a workflow writes only under `./output`. Enforced by comparing the
workspace before and after the run, rather than by a hook in the CLI's configuration.

Two reasons for doing it here. The hook mechanism is configured through the agent file,
whose schema cannot be validated without a credential. And a check the model cannot see is
a check the model cannot reason its way around: this one runs after the process has
exited, on the filesystem, so nothing the model does can suppress it.

It detects rather than prevents, which is weaker than a hook would be, and the difference
matters: a stray write has already happened by the time it is found. What the check
guarantees is that the run is not treated as successful and its results are not stored, so
a workflow that writes outside its lane cannot have its output believed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

logger = logging.getLogger(__name__)

# Excluded from the comparison: the output directory is where writes are meant to go, and
# the run directory is the worker's own scratch, not the workflow's.
RUN_DIR: Final = ".run"
"""Per-run scratch: the scoped HOME, temporary files, and the transcript.

Named here because the guard has to ignore it and the worker has to find it. As two
literals they could drift, and the symptom would be the guard failing every run for a
directory the fleet itself created.
"""

IGNORED_TOP_LEVEL: Final = frozenset({"output", RUN_DIR})


@dataclass(frozen=True)
class Snapshot:
    """What the workspace looked like, outside the places writes are allowed."""

    entries: dict[str, tuple[int, int]] = field(default_factory=dict)

    @classmethod
    def of(cls, workspace: Path) -> "Snapshot":
        entries: dict[str, tuple[int, int]] = {}
        for path in sorted(workspace.rglob("*")):
            relative = path.relative_to(workspace)
            if relative.parts and relative.parts[0] in IGNORED_TOP_LEVEL:
                continue
            if not path.is_file():
                continue
            stat = path.stat()
            # Size and modification time, not content: hashing a large input tree once per
            # work item would cost more than the check is worth, and a workflow that
            # rewrites a file to the same size and timestamp is not the failure being
            # looked for.
            entries[str(relative)] = (stat.st_size, stat.st_mtime_ns)
        return cls(entries)


@dataclass(frozen=True)
class Violations:
    created: tuple[str, ...] = ()
    modified: tuple[str, ...] = ()
    removed: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return bool(self.created or self.modified or self.removed)

    def describe(self) -> str:
        parts = []
        if self.created:
            parts.append(f"created {list(self.created)}")
        if self.modified:
            parts.append(f"modified {list(self.modified)}")
        if self.removed:
            parts.append(f"removed {list(self.removed)}")
        return "; ".join(parts) or "none"


def compare(before: Snapshot, after: Snapshot) -> Violations:
    """Anything that changed outside the output directory is a violation."""
    created = tuple(sorted(set(after.entries) - set(before.entries)))
    removed = tuple(sorted(set(before.entries) - set(after.entries)))
    modified = tuple(
        sorted(
            path
            for path in set(before.entries) & set(after.entries)
            if before.entries[path] != after.entries[path]
        )
    )
    violations = Violations(created=created, modified=modified, removed=removed)
    if violations:
        logger.error(
            "the workflow wrote outside its output directory: %s", violations.describe()
        )
    return violations
