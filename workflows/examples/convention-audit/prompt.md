Audit every file under `./input` against the conventions you have been given.

The conventions are not in this prompt. They are the steering files in effect for this
session, and they are the authority: where this prompt and a steering file disagree, the
steering file wins.

For each input file, write `./output/<the file's name>.findings.md` containing exactly
these sections, in this order:

## File

The path of the file you audited, relative to `./input`.

## Findings

One entry per violation, as a list. Each entry gives the line number, the convention it
violates, and the smallest change that would satisfy it. Quote the offending line.

Write `None` if the file violates nothing. Do not write findings for things no convention
covers — an audit that reports opinions is an audit nobody can act on.

## Not assessed

Anything you could not judge, and why. A convention that needs context outside this file
belongs here rather than in Findings.

Write `None` if you assessed everything.

---

Write nothing outside `./output`. Do not modify the files under `./input`; you are reading
them, not fixing them.
