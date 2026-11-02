# Naming conventions

These are illustrative. Replace them with your own before running this on anything real —
the point of this example is that the rules travel as files, not that these rules are good.

## Required

- A name says what the thing is, not what type it is. `users`, not `user_list`.
- Booleans read as a claim about the subject: `is_enabled`, `has_expired`. A boolean named
  `flag`, `status`, or `check` says nothing.
- A function that returns a value is named for the value. A function that performs an
  action is named for the action. One function does not do both.
- Abbreviations only where the full word is genuinely unwieldy, and then consistently
  across the file. `config` everywhere or `configuration` everywhere, never both.

## Forbidden

- Single-character names outside a comprehension or a two-line loop body.
- `data`, `info`, `value`, `obj`, `tmp`, `temp`, `result` as the whole of a name. Each says
  only "this is a thing", which the reader already knew.
- Negated booleans: `is_not_ready`, `disable_check`. The reader ends up parsing double
  negatives at every call site.
- Names distinguished only by a trailing number: `handler2`, `parse_v2`.

## Inclusive language

- Never `master`/`slave`. Use `primary`/`replica`, `leader`/`follower`, or
  `controller`/`responder`, whichever the relationship actually is.
- Never `whitelist`/`blacklist`. Use `allowlist`/`denylist`.
