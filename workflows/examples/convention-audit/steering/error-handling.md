# Error handling conventions

Illustrative, like the naming rules. Replace them with the ones your codebase is actually
held to.

## Required

- Catch the specific exception. A bare `except:` or `catch (Exception)` around more than a
  single call hides failures that have nothing to do with the one being handled.
- An error that cannot be handled where it is raised propagates. Logging it and continuing
  with a default produces a result that looks successful and is wrong, which is worse than
  failing.
- Every raised error names what was being attempted and with what. `"not found"` sends the
  reader to a debugger; `"no object at s3://bucket/key"` does not.
- Cleanup that must happen goes in `finally` or a context manager, not after the call that
  might not return.

## Forbidden

- Swallowing an exception with a comment saying it cannot happen. If it cannot happen,
  there is nothing to catch; if it can, the comment is wrong.
- Returning a sentinel (`-1`, `None`, `""`) to signal failure where the language has
  exceptions. Callers forget to check, and the sentinel travels.
- Retrying without a bound, or retrying an error that cannot succeed on a second attempt
  such as a validation failure.
- Putting a credential, key, or token into an error message or a log line.

## Not a violation

- A broad catch at the outermost boundary of a process, where its job is to turn any
  failure into an exit status. Note it under "Not assessed" rather than reporting it.
