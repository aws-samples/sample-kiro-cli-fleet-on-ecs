# Reporting conventions

How a finding is written down, as opposed to what counts as one. Separate from the other
files because it applies to every audit regardless of which conventions are being checked,
which is the usual reason steering ends up as several files rather than one.

## Every findings file ends with a count line

The last line of the file, with nothing after it:

```
AUDIT-COMPLETE: <number of entries under Findings>
```

Use `0` when there are no findings. The number is the count of entries, not of lines.

This exists so a batch of several hundred results can be checked by a script without
parsing prose. A truncated model response looks like a short report; a report missing its
count line is unambiguously incomplete, and that difference is what makes an automated
completion check possible at all.

## Entries are actionable or they are not entries

- Give the line number, and quote the offending line exactly as it appears.
- Name the convention violated, in the words the convention uses.
- State the smallest change that satisfies it. "Rename this" is not a change; "rename to
  `is_enabled`" is.
- One entry per violation. A single entry covering four occurrences cannot be ticked off.

## Severity is not yours to assign

Do not rank, score, or label findings as major or minor. A convention either applies or it
does not. Ranking invites a reader to skip the bottom of the list, which is where the
cheap fixes are.
