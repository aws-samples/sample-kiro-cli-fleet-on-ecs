Read every file under `./input` and write one note per file into `./output`.

For each input file `./input/<path>`, write `./output/<path>.md` containing exactly these sections, in
this order, with these headings:

```
# <file name>

## Purpose
One or two sentences on what this file is for.

## Interface
The functions, types, or entry points another file would use from here. If there are none, write "None".

## Dependencies
What this file relies on: imports, includes, external calls. If there are none, write "None".

## Notes
Anything a reader should know before changing it: assumptions, side effects, error handling. If there is
nothing worth saying, write "None".
```

Rules:

- Write only under `./output`. Do not create or modify anything else, including temporary files.
- One output file per input file. Do not combine several inputs into one note.
- Describe only what the file actually contains. If something is unclear, say so in Notes rather than
  guessing; a note that invents an interface is worse than one that admits uncertainty.
- Keep each section short. This is a reading aid, not documentation.

When you have written every note, reply with one line naming how many files you processed.
