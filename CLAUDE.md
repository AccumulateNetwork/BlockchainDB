# Working in this repository

The specification is `docs/SPEC.md`, and it is the authority.

- **Every issue is evaluated against the spec.**  Triage of a bug
  report or a feature request starts by naming the invariant in
  Section 1 it touches; if it touches none, question whether it is
  work worth doing.
- A change that violates Section 1 (Architecture) is a bug even if
  every test passes.
- When code and the spec disagree, fix one of them in the same
  review — never neither.  Section 2.10's deviation register lists
  the known, deliberate gaps; a PR that closes or adds a deviation
  updates that table.
- Design intent goes in the spec, not only in code comments or issue
  threads.  A code comment describes what the code does; the spec
  says what it must do.

Practices the spec encodes that bite most often:

- No environment-variable configuration; invalid configuration fails
  loudly (Section 1.10).
- No protocol-path cost that degrades toward linear in the age of the
  store (Section 1.2).
- Never unlink what a durable manifest names; never publish over an
  existing segment file (Sections 1.7, 1.8).
