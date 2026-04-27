# RFC Process

TSET uses a lightweight RFC process for any change beyond a one-line fix or
documentation tweak. RFCs live as files in `spec/` for spec changes or as
GitHub issues for everything else.

## When to file an RFC

File an RFC for:

- Any change to the binary layout (`SPEC.md`).
- Any change to the manifest schema or conformance obligations.
- New tokenizer support, new metadata column types, new section magics.
- Behavioural changes to the writer/reader that observable consumers may rely
  on (streaming order, batch boundaries, error semantics).
- Any new public Python API on the reference implementation.

You do not need an RFC for:

- Bug fixes that restore documented behaviour.
- Internal refactors with no observable change.
- Documentation polish.

## Template

RFCs should answer, in order:

1. **Summary** — one paragraph.
2. **Motivation** — what problem does this solve, and who has it?
3. **Design** — the proposed change, in enough detail that a third-party
   implementer could build a compliant reader.
4. **Drift implications** — what does this break for existing readers /
   files? How is it versioned (per `SPEC.md` §8)?
5. **Alternatives considered** — and why this one wins.
6. **Open questions** — explicitly, with a default position if not resolved.
7. **Test plan** — what new tests demonstrate correctness?

## Lifecycle

1. **Draft** — author writes the RFC, opens a PR (for spec) or issue (other).
2. **Comment** — minimum 7 days for project members to weigh in. Spec RFCs
   that touch the SMT / signing layer (RFC §5.5) require explicit
   solicitation of cryptography review.
3. **Decision** — lazy consensus per `GOVERNANCE.md`. Either:
   - **Accepted** — author proceeds with implementation.
   - **Postponed** — concrete blockers documented in the RFC; revisit at the
     next gate.
   - **Rejected** — reasons documented; the RFC stays in tree as a record.
4. **Implementation** — code lands referencing the RFC.
5. **Stabilised** — RFC marked as "shipped in vX.Y" once the change is
   released.

## Conventions

- One change per RFC. Bundling unrelated changes makes review harder and
  rollback impossible.
- RFCs are not requests for permission to think about a topic — file one
  when you have a concrete proposal, not before.
- Spec RFCs that affect the binary layout MUST include a section showing the
  change in the canonical "byte-range table" form used in `SPEC.md`.
