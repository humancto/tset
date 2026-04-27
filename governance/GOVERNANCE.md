# Governance

TSET is operated as an open-source project with the explicit goal of moving
to a neutral foundation (LF AI & Data is the Gate 3 target — see RFC §8.4).

## Current state

While the project is small (Gate 1 / Gate 2), governance is intentionally
informal:

- A small group of **maintainers** approve code and spec changes.
- All non-trivial changes go through the [RFC process](RFC_PROCESS.md).
- Spec-affecting RFCs additionally require sign-off from at least one
  maintainer who is not the author.

## Decision making

We default to **lazy consensus**: a proposal is accepted if no maintainer
objects within 7 days. Objections must be substantive (technical, legal, or
process); aesthetic preferences are not blocking.

For decisions where consensus cannot be reached:

1. The author proposes a path forward in the RFC thread.
2. Maintainers vote +1 / 0 / -1 with reasoning.
3. A simple majority decides; ties go to the proposal that preserves the
   format's invariants (RFC §5.6).

## Adding maintainers

A new maintainer is nominated by an existing maintainer and confirmed by lazy
consensus over 14 days. Nominees should have:

- A pattern of substantive contributions over at least 3 months.
- Demonstrated understanding of the binary-format invariants.
- A stated commitment to act in the project's interest, not their employer's.

## Removing maintainers

A maintainer who has been inactive for 6 months may be moved to emeritus
status. A maintainer who acts in bad faith may be removed by majority vote of
the remaining maintainers.

## Spec changes

Spec changes (anything that affects the binary layout, manifest schema, or
[`SPEC.md` §7 conformance obligations](../SPEC.md)) require:

1. An RFC under `spec/` with the change rationale.
2. A test in `python/tests/` demonstrating the change.
3. A version bump per `SPEC.md` §8.
4. Sign-off from two maintainers, one of whom must be familiar with the SMT
   / signing layer if the change touches §5.5.

## Conflicts of interest

Maintainers must disclose employer affiliations and any commercial interest
in TSET. Decisions that would materially benefit a maintainer's employer
require recusal of the affected maintainer from the vote.

## Code of conduct enforcement

See [`CODE_OF_CONDUCT.md`](../CODE_OF_CONDUCT.md). Reports go to any two
maintainers. Investigations follow the Contributor Covenant enforcement
ladder.

## Foundation transition (Gate 3)

The intended end-state is donating the project, the trademark, and the spec
to a neutral foundation (LF AI & Data preferred). Until then the maintainers
hold the marks in trust for the community.
