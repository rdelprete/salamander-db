# Contributing to SalamanderDB

Thanks for your interest. Start with [README.md](README.md) for what the
engine is and how to run it, [CHANGELOG.md](CHANGELOG.md) for the feature
inventory, and [ROADMAP.md](ROADMAP.md) for what's planned, what's
demand-driven, and what is permanently out of scope (please don't open
multi-writer PRs). The `rustdoc` on the public API is the reference for how
each piece behaves.

## Ground rules

These are the invariants the engine is built on; a change that breaks one is
wrong even if it compiles and passes tests:

1. The append-only log is the only durable truth. Catalogs, indexes,
   projections, snapshots, and sidecars are derived: they must be
   reconstructable from the log, and their loss or corruption may change
   performance, never answers.
2. The storage layer may interpret the engine envelope but never
   application payload bytes.
3. No correctness rule may exist only in a comment — tests defend it.
   Failure and recovery paths get tested, not just happy paths.
4. If an on-disk representation changes, check in compatibility fixtures.
5. Breaking API changes are acceptable pre-1.0, but must update
   README/examples/CHANGELOG in the same change.

## Before you open a PR

All of these must be green — CI enforces them:

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

Changes to the log, recovery, batches, snapshots, or healing must also pass
the crash harness (`.github/workflows/crash.yml` runs it; locally:
`cargo run -p salamander-demo -- crashtest parent`).

For Python-facing changes, build the extension with maturin and run the
suite under `salamander-py/tests/`.

## Writing style for code

Match the codebase: comments state constraints the code can't express, not
narration. Public items carry rustdoc that explains behavior and invariants,
not just types.

## Bugs and proposals

- **Bug reports:** a minimal reproduction wins. For durability/corruption
  issues, include OS, filesystem, and the exact crash timing if known — and
  see [SECURITY.md](SECURITY.md) if the issue has security implications.
- **Feature proposals:** check ROADMAP.md first. Items listed as
  demand-driven move up fastest with benchmarks or a concrete workload
  attached; items listed as permanent non-goals won't be accepted.
