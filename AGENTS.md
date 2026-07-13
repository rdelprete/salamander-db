# Working in this repository

Guidance for humans and AI agents making changes here. Start with
[README.md](README.md) for what the engine is, and [CONTRIBUTING.md](CONTRIBUTING.md)
for the full ground rules; this file is the short version.

## Invariants that outrank passing tests

- The append-only log is the only durable truth. Catalogs, indexes,
  projections, snapshots, and sidecars are derived: reconstructable from the
  log, and their loss or corruption may change performance, never answers.
- The storage layer may interpret the engine envelope but never application
  payload bytes.
- No correctness rule may live only in a comment — a test must defend it.
  Failure and recovery paths get tested, not just happy paths.

## Gates (CI enforces all three)

```
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

Changes to the log, recovery, batches, snapshots, or healing must also pass
the crash harness (`cargo run -p salamander-demo -- crashtest parent`).

## Style

Public items carry rustdoc explaining behavior and invariants. Comments
state constraints the code cannot express, not narration. Match the
surrounding code's idiom. Breaking API changes are fine pre-1.0 but must
update README, examples, and CHANGELOG in the same change.
