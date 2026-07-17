# Upgrading SalamanderDB

SalamanderDB follows semantic versioning, but the project is pre-1.0. Minor
releases may therefore contain source-level API changes. The append-only log is
the durable source of truth; catalogs, indexes, projections, snapshots, and
sidecars are derived and may be rebuilt.

## Before upgrading

1. Stop the process that owns the database and close its handle cleanly.
2. Copy the entire database directory to a safe location. A partial copy of a
   live directory is not a supported backup procedure.
3. Read the release notes between the installed and target versions.
4. Test opening and replaying a copied production database with the target
   version before upgrading the original.
5. Run application-level projection and replay checks, not only an open/close
   smoke test.

Do not edit manifests, log segments, catalogs, snapshots, or sidecars by hand.
If an upgrade requires an offline migration, use the release's documented CLI
command and retain the source directory until verification completes.

## Compatibility policy

- Format v2 is the current on-disk format and has checked-in golden fixtures.
- A release must reject an unsupported format explicitly rather than silently
  reinterpret it.
- Payload schema evolution belongs to the application. Keep old event variants
  readable or introduce a deterministic upcasting layer in application code.
- Deleting derived state may make the next query slower, but must not change its
  answer. Deleting log segments destroys durable history and is unsupported.
- Downgrades are not assumed safe unless the destination release explicitly
  documents support for the newer format and metadata it will encounter.

## Python and Rust together

The Python extension delegates storage semantics to the Rust engine facade.
Keep the Python distribution and embedded Rust engine at the same released
version; wheels already package the matching engine. Source builds should use
the dependency versions recorded by the release rather than mixing arbitrary
workspace revisions.

## Current operational boundary

Version 0.1 retains the complete log forever. It does not yet provide online
compaction, selective erasure, or a supported live-backup command. Applications
that require those properties should defer production adoption or provide an
external, tested stop-and-copy backup procedure until the retention contract and
its operational tooling are implemented.
