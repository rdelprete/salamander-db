# Retention and compaction contract

Status: **normative; phases 1–6 implemented — physical whole-segment
compaction enabled** (see §12 for the per-phase breakdown)

Implemented:

- backward-compatible manifest floors defaulting to zero;
- typed unavailable errors and floor enforcement across historical APIs;
- floor exposure through Rust, the engine facade, and Python;
- non-destructive, whole-segment-aligned `KeepFrom(position)` planning, with
  blocker reporting for anchors, branches, projections, consumers, readers,
  and feeds;
- verified engine-core, projection, branch, and consumer retention anchors
  with generation recovery;
- atomic floor publication and best-effort whole-closed-segment reclamation,
  defended by the deterministic publication-fault crash matrix;
- exact-position, latest-event, timestamp-cutoff, and retained-byte policy
  selectors resolving through the proven explicit-floor path.

This contract defines the conditions under which SalamanderDB may make old
history unavailable and reclaim its bytes. A database that has never run a
compaction still holds the complete log; after the first physical compaction,
durable truth is one verified retention anchor plus the retained append-only
log suffix (§2).

The words MUST, MUST NOT, SHOULD, and MAY are normative.

## 1. Purpose

Retention bounds how much history remains queryable. Compaction reclaims the
physical bytes made unnecessary by an accepted retention boundary. They are
separate operations:

- Advancing the retention floor changes the set of valid historical answers.
- Compaction changes storage usage, never any answer at or after that floor.
- Retention without compaction is valid.
- Compaction without a previously committed retention floor is forbidden.

The first implementation will support an explicit global position floor. Age,
size, per-stream, and per-branch policies may later choose a proposed floor,
but they must resolve to the same global-position contract before deletion.

## 2. Durable truth after retention

Before retention, the append-only log is the only durable truth. After the
first physical compaction, durable truth becomes:

> one verified retention anchor plus the retained append-only log suffix.

The anchor is not an ordinary snapshot cache. It is the new genesis record for
the retained database generation and is required to interpret the suffix.
Deleting or corrupting every ordinary catalog, index, projection snapshot, and
sidecar still changes performance only. Deleting or corrupting the active
retention anchor is equivalent to corrupting the retained log.

This is the only intentional qualification of the project's original
"log-only truth" invariant. The anchor MUST be:

- immutable and checksummed;
- bound to the database ID and retained generation;
- bound to an exclusive floor and retained suffix identity;
- written and verified before any old segment is removed;
- published atomically with the retained generation manifest;
- preserved with at least one previous complete generation until publication
  is durable.

The storage layer may interpret engine checkpoint fields, but application and
projection checkpoint bytes remain opaque.

## 3. Position semantics

`retention_floor` is the lowest user-event position that may still be read. It
is exclusive history loss: positions below the floor are unavailable, while
the event at the floor, if one exists, remains readable.

The floor MUST:

- be between the current floor and the durable head, inclusive;
- be a committed batch boundary;
- never move backward;
- preserve original global positions, event IDs, batch IDs, stream revisions,
  timestamps, branch IDs, and database ID;
- resolve to a whole-segment deletion boundary for the initial implementation.

A request inside a closed segment therefore rounds down to that segment's base,
retaining more history than requested. Reports MUST expose both the requested
and effective floors. The active segment MUST NOT be partially rewritten by the
initial implementation.

Head and durable-head positions never reset after compaction.

## 4. Answer contract

For every operation scoped entirely to positions at or after the effective
floor, compaction MUST return exactly the answer produced immediately before
physical deletion.

Requests requiring an earlier position MUST fail with a typed
`position_unavailable` result containing:

- the requested position;
- the effective retention floor;
- the durable head;
- whether an applicable bootstrap checkpoint is available.

The engine MUST NOT silently clamp a replay, view, diff, feed, or branch request
to the floor.

Specific rules:

- Replay with `from < floor` fails, even if filters would have discarded all
  earlier records.
- `view_at(n)` fails when `n < floor`.
- A feed continuation or consumer checkpoint below the floor fails and directs
  the consumer to bootstrap.
- Diff succeeds only when its required shared and suffix windows are retained.
  It must not claim an exact divergence that predates the floor.
- Fork creation fails if reconstructing inherited state at the fork point
  requires unavailable history and no compatible branch bootstrap exists.
- Archived branches remain readable only to the extent covered by their anchor
  and retained suffix.

## 5. Required anchor coverage

A retention anchor contains four independently validated sections.

### 5.1 Engine core

The engine-core section MUST be sufficient to reopen and append without
scanning deleted bytes. It includes:

- database and storage-format identity;
- effective floor and head used to create the anchor;
- branch catalog, ancestry, status, and lineage fingerprints;
- stream heads and next revisions per branch;
- event-ID, batch-ID, and idempotency-key conflict state required by the
  advertised retry horizon;
- committed batch-boundary information needed at and after the floor;
- durable consumer checkpoint state;
- all system metadata whose latest value may have appeared before the floor.

Engine state MUST be derived from verified log records before publication. A
corrupt pre-existing derived catalog must never be promoted into an anchor.

### 5.2 Protected projections

Every registered projection declared `retention_protected` MUST have a verified
checkpoint at the effective floor for every required branch and partition.
The descriptor fingerprint, definition identity/version, state codec, partition
scheme, branch lineage, and cursor must all match.

Ordinary projection snapshots remain disposable. A checkpoint becomes
authoritative only when copied into and referenced by a committed retention
anchor. A protected projection whose runtime cannot create and restore such a
checkpoint blocks retention.

Unprotected projections may be discarded by retention. Querying one afterward
returns `bootstrap_required` until the application supplies an accepted
checkpoint at or after the floor or re-registers a definition capable of
building from the retained suffix alone.

### 5.3 Application consumers

An external feed consumer below the proposed floor blocks retention unless one
of these is explicit:

- it has advanced to the floor or beyond;
- it has registered an opaque, checksummed bootstrap checkpoint compatible
  with the floor;
- the operator abandons that consumer by ID.

The engine stores and verifies consumer bootstrap bytes but does not interpret
them.

### 5.4 Branches

Each readable branch must be reconstructable from an anchor plus the retained
suffix. Branches whose ancestry or visible inherited prefix crosses the floor
need branch-scoped bootstrap coverage.

The initial implementation MUST block compaction when any non-archived branch
lacks coverage. Archived branches may be explicitly dropped from retained
history, but that choice must be listed in the plan and requires a separate
operator acknowledgement; archival alone is not permission to delete.

## 6. Idempotency and retry horizon

Retention must not accidentally turn a safe retry into a duplicate append.

The plan declares an `idempotency_horizon`:

- `Forever` retains the conflict fingerprints for every prior event ID, batch
  ID, and idempotency key in the engine anchor.
- `SinceFloor` retains only identities at or after the floor and explicitly
  permits an older retry to be treated as new.

`Forever` is the default. Implementations MUST report the anchor size consumed
by retained conflict fingerprints. Changing to `SinceFloor` is a semantic
change requiring explicit operator acknowledgement.

## 7. Planning API

Retention is a two-step operation.

`plan_retention(request)` is read-only and returns:

- requested and effective floors;
- segments and estimated bytes eligible for deletion;
- anchor sections and checkpoints to create;
- protected and unprotected projections;
- branch coverage;
- consumer positions and bootstrap status;
- idempotency horizon and estimated anchor size;
- blockers and required acknowledgements.

`apply_retention(plan_id)` MUST reject a stale plan if the database generation,
head, branch catalog, projection descriptors, or consumer state changed.
Applying retention requires exclusive maintenance ownership: no open readers,
feeds, or concurrent append calls.

There is no `force` option that bypasses blockers. Operators resolve or
explicitly abandon named resources and request a new plan.

## 8. Publication and deletion protocol

The initial implementation uses generation replacement rather than deleting
files in place.

1. Acquire exclusive maintenance ownership.
2. Commit and fsync all buffered appends.
3. Rebuild required engine state from verified durable records.
4. Create every required projection and consumer bootstrap.
5. Write the new anchor and retained suffix into a temporary generation.
6. Verify checksums, identity, positions, batches, branch visibility, and
   bootstrap restoration from that generation alone.
7. Fsync all new files and their directories.
8. Atomically publish the generation manifest.
9. Reopen and verify the published generation.
10. Mark the prior generation reclaimable.
11. Delete prior-generation files best-effort.

A crash before step 8 opens the old generation. A crash at or after step 8
opens the complete new generation. Recovery MUST never combine an anchor from
one generation with a suffix from another.

Failure to delete old files is a space leak, not corruption. Cleanup retries on
later maintenance operations.

## 9. Feed bootstrap

When a feed position is below the floor, `position_unavailable` includes a
bootstrap descriptor containing:

- database ID and generation;
- effective floor;
- branch and filter scope;
- checkpoint ID, codec, version, byte length, and checksum;
- the feed continuation to use after restoration.

Fetching bootstrap bytes and opening the resumed feed are separate bounded
operations. The engine never sends a partial bootstrap as a feed page.
Consumers must verify the descriptor and checkpoint before acknowledging a
position at or beyond the floor.

## 10. Retention policy and security boundaries

The initial policy is manual `KeepFrom(position)`. Future selectors such as
`KeepLastEvents`, `KeepNewerThan`, and `KeepUnderBytes` are planning helpers,
not different durability contracts.

Retention is not guaranteed secure erasure. Filesystem snapshots, backups,
journals, copy-on-write storage, and SSD wear leveling may preserve deleted
bytes. Applications needing cryptographic erasure should encrypt payloads with
separately managed keys and destroy the applicable keys.

No policy may inspect application payload bytes. Policy decisions may use
engine envelope fields such as position, timestamp, branch, stream, type, and
schema version.

## 11. Verification requirements

Physical deletion may ship only with tests proving:

- every publication fault opens either the complete old or complete new
  generation;
- corrupt or mismatched anchors are rejected before state exposure;
- replay, query, diff, branch, and feed answers at or after the floor match a
  pre-compaction oracle;
- every request below the floor returns the typed unavailable result;
- protected projection restoration is exact across all partitions;
- an unprotected projection never fabricates state from an incomplete suffix;
- consumer bootstrap and resume have no gap or duplicate;
- idempotent retries obey the chosen horizon;
- missing old-generation cleanup changes disk usage only;
- Linux, macOS, and Windows pass the normal retention suite;
- Linux and Windows pass crash-harness scenarios at every publication phase.

Property tests must vary segment boundaries, batch sizes, branch ancestry,
projection partitioning, consumer positions, and requested floors.

## 12. Initial implementation phases

1. **Implemented:** add typed floors and unavailable errors without deleting
   bytes.
2. **Implemented:** add read-only planning and blocker reporting.
3. **Implemented:** add engine-core anchors and generation recovery.
4. **Implemented:** add protected projection and consumer bootstrap coverage.
5. **Implemented:** enable whole-closed-segment reclamation.
6. **Implemented:** add exact-position, latest-event, timestamp-cutoff, and
   retained-byte policy selectors over the proven explicit-floor path.

No phase may advertise physical compaction until phases 1–5 pass the failure
and cross-platform tests above.
