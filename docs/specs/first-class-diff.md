# Spec: first-class diff — divergence as an engine operation

**Status:** implemented (see Implementation notes at the end)
**Target:** v0.x ([ROADMAP.md](../../ROADMAP.md))
**Location:** engine core (`branch.rs`, `db.rs`), facade (`facade.rs`),
Python binding (`salamander-py`), with demo and future-CLI integration.

## Motivation

Fork is the headline feature: branch a timeline at any point and let two
futures diverge over a shared past. Diff is its missing read half — "where
exactly do these two timelines agree, and what does each say after that?" —
and today it exists only as application code:

- `examples/py/chat.py` implements `/diff` by fully replaying **both**
  timelines and zipping them until the first unequal payload — O(left +
  right) I/O and payload comparison for an answer the engine already knows.
- The Rust session demo hand-computes its side-by-side "diverge" marker the
  same way.
- The playground UI shows branches individually but cannot show where they
  split.

The engine can do categorically better, because of two facts it already
maintains:

1. **Branch ancestry is durable and engine-owned.** Every fork records its
   parent and exclusive `fork_position` in the branch catalog
   (`BranchInfo`), rebuilt from system records on open.
2. **History is immutable and inherited replay is positional.** A branch's
   timeline below its fork position *is* its ancestor's timeline — the same
   physical records, selected by `BranchCatalog::replay_scopes`. Two
   timelines cannot differ below their divergence position by construction.

So a diff never needs to compare records. The divergence position is a pure
function of the branch catalog (O(ancestry depth), no I/O beyond the
already-loaded catalog), and everything else a diff returns is bounded
replay — the existing reader. **A diff is a position plus three replay
plans.** No new durable state, no format change, no new handle lifecycle.

This is also the seam three roadmap items will consume: the inspector
(`salamander-scope`) needs divergence to render branch topology, an MCP
server wants a `diff_timelines` tool, and a future CLI wants
`salamander diff A B`.

## Semantics

### Definitions

- A **timeline** is `(branch, until)`: the branch-scoped replay of `branch`
  from position 0 up to exclusive position `until` (default: the head
  observed when the diff is computed; both sides resolve against the same
  observation).
- The **divergence position** `d` of two timelines is the exclusive upper
  bound of their shared history: for every position `p < d`, a record is
  visible at `p` in the left timeline iff it is visible at `p` in the right
  timeline, and it is the same physical record; and `d` is the largest such
  bound ≤ both untils.

### Computing divergence from the catalog

Root-first ancestries `A = ancestry(left)` and `B = ancestry(right)` always
share a node prefix of length `k ≥ 1` (every ancestry starts at the default
branch, `BranchId::ZERO`).

```text
ancestor = A[k-1]                       # == B[k-1], the common ancestor
d        = min( left_until, right_until,
                fork_position of every node in A[k..],
                fork_position of every node in B[k..] )
```

Every *non-shared* node caps the divergence: its local records sit at
positions at or above its fork position and are visible to one side only.
The minimum runs over the whole divergent tail of both paths — not just
the first split — because fork positions are not monotone along a path: a
fork below its parent's own fork point is legal, inherits strictly up to
its (smaller) position, and its records cap the shared window. (As
originally drafted, this formula used only the first non-shared node; the
property-test oracle found the non-monotone counterexample — see
Implementation notes.)

Worked cases (all fall out of the formula, and all become unit tests):

| Case | Result |
|---|---|
| `diff(main, fork@8)` | ancestor `main`, `d = 8`; left suffix = main's records ≥ 8, right suffix = the fork's local records |
| siblings forked from main at 5 and 9 | ancestor `main`, `d = 5`; right suffix *includes* inherited main records `[5, 9)` — its timeline really does contain them |
| same branch, untils `p < q` | ancestor = the branch, `d = p`; left suffix empty, right suffix `[p, q)` — the `/rewind`-style within-timeline diff, subsumed |
| `diff(b, b)` at equal untils | `d = until`; both suffixes empty |
| `diff(main@3, fork@8)` | `d = min(8, 3) = 3`; left suffix empty, right suffix `[3, …)` including inherited main records |
| grandchild vs. grandchild of a fork | ancestor is the deepest shared *branch node*, not necessarily `main` |

### The result

```text
TimelineDiff
├── common_ancestor            # BranchInfo
├── divergence                 # d, exclusive
├── shared                     # ReplayPlan: ancestor's timeline, until d
├── left:  { branch, until, suffix: ReplayPlan }   # [d, left_until)
└── right: { branch, until, suffix: ReplayPlan }   # [d, right_until)
```

Suffixes are returned as **plans, not materialized records** — divergent
suffixes are unbounded in principle, and the streaming reader already
provides bounded-memory enumeration and resumable continuations. An
optional `StreamSelector` on the request scopes both suffix plans (and the
shared plan) to chosen streams; the divergence position itself is always
positional and stream-independent.

### Normative contract

- **DIFF-1 (exactness).** For every `p < d`, the record visible at `p` is
  the same in both timelines; resolving the `shared` plan against either
  side yields identical record sequences.
- **DIFF-2 (reconstruction).** `timeline(side, until)` equals the
  concatenation of the resolved `shared` plan and that side's resolved
  `suffix` plan — elementwise, by position and record identity.
- **DIFF-3 (symmetry).** `diff(l, r)` and `diff(r, l)` report the same
  ancestor and divergence, with sides swapped.
- **DIFF-4 (self).** `diff(b, b)` at equal untils has empty suffixes.
- **DIFF-5 (derived, read-only).** A diff writes nothing, and its answer
  depends only on the branch catalog and the log. Deleting all derived
  state never changes a diff (AGENTS.md invariant: derived-state loss may
  change performance, never answers).
- **DIFF-6 (payload opacity, INV-9).** Divergence is determined entirely by
  engine-envelope data — ancestry positions — and never by payload bytes.
  Two branches whose divergent suffixes happen to contain byte-identical
  payloads still diverge at their fork position: the diff reports *history*
  identity, not payload equality.

## API surface

### Core (`branch.rs`)

Extend `BranchCatalog` (all `pub(crate)`, alongside the existing
`common_ancestor`):

```rust
/// The common ancestor and exclusive divergence position of two
/// timelines, per the DIFF contract. Pure catalog arithmetic.
fn divergence(
    &self,
    left: BranchId, left_until: u64,
    right: BranchId, right_until: u64,
) -> Result<(BranchInfo, u64)>
```

The position math is a standalone function over the two ancestry vectors so
property tests can drive it without a database.

### Typed API (`db.rs`, on `Salamander<B>`)

```rust
pub struct DiffRequest {
    pub left: BranchId,
    pub right: BranchId,
    pub left_until: ReplayEnd,    // default Head
    pub right_until: ReplayEnd,   // default Head
    pub streams: StreamSelector,  // default All; scopes the emitted plans
}

pub struct DiffSide {
    pub branch: BranchInfo,
    pub until: u64,               // resolved, exclusive
    pub suffix: ReplayPlan,       // this timeline, [divergence, until)
}

pub struct TimelineDiff {
    pub common_ancestor: BranchInfo,
    pub divergence: u64,          // exclusive upper bound of shared history
    pub shared: ReplayPlan,       // ancestor's timeline, [0, divergence)
    pub left: DiffSide,
    pub right: DiffSide,
}

impl<B: …> Salamander<B> {
    pub fn diff(&self, request: DiffRequest) -> Result<TimelineDiff> { … }
}
```

`ReplayEnd::Head` resolves once, against a single head observation, so both
sides of one diff describe the same instant. Explicit `At(n)` beyond head
is rejected with the existing `OffsetBeyondHead`, same as reader
construction. The returned plans feed the existing `Salamander::read` —
diff adds no reading machinery of its own.

### Facade (`facade.rs`, on `Engine`)

DTO mirrors of the typed types, in the facade's existing style
(`[u8; 16]` ids, flat fields):

```rust
pub struct DiffRequestDto {
    pub left_branch_id: [u8; 16],
    pub right_branch_id: [u8; 16],
    pub left_until: Option<u64>,   // None = head
    pub right_until: Option<u64>,
    pub stream: Option<String>,    // same selector shape as ReplayRequest
    pub page_events: u32,          // carried into the emitted ReplayRequests
    pub page_bytes: usize,
}

pub struct DiffSideDto {
    pub branch: BranchDto,
    pub until: u64,
    pub suffix: ReplayRequest,     // ready for open_reader
}

pub struct DiffDto {
    pub common_ancestor: BranchDto,
    pub divergence_position: u64,
    pub shared: ReplayRequest,
    pub left: DiffSideDto,
    pub right: DiffSideDto,
}

impl Engine {
    pub fn diff(&self, request: DiffRequestDto) -> Result<DiffDto, EngineError> { … }
}
```

Returning `ReplayRequest`s rather than opened readers keeps the facade's
handle lifecycle untouched: the caller opens (and owns) readers exactly as
today. Bindings stay translation-only, per the facade's charter.

> `ReplayRequest.until` today means an exclusive position and `None` means
> head — the emitted requests use `from = divergence_position` and the
> resolved untils, so a facade caller can hand them straight to
> `open_reader` without interpreting the diff.

### Python binding (`salamander-py`)

```python
db.diff(left, right, namespace=None, left_until=None, right_until=None,
        page_events=256, page_bytes=1_048_576)
```

`left`/`right` are branch names; `"main"` names the default timeline (the
catalog already resolves it — no special-casing). Returns a dict in the
binding's plain-data style:

```python
{
  "common_ancestor": "main",
  "divergence_offset": 8,
  "left":  {"branch": "main",                 "until": 12, "suffix": <Reader>},
  "right": {"branch": "debug-session-fork-8", "until": 17, "suffix": <Reader>},
  "shared": <Reader>,
}
```

The three readers are the binding's existing `Reader` type, pre-scoped by
the facade's emitted `ReplayRequest`s — iterate them for rows, or drop them
unread; nothing is materialized until asked. `namespace=None` diffs whole
timelines; a name scopes the readers to that stream, matching `replay`'s
vocabulary. Unknown branch names raise `salamander.NotFoundError`; an
`*_until` beyond head raises `salamander.InvalidArgumentError` — the
existing stable exception categories, no new ones.

## Edge cases

- **Archived branches** retain readable history and therefore diff normally
  (archival changes writability, not answers).
- **Forks of forks** work to `MAX_LINEAGE_DEPTH`; the divergence walk is
  bounded by the same limit as `ancestry`.
- **Fork at position 0**: divergence 0, empty shared plan — legal and
  boring, as it should be.
- **Stream scoping can empty a suffix** without moving the divergence
  position: divergence is history identity, not "this stream changed".
- **Unknown branch** → `BranchNotFound`; **until beyond head** →
  `OffsetBeyondHead`; both are existing errors surfaced at diff
  construction, before any plan is emitted.
- **Uncommitted tail**: diff resolves against `head()` like every reader —
  buffered-but-uncommitted records are visible to the handle that wrote
  them, durable only after commit. The diff makes no durability claim
  beyond the reader's own.

## Tests

No format change, no new durable state, no recovery-path change — so no
golden fixtures and no new crash-harness scenarios (AGENTS.md gates:
`fmt`, `clippy`, workspace tests).

1. **Unit** (`branch.rs`): the divergence function over hand-built
   catalogs — every row of the worked-cases table, plus depth-limit and
   unknown-branch errors.
2. **Property** (engine integration): generate random branch trees (random
   fork points, interleaved appends across branches and streams, forks of
   forks), then for random timeline pairs assert DIFF-1…4 against a
   brute-force oracle — full double replay and zip, i.e. exactly the
   algorithm `chat.py` uses today. The demo's implementation is demoted to
   the property test's oracle.
3. **Facade**: DTO round-trip; emitted `ReplayRequest`s drive `open_reader`
   / `next_page` to exhaustion (including the branch-scope-filtered-tail
   pagination case that bit `next_page` before, from both sides of a
   diverged pair).
4. **Python** (`examples/py/` pytest suites): diff of the chat demo's fork
   scenario matches the demo's own zip-diff output; `main`-vs-branch,
   branch-vs-branch, same-branch-two-untils; exception mapping.
5. **DIFF-5 mechanically**: compute a diff, `delete_all_derived_state()`,
   recompute, assert identical — the invariant gets a test, not a comment.

## Integration and documentation

- **`chat.py`**: reimplement `ChatSession.diff` on `db.diff`, preserving
  its `(shared_turn_count, a_suffix, b_suffix)` return shape (shared turn
  count = row count of the `shared` reader scoped to the chat namespace).
  Its tests keep passing; the double-replay zip moves to the test oracle.
- **Rust example**: `salamander/examples/08_diff.rs` — fork the session
  fable, diff parent against fork, print the side-by-side diverge view the
  session demo currently hand-rolls.
- **README**: one bullet under Why ("History is queryable") gains its
  sibling: fork is cheap *and diffable*; quick-start gets the one-liner.
- **Playground/inspector**: the UI's branch switcher can show "diverged at
  offset N from `main`" from one `diff` call — seed work for
  `salamander-scope`.
- **CHANGELOG**: engine, facade, and binding entries under Unreleased.
- **ROADMAP**: add to v0.x (this spec).

## Acceptance criteria

1. `Salamander::diff`, `Engine::diff`, and `db.diff` (Python) ship together
   with the contract statements DIFF-1…6 defended by the tests above.
2. Computing a diff performs no log I/O beyond what the caller's subsequent
   reader consumption requires (catalog arithmetic only) — verified by a
   test that diffs two branches of a large log and asserts the summary
   returns without touching record frames.
3. `chat.py`'s `/diff` runs on the engine call with unchanged user-visible
   output and unchanged tests.
4. Rustdoc on every new public item; the new Rust example runs via
   `cargo run --example 08_diff -p salamander-db`.

## Out of scope

- **State diff** — comparing *projection state* (registered views /
  `view_at`) between two positions or branches: "which keys changed between
  step 3 and step 9". Valuable, but a different mechanism (walking two
  derived states, partition by partition) with its own consistency story;
  it composes on top of this spec's positions and belongs with the
  inspector work. Design sketch deliberately deferred.
- **Semantic/payload diff** — interpreting payload bytes to say *how* two
  events differ is the application's job (INV-9). The engine reports
  history divergence; `chat.py` rendering text diffs on top is the model.
- **Merge** — reconciling divergent branches is not diff's dual here:
  replay-and-reapply onto a new branch is the user-space answer, and
  anything smarter reintroduces multi-writer semantics by the back door
  (permanent non-goal territory; revisit only with a workload in hand).
- **Three-way diff** (two branches against their ancestor's *content*) —
  expressible today as two pairwise diffs; no engine primitive needed.

## Open questions — resolved

1. **Name: `diff` or `divergence`?** `diff` — it is the vocabulary users
   arrive with (git, Dolt, the demo's own `/diff`), and the result type's
   name (`TimelineDiff`) carries the precision.
2. **Materialize suffixes in Python?** No — return pre-scoped readers.
   Suffixes are unbounded; the binding already has a paginated `Reader`;
   `list(reader)` is one call away for demos. A `materialize=` flag can be
   added later without breaking anything; the reverse migration could not.
3. **Should divergence count shared *events* (not just the position)?**
   No — counting requires a scan, which would silently turn O(depth)
   catalog arithmetic into O(shared-history) I/O. Callers who want the
   count consume the `shared` plan (optionally with `max_events` as a
   guard), paying only for what they touch — the same principle as instant
   recovery.

## Implementation notes (July 2026)

Deviations and discoveries from building the spec:

- **Acceptance criterion 2 ("no log I/O") is not mechanically asserted.**
  Instrumenting frame reads would need test-only hooks in the log layer;
  instead, `Salamander::diff` is `&self` over catalog state by
  construction, and DIFF-5 gets its mechanical test at the facade
  (`diff` → `delete_all_derived_state` → identical `diff`). Revisit if the
  log layer ever grows read instrumentation.
- **The property test found a real engine bug on its first full run.**
  For a fork created *below its parent's own fork position* (legal, if
  odd), `replay_scopes` capped each ancestor level only by the immediate
  child's fork position, leaking grandparent records into the window
  between the two fork points — contradicting `fork_branch`'s documented
  "inherits parent history up to `at`". The caps now cascade as a running
  minimum from leaf to root (`branch.rs::replay_scopes`), the divergence
  formula takes the minimum over *every* non-shared fork position (not
  just the first split), and both are pinned by unit tests
  (`a_fork_below_its_parents_fork_caps_the_divergence`,
  `replay_scopes_cascade_downstream_fork_caps`) alongside the oracle
  property test that caught it.
- **`chat.py`'s "shared" count changed meaning, deliberately**: it now
  reports shared *history* (positional identity), so post-fork turns whose
  text happens to coincide on both branches count as divergent, where the
  old zip counted them shared. The demo's tests pass unchanged; the
  docstring states the semantic.
- **`ReplayRequest` gained `PartialEq`/`Eq`** so diff DTOs (which embed
  emitted requests) stay comparable — used by the DIFF-5 facade test.
