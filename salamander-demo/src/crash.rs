//! Real-process crash harness for log recovery and derived-state safety.
//!
//! `crashtest parent <dir> [append|batch|snapshot|heal|all]` prepares any
//! required fixture, starts a worker, waits until the target operation is
//! active, kills it without cleanup, and verifies recovery from log truth.
//! `all` is the default and runs every scenario once in separate directories.

use std::collections::{BTreeMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use salamander::agent::{EventBody, KvProjection};
use salamander::{
    AgentDb, Change, CommitPolicy, DurabilityDto, Engine, EngineAppendBatch, EngineOptions, Event,
    EventData, ExpectedRevisionDto, IndexedView, PartitionStatus, Projection, QueryDefinition,
    QueryOperation, ReplayRequest,
};

const APPEND_STREAM: &str = "storm";
const BATCH_STREAM: &str = "batches";
const SNAPSHOT_STREAM: &str = "snapshots";
const HEAL_STREAM_PREFIX: &str = "heal-";
const QUERY_NAME: &str = "rows";
const HEAL_PARTITIONS: u32 = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    Append,
    Batch,
    Snapshot,
    Heal,
}

impl Scenario {
    const ALL: [Self; 4] = [Self::Append, Self::Batch, Self::Snapshot, Self::Heal];

    fn parse(value: &str) -> Option<Self> {
        match value {
            "append" => Some(Self::Append),
            "batch" => Some(Self::Batch),
            "snapshot" => Some(Self::Snapshot),
            "heal" => Some(Self::Heal),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Batch => "batch",
            Self::Snapshot => "snapshot",
            Self::Heal => "heal",
        }
    }

    fn kill_delay_ms(self, rng: &mut Rng) -> u64 {
        match self {
            Self::Append | Self::Batch => 50 + rng.next_u64() % 450,
            Self::Snapshot => 5 + rng.next_u64() % 45,
            Self::Heal => 1 + rng.next_u64() % 20,
        }
    }
}

/// The live view checked by the append scenario after recovery.
fn len_view() -> IndexedView<String, Vec<u8>, EventBody> {
    IndexedView::builder()
        .project(|event: &Event<EventBody>| match &event.body {
            EventBody::Put { key, value } => Some(Change::put(key.clone(), value.clone())),
            EventBody::Delete { key } => Some(Change::delete(key.clone())),
            _ => None,
        })
        .index("by_len", |value: &Vec<u8>| {
            vec![(value.len() as u64).to_le_bytes().to_vec()]
        })
        .build()
}

pub fn run(mut args: impl Iterator<Item = String>) {
    match args.next().as_deref() {
        Some("worker") => worker(args),
        Some("child") => legacy_append_worker(args),
        Some("parent") => parent(args),
        _ => usage_and_exit(),
    }
}

fn legacy_append_worker(mut args: impl Iterator<Item = String>) {
    let dir = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| usage_and_exit());
    if args.next().is_some() {
        usage_and_exit();
    }
    append_worker(&dir);
}

fn worker(mut args: impl Iterator<Item = String>) {
    let scenario = args
        .next()
        .as_deref()
        .and_then(Scenario::parse)
        .unwrap_or_else(|| usage_and_exit());
    let dir = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| usage_and_exit());
    match scenario {
        Scenario::Append => append_worker(&dir),
        Scenario::Batch => batch_worker(&dir),
        Scenario::Snapshot => snapshot_worker(&dir),
        Scenario::Heal => heal_worker(&dir),
    }
}

fn parent(mut args: impl Iterator<Item = String>) {
    let root = args
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| usage_and_exit());
    let requested = args.next().unwrap_or_else(|| "all".into());
    if args.next().is_some() {
        usage_and_exit();
    }
    std::fs::create_dir_all(&root).expect("parent: create root directory");
    if requested == "all" {
        for scenario in Scenario::ALL {
            run_scenario(scenario, &root.join(scenario.name()));
        }
    } else {
        let scenario = Scenario::parse(&requested).unwrap_or_else(|| usage_and_exit());
        run_scenario(scenario, &root);
    }
}

fn run_scenario(scenario: Scenario, dir: &Path) {
    std::fs::create_dir_all(dir).expect("parent: create scenario directory");
    match scenario {
        Scenario::Snapshot => prepare_snapshot_fixture(dir),
        Scenario::Heal => prepare_heal_fixture(dir),
        Scenario::Append | Scenario::Batch => {}
    }

    let exe = std::env::current_exe().expect("parent: current executable");
    let mut child = Command::new(exe)
        .args(["crashtest", "worker", scenario.name()])
        .arg(dir)
        .stdout(Stdio::piped())
        .spawn()
        .expect("parent: spawn worker");
    let stdout = child.stdout.take().expect("parent: worker stdout");
    let mut output = BufReader::new(stdout);
    wait_until_ready(&mut child, &mut output, scenario);

    let mut rng = Rng::seeded();
    std::thread::sleep(Duration::from_millis(scenario.kill_delay_ms(&mut rng)));
    child.kill().expect("parent: kill worker");
    child.wait().expect("parent: wait for worker");
    let last_reported = read_last_durable_head(&mut output);

    // The process was killed before its lock guard could run.
    let _ = std::fs::remove_file(dir.join("LOCK"));
    match scenario {
        Scenario::Append => verify_append(dir, last_reported),
        Scenario::Batch => verify_batches(dir, last_reported),
        Scenario::Snapshot => verify_projection(dir, SNAPSHOT_STREAM, last_reported, true),
        Scenario::Heal => verify_projection(dir, None::<&str>, last_reported, false),
    }
}

fn wait_until_ready(child: &mut Child, output: &mut impl BufRead, scenario: Scenario) {
    let mut line = String::new();
    output.read_line(&mut line).expect("parent: read READY");
    assert_eq!(
        line.trim(),
        "READY",
        "{} worker did not become ready",
        scenario.name()
    );
    assert!(
        child.try_wait().expect("parent: inspect worker").is_none(),
        "{} worker exited before the crash point",
        scenario.name()
    );
}

fn ready() {
    let mut out = std::io::stdout().lock();
    writeln!(out, "READY").expect("worker: write READY");
    out.flush().expect("worker: flush READY");
}

fn report_durable_head(head: u64) {
    let mut out = std::io::stdout().lock();
    writeln!(out, "DURABLE {head}").expect("worker: write durable head");
    out.flush().expect("worker: flush durable head");
}

fn read_last_durable_head(output: &mut impl BufRead) -> Option<u64> {
    let mut last = None;
    for line in output.lines().map_while(Result::ok) {
        if let Some(value) = line.strip_prefix("DURABLE ") {
            if let Ok(head) = value.parse() {
                last = Some(head);
            }
        }
    }
    last
}

fn append_worker(dir: &Path) {
    let mut db = AgentDb::open(dir).expect("append worker: open");
    db.register("kv", Box::new(len_view()))
        .expect("append worker: register view");
    db.set_commit_policy(CommitPolicy::every_bytes(4096));
    ready();
    let mut rng = Rng::seeded();
    loop {
        let key = format!("k{}", rng.next_u64() % 64);
        let body = if rng.next_u64().is_multiple_of(8) {
            EventBody::Delete { key }
        } else {
            let size = 1 + (rng.next_u64() % 256) as usize;
            let value = (0..size).map(|_| (rng.next_u64() & 0xff) as u8).collect();
            EventBody::Put { key, value }
        };
        db.append(APPEND_STREAM, body)
            .expect("append worker: append");
        if rng.next_u64().is_multiple_of(4) {
            report_durable_head(db.commit().expect("append worker: commit"));
        }
    }
}

fn verify_append(dir: &Path, last_reported: Option<u64>) {
    let mut db = AgentDb::open(dir).expect("append parent: reopen");
    db.register("kv", Box::new(len_view()))
        .expect("append parent: register view");
    assert_reported_head(db.head(), last_reported, Scenario::Append);

    let kv: KvProjection = db.projection().expect("append parent: rebuild projection");
    let mut independent = BTreeMap::new();
    db.replay(APPEND_STREAM, 0..db.head(), |event| match &event.body {
        EventBody::Put { key, value } => {
            independent.insert(key.clone(), value.clone());
        }
        EventBody::Delete { key } => {
            independent.remove(key);
        }
        _ => {}
    })
    .expect("append parent: independent replay");
    assert_eq!(
        *kv.state(),
        independent,
        "projection diverged from log fold"
    );

    let view = db
        .view::<IndexedView<String, Vec<u8>, EventBody>>("kv")
        .expect("append parent: view present");
    assert_eq!(
        view.state(),
        &independent,
        "indexed view diverged from log fold"
    );
    let mut expected_by_len = BTreeMap::<u64, usize>::new();
    for value in independent.values() {
        *expected_by_len.entry(value.len() as u64).or_default() += 1;
    }
    for (len, count) in expected_by_len {
        assert_eq!(view.by("by_len", &len.to_le_bytes()).len(), count);
    }
    println!(
        "OK scenario=append head={} keys={}",
        db.head(),
        independent.len()
    );
}

fn batch_worker(dir: &Path) {
    let mut options = EngineOptions::new(dir);
    options.commit_every_bytes = Some(4096);
    let engine = Engine::open(options).expect("batch worker: open");
    ready();
    let mut rng = Rng::seeded();
    let mut sequence = 0u64;
    loop {
        let len = 1 + (rng.next_u64() % 8) as usize;
        let events = (0..len)
            .map(|index| {
                EventData::json(
                    serde_json::to_vec(&serde_json::json!({
                        "sequence": sequence,
                        "batch_len": len,
                        "batch_index": index,
                        "value": rng.next_u64(),
                    }))
                    .expect("batch worker: encode event"),
                )
            })
            .collect();
        let sync = rng.next_u64().is_multiple_of(5);
        let request = EngineAppendBatch {
            branch_id: [0; 16],
            stream: BATCH_STREAM.into(),
            expected: ExpectedRevisionDto::Any,
            idempotency_key: Some(sequence.to_le_bytes().to_vec()),
            events,
            durability: if sync {
                DurabilityDto::Sync
            } else {
                DurabilityDto::Buffered
            },
        };
        let receipt = engine
            .append(request.clone())
            .expect("batch worker: append batch");
        if rng.next_u64().is_multiple_of(7) {
            assert_eq!(
                engine
                    .append(request)
                    .expect("batch worker: idempotent retry"),
                receipt
            );
        }
        if sync {
            report_durable_head(receipt.last_position + 1);
        } else if rng.next_u64().is_multiple_of(4) {
            report_durable_head(engine.commit().expect("batch worker: commit"));
        }
        sequence += 1;
    }
}

fn verify_batches(dir: &Path, last_reported: Option<u64>) {
    let engine = Engine::open(EngineOptions::new(dir)).expect("batch parent: reopen");
    assert_reported_head(
        engine.head().expect("batch parent: head"),
        last_reported,
        Scenario::Batch,
    );
    let rows = replay_rows(&engine, Some(BATCH_STREAM));
    let mut cursor = 0usize;
    let mut seen_batches = HashSet::new();
    while cursor < rows.len() {
        let batch_id = rows[cursor].batch_id;
        assert!(
            seen_batches.insert(batch_id),
            "batch frames are not contiguous"
        );
        let start = cursor;
        while cursor < rows.len() && rows[cursor].batch_id == batch_id {
            cursor += 1;
        }
        let batch = &rows[start..cursor];
        let declared = serde_json::from_slice::<serde_json::Value>(&batch[0].payload)
            .expect("batch parent: decode payload")["batch_len"]
            .as_u64()
            .expect("batch parent: batch_len") as usize;
        assert_eq!(batch.len(), declared, "partial batch became visible");
        for (index, row) in batch.iter().enumerate() {
            let value = serde_json::from_slice::<serde_json::Value>(&row.payload)
                .expect("batch parent: decode payload");
            assert_eq!(row.batch_index as usize, index);
            assert_eq!(value["batch_index"].as_u64(), Some(index as u64));
            assert_eq!(value["batch_len"].as_u64(), Some(declared as u64));
        }
    }
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(
            row.stream_revision, index as u64,
            "stream revision gap after crash"
        );
    }
    println!(
        "OK scenario=batch head={} events={} batches={}",
        engine.head().unwrap(),
        rows.len(),
        seen_batches.len()
    );
}

fn query_definition() -> QueryDefinition {
    QueryDefinition {
        key_field: "id".into(),
        indexes: BTreeMap::new(),
        filter: None,
    }
}

fn projection_batch(
    stream: &str,
    start: u64,
    count: usize,
    payload_bytes: usize,
    durability: DurabilityDto,
) -> EngineAppendBatch {
    let padding = "x".repeat(payload_bytes);
    let events = (0..count)
        .map(|offset| {
            EventData::json(
                serde_json::to_vec(&serde_json::json!({
                    "id": format!("{stream}-{}", start + offset as u64),
                    "padding": padding,
                }))
                .expect("fixture: encode event"),
            )
        })
        .collect();
    EngineAppendBatch {
        branch_id: [0; 16],
        stream: stream.into(),
        expected: ExpectedRevisionDto::Any,
        idempotency_key: None,
        events,
        durability,
    }
}

fn prepare_snapshot_fixture(dir: &Path) {
    let engine = Engine::open(EngineOptions::new(dir)).expect("snapshot fixture: open");
    let handle = engine
        .register_partitioned_query(QUERY_NAME.into(), query_definition(), 4)
        .expect("snapshot fixture: register query");
    for start in (0..512).step_by(32) {
        engine
            .append(projection_batch(
                SNAPSHOT_STREAM,
                start,
                32,
                2048,
                DurabilityDto::Buffered,
            ))
            .expect("snapshot fixture: append");
    }
    engine.commit().expect("snapshot fixture: commit");
    assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, 512);
    engine
        .create_snapshot(handle)
        .expect("snapshot fixture: baseline snapshot");
    engine.close().expect("snapshot fixture: close");
}

fn snapshot_worker(dir: &Path) {
    let engine = Engine::open(EngineOptions::new(dir)).expect("snapshot worker: open");
    let handle = engine
        .query_named(QUERY_NAME.into())
        .expect("snapshot worker: query handle");
    ready();
    let mut next = 512u64;
    loop {
        let receipt = engine
            .append(projection_batch(
                SNAPSHOT_STREAM,
                next,
                1,
                2048,
                DurabilityDto::Sync,
            ))
            .expect("snapshot worker: append");
        report_durable_head(receipt.last_position + 1);
        engine
            .query(handle, QueryOperation::Len)
            .expect("snapshot worker: heal");
        engine
            .create_snapshot(handle)
            .expect("snapshot worker: publish snapshot");
        next += 1;
    }
}

fn prepare_heal_fixture(dir: &Path) {
    let engine = Engine::open(EngineOptions::new(dir)).expect("heal fixture: open");
    let handle = engine
        .register_partitioned_query(QUERY_NAME.into(), query_definition(), HEAL_PARTITIONS)
        .expect("heal fixture: register query");
    let mut id = 0u64;
    for round in 0..64 {
        for stream in 0..HEAL_PARTITIONS {
            engine
                .append(projection_batch(
                    &format!("{HEAL_STREAM_PREFIX}{stream}"),
                    id,
                    8,
                    128,
                    DurabilityDto::Buffered,
                ))
                .expect("heal fixture: append prefix");
            id += 8;
        }
        if round % 16 == 15 {
            engine.commit().expect("heal fixture: commit prefix");
        }
    }
    assert_eq!(engine.query(handle, QueryOperation::Len).unwrap().len, id);
    for partition in 0..HEAL_PARTITIONS {
        engine
            .create_partition_snapshot(handle, partition)
            .expect("heal fixture: partition snapshot");
    }
    for round in 0..32 {
        for stream in 0..HEAL_PARTITIONS {
            engine
                .append(projection_batch(
                    &format!("{HEAL_STREAM_PREFIX}{stream}"),
                    id,
                    8,
                    128,
                    DurabilityDto::Buffered,
                ))
                .expect("heal fixture: append suffix");
            id += 8;
        }
        if round % 8 == 7 {
            engine.commit().expect("heal fixture: commit suffix");
        }
    }
    engine.commit().expect("heal fixture: final commit");
    engine.close().expect("heal fixture: close");
}

fn heal_worker(dir: &Path) {
    let engine = Engine::open(EngineOptions::new(dir)).expect("heal worker: open");
    let handle = engine
        .query_named(QUERY_NAME.into())
        .expect("heal worker: query handle");
    ready();
    loop {
        engine
            .query(handle, QueryOperation::Len)
            .expect("heal worker: query");
        engine
            .rebuild_projection(handle)
            .expect("heal worker: reset projection");
    }
}

fn verify_projection(
    dir: &Path,
    stream: impl Into<Option<&'static str>>,
    last_reported: Option<u64>,
    verify_snapshots: bool,
) {
    let stream = stream.into();
    let engine = Engine::open(EngineOptions::new(dir)).expect("projection parent: reopen");
    let head = engine.head().expect("projection parent: head");
    assert_reported_head(
        head,
        last_reported,
        if verify_snapshots {
            Scenario::Snapshot
        } else {
            Scenario::Heal
        },
    );
    let handle = engine
        .query_named(QUERY_NAME.into())
        .expect("projection parent: query handle");
    let expected = if let Some(stream) = stream {
        replay_rows(&engine, Some(stream)).len() as u64
    } else {
        replay_rows(&engine, None).len() as u64
    };
    assert_eq!(
        engine.query(handle, QueryOperation::Len).unwrap().len,
        expected
    );
    assert!(engine
        .partition_status(handle)
        .unwrap()
        .iter()
        .all(|status| matches!(status, PartitionStatus::Ready { .. })));
    if verify_snapshots {
        for snapshot in engine
            .list_snapshots(handle)
            .expect("projection parent: list snapshots")
        {
            engine
                .verify_snapshot(snapshot.id)
                .expect("published snapshot must verify");
        }
    }
    println!(
        "OK scenario={} head={head} projected={expected}",
        if verify_snapshots { "snapshot" } else { "heal" }
    );
}

fn replay_rows(engine: &Engine, stream: Option<&str>) -> Vec<salamander::RecordDto> {
    let handle = engine
        .open_reader(ReplayRequest {
            stream: stream.map(str::to_string),
            ..ReplayRequest::default()
        })
        .expect("parent: open replay reader");
    let mut rows = Vec::new();
    loop {
        let page = engine.next_page(handle).expect("parent: replay page");
        rows.extend(page.records);
        if page.done {
            break;
        }
    }
    engine
        .close_reader(handle)
        .expect("parent: close replay reader");
    rows
}

fn assert_reported_head(head: u64, reported: Option<u64>, scenario: Scenario) {
    if let Some(reported) = reported {
        assert!(
            head >= reported,
            "{} recovery head {head} is behind acknowledged durable head {reported}",
            scenario.name()
        );
    }
}

fn usage_and_exit() -> ! {
    eprintln!(
        "usage: salamander-demo crashtest parent <dir> [append|batch|snapshot|heal|all]\n       salamander-demo crashtest worker <append|batch|snapshot|heal> <dir>"
    );
    std::process::exit(2)
}

/// Small dependency-free generator used only to vary crash timing and data.
struct Rng(u64);

impl Rng {
    fn seeded() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let pid = std::process::id() as u64;
        Self(nanos ^ pid.wrapping_mul(0x9e3779b97f4a7c15))
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
}
