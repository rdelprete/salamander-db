use std::collections::HashMap;

use proptest::prelude::*;
use salamander::agent::EventBody;
use salamander::{
    AgentDb, AppendRequest, BranchId, Durability, EventType, ExpectedRevision, IdempotencyKey,
    NewEvent, SalamanderError, StreamName, StreamRevision,
};

#[derive(Clone, Debug)]
enum Op {
    Append {
        stream: u8,
        count: u8,
        expectation: u8,
        key: Option<u8>,
        content: u8,
    },
    Reopen,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Signature {
    stream: u8,
    count: u8,
    expected: String,
    content: u8,
}

#[derive(Clone)]
struct Remembered {
    signature: Signature,
    first: u64,
    last: u64,
    revision: StreamRevision,
}

fn operation() -> impl Strategy<Value = Op> {
    prop_oneof![
        9 => (0u8..2, 1u8..4, 0u8..4, prop::option::of(0u8..4), any::<u8>())
            .prop_map(|(stream, count, expectation, key, content)| Op::Append {
                stream,
                count,
                expectation,
                key,
                content,
            }),
        1 => Just(Op::Reopen),
    ]
}

fn stream_name(stream: u8) -> String {
    format!("stream-{stream}")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn engine_matches_independent_stream_model(ops in prop::collection::vec(operation(), 1..80)) {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Some(AgentDb::open(dir.path()).unwrap());
        let mut head = 0u64;
        let mut revisions: HashMap<u8, StreamRevision> = HashMap::new();
        let mut counts: HashMap<u8, u64> = HashMap::new();
        let mut remembered: HashMap<u8, Remembered> = HashMap::new();

        for op in ops {
            match op {
                Op::Reopen => {
                    drop(db.take());
                    db = Some(AgentDb::open(dir.path()).unwrap());
                }
                Op::Append { stream, count, expectation, key, content } => {
                    let actual = revisions.get(&stream).copied();
                    let expected = match expectation {
                        0 => ExpectedRevision::Any,
                        1 => ExpectedRevision::NoStream,
                        2 => actual.map_or(ExpectedRevision::NoStream, ExpectedRevision::Exact),
                        _ => ExpectedRevision::Exact(StreamRevision(actual.map_or(0, |r| r.0 + 7))),
                    };
                    let signature = Signature {
                        stream,
                        count,
                        expected: format!("{expected:?}"),
                        content,
                    };
                    let request = AppendRequest {
                        branch: BranchId::ZERO,
                        stream: StreamName::new(stream_name(stream)).unwrap(),
                        expected,
                        idempotency_key: key.map(|value| IdempotencyKey::new(vec![value]).unwrap()),
                        events: (0..count)
                            .map(|index| NewEvent::new(
                                EventType::new("model.event").unwrap(),
                                EventBody::Put {
                                    key: format!("{content}-{index}"),
                                    value: vec![content, index],
                                },
                            ))
                            .collect(),
                        durability: Durability::Buffered,
                    };

                    let result = db.as_mut().unwrap().append_batch(request);
                    if let Some(key) = key {
                        if let Some(prior) = remembered.get(&key) {
                            if prior.signature == signature {
                                let receipt = result.unwrap();
                                prop_assert_eq!(receipt.first_position, prior.first);
                                prop_assert_eq!(receipt.last_position, prior.last);
                                prop_assert_eq!(receipt.current_revision, prior.revision);
                            } else {
                                prop_assert!(matches!(result, Err(SalamanderError::IdempotencyConflict)));
                            }
                            prop_assert_eq!(db.as_ref().unwrap().head(), head);
                            continue;
                        }
                    }

                    let expected_matches = match expected {
                        ExpectedRevision::Any => true,
                        ExpectedRevision::NoStream => actual.is_none(),
                        ExpectedRevision::Exact(value) => actual == Some(value),
                    };
                    if !expected_matches {
                        let is_conflict = matches!(
                            result,
                            Err(SalamanderError::RevisionConflict { .. })
                        );
                        prop_assert!(is_conflict);
                        prop_assert_eq!(db.as_ref().unwrap().head(), head);
                        continue;
                    }

                    let receipt = result.unwrap();
                    prop_assert_eq!(receipt.first_position, head);
                    prop_assert_eq!(receipt.last_position, head + u64::from(count) - 1);
                    let next_revision = StreamRevision(actual.map_or(0, |r| r.0 + 1) + u64::from(count) - 1);
                    prop_assert_eq!(receipt.current_revision, next_revision);
                    head += u64::from(count);
                    revisions.insert(stream, next_revision);
                    *counts.entry(stream).or_default() += u64::from(count);
                    if let Some(key) = key {
                        remembered.insert(key, Remembered {
                            signature,
                            first: receipt.first_position,
                            last: receipt.last_position,
                            revision: receipt.current_revision,
                        });
                    }
                }
            }

            prop_assert_eq!(db.as_ref().unwrap().head(), head);
            for stream in 0..2u8 {
                let mut observed = 0u64;
                let database = db.as_ref().unwrap();
                database.replay(&stream_name(stream), 0..database.head(), |_| observed += 1).unwrap();
                prop_assert_eq!(observed, counts.get(&stream).copied().unwrap_or(0));
            }
        }
    }
}
