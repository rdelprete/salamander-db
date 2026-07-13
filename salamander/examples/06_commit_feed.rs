use std::collections::BTreeMap;

use salamander::{
    DurabilityDto, Engine, EngineAppendBatch, EngineOptions, EventData, ExpectedRevisionDto,
    FeedRequest, PayloadCodec,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "salamander-feed-demo".into());
    let engine = Engine::open(EngineOptions::new(path))?;
    engine.append(EngineAppendBatch {
        branch_id: [0; 16],
        stream: "orders".into(),
        expected: ExpectedRevisionDto::Any,
        idempotency_key: None,
        durability: DurabilityDto::Sync,
        events: vec![EventData {
            event_id: None,
            event_type: "order.created".into(),
            schema_version: 1,
            metadata: BTreeMap::new(),
            codec: PayloadCodec::Json,
            payload: br#"{"order":"A-1"}"#.to_vec(),
        }],
    })?;
    let feed = engine.open_feed(FeedRequest::default())?;
    let page = engine.next_feed_page(feed, Some(1_000))?;
    for batch in page.batches {
        println!(
            "batch {:?}: {} event(s)",
            batch.batch_id,
            batch.events.len()
        );
    }
    println!("resume from {} after restart", page.continuation);
    Ok(())
}
