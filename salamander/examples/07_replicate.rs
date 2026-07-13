use salamander::{Engine, EngineOptions, FeedRequest};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = Engine::open(EngineOptions::new("salamander-feed-demo"))?;
    let target = Engine::open(EngineOptions::new("salamander-follower-demo"))?;
    let feed = source.open_feed(FeedRequest::default())?;
    loop {
        let page = source.next_feed_page(feed, None)?;
        for batch in page.batches {
            target.ingest_batch(batch)?;
        }
        if page.continuation == page.durable_head {
            break;
        }
    }
    println!("follower durable head: {}", target.durable_head()?);
    Ok(())
}
