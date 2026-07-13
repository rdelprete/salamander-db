//! The query layer. Run with:
//!
//!     cargo run --example 03_query_views
//!
//! Register a live `IndexedView` and query it with get / range / prefix /
//! by. The view is a fold of the log, updated on every append, so a query
//! never sees stale data. Here we index tool calls by tool name.

use salamander::agent::EventBody;
use salamander::{AgentDb, Change, Event, IndexedView};

fn main() -> salamander::Result<()> {
    let dir = fresh_dir("query_views");
    let mut db = AgentDb::open(&dir)?;

    // Primary key = call_id; value = tool name; secondary index "by_tool".
    let view = IndexedView::builder()
        .project(|e: &Event<EventBody>| match &e.body {
            EventBody::ToolCall { call_id, tool, .. } => {
                Some(Change::put(call_id.clone(), tool.clone()))
            }
            _ => None, // ignore every other event kind
        })
        .index("by_tool", |tool: &String| vec![tool.as_bytes().to_vec()])
        .build();
    db.register("tools", Box::new(view))?;

    for (id, tool) in [
        ("t1", "grep"),
        ("t2", "git_blame"),
        ("t3", "grep"),
        ("t4", "open_pr"),
    ] {
        db.append("s", tool_call(id, tool))?;
    }

    // Downcast the erased view back to its concrete type to query it.
    let tools = db
        .view::<IndexedView<String, String, EventBody>>("tools")
        .unwrap();

    println!("get(\"t2\")          = {:?}", tools.get("t2"));

    let range: Vec<_> = tools.range("t1".to_string().."t3".to_string()).collect();
    println!("range(t1..t3)      = {range:?}");

    println!("prefix(\"t\")        = {} rows", tools.prefix("t").count());

    // Every call that used `grep` (values, in primary-key order).
    println!("by(by_tool, grep)  = {:?}", tools.by("by_tool", b"grep"));

    Ok(())
}

fn tool_call(id: &str, tool: &str) -> EventBody {
    EventBody::ToolCall {
        call_id: id.into(),
        tool: tool.into(),
        args_json: "{}".into(),
    }
}

fn fresh_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("salamander-example-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
