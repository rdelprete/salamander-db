//! `salamander-demo -- ui [dir] [--port N]` — the local playground.
//!
//! A zero-dependency HTTP server (std `TcpListener`, hand-rolled request
//! parsing) over a [`JsonDb`], serving one embedded HTML page. Append
//! JSON events to named streams, scrub the timeline back through
//! history, and fork a branch at any point — the whole page is a thin
//! view over the public engine API (`read(ReplayPlan)`, `fork_branch`,
//! `append_on_branch`).
//!
//! Deliberately single-threaded: the engine is single-writer by
//! contract, so requests are handled one at a time and the process *is*
//! the writer. `Connection: close` keeps the sequential loop honest.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;

use salamander::{
    BranchId, BranchInfo, BranchName, Json, JsonDb, Metadata, RecordReader, ReplayPlan,
};
use serde_json::{json, Value};

const DEFAULT_PORT: u16 = 7171;
const DEFAULT_DIR: &str = "./salamander-playground";
const PAGE: &str = include_str!("ui.html");

pub fn run(args: impl Iterator<Item = String>) {
    let (dir, port) = parse_args(args);
    let mut db = match JsonDb::open(&dir) {
        Ok(db) => db,
        Err(error) => {
            eprintln!("salamander-demo ui: cannot open {}: {error}", dir.display());
            eprintln!("(is another playground already running against this directory?)");
            std::process::exit(1);
        }
    };

    let listener = match TcpListener::bind(("127.0.0.1", port)) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("salamander-demo ui: cannot bind 127.0.0.1:{port}: {error}");
            std::process::exit(1);
        }
    };

    println!("SalamanderDB playground");
    println!("  data dir : {}", dir.display());
    println!("  url      : http://127.0.0.1:{port}");
    println!("Ctrl-C to stop. The directory persists — reopen it any time.");

    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        if let Err(error) = handle(stream, &mut db) {
            eprintln!("salamander-demo ui: request failed: {error}");
        }
    }
}

fn parse_args(args: impl Iterator<Item = String>) -> (PathBuf, u16) {
    let mut dir = PathBuf::from(DEFAULT_DIR);
    let mut port = DEFAULT_PORT;
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        if arg == "--port" {
            port = args.next().and_then(|p| p.parse().ok()).unwrap_or_else(|| {
                eprintln!("usage: salamander-demo ui [dir] [--port N]");
                std::process::exit(2);
            });
        } else {
            dir = PathBuf::from(arg);
        }
    }
    (dir, port)
}

// ---------------------------------------------------------------- http --

struct Request {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    body: Vec<u8>,
}

fn handle(stream: TcpStream, db: &mut JsonDb) -> std::io::Result<()> {
    // The server is deliberately sequential, so an idle connection must
    // never block the loop: browsers open speculative sockets they may
    // never write to. Time out reads and treat a silent socket as no-op.
    stream.set_read_timeout(Some(std::time::Duration::from_secs(2)))?;
    let mut reader = BufReader::new(stream);
    let request = match read_request(&mut reader) {
        Ok(Some(request)) => request,
        Ok(None) => return Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) =>
        {
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let mut stream = reader.into_inner();

    let response = route(&request, db);
    let (status, content_type, body) = match response {
        Ok(RouteReply::Html(html)) => ("200 OK", "text/html; charset=utf-8", html.into_bytes()),
        Ok(RouteReply::Json(value)) => {
            ("200 OK", "application/json", value.to_string().into_bytes())
        }
        Ok(RouteReply::NotFound) => (
            "404 Not Found",
            "application/json",
            json!({"error": "not found"}).to_string().into_bytes(),
        ),
        Err(message) => (
            "400 Bad Request",
            "application/json",
            json!({ "error": message }).to_string().into_bytes(),
        ),
    };

    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    stream.flush()
}

fn read_request(reader: &mut BufReader<TcpStream>) -> std::io::Result<Option<Request>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 || header == "\r\n" || header == "\n" {
            break;
        }
        if let Some(value) = header
            .to_ascii_lowercase()
            .strip_prefix("content-length:")
            .map(str::trim)
            .and_then(|v| v.parse().ok())
        {
            content_length = value;
        }
    }
    // Refuse absurd bodies before allocating (the engine enforces its own
    // limits again below).
    if content_length > 4 * 1024 * 1024 {
        return Ok(None);
    }
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body)?;

    let (path, query_str) = match target.split_once('?') {
        Some((path, query)) => (path.to_string(), query),
        None => (target.clone(), ""),
    };
    let query = query_str
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            Some((key.to_string(), percent_decode(value)))
        })
        .collect();

    Ok(Some(Request {
        method,
        path,
        query,
        body,
    }))
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

// -------------------------------------------------------------- routes --

enum RouteReply {
    Html(String),
    Json(Value),
    NotFound,
}

fn route(request: &Request, db: &mut JsonDb) -> Result<RouteReply, String> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/") => Ok(RouteReply::Html(PAGE.to_string())),
        ("GET", "/api/state") => state(db).map(RouteReply::Json),
        ("GET", "/api/events") => events(request, db).map(RouteReply::Json),
        ("POST", "/api/append") => append(request, db).map(RouteReply::Json),
        ("POST", "/api/fork") => fork(request, db).map(RouteReply::Json),
        _ => Ok(RouteReply::NotFound),
    }
}

fn state(db: &JsonDb) -> Result<Value, String> {
    let mut branches = Vec::new();
    let mut queue = vec![BranchId::ZERO];
    while let Some(id) = queue.pop() {
        if let Some(info) = db.branch(id) {
            branches.push(branch_json(info));
        }
        for child in db.branch_children(id) {
            queue.push(child.id);
        }
    }
    Ok(json!({ "head": db.head(), "branches": branches }))
}

fn branch_json(info: &BranchInfo) -> Value {
    json!({
        "id": hex(info.id),
        "name": info.name.as_str(),
        "parent": info.parent.map(hex),
        "fork_position": info.fork_position,
        "status": format!("{:?}", info.status),
    })
}

fn events(request: &Request, db: &JsonDb) -> Result<Value, String> {
    let branch = branch_param(request)?;
    let mut reader = db
        .read(ReplayPlan {
            branch,
            ..ReplayPlan::default()
        })
        .map_err(|error| error.to_string())?;

    let mut out = Vec::new();
    loop {
        let record = match reader.next() {
            Ok(Some(record)) => record,
            Ok(None) => break,
            Err(error) => return Err(error.to_string()),
        };
        let stream = record
            .envelope
            .metadata
            .get("salamander.stream_name")
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .unwrap_or("?")
            .to_string();
        // Decode through the payload's declared codec; a payload this
        // build can't decode still renders as its byte length.
        let payload: Value = bincode::deserialize::<Json>(record.payload)
            .map(Json::into_inner)
            .unwrap_or_else(|_| json!({ "opaque_bytes": record.payload.len() }));
        out.push(json!({
            "position": record.position,
            "stream": stream,
            "branch": hex(record.envelope.branch_id),
            "timestamp_ms": record.envelope.timestamp_unix_nanos / 1_000_000,
            "payload": payload,
        }));
    }
    Ok(json!({ "events": out, "head": db.head() }))
}

fn append(request: &Request, db: &mut JsonDb) -> Result<Value, String> {
    let body: Value = serde_json::from_slice(&request.body).map_err(|error| error.to_string())?;
    let stream = body
        .get("stream")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("missing \"stream\"")?;
    let payload = body.get("payload").cloned().ok_or("missing \"payload\"")?;
    let branch = body
        .get("branch")
        .and_then(Value::as_str)
        .map(parse_hex)
        .transpose()?
        .unwrap_or(BranchId::ZERO);

    let position = db
        .append_on_branch(branch, stream, Json(payload))
        .map_err(|error| error.to_string())?;
    db.commit().map_err(|error| error.to_string())?;
    Ok(json!({ "position": position, "head": db.head() }))
}

fn fork(request: &Request, db: &mut JsonDb) -> Result<Value, String> {
    let body: Value = serde_json::from_slice(&request.body).map_err(|error| error.to_string())?;
    let parent = body
        .get("branch")
        .and_then(Value::as_str)
        .map(parse_hex)
        .transpose()?
        .unwrap_or(BranchId::ZERO);
    let at = body
        .get("at")
        .and_then(Value::as_u64)
        .ok_or("missing \"at\"")?;
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("missing \"name\"")?;

    let info = db
        .fork_branch(
            parent,
            at,
            BranchName::new(name).map_err(|error| error.to_string())?,
            Metadata::new(),
        )
        .map_err(|error| error.to_string())?;
    Ok(json!({ "branch": branch_json(&info) }))
}

fn branch_param(request: &Request) -> Result<BranchId, String> {
    request
        .query
        .iter()
        .find(|(key, _)| key == "branch")
        .map(|(_, value)| parse_hex(value))
        .transpose()
        .map(|id| id.unwrap_or(BranchId::ZERO))
}

fn hex(id: BranchId) -> String {
    id.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

fn parse_hex(value: &str) -> Result<BranchId, String> {
    if value.len() != 32 {
        return Err(format!("bad branch id {value:?}"));
    }
    let mut bytes = [0u8; 16];
    for (i, chunk) in value.as_bytes().chunks(2).enumerate() {
        let pair = std::str::from_utf8(chunk).map_err(|_| "bad branch id".to_string())?;
        bytes[i] = u8::from_str_radix(pair, 16).map_err(|_| "bad branch id".to_string())?;
    }
    Ok(BranchId::from_bytes(bytes))
}
