//! IMPLEMENTATION.md §1 — demo + crash-harness binary. Subcommands:
//! `session` (M3 flagship), `storm` (M1/M2 write-storm fixture generator),
//! `crashtest child|parent` (M4 conscience harness), `ui` (local
//! playground: browser UI over a JsonDb).

mod crash;
mod migrate;
mod session;
mod storm;
mod ui;

use std::env;

fn main() {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("session") => session::run(),
        Some("storm") => storm::run(args),
        Some("crashtest") => crash::run(args),
        Some("migrate") => migrate::run(args),
        Some("ui") => ui::run(args),
        _ => {
            eprintln!("usage: salamander-demo <session|storm|crashtest|migrate|ui> [args]");
            std::process::exit(2);
        }
    }
}
