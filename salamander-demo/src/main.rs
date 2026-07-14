//! Demo and crash-harness binary. Subcommands: `session` (branching agent
//! session), `storm` (large write/reopen fixture), `crashtest worker|parent`
//! (real-process recovery harness), and `ui` (local JsonDb playground).

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
