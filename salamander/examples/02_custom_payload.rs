//! Bring your own payload. Run with:
//!
//!     cargo run --example 02_custom_payload
//!
//! The engine is generic over the event type (`Salamander<B>`). Define your
//! own event enum and your own projection — a deterministic fold — with no
//! agent vocabulary in sight. Here: a tiny account ledger.

use salamander::{Event, Projection, Salamander};
use serde::{Deserialize, Serialize};

/// Your domain events. Any `serde` type that is `Clone + 'static` works.
#[derive(Clone, Serialize, Deserialize)]
enum Ledger {
    Deposit(i64),
    Withdraw(i64),
}

/// Your projection: fold the ledger into a running balance (in cents).
#[derive(Default)]
struct Balance {
    cents: i64,
    cursor: u64,
}

impl Projection for Balance {
    type Body = Ledger; // ties this projection to a `Salamander<Ledger>`
    type State = i64;

    fn apply(&mut self, e: &Event<Ledger>) {
        match &e.body {
            Ledger::Deposit(c) => self.cents += c,
            Ledger::Withdraw(c) => self.cents -= c,
        }
        self.cursor = e.offset + 1;
    }

    fn cursor(&self) -> u64 {
        self.cursor
    }

    fn state(&self) -> &i64 {
        &self.cents
    }
}

fn main() -> salamander::Result<()> {
    let dir = fresh_dir("custom_payload");

    let mut db: Salamander<Ledger> = Salamander::open(&dir)?;
    db.append("acct", Ledger::Deposit(10_000))?; // offset 0
    db.append("acct", Ledger::Withdraw(2_500))?; // offset 1
    db.append("acct", Ledger::Deposit(1_000))?; //  offset 2
    db.commit()?;

    let balance: Balance = db.projection()?;
    println!("balance after replay:      {} cents", balance.state());

    // Same log, folded only up to offset 2 → the balance two events ago.
    let earlier: Balance = db.view_at(2)?;
    println!("balance as of offset 2:    {} cents", earlier.state());

    Ok(())
}

fn fresh_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("salamander-example-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}
