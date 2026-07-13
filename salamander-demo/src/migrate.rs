use std::path::PathBuf;

pub fn run(mut args: impl Iterator<Item = String>) {
    let source = args.next().map(PathBuf::from).unwrap_or_else(|| usage());
    let destination = args.next().map(PathBuf::from).unwrap_or_else(|| usage());
    if args.next().is_some() {
        usage();
    }
    match salamander::migrate_v1(&source, &destination) {
        Ok(report) => println!(
            "migration complete: source_records={} previous={} imported={} destination_head={}",
            report.source_records,
            report.previously_imported,
            report.newly_imported,
            report.destination_head
        ),
        Err(error) => {
            eprintln!("migration failed: {error}");
            std::process::exit(1);
        }
    }
}

fn usage() -> ! {
    eprintln!("usage: salamander-demo migrate <source-v1> <destination-v2>");
    std::process::exit(2);
}
