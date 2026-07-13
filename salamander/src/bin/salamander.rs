use std::path::PathBuf;

fn main() {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("migrate") => {
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
        Some("migrate-branches") => {
            let source = args.next().map(PathBuf::from).unwrap_or_else(|| usage());
            let destination = args.next().map(PathBuf::from).unwrap_or_else(|| usage());
            if args.next().is_some() {
                usage();
            }
            match salamander::migrate_legacy_branches(&source, &destination) {
                Ok(report) => println!(
                    "branch migration complete: source_records={} migrated={} markers_removed={} branches={} destination_head={}",
                    report.source_records,
                    report.migrated_records,
                    report.removed_marker_records,
                    report.branches_created,
                    report.destination_head
                ),
                Err(error) => {
                    eprintln!("branch migration failed: {error}");
                    std::process::exit(1);
                }
            }
        }
        _ => usage(),
    }
}

fn usage() -> ! {
    eprintln!("usage: salamander <migrate|migrate-branches> <source> <destination>");
    std::process::exit(2);
}
