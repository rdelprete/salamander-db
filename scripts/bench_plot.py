#!/usr/bin/env python3
"""Turn `cargo bench` (criterion) output into a time-vs-log-size chart.

Reads criterion's per-benchmark `new/estimates.json` files, prints a table
of mean times, and (if matplotlib is available) saves a line chart.

Usage:
    cargo bench -p salamander-db --bench open_time
    python scripts/bench_plot.py target/criterion assets/open_time.png
"""
import json
import sys
from collections import defaultdict
from pathlib import Path


def collect(criterion_dir: Path) -> dict[str, dict[int, float]]:
    """bench name -> {event_count: mean_nanoseconds}.

    Criterion lays results out as
      <criterion_dir>/<group>/<bench>/<size>/new/estimates.json
    We only care about size dirs whose name is an integer event count.
    """
    data: dict[str, dict[int, float]] = defaultdict(dict)
    for est in criterion_dir.rglob("new/estimates.json"):
        size_dir = est.parent.parent
        bench = size_dir.parent.name
        try:
            size = int(size_dir.name)
        except ValueError:
            continue  # e.g. a non-parameterized benchmark; skip
        with est.open() as fh:
            mean_ns = json.load(fh)["mean"]["point_estimate"]
        data[bench][size] = mean_ns
    return data


def print_table(data: dict[str, dict[int, float]]) -> None:
    print(f"{'benchmark':<20} {'events':>12} {'mean':>12}")
    print("-" * 46)
    for bench in sorted(data):
        for size in sorted(data[bench]):
            ms = data[bench][size] / 1e6
            print(f"{bench:<20} {size:>12,} {ms:>10.2f} ms")


def plot(data: dict[str, dict[int, float]], out_path: Path) -> bool:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print(
            "note: matplotlib not installed; skipping the chart "
            "(pip install matplotlib to render it).",
            file=sys.stderr,
        )
        return False

    fig, ax = plt.subplots(figsize=(7, 4.5))
    for bench in sorted(data):
        points = sorted(data[bench].items())
        xs = [p[0] for p in points]
        ys = [p[1] / 1e6 for p in points]  # ns -> ms
        ax.plot(xs, ys, marker="o", label=bench)

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("events in log")
    ax.set_ylabel("cold-start time (ms)")
    ax.set_title("SalamanderDB — open time vs. log size")
    ax.grid(True, which="both", ls="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120)
    print(f"wrote {out_path}")
    return True


def main() -> None:
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <criterion-dir> <out.png>", file=sys.stderr)
        sys.exit(2)

    criterion_dir = Path(sys.argv[1])
    out_path = Path(sys.argv[2])
    if not criterion_dir.is_dir():
        print(f"error: {criterion_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    data = collect(criterion_dir)
    if not data:
        print(
            f"error: no criterion estimates found under {criterion_dir}. "
            "Run `cargo bench --bench open_time` first.",
            file=sys.stderr,
        )
        sys.exit(1)

    print_table(data)
    plot(data, out_path)


if __name__ == "__main__":
    main()
