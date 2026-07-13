#!/usr/bin/env bash
# Record the session demo as the README GIF.
#
# Produces assets/session-demo.gif from `salamander-demo session`. Needs
# asciinema (capture) and agg (asciinema-gif-generator, the cast->gif
# renderer):
#   cargo install --locked agg
#   pipx install asciinema      # or: brew install asciinema
#
# The README embeds the recording; until it's regenerated, the README also
# carries the captured text output inline (a <details> block), so nothing is
# broken if the GIF is absent.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

command -v asciinema >/dev/null || { echo "error: asciinema not found (pipx install asciinema)"; exit 1; }
command -v agg >/dev/null || { echo "error: agg not found (cargo install --locked agg)"; exit 1; }

CAST="$(mktemp -t salamander-demo-XXXX.cast)"
OUT="assets/session-demo.gif"
mkdir -p assets

# Build first so the compile output isn't part of the recording.
cargo build --release -p salamander-demo

echo "Recording the session demo…"
asciinema rec --overwrite --command "./target/release/salamander-demo session" "$CAST"

echo "Rendering $OUT…"
agg --theme monokai --font-size 16 "$CAST" "$OUT"
rm -f "$CAST"

echo "Wrote $OUT — embed it in README.md with:"
echo '  ![SalamanderDB session demo](assets/session-demo.gif)'
