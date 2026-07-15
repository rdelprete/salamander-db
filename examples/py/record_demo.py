"""Regenerate the dungeon demo video (README asset).

Starts a fresh dungeon server, drives the "money loop" — fight, rewind,
fork, diverge, pull-the-plug, reload-intact — records it in real time with
Playwright, and transcodes the result to MP4. Deterministic: a fixed seed
plus a fixed script means the same video every run.

    pip install playwright imageio-ffmpeg
    playwright install chromium
    python examples/py/record_demo.py            # -> docs/assets/dungeon-demo.mp4

Re-run whenever the UI changes; commit the resulting MP4.
"""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
DEFAULT_OUT = REPO / "docs" / "assets" / "dungeon-demo.mp4"

VIEWPORT = {"width": 1000, "height": 720}


def start_server(data_dir, port, seed):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(HERE)
    proc = subprocess.Popen(
        [sys.executable, str(HERE / "dungeon.py"), data_dir,
         "--port", str(port), "--seed", str(seed)],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    _wait_up(port)
    return proc


def _wait_up(port, timeout=15):
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/state", timeout=1).read()
            return
        except Exception:
            time.sleep(0.15)
    raise RuntimeError("dungeon server did not come up")


def drive(page, port, data_dir, seed, restart_server):
    """The choreography. Each `beat` is a labelled pause so timing is easy
    to read and tune."""
    def beat(seconds):
        page.wait_for_timeout(int(seconds * 1000))

    def move(key, n=1, pause=0.65):
        page.locator("#board").click()  # focus the page for key events
        for _ in range(n):
            page.keyboard.press(key)
            beat(pause)

    def scrub_to(turn):
        top = int(page.get_attribute("#slider", "max"))
        for t in range(top, turn - 1, -1):     # step down for a rewind feel
            page.eval_on_selector(
                "#slider",
                "(el, v) => { el.value = String(v); el.dispatchEvent(new Event('input')); }",
                t)
            beat(0.18)
        beat(1.0)

    page.goto(f"http://127.0.0.1:{port}/")
    page.wait_for_selector("#board .cell")
    beat(1.6)                                   # 1. establish the dungeon

    move("ArrowDown", 2)                        # 2. venture in, pick a fight
    move("ArrowRight", 4)
    beat(1.0)

    scrub_to(2)                                 # 3. rewind — the world un-happens

    page.click("#forkbtn")                      # 4. branch a new future
    beat(1.4)

    move("ArrowDown", 3)                        # 5. play a different line
    beat(1.2)

    # 6. show both timelines diverging: flip on the ghost of main
    page.check("#ghost")
    page.select_option("#ghostsel", "main")
    beat(2.0)

    # 7. pull the plug — os._exit mid-write
    page.once("dialog", lambda d: d.accept())
    page.click("#crash")
    page.wait_for_selector("text=killed mid-write")
    beat(2.2)

    # 8. relaunch and reload: the run — every timeline — is intact
    restart_server()
    page.reload()
    page.wait_for_selector("#board .cell")
    beat(2.4)


def transcode(webm, out):
    out.parent.mkdir(parents=True, exist_ok=True)
    ffmpeg = _ffmpeg_exe()
    cmd = [ffmpeg, "-y", "-i", str(webm),
           "-movflags", "+faststart", "-pix_fmt", "yuv420p",
           "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
           "-c:v", "libx264", "-crf", "23", str(out)]
    subprocess.run(cmd, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _ffmpeg_exe():
    if shutil.which("ffmpeg"):
        return "ffmpeg"
    import imageio_ffmpeg
    return imageio_ffmpeg.get_ffmpeg_exe()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=7173)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args(argv)

    data_dir = os.path.join(tempfile.gettempdir(), "dungeon-record")
    shutil.rmtree(data_dir, ignore_errors=True)
    video_dir = Path(tempfile.mkdtemp(prefix="dungeon-video-"))

    server = start_server(data_dir, args.port, args.seed)

    def restart_server():
        nonlocal server
        if server.poll() is None:
            server.kill()
        # The crash left the port free and a stale LOCK; the server clears it.
        server = start_server(data_dir, args.port, args.seed)

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            context = browser.new_context(
                viewport=VIEWPORT,
                record_video_dir=str(video_dir),
                record_video_size=VIEWPORT)
            page = context.new_page()
            drive(page, args.port, data_dir, args.seed, restart_server)
            page.wait_for_timeout(300)
            context.close()          # finalizes the .webm
            webm = next(video_dir.glob("*.webm"))
            browser.close()
    finally:
        if server.poll() is None:
            server.kill()

    transcode(webm, args.out)
    shutil.rmtree(video_dir, ignore_errors=True)
    print(f"wrote {args.out} ({args.out.stat().st_size // 1024} KiB)")


if __name__ == "__main__":
    main()
