#!/usr/bin/env python3
"""
clusterweather: the weather on all your Slurm clusters at once, to decide where to work today.

Runs on your laptop. For each cluster it opens one SSH connection (you approve MFA once),
keeps it open, and runs slurmdash on the login node through it. slurmdash is sent over
the connection each time, so nothing needs installing on the clusters; it queries Slurm
there and sends back one small JSON summary per refresh.

  clusterweather              log in where needed, then show the forecast
  clusterweather setup        create or edit your profile
  clusterweather login fir    reopen the connection to one cluster
  clusterweather logout       close every connection
  clusterweather --demo       simulated clusters, no SSH

Keys: 1-9 open a cluster's full dashboard · w or Esc back to the forecast · Tab next
cluster · l log in again · q quit (connections stay open for next time)

Needs Python 3.8+, rich and OpenSSH (macOS, Linux, or WSL on Windows). slurmdash ships in the
same package.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import select
import shlex
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path

from . import __version__
from . import slurmdash as sd

SLURMDASH = Path(sd.__file__)                    # sent to each login node and run there as a script
if not sd.HAVE_RICH:
    sys.exit("clusterweather needs the 'rich' package:  pip install rich")
try:
    import termios
    import tty
except ImportError:                              # native Windows
    termios = tty = None

from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

PROFILE = Path(os.environ.get("CLUSTERWEATHER_PROFILE", "~/.config/clusterweather/profile.json")).expanduser()
OLD_PROFILE = Path("~/.config/slurmweather/profile.json").expanduser()      # from before the rename
KNOWN_HOSTS = {"fir": "fir.alliancecan.ca", "killarney": "killarney.alliancecan.ca",
               "vulcan": "vulcan.alliancecan.ca", "trillium": "trillium-gpu.alliancecan.ca",
               "nibi": "nibi.alliancecan.ca", "rorqual": "rorqual.alliancecan.ca",
               "narval": "narval.alliancecan.ca"}
CLUSTER_EMOJI = {"fir": "🌲", "killarney": "🍀", "vulcan": "🌋", "trillium": "🌸", "nibi": "💧",
                 "rorqual": "🐋", "narval": "🐳"}
# (upper bound on the estimated wait for a new job, icon, word)
FORECAST = [(0.25, "🌞", "clear"), (2.0, "⛅", "cloudy"), (12.0, "☔", "rainy"), (float("inf"), "🌀", "stormy")]
CYAN, GREEN, YELLOW, RED, DIM, MUTED = sd.CYAN, sd.GREEN, sd.YELLOW, sd.RED, sd.DIM, sd.MUTED


# ------------------------------------------------------------------------ profile
def ask(question, default=None):
    hint = f" [{default}]" if default else ""
    answer = input(f"{question}{hint}: ").strip()
    return answer or (default or "")


def load_profile():
    if not PROFILE.exists() and OLD_PROFILE.exists() and "CLUSTERWEATHER_PROFILE" not in os.environ:
        PROFILE.parent.mkdir(parents=True, exist_ok=True)
        PROFILE.write_text(OLD_PROFILE.read_text())
    try:
        return json.loads(PROFILE.read_text())
    except (OSError, ValueError):
        return None


def setup(existing=None):
    p = existing or {}
    print("\nclusterweather profile (Enter keeps the value in brackets)\n")
    lab = ask("Your lab (a Slurm account such as aip-yourpi or def-yourpi)", p.get("lab"))
    user = ask("Your cluster username", p.get("user") or os.environ.get("USER"))
    known = ", ".join(KNOWN_HOSTS)
    names = ask(f"Clusters to monitor, comma-separated (known: {known})",
                ", ".join(c["name"] for c in p.get("clusters", [])) or "fir, killarney")
    old = {c["name"]: c for c in p.get("clusters", [])}
    clusters = []
    for name in [n.strip().lower() for n in names.split(",") if n.strip()]:
        prev = old.get(name, {})
        host = ask(f"  {name}: login host or ssh alias",
                   prev.get("host") or KNOWN_HOSTS.get(name) or f"{name}.alliancecan.ca")
        acct = ask(f"  {name}: Slurm account (Enter = work it out from your lab)", prev.get("account") or "")
        clusters.append({"name": name, "host": host, "account": acct or None})
    refresh = ask("Refresh every N seconds (60 is gentle on the schedulers)", str(p.get("refresh", 60)))
    profile = {"lab": lab, "user": user, "clusters": clusters, "refresh": max(30, int(refresh or 60))}
    PROFILE.parent.mkdir(parents=True, exist_ok=True)
    PROFILE.write_text(json.dumps(profile, indent=2) + "\n")
    print(f"\nSaved to {PROFILE}\n")
    return profile


# ---------------------------------------------------------------------- clusters
class Cluster:
    """One cluster: its SSH control master, the slurmdash stream and the latest snapshot."""

    def __init__(self, spec, profile, demo=False):
        self.name, self.host, self.account = spec["name"], spec["host"], spec.get("account")
        self.user, self.lab, self.demo = profile["user"], profile["lab"], demo
        self.refresh = profile.get("refresh", 60)
        self.extra = spec.get("extra_args", [])
        self.emoji = CLUSTER_EMOJI.get(self.name, "🧮")
        self.state = sd.State(self.user, self.refresh)
        self.state.cluster = self.name
        self.state.note = None
        self.proc, self.status, self.err = None, "connecting", None
        self.stderr = deque(maxlen=20)
        self.last, self.next_try, self.failures = None, 0.0, 0

    @property
    def target(self):
        return self.host if "@" in self.host else f"{self.user}@{self.host}"

    @property
    def ctl(self):                                # short path: macOS caps socket paths at 104 chars
        ssh = Path("~/.ssh").expanduser()
        new, old = ssh / f"cw-{self.name}", ssh / f"sw-{self.name}"   # sw-: opened before the rename
        return str(old if old.exists() and not new.exists() else new)

    # -- SSH control master -----------------------------------------------------
    def master_alive(self):
        if self.demo:
            return True
        r = subprocess.run(["ssh", "-S", self.ctl, "-O", "check", self.target], capture_output=True)
        return r.returncode == 0

    def login(self):
        """Open the master in the foreground, so you can answer the password and MFA prompts.
        ControlPersist=yes keeps it open until it's closed or the network drops."""
        if self.demo:
            return True
        print(f"\n{self.emoji}  Logging into {self.name} ({self.target}). Approve the MFA prompt when it appears.")
        Path(self.ctl).parent.mkdir(mode=0o700, exist_ok=True)
        r = subprocess.run(["ssh", "-M", "-S", self.ctl, "-o", "ControlPersist=yes",
                            "-o", "ServerAliveInterval=60", "-o", "ServerAliveCountMax=5", "-fN", self.target])
        ok = r.returncode == 0
        self.status, self.next_try, self.failures = ("connecting" if ok else "needs login"), 0.0, 0
        return ok

    def logout(self):
        if not self.demo:
            subprocess.run(["ssh", "-S", self.ctl, "-O", "exit", self.target], capture_output=True)

    # -- the slurmdash stream ---------------------------------------------------
    def start(self):
        args = ["stream", "-u", self.user, "--lab", self.lab, "--refresh", str(self.refresh)]
        args += (["-A", self.account] if self.account else []) + list(self.extra)
        if self.demo:
            cmd = [sys.executable, str(SLURMDASH), *args, "--demo", "--demo-flavor", self.name]
        else:
            inner = 'PY=$(command -v python3 || echo /usr/bin/python3); exec "$PY" - ' + \
                    " ".join(shlex.quote(a) for a in args)
            # a login shell, so PATH matches an interactive login (some clusters only add
            # Slurm's commands to PATH in their login scripts); stray output is ignored
            remote = "bash -lc " + shlex.quote(inner)
            cmd = ["ssh", "-S", self.ctl, "-o", "ControlMaster=no", "-o", "BatchMode=yes", self.target, remote]
        self.proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL if self.demo else subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        if not self.demo:                         # `python3 -` runs the script it reads from stdin
            self.proc.stdin.write(SLURMDASH.read_text())
            self.proc.stdin.close()
        self.status, self.err = "connecting", None
        threading.Thread(target=self._read, args=(self.proc,), daemon=True).start()
        threading.Thread(target=self._read_err, args=(self.proc,), daemon=True).start()

    def _read(self, proc):
        for line in proc.stdout:
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            self._apply(msg)

    def _read_err(self, proc):
        for line in proc.stderr:
            if line.strip():
                self.stderr.append(line.rstrip())

    def _apply(self, msg):
        st = self.state
        charts = None
        if msg.get("ok"):
            try:
                charts = sd.make_charts(msg["snap"], msg.get("hist") or [])
            except Exception as e:                # a bad chart shouldn't hide the rest
                self.stderr.append(f"charts: {e}")
        with st.lock:
            if msg.get("ok"):
                st.snap, st.err, st.ok_ts, st.charts = msg["snap"], None, time.time(), charts
                st.hint = tuple(msg.get("hint") or st.hint)
                st.note = msg.get("note")
            else:
                st.err = msg.get("err")
        self.err = None if msg.get("ok") else msg.get("err")
        self.status = "live" if msg.get("ok") or st.snap is not None else "error"
        self.last, self.failures = time.time(), 0

    def tick(self):
        """Called every few seconds: restart a dead stream, or flag that a login is needed."""
        if self.proc is not None and self.proc.poll() is None:
            if self.last and time.time() - self.last > 3 * self.refresh + 90:
                self.status = "stale"
            return
        if self.proc is not None:                 # the stream ended
            tail = [line for line in self.stderr if "Warning" not in line][-2:]
            self.err = " ".join(tail)[-200:] or "connection closed"
            self.proc, self.failures = None, self.failures + 1
            self.next_try = time.time() + min(300, 20 * self.failures)
        if time.time() < self.next_try or self.status == "needs login":
            return
        if self.master_alive():
            self.start()
        else:
            self.status = "needs login"

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()


# --------------------------------------------------------------------- forecast
def forecast(h):
    if h is None:
        return "❓", "unknown"
    return next((icon, word) for limit, icon, word in FORECAST if h < limit)


def full_types(snap):
    """GPU kinds worth recommending: full GPUs, or MIG slices if that's all there is."""
    types = snap["type_order"]
    return [t for t in types if not sd.is_mig(t)] or types


def best_bets(clusters):
    """Every (cluster, GPU kind) we have data for, quickest estimated start first."""
    cands = []
    for cl in clusters:
        snap = cl.state.snap
        if not snap or cl.status not in ("live", "stale"):
            continue
        for t in full_types(snap):
            T = snap["types"][t]
            wait = T["wait_h"] if T["wait_h"] is not None else 1e9
            cands.append((wait, -T["free"] * sd.gpu_frac(t), cl, t, T))
    return sorted(cands, key=lambda c: (c[0], c[1]))


def start_words(h):
    wt, _ = sd.wait_text(h)
    return "now" if wt == "<15 min" else f"in {wt}"


def p_top(clusters, profile):
    bets = best_bets(clusters)
    if bets:
        wait, _, cl, t, T = bets[0]
        icon, _ = forecast(T["wait_h"])
        msg = (f"{sd.E(icon + ' ')}Best bet now: {cl.name.upper()} {sd.pretty_type(t)} · {T['free']} free · "
               f"a new job of yours would start {start_words(T['wait_h'])}")
        other = next((b for b in bets[1:] if b[2] is not cl), None)
        if other:
            msg += f"   (next: {other[2].name.upper()} {sd.pretty_type(other[3])}, {start_words(other[4]['wait_h'])})"
        bg = "#2e6b34" if wait < 2 else ("#8a6d00" if wait < 12 else "#7a2e2e")
    else:
        msg, bg = "Waiting for the first forecasts…", "grey30"
    g = Table.grid(expand=True, padding=(0, 2))
    g.add_column(no_wrap=True)
    g.add_column(ratio=1)
    g.add_column(justify="right", no_wrap=True)
    g.add_row(Text.assemble((sd.E("⛅ ") + "clusterweather ", f"bold {CYAN}"), (profile["user"], "bold"),
                            (" · ", DIM), (profile["lab"], "")),
              Align.center(Text(msg, style="bold white", no_wrap=True, overflow="ellipsis"), style=f"on {bg}"),
              Text(datetime.now().strftime("%a %H:%M:%S"), style="bold"))
    return Panel(g, border_style=sd.BLUE, padding=(0, 1))


def p_card(i, cl):
    st = cl.state
    with st.lock:
        snap, hint, note = st.snap, st.hint, getattr(st, "note", None)
    head = Text.assemble((f"[{i}] ", DIM), (sd.E(cl.emoji + " ") + cl.name.upper(), f"bold {CYAN}"),
                         (f"  {snap['account']}" if snap else "", MUTED))
    status = {"live": (f"● live, {time.time() - cl.last:.0f}s ago" if cl.last else "● live", GREEN),
              "stale": ("● stale, still waiting for an update", YELLOW),
              "connecting": ("● connecting…", MUTED),
              "error": ("● connected, but Slurm can't be read there", RED),
              "needs login": (sd.E("🔌 ") + "connection closed: press l to log in", RED)}.get(cl.status, ("", DIM))
    sub = Text(status[0], style=status[1])
    if not snap:
        body = Text(cl.err or ("starting slurmdash on the login node…" if cl.status != "needs login"
                               else "no connection"), style=RED if cl.err else DIM)
        return Panel(body, title=head, subtitle=sub, border_style=sd.FRAME, padding=(0, 1))

    g = Table.grid(padding=(0, 2))
    for _ in range(5):
        g.add_column(no_wrap=True)
    for t in snap["type_order"]:
        T = snap["types"][t]
        icon, word = forecast(T["wait_h"])
        wt, wc = sd.wait_text(T["wait_h"])
        g.add_row(sd.E(icon) or word, Text(sd.pretty_type(t), style="bold" if not sd.is_mig(t) else MUTED),
                  Text(f"{T['free']:,}/{T['total']:,} free", style=GREEN if T["free"] else RED),
                  Text(f"{T['pending']:,} pending", style=MUTED),
                  Text(f"start {start_words(T['wait_h'])}", style=f"bold {wc}"))
    if not snap["type_order"]:
        g.add_row("", Text("no GPU partitions", style=DIM), "", "", "")

    f, a = snap["fs"], snap["acct"] or {}
    run = sd.my_running(snap)
    pend = [j for j in snap["myjobs"] if j["st"] == "PD"]
    lines = [Text.assemble(("you  ", MUTED), (f"fairshare {f['factor']:.3f}", sd.fair_style(f["factor"])),
                           ("  ·  ", DIM), (f"{f['xs']:.2f}x your share" + sd.E(sd.hog(f["xs"])), sd.xs_style(f["xs"])),
                           ("  ·  ", DIM), (f"{len(run)} running ({sum(j['gpus'] for j in run)} GPUs), "
                                            f"{len(pend)} pending", ""))]
    ours = next((r for r in snap.get("labs", []) if r["acct"] == snap["account"]), None)
    lab = Text.assemble(("lab  ", MUTED), (f"{a.get('xs', 0):.1f}x its share" + sd.E(sd.hog(a.get("xs", 0))),
                                           sd.xs_style(a.get("xs", 0))))
    if ours and ours.get("gpuh") is not None:
        lab.append(f"  ·  #{ours['rank']} of {snap.get('labs_total', '?')} labs by GPU-hours (30 d)", style=MUTED)
    lines.append(lab)
    if hint and hint[1] != "grey30":
        lines.append(Text(f" {hint[0]} ", style=f"bold white on {hint[1]}"))
    if st.err or cl.err:
        lines.append(Text(f"last update failed: {st.err or cl.err}"[:160], style=YELLOW))
    return Panel(Group(g, Text(""), *lines), title=head, subtitle=sub, border_style=sd.GREEN if cl.status == "live"
                 else sd.FRAME, padding=(0, 1))


def weather_view(console, clusters, profile):
    W, H = console.size
    cols = 2 if W >= 150 and len(clusters) > 1 else 1
    grid = Table.grid(expand=True, padding=(0, 1))
    for _ in range(cols):
        grid.add_column(ratio=1)
    cards = [p_card(i, cl) for i, cl in enumerate(clusters, 1)]
    for k in range(0, len(cards), cols):
        row = cards[k:k + cols]
        grid.add_row(*row, *[""] * (cols - len(row)))
    root = Layout()
    root.split_column(Layout(p_top(clusters, profile), size=3), Layout(grid),
                      Layout(Text("1-9 open a cluster's dashboard  ·  Tab next  ·  w back  ·  l log in  ·  q quit",
                                  style=DIM, justify="center"), size=1))
    return root


def cluster_view(console, cl, i, n):
    W, H = console.size
    footer = Text(f"{cl.name.upper()} ({i}/{n})  ·  Tab next cluster  ·  w or Esc back to the forecast  ·  q quit",
                  style=DIM, justify="center")
    root = Layout()
    if cl.state.snap is None:
        body = Panel(Align.center(Text(cl.err or "waiting for the first update…", style=DIM), vertical="middle"),
                     title=cl.name.upper(), border_style=sd.FRAME)
    else:
        body = sd.build_layout(console, cl.state, size=(W, H - 1))
    root.split_column(Layout(body), Layout(footer, size=1))
    return root


# ------------------------------------------------------------------- keyboard
class Keys:
    """Single keypresses without Enter. Paused while ssh needs the terminal for a login."""

    def __init__(self):
        self.fd = sys.stdin.fileno()
        self.saved = termios.tcgetattr(self.fd)
        self.q, self.on = queue.Queue(), threading.Event()
        self.resume()
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            if not self.on.is_set():
                time.sleep(0.05)
                continue
            ready, _, _ = select.select([self.fd], [], [], 0.1)
            if ready and self.on.is_set():
                try:
                    self.q.put(os.read(self.fd, 1).decode(errors="ignore"))
                except OSError:
                    pass

    def drain(self):
        out = []
        while not self.q.empty():
            out.append(self.q.get_nowait())
        return out

    def pause(self):
        self.on.clear()
        time.sleep(0.15)                           # let the reader finish its last select()
        termios.tcsetattr(self.fd, termios.TCSADRAIN, self.saved)

    def resume(self):
        tty.setcbreak(self.fd)
        self.on.set()


# ------------------------------------------------------------------------- main
def supervise(clusters, paused, stop):
    while not stop.is_set():
        if not paused.is_set():
            for cl in clusters:
                try:
                    cl.tick()
                except Exception as e:
                    cl.err = f"{type(e).__name__}: {e}"
        stop.wait(3)


def run_ui(clusters, profile, paused):
    console = Console()
    keys = Keys() if sys.stdin.isatty() and termios else None
    view = None                                    # None = the forecast; otherwise a cluster index
    try:
        with Live(console=console, screen=True, auto_refresh=False) as live:
            while True:
                for ch in (keys.drain() if keys else []):
                    if ch in "qQ":
                        return
                    if ch.isdigit() and 0 < int(ch) <= len(clusters):
                        view = int(ch) - 1
                    elif ch in ("w", "W", "0", "\x1b"):
                        view = None
                    elif ch == "\t":
                        view = 0 if view is None else (view + 1) % len(clusters)
                    elif ch in "lL":
                        live.stop()
                        keys.pause()
                        paused.set()
                        for cl in clusters:
                            if not cl.master_alive():
                                cl.login()
                            else:
                                cl.status, cl.next_try = ("connecting" if cl.status == "needs login" else cl.status), 0
                        paused.clear()
                        keys.resume()
                        live.start()
                frame = (weather_view(console, clusters, profile) if view is None
                         else cluster_view(console, clusters[view], view + 1, len(clusters)))
                live.update(frame, refresh=True)
                time.sleep(0.25)
    finally:
        if keys:
            keys.pause()


def main():
    ap = argparse.ArgumentParser(description="Forecast GPU queues on several Slurm clusters.")
    ap.add_argument("mode", nargs="?", default="run", choices=["run", "setup", "login", "logout"])
    ap.add_argument("clusters", nargs="*", help="limit to these clusters (default: all in your profile)")
    ap.add_argument("--demo", action="store_true", help="simulated clusters, no SSH")
    ap.add_argument("--version", action="version", version=f"clusterweather {__version__}")
    ap.add_argument("--serious", action="store_true", help="no emojis")
    ap.add_argument("--once", action="store_true", help="print one forecast and exit (waits for every cluster)")
    ap.add_argument("--view", help="with --once: print this cluster's full dashboard instead")
    args = ap.parse_args()
    sd.FUN = not args.serious

    if args.demo:
        profile = {"lab": "aip-demo", "user": os.environ.get("USER", "you"), "refresh": 5,
                   "clusters": [{"name": n, "host": f"{n}.example"} for n in ("fir", "killarney", "vulcan", "trillium")]}
    elif args.mode == "setup" or load_profile() is None:
        profile = setup(load_profile())
        if args.mode == "setup":
            return
    else:
        profile = load_profile()
    clusters = [Cluster(c, profile, args.demo) for c in profile["clusters"]
                if not args.clusters or c["name"] in args.clusters]
    if not clusters:
        sys.exit("no matching clusters in your profile (run: clusterweather setup)")

    if args.mode == "logout":
        for cl in clusters:
            cl.logout()
            print(f"closed {cl.name}")
        return
    for cl in clusters:                            # one MFA prompt per cluster, then never again
        if not cl.master_alive():
            cl.login()
    if args.mode == "login":
        return

    paused, stop = threading.Event(), threading.Event()
    threading.Thread(target=supervise, args=(clusters, paused, stop), daemon=True).start()
    try:
        if args.once:
            deadline = time.time() + 90
            while time.time() < deadline and any(cl.state.snap is None and cl.status != "needs login"
                                                 for cl in clusters):
                time.sleep(0.5)
            console = Console()
            if args.view:
                k = next(i for i, cl in enumerate(clusters) if cl.name == args.view)
                console.print(cluster_view(console, clusters[k], k + 1, len(clusters)))
            else:
                console.print(weather_view(console, clusters, profile))
        else:
            run_ui(clusters, profile, paused)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        for cl in clusters:
            cl.stop()                              # the streams stop; the SSH connections stay open
        if not args.demo and not args.once:
            print("Connections stay open for next time. Close them with:  clusterweather logout")


if __name__ == "__main__":
    main()
