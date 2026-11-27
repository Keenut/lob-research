r"""check_continuity.py: verify Binance depth-stream sequence continuity in the raw recorder logs.

Usage (PowerShell):
    python check_continuity.py data\raw\BTCUSDT\*.log*

The pattern *.log* takes both plain (.log) and compressed (.log.gz, see compress_raw.py) hours;
files are processed in hour order whatever order they are given in.

Within each session (one WebSocket connection) it applies the Binance local order book rules:
  1. Events that arrive before the first usable snapshot are buffered.
  2. With a snapshot (lastUpdateId = L): drop buffered events with u <= L.
     The first remaining event must satisfy U <= L + 1 <= u (after the drop, u > L, so U <= L + 1 is enough).
     If its U > L + 1 the snapshot is too old: wait for a newer snapshot.
  3. Once synced, each event must continue the sequence: U == last_u + 1.
       u <= last_u     -> stale/duplicate, ignored
       U >  last_u + 1 -> GAP (events were missed)
       U <  last_u + 1 -> overlap (allowed by the docs, counted)
     After a gap we keep checking from the new event, so every gap in the file is reported.
     (A replay/book-building engine would have to resync from the next snapshot instead.)

Across sessions it also reports:
  - data holes: the time with no depth data between the end of one session and the start of the next
    (a new session means the connection dropped, so the sequence check cannot see what was missed);
  - the 'err' lines the recorder logged (why a connection or snapshot request failed);
  - receive latency (rx - E) per session, whose median also shows the clock offset to Binance.
Times are printed in UTC, like the file names (a trailing Z marks UTC).
"""
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from etl import hour_of, open_raw


def read_records(paths):
    for p in paths:
        with open_raw(p) as f:
            for line in f:
                parts = line.rstrip("\n").split(" ", 3)
                if len(parts) < 4:
                    continue  # truncated last line of a file still being written
                rx_ns, session, kind, payload = parts
                yield int(rx_ns), int(session), kind, payload


class Session:
    def __init__(self):
        self.snap_id = None   # lastUpdateId of the newest usable snapshot, while not synced
        self.synced = False
        self.last_u = None
        self.buffer = []      # (rx_ns, U, u) received before the book is synced
        self.events = 0
        self.ignored_old = 0
        self.overlaps = 0
        self.gaps = []        # (rx_ns, expected_U, got_U)
        self.first_rx = None  # first record of any kind
        self.last_rx = None   # last record of any kind
        self.first_depth_rx = None
        self.last_depth_rx = None
        self.errors = Counter()
        self.lat = []         # rx - E in ms, one value per depth event


def apply(s, rx_ns, U, u):
    """Synced state: the event must continue exactly where the previous one ended."""
    if u <= s.last_u:
        s.ignored_old += 1
        return
    if U > s.last_u + 1:
        s.gaps.append((rx_ns, s.last_u + 1, U))
    elif U < s.last_u + 1:
        s.overlaps += 1
    s.last_u = u


def try_sync(s):
    """Run whenever a snapshot is known and events are buffered."""
    if s.snap_id is None:
        return
    kept = [e for e in s.buffer if e[2] > s.snap_id]
    s.ignored_old += len(s.buffer) - len(kept)
    s.buffer = kept
    if not s.buffer:
        return                # wait for events newer than the snapshot
    _, U, u = s.buffer[0]
    if U > s.snap_id + 1:
        s.snap_id = None      # snapshot too old to bridge into the stream: wait for the next one
        return
    s.synced = True           # here U <= snap_id + 1 <= u
    s.last_u = u
    for rx_ns, U, u in s.buffer[1:]:
        apply(s, rx_ns, U, u)
    s.buffer = []


def percentile(sorted_vals, q):
    return sorted_vals[min(len(sorted_vals) - 1, int(q * len(sorted_vals)))]


def ts(rx_ns):
    return datetime.fromtimestamp(rx_ns / 1e9, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def check(paths):
    sessions = {}
    for rx_ns, sid, kind, payload in read_records(paths):
        s = sessions.setdefault(sid, Session())
        if s.first_rx is None:
            s.first_rx = rx_ns
        s.last_rx = rx_ns
        if kind == "err":
            s.errors[payload[:140]] += 1
        elif kind == "snap":
            if not s.synced:
                try:
                    s.snap_id = json.loads(payload)["lastUpdateId"]
                except json.JSONDecodeError:
                    continue  # truncated last line (recorder killed, or the file is still being written)
                try_sync(s)
        elif kind == "ws":
            try:
                msg = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if not msg["stream"].endswith("@depth@100ms"):
                continue      # trades are not part of the sequence
            d = msg["data"]
            s.events += 1
            if s.first_depth_rx is None:
                s.first_depth_rx = rx_ns
            s.last_depth_rx = rx_ns
            s.lat.append(rx_ns / 1e6 - d["E"])   # local receive time minus exchange event time
            if s.synced:
                apply(s, rx_ns, d["U"], d["u"])
            else:
                s.buffer.append((rx_ns, d["U"], d["u"]))
                try_sync(s)

    order = sorted(sessions.items(), key=lambda kv: kv[1].first_rx)
    total_gaps = 0
    all_lat = []
    for sid, s in order:
        total_gaps += len(s.gaps)
        all_lat += s.lat
        hours = (s.last_rx - s.first_rx) / 3.6e12
        print(f"session {sid}: {ts(s.first_rx)} -> {ts(s.last_rx)} ({hours:.2f} h)  events={s.events} "
              f"synced={s.synced} ignored_old={s.ignored_old} overlaps={s.overlaps} "
              f"gaps={len(s.gaps)} unsynced_buffer={len(s.buffer)}")
        for rx_ns, expected, got in s.gaps[:10]:
            print(f"    GAP at {ts(rx_ns)}: expected U={expected}, got U={got} ({got - expected} ids missing)")
        if s.lat:
            lat = sorted(s.lat)
            print(f"    latency rx-E: p50={percentile(lat, 0.5):.0f} ms  p99={percentile(lat, 0.99):.0f} ms")
        for msg, n in s.errors.most_common(3):
            print(f"    log error x{n}: {msg}")

    live = [(sid, s) for sid, s in order if s.events]
    holes = []
    for (a_id, a), (b_id, b) in zip(live, live[1:]):
        holes.append((a_id, b_id, a.last_depth_rx, b.first_depth_rx))
    print("\ndata holes between consecutive sessions with depth data:")
    hole_s = 0.0
    for a_id, b_id, t0, t1 in holes:
        d = (t1 - t0) / 1e9
        hole_s += d
        print(f"    {a_id} -> {b_id}: {d:.1f} s  ({ts(t0)} to {ts(t1)})")
    if not holes:
        print("    none")
    span_s = (live[-1][1].last_depth_rx - live[0][1].first_depth_rx) / 1e9 if live else 0.0
    cover = 100 * (1 - hole_s / span_s) if span_s else 0.0
    print(f"\ntotal: sessions={len(sessions)} ({len(live)} with data)  sequence gaps={total_gaps}  "
          f"holes={len(holes)} ({hole_s:.1f} s of {span_s / 3600:.2f} h, coverage {cover:.2f}%)")
    if all_lat:
        all_lat.sort()
        print(f"receive latency (rx - E): p50={percentile(all_lat, 0.5):.0f} ms  p99={percentile(all_lat, 0.99):.0f} ms"
              "  (includes the clock offset between your PC and Binance)")


if __name__ == "__main__":
    files = []
    for arg in sys.argv[1:]:
        p = Path(arg)
        files += sorted(p.parent.glob(p.name)) if "*" in p.name else [p]
    check(sorted(files, key=hour_of))