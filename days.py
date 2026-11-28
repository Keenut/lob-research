r"""days.py: data quality per UTC day, to choose the days that a walk-forward evaluation can use.

Usage (PowerShell, from the repo root, after python etl.py):
    python days.py                                    # every converted hour of data\parquet
    python days.py --parquet data\parquet_msi         # another source (one Parquet root per PC)
    python days.py --from 2026-11-25T00 --to 2026-11-26T23 --top 1000

For each UTC day, on the receive time rx (the PC clock; its offset of a second or two does not matter
at the scale of a day):
  time coverage   share of the 86,400 s of the day between the first and the last depth event of a
                  session (the definition of check_continuity.py, applied to whole days)
  book coverage   share of the day with a synced book: from a sync to the next sequence gap or to the
                  end of the session, as the replay state machine of replay.py decides
  holes           time between consecutive sessions with depth data that falls in the day
  checks          snapshot comparisons (top --top levels per side) made in the day; unexplained must be 0
A day only partly recorded (the first and the last day of a recording) has a low time coverage by
construction. The replay runs once over the whole window and the intervals are then cut at midnight,
because a session can last across days: replaying each day apart would add a spurious unsynced start.
No threshold for a "usable" day is applied here: the report gives the numbers.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

import clock
import replay

DAY_NS = 86_400 * 10**9


def day_name(day):
    return datetime.fromtimestamp(day * 86_400, timezone.utc).strftime("%Y-%m-%d")


def hms(ns):
    return datetime.fromtimestamp(ns / 1e9, timezone.utc).strftime("%H:%M:%S")


def merge(intervals):
    """Union of [start, end) intervals, sorted and disjoint."""
    out = []
    for a, b in sorted(intervals):
        if out and a <= out[-1][1]:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return [(a, b) for a, b in out]


def per_day(intervals):
    """{day: ns covered} of a list of [start, end) intervals, cut at midnight UTC."""
    out = Counter()
    for a, b in merge(intervals):
        while a < b:
            day = a // DAY_NS
            end = min(b, (day + 1) * DAY_NS)
            out[day] += end - a
            a = end
    return out


def synced_intervals(s):
    """[start, end) intervals of a session (replay.SessionReplay) with a synced book: from a sync to the
    next sequence gap or to the last event of the session."""
    marks = sorted([(rx, 0) for rx, _, _ in s.gaps] + [(rx, 1) for _, rx, _ in s.syncs])  # gap first if equal
    out, start = [], None
    for rx, is_sync in marks:
        if is_sync and start is None:
            start = rx
        elif not is_sync and start is not None:
            out.append((start, rx))
            start = None
    if start is not None:
        out.append((start, s.last_rx))
    return out


def session_spans(rx_ns, session):
    """{session: (first rx, last rx)} of the depth events."""
    spans = {}
    for sid in np.unique(session):
        rx = rx_ns[session == sid]
        spans[int(sid)] = (int(rx.min()), int(rx.max()))
    return spans


def holes(spans):
    """[(start, end)] between consecutive sessions with depth data (ordered by their first event)."""
    order = sorted(spans.values())
    return [(a[1], b[0]) for a, b in zip(order, order[1:]) if b[0] > a[1]]


def count_by_day(rx_ns):
    days, n = np.unique(np.asarray(rx_ns, dtype=np.int64) // DAY_NS, return_counts=True)
    return dict(zip(days.tolist(), n.tolist()))


def session_counts(spans):
    """{day: number of sessions with depth data in that day}."""
    out = Counter()
    for a, b in spans.values():
        for day in range(a // DAY_NS, b // DAY_NS + 1):
            out[day] += 1
    return out


def hole_counts(hole_list):
    """{day: (number of holes touching the day, length of the longest of them in ns)}."""
    out = {}
    for a, b in hole_list:
        for day in range(a // DAY_NS, (b - 1) // DAY_NS + 1):
            n, longest = out.get(day, (0, 0))
            out[day] = (n + 1, max(longest, b - a))
    return out


def replay_counts(sessions):
    """{day: Counter of gaps, syncs, snapshot checks and their mismatches} of replayed sessions
    (replay.SessionReplay). A gap counts on the day of its event, a sync on the day of the event that
    bridges the snapshot into the stream, a check on the day the snapshot was received."""
    out = defaultdict(Counter)
    for s in sessions:
        for rx, _, _ in s.gaps:
            out[rx // DAY_NS]["gaps"] += 1
        for _, rx, _ in s.syncs:
            out[rx // DAY_NS]["syncs"] += 1
        for c in s.checks:
            d = out[c.snap_rx_ns // DAY_NS]
            d["checks"] += 1
            d["mismatched"] += len(c.mismatches)
            d["unexplained"] += len(c.unexplained)
            d["crossed"] += int(c.crossed)
    return out


def day_table(base, stems, top_n=100):
    """Per-day metrics of the given hours: {day: dict}."""
    ev = clock.read_table(base, "depth_events", ["rx_ns", "session"], stems)
    tr = clock.read_table(base, "trades", ["rx_ns"], stems)
    rx, session = ev["rx_ns"].astype(np.int64), ev["session"].astype(np.int64)
    spans = session_spans(rx, session)
    sessions = replay.replay(replay.iter_depth_events(base, stems), replay.load_snapshots(base, stems), top_n)

    hole_list = holes(spans)
    time_cov = per_day(list(spans.values()))
    book_cov = per_day([iv for s in sessions for iv in synced_intervals(s)])
    hole_ns = per_day(hole_list)
    depth, trades = count_by_day(rx), count_by_day(tr["rx_ns"])
    hours = Counter(int(datetime.strptime(stem, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc).timestamp()) // 86_400
                    for stem in stems)
    n_sessions, hole_n, counts = session_counts(spans), hole_counts(hole_list), replay_counts(sessions)
    table = {}
    for day in sorted(set(depth) | set(n_sessions) | set(hole_n) | set(counts)):
        in_day = rx[rx // DAY_NS == day]
        c = counts.get(day, Counter())
        table[day] = dict(
            hours=hours.get(day, 0), depth=depth.get(day, 0), trades=trades.get(day, 0),
            first_rx=int(in_day.min()) if len(in_day) else None, last_rx=int(in_day.max()) if len(in_day) else None,
            sessions=n_sessions.get(day, 0), holes=hole_n.get(day, (0, 0))[0], max_hole_ns=hole_n.get(day, (0, 0))[1],
            hole_ns=hole_ns.get(day, 0), time_ns=time_cov.get(day, 0), book_ns=book_cov.get(day, 0),
            **{k: c[k] for k in ("gaps", "syncs", "checks", "mismatched", "unexplained", "crossed")})
    return table, sessions


def report(table, top_n):
    cols = (f"{'day':10} {'hours':>5} {'first':>8} {'last':>8} {'depth ev':>10} {'trades':>10} {'sess':>4} "
            f"{'holes':>5} {'hole s':>8} {'max hole s':>10} {'time cov':>9} {'book cov':>9} {'gaps':>4} "
            f"{'syncs':>5} {'checks':>6} {'mismatch':>8} {'unexpl':>6} {'crossed':>7}")
    print(cols)
    for day, d in table.items():
        print(f"{day_name(day):10} {d['hours']:>5} "
              f"{hms(d['first_rx']) if d['first_rx'] is not None else '-':>8} "
              f"{hms(d['last_rx']) if d['last_rx'] is not None else '-':>8} "
              f"{d['depth']:>10,} {d['trades']:>10,} {d['sessions']:>4} {d['holes']:>5} "
              f"{d['hole_ns'] / 1e9:>8.1f} {d['max_hole_ns'] / 1e9:>10.1f} "
              f"{100 * d['time_ns'] / DAY_NS:>8.2f}% {100 * d['book_ns'] / DAY_NS:>8.2f}% {d['gaps']:>4} "
              f"{d['syncs']:>5} {d['checks']:>6} {d['mismatched']:>8,} {d['unexplained']:>6} {d['crossed']:>7}")
    print(f"""
time cov    share of the UTC day between the first and the last depth event of a session
book cov    share of the day with a synced book (from a sync to a sequence gap or the end of the session)
hole s      time between sessions in the day; max hole s: the longest hole touching the day (whole length)
checks      snapshot comparisons over the top {top_n} levels per side; mismatch: mismatched levels
            (untracked or unexplained, see replay.py); unexpl must be 0
first/last  first and last depth event of the day; all times are receive times (PC clock), UTC""")


def main():
    ap = argparse.ArgumentParser(description="Data quality per UTC day")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--parquet", type=Path, default=Path("data/parquet"))
    ap.add_argument("--top", type=int, default=100, help="levels per side compared with each snapshot")
    replay.add_window_args(ap)
    a = ap.parse_args()
    base = a.parquet / a.symbol
    stems = replay.hour_stems(base, a.max_files, a.first, a.last)
    if not stems:
        print(f"no Parquet files for these hours in {base / 'depth_events'} (run python etl.py first)")
        return
    print(f"{base}: hours {stems[0]} .. {stems[-1]} ({len(stems)} files)\n")
    table, _ = day_table(base, stems, a.top)
    report(table, a.top)


if __name__ == "__main__":
    main()
