r"""bench.py: speed of the C++ replay engine (vector book, std::map book, price ladder) and of the Python
reference, on data already in memory: reading and converting the Parquet files is NOT timed.

Usage (PowerShell, from the repo root, after building cpp/ in Release):
    python bench.py                                   # all converted hours
    python bench.py --max-files 4 --repeat 5 --python-hours 1
    python bench.py --max-files 24 --profile-only     # sections 4-6 only (no timings)

What it measures:
  1. throughput on all hours: wall time of the process() calls only (the replay state machine, the book
     updates, the snapshot checks and the top-of-book output), median of --repeat runs after a warm-up;
     the price ladder with windows of 2^12 to 2^18 ticks, and for each window how many level updates
     went to its std::map and how often the window moved (one more, untimed run);
  2. the same on the first --python-hours hours for the C++ engines AND the Python reference
     (replay.replay on events already read into memory): a like-for-like speed-up;
  3. time per applied event (mean, p50, p90, p99, p99.9, max) from a separate run with per-event timing
     on (steady_clock around each event; its measured resolution is printed), for the vector, the map and
     every ladder window. Then the same events split by what else happened in them: a sync (the book
     loaded from a REST snapshot), a check against a snapshot, a move of a ladder's window, or none of
     these, with the slowest of the last group. The moves are counted per event in one more, untimed
     run per ladder that gives the engine one event at a time (slow: a few seconds per ladder);
  4. where the level updates land in the book (a separate profiling run): inserts / erases / in-place
     changes / removals of missing levels, and how many levels are better than the updated price.
     For the sorted vector an insert or erase shifts exactly that many elements: this is what explains
     the difference between the vector and the map book.
  5. (same profiling run) the distance in TICKS from each update's price to the best price of its side,
     just before the update. Section 4 counts levels; a container indexed by price (a price ladder)
     depends on ticks, so this is what sizes its window.
  6. (same run) when an update erases the best level itself: the distance in ticks to the level that
     becomes the new best, i.e. how far a price ladder has to scan to find it.
Distances are grouped in power-of-two buckets (2^(k-1) <= d < 2^k), with cumulative shares.
Run it on an otherwise idle PC (the recorder may keep running: it uses little CPU), and say so when
quoting the numbers.
"""
import argparse
import os
import platform
import statistics
import sys
import time
from pathlib import Path

import numpy as np

import bookdata
import replay

ARRAYS = ("rx", "session", "first_id", "last_id", "offsets", "side", "price", "qty")
LADDERS = ("ladder12", "ladder14", "ladder16", "ladder18")   # price ladders compared in sections 1 and 3
OTHER, MOVE, CHECK, SYNC = 0, 1, 2, 3   # what else happened in an event (section 3): the highest that applies


def cpu_name():
    if sys.platform == "win32":
        try:
            import winreg
            key = r"HARDWARE\DESCRIPTION\System\CentralProcessor\0"
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key) as k:
                return winreg.QueryValueEx(k, "ProcessorNameString")[0].strip()
        except OSError:
            pass
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown CPU"


def run_cpp(book, hours, snaps, top_n, timings=False, profile=False):
    """One full replay. Returns (seconds spent in process(), replayer, per-hour outputs)."""
    r = replay.cpp_replayer(book, top_n, profile)
    r.set_snapshots(snaps["rx"], snaps["session"], snaps["last_id"], snaps["offsets"],
                    snaps["side"], snaps["price"], snaps["qty"])
    seconds, outs = 0.0, []
    for h in hours:
        t0 = time.perf_counter()
        out = r.process(*(h[k] for k in ARRAYS), timings=timings)
        seconds += time.perf_counter() - t0
        outs.append(out)
    r.finish()
    return seconds, r, outs


def name_of(book):
    """Display name of an engine (a key of replay.CPP_REPLAYERS)."""
    if book.startswith("ladder"):
        import lobcpp

        return f"cpp ladder 2^{book[len('ladder'):] or lobcpp.ladder_log2_slots}"
    return f"cpp {book}"


def size_text(nbytes):
    return f"{nbytes / 1024:,.0f} KiB" if nbytes < 1 << 20 else f"{nbytes / (1 << 20):,.0f} MiB"


def median_time(fn, repeat):
    fn()                                   # warm-up: caches, allocator, first-touch page faults
    times = [fn() for _ in range(repeat)]
    return statistics.median(times), times


def line(name, seconds, events, levels, base=None):
    speed = f"  x{base / seconds:5.1f}" if base else ""
    return (f"   {name:<17} {seconds:8.3f} s  {events / seconds:>12,.0f} events/s  "
            f"{levels / seconds / 1e6:7.2f} M level updates/s  {1e9 * seconds / levels:7.1f} ns/update{speed}")


def bucket_label(k):
    """Distances in power-of-two bucket k (see pow2_bucket in replay.hpp): 0 for k = 0, else 2^(k-1) .. 2^k - 1."""
    if k == 0:
        return "0"
    lo, hi = 1 << (k - 1), (1 << k) - 1
    return f"{lo:,}" if lo == hi else f"{lo:,}-{hi:,}"


def print_buckets(counts, total):
    """One line per non-empty bucket: tick range, its upper end in USDT, count, share and cumulative share
    (both in % of total)."""
    print(f"      {'ticks':>17}  {'up to USDT':>12}  {'updates':>12}  {'share':>8}  {'cumulative':>10}")
    cum = 0
    for k, n in enumerate(counts):
        if n == 0:
            continue
        cum += n
        top_usdt = ((1 << k) - 1) / bookdata.PRICE_SCALE if k else 0.0   # largest distance in the bucket
        print(f"      {bucket_label(k):>17}  {top_usdt:>12,.2f}  {n:>12,}  {100 * n / total:7.2f} %  "
              f"{100 * cum / total:9.2f} %")


def main():
    ap = argparse.ArgumentParser(description="Benchmark of the replay engines on in-memory data")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--parquet", type=Path, default=Path("data/parquet"))
    ap.add_argument("--max-files", type=int, default=None, help="only the first N hours")
    ap.add_argument("--repeat", type=int, default=3, help="timed runs per engine (after one warm-up)")
    ap.add_argument("--python-hours", type=int, default=2, help="hours for the comparison with Python")
    ap.add_argument("--top", type=int, default=100, help="levels per side compared with each snapshot")
    ap.add_argument("--profile-only", action="store_true", help="skip the timings (sections 1-3)")
    a = ap.parse_args()

    import lobcpp

    base = a.parquet / a.symbol
    stems = replay.hour_stems(base, a.max_files)
    if not stems:
        sys.exit(f"no Parquet files in {base}")
    t0 = time.perf_counter()
    hours = [bookdata.load_events(base, s) for s in stems]
    snaps = bookdata.load_snapshots(base, stems)
    n_events = sum(len(h["rx"]) for h in hours)
    n_levels = sum(len(h["side"]) for h in hours)
    print(f"data: {len(stems)} hours ({stems[0]} .. {stems[-1]}), {n_events:,} depth events, "
          f"{n_levels:,} level updates, {len(snaps['rx'])} snapshots")
    print(f"      read and converted in {time.perf_counter() - t0:.1f} s (not timed below)")
    print(f"machine: {cpu_name()}, {os.cpu_count()} logical CPUs, {platform.platform()}, "
          f"Python {platform.python_version()}")
    print(f"lobcpp: {lobcpp.compiler}, {lobcpp.build} build; top {a.top} levels per side checked\n")
    if lobcpp.build != "release":
        print("WARNING: lobcpp is a debug build: the C++ timings below are not meaningful\n")

    if not a.profile_only:
        timings(a, base, stems, hours, snaps, n_events, n_levels)
    profile_report(a, hours, snaps)


def timings(a, base, stems, hours, snaps, n_events, n_levels):
    """Sections 1-3: throughput, the same subset with the Python reference, time per event."""
    import lobcpp

    # 1. throughput on all hours
    print(f"1. throughput, all hours (process() calls only; median of {a.repeat} runs after 1 warm-up)")
    for book in ("vector", "map") + LADDERS:
        med, times = median_time(lambda: run_cpp(book, hours, snaps, a.top)[0], a.repeat)
        print(line(name_of(book), med, n_events, n_levels) + f"   runs: {', '.join(f'{t:.3f}' for t in times)}")

    # where the ladders' level updates went (one more run each, untimed)
    applied = None
    print("\n   price ladder by window size (one more run each, untimed):")
    print(f"      {'window':>8} {'= USDT':>10} {'array per side':>15} {'updates via std::map':>21} "
          f"{'window moves up':>16} {'down':>6}")
    for book in LADDERS:
        _, r, outs = run_cpp(book, hours, snaps, a.top)
        if applied is None:   # level updates of the applied events (the same for every engine)
            applied = sum(int(np.diff(h["offsets"])[o["status"] == 0].sum()) for h, o in zip(hours, outs))
        st = r.ladder_stats()
        print(f"      {'2^' + book[len('ladder'):]:>8} {st['slots'] / bookdata.PRICE_SCALE:>10,.2f} "
              f"{size_text(8 * st['slots']):>15} {st['deep_sets']:>12,} ({100 * st['deep_sets'] / applied:5.2f} %) "
              f"{st['shifts_up']:>16,} {st['shifts_down']:>6,}")

    # 2. same subset, with the Python reference
    k = min(a.python_hours, len(stems))
    sub_stems = stems[:k]
    sub_hours = hours[:k]
    sub_snaps = bookdata.load_snapshots(base, sub_stems)
    sub_events = sum(len(h["rx"]) for h in sub_hours)
    sub_levels = sum(len(h["side"]) for h in sub_hours)
    py_events = list(replay.iter_depth_events(base, sub_stems))     # read into memory: not timed
    py_snaps = replay.load_snapshots(base, sub_stems)

    def run_python():
        t = time.perf_counter()
        replay.replay(py_events, py_snaps, a.top)
        return time.perf_counter() - t

    print(f"\n2. same data for every engine: the first {k} hour(s), {sub_events:,} events, {sub_levels:,} level updates")
    py_med, _ = median_time(run_python, a.repeat)
    print(line("python reference", py_med, sub_events, sub_levels, py_med))
    for book in ("vector", "map", "ladder"):
        med, _ = median_time(lambda: run_cpp(book, sub_hours, sub_snaps, a.top)[0], a.repeat)
        print(line(name_of(book), med, sub_events, sub_levels, py_med))
    print("   (xN: speed-up over the Python reference on the same events)")

    # 3. time per applied event
    res = lobcpp.clock_resolution_ns()
    print(f"\n3. time per applied event in microseconds (separate run with timing on; clock resolution {res} ns)")
    print(f"   {'engine':<17} {'mean':>8} {'p50':>8} {'p90':>8} {'p99':>8} {'p99.9':>8} {'max':>9}")
    timed = {}   # engine -> (ns per event, applied mask, sessions): for the split below
    for book in ("vector", "map") + LADDERS:
        _, r, outs = run_cpp(book, hours, snaps, a.top, timings=True)
        ns = np.concatenate([o["ns"] for o in outs])
        applied = np.concatenate([o["status"] for o in outs]) == 0
        timed[book] = (ns, applied, r.sessions())
        x = ns[applied]
        q = np.percentile(x, [50, 90, 99, 99.9]) / 1e3
        print(f"   {name_of(book):<17} {x.mean() / 1e3:8.2f} {q[0]:8.2f} {q[1]:8.2f} {q[2]:8.2f} {q[3]:8.2f} "
              f"{x.max() / 1e3:9.1f}")
    split_by_kind(a, hours, snaps, timed)
    print()


def snapshot_events(sessions, session, last_id, rx):
    """Indices of the events at which the replay used a snapshot, as two lists: syncs and checks.
    sessions: r.sessions() of a C++ engine; session, last_id, rx: per event, all hours concatenated.
    replay.hpp uses the snapshot with update id L at the first event of its session with u > L."""
    syncs, checks = [], []
    for s in sessions:
        idx = np.flatnonzero(session == s["session"])   # this session's events, in replay order
        u = last_id[idx]

        def at(L):
            later = u > L
            if not later.any():
                sys.exit(f"internal error: no event after update id {L} in session {s['session']}")
            return int(idx[np.argmax(later)])           # argmax of a bool array: the first True

        for _, event_rx, L in s["syncs"]:
            i = at(L)
            if rx[i] != event_rx:                        # the engine also reports the rx of its sync event
                sys.exit(f"internal error: sync at update id {L} of session {s['session']} not found")
            syncs.append(i)
        checks += [at(c["last_update_id"]) for c in s["checks"]]
    return syncs, checks


def moves_per_event(book, hours, snaps, top_n):
    """Window moves of a price ladder in each event (both sides), from one more replay, untimed, that
    gives the engine one event at a time and reads its move counters after each event. Exact, but the
    Python loop around every event makes it slow."""
    r = replay.cpp_replayer(book, top_n)
    r.set_snapshots(snaps["rx"], snaps["session"], snaps["last_id"], snaps["offsets"], snaps["side"],
                    snaps["price"], snaps["qty"])
    moves, before = [], 0
    for h in hours:
        off = h["offsets"]
        for i in range(len(h["rx"])):
            b, e = off[i], off[i + 1]                    # this event's levels
            one = slice(i, i + 1)
            r.process(h["rx"][one], h["session"][one], h["first_id"][one], h["last_id"][one],
                      np.array([0, e - b], dtype=np.int64), h["side"][b:e], h["price"][b:e], h["qty"][b:e])
            st = r.ladder_stats()
            now = st["shifts_up"] + st["shifts_down"]   # moves since the start of the replay
            moves.append(now - before)
            before = now
    r.finish()
    return np.array(moves)


def split_by_kind(a, hours, snaps, timed):
    """Section 3, continued: the timed events by what else happened in them (OTHER .. SYNC above)."""
    rx = np.concatenate([h["rx"] for h in hours])
    session = np.concatenate([h["session"] for h in hours])
    last_id = np.concatenate([h["last_id"] for h in hours])
    levels = np.concatenate([np.diff(h["offsets"]) for h in hours])   # level updates per event
    print("\n   the same applied events by what else happened in them (n = number of events, median and max in")
    print("   microseconds; an event is counted once, in the first of these that applies):")
    print("      sync:  the book was loaded from a REST snapshot (at the start of a session, or after a gap)")
    print(f"      check: the book was compared with a snapshot ({a.top} levels per side)")
    print("      move:  the window of the price ladder moved, on either side (counted per event in one more,")
    print("             untimed run per ladder; that run is slow)")
    print(f"   {'engine':<17} {'sync: n':>8} {'median':>7} {'max':>7}   {'check: n':>8} {'median':>7} {'max':>7}   "
          f"{'move: n':>8} {'median':>7} {'max':>7}")
    others = []
    for book, (ns, applied, sessions) in timed.items():
        kind = np.full(len(ns), OTHER)
        ladder = book.startswith("ladder")
        if ladder:
            kind[moves_per_event(book, hours, snaps, a.top) > 0] = MOVE
        syncs, checks = snapshot_events(sessions, session, last_id, rx)
        kind[checks] = CHECK
        kind[syncs] = SYNC
        cells = []
        for k in (SYNC, CHECK, MOVE):
            x = ns[applied & (kind == k)] / 1e3
            if k == MOVE and not ladder:
                cells.append(f"{'-':>8} {'':>7} {'':>7}")      # no window to move
            elif len(x) == 0:
                cells.append(f"{0:>8} {'-':>7} {'-':>7}")
            else:
                cells.append(f"{len(x):>8,} {np.median(x):7.1f} {x.max():7.1f}")
        print(f"   {name_of(book):<17} " + "   ".join(cells).rstrip())
        rest = np.flatnonzero(applied & (kind == OTHER))
        others.append((book, ns[rest], rest[np.argmax(ns[rest])]))

    print(f"\n   all other applied events (no sync, check or move; an event has {levels.mean():.0f} level updates "
          "on average):")
    print(f"   {'engine':<17} {'p99.9':>8} {'max':>8}   {'slowest event: level updates':>29}  received")
    for book, x, i in others:
        print(f"   {name_of(book):<17} {np.percentile(x, 99.9) / 1e3:8.2f} {x.max() / 1e3:8.1f}   "
              f"{levels[i]:>29,}  {replay.utc(rx[i])}")


def profile_report(a, hours, snaps):
    """Sections 4-6, from one profiling run of the vector engine (its probe() is a binary search)."""
    _, r, _ = run_cpp("vector", hours, snaps, a.top, profile=True)
    p = r.profile()

    # 4. where the updates land
    total = p["inserts"] + p["erases"] + p["in_place"] + p["missing"]
    moves = p["inserts"] + p["erases"]
    print(f"4. where the {total:,} applied level updates land (profiling run)")
    for name in ("inserts", "erases", "in_place", "missing"):
        what = {"inserts": "new level", "erases": "level removed", "in_place": "quantity changed",
                "missing": "removal of a level not in the book"}[name]
        print(f"   {name:<9} {p[name]:>12,}  {100 * p[name] / total:5.1f} %  ({what})")
    labels = ("0", "1-9", "10-99", "100-999", "1,000-9,999", ">= 10,000")
    print("   inserts + erases by number of better levels (= elements a sorted vector shifts):")
    for lab, n in zip(labels, p["by_depth"]):
        print(f"      {lab:>12}: {n:>12,}  {100 * n / moves:5.1f} %")
    print(f"   mean shift per insert or erase: {p['shifted'] / moves:,.0f} elements "
          f"({16 * p['shifted'] / moves / 1024:,.1f} KiB with 16-byte levels)")
    print(f"   mean book size over applied events: {p['bid_levels'] / p['events']:,.0f} bid levels, "
          f"{p['ask_levels'] / p['events']:,.0f} ask levels")

    # 5. distance from the best price, in ticks
    behind, ahead = sum(p["behind"]), sum(p["ahead"])
    if behind + ahead + p["no_best"] != total:
        sys.exit("profile error: the distance counts do not add up to the number of updates")
    print(f"\n5. distance in ticks (1 tick = {1 / bookdata.PRICE_SCALE} USDT) from the best price of the same side, "
          "just before each update")
    print(f"   at or behind the best: {behind:,} ({100 * behind / total:.2f} %); better than the best: {ahead:,} "
          f"({100 * ahead / total:.2f} %); side empty: {p['no_best']:,}")
    print("   at or behind the best (share and cumulative share of ALL applied updates):")
    print_buckets(p["behind"], total)
    print("   better than the best, i.e. a new best price (share and cumulative share of ALL applied updates):")
    print_buckets(p["ahead"], total)

    # 6. erases of the best level: how far away is the next one?
    erased_best = sum(p["next_gap"]) + p["last_level"]
    print(f"\n6. updates that erase the best level itself: {erased_best:,} ({100 * erased_best / total:.2f} % of "
          "applied updates); distance in ticks to the level that becomes the new best:")
    if erased_best:
        print_buckets(p["next_gap"], erased_best)
    print(f"   the side had no other level: {p['last_level']:,}")

if __name__ == "__main__":
    main()
