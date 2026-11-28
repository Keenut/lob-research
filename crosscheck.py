r"""crosscheck.py: run the Python reference and the C++ engines (vector, std::map and price ladder books;
the ladder with both window sizes considered for BTCUSDT, 2^16 and 2^18 ticks) on the same Parquet hours
and check that they agree exactly (differential testing on real data).

Usage (PowerShell, from the repo root, after building cpp/):
    python crosscheck.py                          # all converted hours, top 100 levels per side
    python crosscheck.py --top 1000 --every 50    # deeper checks, more top-of-book samples
    python crosscheck.py --max-files 2            # quick run
    python crosscheck.py --from 2026-11-26T00 --to 2026-11-26T23   # a window of UTC hours (both included)

What is compared:
  1. every session result (event counts, gaps, syncs, every snapshot check with its full mismatch
     lists): python == cpp (vector book) == cpp (std::map book) == cpp (price ladder, 2^16 ticks)
     == cpp (price ladder, 2^18 ticks);
  2. the status and the top of book (best bid / ask with quantities) after EVERY event:
     cpp vector == each of the other C++ engines;
  3. the same top of book in the Python reference at every --every-th applied event (in Python the
     best price is a max() over every level of a dict, too slow to do after each event).
Exit code 0 when everything agrees, 1 otherwise.
"""
import argparse
import sys
import time
from pathlib import Path

import numpy as np

import replay

# Keys of replay.CPP_REPLAYERS. Both ladder windows are named explicitly, so that both are checked
# whichever one is the default (lobcpp.LadderReplayer, see ladder_log2_slots).
ENGINES = ("vector", "map", "ladder16", "ladder18")
FIELDS = ("session", "first_rx", "last_rx", "events", "applied", "stale", "unsynced_events", "removed_missing",
          "unusable_snaps", "skipped_snaps", "snaps_after_end", "unsynced_ns", "gaps", "syncs", "bid_floor",
          "ask_cap", "checks")   # the order of replay.session_summary()


def first_difference(a, b):
    """Describe where two lists of session summaries differ first."""
    if len(a) != len(b):
        return f"{len(a)} vs {len(b)} sessions"
    for sa, sb in zip(a, b):
        for name, x, y in zip(FIELDS, sa, sb):
            if x != y:
                if name == "checks":
                    for k, (cx, cy) in enumerate(zip(x, y)):
                        if cx != cy:
                            return f"session {sa[0]}, check {k}:\n    {cx}\n    {cy}"
                return f"session {sa[0]}, {name}: {x!r} vs {y!r}"
    return "no difference"


def main():
    ap = argparse.ArgumentParser(description="Python reference vs C++ engines on the same data")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--parquet", type=Path, default=Path("data/parquet"))
    ap.add_argument("--top", type=int, default=100, help="levels per side compared with each snapshot")
    ap.add_argument("--every", type=int, default=100, help="sample the Python top of book every N events")
    replay.add_window_args(ap)
    a = ap.parse_args()
    base = a.parquet / a.symbol
    stems = replay.hour_stems(base, a.max_files, a.first, a.last)
    if not stems:
        sys.exit(f"no Parquet files for these hours in {base}")
    print(f"hours {stems[0]} .. {stems[-1]} ({len(stems)} files), top {a.top} levels per side\n")

    # 1. Python reference, with the top of book sampled after every --every-th event (if applied)
    t0 = time.time()
    index = {"i": -1}
    samples = {}

    def numbered():
        for e in replay.iter_depth_events(base, stems):
            index["i"] += 1
            yield e

    def sample(e, book):
        i = index["i"]
        if i % a.every == 0:
            bb, ba = book.best_bid(), book.best_ask()
            samples[i] = (bb, book.bids.get(bb), ba, book.asks.get(ba))

    py = replay.replay(numbered(), replay.load_snapshots(base, stems), a.top, on_update=sample)
    seconds = {"python": time.time() - t0}

    # 2. C++ engines, keeping the per-event outputs of every hour
    cpp = {}
    for book in ENGINES:
        outs = []
        t0 = time.time()
        sessions = replay.run_cpp(base, stems, a.top, book, on_events=lambda stem, ev, out: outs.append(out))
        seconds[f"cpp {book}"] = time.time() - t0
        cpp[book] = (sessions, {k: np.concatenate([o[k] for o in outs]) for k in outs[0]})

    # 3. comparisons
    ok = True
    want = [replay.session_summary(s) for s in py]
    for book in ENGINES:
        got = [replay.session_summary(s) for s in cpp[book][0]]
        same = got == want
        ok &= same
        print(f"[{'OK' if same else 'DIFF'}] session results, python == cpp {book}")
        if not same:
            print("    " + first_difference(want, got))

    vec = cpp["vector"][1]
    for book in ENGINES[1:]:
        other = cpp[book][1]
        same = all(np.array_equal(vec[k], other[k]) for k in vec)
        ok &= same
        print(f"[{'OK' if same else 'DIFF'}] status and top of book after each of {len(vec['status']):,} events, "
              f"cpp vector == cpp {book}")

    bad = []
    for i, (bb, bq, ba, aq) in samples.items():
        expect = (round(bb * 100) if bb is not None else 0, round(bq * 1e8) if bq is not None else 0,
                  round(ba * 100) if ba is not None else 0, round(aq * 1e8) if aq is not None else 0)
        got = (int(vec["bid_px"][i]), int(vec["bid_qty"][i]), int(vec["ask_px"][i]), int(vec["ask_qty"][i]))
        if vec["status"][i] != 0 or got != expect:
            bad.append((i, expect, got))
    ok &= not bad
    print(f"[{'OK' if not bad else 'DIFF'}] top of book python == cpp at {len(samples):,} sampled applied events")
    for i, expect, got in bad[:5]:
        print(f"    event {i}: python {expect} cpp {got}")

    checks = [c for s in py for c in s.checks]
    print(f"\ncompared: {len(py)} sessions, {sum(s.events for s in py):,} events, {len(checks)} snapshot checks, "
          f"{sum(c.compared for c in checks):,} levels, {sum(len(c.mismatches) for c in checks):,} mismatched "
          f"({sum(len(c.unexplained) for c in checks):,} unexplained)")
    print("time incl. Parquet reading: " + ", ".join(f"{k} {v:.1f} s" for k, v in seconds.items()))
    print("ALL ENGINES AGREE" if ok else "ENGINES DISAGREE")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
