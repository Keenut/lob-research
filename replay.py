r"""replay.py: rebuild the L2 order book from the Parquet tables and validate it against REST snapshots.

Usage (PowerShell, from the repo root, after python etl.py):
    python replay.py                          # all converted hours, compare the top 100 levels per side
    python replay.py --max-files 2            # quick run on the first 2 hours
    python replay.py --top 1000 --csv docs\validation_top1000.csv
    python replay.py --engine cpp             # same replay with the C++ engine (build cpp/ first)

Engines: python (this file, the reference), cpp (C++ engine, vector book), cpp-map (C++ engine,
std::map book), cpp-ladder (C++ engine, price ladder). All four must print the same report except
the last line (time and engine).

Rules, per session (one WebSocket connection; every session starts with no book):
  * Events are processed in receive order. Snapshots are placed by their update id
    (lastUpdateId = L), not by their receive time: offline we know every snapshot in advance,
    so the book can be synced as early as the data allows.
  * Sync: the first event with u > L must satisfy U <= L + 1 (it bridges the snapshot into the
    stream). Book = snapshot, then that event is applied (updates <= L inside it are re-applied,
    which is harmless because a diff carries absolute quantities). A snapshot with L + 1 < U is
    older than the stream data we have and is unusable.
  * Synced: event with u <= last_u is stale and ignored; U > last_u + 1 is a GAP: the book is
    dropped and we wait for the next usable snapshot (resync).
  * Validation: every later snapshot (not used for sync) is compared with the book in the state
    of update id L, i.e. after the last event with u <= L. If L falls strictly inside the next event
    (U <= L < u), the snapshot contains only part of that event's updates, so the levels that event
    touches are ambiguous and excluded; all other levels must match exactly.
Limitation: a snapshot has at most 5000 levels per side. A level deeper than the sync snapshot's
last level (bid below its lowest bid, ask above its highest ask) is unknown until a diff touches it.
So each mismatch is classified:
    untracked   : the level is missing from the book (book qty 0) AND lies beyond the sync snapshot's
                  depth: expected when the price drifts toward the edge of the synced range;
    unexplained : every other mismatch: this would be a real bug (or bad data) and must stay 0.
"""
import argparse
import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

from book import ASK, BID, L2Book, compare_top


class DepthEvent(NamedTuple):
    rx_ns: int
    session: int
    first_id: int    # U
    last_id: int     # u
    bid_px: list
    bid_qty: list
    ask_px: list
    ask_qty: list


class Snapshot(NamedTuple):
    rx_ns: int
    session: int
    last_update_id: int
    bids: dict       # price -> qty
    asks: dict


class Check(NamedTuple):
    session: int
    snap_rx_ns: int
    last_update_id: int
    straddle: bool   # L was inside an event: that event's levels were excluded
    compared: int
    excluded: int
    mismatches: list   # all of them: [(side, price, book_qty, snap_qty)]
    unexplained: list  # the subset NOT explained by the snapshot depth limit (see module docstring)
    crossed: bool
    book_levels: int


# ---------------------------------------------------------------- loading (Parquet -> Python objects)

def hour_stems(base, max_files=None):
    stems = sorted(p.stem for p in (base / "depth_events").glob("*.parquet"))
    return stems[:max_files] if max_files else stems


def iter_depth_events(base, stems):
    """Yield DepthEvent in file order. depth_updates rows are matched to depth_events rows by
    position: etl.py writes, per message, n_bids bid rows then n_asks ask rows, in message order."""
    import pyarrow.parquet as pq

    for stem in stems:
        ev = pq.read_table(base / "depth_events" / f"{stem}.parquet",
                           columns=["rx_ns", "session", "first_id", "last_id", "n_bids", "n_asks"])
        ev = {c: ev.column(c).to_pylist() for c in ev.column_names}
        up_path = base / "depth_updates" / f"{stem}.parquet"
        if up_path.exists():
            up = pq.read_table(up_path, columns=["last_id", "side", "price", "qty"])
            up_last = up.column("last_id").to_numpy()    # numpy: used only for the alignment check
            up_side = up.column("side").to_numpy()
            price = up.column("price").to_pylist()        # Python floats: used as dict keys
            qty = up.column("qty").to_pylist()
        else:                                             # etl.py writes no file for an empty table
            up_last = up_side = price = qty = []
        off = 0
        for rx, sid, U, u, nb, na in zip(ev["rx_ns"], ev["session"], ev["first_id"], ev["last_id"],
                                         ev["n_bids"], ev["n_asks"]):
            mid, end = off + nb, off + nb + na
            if nb + na and (up_last[off] != u or (nb and up_side[off] != BID) or (na and up_side[mid] != ASK)):
                raise ValueError(f"{stem}: depth_updates not aligned with depth_events at row {off}")
            yield DepthEvent(rx, sid, U, u, price[off:mid], qty[off:mid], price[mid:end], qty[mid:end])
            off = end
        if off != len(price):
            raise ValueError(f"{stem}: {len(price) - off} depth_updates rows left over")


def load_snapshots(base, stems):
    """Return {session: [Snapshot, ...] sorted by last_update_id}. One REST response = one rx_ns."""
    import pyarrow.parquet as pq

    by_key = {}
    for stem in stems:
        path = base / "snapshots" / f"{stem}.parquet"
        if not path.exists():
            continue
        t = pq.read_table(path)
        cols = {c: t.column(c).to_pylist() for c in ("rx_ns", "session", "last_update_id", "side", "price", "qty")}
        for rx, sid, L, side, p, q in zip(*cols.values()):
            s = by_key.get((sid, rx))
            if s is None:
                s = by_key[(sid, rx)] = Snapshot(rx, sid, L, {}, {})
            (s.bids if side == BID else s.asks)[p] = q
    out = {}
    for s in by_key.values():
        out.setdefault(s.session, []).append(s)
    for snaps in out.values():
        snaps.sort(key=lambda s: s.last_update_id)
    return out


# ---------------------------------------------------------------- replay state machine (one session)

class SessionReplay:
    def __init__(self, session, snapshots, top_n):
        self.session = session
        self.snaps = snapshots       # sorted by last_update_id
        self.next_snap = 0
        self.top_n = top_n
        self.book = None             # None = not synced
        self.bid_floor = self.ask_cap = None   # depth of the sync snapshot: book complete inside it
        self.events = self.applied = self.stale = self.unsynced_events = 0
        self.removed_missing = 0
        self.unusable_snaps = self.skipped_snaps = 0
        self.gaps = []               # (rx_ns, expected_U, got_U)
        self.syncs = []              # (snap_rx_ns, event_rx_ns, L)
        self.checks = []
        self.first_rx = self.last_rx = None
        self.unsynced_ns = 0         # time spent without a valid book
        self._unsynced_since = None

    def on_event(self, e):
        self.events += 1
        if self.first_rx is None:
            self.first_rx = self._unsynced_since = e.rx_ns
        self.last_rx = e.rx_ns

        # 1. sequence check: a gap makes the book wrong from here on
        if self.book is not None and e.first_id > self.book.last_update_id + 1:
            self.gaps.append((e.rx_ns, self.book.last_update_id + 1, e.first_id))
            self.book = None
            self._unsynced_since = e.rx_ns

        # 2. snapshots whose state lies before the end of this event (L < u): sync or validate now
        while self.next_snap < len(self.snaps) and self.snaps[self.next_snap].last_update_id < e.last_id:
            s = self.snaps[self.next_snap]
            self.next_snap += 1
            self._use_snapshot(s, e)

        # 3. apply the event
        if self.book is None:
            self.unsynced_events += 1
            return
        if e.last_id <= self.book.last_update_id:
            self.stale += 1
            return
        self.removed_missing += self.book.apply_event(e.last_id, e.bid_px, e.bid_qty, e.ask_px, e.ask_qty)
        self.applied += 1

    def _use_snapshot(self, s, e):
        L = s.last_update_id
        if self.book is None:
            if L + 1 >= e.first_id:                      # U <= L + 1 <= u: the snapshot bridges into e
                self.book = L2Book.from_snapshot(L, s.bids.items(), s.asks.items())
                self.bid_floor = min(s.bids) if s.bids else float("inf")
                self.ask_cap = max(s.asks) if s.asks else float("-inf")
                self.syncs.append((s.rx_ns, e.rx_ns, L))
                self.unsynced_ns += e.rx_ns - self._unsynced_since
                self._unsynced_since = None
            else:
                self.unusable_snaps += 1                 # older than the data we have
            return
        last = self.book.last_update_id
        if L < last:                                     # should not happen (snapshots sorted by L)
            self.skipped_snaps += 1
            return
        exclude = set()
        if L > last:                                     # last < L < u: snapshot taken inside event e
            exclude = {(BID, p) for p in e.bid_px} | {(ASK, p) for p in e.ask_px}
        res = compare_top(self.book, s.bids, s.asks, self.top_n, exclude)
        unexplained = [m for m in res.mismatches if not self._untracked(*m)]
        self.checks.append(Check(self.session, s.rx_ns, L, L > last, res.compared, res.excluded,
                                 res.mismatches, unexplained, self.book.crossed(),
                                 len(self.book.bids) + len(self.book.asks)))

    def _untracked(self, side, price, book_qty, snap_qty):
        """Missing from the book and beyond the sync snapshot's depth: unknown to us, not a bug."""
        beyond = price < self.bid_floor if side == BID else price > self.ask_cap
        return book_qty == 0.0 and beyond

    def finish(self):
        if self._unsynced_since is not None and self.last_rx is not None:
            self.unsynced_ns += self.last_rx - self._unsynced_since
            self._unsynced_since = None

    @property
    def snaps_after_end(self):
        return len(self.snaps) - self.next_snap          # L >= last u of the session: nothing to compare


def replay(events, snaps_by_session, top_n=100, on_update=None):
    """Run the state machine over an event iterable. Returns [SessionReplay] in session order.
    on_update(event, book), if given, is called after each applied event (hook for later research)."""
    done = {}
    cur = None
    for e in events:
        if cur is None or e.session != cur.session:
            if cur is not None:
                cur.finish()
            if e.session in done:
                raise ValueError(f"session {e.session} appears again after another session started")
            cur = done[e.session] = SessionReplay(e.session, snaps_by_session.get(e.session, []), top_n)
        before = cur.applied
        cur.on_event(e)
        if on_update is not None and cur.applied != before:
            on_update(e, cur.book)
    if cur is not None:
        cur.finish()
    return list(done.values())


# ---------------------------------------------------------------- C++ engine (cpp/include/lob/replay.hpp)

class CppSession:
    """Results of one session from the C++ engine, with the attributes of SessionReplay. Prices and
    quantities come back as integer ticks / lots and are converted to floats; ticks / PRICE_SCALE
    is exactly the float that the exchange's decimal string gives, so the values compare equal."""

    def __init__(self, d, price_scale, qty_scale):
        for k in ("session", "first_rx", "last_rx", "events", "applied", "stale", "unsynced_events",
                  "removed_missing", "unusable_snaps", "skipped_snaps", "snaps_after_end", "unsynced_ns"):
            setattr(self, k, d[k])
        self.gaps = list(d["gaps"])
        self.syncs = list(d["syncs"])
        if not self.syncs:                       # as SessionReplay: no sync, no depth
            self.bid_floor = self.ask_cap = None
        else:
            self.bid_floor = d["bid_floor"] / price_scale if d["bid_floor"] is not None else float("inf")
            self.ask_cap = d["ask_cap"] / price_scale if d["ask_cap"] is not None else float("-inf")
        self.checks = []
        for c in d["checks"]:
            mm = [(side, p / price_scale, a / qty_scale, b / qty_scale, untracked)
                  for side, p, a, b, untracked in c["mismatches"]]
            self.checks.append(Check(self.session, c["snap_rx_ns"], c["last_update_id"], c["straddle"],
                                     c["compared"], c["excluded"], [m[:4] for m in mm],
                                     [m[:4] for m in mm if not m[4]], c["crossed"], c["book_levels"]))


# C++ replay engines (classes of the lobcpp module) by book container. "ladder" is the price ladder with
# the window chosen for BTCUSDT (2^18 ticks, see book.hpp); "ladderN" has a window of 2^N ticks (N = 3: for
# the tests, the window moves all the time; N = 12 .. 18: for the window-size comparison in bench.py).
CPP_REPLAYERS = {"vector": "VecReplayer", "map": "MapReplayer", "ladder": "LadderReplayer",
                 "ladder3": "Ladder3Replayer", "ladder12": "Ladder12Replayer", "ladder14": "Ladder14Replayer",
                 "ladder16": "Ladder16Replayer", "ladder18": "Ladder18Replayer"}


def cpp_replayer(book, top_n=100, profile=False):
    """A new C++ replay engine with the given book container (a key of CPP_REPLAYERS)."""
    import lobcpp

    return getattr(lobcpp, CPP_REPLAYERS[book])(top_n, profile)


def run_cpp(base, stems, top_n=100, book="vector", on_events=None):
    """Replay with the C++ engine, one hour at a time. on_events(stem, arrays, outputs), if given, receives
    the input arrays and the per-event outputs (status, top of book) of each hour."""
    import bookdata

    r = cpp_replayer(book, top_n)
    sn = bookdata.load_snapshots(base, stems)
    r.set_snapshots(sn["rx"], sn["session"], sn["last_id"], sn["offsets"], sn["side"], sn["price"], sn["qty"])
    for stem in stems:
        ev = bookdata.load_events(base, stem)
        out = r.process(ev["rx"], ev["session"], ev["first_id"], ev["last_id"], ev["offsets"],
                        ev["side"], ev["price"], ev["qty"])
        if on_events is not None:
            on_events(stem, ev, out)
    r.finish()
    return [CppSession(d, bookdata.PRICE_SCALE, bookdata.QTY_SCALE) for d in r.sessions()]


def session_summary(s):
    """Every result of a session (Python or C++ engine) as plain values, to compare engines with ==."""
    return (s.session, s.first_rx, s.last_rx, s.events, s.applied, s.stale, s.unsynced_events,
            s.removed_missing, s.unusable_snaps, s.skipped_snaps, s.snaps_after_end, s.unsynced_ns,
            tuple(s.gaps), tuple(s.syncs), s.bid_floor, s.ask_cap, tuple(s.checks))


# ---------------------------------------------------------------- report

def utc(rx_ns):
    return datetime.fromtimestamp(rx_ns / 1e9, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def report(sessions, top_n, seconds, engine="python"):
    keys = ("events", "applied", "stale", "unsynced", "gaps", "syncs", "checks", "bad_checks", "compared",
            "excluded", "mismatches", "unexplained", "crossed", "unusable", "after_end")
    tot = dict.fromkeys(keys, 0)
    for s in sessions:
        n = dict(events=s.events, applied=s.applied, stale=s.stale, unsynced=s.unsynced_events,
                 gaps=len(s.gaps), syncs=len(s.syncs), checks=len(s.checks),
                 bad_checks=sum(1 for c in s.checks if c.unexplained),
                 compared=sum(c.compared for c in s.checks), excluded=sum(c.excluded for c in s.checks),
                 mismatches=sum(len(c.mismatches) for c in s.checks),
                 unexplained=sum(len(c.unexplained) for c in s.checks),
                 crossed=sum(c.crossed for c in s.checks), unusable=s.unusable_snaps, after_end=s.snaps_after_end)
        for k in keys:
            tot[k] += n[k]
        first_sync = f"{(s.syncs[0][1] - s.first_rx) / 1e9:.1f} s" if s.syncs else "never"
        depth = f"bid>={s.bid_floor} ask<={s.ask_cap}" if s.syncs else "-"
        print(f"session {s.session}: {utc(s.first_rx)} -> {utc(s.last_rx)}  events={s.events:,} "
              f"applied={s.applied:,} unsynced={s.unsynced_events:,} ({s.unsynced_ns / 1e9:.1f} s) "
              f"first_sync_after={first_sync} gaps={n['gaps']} syncs={n['syncs']}")
        print(f"    checks={n['checks']} (straddle={sum(c.straddle for c in s.checks)}) "
              f"levels_compared={n['compared']:,} mismatched_levels={n['mismatches']} "
              f"(untracked={n['mismatches'] - n['unexplained']} unexplained={n['unexplained']}) "
              f"crossed={n['crossed']} unusable_snaps={n['unusable']} snaps_after_end={n['after_end']} "
              f"stale={n['stale']} removed_missing={s.removed_missing:,}  last_sync_depth: {depth}")
        for rx_ns, exp, got in s.gaps[:5]:
            print(f"    GAP at {utc(rx_ns)}: expected U={exp}, got U={got}")
        for c in [c for c in s.checks if c.unexplained][:3]:
            print(f"    UNEXPLAINED MISMATCH snapshot {utc(c.snap_rx_ns)} L={c.last_update_id}: "
                  f"{len(c.unexplained)} levels, first: {c.unexplained[:3]}  (side, price, book_qty, snap_qty)")
    t = tot
    print(f"\ntotal: sessions={len(sessions)} events={t['events']:,} applied={t['applied']:,} "
          f"unsynced={t['unsynced']:,} stale={t['stale']} gaps={t['gaps']} syncs={t['syncs']}")
    print(f"validation (top {top_n} levels per side): checks={t['checks']} levels_compared={t['compared']:,} "
          f"mismatched_levels={t['mismatches']:,} (untracked={t['mismatches'] - t['unexplained']:,} "
          f"unexplained={t['unexplained']:,}) checks_with_unexplained={t['bad_checks']} "
          f"excluded_ambiguous={t['excluded']:,} crossed_books={t['crossed']}")
    print(f"snapshots not compared: unusable={t['unusable']} after_session_end={t['after_end']}")
    print(f"wall time {seconds:.1f} s incl. Parquet reading ({t['events'] / seconds:,.0f} events/s, engine {engine})")


def write_csv(sessions, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["session", "snap_rx_utc", "last_update_id", "straddle", "compared", "excluded",
                    "mismatches", "untracked", "unexplained", "crossed", "book_levels"])
        for s in sessions:
            for c in s.checks:
                w.writerow([c.session, utc(c.snap_rx_ns), c.last_update_id, int(c.straddle), c.compared,
                            c.excluded, len(c.mismatches), len(c.mismatches) - len(c.unexplained),
                            len(c.unexplained), int(c.crossed), c.book_levels])


def main():
    ap = argparse.ArgumentParser(description="Rebuild the L2 book from Parquet and validate it against snapshots")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--parquet", type=Path, default=Path("data/parquet"))
    ap.add_argument("--top", type=int, default=100, help="levels per side compared with each snapshot")
    ap.add_argument("--max-files", type=int, default=None, help="only the first N hour files")
    ap.add_argument("--csv", type=Path, default=None, help="write one row per snapshot check")
    ap.add_argument("--engine", choices=("python", "cpp", "cpp-map", "cpp-ladder"), default="python",
                    help="python: reference; cpp / cpp-map / cpp-ladder: C++ engine with a vector / "
                         "std::map / price ladder book")
    a = ap.parse_args()

    base = a.parquet / a.symbol
    stems = hour_stems(base, a.max_files)
    if not stems:
        print(f"no Parquet files in {base / 'depth_events'} (run python etl.py first)")
        return
    t0 = time.time()
    if a.engine == "python":
        sessions = replay(iter_depth_events(base, stems), load_snapshots(base, stems), a.top)
    else:
        sessions = run_cpp(base, stems, a.top, {"cpp": "vector", "cpp-map": "map", "cpp-ladder": "ladder"}[a.engine])
    print(f"hours {stems[0]} .. {stems[-1]} ({len(stems)} files)\n")
    report(sessions, a.top, time.time() - t0, a.engine)
    if a.csv:
        write_csv(sessions, a.csv)
        print(f"wrote {a.csv}")


if __name__ == "__main__":
    main()
