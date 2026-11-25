r"""replay.py: rebuild the L2 order book from the Parquet tables and validate it against REST snapshots.

Usage (PowerShell, from the repo root, after python etl.py):
    python replay.py                          # all converted hours, compare the top 100 levels per side
    python replay.py --max-files 2            # quick run on the first 2 hours
    python replay.py --top 1000 --csv docs\validation_top1000.csv

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
Limitation: a snapshot has at most 5000 levels per side, so levels deeper than the sync snapshot
that never changed are unknown. Comparing with a large --top can show mismatches at the deep end
that are this effect, not bugs.
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
    mismatches: list
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
        self.checks.append(Check(self.session, s.rx_ns, L, L > last, res.compared, res.excluded,
                                 res.mismatches, self.book.crossed(),
                                 len(self.book.bids) + len(self.book.asks)))

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


# ---------------------------------------------------------------- report

def utc(rx_ns):
    return datetime.fromtimestamp(rx_ns / 1e9, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def report(sessions, top_n, seconds):
    tot = dict(events=0, applied=0, stale=0, unsynced=0, gaps=0, syncs=0, checks=0, bad_checks=0,
               compared=0, excluded=0, mismatches=0, crossed=0, unusable=0, after_end=0)
    for s in sessions:
        bad = [c for c in s.checks if c.mismatches]
        first_sync = f"{(s.syncs[0][1] - s.first_rx) / 1e9:.1f} s" if s.syncs else "never"
        print(f"session {s.session}: {utc(s.first_rx)} -> {utc(s.last_rx)}  events={s.events:,} "
              f"applied={s.applied:,} unsynced={s.unsynced_events:,} ({s.unsynced_ns / 1e9:.1f} s) "
              f"first_sync_after={first_sync} gaps={len(s.gaps)} syncs={len(s.syncs)}")
        print(f"    checks={len(s.checks)} (straddle={sum(c.straddle for c in s.checks)}) "
              f"levels_compared={sum(c.compared for c in s.checks):,} "
              f"mismatched_levels={sum(len(c.mismatches) for c in s.checks)} checks_with_mismatch={len(bad)} "
              f"crossed={sum(c.crossed for c in s.checks)} unusable_snaps={s.unusable_snaps} "
              f"snaps_after_end={s.snaps_after_end} stale={s.stale} removed_missing={s.removed_missing:,}")
        for rx_ns, exp, got in s.gaps[:5]:
            print(f"    GAP at {utc(rx_ns)}: expected U={exp}, got U={got}")
        for c in bad[:3]:
            print(f"    MISMATCH snapshot {utc(c.snap_rx_ns)} L={c.last_update_id}: {len(c.mismatches)} levels, "
                  f"first: {c.mismatches[:3]}  (side, price, book_qty, snap_qty)")
        tot["events"] += s.events
        tot["applied"] += s.applied
        tot["stale"] += s.stale
        tot["unsynced"] += s.unsynced_events
        tot["gaps"] += len(s.gaps)
        tot["syncs"] += len(s.syncs)
        tot["checks"] += len(s.checks)
        tot["bad_checks"] += len(bad)
        tot["compared"] += sum(c.compared for c in s.checks)
        tot["excluded"] += sum(c.excluded for c in s.checks)
        tot["mismatches"] += sum(len(c.mismatches) for c in s.checks)
        tot["crossed"] += sum(c.crossed for c in s.checks)
        tot["unusable"] += s.unusable_snaps
        tot["after_end"] += s.snaps_after_end
    t = tot
    print(f"\ntotal: sessions={len(sessions)} events={t['events']:,} applied={t['applied']:,} "
          f"unsynced={t['unsynced']:,} stale={t['stale']} gaps={t['gaps']} syncs={t['syncs']}")
    print(f"validation (top {top_n} levels per side): checks={t['checks']} checks_with_mismatch={t['bad_checks']} "
          f"levels_compared={t['compared']:,} mismatched_levels={t['mismatches']} "
          f"excluded_ambiguous={t['excluded']:,} crossed_books={t['crossed']}")
    print(f"snapshots not compared: unusable={t['unusable']} after_session_end={t['after_end']}")
    print(f"wall time {seconds:.1f} s incl. Parquet reading ({t['events'] / seconds:,.0f} events/s)")


def write_csv(sessions, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["session", "snap_rx_utc", "last_update_id", "straddle", "compared", "excluded",
                    "mismatches", "crossed", "book_levels"])
        for s in sessions:
            for c in s.checks:
                w.writerow([c.session, utc(c.snap_rx_ns), c.last_update_id, int(c.straddle), c.compared,
                            c.excluded, len(c.mismatches), int(c.crossed), c.book_levels])


def main():
    ap = argparse.ArgumentParser(description="Rebuild the L2 book from Parquet and validate it against snapshots")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--parquet", type=Path, default=Path("data/parquet"))
    ap.add_argument("--top", type=int, default=100, help="levels per side compared with each snapshot")
    ap.add_argument("--max-files", type=int, default=None, help="only the first N hour files")
    ap.add_argument("--csv", type=Path, default=None, help="write one row per snapshot check")
    a = ap.parse_args()

    base = a.parquet / a.symbol
    stems = hour_stems(base, a.max_files)
    if not stems:
        print(f"no Parquet files in {base / 'depth_events'} (run python etl.py first)")
        return
    t0 = time.time()
    snaps = load_snapshots(base, stems)
    sessions = replay(iter_depth_events(base, stems), snaps, a.top)
    print(f"hours {stems[0]} .. {stems[-1]} ({len(stems)} files)\n")
    report(sessions, a.top, time.time() - t0)
    if a.csv:
        write_csv(sessions, a.csv)
        print(f"wrote {a.csv}")


if __name__ == "__main__":
    main()