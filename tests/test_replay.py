"""Tests for replay.py: the sync / gap / validation state machine, then an end-to-end run
raw log -> etl.py -> Parquet -> replay.py on a synthetic market whose true book is known."""
import itertools
import json
import random

import pytest

import etl
import replay
from book import ASK, BID
from replay import DepthEvent, Snapshot

_rx = itertools.count(1_000_000_000)   # increasing fake receive times


def ev(U, u, bids=(), asks=(), session=1):
    return DepthEvent(next(_rx), session, U, u, [p for p, _ in bids], [q for _, q in bids],
                      [p for p, _ in asks], [q for _, q in asks])


def snap(L, bids, asks, session=1):
    return Snapshot(next(_rx), session, L, dict(bids), dict(asks))


def run(events, snaps, top_n=100):
    by_session = {}
    for s in sorted(snaps, key=lambda s: s.last_update_id):
        by_session.setdefault(s.session, []).append(s)
    return replay.replay(events, by_session, top_n)


BASE = snap(99, {99.0: 1.0, 98.0: 1.0}, {101.0: 1.0})   # L = 99: syncs on an event with U = 100


def test_sync_on_event_that_straddles_the_snapshot():
    s = run([ev(100, 102), ev(103, 106, bids=[(99.0, 2.0)])],
            [snap(104, {99.0: 1.0}, {101.0: 1.0})])[0]
    assert s.unsynced_events == 1          # u = 102 <= L: already inside the snapshot
    assert len(s.syncs) == 1 and s.applied == 1
    assert s.book.bids == {99.0: 2.0} and s.book.last_update_id == 106


def test_snapshot_older_than_stream_is_unusable():
    s = run([ev(100, 102), ev(103, 106)], [snap(50, {99.0: 1.0}, {101.0: 1.0})])[0]
    assert s.unusable_snaps == 1 and s.book is None and s.unsynced_events == 2


def test_gap_drops_book_and_resyncs_on_next_snapshot():
    events = [ev(100, 105, bids=[(99.0, 2.0)]), ev(106, 110),
              ev(120, 125, bids=[(98.0, 9.0)]),           # GAP: 111..119 missing
              ev(126, 130, bids=[(99.0, 5.0)])]
    resync = snap(127, {99.0: 4.0, 98.0: 1.0}, {101.0: 1.0})
    s = run(events, [BASE, resync])[0]
    assert s.gaps == [(events[2].rx_ns, 111, 120)]
    assert len(s.syncs) == 2 and s.unsynced_events == 1
    assert s.book.bids == {99.0: 5.0, 98.0: 1.0}        # 98 = 1.0 from the resync snapshot, not 9.0


def test_stale_event_is_ignored():
    s = run([ev(100, 105, bids=[(99.0, 2.0)]), ev(101, 104, bids=[(99.0, 7.0)])], [BASE])[0]
    assert s.stale == 1 and s.book.bids[99.0] == 2.0


def test_validation_exact_match_and_detected_error():
    events = [ev(100, 105, bids=[(99.0, 2.0)]), ev(106, 108)]
    good = run(events, [BASE, snap(105, {99.0: 2.0, 98.0: 1.0}, {101.0: 1.0})])[0]
    (c,) = good.checks
    assert not c.straddle and c.mismatches == [] and c.compared == 3
    bad = run(events, [BASE, snap(105, {99.0: 3.0, 98.0: 1.0}, {101.0: 1.0})])[0]
    assert bad.checks[0].mismatches == [(BID, 99.0, 2.0, 3.0)]


def test_validation_inside_an_event_excludes_its_levels():
    events = [ev(100, 105, bids=[(99.0, 2.0)]), ev(106, 110, bids=[(98.0, 0.7)])]
    # L = 107 is inside the second event: 98 may hold an intermediate value (0.5), never sent to us
    s = run(events, [BASE, snap(107, {99.0: 2.0, 98.0: 0.5}, {101.0: 1.0})])[0]
    (c,) = s.checks
    assert c.straddle and c.excluded == 1 and c.mismatches == []
    s = run(events, [BASE, snap(107, {99.0: 2.5, 98.0: 0.5}, {101.0: 1.0})])[0]
    assert s.checks[0].mismatches == [(BID, 99.0, 2.0, 2.5)]   # untouched levels must still match


def test_new_session_starts_without_book():
    events = [ev(100, 105), ev(500, 505, session=2), ev(506, 510, session=2)]
    s1, s2 = run(events, [BASE, snap(507, {99.0: 1.0}, {101.0: 1.0}, session=2)])
    assert s1.applied == 1 and s1.gaps == []
    assert s2.unsynced_events == 1 and len(s2.syncs) == 1 and s2.applied == 1


def test_interleaved_sessions_raise():
    with pytest.raises(ValueError):
        run([ev(100, 105), ev(500, 505, session=2), ev(106, 110)], [BASE])


def test_on_update_hook_sees_every_applied_event():
    seen = []
    by_session = {1: [BASE]}
    replay.replay([ev(100, 105), ev(101, 104), ev(106, 110)], by_session,
                  on_update=lambda e, book: seen.append(book.last_update_id))
    assert seen == [105, 110]               # the stale event is not reported


# ---------------------------------------------------------------- end to end on a synthetic market

def synthetic_market(seed, n_updates=4000, first_id=1001):
    """Individual updates with ids, grouped into 100 ms-like events. Snapshots are taken at random
    update ids, often in the middle of an event, from the TRUE book at that id."""
    rng = random.Random(seed)
    grid = {BID: [f"{99.50 + i / 100:.2f}" for i in range(50)],
            ASK: [f"{100.00 + i / 100:.2f}" for i in range(50)]}
    true = {BID: {}, ASK: {}}
    for side in (BID, ASK):
        for p in rng.sample(grid[side], 30):
            true[side][float(p)] = float(f"{rng.randint(1, 500) / 1000:.8f}")
    snap_ids = sorted(rng.sample(range(first_id, first_id + n_updates), 12))
    snap_ids[0] = first_id + rng.randint(0, 10)          # the first one syncs the session early
    events, snaps = [], []
    uid = first_id - 1
    while uid < first_id + n_updates - 1:
        U = uid + 1
        touched = {}                                      # (side, price string) -> qty string, last wins
        for _ in range(rng.randint(1, 25)):
            uid += 1
            side = rng.choice((BID, ASK))
            p = rng.choice(grid[side])
            q = "0.00000000" if rng.random() < 0.3 else f"{rng.randint(1, 500) / 1000:.8f}"
            touched[(side, p)] = q
            if float(q) == 0:
                true[side].pop(float(p), None)
            else:
                true[side][float(p)] = float(q)
            if uid in snap_ids:
                snaps.append((uid, {s: dict(true[s]) for s in (BID, ASK)}))
        events.append((U, uid, touched))
    return events, snaps, true


def write_raw_log(path, events, snaps, session, rx0):
    """Same line format as recorder.py. A snapshot line is written 3 events after its id (REST delay)."""
    lines, rx = [], rx0
    pending = list(snaps)
    for i, (U, u, touched) in enumerate(events):
        rx += 100_000_000
        b = [[p, q] for (s, p), q in touched.items() if s == BID]
        a = [[p, q] for (s, p), q in touched.items() if s == ASK]
        msg = {"stream": "btcusdt@depth@100ms",
               "data": {"e": "depthUpdate", "E": rx // 1_000_000, "s": "BTCUSDT", "U": U, "u": u, "b": b, "a": a}}
        lines.append(f"{rx} {session} ws {json.dumps(msg)}")
        last = i == len(events) - 1
        while pending and (last or pending[0][0] <= events[max(0, i - 3)][1]):
            L, book = pending.pop(0)
            body = {"lastUpdateId": L,
                    "bids": [[f"{p:.2f}", f"{q:.8f}"] for p, q in sorted(book[BID].items(), reverse=True)],
                    "asks": [[f"{p:.2f}", f"{q:.8f}"] for p, q in sorted(book[ASK].items())]}
            rx += 1                                       # one rx_ns per REST response
            lines.append(f"{rx} {session} snap {json.dumps(body)}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return rx


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_end_to_end_raw_log_to_validated_book(tmp_path, seed):
    events, snaps, true = synthetic_market(seed)
    half = len(events) // 2                               # the session continues over two hour files
    raw = tmp_path / "raw"
    raw.mkdir()
    rx = write_raw_log(raw / "2026-01-01T00.log", events[:half], [s for s in snaps if s[0] <= events[half - 1][1]],
                       session=7, rx0=10**18)
    write_raw_log(raw / "2026-01-01T01.log", events[half:], [s for s in snaps if s[0] > events[half - 1][1]],
                  session=7, rx0=rx)
    base = tmp_path / "parquet"
    for f in sorted(raw.glob("*.log")):
        tables, bad = etl.parse_log(f)
        assert bad == 0
        etl.write_tables(tables, base, f.stem)

    stems = replay.hour_stems(base)
    assert len(stems) == 2
    (s,) = replay.replay(replay.iter_depth_events(base, stems), replay.load_snapshots(base, stems), top_n=1000)
    assert len(s.syncs) == 1 and s.gaps == [] and s.stale == 0
    assert len(s.checks) + s.snaps_after_end == len(snaps) - 1      # every other snapshot was checked
    assert len(s.checks) >= 5
    assert all(c.mismatches == [] and not c.crossed for c in s.checks)
    assert s.book.bids == true[BID] and s.book.asks == true[ASK]   # final book = true book, all levels