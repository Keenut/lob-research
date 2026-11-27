"""Differential tests of the C++ replay engine (lobcpp.VecReplayer / MapReplayer / LadderReplayer, and
Ladder3Replayer whose window of 8 ticks moves all the time) against the Python reference
(replay.replay): every session result (counts, gaps, syncs, every snapshot check with its mismatch
lists) must be identical, and the top of book must match after every applied event.
Skipped when the C++ module has not been built."""
import random

import numpy as np
import pytest

import bookdata
import etl
import replay
from book import ASK, BID
from replay import DepthEvent, Snapshot
from test_replay import BASE, ev, snap, synthetic_market, write_raw_log

lobcpp = pytest.importorskip("lobcpp")

KINDS = ("vector", "map", "ladder", "ladder3")   # keys of replay.CPP_REPLAYERS


def run_python(events, snaps, top_n):
    """Python reference. Returns (session summaries, {event index: top of book} for applied events)."""
    by_session = {}
    for s in sorted(snaps, key=lambda s: s.last_update_id):
        by_session.setdefault(s.session, []).append(s)
    index = {"i": -1}
    tops = {}

    def numbered():
        for e in events:
            index["i"] += 1
            yield e

    def hook(e, book):
        bb, ba = book.best_bid(), book.best_ask()
        tops[index["i"]] = (bb, book.bids.get(bb), ba, book.asks.get(ba))

    sessions = replay.replay(numbered(), by_session, top_n, on_update=hook)
    return [replay.session_summary(s) for s in sessions], tops


def run_engine(events, snaps, top_n, kind):
    """C++ engine (a key of replay.CPP_REPLAYERS) on the same objects. Returns (engine, per-event outputs)."""
    r = replay.cpp_replayer(kind, top_n)
    sn = bookdata.snapshots_from_objects(sorted(snaps, key=lambda s: s.rx_ns))   # receive order
    r.set_snapshots(sn["rx"], sn["session"], sn["last_id"], sn["offsets"], sn["side"], sn["price"], sn["qty"])
    a = bookdata.events_from_objects(events)
    out = r.process(a["rx"], a["session"], a["first_id"], a["last_id"], a["offsets"], a["side"], a["price"], a["qty"])
    r.finish()
    return r, out


def run_cpp(events, snaps, top_n, kind):
    """C++ engine on the same objects. Returns (session summaries, per-event outputs)."""
    r, out = run_engine(events, snaps, top_n, kind)
    sessions = [replay.CppSession(d, bookdata.PRICE_SCALE, bookdata.QTY_SCALE) for d in r.sessions()]
    return [replay.session_summary(s) for s in sessions], out


def assert_same(events, snaps, top_n=100):
    py, tops = run_python(events, snaps, top_n)
    for kind in KINDS:
        cpp, out = run_cpp(events, snaps, top_n, kind)
        assert cpp == py, kind
        applied = np.flatnonzero(out["status"] == 0)
        assert sorted(tops) == applied.tolist(), kind           # the same events were applied
        for i in applied:
            bb, bq, ba, aq = tops[i]
            got = (out["bid_px"][i], out["bid_qty"][i], out["ask_px"][i], out["ask_qty"][i])
            want = (round(bb * 100) if bb is not None else 0, round(bq * 1e8) if bq is not None else 0,
                    round(ba * 100) if ba is not None else 0, round(aq * 1e8) if aq is not None else 0)
            assert got == want, (kind, i)
    return py


# ---------------------------------------------------------------- the scenarios of test_replay.py

def scenarios():
    yield "sync on straddling event", [ev(100, 102), ev(103, 106, bids=[(99.0, 2.0)])], \
        [snap(104, {99.0: 1.0}, {101.0: 1.0})]
    yield "unusable snapshot", [ev(100, 102), ev(103, 106)], [snap(50, {99.0: 1.0}, {101.0: 1.0})]
    yield "gap and resync", [ev(100, 105, bids=[(99.0, 2.0)]), ev(106, 110), ev(120, 125, bids=[(98.0, 9.0)]),
                             ev(126, 130, bids=[(99.0, 5.0)])], \
        [BASE, snap(127, {99.0: 4.0, 98.0: 1.0}, {101.0: 1.0})]
    yield "stale event", [ev(100, 105, bids=[(99.0, 2.0)]), ev(101, 104, bids=[(99.0, 7.0)])], [BASE]
    yield "exact validation, wrong snapshot", [ev(100, 105, bids=[(99.0, 2.0)]), ev(106, 108)], \
        [BASE, snap(105, {99.0: 3.0, 98.0: 1.0}, {101.0: 1.0})]
    yield "straddle exclusion", [ev(100, 105, bids=[(99.0, 2.0)]), ev(106, 110, bids=[(98.0, 0.7)])], \
        [BASE, snap(107, {99.0: 2.5, 98.0: 0.5}, {101.0: 1.0})]
    yield "untracked vs unexplained", [ev(100, 105), ev(106, 108)], \
        [BASE, snap(105, {99.0: 1.0, 98.5: 2.0, 98.0: 1.0, 97.0: 4.0}, {101.0: 1.0, 103.0: 6.0})]
    yield "new session", [ev(100, 105), ev(500, 505, session=2), ev(506, 510, session=2)], \
        [BASE, snap(507, {99.0: 1.0}, {101.0: 1.0}, session=2)]
    yield "no snapshot at all", [ev(100, 105), ev(106, 110)], []
    # cases found missing by injecting bugs into replay.hpp (each one let a bug through before):
    yield "gap of a single update id", [ev(100, 105, bids=[(99.0, 2.0)]), ev(107, 110, bids=[(99.0, 3.0)])], [BASE]
    yield "duplicate of the last event", [ev(100, 105, bids=[(99.0, 2.0)]), ev(100, 105, bids=[(99.0, 2.0)]),
                                          ev(106, 108)], [BASE]
    yield "beyond sync depth with a wrong qty", [ev(100, 105, bids=[(97.0, 3.0)]), ev(106, 108)], \
        [BASE, snap(105, {99.0: 1.0, 98.0: 1.0, 97.0: 4.0}, {101.0: 1.0})]


@pytest.mark.parametrize("name,events,snaps", list(scenarios()), ids=lambda x: x if isinstance(x, str) else "")
def test_scenarios_identical(name, events, snaps):
    assert_same(events, snaps, top_n=100)
    assert_same(events, snaps, top_n=1)


def test_interleaved_sessions_raise_in_both_engines():
    events, snaps = [ev(100, 105), ev(500, 505, session=2), ev(106, 110)], [BASE]
    with pytest.raises(ValueError):
        run_python(events, snaps, 100)
    for kind in KINDS:
        with pytest.raises(ValueError):
            run_cpp(events, snaps, 100, kind)


def test_bad_arrays_are_rejected():
    r = lobcpp.VecReplayer(10)
    z = np.zeros(1, np.int64)
    with pytest.raises(ValueError):    # offsets must have n + 1 elements
        r.process(z, z, z, z, np.zeros(1, np.int64), np.zeros(0, np.int8), np.zeros(0, np.int64), np.zeros(0, np.int64))
    with pytest.raises(ValueError):    # offsets must end at the number of levels
        r.process(z, z, z, z, np.array([0, 2]), np.zeros(1, np.int8), np.zeros(1, np.int64), np.zeros(1, np.int64))


# ---------------------------------------------------------------- random multi-session streams

def objects_from_market(market_events, market_snaps, session, rx0, depth=None, corrupt=None):
    """synthetic_market() output -> DepthEvent / Snapshot objects. depth: keep only the n best snapshot
    levels per side (like Binance's 5000-level limit). corrupt: index of a snapshot to falsify."""
    events, rx = [], rx0
    for U, u, touched in market_events:
        rx += 100
        b = [(float(p), float(q)) for (s, p), q in touched.items() if s == BID]
        a = [(float(p), float(q)) for (s, p), q in touched.items() if s == ASK]
        events.append(DepthEvent(rx, session, U, u, [p for p, _ in b], [q for _, q in b],
                                 [p for p, _ in a], [q for _, q in a]))
    snaps = []
    for k, (L, book) in enumerate(market_snaps):
        bids = dict(sorted(book[BID].items(), reverse=True)[:depth])
        asks = dict(sorted(book[ASK].items())[:depth])
        if k == corrupt and bids:
            p = next(iter(bids))
            bids[p] = round(bids[p] + 0.001, 8)   # a wrong quantity (still 8 decimals): unexplained mismatch
        snaps.append(Snapshot(rx0 + 50 + k, session, L, bids, asks))
    return events, snaps


@pytest.mark.parametrize("seed", range(6))
def test_random_streams_identical(seed):
    rng = random.Random(seed)
    all_events, all_snaps = [], []
    for k, session in enumerate((11, 12, 13)):
        mev, msn, _ = synthetic_market(seed * 10 + k, n_updates=2500, first_id=1001 + k * 10**6)
        events, snaps = objects_from_market(mev, msn, session, rx0=10**12 * (k + 1),
                                            depth=rng.choice((None, 12, 20)), corrupt=rng.randrange(len(msn)))
        if rng.random() < 0.7:                    # a gap: drop one event in the middle
            del events[rng.randrange(len(events) // 4, len(events) // 2)]
        if rng.random() < 0.7:                    # a stale event: an old event received again
            j = rng.randrange(5, len(events) - 5)
            events.insert(j + 3, events[j])
        all_events += events
        all_snaps += snaps
    all_snaps.append(Snapshot(1, 11, 5, {99.9: 1.0}, {100.1: 1.0}))   # older than the stream: unusable
    for top_n in (3, 10, 1000):
        summaries = assert_same(all_events, all_snaps, top_n)
    assert len(summaries) == 3


def test_small_ladder_uses_its_map_and_moves():
    """The differential tests above are strong for the ladder only if its std::map is used and its window
    moves. With 8 slots on the synthetic market (50 ticks per side) most updates go to the map and the
    window moves a few times per stream (cpp/tests/test_book.cpp moves it thousands of times, with the
    invariants checked). With the default window (2^18 slots) everything stays in the window."""
    mev, msn, _ = synthetic_market(3, n_updates=3000)
    events, snaps = objects_from_market(mev, msn, session=5, rx0=10**12)
    assert_same(events, snaps)
    st = run_engine(events, snaps, 100, "ladder3")[0].ladder_stats()
    updates = sum(len(e.bid_px) + len(e.ask_px) for e in events)
    assert st["slots"] == 8 and st["shifts_up"] > 0 and st["shifts_down"] > 0 and st["deep_sets"] > updates // 2
    st = run_engine(events, snaps, 100, "ladder")[0].ladder_stats()
    assert st == dict(slots=2**18, shifts_up=0, shifts_down=0, deep_sets=0)


# ---------------------------------------------------------------- end to end through Parquet

def test_parquet_end_to_end_same_report(tmp_path, capsys):
    events, snaps, _ = synthetic_market(4, n_updates=4000)
    raw = tmp_path / "raw"
    raw.mkdir()
    half = len(events) // 2
    rx = write_raw_log(raw / "2026-01-01T00.log", events[:half], [s for s in snaps if s[0] <= events[half - 1][1]],
                       session=7, rx0=10**18)
    write_raw_log(raw / "2026-01-01T01.log", events[half:], [s for s in snaps if s[0] > events[half - 1][1]],
                  session=7, rx0=rx)
    base = tmp_path / "parquet"
    for f in etl.raw_files(raw):
        tables, _ = etl.parse_log(f)
        etl.write_tables(tables, base, etl.hour_of(f))
    stems = replay.hour_stems(base)

    py = replay.replay(replay.iter_depth_events(base, stems), replay.load_snapshots(base, stems), 5)
    replay.report(py, 5, 1.0)
    want = capsys.readouterr().out.splitlines()[:-1]          # all but the time line
    assert sum(len(s.checks) for s in py) >= 5
    for kind in KINDS:
        cpp = replay.run_cpp(base, stems, 5, kind)
        assert [replay.session_summary(s) for s in cpp] == [replay.session_summary(s) for s in py]
        replay.report(cpp, 5, 1.0, kind)
        assert capsys.readouterr().out.splitlines()[:-1] == want


def test_to_int_rejects_values_off_the_grid():
    assert bookdata.to_int([57848.86, 0.01], 100, "p").tolist() == [5784886, 1]
    with pytest.raises(ValueError):
        bookdata.to_int([57848.865], 100, "p")                 # half a tick: wrong tick size


# ---------------------------------------------------------------- benchmark instrumentation

def test_profile_matches_a_brute_force_count_and_changes_nothing():
    """profile=True must not change any result, and its counts must equal a direct recount: for every
    applied level update, how many levels were better, whether it inserted, erased, etc., its distance
    in ticks from the best price of its side, and (when it erases the best level) the distance to the
    next level. Distance buckets are int.bit_length(), the same as pow2_bucket() in replay.hpp."""
    mev, msn, _ = synthetic_market(9, n_updates=3000)
    events, snaps = objects_from_market(mev, msn, session=5, rx0=10**12)
    plain, _ = run_cpp(events, snaps, 10, "vector")
    r = lobcpp.VecReplayer(10, profile=True)
    sn = bookdata.snapshots_from_objects(snaps)
    r.set_snapshots(sn["rx"], sn["session"], sn["last_id"], sn["offsets"], sn["side"], sn["price"], sn["qty"])
    a = bookdata.events_from_objects(events)
    out = r.process(a["rx"], a["session"], a["first_id"], a["last_id"], a["offsets"], a["side"], a["price"],
                    a["qty"], timings=True)
    r.finish()
    assert [replay.session_summary(replay.CppSession(d, 100, 10**8)) for d in r.sessions()] == plain
    assert len(out["ns"]) == len(events) and out["ns"].min() >= 0

    # recount: the first snapshot syncs (no gaps in this stream), then every event with u > L is applied
    first = snaps[0]
    book = {BID: {round(p * 100): round(q * 1e8) for p, q in first.bids.items()},
            ASK: {round(p * 100): round(q * 1e8) for p, q in first.asks.items()}}
    want = dict(inserts=0, erases=0, in_place=0, missing=0, shifted=0, by_depth=[0] * 6,
                behind=[0] * 64, ahead=[0] * 64, no_best=0, next_gap=[0] * 64, last_level=0)
    for e in events:
        if e.last_id <= first.last_update_id:
            continue
        for side, px, qs in ((BID, e.bid_px, e.bid_qty), (ASK, e.ask_px, e.ask_qty)):
            for p, q in zip(px, qs):
                p, q = round(p * 100), round(q * 1e8)
                levels = book[side]
                better = sum(1 for x in levels if (x > p if side == BID else x < p))
                found = p in levels
                if q == 0:
                    want["erases" if found else "missing"] += 1
                else:
                    want["in_place" if found else "inserts"] += 1
                if (q == 0) == found:            # erase or insert: the vector shifts 'better' elements
                    want["shifted"] += better
                    want["by_depth"][min(len(str(better)) if better else 0, 5)] += 1
                if not levels:                   # distances, measured before the update
                    want["no_best"] += 1
                else:
                    ranked = sorted(levels, reverse=(side == BID))    # best price first
                    d = ranked[0] - p if side == BID else p - ranked[0]
                    want["behind" if d >= 0 else "ahead"][abs(d).bit_length()] += 1
                    if q == 0 and found and d == 0:                    # the best level itself is erased
                        if len(ranked) == 1:
                            want["last_level"] += 1
                        else:
                            want["next_gap"][abs(ranked[1] - ranked[0]).bit_length()] += 1
                if q == 0:
                    levels.pop(p, None)
                else:
                    levels[p] = q
    got = r.profile()
    assert {k: got[k] for k in want} == want
    assert got["events"] == (out["status"] == 0).sum()
    # the stream really exercises the new counters (an all-zero recount would prove nothing)
    assert sum(want["ahead"]) > 0 and sum(want["next_gap"]) > 0
    assert sum(1 for n in want["behind"] if n) >= 3      # several distance buckets, not just one
    assert sum(want["behind"]) + sum(want["ahead"]) + want["no_best"] == \
        want["inserts"] + want["erases"] + want["in_place"] + want["missing"]
