"""Unit tests for book.py (run from the repo root: python -m pytest -q)."""
from book import ASK, BID, L2Book, compare_top


def make_book():
    return L2Book.from_snapshot(10, bids=[(99.0, 1.0), (98.0, 2.0), (97.0, 3.0)],
                                asks=[(101.0, 1.5), (102.0, 2.5)])


def test_from_snapshot_and_best():
    b = make_book()
    assert b.last_update_id == 10
    assert b.best_bid() == 99.0 and b.best_ask() == 101.0
    assert not b.crossed()
    assert b.top(BID, 2) == [(99.0, 1.0), (98.0, 2.0)]
    assert b.top(ASK, 5) == [(101.0, 1.5), (102.0, 2.5)]


def test_qty_is_absolute_and_zero_removes():
    b = make_book()
    missing = b.apply_event(11, bid_px=[99.0, 98.0, 96.0], bid_qty=[4.0, 0.0, 0.0],
                            ask_px=[100.5], ask_qty=[0.7])
    assert b.bids == {99.0: 4.0, 97.0: 3.0}       # 99 replaced (not added to), 98 removed
    assert b.asks[100.5] == 0.7 and b.best_ask() == 100.5
    assert missing == 1                           # 96.0 was not in the book
    assert b.last_update_id == 11


def test_crossed_detection():
    b = make_book()
    b.apply_levels(ASK, [99.0], [1.0])
    assert b.crossed()


def test_compare_identical_is_clean():
    b = make_book()
    res = compare_top(b, dict(b.bids), dict(b.asks), n=2)
    assert res.mismatches == [] and res.compared == 4 and res.excluded == 0


def test_compare_finds_wrong_missing_and_extra_levels():
    b = make_book()
    snap_bids = {99.0: 1.0, 98.0: 2.5, 97.0: 3.0}          # 98 has a different qty
    snap_asks = {101.0: 1.5, 102.0: 2.5, 101.5: 9.0}       # 101.5 missing from the book
    b.apply_levels(BID, [98.5], [5.0])                     # 98.5 is an extra (stale) level in the book
    res = compare_top(b, snap_bids, snap_asks, n=3)
    assert sorted(res.mismatches) == [(BID, 98.0, 2.0, 2.5), (BID, 98.5, 5.0, 0.0), (ASK, 101.5, 0.0, 9.0)]


def test_compare_ignores_levels_beyond_top_n_and_excluded():
    b = make_book()
    snap_bids = {99.0: 1.0, 98.0: 2.0, 97.0: 999.0}        # 97 is wrong but outside the top 2
    snap_asks = {101.0: 7.0, 102.0: 2.5}                   # 101 is wrong but excluded
    res = compare_top(b, snap_bids, snap_asks, n=2, exclude={(ASK, 101.0)})
    assert res.mismatches == [] and res.excluded == 1 and res.compared == 3