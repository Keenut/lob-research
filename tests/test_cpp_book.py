"""Differential tests: the C++ books (lobcpp.VecBook, MapBook, LadderBook, and Ladder3Book whose window of
8 ticks moves all the time) against the Python reference (book.L2Book) on random depth events, comparing
the WHOLE book after every event.

Skipped when the C++ module has not been built (see cpp/CMakeLists.txt). The Python reference is
fed the same integer ticks and lots as the C++ books, so equality is exact.
"""
import random

import numpy as np
import pytest

from book import ASK, BID, L2Book

lobcpp = pytest.importorskip("lobcpp")

ALL = 10**9   # "all levels" for top()
BOOKS = ("VecBook", "MapBook", "LadderBook", "Ladder3Book")


def random_events(rng, n_events, start_mid=1_000_000, width=200):
    """Depth events (side, price, qty lists) around a drifting mid price; about 35 % removals.
    The drift lets old levels end up on the wrong side of the mid, like stale levels in a real book."""
    mid = start_mid
    for _ in range(n_events):
        mid += rng.choice((-2, 0, 0, 2))
        side, price, qty = [], [], []
        for _ in range(rng.randint(1, 15)):
            s = rng.choice((BID, ASK))
            p = mid - rng.randint(1, width) if s == BID else mid + rng.randint(1, width)
            q = 0 if rng.random() < 0.35 else rng.randint(1, 10**8)
            side.append(s)
            price.append(p)
            qty.append(q)
        yield side, price, qty


def apply_python(book, side, price, qty, last_id):
    """book.py applies one side at a time; the levels of a side keep their order inside the event."""
    bids = [(p, q) for s, p, q in zip(side, price, qty) if s == BID]
    asks = [(p, q) for s, p, q in zip(side, price, qty) if s == ASK]
    return book.apply_event(last_id, [p for p, _ in bids], [q for _, q in bids],
                            [p for p, _ in asks], [q for _, q in asks])


@pytest.mark.parametrize("seed", range(5))
def test_cpp_books_equal_python_reference_after_every_event(seed):
    rng = random.Random(seed)
    snap_bids = {1_000_000 - k: rng.randint(1, 10**8) for k in range(1, 100)}
    snap_asks = {1_000_000 + k: rng.randint(1, 10**8) for k in range(1, 100)}
    py = L2Book.from_snapshot(41, snap_bids.items(), snap_asks.items())
    cpp = [getattr(lobcpp, name)() for name in BOOKS]
    for b in cpp:
        b.load_snapshot(41, list(snap_bids), list(snap_bids.values()), list(snap_asks), list(snap_asks.values()))

    for i, (side, price, qty) in enumerate(random_events(rng, 3000)):
        last_id = 42 + i
        missing_py = apply_python(py, side, price, qty, last_id)
        arrays = (np.array(side, np.int8), np.array(price, np.int64), np.array(qty, np.int64))
        for b in cpp:
            assert b.apply(*arrays, last_id) == missing_py
            assert b.last_update_id == py.last_update_id
            assert b.top(BID, ALL) == py.top(BID, ALL)       # every level, best first
            assert b.top(ASK, ALL) == py.top(ASK, ALL)
            assert b.crossed() == py.crossed()
    st = cpp[BOOKS.index("Ladder3Book")].ladder_stats()     # the small window really moved
    assert st["shifts_up"] > 0 and st["shifts_down"] > 0 and st["deep_sets"] > 0


def test_cpp_best_and_empty_sides():
    for name in BOOKS:
        b = getattr(lobcpp, name)()
        assert b.best(BID) is None and b.best(ASK) is None and not b.crossed()
        assert b.last_update_id == -1
        b.load_snapshot(7, [99, 98], [1, 0], [101], [5])     # the qty-0 level is skipped
        assert b.best(BID) == (99, 1) and b.best(ASK) == (101, 5)
        assert b.size(BID) == 1 and b.top(BID, 5) == [(99, 1)]
        assert b.set(ASK, 99, 2) and b.crossed()            # ask AT the best bid: crossed (>=)
        assert b.set(ASK, 99, 0) and not b.crossed()
        assert b.set(BID, 99, 0) and not b.set(BID, 99, 0)  # second removal: level missing
        assert b.best(BID) is None
        assert b.set(BID, 50, 3) and b.best(BID) == (50, 3)  # first level of an empty side, far from before
        assert b.top(BID, 5) == [(50, 3)] and b.size(BID) == 1


def test_cpp_rejects_bad_input():
    b = lobcpp.VecBook()
    with pytest.raises(ValueError):
        b.apply([0, 1], [100], [1, 2], 5)                    # length mismatch
    with pytest.raises(ValueError):
        b.apply([2], [100], [1], 5)                          # side must be 0 or 1
    with pytest.raises(ValueError):
        b.best(3)