"""book.py: reference L2 order book (pure Python), rebuilt from Binance depth diffs.

L2 means one aggregated quantity per price level: no individual orders, so no queue
position and no price-time priority can be reconstructed from this data.

A depth diff gives the NEW total quantity of a price level (not a change): qty 0 removes it.
This is the slow, simple reference: two dicts {price: qty}, sorting only when asked.
The C++ engine (phase 4) must give identical results on the same input (differential tests).
Prices and quantities are floats parsed from the exchange's decimal strings; the same string
always gives the same float, so exact == comparisons between book and snapshot are valid.
"""
import heapq
from typing import NamedTuple

BID, ASK = 0, 1  # same encoding as the 'side' column of the Parquet tables


class L2Book:
    __slots__ = ("bids", "asks", "last_update_id")

    def __init__(self):
        self.bids = {}               # price -> qty, unsorted
        self.asks = {}
        self.last_update_id = None   # Binance update id the book is consistent with

    @classmethod
    def from_snapshot(cls, last_update_id, bids, asks):
        """bids, asks: iterables of (price, qty), e.g. dict.items()."""
        book = cls()
        book.bids = {p: q for p, q in bids if q > 0}
        book.asks = {p: q for p, q in asks if q > 0}
        book.last_update_id = last_update_id
        return book

    def apply_levels(self, side, prices, qtys):
        """Set each price level to its new quantity. Returns how many removals hit a level
        that was not in the book (normal: it was outside the snapshot depth, or already gone)."""
        levels = self.bids if side == BID else self.asks
        missing = 0
        for p, q in zip(prices, qtys):
            if q == 0.0:
                if levels.pop(p, None) is None:
                    missing += 1
            else:
                levels[p] = q
        return missing

    def apply_event(self, last_id, bid_px, bid_qty, ask_px, ask_qty):
        """Apply one depth event. Sequence checks (gaps, stale events) are the caller's job."""
        missing = self.apply_levels(BID, bid_px, bid_qty) + self.apply_levels(ASK, ask_px, ask_qty)
        self.last_update_id = last_id
        return missing

    def best_bid(self):
        return max(self.bids) if self.bids else None   # O(levels): fine for checks, not per event

    def best_ask(self):
        return min(self.asks) if self.asks else None

    def crossed(self):
        bb, ba = self.best_bid(), self.best_ask()
        return bb is not None and ba is not None and bb >= ba

    def top(self, side, n):
        """The n best levels of one side as [(price, qty), ...], best first."""
        if side == BID:
            return [(p, self.bids[p]) for p in heapq.nlargest(n, self.bids)]
        return [(p, self.asks[p]) for p in heapq.nsmallest(n, self.asks)]


class Comparison(NamedTuple):
    compared: int      # price levels checked
    excluded: int      # levels skipped because their true value is ambiguous
    mismatches: list   # [(side, price, book_qty, snapshot_qty)], qty 0.0 = level absent


def compare_top(book, snap_bids, snap_asks, n, exclude=frozenset()):
    """Compare the book with a snapshot ({price: qty} per side) over the snapshot's n best levels.

    The checked price range of a side runs from its best level to its n-th level. Inside it, every
    price present in the book OR in the snapshot is checked, so both a missing level and an extra
    (stale) level in the book count as a mismatch. exclude holds (side, price) pairs to skip.
    """
    compared = excluded = 0
    mismatches = []
    for side, mine, ref in ((BID, book.bids, snap_bids), (ASK, book.asks, snap_asks)):
        if not ref:
            continue
        if side == BID:
            top = heapq.nlargest(n, ref)
            edge = top[-1]
            prices = set(top) | {p for p in mine if p >= edge}
        else:
            top = heapq.nsmallest(n, ref)
            edge = top[-1]
            prices = set(top) | {p for p in mine if p <= edge}
        for p in sorted(prices):
            if (side, p) in exclude:
                excluded += 1
                continue
            compared += 1
            a, b = mine.get(p, 0.0), ref.get(p, 0.0)
            if a != b:
                mismatches.append((side, p, a, b))
    return Comparison(compared, excluded, mismatches)