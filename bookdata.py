"""bookdata.py: Parquet tables -> flat integer numpy arrays for the C++ engine (lobcpp).

Prices become integer ticks and quantities integer lots:
    ticks = price * PRICE_SCALE   (BTCUSDT tick size 0.01 USDT -> 100 ticks per USDT)
    lots  = qty * QTY_SCALE       (Binance sends quantities with 8 decimals -> 1e8 lots per BTC)
The conversion is checked: a value that is not a whole number of ticks / lots raises an error
instead of being rounded silently (that would mean a wrong tick size for the symbol).
Converting back (ticks / PRICE_SCALE) gives exactly the float that the exchange's decimal string
gives, because both are the double closest to the same decimal number.

Levels use a "CSR" layout (as in sparse matrices): event i owns the levels
[offsets[i], offsets[i + 1]) of the side / price / qty arrays. One array per column, no Python
objects per event, so a whole hour goes to C++ in one call without copying.
"""
from pathlib import Path

import numpy as np

PRICE_SCALE = 100
QTY_SCALE = 100_000_000


def to_int(values, scale, what):
    """float array -> int64 array of values * scale, which must be whole numbers (up to float error)."""
    x = np.asarray(values, dtype=np.float64) * scale
    r = np.rint(x)
    if len(x):
        err = np.abs(x - r)
        i = int(np.argmax(err))
        if err[i] > 1e-3:
            raise ValueError(f"{what}: {values[i]!r} is not a multiple of 1/{scale}; wrong scale for this symbol?")
    return r.astype(np.int64)


def _read(path, columns):
    import pyarrow.parquet as pq

    t = pq.read_table(path, columns=columns)
    return {c: t.column(c).to_numpy() for c in columns}


def load_events(base, stem, price_scale=PRICE_SCALE, qty_scale=QTY_SCALE):
    """One hour of depth events as arrays: rx, session, first_id, last_id (per event), offsets (CSR)
    and side, price (ticks), qty (lots) per level. Checks that depth_updates lines up with depth_events."""
    base = Path(base)
    ev = _read(base / "depth_events" / f"{stem}.parquet",
               ["rx_ns", "session", "first_id", "last_id", "n_bids", "n_asks"])
    n_bids = ev["n_bids"].astype(np.int64)
    n_asks = ev["n_asks"].astype(np.int64)
    offsets = np.zeros(len(n_bids) + 1, dtype=np.int64)
    np.cumsum(n_bids + n_asks, out=offsets[1:])
    up_path = base / "depth_updates" / f"{stem}.parquet"
    if up_path.exists():
        up = _read(up_path, ["last_id", "side", "price", "qty"])
    else:   # etl.py writes no file for an empty table
        up = {"last_id": np.zeros(0, np.int64), "side": np.zeros(0, np.int8),
              "price": np.zeros(0), "qty": np.zeros(0)}
    if offsets[-1] != len(up["side"]):
        raise ValueError(f"{stem}: depth_events announce {offsets[-1]} levels, depth_updates has {len(up['side'])}")
    # etl.py writes, per message, its n_bids bid rows then its n_asks ask rows, all with the message's last_id
    if not np.array_equal(up["last_id"], np.repeat(ev["last_id"], n_bids + n_asks)):
        raise ValueError(f"{stem}: depth_updates.last_id does not line up with depth_events")
    expected_side = np.repeat(np.tile(np.array([0, 1], np.int8), len(n_bids)),
                              np.column_stack([n_bids, n_asks]).ravel())
    if not np.array_equal(up["side"], expected_side):
        raise ValueError(f"{stem}: depth_updates.side does not line up with n_bids / n_asks")
    return {
        "rx": ev["rx_ns"].astype(np.int64),
        "session": ev["session"].astype(np.int64),
        "first_id": ev["first_id"].astype(np.int64),
        "last_id": ev["last_id"].astype(np.int64),
        "offsets": offsets,
        "side": up["side"].astype(np.int8),
        "price": to_int(up["price"], price_scale, f"{stem} depth price"),
        "qty": to_int(up["qty"], qty_scale, f"{stem} depth qty"),
    }


def load_snapshots(base, stems, price_scale=PRICE_SCALE, qty_scale=QTY_SCALE):
    """All REST snapshots of the given hours, in receive order: rx, session, last_id (per snapshot),
    offsets (CSR) and side, price (ticks), qty (lots) per level."""
    base = Path(base)
    cols = ["rx_ns", "session", "last_update_id", "side", "price", "qty"]
    parts = [_read(base / "snapshots" / f"{s}.parquet", cols) for s in stems
             if (base / "snapshots" / f"{s}.parquet").exists()]
    if parts:
        t = {c: np.concatenate([p[c] for p in parts]) for c in cols}
    else:
        t = {"rx_ns": np.zeros(0, np.int64), "session": np.zeros(0, np.int64),
             "last_update_id": np.zeros(0, np.int64), "side": np.zeros(0, np.int8),
             "price": np.zeros(0), "qty": np.zeros(0)}
    rx, session, last = (t[c].astype(np.int64) for c in ("rx_ns", "session", "last_update_id"))
    # one REST response = one (session, rx_ns); etl.py writes its rows consecutively
    new = np.ones(len(rx), dtype=bool)
    new[1:] = (rx[1:] != rx[:-1]) | (session[1:] != session[:-1])
    starts = np.flatnonzero(new)
    offsets = np.append(starts, len(rx)).astype(np.int64)
    if len(set(zip(session[starts].tolist(), rx[starts].tolist()))) != len(starts):
        raise ValueError("the rows of a snapshot are not consecutive")
    if not np.array_equal(last, np.repeat(last[starts], np.diff(offsets))):
        raise ValueError("a snapshot has more than one last_update_id")
    return {
        "rx": rx[starts],
        "session": session[starts],
        "last_id": last[starts],
        "offsets": offsets,
        "side": t["side"].astype(np.int8),
        "price": to_int(t["price"], price_scale, "snapshot price"),
        "qty": to_int(t["qty"], qty_scale, "snapshot qty"),
    }


def events_from_objects(events, price_scale=PRICE_SCALE, qty_scale=QTY_SCALE):
    """replay.DepthEvent objects -> the arrays of load_events (used by the tests)."""
    side, price, qty, n = [], [], [], []
    for e in events:
        side += [0] * len(e.bid_px) + [1] * len(e.ask_px)
        price += list(e.bid_px) + list(e.ask_px)
        qty += list(e.bid_qty) + list(e.ask_qty)
        n.append(len(e.bid_px) + len(e.ask_px))
    offsets = np.zeros(len(n) + 1, dtype=np.int64)
    np.cumsum(n, out=offsets[1:])
    return {
        "rx": np.array([e.rx_ns for e in events], np.int64),
        "session": np.array([e.session for e in events], np.int64),
        "first_id": np.array([e.first_id for e in events], np.int64),
        "last_id": np.array([e.last_id for e in events], np.int64),
        "offsets": offsets,
        "side": np.array(side, np.int8),
        "price": to_int(price, price_scale, "price"),
        "qty": to_int(qty, qty_scale, "qty"),
    }


def snapshots_from_objects(snapshots, price_scale=PRICE_SCALE, qty_scale=QTY_SCALE):
    """replay.Snapshot objects (in receive order) -> the arrays of load_snapshots (used by the tests)."""
    side, price, qty, n = [], [], [], []
    for s in snapshots:
        side += [0] * len(s.bids) + [1] * len(s.asks)
        price += list(s.bids) + list(s.asks)
        qty += list(s.bids.values()) + list(s.asks.values())
        n.append(len(s.bids) + len(s.asks))
    offsets = np.zeros(len(n) + 1, dtype=np.int64)
    np.cumsum(n, out=offsets[1:])
    return {
        "rx": np.array([s.rx_ns for s in snapshots], np.int64),
        "session": np.array([s.session for s in snapshots], np.int64),
        "last_id": np.array([s.last_update_id for s in snapshots], np.int64),
        "offsets": offsets,
        "side": np.array(side, np.int8),
        "price": to_int(price, price_scale, "snapshot price"),
        "qty": to_int(qty, qty_scale, "snapshot qty"),
    }
