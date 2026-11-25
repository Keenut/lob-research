r"""etl.py: convert the raw recorder logs into Parquet tables (one file per table per UTC hour).

Usage (PowerShell, from the repo root):
    python etl.py                    # every finished hour in data\raw\BTCUSDT -> data\parquet\BTCUSDT
    python etl.py --include-latest   # also convert the hour the recorder is still writing
    python etl.py --force            # redo hours that already have output
    python etl.py --symbol ETHUSDT

Output: data/parquet/<SYMBOL>/<table>/<YYYY-MM-DDTHH>.parquet, read back with DuckDB using
read_parquet('data/parquet/<SYMBOL>/<table>/*.parquet').

Tables
  depth_events   one row per depth message:  rx_ns, session, event_ms (E), first_id (U), last_id (u), n_bids, n_asks
  depth_updates  one row per price level:    rx_ns, session, event_ms, last_id, side (0 bid, 1 ask), price, qty
                 (qty 0 means the level was removed; join to depth_events on session + last_id)
  trades         one row per trade:          rx_ns, session, event_ms, trade_id, price, qty, trade_ms, buyer_is_maker
  snapshots      one row per snapshot level: rx_ns, session, last_update_id, side, price, qty
  clock          one row per clock sample:   rx_ns, session, offset_ms, rtt_ms, server_ms
                 (offset_ms = Binance time - local time at the midpoint, positive: this PC is behind)
  session_log    open / err / close records: rx_ns, session, kind, text

Prices and quantities are stored as float64 for research. The C++ order book (phase 2) should
use integer ticks, converting at load time.
"""
import argparse
import json
import time
from pathlib import Path

SCHEMAS = {
    "depth_events": [("rx_ns", "i64"), ("session", "i64"), ("event_ms", "i64"), ("first_id", "i64"),
                     ("last_id", "i64"), ("n_bids", "i32"), ("n_asks", "i32")],
    "depth_updates": [("rx_ns", "i64"), ("session", "i64"), ("event_ms", "i64"), ("last_id", "i64"),
                      ("side", "i8"), ("price", "f64"), ("qty", "f64")],
    "trades": [("rx_ns", "i64"), ("session", "i64"), ("event_ms", "i64"), ("trade_id", "i64"),
               ("price", "f64"), ("qty", "f64"), ("trade_ms", "i64"), ("buyer_is_maker", "bool")],
    "snapshots": [("rx_ns", "i64"), ("session", "i64"), ("last_update_id", "i64"), ("side", "i8"),
                  ("price", "f64"), ("qty", "f64")],
    "clock": [("rx_ns", "i64"), ("session", "i64"), ("offset_ms", "f64"), ("rtt_ms", "f64"),
              ("server_ms", "i64")],
    "session_log": [("rx_ns", "i64"), ("session", "i64"), ("kind", "str"), ("text", "str")],
}


def parse_log(path):
    """Parse one raw log file into column lists. Returns (tables, number_of_bad_lines)."""
    t = {name: {col: [] for col, _ in cols} for name, cols in SCHEMAS.items()}
    ev, up, tr, sn, ck, sl = (t[k] for k in ("depth_events", "depth_updates", "trades",
                                            "snapshots", "clock", "session_log"))
    bad = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split(" ", 3)
            if len(parts) < 4:
                bad += 1          # truncated line
                continue
            try:
                rx_ns, sid = int(parts[0]), int(parts[1])
            except ValueError:
                bad += 1
                continue
            kind, payload = parts[2], parts[3]
            try:
                if kind == "ws":
                    msg = json.loads(payload)
                    d, stream = msg["data"], msg["stream"]
                    if stream.endswith("@depth@100ms"):
                        for col, val in (("rx_ns", rx_ns), ("session", sid), ("event_ms", d["E"]),
                                         ("first_id", d["U"]), ("last_id", d["u"])):
                            ev[col].append(val)
                        ev["n_bids"].append(len(d["b"]))
                        ev["n_asks"].append(len(d["a"]))
                        for side, levels in ((0, d["b"]), (1, d["a"])):
                            n = len(levels)
                            if n:
                                up["rx_ns"].extend([rx_ns] * n)
                                up["session"].extend([sid] * n)
                                up["event_ms"].extend([d["E"]] * n)
                                up["last_id"].extend([d["u"]] * n)
                                up["side"].extend([side] * n)
                                up["price"].extend(float(p) for p, _ in levels)
                                up["qty"].extend(float(q) for _, q in levels)
                    elif stream.endswith("@trade"):
                        tr["rx_ns"].append(rx_ns)
                        tr["session"].append(sid)
                        tr["event_ms"].append(d["E"])
                        tr["trade_id"].append(d["t"])
                        tr["price"].append(float(d["p"]))
                        tr["qty"].append(float(d["q"]))
                        tr["trade_ms"].append(d["T"])
                        tr["buyer_is_maker"].append(bool(d["m"]))
                elif kind == "snap":
                    obj = json.loads(payload)
                    for side, levels in ((0, obj["bids"]), (1, obj["asks"])):
                        n = len(levels)
                        sn["rx_ns"].extend([rx_ns] * n)
                        sn["session"].extend([sid] * n)
                        sn["last_update_id"].extend([obj["lastUpdateId"]] * n)
                        sn["side"].extend([side] * n)
                        sn["price"].extend(float(p) for p, _ in levels)
                        sn["qty"].extend(float(q) for _, q in levels)
                elif kind == "clk":
                    obj = json.loads(payload)
                    t0, t1, srv = obj["t0"], obj["t1"], obj["srv"]["serverTime"]
                    ck["rx_ns"].append(rx_ns)
                    ck["session"].append(sid)
                    ck["offset_ms"].append(srv - (t0 + t1) / 2e6)
                    ck["rtt_ms"].append((t1 - t0) / 1e6)
                    ck["server_ms"].append(srv)
                elif kind in ("open", "err", "close"):
                    sl["rx_ns"].append(rx_ns)
                    sl["session"].append(sid)
                    sl["kind"].append(kind)
                    sl["text"].append(payload)
            except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                bad += 1
    return t, bad


def write_tables(tables, out_dir, stem):
    """Write each non-empty table to <out_dir>/<table>/<stem>.parquet (via a temp file). Returns row counts."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    types = {"i64": pa.int64(), "i32": pa.int32(), "i8": pa.int8(), "f64": pa.float64(),
             "bool": pa.bool_(), "str": pa.string()}
    counts = {}
    for name, cols in SCHEMAS.items():
        data = tables[name]
        n = len(data[cols[0][0]])
        counts[name] = n
        if n == 0:
            continue
        schema = pa.schema([(c, types[tp]) for c, tp in cols])
        table = pa.Table.from_pydict(data, schema=schema)
        folder = out_dir / name
        folder.mkdir(parents=True, exist_ok=True)
        tmp = folder / f"{stem}.parquet.tmp"
        pq.write_table(table, tmp, compression="zstd")
        tmp.replace(folder / f"{stem}.parquet")
    return counts


def main():
    ap = argparse.ArgumentParser(description="Raw recorder logs -> Parquet tables")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--raw", type=Path, default=Path("data/raw"))
    ap.add_argument("--out", type=Path, default=Path("data/parquet"))
    ap.add_argument("--include-latest", action="store_true", help="also convert the newest (maybe still open) file")
    ap.add_argument("--force", action="store_true", help="redo hours that already have output")
    a = ap.parse_args()

    raw_dir, out_dir = a.raw / a.symbol, a.out / a.symbol
    files = sorted(raw_dir.glob("*.log"))
    if files and not a.include_latest:
        files = files[:-1]        # the newest hour is probably still being written by the recorder
    if not files:
        print(f"no log files to convert in {raw_dir}")
        return
    for f in files:
        stem = f.stem
        if (out_dir / "depth_events" / f"{stem}.parquet").exists() and not a.force:
            print(f"{stem}: already converted, skipping")
            continue
        t0 = time.time()
        tables, bad = parse_log(f)
        counts = write_tables(tables, out_dir, stem)
        rows = ", ".join(f"{k}={v:,}" for k, v in counts.items())
        print(f"{stem}: {rows}  bad_lines={bad}  ({time.time() - t0:.1f}s)")


if __name__ == "__main__":
    main()