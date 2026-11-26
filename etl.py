r"""etl.py: convert the raw recorder logs into Parquet tables (one file per table per UTC hour).

Usage (PowerShell, from the repo root):
    python etl.py                    # every finished hour in data\raw\BTCUSDT -> data\parquet\BTCUSDT
    python etl.py --include-latest   # also convert the hour the recorder is still writing
    python etl.py --force            # redo hours that already have output
    python etl.py --symbol ETHUSDT

Raw hours are read either as plain text (<hour>.log, what the recorder writes) or gzip-compressed
(<hour>.log.gz, made by compress_raw.py); the content is the same.

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

A line that cannot be parsed (truncated, invalid JSON, a missing field) is counted in bad_lines and
skipped as a whole: every record is parsed completely before anything is appended to the columns,
so the columns of a table always stay the same length.

Prices and quantities are stored as float64 for research. The C++ order book converts them to
integer ticks and lots at load time.
"""
import argparse
import gzip
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


def hour_of(path):
    """'2026-11-25T22.log' and '2026-11-25T22.log.gz' both belong to hour '2026-11-25T22'."""
    return path.name.split(".")[0]


def open_raw(path):
    """Open a raw hour for reading as text, whether it is plain (.log) or gzip-compressed (.log.gz)."""
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, encoding="utf-8")


def raw_files(raw_dir):
    """Raw hour files sorted by hour. Raises if an hour exists both plain and compressed."""
    files = sorted(list(raw_dir.glob("*.log")) + list(raw_dir.glob("*.log.gz")), key=hour_of)
    hours = [hour_of(f) for f in files]
    dupes = sorted({h for h in hours if hours.count(h) > 1})
    if dupes:
        raise SystemExit(f"hours present both as .log and .log.gz: {dupes} (run compress_raw.py again)")
    return files


def parse_log(path):
    """Parse one raw log file into column lists. Returns (tables, number_of_bad_lines)."""
    t = {name: {col: [] for col, _ in cols} for name, cols in SCHEMAS.items()}
    ev, up, tr, sn, ck, sl = (t[k] for k in ("depth_events", "depth_updates", "trades",
                                            "snapshots", "clock", "session_log"))
    bad = 0
    with open_raw(path) as f:
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
                # 1. parse everything this record needs (any error skips the whole record) ...
                if kind == "ws":
                    msg = json.loads(payload)
                    d, stream = msg["data"], msg["stream"]
                    if stream.endswith("@depth@100ms"):
                        levels = [(side, float(p), float(q)) for side, key in ((0, "b"), (1, "a"))
                                  for p, q in d[key]]
                        row = (rx_ns, sid, int(d["E"]), int(d["U"]), int(d["u"]), len(d["b"]), len(d["a"]))
                    elif stream.endswith("@trade"):
                        row = (rx_ns, sid, int(d["E"]), int(d["t"]), float(d["p"]), float(d["q"]),
                               int(d["T"]), bool(d["m"]))
                    else:
                        continue      # a stream we do not convert
                elif kind == "snap":
                    obj = json.loads(payload)
                    last_update_id = int(obj["lastUpdateId"])
                    levels = [(side, float(p), float(q)) for side, key in ((0, "bids"), (1, "asks"))
                              for p, q in obj[key]]
                elif kind == "clk":
                    obj = json.loads(payload)
                    t0, t1, srv = obj["t0"], obj["t1"], int(obj["srv"]["serverTime"])
                    row = (rx_ns, sid, srv - (t0 + t1) / 2e6, (t1 - t0) / 1e6, srv)
                elif kind in ("open", "err", "close"):
                    row = (rx_ns, sid, kind, payload)
                else:
                    bad += 1          # unknown kind
                    continue
            except (json.JSONDecodeError, KeyError, ValueError, TypeError):
                bad += 1
                continue
            # 2. ... then append (nothing below can fail on bad data)
            if kind == "ws" and stream.endswith("@depth@100ms"):
                for col, val in zip(("rx_ns", "session", "event_ms", "first_id", "last_id", "n_bids", "n_asks"), row):
                    ev[col].append(val)
                n = len(levels)
                up["rx_ns"].extend([rx_ns] * n)
                up["session"].extend([sid] * n)
                up["event_ms"].extend([row[2]] * n)
                up["last_id"].extend([row[4]] * n)
                for side, p, q in levels:
                    up["side"].append(side)
                    up["price"].append(p)
                    up["qty"].append(q)
            elif kind == "ws":
                for col, val in zip(("rx_ns", "session", "event_ms", "trade_id", "price", "qty", "trade_ms",
                                     "buyer_is_maker"), row):
                    tr[col].append(val)
            elif kind == "snap":
                for side, p, q in levels:
                    for col, val in zip(("rx_ns", "session", "last_update_id", "side", "price", "qty"),
                                        (rx_ns, sid, last_update_id, side, p, q)):
                        sn[col].append(val)
            elif kind == "clk":
                for col, val in zip(("rx_ns", "session", "offset_ms", "rtt_ms", "server_ms"), row):
                    ck[col].append(val)
            else:
                for col, val in zip(("rx_ns", "session", "kind", "text"), row):
                    sl[col].append(val)
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
    files = raw_files(raw_dir)
    if files and not a.include_latest:
        files = files[:-1]        # the newest hour is probably still being written by the recorder
    if not files:
        print(f"no log files to convert in {raw_dir}")
        return
    for f in files:
        stem = hour_of(f)
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