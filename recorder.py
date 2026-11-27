"""Binance spot raw recorder: depth diffs + trades + periodic REST snapshots + clock samples.

Design rule: RECORD RAW, DERIVE LATER. This script never builds an order book.
It only appends what it received, byte-for-byte, with a local receive timestamp.
Book reconstruction, validation and ETL happen offline on these logs, so any bug
found later can be fixed and the data replayed.

Log line format (one record per line):
    <rx_ns> <session> <kind> <payload>
    rx_ns   : local wall-clock receive time, nanoseconds since epoch
    session : id of the WebSocket connection (unix seconds at connect)
    kind    : open | ws | snap | clk | err | close
    payload : exchange JSON, untouched (empty for 'close')

'clk' records sample Binance's clock so this PC's clock error can be estimated offline:
    {"t0": <local ns before request>, "t1": <local ns after response>, "srv": {"serverTime": <ms>}}
    offset_ms = serverTime - (t0 + t1) / 2e6   (positive: this PC's clock is behind Binance)
    rtt_ms    = (t1 - t0) / 1e6
"""
import argparse
import asyncio
import contextlib
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import websockets

WS_URL = "wss://stream.binance.com:9443/stream?streams={s}@depth@100ms/{s}@trade"
REST_URL = "https://api.binance.com/api/v3/depth"
TIME_URL = "https://api.binance.com/api/v3/time"
SNAPSHOT_LIMIT = 5000  # max levels per side the REST endpoint returns
SNAPSHOT_RETRY_FIRST = 5.0   # seconds before retrying a failed snapshot request; doubles after each failure
SNAPSHOT_RETRY_MAX = 60.0    # ... up to this


class RotatingLog:
    """Append-only text log, one file per UTC hour: <root>/<SYMBOL>/<YYYY-MM-DDTHH>.log"""

    def __init__(self, root: Path, symbol: str):
        self.dir = root / symbol
        self.dir.mkdir(parents=True, exist_ok=True)
        self.hour = None
        self.f = None

    def write(self, session: int, kind: str, payload: str) -> None:
        rx_ns = time.time_ns()
        hour = datetime.fromtimestamp(rx_ns / 1e9, timezone.utc).strftime("%Y-%m-%dT%H")
        if hour != self.hour:
            self.close()
            self.f = open(self.dir / f"{hour}.log", "a", buffering=1 << 16, encoding="utf-8")
            self.hour = hour
        self.f.write(f"{rx_ns} {session} {kind} {payload}\n")

    def flush(self) -> None:
        if self.f:
            self.f.flush()

    def close(self) -> None:
        if self.f:
            self.f.close()
            self.f = None


def retry_after(headers, default):
    """Seconds asked for by a Retry-After header (Binance sends one with 429 and 418), else `default`."""
    try:
        return max(float(headers.get("Retry-After", "")), 1.0)
    except ValueError:
        return default


async def snapshot_loop(http, log, symbol, session, interval):
    """Fetch a REST depth snapshot now, then every `interval` seconds.

    A failed request is retried sooner: after 5 s, then 10, 20, 40 and at most 60 s, until one succeeds.
    A session cannot build a book before its first snapshot, so waiting the full interval after a
    timeout at connect would cost up to 10 minutes of data. The exceptions are HTTP 429 (request rate
    limit exceeded), 418 (IP banned for not backing off after 429s) and 403 (blocked by Binance's web
    application firewall): then wait as long as the Retry-After header asks, or the full interval if it
    is missing, because Binance extends the bans of clients that keep sending."""
    retry = SNAPSHOT_RETRY_FIRST
    while True:
        wait = None   # set when the request succeeded or was rate-limited
        try:
            params = {"symbol": symbol, "limit": SNAPSHOT_LIMIT}
            async with http.get(REST_URL, params=params) as r:
                body = await r.text()
                if r.status == 200:
                    log.write(session, "snap", body)
                    wait = interval
                    retry = SNAPSHOT_RETRY_FIRST
                else:
                    log.write(session, "err", f"snapshot http {r.status} {body[:200]}")
                    if r.status in (403, 418, 429):
                        wait = retry_after(r.headers, interval)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.write(session, "err", f"snapshot {e!r}")
        log.flush()
        if wait is None:   # network error, timeout or another HTTP error: retry sooner
            wait = retry
            retry = min(retry * 2, SNAPSHOT_RETRY_MAX)
        await asyncio.sleep(wait)


async def clock_loop(http, log, session, interval):
    """Sample Binance's clock now, then every `interval` seconds (see 'clk' in the module docstring)."""
    while True:
        try:
            t0 = time.time_ns()
            async with http.get(TIME_URL) as r:
                body = await r.text()
                t1 = time.time_ns()
            if r.status == 200:
                log.write(session, "clk", f'{{"t0":{t0},"t1":{t1},"srv":{body}}}')
            else:
                log.write(session, "err", f"clock http {r.status} {body[:200]}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.write(session, "err", f"clock {e!r}")
        await asyncio.sleep(interval)


async def run_session(http, log, symbol, session, snap_interval, clock_interval):
    url = WS_URL.format(s=symbol.lower())  # stream names must be lowercase
    async with websockets.connect(url, max_size=None) as ws:
        log.write(session, "open", url)
        # Snapshot is requested AFTER the socket is open, so every event that
        # arrives while the request is in flight is already in the log before it.
        tasks = [
            asyncio.create_task(snapshot_loop(http, log, symbol, session, snap_interval)),
            asyncio.create_task(clock_loop(http, log, session, clock_interval)),
        ]
        try:
            async for msg in ws:
                log.write(session, "ws", msg)
        finally:
            for t in tasks:
                t.cancel()
            for t in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await t


async def main(symbol: str, out: Path, snap_interval: float, clock_interval: float) -> None:
    log = RotatingLog(out, symbol)
    backoff = 1
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as http:
            while True:  # connections drop (24h limit, network blips), so reconnect forever
                session = int(time.time())
                started = time.monotonic()
                try:
                    await run_session(http, log, symbol, session, snap_interval, clock_interval)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # network errors, resets, etc.
                    log.write(session, "err", repr(e))
                log.write(session, "close", "")
                log.flush()
                lived = time.monotonic() - started
                backoff = 1 if lived > 60 else min(backoff * 2, 60)
                await asyncio.sleep(backoff)
    finally:
        log.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--out", type=Path, default=Path("data/raw"))
    ap.add_argument("--snap-interval", type=float, default=600.0, help="seconds between REST snapshots")
    ap.add_argument("--clock-interval", type=float, default=60.0, help="seconds between Binance clock samples")
    args = ap.parse_args()
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main(args.symbol.upper(), args.out, args.snap_interval, args.clock_interval))