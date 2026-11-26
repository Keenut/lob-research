# lob-research

Research platform for the Binance spot limit order book (BTCUSDT): a raw market-data recorder,
an ETL to Parquet, a Python reference L2 order book validated against exchange snapshots, and
clock-corrected latency measurement.

**Status: in progress.** Data capture, the Python reference book and the clock/latency analysis
are done. The C++ book engine, the microstructure research and the trading simulator are planned.

All numbers below were measured on the 24 UTC hour files `2026-11-24T23` to `2026-11-25T22`
and come from the reports in [`docs/`](docs/). All times are UTC.

| Component | State |
|---|---|
| Recorder, continuity checker, ETL to Parquet, DuckDB queries | done |
| Python reference L2 book, validated against REST snapshots | done |
| Clock offset to the exchange, corrected latency | done |
| C++17 L2 book engine (pybind11), differential tests against the Python reference, benchmark | planned |
| Microstructure research (spread, depth imbalance, order flow imbalance, microprice, short-horizon prediction) | planned |
| Simulator (Avellaneda–Stoikov quoting, signal-based taking, fees, latency and queue models, P&L attribution) | planned |

## Architecture

```
Binance spot, BTCUSDT
  WebSocket: depth@100ms diffs + trades (one combined stream)
  REST: /api/v3/depth?limit=5000 at connect and every 10 min, /api/v3/time every 60 s
        |
   recorder.py                         appends what it receives, never builds a book
        |
   data/raw/BTCUSDT/<UTC hour>.log     "<rx_ns> <session> <kind> <payload>"
        |                    |
 check_continuity.py      etl.py       finished hours only, atomic writes
 (gaps, holes, errors)       |
   data/parquet/BTCUSDT/<table>/<UTC hour>.parquet
        |                    |                       |
   explore.py         replay.py + book.py        clock.py
   (DuckDB queries)   (L2 book, validation)      (clock offset, latency)
```

Design rules:

- **Record raw, derive later.** The recorder stores the exchange JSON unchanged with a local
  receive timestamp. A bug found later in the ETL, the book or an analysis is fixed by replaying the logs.
- **Check every result against an independent source:** update ids for continuity, REST snapshots
  for the book, Binance server time for the clock.
- **Idempotent ETL:** one Parquet file per table per UTC hour, written to a temporary file and then
  renamed; hours already converted are skipped unless `--force`.

## Quick start (Windows PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python recorder.py                                   # terminal 1: records and reconnects by itself
python etl.py                                        # terminal 2: converts every finished UTC hour
python check_continuity.py data\raw\BTCUSDT\*.log    # sequence gaps, holes between sessions, errors
python replay.py                                     # rebuilds the book, validates the top 100 levels
python clock.py                                      # clock offset and corrected latency
python explore.py                                    # DuckDB sanity, latency spike and clock step queries
python -m pytest -q                                  # tests
```

## Data format

### Raw logs: `data/raw/<SYMBOL>/<YYYY-MM-DDTHH>.log` (git-ignored)

One record per line: `<rx_ns> <session> <kind> <payload>`

| Field | Meaning |
|---|---|
| `rx_ns` | local wall-clock receive time, ns since epoch (PC clock, not corrected) |
| `session` | id of the WebSocket connection: unix seconds at connect |
| `kind` | `open`, `ws` (stream message), `snap` (REST snapshot), `clk` (clock sample), `err`, `close` |
| `payload` | exchange JSON, unchanged; `clk`: `{"t0", "t1", "srv"}` with local ns before and after `GET /api/v3/time` |

### Parquet tables: `data/parquet/<SYMBOL>/<table>/<YYYY-MM-DDTHH>.parquet` (git-ignored)

| Table | One row per | Columns |
|---|---|---|
| `depth_events` | depth message | `rx_ns, session, event_ms (E), first_id (U), last_id (u), n_bids, n_asks` |
| `depth_updates` | price level in a depth message | `rx_ns, session, event_ms, last_id, side (0 bid, 1 ask), price, qty` (qty 0 removes the level) |
| `trades` | trade | `rx_ns, session, event_ms, trade_id, price, qty, trade_ms, buyer_is_maker` |
| `snapshots` | level of a REST snapshot | `rx_ns, session, last_update_id, side, price, qty` |
| `clock` | clock sample | `rx_ns, session, offset_ms, rtt_ms, server_ms` |
| `session_log` | `open` / `err` / `close` record | `rx_ns, session, kind, text` |

Prices and quantities are float64 parsed from the exchange's decimal strings; the same string always
gives the same float, so exact comparisons are valid. The C++ engine will use integer ticks.

## L2 book reconstruction (`book.py`, `replay.py`)

A depth diff gives the new total quantity of each changed price level; quantity 0 removes the level.
The data are aggregated per price level (L2): individual orders, queue positions and price-time
priority cannot be reconstructed from them.

Per session (one WebSocket connection; every session starts without a book), following Binance's
"How to manage a local order book correctly":

1. **Sync.** Snapshots are placed by their update id `L`, not by their receive time (offline, all
   snapshots are known in advance). The first event with `u > L` must satisfy `U <= L + 1`; the book
   becomes the snapshot and that event is applied. A snapshot with `L + 1 < U` is older than the
   stream data and unusable.
2. **Apply.** An event with `u <= last_u` is stale and ignored. `U > last_u + 1` is a gap: the book is
   dropped until the next usable snapshot (resync).
3. **Validate.** Every later snapshot is compared with the book in the state of the same update id,
   over the snapshot's top N levels per side; a missing level and an extra level both count. If `L`
   falls strictly inside a 100 ms event, the levels that event touches are ambiguous and excluded.
4. **Classify mismatches.** *Untracked*: the level is missing from the book and lies beyond the sync
   snapshot's depth (below its lowest bid or above its highest ask), where a 5000-level snapshot
   shows nothing until a diff touches the level. *Unexplained*: any other mismatch, which would be a
   bug or bad data and must be 0.

## Clock and latency (`clock.py`)

`rx_ns` comes from the PC clock and `E` from Binance's, so `rx − E` mixes network latency with the
clock error. Each clock sample brackets `GET /api/v3/time` between local times `t0` and `t1`:

- offset estimate (Binance − PC, Cristian's algorithm): `serverTime − (t0 + t1) / 2`, exact only if
  the request and the response take equal time;
- without that assumption: `serverTime − t1 <= offset <= serverTime + 1 − t0` (width = RTT + 1 ms).

Only samples with `RTT <= min RTT + 20 ms` are used. The offset is interpolated linearly between
them; messages more than 10 min from a good sample are not corrected. Corrected latency =
`rx + offset − E`. Its absolute level carries an uncertainty of about ±RTT/2 (path asymmetry cannot be
observed); its changes over time are measured much more precisely.

## Data-quality report

Window: 24 UTC hour files, first depth message 2026-11-24 23:02:12, last 2026-11-25 22:59:59 (23.96 h).

### Volume ([`explore_report.txt`](docs/explore_report.txt))

| Table | Rows |
|---|---|
| depth_events | 853,159 (about 36,000 per full hour: one per 100 ms) |
| depth_updates | 44,064,700 |
| trades | 5,310,816 |
| snapshots | 1,460,000 (146 snapshots × 5000 levels per side) |
| clock | 73 (clock sampling started at 21:46 on 2026-11-25) |
| session_log | 21 |

### Continuity, sessions and holes ([`continuity_report.txt`](docs/continuity_report.txt))

- **0 sequence gaps** in all 7 sessions with data. The Python checker, the SQL `lag()` cross-check
  in `explore.py` and `replay.py` agree.
- 10 sessions, 7 with data (3 connection attempts failed on DNS).
- 6 holes between sessions, 827.0 s in total: **coverage 99.04 %** of the 23.96 h span.
  Two holes are manual restarts (692.2 s, and 53.6 s to deploy clock sampling); the 4 automatic
  reconnects account for 81.2 s.

| Session | Start → end (UTC) | Hours | Depth events | How it ended | Hole before next |
|---|---|---|---|---|---|
| 1795561331 | 11-24 23:02 → 23:07 | 0.08 | 2,957 | manual restart | 692.2 s |
| 1795562322 | 11-24 23:18 → 11-25 01:05 | 1.79 | 63,901 | keepalive ping timeout (close code 1011) | 12.1 s |
| 1795568757 | 01:05 → 07:06 | 6.00 | 215,529 | connection lost without a close frame, then 3 DNS failures | 47.7 s |
| 1795590376 | 07:06 → 14:10 | 7.07 | 254,173 | connection lost without a close frame | 10.1 s |
| 1795615814 | 14:10 → 17:15 | 3.08 | 110,848 | connection lost without a close frame | 11.3 s |
| 1795626911 | 17:15 → 21:45 | 4.51 | 162,123 | manual restart (clock sampling deployed) | 53.6 s |
| 1795643189 | 21:46 → 22:59 | 1.22 | 43,628 | still recording at the end of the window | – |

### Order book validation ([`replay_top100.txt`](docs/replay_top100.txt), [`replay_top1000.txt`](docs/replay_top1000.txt), [`replay_top5000.txt`](docs/replay_top5000.txt), `validation_top*.csv`)

- Every session synced 0.2–0.7 s after its first depth message; 19 events in total arrived before
  sync. 0 stale events, 7 syncs (one per session), no resync needed.
- 139 snapshot checks (146 snapshots minus the 7 used for sync); the book was never crossed at a check.

| Levels per side | Levels compared | Mismatches | Untracked | Unexplained |
|---|---|---|---|---|
| 100 | 27,800 | **0** | 0 | 0 |
| 1000 | 278,000 | 344 (3 checks: 13:46, 13:56, 14:06) | 344 | **0** |
| 5000 | 1,390,000 | 159,037 (138 of 139 checks) | 159,037 | **0** |

- The sync snapshots spanned 2,910–3,410 USD from deepest bid to deepest ask. The top-1000
  mismatches all come from the end of the 7.07 h session, when the price had moved toward the edge
  of the range known since sync.
- In all 139 checks the snapshot's `lastUpdateId` was equal to the last update id `u` of a diff event:
  in this data no snapshot fell inside a 100 ms event, so the ambiguity exclusion (implemented and
  tested) was never used.
- Speed of the Python reference: 44.8k–68.1k events/s over six full replays on one PC, including
  Parquet reading. This is a baseline for the C++ engine, not a benchmark.

### Clock ([`clock_report.txt`](docs/clock_report.txt), [`clock_report_slack40.txt`](docs/clock_report_slack40.txt), [`explore_report.txt`](docs/explore_report.txt))

- 73 clock samples over 1.21 h; RTT min / p50 / p99 = 223 / 249 / 5544 ms; 12 good samples
  (RTT <= 243 ms).
- Offset (Binance − PC) median 216.6 ms: the PC clock was behind. Bounds width about 243 ms.
- **Drift +73.8 ms/h** (linear fit on the good samples; +69.9 ms/h with a 40 ms RTT slack), about
  20 ppm. Between clock steps, the raw `rx − E` session medians fall at about the same rate
  (≈ 75 ms/h, from session medians and session mid-times).
- **Clock steps** (the PC clock set at once):
  - at 19:29 the per-minute median of `rx − E` jumped from −1424 to +112 ms and stayed there (+1536 ms);
  - between 23:07 and 23:18 on 2026-11-24 (during the first manual restart) the session median went
    from −1813 to +36 ms, about +1.85 s.

  Cause not yet verified (a time-service resynchronisation would produce this pattern).
- Consequence: uncorrected `rx − E` is not comparable across hours, and sessions before 21:46 on
  2026-11-25 cannot be corrected (no clock samples; extrapolating the drift across unknown steps is
  not reliable).

### Latency ([`clock_report.txt`](docs/clock_report.txt), [`explore_report.txt`](docs/explore_report.txt))

Corrected one-way latency, last session (21:46–22:59, 100 % covered by clock samples):

| Messages | Corrected p50 | Corrected p99 | p50 bounds without the symmetry assumption |
|---|---|---|---|
| depth events (`rx − E`) | 120 ms | 2,224 ms | [1, 242] ms |
| trades (`rx − T`, trade time) | 124 ms | 601 ms | [5, 245] ms |

- With a 40 ms RTT slack the p50 values are 122 / 126 ms. The p50 is close to RTT/2, as expected
  under the symmetric-path assumption.
- Hourly check: from 21:00 to 22:00 the raw `rx − E` p50 moved from −58 to −99 ms with the clock
  drift; the corrected p50 moved from 124 to 120 ms.
- **Spikes:** in 110 of 1,428 minutes (7.7 %) the max − median of `rx − E` within the minute exceeded
  500 ms (this measure needs no clock correction). Not significantly more frequent in minutes with a
  REST snapshot: 15 of 146 (10.3 %) vs 95 of 1,282 (7.4 %), Fisher's exact test p ≈ 0.25.
- **Stalls:** before the keepalive timeout at 01:05 the minute median of `rx − E` rose by 4.4 s; at
  22:32–22:33 messages arrived up to 20.6 s after the minute median, without a disconnect, and a clock
  request in the same minute took 13.9 s, so the stall was not specific to the WebSocket. Messages were
  delayed, not lost (0 sequence gaps).
- Consequence for research: use the exchange time `E` as the event clock and `rx` only to model latency.

## Limitations

- L2 data only: no queue position or price-time priority (to be modelled in the simulator).
- A snapshot shows at most 5000 levels per side: deeper levels that do not change after sync are
  unknown (the *untracked* mismatches above). Research features will use the top levels.
- After a sequence gap the book would wait for the next snapshot, up to 10 min (no gap occurred here).
- One machine, one connection: every reconnect leaves a hole (81.2 s for 4 reconnects here). A second,
  independent recorder could fill them.
- Clock: the absolute one-way latency is uncertain by about ±RTT/2 (≈ ±120 ms); clock samples exist
  only from 21:46 on 2026-11-25; the PC clock drifts about 20 ppm and was stepped twice in the window.
- `rx_ns` is distorted by stalls of up to about 20 s.
- The recorder writes through a 64 KiB buffer: a crash can lose the last buffered lines (the ETL and
  the checker skip a truncated last line).
- One symbol, one venue, one day: nothing here is yet a statistical result about markets.
- `explore.py`, `replay.py` and `clock.py` read every converted hour: re-running them after converting
  more hours changes the numbers. The reports in `docs/` were produced on the 24 hours above.

## Repository layout

| Path | Role |
|---|---|
| `recorder.py` | asyncio recorder: WebSocket depth + trades, REST snapshots, clock samples, reconnection with backoff |
| `check_continuity.py` | sequence continuity per session, holes between sessions, logged errors, raw latency (reads raw logs) |
| `etl.py` | raw logs → Parquet tables, one file per table per UTC hour |
| `explore.py` | DuckDB queries: row counts, gaps cross-check, trades, errors, latency spikes, clock steps |
| `book.py` | reference L2 book (two dicts) and top-N comparison with a snapshot |
| `replay.py` | per-session sync / gap / resync state machine, snapshot validation, mismatch classification |
| `clock.py` | clock offset from the samples, drift, corrected latency with bounds |
| `tests/` | pytest: book operations and comparison; sync, gap/resync, stale events, validation and mismatch classification; end-to-end raw log → ETL → Parquet → replay on a synthetic market with a known true book; clock offset recovery, path-asymmetry bias, drift, interpolation |
| `docs/` | the reports quoted above |
| `data/` | raw logs and Parquet (git-ignored) |

Reports in `docs/` and the commands that produced them:

| Report | Command |
|---|---|
| `continuity_report.txt` | `python check_continuity.py` on the 24 raw hour files |
| `explore_report.txt` | `python explore.py` |
| `replay_top100.txt` | `python replay.py` |
| `replay_top1000.txt`, `validation_top1000.csv` | `python replay.py --top 1000 --csv docs\validation_top1000.csv` |
| `replay_top5000.txt`, `validation_top5000.csv` | `python replay.py --top 5000 --csv docs\validation_top5000.csv` |
| `clock_report.txt` | `python clock.py` |
| `clock_report_slack40.txt` | `python clock.py --slack-ms 40` |

## References

- Binance, *How to manage a local order book correctly*, Spot API documentation, WebSocket streams:
  https://developers.binance.com/en/docs/products/spot/web-socket-streams
- F. Cristian, "Probabilistic clock synchronization", *Distributed Computing* 3 (1989).
- R. Cont, A. Kukanov, S. Stoikov, "The price impact of order book events", *Journal of Financial
  Econometrics* 12 (2014).
- M. Avellaneda, S. Stoikov, "High-frequency trading in a limit order book", *Quantitative Finance* 8 (2008).
- S. Stoikov, "The micro-price: a high-frequency estimator of future prices", *Quantitative Finance* 18 (2018).