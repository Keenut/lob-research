# lob-research

Research platform for the Binance spot limit order book (BTCUSDT): a raw market-data recorder,
an ETL to Parquet, a Python reference L2 order book validated against exchange snapshots, a C++
book and replay engine checked against that reference, and clock-corrected latency measurement.

**Status: in progress.** Data capture, the Python reference book, the C++ engine and the
clock/latency analysis are done. The microstructure research and the trading simulator are planned.

All numbers below were measured on the 24 UTC hour files `2026-11-24T23` to `2026-11-25T22`
and come from the reports in [`docs/`](docs/). All times are UTC.

| Component | State |
|---|---|
| Recorder, continuity checker, ETL to Parquet, DuckDB queries | done |
| Python reference L2 book, validated against REST snapshots | done |
| Clock offset to the exchange, corrected latency | done |
| C++17 L2 book and replay engine (pybind11): sorted vector, `std::map` and a price ladder designed from a profile of the update stream; differential tests against the Python reference; benchmark | done |
| Microstructure research (spread, depth imbalance, order flow imbalance, microprice, short-horizon prediction) | planned |
| Simulator (Avellaneda–Stoikov quoting, signal-based taking, fees, latency and queue models, P&L attribution) | planned |

## Architecture

```
Binance spot, BTCUSDT
  WebSocket: depth@100ms diffs + trades (one combined stream)
  REST: /api/v3/depth?limit=5000 at connect and every 10 min (a failed request is retried after
        5 s, doubling up to 60 s), /api/v3/time every 60 s
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
                             |
                      lobcpp: C++ book + replay (cpp/), same results (crosscheck.py)
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
python check_continuity.py data\raw\BTCUSDT\*.log*   # sequence gaps, holes between sessions, errors
python compress_raw.py                               # gzip finished raw hours (lossless, verified)
python replay.py                                     # rebuilds the book, validates the top 100 levels
python clock.py                                      # clock offset and corrected latency
python explore.py                                    # DuckDB sanity, latency spike and clock step queries
python -m pytest -q                                  # tests

# C++ engine: Visual Studio 2022 with "Desktop development with C++", venv active
$py = (Get-Command python).Source -replace '\\', '/'
$pb = (python -c "import pybind11; print(pybind11.get_cmake_dir())") -replace '\\', '/'
cmake -S cpp -B build -G "Visual Studio 17 2022" -A x64 -DPython_EXECUTABLE="$py" -Dpybind11_DIR="$pb"
cmake --build build --config Release
.\build\Release\lob_tests.exe                        # C++ unit tests
python replay.py --engine cpp-ladder                 # same report as replay.py, C++ engine (price ladder)
python crosscheck.py                                 # Python and C++ engines must agree exactly
python bench.py                                      # speed (data already in memory) and update profile
```

## Data format

### Raw logs: `data/raw/<SYMBOL>/<YYYY-MM-DDTHH>.log` (git-ignored)

One record per line: `<rx_ns> <session> <kind> <payload>`. Finished hours can be gzip-compressed to
`<YYYY-MM-DDTHH>.log.gz` by `compress_raw.py` (lossless: the decompressed bytes are checked against
the original with SHA-256 before the `.log` is deleted); `etl.py` and `check_continuity.py` read both.

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
gives the same float, so exact comparisons are valid. The C++ engine uses integer ticks and lots.

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

## C++ engine (`cpp/`, Python module `lobcpp`)

- `book.hpp`: the same L2 book semantics as `book.py`, with prices as integer ticks (0.01 USDT) and
  quantities as integer lots (1e-8 BTC). Each side is one of three interchangeable containers with the
  same interface: a sorted `std::vector`, a `std::map`, or a price ladder (below, the default).
- `replay.hpp`: the state machine of `replay.py` (sync, gaps, stale events, snapshot checks,
  untracked / unexplained mismatches), generic over the book container, processing one hour per call
  and giving the top of book after every event.
- `bindings.cpp`: the pybind11 module. Events and levels travel as NumPy arrays in CSR layout
  (event *i* owns levels `offsets[i]` to `offsets[i+1]`), so an hour goes to C++ in one call without
  copying.
- `bookdata.py`: Parquet → integer arrays. A price that is not a whole number of ticks raises an error
  instead of being rounded. The conversion is exact both ways: `ticks / 100` is the same double as the
  exchange's decimal string, so the two engines' results can be compared with `==`.

### Price ladder (`LadderSide` in `book.hpp`)

Designed from a profile of the real update stream (`bench.py` sections 4–6 in
[`benchmark.txt`](docs/benchmark.txt)), not assumed:

- 70.5 % of the applied level updates insert or erase a price level, with on average 275 better levels
  above it: expensive for a sorted vector (see "Why the vector loses" below).
- Counted in ticks from the best price of the same side, just before each update: 63.78 % of the
  updates land within 20.47 USDT of it, 89.05 % within 163.83 USDT, 94.66 % within 655.35 USDT. 0.77 %
  set a new best price, all within 163.83 USDT of the old one. A far cluster of 2.95 % lands 5,243 to
  20,972 USDT behind the best price.
- When an update erases the best level, the next level is 1 tick away in 27.17 % of the cases and
  within 255 ticks in 99.29 %.

The design that follows from this:

- **Array window.** Each side has an array of W = 2^N quantities, one slot per tick, over a window of
  prices around the best price: slot *i* holds the level whose key is `base + i` (quantity 0: no
  level). An update inside the window is one array access: no search, no allocation, no shifting.
- **Keys.** Bid key = price, ask key = −price: on both sides a larger key is a better price, so one
  implementation serves both.
- **Deep levels.** Levels below the window, the far cluster among them, live in a `std::map`.
- **Best level.** The highest occupied slot. When it is erased, the array is scanned downwards: the
  next level is close, so a bitmap of the occupied slots is not needed.
- **Window moves.** When an insert lands above the window, or erases leave the best level below the
  middle of the window, the window moves so that the best level lands on slot 3W/4 − 1. A move costs
  O(W) and is rare: the best price has to move by about W/4 first.
- **Invariants**, checked by `valid()` after the updates in the tests: levels at or above `base` are in
  the array, deeper ones in the map, none above the window; the level count and the highest occupied
  slot are exact; a side that is not empty has its best level in the upper half of the window. So the
  window always covers at least W/2 ticks behind the best price.

**Window size.** The last invariant turns the distance profile into an upper bound on the share of the
updates that can reach the `std::map`: those more than W/2 ticks behind the best price. The measured
share stays under it for every window (`bench.py` section 1):

| Window | Width | Array per side | Updates via the `std::map`: bound / measured | Window moves in 24 h |
|---|---|---|---|---|
| 2^12 ticks | 40.96 USDT | 32 KiB | ≤ 35.5 % / 28.32 % | 36,281 |
| 2^14 ticks | 163.84 USDT | 128 KiB | ≤ 15.7 % / 12.68 % | 4,517 |
| 2^16 ticks | 655.36 USDT | 512 KiB | ≤ 5.8 % / 4.83 % | 361 |
| **2^18 ticks (default)** | 2,621.44 USDT | 2 MiB | ≤ 3.5 % / 3.37 % | 19 |

2^16 is already past the knee of the distribution and was the first choice. The per-event timings
(below) changed it to 2^18. After a sync the window reaches 3W/4 behind the best price: 491.5 USDT for
2^16 but 1,966 USDT for 2^18, while the sync snapshots spanned 2,910–3,410 USDT from the deepest bid to
the deepest ask. With 2^16 every deeper level of the snapshot is a `std::map` insert, and the session
sync is the costliest kind of event: median 377.2 µs, against 185.3 µs with 2^18. 2^18 also moves the
window 19 times in 24 h instead of 361. The price is memory: 2 MiB per side instead of 512 KiB. Of its
remaining 3.37 % of updates via the map, 2.95 points are the far cluster, which no reasonable window
covers.

**Testing.**

- `lob_tests` (C++) runs every container against a deliberately naive book (unsorted vector, linear
  search) on 200,000 random updates per seed, the ladder also with windows of 4 and 8 slots, where the
  window moves at almost every update and the map is used all the time. A random walk with price jumps
  of up to 3/4 of a window moves the ladder's window thousands of times in both directions, with the
  invariants checked after every update (every 64th update for windows above 1024 slots), plus
  hand-computed edge cases on 8 slots.
- pytest compares the C++ books and replay engines, an 8-slot ladder among them, with the Python
  reference after every event, on random multi-session streams and through Parquet.
- Mutation testing by hand, in a Linux build: bugs deliberately injected into the ladder (off-by-ones
  in the window bounds and in the moves, missing count updates, a wrong scan bound, ask keys not
  negated, an empty side not re-anchored) were all caught by `lob_tests`. AddressSanitizer and
  UndefinedBehaviorSanitizer runs of `lob_tests` and of pytest were clean.
- On real data: the same results as the Python reference with both windows (next section).

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
- Speed: see "C++ engine: agreement and speed" below.

### Clock ([`clock_report.txt`](docs/clock_report.txt), [`clock_report_slack40.txt`](docs/clock_report_slack40.txt), [`explore_report.txt`](docs/explore_report.txt))

- 73 clock samples over 1.21 h; RTT min / p50 / p99 = 223 / 249 / 5544 ms; 12 good samples
  (RTT <= 243 ms).
- Offset (Binance − PC) median 216.6 ms: the PC clock was behind. Bounds width about 243 ms.
- **Clock steps**, confirmed by the Windows event log ([`windows_time_events.txt`](docs/windows_time_events.txt):
  System log, Kernel-General event 1; both changes were made by `svchost.exe`, the process that hosts
  the Windows Time service):

  | Time (UTC) | Step in the event log | Estimate from the market data alone |
  |---|---|---|
  | 2026-11-24 23:12:18 | +1,936 ms | +1,933 ms: session medians of `rx − E` −1813 → +36 ms across the first manual restart, plus 1.13 h of drift at ≈ 75 ms/h |
  | 2026-11-25 19:29:25 | +1,539 ms | +1,536 ms: per-minute median of `rx − E` −1424 → +112 ms, and it stays there |

- **Drift: three estimates agree.** +73.8 ms/h from a linear fit on the good clock samples (+69.9 ms/h
  with a 40 ms RTT slack); ≈ 75 ms/h from the session medians of `rx − E` between the steps (session
  mid-times); 75.9 ms/h from the event log (1,539 ms accumulated in the 20.29 h between the two syncs,
  assuming the first one set the clock exactly). About 20 ppm.
- **Offset cross-check.** 75.9 ms/h over the 3.0 h from the 19:29 sync to 22:30 predicts an offset of
  228 ms; the clock samples give 223–224 ms (median offset over 22:00–23:00). The agreement within about
  5 ms suggests that the path asymmetry to Binance is small, but it rests on the accuracy of Windows'
  own time sync, which is not measured here.
- The same log has no sleep/resume or boot event during the window: the last time Windows restored the
  clock from the hardware clock was 2026-11-24 22:43:54, before recording started.
- Consequence: uncorrected `rx − E` is not comparable across hours. Sessions before 21:46 on
  2026-11-25 have no clock samples. The event-log steps and the 75.9 ms/h drift would allow a
  piecewise-linear correction of them; it is not done here because it would rest entirely on the
  accuracy of Windows' own time sync.

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

## C++ engine: agreement and speed

### Agreement with the Python reference ([`crosscheck_top1000.txt`](docs/crosscheck_top1000.txt))

On the 24 hours (853,159 events), the Python reference and four C++ engines (vector book, map book, and
the price ladder with windows of 2^16 and 2^18 ticks) give identical results: every session result and
all 139 snapshot checks with their complete mismatch lists, at 1000 levels per side (278,000 levels
compared, 344 mismatches, all untracked). The status and the top of book (best bid and ask with their
quantities) after each of the 853,159 events are identical across the C++ engines, and equal to the
Python reference at the 8,531 applied events sampled for it.

### Speed ([`benchmark.txt`](docs/benchmark.txt))

Intel Core Ultra 5 125H laptop, Windows 11, MSVC 19.44 release build, Python 3.14; plugged in, Windows
power mode "Best performance", the recorder running in the background. Data already in memory (Parquet
reading not timed); median of 3 runs after a warm-up run. The per-event times come from a separate run
with a clock around each event (resolution 100 ns).

All 24 hours (853,159 events, 44,064,700 level updates):

| Engine | Time | Level updates/s | ns per update | Per event p50 / p99 / p99.9 |
|---|---|---|---|---|
| C++, sorted `std::vector` book | 13.834 s | 3.19 M | 313.9 | 4.70 / 131.40 / 275.70 µs |
| C++, `std::map` book | 4.551 s | 9.68 M | 103.3 | 2.50 / 38.40 / 69.90 µs |
| C++, price ladder, 2^12 ticks | 1.924 s | 22.90 M | 43.7 | 0.90 / 20.60 / 44.60 µs |
| C++, price ladder, 2^14 ticks | 1.056 s | 41.73 M | 24.0 | 0.60 / 10.20 / 25.80 µs |
| C++, price ladder, 2^16 ticks | 0.638 s | 69.08 M | 14.5 | 0.40 / 4.50 / 11.70 µs |
| **C++, price ladder, 2^18 ticks (default)** | 0.560 s | 78.69 M | 12.7 | 0.30 / 4.10 / 9.00 µs |

The default ladder is 8.1× faster than the map book and 24.7× faster than the vector book, with a
per-event p99 of 4.10 µs instead of 38.40 and 131.40 µs.

**Where the slowest events come from** (`bench.py` section 3). Each timed event is counted in the first
of these classes that applies: a session sync (the book loaded from a REST snapshot), a snapshot check
(100 levels per side), an event in which a ladder window moved, or none of these. Times in µs:

| Engine | Sync, 7 events: median / max | Check, 139 events: median | Window moved: events, median / max | Other events: p99.9 / max |
|---|---|---|---|---|
| C++, sorted `std::vector` book | 125.9 / 213.9 | 9.6 | – | 275.70 / 2771.2 |
| C++, `std::map` book | 629.6 / 952.5 | 10.1 | – | 69.80 / 323.0 |
| C++, price ladder, 2^16 ticks | 377.2 / 533.5 | 6.4 | 347, 40.6 / 158.8 | 9.70 / 298.6 |
| C++, price ladder, 2^18 ticks | 185.3 / 226.8 | 6.7 | 13, 202.6 / 323.7 | 8.80 / 137.6 |

- The session sync, once per connection, loads 5000 levels per side from the snapshot. The map book
  pays a tree insert for every level; the 2^16 ladder for every level more than 491.5 USDT behind the
  best price, while the 2^18 ladder puts most of the snapshot in its array.
- A window move costs O(W): a 2^18 move costs more than a 2^16 move, but happens in 13 events instead
  of 347.
- The maxima of the other events, 138–329 µs for the map and the ladders, fall on ordinary events at
  different times in each engine, one of them with only 18 level updates: most likely the operating
  system interrupting the thread, not the data structure. The vector is the exception: its slowest
  event, 400 level updates at 2026-11-25 13:46:04, was its slowest in each of the three runs of this
  benchmark, so its 2,771 µs is a real cost of the vector's shifts, not noise.

**Why the vector loses.** "A sorted vector beats a tree" holds when changes happen near the end of the
vector. Profiling the 44,064,232 applied level updates shows that here they do not: 35.3 % insert a
new price level and 35.2 % erase one (18.3 % change a quantity, 11.3 % remove a level that is not in
the book), and an insert or erase has on average 275 better levels above it (46.7 % between 10 and 99,
34.3 % between 100 and 999). In a vector ordered with the best price last, each of them shifts that
many 16-byte elements, about 4.3 KiB, in a book of about 7,100 bid and 6,300 ask levels. The tree pays
a few pointer hops instead.

**Against the Python reference** (first 2 hours, 63,392 events): Python 0.265 s, C++ map 0.198 s (1.3×
faster), C++ vector 0.677 s (0.4×), C++ price ladder 2^18 0.026 s (10.3× faster). This is not a
like-for-like language comparison: the Python reference keeps each side in an unordered dict (O(1) per
update) and never computes the top of book per event, while the C++ engines keep the levels in price
order and output the top of book after every event. The ladder keeps that order almost for free.

## Limitations

- L2 data only: no queue position or price-time priority (to be modelled in the simulator).
- A snapshot shows at most 5000 levels per side: deeper levels that do not change after sync are
  unknown (the *untracked* mismatches above). Research features will use the top levels.
- After a sequence gap the book would wait for the next snapshot, up to 10 min (no gap occurred here).
- One machine, one connection: every reconnect leaves a hole (81.2 s for 4 reconnects here). A second,
  independent recorder could fill them.
- Clock: the absolute one-way latency is uncertain by about ±RTT/2 (≈ ±120 ms) from the clock samples
  alone; clock samples exist only from 21:46 on 2026-11-25; the PC clock drifts about 20 ppm and was
  stepped twice in the window. `clock.py` interpolates linearly between good samples, so a future step
  would be smeared over the interval between the two samples around it (minutes); such steps can be
  located with the event log or the per-minute medians in `explore.py`.
- `rx_ns` is distorted by stalls of up to about 20 s.
- The recorder writes through a 64 KiB buffer: a crash can lose the last buffered lines (the ETL and
  the checker skip a truncated last line).
- One symbol, one venue, one day: nothing here is yet a statistical result about markets.
- Speed was measured on one laptop with the recorder running; per-event timings have the 100 ns
  resolution of the Windows clock. The timings depend on the machine's state: in a power-saving state
  every engine, the Python reference included, ran markedly slower, and the process was not pinned to a
  core type on this CPU, which has performance and efficiency cores. The run above (plugged in, power
  mode "Best performance") agrees within 1 % with the first benchmark (commit 897586a) for the vector
  and map books.
- Price ladder: a window move and a sync cost O(W) (the array is moved or cleared, 2 MiB per side);
  the window size is a compile-time constant chosen for BTCUSDT's tick and price; deep levels stay in a
  `std::map`; `probe()` (profiling only) and listing every level are O(W).
- `explore.py` and `clock.py` read every converted hour: re-running them after converting more hours
  changes the numbers (`replay.py`, `crosscheck.py` and `bench.py` take `--max-files 24`). The reports in
  `docs/` were produced on the 24 hours above.

## Repository layout

| Path | Role |
|---|---|
| `recorder.py` | asyncio recorder: WebSocket depth + trades, REST snapshots (a failed one is retried within seconds, rate limits respected), clock samples, reconnection with backoff |
| `check_continuity.py` | sequence continuity per session, holes between sessions, logged errors, raw latency (reads raw logs) |
| `etl.py` | raw logs → Parquet tables, one file per table per UTC hour |
| `compress_raw.py` | gzip finished raw hours, verified, to save disk space |
| `explore.py` | DuckDB queries: row counts, gaps cross-check, trades, errors, latency spikes, clock steps |
| `book.py` | reference L2 book (two dicts) and top-N comparison with a snapshot |
| `replay.py` | per-session sync / gap / resync state machine, snapshot validation, mismatch classification |
| `clock.py` | clock offset from the samples, drift, corrected latency with bounds |
| `cpp/include/lob/book.hpp` | C++ L2 book: integer ticks and lots; sorted vector, `std::map` or price-ladder sides |
| `cpp/include/lob/replay.hpp` | C++ version of the replay state machine, optional timing and depth profiling |
| `cpp/src/bindings.cpp` | pybind11 module `lobcpp` |
| `cpp/tests/test_book.cpp` | C++ unit tests: every container against a deliberately naive book; the ladder's window moves and invariants, with tiny windows |
| `cpp/CMakeLists.txt` | build of the tests and of the module |
| `bookdata.py` | Parquet → integer NumPy arrays for the C++ engine |
| `crosscheck.py` | runs the Python reference and the C++ engines (vector, map, price ladder 2^16 and 2^18) on the same data and checks that they agree |
| `bench.py` | speed of the engines on in-memory data; per-event times split by sync, snapshot check and window move; profile of where the updates land, in levels and in ticks |
| `tests/` | pytest: the recorder's snapshot retry policy (fake HTTP session); book operations and comparison; sync, gap/resync, stale events, validation and mismatch classification; end-to-end raw log → ETL → Parquet → replay on a synthetic market with a known true book; clock offset recovery, path-asymmetry bias, drift, interpolation; C++ books and replay against the Python reference on scenarios, random multi-session streams and Parquet |
| `docs/` | the reports quoted above |
| `data/` | raw logs and Parquet (git-ignored) |

Reports in `docs/` and the commands that produced them:

| Report | Command |
|---|---|
| `continuity_report.txt` | `python check_continuity.py` on the 24 raw hour files |
| `explore_report.txt` | `python explore.py` |
| `replay_top100.txt` | `python replay.py --max-files 24` |
| `replay_top1000.txt`, `validation_top1000.csv` | `python replay.py --max-files 24 --top 1000 --csv docs\validation_top1000.csv` |
| `replay_top5000.txt`, `validation_top5000.csv` | `python replay.py --max-files 24 --top 5000 --csv docs\validation_top5000.csv` |
| `clock_report.txt` | `python clock.py` |
| `clock_report_slack40.txt` | `python clock.py --slack-ms 40` |
| `crosscheck_top1000.txt` | `python crosscheck.py --max-files 24 --top 1000` |
| `benchmark.txt` | `python bench.py --max-files 24` (plugged in, power mode "Best performance") |
| `windows_time_events.txt` | `Get-WinEvent` on the System log, Kernel-General event 1 (times in the `TimeCreated` lines are local, UTC+1; message language: Turkish) |

## References

- Binance, *How to manage a local order book correctly*, Spot API documentation, WebSocket streams:
  https://developers.binance.com/en/docs/products/spot/web-socket-streams
- F. Cristian, "Probabilistic clock synchronization", *Distributed Computing* 3 (1989).
- R. Cont, A. Kukanov, S. Stoikov, "The price impact of order book events", *Journal of Financial
  Econometrics* 12 (2014).
- M. Avellaneda, S. Stoikov, "High-frequency trading in a limit order book", *Quantitative Finance* 8 (2008).
- S. Stoikov, "The micro-price: a high-frequency estimator of future prices", *Quantitative Finance* 18 (2018).