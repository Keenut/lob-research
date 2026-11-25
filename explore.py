r"""explore.py: sanity queries on the Parquet tables with DuckDB.

Usage (PowerShell, from the repo root, after python etl.py):
    python explore.py [SYMBOL]

Latency spikes are measured as (max - median) of rx - E within one UTC minute. The clock offset
is the same for every message of a minute, so this jitter measure needs no clock correction.
Clock steps (the PC clock being reset) show up the other way round: the per-minute MEDIAN of rx - E
jumps and stays at the new level, while a network stall makes it jump and come back.
"""
import sys

import duckdb

symbol = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
base = f"data/parquet/{symbol}"
con = duckdb.connect()
for table in ("depth_events", "depth_updates", "trades", "snapshots", "clock", "session_log"):
    con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{base}/{table}/*.parquet')")


def show(title, sql):
    print(f"\n== {title}")
    print(con.execute(sql).fetchdf().to_string(index=False))


show("rows per table", """
    SELECT 'depth_events' AS tbl, count(*) AS n FROM depth_events
    UNION ALL SELECT 'depth_updates', count(*) FROM depth_updates
    UNION ALL SELECT 'trades', count(*) FROM trades
    UNION ALL SELECT 'snapshots', count(*) FROM snapshots
    UNION ALL SELECT 'clock', count(*) FROM clock
    UNION ALL SELECT 'session_log', count(*) FROM session_log""")

show("depth events per UTC hour", """
    SELECT date_trunc('hour', make_timestamp(rx_ns // 1000)) AS hour_utc,
           count(*) AS events, round(avg(n_bids + n_asks), 1) AS levels_per_event
    FROM depth_events GROUP BY 1 ORDER BY 1""")

show("sequence gaps per session (SQL cross-check of check_continuity.py)", """
    WITH x AS (
        SELECT session, first_id, last_id,
               lag(last_id) OVER (PARTITION BY session ORDER BY rx_ns) AS prev_last
        FROM depth_events)
    SELECT session, count(*) AS events,
           count(*) FILTER (WHERE first_id > prev_last + 1) AS gaps
    FROM x GROUP BY session ORDER BY session""")

show("trades per UTC hour", """
    SELECT date_trunc('hour', make_timestamp(rx_ns // 1000)) AS hour_utc,
           count(*) AS trades, round(sum(qty), 2) AS base_volume,
           round(sum(price * qty), 0) AS quote_volume,
           round(avg(buyer_is_maker::INT), 3) AS share_buyer_is_maker
    FROM trades GROUP BY 1 ORDER BY 1""")

show("clock offset per session (samples with RTT <= 1.5 x the session's minimum RTT)", """
    WITH s AS (SELECT session, min(rtt_ms) AS min_rtt FROM clock GROUP BY session)
    SELECT c.session, count(*) AS good_samples, round(min(s.min_rtt), 0) AS min_rtt_ms,
           round(median(c.offset_ms), 0) AS median_offset_ms
    FROM clock c JOIN s ON c.session = s.session
    WHERE c.rtt_ms <= 1.5 * s.min_rtt GROUP BY c.session ORDER BY c.session""")

show("logged errors", """
    SELECT make_timestamp(rx_ns // 1000) AS t_utc, session, left(text, 90) AS text
    FROM session_log WHERE kind = 'err' ORDER BY rx_ns""")

SPIKES = """
    WITH d AS (
        SELECT date_trunc('minute', make_timestamp(rx_ns // 1000)) AS minute_utc,
               rx_ns / 1e6 - event_ms AS raw_ms
        FROM depth_events),
    m AS (
        SELECT minute_utc, count(*) AS events, median(raw_ms) AS p50, max(raw_ms) AS mx
        FROM d GROUP BY 1),
    s AS (SELECT DISTINCT date_trunc('minute', make_timestamp(rx_ns // 1000)) AS minute_utc FROM snapshots),
    c AS (SELECT date_trunc('minute', make_timestamp(rx_ns // 1000)) AS minute_utc, max(rtt_ms) AS clk_rtt
          FROM clock GROUP BY 1)
    SELECT m.minute_utc, m.events, round(m.mx - m.p50) AS spike_ms,
           s.minute_utc IS NOT NULL AS snapshot_in_minute, round(c.clk_rtt) AS clk_rtt_ms
    FROM m LEFT JOIN s ON s.minute_utc = m.minute_utc LEFT JOIN c ON c.minute_utc = m.minute_utc"""

show("minutes with a depth latency spike > 500 ms (max - median of rx - E within the minute), worst 30", f"""
    SELECT * FROM ({SPIKES}) WHERE spike_ms > 500 ORDER BY spike_ms DESC LIMIT 30""")

show("is a spike more likely in a minute with a REST snapshot? (snapshots: ~1 minute in 10)", f"""
    SELECT snapshot_in_minute, count(*) AS minutes,
           count(*) FILTER (WHERE spike_ms > 500) AS spike_minutes,
           round(100.0 * count(*) FILTER (WHERE spike_ms > 500) / count(*), 2) AS pct_spike
    FROM ({SPIKES}) GROUP BY 1 ORDER BY 1""")

show("per-minute median of rx - E jumping > 300 ms: step (stays) or stall (comes back)?", """
    WITH m AS (
        SELECT date_trunc('minute', make_timestamp(rx_ns // 1000)) AS minute_utc,
               median(rx_ns / 1e6 - event_ms) AS p50
        FROM depth_events GROUP BY 1),
    j AS (
        SELECT minute_utc, p50,
               lag(p50) OVER w AS prev_p50, lead(p50) OVER w AS next_p50,
               lag(minute_utc) OVER w AS prev_minute
        FROM m WINDOW w AS (ORDER BY minute_utc))
    SELECT minute_utc, round(prev_p50) AS prev_median_ms, round(p50) AS median_ms,
           round(next_p50) AS next_median_ms, round(p50 - prev_p50) AS jump_ms
    FROM j
    WHERE prev_minute = minute_utc - INTERVAL 1 MINUTE AND abs(p50 - prev_p50) > 300
    ORDER BY minute_utc""")