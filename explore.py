r"""explore.py: sanity queries on the Parquet tables with DuckDB.

Usage (PowerShell, from the repo root, after python etl.py):
    python explore.py [SYMBOL]
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