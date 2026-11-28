r"""clock.py: this PC's clock offset to Binance (from the 'clk' samples) and corrected receive latency.

Usage (PowerShell, from the repo root, after python etl.py):
    python clock.py                    # clock report + latency per session and per UTC hour
    python clock.py --slack-ms 10      # stricter filter for 'good' clock samples
    python clock.py --from 2026-11-24T23 --to 2026-11-25T22   # a window of UTC hours (both included)

Clock sample (recorder.py, every 60 s): local times t0, t1 around GET /api/v3/time -> serverTime (ms).
    offset = serverTime - (t0 + t1) / 2        Binance time - local time; positive: this PC is behind
This midpoint estimate (Cristian's algorithm) assumes the request and the response take equal time.
Without that assumption we only know, because the server read its clock somewhere between t0 and t1
and serverTime is truncated to whole ms:
    serverTime - t1  <=  offset  <=  serverTime + 1 - t0        (interval width = rtt + 1 ms)
A sample with a large rtt (queueing, retransmission) is less precise, so only samples with
rtt <= min_rtt + slack are kept ('good' samples).

The offset drifts (the PC clock runs at a slightly wrong rate and Windows corrects it from time to time),
so it is interpolated linearly in time between good samples. A message farther than --max-gap-s from
every good sample gets no correction (e.g. sessions recorded before clock sampling existed).

Latency of a message received at local time rx, with exchange timestamp E (ms):
    raw       = rx - E                    (check_continuity.py prints this: it includes the clock error)
    corrected = rx + offset - E           (one-way network + Binance-side delay, symmetric-path assumption)
    bounds    = [rx + offset_lo - E, rx + offset_hi - E]     (no symmetry assumption)
Changes of latency over time (jitter, p99 - p50, spikes) are measured far more precisely than its
absolute level, whose uncertainty is about +-rtt/2 because path asymmetry cannot be observed.
"""
import argparse
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

import replay


# ---------------------------------------------------------------- pure functions (tested)

def clock_samples(server_ms, offset_ms, rtt_ms):
    """Rebuild each sample's local midpoint time and offset bounds from the Parquet clock columns.
    etl.py stored offset = srv - (t0 + t1) / 2 and rtt = t1 - t0, so (t0 + t1) / 2 = srv - offset."""
    server_ms, offset_ms, rtt_ms = (np.asarray(a, dtype=float) for a in (server_ms, offset_ms, rtt_ms))
    order = np.argsort(server_ms - offset_ms)
    return {
        "t_ms": (server_ms - offset_ms)[order],          # local time of the sample (midpoint)
        "offset": offset_ms[order],
        "lo": (offset_ms - rtt_ms / 2)[order],           # srv - t1
        "hi": (offset_ms + rtt_ms / 2 + 1.0)[order],     # srv + 1 - t0
        "rtt": rtt_ms[order],
    }


def select_good(samples, slack_ms):
    """Keep the samples whose rtt is within slack_ms of the smallest rtt."""
    keep = samples["rtt"] <= samples["rtt"].min() + slack_ms
    return {k: v[keep] for k, v in samples.items()}


def interp_offset(t_ms, good, max_gap_ms):
    """Offset, lo, hi at local times t_ms (linear between good samples, constant past the ends).
    NaN where the nearest good sample is more than max_gap_ms away. Returns (offset, lo, hi, distance_ms)."""
    t_ms = np.asarray(t_ms, dtype=float)
    ts = good["t_ms"]
    if len(ts) == 0:
        nan = np.full(t_ms.shape, np.nan)
        return nan, nan.copy(), nan.copy(), nan.copy()
    i = np.searchsorted(ts, t_ms)
    left = ts[np.clip(i - 1, 0, len(ts) - 1)]
    right = ts[np.clip(i, 0, len(ts) - 1)]
    dist = np.minimum(np.abs(t_ms - left), np.abs(right - t_ms))
    out = [np.interp(t_ms, ts, good[k]) for k in ("offset", "lo", "hi")]
    for a in out:
        a[dist > max_gap_ms] = np.nan
    return out[0], out[1], out[2], dist


def drift(good):
    """Least-squares line offset = a + b * hours. Returns (b in ms/h, residual sd in ms, largest step
    between consecutive good samples in ms). A sudden step means Windows corrected the clock."""
    if len(good["t_ms"]) < 3:
        return np.nan, np.nan, np.nan
    hours = (good["t_ms"] - good["t_ms"][0]) / 3.6e6
    b, a = np.polyfit(hours, good["offset"], 1)
    resid = good["offset"] - (a + b * hours)
    return b, resid.std(ddof=2), np.abs(np.diff(good["offset"])).max()


def latency(rx_ns, exch_ms, good, max_gap_ms):
    """raw, corrected, lower bound, upper bound (ms) for messages received at rx_ns with exchange time exch_ms."""
    rx_ms = np.asarray(rx_ns, dtype=np.int64) / 1e6
    raw = rx_ms - np.asarray(exch_ms, dtype=float)
    off, lo, hi, _ = interp_offset(rx_ms, good, max_gap_ms)
    return raw, raw + off, raw + lo, raw + hi


# ---------------------------------------------------------------- loading and report

def read_table(base, table, columns, stems=None):
    """The hour files of one Parquet table as {column: numpy array}: all of them, or those of the given
    hours (stems such as '2026-11-25T22'; an hour without rows in this table has no file)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if stems is None:
        files = sorted((base / table).glob("*.parquet"))
    else:
        files = [f for f in (base / table / f"{stem}.parquet" for stem in stems) if f.exists()]
    if not files:
        return {c: np.array([]) for c in columns}
    t = pa.concat_tables([pq.read_table(f, columns=columns) for f in files])
    return {c: t.column(c).to_numpy() for c in columns}


def pct(a, q):
    a = a[~np.isnan(a)]
    return f"{np.percentile(a, q):.0f}" if len(a) else "-"


def report_messages(name, rx_ns, session, exch_ms, good, max_gap_ms):
    raw, corr, lo, hi = latency(rx_ns, exch_ms, good, max_gap_ms)
    print(f"\n== {name}: latency per session (ms); corrected needs a good clock sample within the max gap")
    print(f"{'session':>11} {'messages':>10} {'covered':>8} {'raw p50':>8} {'raw p99':>8} "
          f"{'corr p50':>9} {'corr p99':>9} {'bounds p50':>14}")
    for sid in np.unique(session):
        m = session == sid
        cov = 100 * np.mean(~np.isnan(corr[m]))
        print(f"{sid:>11} {m.sum():>10,} {cov:>7.1f}% {pct(raw[m], 50):>8} {pct(raw[m], 99):>8} "
              f"{pct(corr[m], 50):>9} {pct(corr[m], 99):>9} {'[' + pct(lo[m], 50) + ', ' + pct(hi[m], 50) + ']':>14}")
    return raw, corr


def main():
    ap = argparse.ArgumentParser(description="Clock offset to Binance and corrected receive latency")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--parquet", type=Path, default=Path("data/parquet"))
    ap.add_argument("--slack-ms", type=float, default=20.0, help="good sample: rtt <= min rtt + slack")
    ap.add_argument("--max-gap-s", type=float, default=600.0, help="max distance to the nearest good sample")
    replay.add_window_args(ap)
    a = ap.parse_args()
    base = a.parquet / a.symbol
    max_gap_ms = a.max_gap_s * 1000
    stems = replay.hour_stems(base, a.max_files, a.first, a.last)
    if not stems:
        print(f"no Parquet files for these hours in {base / 'depth_events'} (run python etl.py first)")
        return
    print(f"hours {stems[0]} .. {stems[-1]} ({len(stems)} files)")

    clk = read_table(base, "clock", ["server_ms", "offset_ms", "rtt_ms"], stems)
    samples = clock_samples(clk["server_ms"], clk["offset_ms"], clk["rtt_ms"])
    if len(samples["t_ms"]) == 0:
        print("no clock samples: run the updated recorder.py, then python etl.py")
        return
    good = select_good(samples, a.slack_ms)
    span_h = (samples["t_ms"][-1] - samples["t_ms"][0]) / 3.6e6
    print(f"clock samples: {len(samples['t_ms'])} over {span_h:.2f} h, "
          f"rtt min/p50/p99 = {samples['rtt'].min():.0f}/{np.percentile(samples['rtt'], 50):.0f}/"
          f"{np.percentile(samples['rtt'], 99):.0f} ms")
    print(f"good samples (rtt <= {samples['rtt'].min():.0f} + {a.slack_ms:.0f} ms): {len(good['t_ms'])}")
    width = good["hi"] - good["lo"]
    print(f"offset (Binance - local, good samples): median {np.median(good['offset']):.1f} ms, "
          f"range {good['offset'].min():.1f} .. {good['offset'].max():.1f} ms, "
          f"bounds width median {np.median(width):.0f} ms")
    b, sd, step = drift(good)
    print(f"drift: {b:+.1f} ms/h (linear fit), residual sd {sd:.1f} ms, largest step between good samples {step:.1f} ms")

    ev = read_table(base, "depth_events", ["rx_ns", "session", "event_ms"], stems)
    raw, corr = report_messages("depth events (rx - E)", ev["rx_ns"], ev["session"], ev["event_ms"], good, max_gap_ms)
    tr = read_table(base, "trades", ["rx_ns", "session", "trade_ms"], stems)
    report_messages("trades (rx - T, trade time)", tr["rx_ns"], tr["session"], tr["trade_ms"], good, max_gap_ms)

    print("\n== depth events per UTC hour (covered events only): raw drifts with the clock, corrected should not")
    print(f"{'hour_utc':>14} {'events':>8} {'offset p50':>11} {'raw p50':>8} {'corr p50':>9} {'corr p99':>9}")
    hour = (ev["rx_ns"] // 3_600_000_000_000).astype(np.int64)
    covered = ~np.isnan(corr)
    off = corr - raw
    for h in np.unique(hour[covered]):
        m = covered & (hour == h)
        label = datetime.fromtimestamp(h * 3600, timezone.utc).strftime("%Y-%m-%dT%H")
        print(f"{label:>14} {m.sum():>8,} {pct(off[m], 50):>11} {pct(raw[m], 50):>8} "
              f"{pct(corr[m], 50):>9} {pct(corr[m], 99):>9}")


if __name__ == "__main__":
    main()