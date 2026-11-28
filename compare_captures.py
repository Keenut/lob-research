r"""compare_captures.py: compare two independent recordings (captures) of the same Binance streams.

Usage (PowerShell, from the repo root):
    python compare_captures.py --a data\raw --label-a huawei --b data\raw_msi --label-b msi
    python compare_captures.py --a data\raw --b data\raw_msi --from 2026-11-27T22 --to 2026-11-28T00

--a and --b are raw roots (the folders that contain BTCUSDT\). Without --from/--to, the hours present
in both roots are compared. Plain (.log) and compressed (.log.gz) hours are read alike.

Two recorders on two PCs, each with its own WebSocket connection, receive the same exchange events.
Their raw logs (the recorder's own output, no ETL in between) answer three questions:
  1. Does the recorder lose or change anything? A message of one capture must also be in the other,
     unless the other was not connected at that moment, and a message in both must be identical.
  2. Are the 100 ms depth event boundaries (first and last update id U, u) the same on two connections?
  3. How do the receive latencies of the two PCs and networks compare?

A depth event is identified by (U, u), a trade by its trade id t. Messages are not kept in memory,
only 128-bit BLAKE2b digests of the raw payload and of its fields (E, b, a, the rest).

Common window: the update ids (trade ids) that both captures span, from the larger of the two first
ids to the smaller of the two last ids. A message in the window that only one capture has falls into
one of three classes:
    other not connected   its ids lie in a hole between two sessions of the other capture
    gap in the other      the other was connected (inside one of its sessions) but did not receive
                          these ids: a real loss
    other boundaries      depth only: the other received these update ids in events with other
                          boundaries (the [U, u] ranges intersect without being equal)

Latency, for the messages in both captures:
  - raw rx - E per capture (rx - T, the trade time, for trades, as in clock.py), and corrected with
    the method and the functions of clock.py, from each capture's own 'clk' samples;
  - the receive time difference of the same message, rx_A - rx_B. Raw, it contains the difference of
    the two PC clocks. Corrected, its constant part is still uncertain by about +-(rtt_A + rtt_B) / 2,
    because the path asymmetry of either clock sample cannot be observed; its changes over time are
    measured far more precisely (to about the rtt slack of the good clock samples);
  - spike minutes: (max - median) of raw rx - E within one minute > 500 ms, as in explore.py, but the
    minutes are taken in exchange time E so that both captures put the same events in the same minute.
    The clock offset is the same for all events of a minute, so this needs no correction.
"""
import argparse
import hashlib
import json
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

import numpy as np

import clock
from etl import hour_of, open_raw, raw_files

SPIKE_MS = 500.0     # a spike minute: max - median of rx - E above this (same threshold as explore.py)
N_EXAMPLES = 3       # examples kept per class of difference
CLASSES = ("other not connected", "gap in the other", "other boundaries")
FIELDS = {"depth": ("E", "b", "a", "rest"), "trades": ("E", "rest")}   # compared one by one


# ---------------------------------------------------------------- loading

class DepthRec(NamedTuple):
    session: int
    rx_ns: int
    E: int          # event time (ms, exchange clock)
    raw: bytes      # digest of the raw payload, byte for byte
    b: bytes        # digest of the bid levels
    a: bytes        # digest of the ask levels
    rest: bytes     # digest of every other field (e, s, U, u, and any field Binance may add)


class TradeRec(NamedTuple):
    session: int
    rx_ns: int
    E: int          # event time
    T: int          # trade time
    raw: bytes
    rest: bytes     # digest of every field except E


@dataclass
class SessionInfo:
    first_depth_rx: int | None = None
    last_depth_rx: int | None = None
    depth_keys: list = field(default_factory=list)   # (U, u) of each depth event, in arrival order
    trade_ids: list = field(default_factory=list)
    errors: Counter = field(default_factory=Counter)


@dataclass
class Capture:
    label: str
    files: list                                     # paths of the hour files read
    depth: dict = field(default_factory=dict)       # (U, u) -> DepthRec
    trades: dict = field(default_factory=dict)      # t -> TradeRec
    sessions: dict = field(default_factory=dict)    # session id -> SessionInfo
    clk: list = field(default_factory=list)         # (server_ms, offset_ms, rtt_ms) of each clock sample
    skipped: int = 0         # lines that cannot be parsed (truncated, invalid JSON, a missing field)
    other_streams: int = 0   # ws messages of another stream
    dup_depth: int = 0       # an (U, u) received a second time in this capture (not counted again)
    dup_trades: int = 0


def digest(text):
    return hashlib.blake2b(text.encode("utf-8"), digest_size=16).digest()


def canonical(obj):
    """JSON with sorted keys and no spaces: equal objects give equal text."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def read_lines(files, bad):
    """(rx_ns, session, kind, payload) of every line, in file order; bad[0] counts the lines skipped."""
    for path in files:
        with open_raw(path) as f:
            for line in f:
                parts = line.rstrip("\n").split(" ", 3)
                try:
                    yield int(parts[0]), int(parts[1]), parts[2], parts[3]
                except (IndexError, ValueError):
                    bad[0] += 1      # truncated last line of a recorder that was killed


def parse_ws(payload):
    """('depth', (U, u), E, b, a, rest) or ('trade', t, E, T, rest) from a combined-stream message,
    ('other',) for another stream. Raises on a message that cannot be parsed."""
    msg = json.loads(payload)
    stream, d = msg["stream"], msg["data"]
    if stream.endswith("@depth@100ms"):
        rest = {k: v for k, v in d.items() if k not in ("E", "b", "a")}
        return "depth", (int(d["U"]), int(d["u"])), int(d["E"]), d["b"], d["a"], rest
    if stream.endswith("@trade"):
        rest = {k: v for k, v in d.items() if k != "E"}
        return "trade", int(d["t"]), int(d["E"]), int(d["T"]), rest
    return ("other",)


def load_capture(label, files):
    c = Capture(label, list(files))
    bad = [0]
    for rx_ns, sid, kind, payload in read_lines(files, bad):
        s = c.sessions.get(sid)
        if s is None:
            s = c.sessions[sid] = SessionInfo()
        if kind == "ws":
            try:
                m = parse_ws(payload)
                if m[0] == "depth":
                    _, key, E, b, a, rest = m
                    rec = DepthRec(sid, rx_ns, E, digest(payload), digest(canonical(b)),
                                   digest(canonical(a)), digest(canonical(rest)))
                elif m[0] == "trade":
                    _, key, E, T, rest = m
                    rec = TradeRec(sid, rx_ns, E, T, digest(payload), digest(canonical(rest)))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError, AttributeError):
                bad[0] += 1
                continue
            if m[0] == "other":
                c.other_streams += 1
            elif m[0] == "depth":
                if key in c.depth:
                    c.dup_depth += 1
                    continue
                c.depth[key] = rec
                s.depth_keys.append(key)
                if s.first_depth_rx is None:
                    s.first_depth_rx = rx_ns
                s.last_depth_rx = rx_ns
            else:
                if key in c.trades:
                    c.dup_trades += 1
                    continue
                c.trades[key] = rec
                s.trade_ids.append(key)
        elif kind == "clk":
            try:
                obj = json.loads(payload)
                t0, t1, srv = int(obj["t0"]), int(obj["t1"]), int(obj["srv"]["serverTime"])
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                bad[0] += 1
                continue
            c.clk.append((srv, srv - (t0 + t1) / 2e6, (t1 - t0) / 1e6))   # as in etl.py's clock table
        elif kind == "err":
            s.errors[payload[:140]] += 1
    c.skipped = bad[0]
    return c


def select_files(raw_a, raw_b, symbol, first=None, last=None):
    """The hour files of both roots from hour `first` to hour `last` (UTC hour stems, inclusive).
    A missing bound is taken from the hours present in both roots. Returns (files A, files B, first, last)."""
    fa, fb = raw_files(raw_a / symbol), raw_files(raw_b / symbol)
    common = sorted({hour_of(f) for f in fa} & {hour_of(f) for f in fb})
    if first is None or last is None:
        if not common:
            raise SystemExit(f"no hour is present in both {raw_a / symbol} and {raw_b / symbol}")
        first, last = first or common[0], last or common[-1]

    def pick(files):
        return [f for f in files if first <= hour_of(f) <= last]

    return pick(fa), pick(fb), first, last


# ---------------------------------------------------------------- id coverage

def runs(ranges):
    """Merge (first, last) id ranges into the maximal runs of consecutive ids, sorted."""
    out = []
    for lo, hi in sorted(ranges):
        if out and lo <= out[-1][1] + 1:
            out[-1][1] = max(out[-1][1], hi)
        else:
            out.append([lo, hi])
    return [(lo, hi) for lo, hi in out]


@dataclass
class Coverage:
    """Which ids (update ids or trade ids) one capture received, and when it was connected."""
    runs: list        # runs of consecutive ids received, over all sessions
    spans: list       # union of the sessions' [first id, last id]: the capture was connected there
    sessions: list    # (first id, last id, session) of each session, sorted
    gaps: dict        # session -> number of missing id ranges inside that session

    def __post_init__(self):
        self.run_starts = [lo for lo, _ in self.runs]
        self.span_starts = [lo for lo, _ in self.spans]
        self.session_starts = [lo for lo, _, _ in self.sessions]


def coverage(c, kind):
    per_session = {}
    for sid, s in c.sessions.items():
        ranges = s.depth_keys if kind == "depth" else [(t, t) for t in s.trade_ids]
        if ranges:
            per_session[sid] = runs(ranges)
    return Coverage(runs([r for rs in per_session.values() for r in rs]),
                    runs([(rs[0][0], rs[-1][1]) for rs in per_session.values()]),
                    sorted((rs[0][0], rs[-1][1], sid) for sid, rs in per_session.items()),
                    {sid: len(rs) - 1 for sid, rs in per_session.items()})


def intersects(intervals, starts, lo, hi):
    """Does [lo, hi] intersect one of the sorted, disjoint intervals?"""
    i = bisect_right(starts, hi) - 1          # the last interval that starts at or before hi
    return i >= 0 and intervals[i][1] >= lo


def inside(intervals, starts, lo, hi):
    """Is [lo, hi] inside one of the sorted, disjoint intervals?"""
    i = bisect_right(starts, lo) - 1
    return i >= 0 and intervals[i][1] >= hi


def classify(lo, hi, other):
    """Why the other capture has no message with ids [lo, hi] (classes: see the module docstring)."""
    if intersects(other.runs, other.run_starts, lo, hi):
        return "other boundaries"
    if inside(other.spans, other.span_starts, lo, hi):
        return "gap in the other"
    return "other not connected"


def hole_of(other, lo):
    """(session before, session after) of the other capture around id lo; None for a missing side."""
    i = bisect_right(other.session_starts, lo)
    before = other.sessions[i - 1][2] if i > 0 else None
    after = other.sessions[i][2] if i < len(other.sessions) else None
    return before, after


def window(cov_a, cov_b):
    """The ids that both captures span, (first, last), or None."""
    if not cov_a.runs or not cov_b.runs:
        return None
    lo, hi = max(cov_a.runs[0][0], cov_b.runs[0][0]), min(cov_a.runs[-1][1], cov_b.runs[-1][1])
    return (lo, hi) if lo <= hi else None


def overlapping(keys_sorted, starts, lo, hi):
    """Depth keys (U, u) of a sorted list of non-overlapping events that intersect [lo, hi]."""
    i = bisect_right(starts, hi) - 1
    out = []
    while i >= 0 and keys_sorted[i][1] >= lo:
        out.append(keys_sorted[i])
        i -= 1
    return out[::-1]


# ---------------------------------------------------------------- comparison

@dataclass
class PerCapture:
    """The messages of one capture, of one kind (depth or trades)."""
    total: int = 0                                     # distinct messages received
    outside: int = 0                                   # entirely outside the common window
    only: Counter = field(default_factory=Counter)     # class -> messages in the window only here
    examples: dict = field(default_factory=dict)       # class -> a few keys
    holes: Counter = field(default_factory=Counter)    # (session before, after) of the OTHER -> messages


@dataclass
class Result:
    kind: str
    window: tuple | None
    a: PerCapture
    b: PerCapture
    cov_a: Coverage
    cov_b: Coverage
    both: list = field(default_factory=list)           # keys in both captures (always in the window)
    identical: int = 0                                 # raw payloads identical byte for byte
    same_fields: int = 0                               # every field equal, raw bytes different
    differ: int = 0                                    # at least one field different
    differ_fields: Counter = field(default_factory=Counter)
    differ_examples: list = field(default_factory=list)
    e_diff: list = field(default_factory=list)         # E_A - E_B (ms) where E differs
    pairs: int = 0                                     # depth: intersecting, unequal (A, B) event pairs
    pair_examples: list = field(default_factory=list)  # (A key, [B keys])


def compare(kind, A, B):
    """Compare the depth events (kind 'depth') or the trades ('trades') of two captures."""
    ma, mb = (A.depth, B.depth) if kind == "depth" else (A.trades, B.trades)
    cov_a, cov_b = coverage(A, kind), coverage(B, kind)
    win = window(cov_a, cov_b)
    res = Result(kind, win, PerCapture(), PerCapture(), cov_a, cov_b)

    def ids(key):
        return key if kind == "depth" else (key, key)

    for mine, other, cov_other, side in ((ma, mb, cov_b, res.a), (mb, ma, cov_a, res.b)):
        side.total = len(mine)
        for key in mine:
            lo, hi = ids(key)
            if win is None or hi < win[0] or lo > win[1]:
                side.outside += 1
            elif key not in other:
                cls = classify(lo, hi, cov_other)
                side.only[cls] += 1
                ex = side.examples.setdefault(cls, [])
                if len(ex) < N_EXAMPLES:
                    ex.append(key)
                if cls == "other not connected":
                    side.holes[hole_of(cov_other, lo)] += 1

    for key, ra in ma.items():
        rb = mb.get(key)
        if rb is None:
            continue
        res.both.append(key)
        if ra.raw == rb.raw:
            res.identical += 1
            continue
        diff = [f for f in FIELDS[kind] if getattr(ra, f) != getattr(rb, f)]
        if not diff:
            res.same_fields += 1       # e.g. the same JSON with another key order
            continue
        res.differ += 1
        res.differ_fields.update(diff)
        if "E" in diff:
            res.e_diff.append(ra.E - rb.E)
        if len(res.differ_examples) < N_EXAMPLES:
            res.differ_examples.append(key)

    if kind == "depth" and (res.a.only["other boundaries"] or res.b.only["other boundaries"]):
        keys_b = sorted(mb)
        starts_b = [U for U, _ in keys_b]
        for key in ma:
            if key in mb or win is None or key[1] < win[0] or key[0] > win[1]:
                continue
            hit = overlapping(keys_b, starts_b, *key)
            res.pairs += len(hit)
            if hit and len(res.pair_examples) < N_EXAMPLES:
                res.pair_examples.append((key, hit))
    return res


def fetch_payloads(files, kind, keys):
    """Second pass over the files: the parsed data of a few messages, {key: data}."""
    want, out = set(keys), {}
    if not want:
        return out
    for _, _, rec_kind, payload in read_lines(files, [0]):
        if rec_kind != "ws":
            continue
        try:
            msg = json.loads(payload)
            d = msg["data"]
            if kind == "depth" and msg["stream"].endswith("@depth@100ms"):
                key = (int(d["U"]), int(d["u"]))
            elif kind == "trades" and msg["stream"].endswith("@trade"):
                key = int(d["t"])
            else:
                continue
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue
        if key in want and key not in out:
            out[key] = d
    return out


def field_diff(da, db, label_a, label_b):
    """Short text of the fields that differ between two parsed messages."""
    parts = []
    for k in sorted(set(da) | set(db)):
        va, vb = da.get(k), db.get(k)
        if va == vb:
            continue
        if isinstance(va, list) and isinstance(vb, list):
            sa, sb = {tuple(x) for x in va}, {tuple(x) for x in vb}
            if sa == sb:
                parts.append(f"{k}: the same levels in another order")
            else:
                parts.append(f"{k}: only {label_a} {sorted(sa - sb)[:3]}, only {label_b} {sorted(sb - sa)[:3]}")
        else:
            parts.append(f"{k}: {label_a} {va!r}, {label_b} {vb!r}")
    return "; ".join(parts)


# ---------------------------------------------------------------- latency

def good_samples(c, slack_ms):
    """(all samples, good samples) of a capture's clock samples, selected as clock.py does;
    (None, None) without samples."""
    if not c.clk:
        return None, None
    srv, off, rtt = (np.array(col, dtype=float) for col in zip(*c.clk))
    samples = clock.clock_samples(srv, off, rtt)
    return samples, clock.select_good(samples, slack_ms)


def offsets(rx_ns, good, max_gap_ms):
    """Clock offset, its lower and upper bound (ms) at the receive times; NaN without a near good sample."""
    if good is None:
        nan = np.full(len(rx_ns), np.nan)
        return nan, nan.copy(), nan.copy()
    off, lo, hi, _ = clock.interp_offset(np.asarray(rx_ns, dtype=np.int64) / 1e6, good, max_gap_ms)
    return off, lo, hi


@dataclass
class Latency:
    """Latency (ms) of the messages of one kind that are in both captures."""
    raw_a: np.ndarray       # rx - E (depth) or rx - T (trades), PC clock
    raw_b: np.ndarray
    corr_a: np.ndarray      # raw + clock offset (NaN where no good clock sample is near)
    corr_b: np.ndarray
    lo_a: np.ndarray        # bounds of the corrected value without the symmetric-path assumption
    hi_a: np.ndarray
    lo_b: np.ndarray
    hi_b: np.ndarray
    d_raw: np.ndarray       # rx_A - rx_B
    d_corr: np.ndarray      # (rx_A + offset_A) - (rx_B + offset_B)
    d_lo: np.ndarray
    d_hi: np.ndarray
    minute: np.ndarray      # minute of exchange time (capture A's E // 60000)


def latency(kind, res, A, B, good_a, good_b, max_gap_ms):
    ma, mb = (A.depth, B.depth) if kind == "depth" else (A.trades, B.trades)
    keys = res.both
    rx_a = np.array([ma[k].rx_ns for k in keys], dtype=np.int64)
    rx_b = np.array([mb[k].rx_ns for k in keys], dtype=np.int64)
    attr = "E" if kind == "depth" else "T"
    t_a = np.array([getattr(ma[k], attr) for k in keys], dtype=np.int64)
    t_b = np.array([getattr(mb[k], attr) for k in keys], dtype=np.int64)
    raw_a, raw_b = rx_a / 1e6 - t_a, rx_b / 1e6 - t_b
    off_a, lo_a, hi_a = offsets(rx_a, good_a, max_gap_ms)
    off_b, lo_b, hi_b = offsets(rx_b, good_b, max_gap_ms)
    d_raw = (rx_a - rx_b) / 1e6          # subtract in integer ns first: no float rounding of ~1.8e18 ns
    e_a = np.array([ma[k].E for k in keys], dtype=np.int64)
    return Latency(raw_a, raw_b, raw_a + off_a, raw_b + off_b, raw_a + lo_a, raw_a + hi_a,
                   raw_b + lo_b, raw_b + hi_b, d_raw, d_raw + off_a - off_b,
                   d_raw + lo_a - hi_b, d_raw + hi_a - lo_b, e_a // 60_000)


def spike_minutes(minute, raw_a, raw_b):
    """[(minute, messages, spike A, spike B)] per minute, spike = max - median of the raw latency."""
    order = np.argsort(minute, kind="stable")
    m, ra, rb = minute[order], raw_a[order], raw_b[order]
    if len(m) == 0:
        return []
    cuts = np.flatnonzero(np.diff(m)) + 1
    out = []
    for lo, hi in zip(np.r_[0, cuts], np.r_[cuts, len(m)]):
        a, b = ra[lo:hi], rb[lo:hi]
        out.append((int(m[lo]), int(hi - lo), float(a.max() - np.median(a)), float(b.max() - np.median(b))))
    return out


# ---------------------------------------------------------------- report

def utc(ns):
    return datetime.fromtimestamp(ns / 1e9, timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def utc_ms(ms):
    return datetime.fromtimestamp(ms / 1e3, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def q(a, p, digits=0):
    """Percentile p of the non-NaN values, formatted; '-' if there are none."""
    a = np.asarray(a, dtype=float)
    a = a[~np.isnan(a)]
    if not len(a):
        return "-"
    text = f"{np.percentile(a, p):.{digits}f}"
    return text[1:] if text.startswith("-") and float(text) == 0 else text   # no "-0"


def pct(n, total):
    return f"{100 * n / total:.2f} %" if total else "-"


def session_report(c, other_label, depth_cov, trade_cov, other_depth_in_holes, other_trades_in_holes):
    """Sessions of capture c and its holes; for each hole, how many messages the other capture has in it."""
    live = sorted((s.first_depth_rx, sid, s) for sid, s in c.sessions.items() if s.depth_keys)
    no_data = sum(1 for s in c.sessions.values() if not s.depth_keys and not s.trade_ids)
    print(f"{c.label}: {len(live)} session(s) with depth data"
          + (f", {no_data} without data (failed connection attempts)" if no_data else ""))
    for _, sid, s in live:
        print(f"  session {sid}: {utc(s.first_depth_rx)} -> {utc(s.last_depth_rx)}  "
              f"depth {len(s.depth_keys):,}  trades {len(s.trade_ids):,}  "
              f"update id gaps {depth_cov.gaps.get(sid, 0)}  trade id gaps {trade_cov.gaps.get(sid, 0)}")
        for msg, n in s.errors.most_common(3):
            print(f"      log error x{n}: {msg}")
    for (_, sa, a), (_, sb, b) in zip(live, live[1:]):
        print(f"  hole {sa} -> {sb}: {(b.first_depth_rx - a.last_depth_rx) / 1e9:.1f} s "
              f"({utc(a.last_depth_rx)} to {utc(b.first_depth_rx)}); {other_label} has "
              f"{other_depth_in_holes.get((sa, sb), 0):,} depth events and "
              f"{other_trades_in_holes.get((sa, sb), 0):,} trades in it")
    if len(live) < 2:
        print("  holes: none")


def window_report(res, A):
    msgs = A.depth if res.kind == "depth" else A.trades
    name = "update ids" if res.kind == "depth" else "trade ids"
    if res.window is None:
        print(f"{name}: none in common")
        return
    lo, hi = res.window
    line = f"{name} {lo} .. {hi} ({hi - lo + 1:,} ids)"
    if res.both:
        es = [msgs[k].E for k in res.both]
        line += f", messages in both from E = {utc_ms(min(es))} to {utc_ms(max(es))} ({(max(es) - min(es)) / 6e4:.1f} min)"
    print(line)


def kind_report(res, A, B):
    la, lb = A.label, B.label
    name, key = (("depth events", "first and last update id (U, u)") if res.kind == "depth"
                 else ("trades", "trade id t"))
    print(f"\n== {name} (key: {key})")

    def row(text, va="", vb=""):
        print(f"{text:46}{va:>14}{vb:>14}")

    row("", la, lb)
    dup = (A.dup_depth, B.dup_depth) if res.kind == "depth" else (A.dup_trades, B.dup_trades)
    n = len(res.both)
    row("received (distinct messages)", f"{res.a.total:,}", f"{res.b.total:,}")
    row("  = outside the common window", f"{res.a.outside:,}", f"{res.b.outside:,}")
    row("  + in the window, only in this capture", f"{sum(res.a.only.values()):,}", f"{sum(res.b.only.values()):,}")
    for cls in CLASSES:
        if res.kind == "trades" and cls == "other boundaries":
            continue
        row(f"      {cls}", f"{res.a.only[cls]:,}", f"{res.b.only[cls]:,}")
    row("  + in both", f"{n:,}", f"{n:,}")
    row("received a second time (not counted above)", f"{dup[0]:,}", f"{dup[1]:,}")
    print(f"in both: raw payload identical byte for byte {res.identical:,} ({pct(res.identical, n)}), "
          f"same fields in other bytes {res.same_fields:,}, different fields {res.differ:,}")
    if res.differ:
        print("  fields that differ: " + ", ".join(f"{f} {k:,}" for f, k in res.differ_fields.most_common()))
    if res.e_diff:
        e = np.array(res.e_diff)
        print(f"  E_{la} - E_{lb} where E differs (ms): min {e.min()}, p50 {np.median(e):.0f}, max {e.max()}")
    if res.differ_examples:
        pa = fetch_payloads(A.files, res.kind, res.differ_examples)
        pb = fetch_payloads(B.files, res.kind, res.differ_examples)
        for k in res.differ_examples:
            if k in pa and k in pb:
                print(f"    {k}: {field_diff(pa[k], pb[k], la, lb)}")
    if res.kind == "depth":
        print(f"event pairs whose [U, u] intersect without being equal: {res.pairs:,}")
        for ka, hit in res.pair_examples:
            print(f"    {la} {ka} intersects {lb} {hit}")
    for side, cap in ((res.a, A), (res.b, B)):
        for cls in ("gap in the other", "other boundaries"):
            if side.only[cls]:
                print(f"    examples, only in {cap.label}, {cls}: {side.examples[cls]}")


def clock_line(c, samples, good):
    if samples is None:
        return f"{c.label}: no clock samples in these hours, no correction"
    b, sd, step = clock.drift(good)
    return (f"{c.label}: {len(samples['t_ms'])} samples, rtt min/p50 {samples['rtt'].min():.0f}/"
            f"{np.percentile(samples['rtt'], 50):.0f} ms, {len(good['t_ms'])} good, offset median "
            f"{np.median(good['offset']):.1f} ms, drift {b:+.1f} ms/h, "
            f"largest step between good samples {step:.1f} ms")


def latency_report(kind, lat, la, lb):
    name, t = ("depth events", "rx - E") if kind == "depth" else ("trades", "rx - T")
    if len(lat.d_raw) == 0:
        print(f"\n{name}: no message in both captures")
        return
    print(f"\n{name}, {len(lat.d_raw):,} messages in both, {t} (ms):")
    print(f"  {'':16}{'raw p50':>9}{'raw p99':>9}{'corr p50':>10}{'corr p99':>10}{'corr p50 bounds':>18}{'corrected':>11}")
    for lab, raw, corr, lo, hi in ((la, lat.raw_a, lat.corr_a, lat.lo_a, lat.hi_a),
                                   (lb, lat.raw_b, lat.corr_b, lat.lo_b, lat.hi_b)):
        bounds = f"[{q(lo, 50)}, {q(hi, 50)}]"
        covered = 100 * np.mean(~np.isnan(corr))
        print(f"  {lab:16}{q(raw, 50):>9}{q(raw, 99):>9}{q(corr, 50):>10}{q(corr, 99):>10}{bounds:>18}{covered:>10.1f}%")
    d = lat.d_corr
    ok = ~np.isnan(d)
    print(f"  receive time {la} - {lb} of the same message: raw p50 {q(lat.d_raw, 50, 1)}; corrected "
          f"p1 {q(d, 1, 1)}, p10 {q(d, 10, 1)}, p50 {q(d, 50, 1)}, p90 {q(d, 90, 1)}, p99 {q(d, 99, 1)}, "
          f"p50 bounds [{q(lat.d_lo, 50, 1)}, {q(lat.d_hi, 50, 1)}]")
    if ok.any():
        print(f"  {la} received it first in {100 * np.mean(d[ok] < 0):.1f} % of the messages "
              f"(corrected; rests on the symmetric-path assumption of both clocks)")


def spikes_report(lat, la, lb):
    rows = spike_minutes(lat.minute, lat.raw_a, lat.raw_b)
    if not rows:
        return
    sa = np.array([r[2] for r in rows]) > SPIKE_MS
    sb = np.array([r[3] for r in rows]) > SPIKE_MS
    print(f"\nspike minutes: max - median of raw rx - E > {SPIKE_MS:.0f} ms within a minute of exchange time "
          f"(depth events in both, {len(rows)} minutes)")
    print(f"  in both {np.sum(sa & sb)}, only {la} {np.sum(sa & ~sb)}, only {lb} {np.sum(~sa & sb)}, "
          f"in neither {np.sum(~sa & ~sb)}")
    worst = [r for r in sorted(rows, key=lambda r: -max(r[2], r[3]))[:10] if max(r[2], r[3]) > SPIKE_MS]
    if worst:
        print(f"  {'minute (E, UTC)':>20}{'events':>8}{'spike ' + la:>16}{'spike ' + lb:>16}")
        for m, n, a, b in sorted(worst):
            print(f"  {utc_ms(m * 60_000)[:16] + 'Z':>20}{n:>8}{a:>16.0f}{b:>16.0f}")


def report(A, B, first, last, slack_ms=20.0, max_gap_ms=600_000.0):
    print(f"compare_captures: A = {A.label}, B = {B.label}, hours {first} .. {last}")
    for c in (A, B):
        print(f"  {c.label}: {', '.join(p.name for p in c.files)}; lines skipped {c.skipped}, "
              f"messages of other streams {c.other_streams}")
    depth, trades = compare("depth", A, B), compare("trades", A, B)

    print("\n== sessions (receive time rx on each PC's own clock, UTC)")
    session_report(A, B.label, depth.cov_a, trades.cov_a, depth.b.holes, trades.b.holes)
    session_report(B, A.label, depth.cov_b, trades.cov_b, depth.a.holes, trades.a.holes)

    print("\n== common window (the ids that both captures span)")
    window_report(depth, A)
    window_report(trades, A)

    kind_report(depth, A, B)
    kind_report(trades, A, B)

    print(f"\n== latency (messages in both captures; clock correction as in clock.py: good samples "
          f"rtt <= min + {slack_ms:.0f} ms, max {max_gap_ms / 1000:.0f} s to a good sample)")
    sa, ga = good_samples(A, slack_ms)
    sb, gb = good_samples(B, slack_ms)
    print("clock samples: " + clock_line(A, sa, ga))
    print("               " + clock_line(B, sb, gb))
    lat_depth = latency("depth", depth, A, B, ga, gb, max_gap_ms)
    latency_report("depth", lat_depth, A.label, B.label)
    latency_report("trades", latency("trades", trades, A, B, ga, gb, max_gap_ms), A.label, B.label)
    spikes_report(lat_depth, A.label, B.label)
    return depth, trades


def main():
    ap = argparse.ArgumentParser(description="Compare two raw captures of the same Binance streams")
    ap.add_argument("--a", type=Path, default=Path("data/raw"), help="raw root of capture A")
    ap.add_argument("--b", type=Path, default=Path("data/raw_msi"), help="raw root of capture B")
    ap.add_argument("--label-a", default=None, help="name of capture A in the report (default: folder name)")
    ap.add_argument("--label-b", default=None, help="name of capture B in the report (default: folder name)")
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--from", dest="first", help="first UTC hour, e.g. 2026-11-27T22")
    ap.add_argument("--to", dest="last", help="last UTC hour, inclusive")
    ap.add_argument("--slack-ms", type=float, default=20.0, help="good clock sample: rtt <= min rtt + slack")
    ap.add_argument("--max-gap-s", type=float, default=600.0, help="max distance to the nearest good clock sample")
    a = ap.parse_args()
    fa, fb, first, last = select_files(a.a, a.b, a.symbol, a.first, a.last)
    A = load_capture(a.label_a or a.a.name, fa)
    B = load_capture(a.label_b or a.b.name, fb)
    report(A, B, first, last, a.slack_ms, a.max_gap_s * 1000)


if __name__ == "__main__":
    main()
