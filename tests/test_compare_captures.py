"""Tests for compare_captures.py: two synthetic captures of one exchange stream, written in the
recorder's line format, with holes, a changed payload, a shifted event boundary, a missing message
inside a session and known clock offsets."""
import gzip
import json
import sys

import numpy as np
import pytest

import compare_captures as cc

E0 = 1_795_000_000_000          # exchange time of the first event (ms)


def market(n_events=60, n_trades=90):
    """The exchange side: a depth event every 100 ms with 1 to 4 update ids, and trades."""
    events, uid = [], 10_000
    for i in range(n_events):
        U, u = uid + 1, uid + 1 + i % 4
        uid = u
        events.append({"e": "depthUpdate", "E": E0 + 100 * i, "s": "BTCUSDT", "U": U, "u": u,
                       "b": [[f"{100 - i % 7 / 100:.2f}", f"{i / 1000:.8f}"]],
                       "a": [[f"{100.01 + i % 5 / 100:.2f}", "0.50000000"]]})
    trades = [{"e": "trade", "E": E0 + 66 * j + 1, "s": "BTCUSDT", "t": 500 + j, "p": "100.00",
               "q": "0.01000000", "T": E0 + 66 * j, "m": j % 2 == 0, "M": True} for j in range(n_trades)]
    return events, trades


def ws(stream, data):
    return json.dumps({"stream": stream, "data": data}, separators=(",", ":"))


def capture(events, trades, latency_ms=100.0, offset_ms=0.0, session_of=lambda E: 1):
    """Recorder lines (rx_ns, session, kind, payload) of one capture. A message with exchange time E
    arrives at true time E + latency, read on a PC clock that is offset_ms behind the exchange
    (offset = exchange - PC, as in clock.py)."""
    lines = []
    for stream, msgs in (("btcusdt@depth@100ms", events), ("btcusdt@trade", trades)):
        for d in msgs:
            rx = int((d["E"] + latency_ms - offset_ms) * 1e6)
            lines.append((rx, session_of(d["E"]), "ws", ws(stream, d)))
    return lines


def clock_lines(rtt_ms, offset_ms, session=1, every_ms=1000):
    """Clock samples around the whole market: request at true time tau, the server reads its clock
    halfway through the round trip (a symmetric path)."""
    out = []
    for tau in range(E0 - 1000, E0 + 10_000, every_ms):
        t0, t1 = int((tau - offset_ms) * 1e6), int((tau + rtt_ms - offset_ms) * 1e6)
        srv = int(tau + rtt_ms / 2)
        out.append((t1, session, "clk", json.dumps({"t0": t0, "t1": t1, "srv": {"serverTime": srv}})))
    return out


def write(root, lines, hour="2026-01-01T00", gz=False, tail=""):
    folder = root / "BTCUSDT"
    folder.mkdir(parents=True, exist_ok=True)
    text = "".join(f"{rx} {s} {k} {p}\n" for rx, s, k, p in sorted(lines)) + tail
    if gz:
        with gzip.open(folder / f"{hour}.log.gz", "wt", encoding="utf-8") as f:
            f.write(text)
    else:
        (folder / f"{hour}.log").write_text(text, encoding="utf-8")


def run(tmp_path, lines_a, lines_b):
    write(tmp_path / "a", lines_a)
    write(tmp_path / "b", lines_b)
    fa, fb, _, _ = cc.select_files(tmp_path / "a", tmp_path / "b", "BTCUSDT")
    A, B = cc.load_capture("A", fa), cc.load_capture("B", fb)
    return A, B, cc.compare("depth", A, B), cc.compare("trades", A, B)


def key(e):
    return e["U"], e["u"]


def test_runs_merge_adjacent_and_overlapping_ranges():
    assert cc.runs([(5, 6), (1, 2), (3, 4), (10, 12), (11, 15), (17, 17)]) == [(1, 6), (10, 15), (17, 17)]


def test_identical_captures(tmp_path):
    ev, tr = market()
    A, B, d, t = run(tmp_path, capture(ev, tr, 120), capture(ev, tr, 60, session_of=lambda E: 2))
    assert d.window == (ev[0]["U"], ev[-1]["u"])
    assert len(d.both) == d.identical == len(ev) and d.differ == d.same_fields == 0
    assert d.a.outside == d.b.outside == 0 and not +d.a.only and not +d.b.only and d.pairs == 0
    assert len(t.both) == t.identical == len(tr) and not +t.a.only and not +t.b.only
    assert A.skipped == B.skipped == 0 and d.cov_a.gaps == {1: 0} and t.cov_b.gaps == {2: 0}
    lat = cc.latency("depth", d, A, B, None, None, 600_000.0)       # no clock samples: no correction
    assert np.allclose(lat.d_raw, 60) and np.isnan(lat.corr_a).all() and np.isnan(lat.d_corr).all()


def test_hole_in_one_capture_is_explained_and_counted_per_hole(tmp_path):
    ev, tr = market()
    hole = lambda E: E0 + 2000 <= E < E0 + 3000            # B reconnects: sessions 2, then 3
    lines_b = capture([e for e in ev if not hole(e["E"])], [x for x in tr if not hole(x["E"])], 60,
                      session_of=lambda E: 2 if E < E0 + 2000 else 3)
    A, B, d, t = run(tmp_path, capture(ev, tr, 120), lines_b)
    n_tr = sum(hole(x["E"]) for x in tr)
    assert d.a.only == {"other not connected": 10} and d.a.holes == {(2, 3): 10}
    assert t.a.only == {"other not connected": n_tr} and t.a.holes == {(2, 3): n_tr}
    assert not +d.b.only and d.identical == len(ev) - 10 and d.cov_b.gaps == {2: 0, 3: 0}


def test_messages_outside_the_common_window(tmp_path):
    ev, tr = market()
    a_on = lambda E: E < E0 + 5000                            # A stops at 5 s
    b_on = lambda E: E >= E0 + 2000                           # B starts at 2 s
    A, B, d, t = run(tmp_path, capture([e for e in ev if a_on(e["E"])], [x for x in tr if a_on(x["E"])]),
                     capture([e for e in ev if b_on(e["E"])], [x for x in tr if b_on(x["E"])]))
    assert d.window == (ev[20]["U"], ev[49]["u"])
    assert d.a.outside == 20 and d.b.outside == 10 and not +d.a.only and not +d.b.only
    assert t.a.outside == sum(not b_on(x["E"]) for x in tr) and t.b.outside == sum(not a_on(x["E"]) for x in tr)
    assert not +t.a.only and not +t.b.only and len(d.both) == 30


def test_changed_fields_are_reported_field_by_field(tmp_path):
    ev, tr = market()
    ev_b, tr_b = [dict(e) for e in ev], [dict(x) for x in tr]
    ev_b[10]["b"] = [[ev[10]["b"][0][0], "9.00000000"]]       # another quantity
    ev_b[20]["E"] += 1                                         # another event time
    ev_b[30] = dict(reversed(list(ev[30].items())))           # same fields, other key order
    tr_b[5]["E"] += 2
    A, B, d, t = run(tmp_path, capture(ev, tr), capture(ev_b, tr_b))
    assert d.differ == 2 and d.differ_fields == {"b": 1, "E": 1} and d.e_diff == [-1]
    assert d.same_fields == 1 and d.identical == len(ev) - 3
    assert t.differ == 1 and t.differ_fields == {"E": 1} and t.e_diff == [-2]
    pa = cc.fetch_payloads(A.files, "depth", d.differ_examples)
    pb = cc.fetch_payloads(B.files, "depth", d.differ_examples)
    assert "only B [('99.97', '9.00000000')]" in cc.field_diff(pa[key(ev[10])], pb[key(ev[10])], "A", "B")
    assert cc.field_diff(pa[key(ev[20])], pb[key(ev[20])], "A", "B") == f"E: A {ev[20]['E']}, B {ev[20]['E'] + 1}"


def test_split_event_is_a_boundary_difference(tmp_path):
    ev, tr = market()
    e = ev[15]                                                 # update ids U .. U + 3
    U, u = key(e)
    ev_b = ev[:15] + [dict(e, u=U + 1), dict(e, U=U + 2, E=e["E"] + 50)] + ev[16:]
    A, B, d, t = run(tmp_path, capture(ev, tr), capture(ev_b, tr))
    assert d.a.only == {"other boundaries": 1} and d.b.only == {"other boundaries": 2}
    assert d.pairs == 2 and d.pair_examples == [((U, u), [(U, U + 1), (U + 2, u)])]
    assert len(d.both) == len(ev) - 1 and d.differ == 0


def test_shifted_boundary_is_a_boundary_difference(tmp_path):
    ev, tr = market()
    V = ev[17]["U"]                                            # A: [V, V + 1] and [V + 2, V + 4]
    assert key(ev[17]) == (V, V + 1) and key(ev[18]) == (V + 2, V + 4)
    ev_b = ev[:17] + [dict(ev[17], u=V + 2), dict(ev[18], U=V + 3)] + ev[19:]   # B: [V, V + 2], [V + 3, V + 4]
    A, B, d, t = run(tmp_path, capture(ev, tr), capture(ev_b, tr))
    assert d.a.only == {"other boundaries": 2} and d.b.only == {"other boundaries": 2}
    assert d.pairs == 3       # [V, V+1] meets [V, V+2]; [V+2, V+4] meets [V, V+2] (at V + 2) and [V+3, V+4]


def test_boundary_difference_at_the_end_of_a_session(tmp_path):
    ev, tr = market()
    U15 = ev[15]["U"]
    # B's session 2 ends with event 14 extended by one update id, so B received up to U15 only;
    # session 3 starts at event 20
    ev_b = ev[:14] + [dict(ev[14], u=U15)] + ev[20:]
    A, B, d, t = run(tmp_path, capture(ev, tr),
                     capture(ev_b, tr, session_of=lambda E: 2 if E <= ev[14]["E"] else 3))
    assert d.a.only == {"other boundaries": 2, "other not connected": 4}   # events 14, 15 | 16 .. 19
    assert d.a.holes == {(2, 3): 4} and d.pairs == 2


def test_missing_messages_inside_a_session_are_gaps(tmp_path):
    ev, tr = market()
    dup = capture(ev[3:4], [])                                 # B also receives event 3 twice
    A, B, d, t = run(tmp_path, capture(ev, tr), capture(ev[:25] + ev[26:], tr[:40] + tr[41:]) + dup)
    assert d.a.only == {"gap in the other": 1} and d.a.examples["gap in the other"] == [key(ev[25])]
    assert t.a.only == {"gap in the other": 1} and t.a.examples["gap in the other"] == [tr[40]["t"]]
    assert d.cov_b.gaps == {1: 1} and t.cov_b.gaps == {1: 1}
    assert not d.a.holes and not t.a.holes                     # a gap is not a hole between sessions
    assert B.dup_depth == 1 and d.b.total == len(ev) - 1


def test_clock_correction_recovers_the_latency_difference(tmp_path):
    ev, tr = market()
    lines_a = capture(ev, tr, 120, offset_ms=1000) + clock_lines(240, 1000)
    lines_b = capture(ev, tr, 80, offset_ms=-30, session_of=lambda E: 2) + clock_lines(160, -30, session=2)
    A, B, d, t = run(tmp_path, lines_a, lines_b)
    _, ga = cc.good_samples(A, 20.0)
    _, gb = cc.good_samples(B, 20.0)
    lat = cc.latency("depth", d, A, B, ga, gb, 600_000.0)
    assert np.allclose(lat.d_raw, (120 - 1000) - (80 + 30), atol=0.01)   # the two clocks are 1030 ms apart
    assert np.allclose(lat.d_corr, 40, atol=0.01)
    assert np.allclose(lat.corr_a, 120, atol=0.01) and np.allclose(lat.corr_b, 80, atol=0.01)
    assert np.all(lat.d_lo <= 40) and np.all(lat.d_hi >= 40)                  # bounds hold the truth
    assert np.allclose(lat.d_hi - lat.d_lo, (240 + 1) + (160 + 1), atol=0.01)  # rtt + 1 ms per clock
    lat_t = cc.latency("trades", t, A, B, ga, gb, 600_000.0)
    assert np.allclose(lat_t.corr_a, 121, atol=0.01)                         # rx - T, and E = T + 1 ms
    assert len(set(lat.minute)) == 1                  # the 6 s of the market fall in one minute of E


def test_spike_minutes_per_capture():
    minute = np.array([0, 0, 0, 1, 1, 1, 2, 2, 2])
    raw_a = np.array([100, 101, 900, 100, 100, 100, 100, 100, 1000.0])
    raw_b = np.array([50, 51, 52, 50, 50, 700, 50, 50, 900.0])
    assert cc.spike_minutes(minute, raw_a, raw_b) == [(0, 3, 799.0, 1.0), (1, 3, 0.0, 650.0), (2, 3, 900.0, 850.0)]


def test_file_selection_compressed_hours_and_truncated_line(tmp_path):
    ev, tr = market()
    a, b = tmp_path / "a", tmp_path / "b"
    write(a, capture(ev[:30], tr), hour="2026-01-01T00", gz=True)
    write(a, capture(ev[30:], []), hour="2026-01-01T01",
          tail='1795000009000000000 1 ws {"stream":"btcu\n17950000090')       # two truncated lines
    write(a, capture(ev[:1], []) + [(1, 1, "ws", ws("btcusdt@bookTicker", {}))],
          hour="2026-01-01T02")                                 # only in A: not read by default
    write(b, capture(ev[:30], tr), hour="2026-01-01T00")
    write(b, capture(ev[30:], []), hour="2026-01-01T01")
    fa, fb, first, last = cc.select_files(a, b, "BTCUSDT")
    assert (first, last) == ("2026-01-01T00", "2026-01-01T01")
    assert [f.name for f in fa] == ["2026-01-01T00.log.gz", "2026-01-01T01.log"]
    A, B = cc.load_capture("A", fa), cc.load_capture("B", fb)
    assert A.skipped == 2 and len(A.depth) == len(ev) and len(A.trades) == len(tr)
    assert cc.compare("depth", A, B).identical == len(ev)
    fa, fb, _, _ = cc.select_files(a, b, "BTCUSDT", "2026-01-01T01", "2026-01-01T02")
    assert [f.name for f in fa] == ["2026-01-01T01.log", "2026-01-01T02.log"]
    assert [f.name for f in fb] == ["2026-01-01T01.log"]
    assert cc.load_capture("A", fa).other_streams == 1
    write(b, [], hour="2026-01-01T00", gz=True)               # the same hour as .log and .log.gz
    with pytest.raises(SystemExit):
        cc.select_files(a, b, "BTCUSDT")


def test_report_from_the_command_line(tmp_path, monkeypatch, capsys):
    ev, tr = market()
    write(tmp_path / "a", capture(ev, tr, 120, offset_ms=1000) + clock_lines(240, 1000))
    write(tmp_path / "b", capture(ev, tr, 80, offset_ms=-30, session_of=lambda E: 2) + clock_lines(160, -30, session=2))
    monkeypatch.setattr(sys, "argv", ["compare_captures.py", "--a", str(tmp_path / "a"), "--label-a", "pc1",
                                      "--b", str(tmp_path / "b"), "--label-b", "pc2"])
    cc.main()
    out = capsys.readouterr().out
    assert f"in both: raw payload identical byte for byte {len(ev)} (100.00 %)" in out
    assert f"in both: raw payload identical byte for byte {len(tr)} (100.00 %)" in out
    assert "receive time pc1 - pc2 of the same message: raw p50 -990.0; corrected p1 40.0" in out
    assert "event pairs whose [U, u] intersect without being equal: 0" in out
    assert "spike minutes" in out and "holes: none" in out
