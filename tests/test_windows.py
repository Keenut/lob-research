"""Tests for the hour window of the reports (--from / --to / --max-files): replay.hour_stems, the option
parser, and the windowed reading of the Parquet tables in clock.py and explore.py."""
import argparse
import json

import duckdb
import pytest

import clock
import etl
import explore
import replay

H0 = 1_767_225_600 * 10**9        # 2026-01-01T00:00:00Z in ns


def test_hour_stems_window(tmp_path):
    folder = tmp_path / "depth_events"
    folder.mkdir()
    hours = ["2026-01-01T22", "2026-01-01T23", "2026-01-02T00", "2026-01-02T01"]
    for h in hours:
        (folder / f"{h}.parquet").write_bytes(b"")
    assert replay.hour_stems(tmp_path) == hours
    assert replay.hour_stems(tmp_path, first="2026-01-01T23", last="2026-01-02T00") == hours[1:3]
    assert replay.hour_stems(tmp_path, first="2026-01-02T00") == hours[2:]
    assert replay.hour_stems(tmp_path, last="2026-01-01T22") == hours[:1]
    assert replay.hour_stems(tmp_path, 1, first="2026-01-01T23") == hours[1:2]   # max_files after the window
    assert replay.hour_stems(tmp_path, 2) == hours[:2]
    assert replay.hour_stems(tmp_path, first="2026-01-03T00") == []


def test_window_options_accept_only_the_file_name_spelling():
    ap = argparse.ArgumentParser()
    replay.add_window_args(ap)
    a = ap.parse_args(["--from", "2026-01-01T23", "--to", "2026-01-02T00", "--max-files", "3"])
    assert (a.first, a.last, a.max_files) == ("2026-01-01T23", "2026-01-02T00", 3)
    a = ap.parse_args([])
    assert (a.first, a.last, a.max_files) == (None, None, None)
    for bad in ("2026-01-01", "2026-01-01 23", "2026-01-01T23:00", "2026-1-1T23", "T23"):
        with pytest.raises(SystemExit):
            ap.parse_args(["--from", bad])


def write_hour(raw, stem, rx0, first_id, n_events, clock_sample):
    """A raw hour in the recorder's format: n_events depth events, and one clock sample if asked."""
    lines = []
    for i in range(n_events):
        rx = rx0 + i * 100_000_000
        data = {"e": "depthUpdate", "E": rx // 10**6, "s": "BTCUSDT", "U": first_id + i, "u": first_id + i,
                "b": [["100.00", f"{i + 1}.00000000"]], "a": []}
        lines.append(f"{rx} 7 ws {json.dumps({'stream': 'btcusdt@depth@100ms', 'data': data})}")
    if clock_sample:
        lines.append(f'{rx0 + 1} 7 clk {{"t0":{rx0},"t1":{rx0 + 2 * 10**6},"srv":{{"serverTime":{rx0 // 10**6 + 1}}}}}')
    path = raw / f"{stem}.log"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_clock_and_explore_read_only_the_hours_of_the_window(tmp_path):
    raw, base = tmp_path / "raw", tmp_path / "parquet"
    raw.mkdir()
    for stem, rx0, first_id, n, clk in (("2026-01-01T00", H0, 100, 5, True),
                                        ("2026-01-01T01", H0 + 3600 * 10**9, 200, 3, False)):
        tables, bad = etl.parse_log(write_hour(raw, stem, rx0, first_id, n, clk))
        assert bad == 0
        etl.write_tables(tables, base, stem)
    assert not (base / "clock" / "2026-01-01T01.parquet").exists()      # an hour without rows has no file

    both = replay.hour_stems(base)
    assert len(clock.read_table(base, "clock", ["offset_ms"], both)["offset_ms"]) == 1
    assert len(clock.read_table(base, "clock", ["offset_ms"], ["2026-01-01T01"])["offset_ms"]) == 0
    assert list(clock.read_table(base, "depth_events", ["last_id"], ["2026-01-01T01"])["last_id"]) == [200, 201, 202]
    assert len(clock.read_table(base, "depth_events", ["last_id"])["last_id"]) == 8   # no window: every file

    con = duckdb.connect()
    explore.create_views(con, base, ["2026-01-01T01"])
    assert con.execute("SELECT count(*) FROM depth_events").fetchone()[0] == 3
    assert con.execute("SELECT count(*) FROM clock").fetchone()[0] == 0      # an empty view, same columns
    assert "offset_ms" in [d[0] for d in con.execute("SELECT * FROM clock").description]
    con = duckdb.connect()
    explore.create_views(con, base, both)
    assert con.execute("SELECT count(*) FROM depth_events").fetchone()[0] == 8
    assert con.execute("SELECT count(*) FROM clock").fetchone()[0] == 1


def test_sql_text_of_a_path_with_a_quote():
    assert explore.sql_text("data/it's/x.parquet") == "'data/it''s/x.parquet'"
