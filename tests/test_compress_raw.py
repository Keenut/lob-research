"""Tests for compress_raw.py and for reading compressed raw hours in etl.py / check_continuity.py."""
import gzip
import json
from datetime import datetime, timezone

import pytest

import check_continuity
import compress_raw
import etl
from test_replay import synthetic_market, write_raw_log


def make_hour(dir_, hour, seed=1):
    """A realistic raw hour file (depth events + snapshots in recorder format)."""
    events, snaps, _ = synthetic_market(seed, n_updates=1500)
    path = dir_ / f"{hour}.log"
    write_raw_log(path, events, snaps, session=3, rx0=10**18)
    return path


def test_compress_one_is_lossless_and_removes_the_original(tmp_path):
    src = make_hour(tmp_path, "2026-01-01T00")
    original = src.read_bytes()
    before, after = compress_raw.compress_one(src)
    gz = tmp_path / "2026-01-01T00.log.gz"
    assert not src.exists() and gz.exists() and not (tmp_path / "2026-01-01T00.log.gz.tmp").exists()
    assert gzip.decompress(gz.read_bytes()) == original
    assert before == len(original) and after == gz.stat().st_size < before


def test_select_never_touches_the_current_or_previous_hour(tmp_path):
    for hour in ("2026-01-01T07", "2026-01-01T08", "2026-01-01T09", "2026-01-01T10"):
        (tmp_path / f"{hour}.log").write_text("x\n")
    now = datetime(2026, 1, 1, 10, 15, tzinfo=timezone.utc)   # current hour T10, previous T09
    assert [p.name for p in compress_raw.select(tmp_path, now)] == ["2026-01-01T07.log", "2026-01-01T08.log"]


def test_duplicate_is_resolved_only_when_identical(tmp_path):
    src = make_hour(tmp_path, "2026-01-01T00")
    dst = tmp_path / "2026-01-01T00.log.gz"
    dst.write_bytes(gzip.compress(src.read_bytes()))
    assert compress_raw.resolve_duplicate(src, dst) > 0 and not src.exists()

    src.write_text("different content\n")
    with pytest.raises(RuntimeError):
        compress_raw.resolve_duplicate(src, dst)
    assert src.exists() and dst.exists()                      # nothing deleted


def test_etl_reads_a_compressed_hour_exactly_like_the_plain_one(tmp_path):
    src = make_hour(tmp_path, "2026-01-01T00")
    plain_tables, plain_bad = etl.parse_log(src)
    compress_raw.compress_one(src)
    gz_tables, gz_bad = etl.parse_log(tmp_path / "2026-01-01T00.log.gz")
    assert gz_tables == plain_tables and gz_bad == plain_bad
    assert len(plain_tables["depth_events"]["rx_ns"]) > 100


def test_etl_lists_hours_in_order_and_rejects_duplicates(tmp_path):
    (tmp_path / "2026-01-01T01.log").write_text("")
    (tmp_path / "2026-01-01T00.log.gz").write_bytes(gzip.compress(b""))
    assert [etl.hour_of(p) for p in etl.raw_files(tmp_path)] == ["2026-01-01T00", "2026-01-01T01"]
    (tmp_path / "2026-01-01T01.log.gz").write_bytes(gzip.compress(b""))
    with pytest.raises(SystemExit):
        etl.raw_files(tmp_path)


def test_etl_skips_a_record_with_a_missing_field_and_keeps_columns_aligned(tmp_path):
    good = {"stream": "btcusdt@depth@100ms",
            "data": {"E": 1, "U": 10, "u": 11, "b": [["100.00", "1.0"]], "a": [["101.00", "2.0"]]}}
    broken = {"stream": "btcusdt@depth@100ms", "data": {"E": 2, "U": 12, "u": 13, "b": [["100.00", "0"]]}}  # no "a"
    path = tmp_path / "2026-01-01T00.log"
    path.write_text(f"1 7 ws {json.dumps(good)}\n2 7 ws {json.dumps(broken)}\n", encoding="utf-8")
    tables, bad = etl.parse_log(path)
    assert bad == 1
    assert {k: len(v) for k, v in tables["depth_events"].items()} == dict.fromkeys(tables["depth_events"], 1)
    assert {k: len(v) for k, v in tables["depth_updates"].items()} == dict.fromkeys(tables["depth_updates"], 2)
    counts = etl.write_tables(tables, tmp_path / "parquet", "2026-01-01T00")   # would fail if misaligned
    assert counts["depth_events"] == 1 and counts["depth_updates"] == 2


def test_check_continuity_reads_compressed_hours(tmp_path, capsys):
    src = make_hour(tmp_path, "2026-01-01T00")
    check_continuity.check([src])
    plain = capsys.readouterr().out
    compress_raw.compress_one(src)
    check_continuity.check([tmp_path / "2026-01-01T00.log.gz"])
    assert capsys.readouterr().out == plain and "gaps=0" in plain