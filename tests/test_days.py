"""Tests for days.py: intervals cut at midnight, synced intervals from the replay's syncs and gaps, and
the day table of two synthetic sessions across midnight (a hole between them, a sequence gap inside
the second), written in the recorder's format and converted by etl.py."""
import sys
from collections import namedtuple
from datetime import datetime, timezone

import days
import etl
import replay
from test_replay import synthetic_market, write_raw_log

D = days.DAY_NS
MIDNIGHT = (1_767_225_600 + 86_400) * 10**9       # 2026-01-02T00:00:00Z in ns


def test_merge_and_cut_at_midnight():
    assert days.merge([(5, 9), (1, 3), (2, 4), (9, 10)]) == [(1, 4), (5, 10)]
    assert days.per_day([(D - 10, D + 5), (3 * D, 3 * D + 1)]) == {0: 10, 1: 5, 3: 1}
    assert days.per_day([(0, 2 * D + 7)]) == {0: D, 1: D, 2: 7}
    assert days.per_day([(4, 6), (5, 8)]) == {0: 4}                     # overlaps counted once


def test_synced_intervals_from_syncs_and_gaps():
    S = namedtuple("S", "syncs gaps last_rx")      # the fields of replay.SessionReplay that are used
    s = S(syncs=[(0, 10, 1), (0, 50, 2), (0, 70, 3)], gaps=[(40, 0, 0), (70, 0, 0)], last_rx=100)
    # synced from 10, gap at 40, resync at 50, gap at 70 and resync on the same event, end at 100
    assert days.synced_intervals(s) == [(10, 40), (50, 70), (70, 100)]
    assert days.synced_intervals(S([], [], 100)) == []
    assert days.synced_intervals(S([(0, 30, 1)], [], 30)) == [(30, 30)]


def test_holes_sessions_and_replay_counts_per_day():
    spans = {1: (D - 50, D - 40), 2: (D - 45, D - 30), 3: (D - 20, D + 10), 4: (D + 30, D + 35), 5: (3 * D, 3 * D + 1)}
    assert days.holes(spans) == [(D - 30, D - 20), (D + 10, D + 30), (D + 35, 3 * D)]   # 1 and 2 overlap
    assert days.session_counts(spans) == {0: 3, 1: 2, 3: 1}
    # the third hole lasts from day 1 until midnight of day 3 (excluded): days 1 and 2, with its whole length
    assert days.hole_counts(days.holes(spans)) == {0: (1, 10), 1: (2, 2 * D - 35), 2: (1, 2 * D - 35)}
    C = namedtuple("C", "snap_rx_ns mismatches unexplained crossed")
    S = namedtuple("S", "gaps syncs checks")
    # the second snapshot arrives after midnight, the event that it syncs with is from before midnight
    s = S(gaps=[(D + 1, 0, 0)], syncs=[(D + 4, D - 1, 0), (D + 1, D + 2, 0)],
          checks=[C(D - 1, [1, 2, 3], [], False), C(D + 5, [1], [1], True), C(D + 6, [], [], False)])
    counts = days.replay_counts([s])
    assert counts[0] == {"syncs": 1, "checks": 1, "mismatched": 3, "unexplained": 0, "crossed": 0}
    assert counts[1] == {"gaps": 1, "syncs": 1, "checks": 2, "mismatched": 1, "unexplained": 1, "crossed": 1}


def build(tmp_path):
    """Session 7 from 10 s before midnight (about 30 s long); session 8, another market, 5 s after it,
    with one event missing (a sequence gap). Lines go to hour files by receive time, like the recorder."""
    ev7, sn7, _ = synthetic_market(1)
    end7 = write_raw_log(tmp_path / "s7.txt", ev7, sn7, session=7, rx0=MIDNIGHT - 10 * 10**9)
    ev8, sn8, _ = synthetic_market(2, first_id=500_001)
    del ev8[100]
    write_raw_log(tmp_path / "s8.txt", ev8, sn8, session=8, rx0=end7 + 5 * 10**9)
    by_hour = {}
    for name in ("s7.txt", "s8.txt"):
        for line in (tmp_path / name).read_text(encoding="utf-8").splitlines():
            hour = datetime.fromtimestamp(int(line.split(" ", 1)[0]) / 1e9, timezone.utc).strftime("%Y-%m-%dT%H")
            by_hour.setdefault(hour, []).append(line)
    raw, base = tmp_path / "raw", tmp_path / "parquet"
    raw.mkdir()
    for hour, lines in by_hour.items():
        (raw / f"{hour}.log").write_text("\n".join(lines) + "\n", encoding="utf-8")
        tables, bad = etl.parse_log(raw / f"{hour}.log")
        assert bad == 0
        etl.write_tables(tables, base, hour)
    return base


def test_day_table_of_sessions_across_midnight(tmp_path):
    base = build(tmp_path)
    stems = replay.hour_stems(base)
    assert stems == ["2026-01-01T23", "2026-01-02T00"]
    table, (s7, s8) = days.day_table(base, stems, top_n=1000)
    d1, d2 = MIDNIGHT // D - 1, MIDNIGHT // D
    assert list(table) == [d1, d2]
    a, b = table[d1], table[d2]
    assert (a["hours"], b["hours"]) == (1, 1) and (a["sessions"], b["sessions"]) == (1, 2)
    assert a["time_ns"] == MIDNIGHT - s7.first_rx                         # session 7 is cut at midnight
    assert b["time_ns"] == (s7.last_rx - MIDNIGHT) + (s8.last_rx - s8.first_rx)
    assert a["first_rx"] == s7.first_rx and b["last_rx"] == s8.last_rx
    assert a["last_rx"] < MIDNIGHT <= b["first_rx"]
    for s in (s7, s8):    # synced time = what the replay counts as not unsynced
        assert sum(y - x for x, y in days.synced_intervals(s)) == s.last_rx - s.first_rx - s.unsynced_ns
    assert a["book_ns"] + b["book_ns"] == sum(s.last_rx - s.first_rx - s.unsynced_ns for s in (s7, s8))
    assert a["book_ns"] <= a["time_ns"] and b["book_ns"] < b["time_ns"]  # the gap in session 8 costs time
    assert (a["holes"], b["holes"]) == (0, 1)
    assert b["hole_ns"] == b["max_hole_ns"] == s8.first_rx - s7.last_rx
    assert (a["gaps"], b["gaps"]) == (0, 1) and len(s8.gaps) == 1
    assert a["syncs"] + b["syncs"] == len(s7.syncs) + len(s8.syncs)
    assert a["checks"] + b["checks"] == len(s7.checks) + len(s8.checks) > 0
    assert a["unexplained"] == b["unexplained"] == 0
    assert a["depth"] + b["depth"] == s7.events + s8.events


def test_report_from_the_command_line(tmp_path, monkeypatch, capsys):
    base = build(tmp_path)
    monkeypatch.setattr(sys, "argv", ["days.py", "--parquet", str(base.parent), "--symbol", base.name,
                                      "--top", "1000"])
    days.main()
    out = capsys.readouterr().out
    assert "hours 2026-01-01T23 .. 2026-01-02T00 (2 files)" in out
    lines = [line for line in out.splitlines() if line.startswith("2026-01-0")]
    assert [line.split()[0] for line in lines] == ["2026-01-01", "2026-01-02"]
    assert "top 1000 levels per side" in out
