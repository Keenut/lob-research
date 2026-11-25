"""Tests for clock.py on a simulated clock whose true offset and true latency are known."""
import math

import numpy as np
import pytest

import clock

TRUE_LAT = 120.0       # true one-way delay Binance -> PC (ms)


def true_offset(local_ms):
    """Binance time - local time: PC 190 ms behind, drifting +2 ms per hour."""
    return 190.0 + 2.0 * (local_ms - 1.7e12) / 3.6e6


def simulate_clk(n=60, up=110.0, down=110.0, every_s=60):
    """Clock samples as etl.py stores them: server_ms, offset_ms, rtt_ms. up/down: path delays (ms)."""
    srv, off, rtt = [], [], []
    for k in range(n):
        t0 = 1.7e12 + k * every_s * 1000
        server_true = t0 + true_offset(t0) + up            # the server reads its clock 'up' ms after t0
        t1 = t0 + up + down
        s = math.floor(server_true)                        # serverTime is whole ms
        srv.append(s)
        off.append(s - (t0 + t1) / 2)
        rtt.append(t1 - t0)
    return np.array(srv), np.array(off), np.array(rtt)


def simulate_events(n=500):
    """Exchange times E and local receive times rx_ns for messages with TRUE_LAT one-way delay."""
    E = 1.7e12 + np.linspace(0, 59 * 60_000, n)
    arrive_true = E + TRUE_LAT
    rx_ms = arrive_true - true_offset(arrive_true)         # local clock reading (offset changes slowly)
    return (rx_ms * 1e6).astype(np.int64), E


def test_symmetric_path_recovers_true_latency_and_bounds_contain_it():
    good = clock.select_good(clock.clock_samples(*simulate_clk()), slack_ms=20)
    rx_ns, E = simulate_events()
    raw, corr, lo, hi = clock.latency(rx_ns, E, good, max_gap_ms=600_000)
    assert np.allclose(raw, TRUE_LAT - 190.0, atol=3)       # raw is wrong by the clock offset (+ drift)
    assert np.all(np.abs(corr - TRUE_LAT) < 1.5)           # ms truncation of serverTime: < 1 ms error
    assert np.all((lo <= TRUE_LAT + 0.01) & (TRUE_LAT <= hi + 0.01))


def test_asymmetric_path_biases_the_estimate_but_not_the_bounds():
    # request 150 ms, response 70 ms: same rtt (220) but the midpoint assumption is wrong by (150-70)/2 = 40 ms
    good = clock.select_good(clock.clock_samples(*simulate_clk(up=150, down=70)), slack_ms=20)
    rx_ns, E = simulate_events()
    _, corr, lo, hi = clock.latency(rx_ns, E, good, max_gap_ms=600_000)
    assert np.allclose(corr - TRUE_LAT, 40, atol=1.5)      # invisible from the data: a systematic bias
    assert np.all((lo <= TRUE_LAT + 0.01) & (TRUE_LAT <= hi + 0.01))


def test_drift_is_measured():
    good = clock.clock_samples(*simulate_clk(n=120))        # 2 hours
    slope, sd, step = clock.drift(good)
    assert slope == pytest.approx(2.0, abs=0.2) and sd < 1.0 and step < 2.0


def test_select_good_drops_slow_samples():
    s = clock.clock_samples([100, 200, 300], [10.0, 11.0, 50.0], [220.0, 230.0, 400.0])
    good = clock.select_good(s, slack_ms=20)
    assert list(good["rtt"]) == [220.0, 230.0]


def test_interpolation_and_max_gap():
    good = {"t_ms": np.array([0.0, 60_000.0]), "offset": np.array([100.0, 101.0]),
            "lo": np.array([0.0, 1.0]), "hi": np.array([200.0, 201.0])}
    off, lo, hi, dist = clock.interp_offset([30_000.0, 90_000.0, 1_000_000.0], good, max_gap_ms=60_000)
    assert off[0] == pytest.approx(100.5) and lo[0] == pytest.approx(0.5)
    assert off[1] == pytest.approx(101.0)                  # past the last sample: constant, within the gap
    assert np.isnan(off[2]) and dist[2] == pytest.approx(940_000)


def test_no_samples_gives_nan():
    empty = clock.clock_samples([], [], [])
    off, lo, hi, dist = clock.interp_offset([1.0, 2.0], empty, max_gap_ms=1e9)
    assert np.isnan(off).all() and np.isnan(hi).all()