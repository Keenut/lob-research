"""Tests for recorder.py: the retry policy of the snapshot loop (no network: a fake HTTP session)."""
import asyncio

import aiohttp
import pytest

import recorder


class Response:
    def __init__(self, status, body="{}", headers=None):
        self.status, self.body, self.headers = status, body, headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def text(self):
        return self.body


class Failing:
    """Context manager whose entry raises, like a request that times out or cannot connect."""

    def __init__(self, exc):
        self.exc = exc

    async def __aenter__(self):
        raise self.exc

    async def __aexit__(self, *exc):
        return False


class FakeHttp:
    """Answers each GET with the next scripted outcome: an exception or a Response."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)

    def get(self, url, params=None):
        out = self.outcomes.pop(0)
        return Failing(out) if isinstance(out, BaseException) else out


class FakeLog:
    def __init__(self):
        self.kinds = []

    def write(self, session, kind, payload):
        self.kinds.append(kind)

    def flush(self):
        pass


class Stop(Exception):
    pass


def run_loop(outcomes, monkeypatch, interval=600.0):
    """Run snapshot_loop until the outcomes are used up; return (sleep durations, logged kinds)."""
    sleeps, log = [], FakeLog()
    n = len(outcomes)

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == n:
            raise Stop

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    with pytest.raises(Stop):
        asyncio.run(recorder.snapshot_loop(FakeHttp(outcomes), log, "BTCUSDT", 1, interval))
    return sleeps, log.kinds


def test_failures_are_retried_sooner_with_a_doubling_delay(monkeypatch):
    outcomes = [asyncio.TimeoutError(), aiohttp.ClientConnectionError(), Response(500, "oops"),
                asyncio.TimeoutError(), asyncio.TimeoutError(), asyncio.TimeoutError(), Response(200)]
    sleeps, kinds = run_loop(outcomes, monkeypatch)
    assert sleeps == [5, 10, 20, 40, 60, 60, 600]            # capped at 60 s, then the normal interval
    assert kinds == ["err"] * 6 + ["snap"]


def test_a_success_resets_the_retry_delay(monkeypatch):
    sleeps, kinds = run_loop([asyncio.TimeoutError(), asyncio.TimeoutError(), Response(200),
                              asyncio.TimeoutError(), Response(200)], monkeypatch)
    assert sleeps == [5, 10, 600, 5, 600]
    assert kinds == ["err", "err", "snap", "err", "snap"]


@pytest.mark.parametrize("status", [429, 418, 403])
def test_rate_limits_wait_as_asked_and_do_not_retry_sooner(monkeypatch, status):
    sleeps, kinds = run_loop([Response(status, headers={"Retry-After": "30"}), Response(status),
                              Response(status, headers={"Retry-After": "soon"}), Response(200)], monkeypatch)
    assert sleeps == [30, 600, 600, 600]                      # no header or a bad one: the full interval
    assert kinds == ["err", "err", "err", "snap"]


def test_retry_after_parses_seconds_and_falls_back():
    assert recorder.retry_after({"Retry-After": "12"}, 600) == 12
    assert recorder.retry_after({"Retry-After": "0"}, 600) == 1      # never a busy loop
    assert recorder.retry_after({}, 600) == 600
