import pytest

from ratchetflow import Engine


class Clock:
    def __init__(self, t: float = 1_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, s: float) -> None:
        self.t += s


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def make_engine(tmp_path, clock):
    """Several engines on the same database = several processes (dead worker, new worker)."""
    sleeps: list[float] = []

    def make(**kw) -> Engine:
        kw.setdefault("clock", clock)
        kw.setdefault("sleeper", sleeps.append)
        kw.setdefault("log", lambda _m: None)
        kw.setdefault("heartbeat_s", 3600)
        return Engine(tmp_path / "r.db", **kw)

    make.sleeps = sleeps  # type: ignore[attr-defined]
    return make


class Crash(BaseException):
    """Simulates the process dying: not an Exception, so the engine does not handle it."""
