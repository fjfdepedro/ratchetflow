"""The real CLI: the app's database and stopping with SIGTERM."""

import os
import signal
import stat
import subprocess
import sys
import textwrap
import time

import pytest

from ratchet.cli import main
from ratchet.store import Store

from .test_claude_agent import FAKE


def write_app(tmp_path, body: str) -> str:
    app = tmp_path / "flows.py"
    app.write_text(textwrap.dedent(f"""\
        from ratchet import Engine
        from ratchet.agents import claude
        engine = Engine({str(tmp_path / "flows.db")!r}, log=lambda m: None)
    """) + textwrap.dedent(body))
    return f"{app}:engine"


def test_runs_and_show_read_the_apps_database(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    app = write_app(tmp_path, """
        @engine.workflow("w")
        def w(ctx):
            return ctx.step("x", lambda: 1)
    """)
    main(["--app", app, "start", "w", "--id", "r"])
    capsys.readouterr()
    main(["--app", app, "show", "r"])
    assert "completed" in capsys.readouterr().out
    main(["--app", app, "runs"])
    assert "r " in capsys.readouterr().out


def test_sigterm_kills_the_agent_and_releases_the_lease(tmp_path):
    bin_ = tmp_path / "claude-fake"
    bin_.write_text(FAKE)
    bin_.chmod(bin_.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "calls.jsonl"
    app = write_app(tmp_path, f"""
        @engine.workflow("w")
        def w(ctx):
            return ctx.step("a", claude, ctx, "hi", bin={str(bin_)!r}, timeout_s=600)
    """)
    env = {**os.environ, "FAKE_LOG": str(log), "FAKE_MODES": "hang"}
    proc = subprocess.Popen([sys.executable, "-m", "ratchet.cli", "--app", app,
                             "start", "w", "--id", "r"], env=env, cwd=tmp_path)
    child_file = tmp_path / "calls.jsonl.child"
    end = time.monotonic() + 20
    while not child_file.exists() and time.monotonic() < end:
        time.sleep(0.05)
    time.sleep(0.2)
    child = int(child_file.read_text())
    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=30) == 130
    time.sleep(0.2)
    with pytest.raises(ProcessLookupError):
        os.kill(child, 0)                                  # the grandchild died with its parent
    run = Store(tmp_path / "flows.db").get_run("r")
    assert run.owner is None and run.lease_until is None   # can be resumed right away
