"""The real CLI: the app's database, the inbox and stopping with SIGTERM."""

import json
import os
import signal
import stat
import subprocess
import sys
import textwrap
import time

import pytest

from ratchetflow.cli import main
from ratchetflow.store import Store

from .test_claude_agent import FAKE


def write_app(tmp_path, body: str) -> str:
    app = tmp_path / "flows.py"
    app.write_text(textwrap.dedent(f"""\
        from ratchetflow import Engine
        from ratchetflow.agents import claude
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
    proc = subprocess.Popen([sys.executable, "-m", "ratchetflow.cli", "--app", app,
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


def test_inbox_shows_the_whole_question_its_details_and_how_to_answer(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    app = write_app(tmp_path, """
        from ratchetflow import NeedsHuman

        @engine.workflow("w")
        def w(ctx):
            def approval():
                if ctx.scratch.get("signal") is None:
                    raise NeedsHuman("outline.ok", "Approve the outline? " + "Long context. " * 40,
                                     details={"title": "Durable execution"})
                return "ok"
            return ctx.step("approval", approval)
    """)
    main(["--app", app, "start", "w", "--id", "r"])
    capsys.readouterr()
    main(["--app", app, "inbox"])
    out = capsys.readouterr().out
    assert out.count("Long context.") == 40
    assert "Durable execution" in out
    assert "ratchet signal r outline.ok" in out
    main(["--app", app, "show", "r"])
    assert capsys.readouterr().out.count("Long context.") >= 40   # show no longer cuts it


def test_inbox_lists_open_questions_apart_and_numbered(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    app = write_app(tmp_path, """
        from ratchetflow import NeedsHuman

        @engine.workflow("w")
        def w(ctx):
            def review():
                if ctx.scratch.get("signal") is None:
                    raise NeedsHuman("draft.ok", "Publish the draft?",
                                     details={"report": "long report"},
                                     questions=["Keep the old title?", "Which tag fits?"])
                return "ok"
            return ctx.step("review", review)
    """)
    main(["--app", app, "start", "w", "--id", "r"])
    capsys.readouterr()
    main(["--app", app, "inbox"])
    out = capsys.readouterr().out
    assert "  open questions:\n    1. Keep the old title?\n    2. Which tag fits?\n" in out
    assert out.index("open questions") < out.index("details:")
    main(["--app", app, "inbox", "--json"])
    assert json.loads(capsys.readouterr().out)[0]["questions"] == ["Keep the old title?",
                                                                   "Which tag fits?"]


def test_inbox_as_json(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    app = write_app(tmp_path, """
        @engine.workflow("w")
        def w(ctx):
            return ctx.step("x", lambda: 1)
    """)
    main(["--app", app, "start", "w", "--id", "r"])
    capsys.readouterr()
    main(["--app", app, "inbox", "--json"])
    assert json.loads(capsys.readouterr().out) == []


def test_show_separates_working_time_from_waiting(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    app = write_app(tmp_path, """
        from ratchetflow import NeedsHuman

        @engine.workflow("w")
        def w(ctx):
            def ask():
                if ctx.scratch.get("signal") is None:
                    raise NeedsHuman("ok", "go on?")
                return 1
            return ctx.step("gate", ask)
    """)
    main(["--app", app, "start", "w", "--id", "r"])
    Store(tmp_path / "flows.db").x("UPDATE steps SET started_at = started_at - 2220")
    main(["--app", app, "signal", "r", "ok", "{}"])
    main(["--app", app, "tick"])
    capsys.readouterr()
    main(["--app", app, "show", "r"])
    assert "0s (+37m waiting)" in capsys.readouterr().out
