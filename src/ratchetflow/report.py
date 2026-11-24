"""One run as a self-contained HTML page: how it ran, where the time went and what it cost.

No server and nothing loaded from the network: the page carries its own CSS and draws its charts
as inline SVG, so it can be opened from disk, attached to an issue or kept next to the run's
output. Everything it shows comes from the database (`runs`, `steps`, `events`).

What it cannot know: the workflow is a plain function, so the page shows the steps that ran,
not the ones that would have run. Branches of `ctx.parallel` are found by their times overlapping.
Within a step, work is drawn first and the wait after it: the database keeps how long a step
worked (`busy_s`), not when.
"""

from __future__ import annotations

import html
import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .store import RunRow, StepRow, Store

# Quiet stretches longer than this are folded on the timeline, so a step that waited a day for a
# person does not squash every other bar into a line.
FOLD_S = 30.0
FOLD_PX = 28.0


@dataclass
class _Bar:
    step: StepRow
    kind: str                # agent | human | sleep | event | step
    start: float
    busy_end: float
    end: float


def _span(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms" if seconds > 0 else "0s"
    if seconds < 120:
        return f"{seconds:.0f}s"
    if seconds < 7200:
        return f"{seconds / 60:.0f}m"
    if seconds < 3 * 86400:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def _usd(x: float) -> str:
    return f"${x:.4f}" if 0 < x < 0.01 else f"${x:.2f}"


def _when(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _e(s: Any) -> str:
    return html.escape(str(s), quote=True)


def _kind(step: StepRow, run: RunRow) -> str:
    out = step.output
    if isinstance(out, dict) and "session_id" in out and "turns" in out:
        return "agent"
    if "question" in step.scratch:
        return "human"
    if "child" in step.scratch:
        return "event"   # waits for another run
    if "wake_at" in step.scratch:
        return "sleep"
    if step.status == "waiting" and run.wait_event:
        return "event"
    return "step"


def _bars(run: RunRow, steps: list[StepRow], now: float) -> list[_Bar]:
    bars = []
    for s in steps:
        end = s.finished_at if s.finished_at is not None else max(now, s.started_at)
        kind = _kind(s, run)
        if s.busy_s is not None:
            busy = s.busy_s
        else:   # written before 0.2.0: a wait was mostly waiting, anything else mostly work
            busy = 0.0 if kind in ("human", "sleep", "event") else end - s.started_at
        busy_end = min(end, s.started_at + max(busy, 0.0))
        bars.append(_Bar(s, kind, s.started_at, busy_end, end))
    return bars


def _stages(bars: list[_Bar]) -> list[list[_Bar]]:
    """Steps in order; a step that starts before the stage before it has ended ran beside it."""
    stages: list[list[_Bar]] = []
    stage_end = float("-inf")
    for b in bars:
        if stages and b.start < stage_end:
            stages[-1].append(b)
        else:
            stages.append([b])
            stage_end = float("-inf")
        stage_end = max(stage_end, b.end)
    return stages


class _Axis:
    """Time to x, with quiet stretches folded to a fixed width."""

    def __init__(self, bars: list[_Bar], t0: float, t1: float, width: float):
        busy = sorted((b.start, b.busy_end) for b in bars)
        merged: list[list[float]] = []
        for a, z in busy:
            if merged and a <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], z)
            else:
                merged.append([a, z])
        # The pieces of [t0, t1]: quiet ones longer than FOLD_S are folded.
        pieces: list[tuple[float, float, bool]] = []
        cursor = t0
        for a, z in merged + [[t1, t1]]:
            a, z = max(a, t0), min(z, t1)
            if a > cursor:
                pieces.append((cursor, a, a - cursor > FOLD_S))
            if z > a:
                pieces.append((a, z, False))
            cursor = max(cursor, z)
        if not pieces:
            pieces = [(t0, max(t1, t0 + 1), False)]
        folded = [p for p in pieces if p[2]]
        linear = sum(z - a for a, z, f in pieces if not f) or 1.0
        px_per_s = max(width - FOLD_PX * len(folded), width * 0.2) / linear
        self.segments: list[tuple[float, float, float, float]] = []
        self.folds: list[tuple[float, float]] = []   # (x, seconds folded)
        x = 0.0
        for a, z, f in pieces:
            w = FOLD_PX if f else (z - a) * px_per_s
            self.segments.append((a, z, x, x + w))
            if f:
                self.folds.append((x, z - a))
            x += w
        self.width = x

    def __call__(self, t: float) -> float:
        for a, z, x0, x1 in self.segments:
            if t <= z:
                if t <= a or z == a:
                    return x0
                return x0 + (t - a) / (z - a) * (x1 - x0)
        return self.width


def _timeline(bars: list[_Bar], t0: float, t1: float) -> str:
    label_w, row_h, chart_w, top = 190.0, 24.0, 700.0, 22.0
    axis = _Axis(bars, t0, t1, chart_w)
    h = top + row_h * len(bars) + 8
    w = label_w + axis.width + 12
    parts = [f'<svg class="timeline" viewBox="0 0 {w:.0f} {h:.0f}" '
             f'width="{w:.0f}" height="{h:.0f}" role="img" '
             f'aria-label="Timeline of the run\'s steps">']
    for x, secs in axis.folds:
        cx = label_w + x
        parts.append(f'<rect class="fold" x="{cx:.1f}" y="{top - 6}" width="{FOLD_PX}" '
                     f'height="{h - top}"/>'
                     f'<text class="fold-label" x="{cx + FOLD_PX / 2:.1f}" y="{top - 9}" '
                     f'text-anchor="middle">{_e(_span(secs))}</text>')
    for i, b in enumerate(bars):
        y = top + i * row_h
        xs, xb, xe = label_w + axis(b.start), label_w + axis(b.busy_end), label_w + axis(b.end)
        name = b.step.name if len(b.step.name) <= 26 else b.step.name[:25] + "…"
        parts.append(f'<text class="row-label" x="{label_w - 10}" y="{y + row_h * 0.62:.1f}" '
                     f'text-anchor="end">{_e(name)}</text>')
        if xe - xb > 0.5:
            parts.append(f'<rect class="wait" x="{xb:.1f}" y="{y + 6:.1f}" '
                         f'width="{xe - xb:.1f}" height="{row_h - 12:.1f}" rx="2"/>')
        parts.append(f'<rect class="busy {b.kind} st-{b.step.status}" x="{xs:.1f}" '
                     f'y="{y + 4:.1f}" width="{max(xb - xs, 2.5):.1f}" height="{row_h - 8:.1f}" '
                     f'rx="3"><title>{_e(b.step.name)}: worked {_e(_span(b.busy_end - b.start))}'
                     f', waited {_e(_span(b.end - b.busy_end))}</title></rect>')
    parts.append("</svg>")
    return "".join(parts)


def _node(b: _Bar) -> str:
    s = b.step
    meta = [_span(b.busy_end - b.start)]
    if b.end - b.busy_end >= 1:
        meta.append(f"+{_span(b.end - b.busy_end)} waiting")
    if s.cost_usd:
        meta.append(_usd(s.cost_usd))
    if s.attempts > 1:
        meta.append(f"×{s.attempts} attempts")
    if b.kind == "agent" and isinstance(s.output, dict):
        meta.append(f"{s.output.get('turns')} turns")
    err = f'<div class="node-err">{_e(s.error.splitlines()[0])}</div>' if s.error else ""
    return (f'<div class="node {b.kind} st-{s.status}"><div class="node-top">'
            f'<span class="tag">{b.kind}</span><span class="dot" title="{_e(s.status)}"></span>'
            f'</div><div class="node-name">{_e(s.name)}</div>'
            f'<div class="node-meta">{_e(" · ".join(meta))}</div>{err}</div>')


def _flow(stages: list[list[_Bar]]) -> str:
    rows = []
    for st in stages:
        cls = "stage par" if len(st) > 1 else "stage"
        label = f'<div class="par-label">{len(st)} at once</div>' if len(st) > 1 else ""
        rows.append(f'<div class="{cls}">{label}<div class="stage-nodes">'
                    + "".join(_node(b) for b in st) + "</div></div>")
    return '<div class="flow">' + '<div class="arrow"></div>'.join(rows) + "</div>"


def _costs(bars: list[_Bar], budget: float | None) -> str:
    paid = [b for b in bars if b.step.cost_usd > 0]
    if not paid:
        return '<p class="muted">No step recorded a cost.</p>'
    top = max(b.step.cost_usd for b in paid)
    rows = "".join(
        f'<div class="cost-row"><span class="cost-name">{_e(b.step.name)}</span>'
        f'<span class="cost-track"><span class="cost-bar {b.kind}" '
        f'style="width:{b.step.cost_usd / top * 100:.1f}%"></span></span>'
        f'<span class="cost-val">{_usd(b.step.cost_usd)}</span></div>'
        for b in sorted(paid, key=lambda b: -b.step.cost_usd))
    meter = ""
    if budget:
        spent = sum(b.step.cost_usd for b in bars)
        pct = min(spent / budget, 1.0) * 100
        over = " over" if spent > budget else ""
        meter = (f'<div class="meter{over}"><span style="width:{pct:.1f}%"></span></div>'
                 f'<p class="muted">{_usd(spent)} of a {_usd(budget)} budget</p>')
    return meter + rows


def _table(bars: list[_Bar]) -> str:
    head = ("<tr><th>#</th><th>step</th><th>kind</th><th>status</th><th>attempts</th>"
            "<th>worked</th><th>waited</th><th>cost</th><th>tokens in / out</th></tr>")
    rows = []
    for b in bars:
        s = b.step
        tokens = ""
        if b.kind == "agent" and isinstance(s.output, dict):
            u = s.output.get("usage") or {}
            if isinstance(u, dict) and u:
                read = sum(int(u.get(k) or 0) for k in ("input", "cache_read", "cache_write"))
                tokens = f"{read:,} / {int(u.get('output') or 0):,}"
        rows.append(f"<tr><td>{s.seq}</td><td>{_e(s.name)}</td><td>{b.kind}</td>"
                    f'<td><span class="pill st-{s.status}">{_e(s.status)}</span></td>'
                    f"<td>{s.attempts}</td><td>{_span(b.busy_end - b.start)}</td>"
                    f"<td>{_span(b.end - b.busy_end)}</td><td>{_usd(s.cost_usd)}</td>"
                    f"<td>{tokens}</td></tr>")
    return f'<div class="scroll"><table>{head}{"".join(rows)}</table></div>'


def _notice(run: RunRow, store: Store) -> str:
    if run.status == "failed" and run.error:
        return (f'<div class="notice bad"><b>Failed.</b> {_e(run.error.splitlines()[0])}<br>'
                f"<code>ratchet retry {_e(run.id)}</code></div>")
    if run.status == "cancelled" and run.error:
        return (f'<div class="notice"><b>Cancelled.</b> {_e(run.error.splitlines()[0])}</div>')
    if run.status != "waiting":
        return ""
    for item in store.inbox("ratchet.budget"):
        if item["run_id"] == run.id:
            payload = '{"add_usd": 5}' if item["event"] == "ratchet.budget" else "<json>"
            qs = "".join(f"<li>{_e(q)}</li>" for q in item["questions"])
            return (f'<div class="notice warn"><b>Waiting for you.</b> {_e(item["question"])}'
                    + (f"<ol>{qs}</ol>" if qs else "")
                    + f"<code>ratchet signal {_e(run.id)} {_e(item['event'])} "
                      f"'{_e(payload)}'</code></div>")
    why = (f"the event <code>{_e(run.wait_event)}</code>" if run.wait_event
           else _when(run.wake_at) if run.wake_at else "its next tick")
    return f'<div class="notice">Waiting for {why}.</div>'


def render(store: Store, run_id: str, now: float | None = None) -> str:
    """The HTML page for one run. Raises KeyError when there is no such run."""
    run = store.get_run(run_id)
    if run is None:
        raise KeyError(run_id)
    now = time.time() if now is None else now
    steps = store.list_steps(run_id)
    bars = _bars(run, steps, now)
    t0 = min([run.created_at] + [b.start for b in bars])
    active = run.status in ("pending", "running", "waiting")
    t1 = max([b.end for b in bars] + [now if active else run.updated_at])
    worked = sum(b.busy_end - b.start for b in bars)
    cost = sum(s.cost_usd for s in steps)
    retries = sum(max(s.attempts - 1, 0) for s in steps)
    budget = f" / {_usd(run.budget_usd)}" if run.budget_usd else ""
    stats = [("status", f'<span class="pill st-{run.status}">{_e(run.status)}</span>'),
             ("wall time", _span(t1 - t0)), ("steps worked", _span(worked)),
             ("cost", _usd(cost) + budget), ("steps", str(len(steps))),
             ("retries", str(retries))]
    stat_html = "".join(f'<div class="stat"><div class="k">{k}</div><div class="v">{v}</div></div>'
                        for k, v in stats)
    body = (
        f'<header><div class="eyebrow">ratchet · {_e(run.workflow)}</div>'
        f"<h1>{_e(run.id)}</h1>"
        f'<p class="muted">started {_when(t0)} · report {_when(now)}</p></header>'
        f'<div class="stats">{stat_html}</div>{_notice(run, store)}'
        '<section><h2>Flow</h2><p class="muted">The steps that ran, in order. Side by side: '
        "they ran at the same time.</p>"
        f"{_flow(_stages(bars)) if bars else '<p class=muted>No steps yet.</p>'}</section>"
        '<section><h2>Timeline</h2><p class="muted">Solid: working. Light: waiting (a person, '
        "a pause, a retry). Grey bands fold quiet stretches longer than "
        f"{FOLD_S:.0f}s.</p><div class=\"scroll\">"
        f"{_timeline(bars, t0, t1) if bars else ''}</div>{_legend()}</section>"
        f"<section><h2>Cost</h2>{_costs(bars, run.budget_usd)}</section>"
        f"<section><h2>Steps</h2>{_table(bars)}</section>"
        f'<section><h2>Input</h2><pre>{_e(json.dumps(run.input, indent=2, ensure_ascii=False))}'
        "</pre></section>")
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8">'
            f'<meta name="viewport" content="width=device-width, initial-scale=1">'
            f"<title>{_e(run.id)} · ratchet</title><style>{_CSS}</style></head>"
            f"<body><main>{body}</main></body></html>")


def _legend() -> str:
    kinds = [("agent", "agent session"), ("human", "asks a person"), ("sleep", "sleep"),
             ("event", "waits for an event"), ("step", "step")]
    return '<div class="legend">' + "".join(
        f'<span><i class="sw {k}"></i>{label}</span>' for k, label in kinds) + "</div>"


_CSS = """
:root{--bg:#f7f7f5;--surface:#fff;--sunk:#efefec;--ink:#1b1d21;--ink2:#565b66;--ink3:#8a8f99;
--line:#e2e2de;--agent:#3b6fd8;--human:#c27a12;--sleep:#7b62c9;--event:#1f8a8a;--step:#6b7280;
--ok:#1d7f4e;--warn:#a86a00;--bad:#b83a31;--fold:#e9e9e5}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#121316;--surface:#1a1c20;
--sunk:#22252a;--ink:#e8e9ec;--ink2:#a8adb7;--ink3:#737985;--line:#2c2f35;--agent:#6d97ee;
--human:#e3a34a;--sleep:#a18ce6;--event:#4fb8b8;--step:#9aa1ad;--ok:#52bf86;--warn:#e0a640;
--bad:#ec7d73;--fold:#24272c}}
:root[data-theme=dark]{--bg:#121316;--surface:#1a1c20;--sunk:#22252a;--ink:#e8e9ec;
--ink2:#a8adb7;--ink3:#737985;--line:#2c2f35;--agent:#6d97ee;--human:#e3a34a;--sleep:#a18ce6;
--event:#4fb8b8;--step:#9aa1ad;--ok:#52bf86;--warn:#e0a640;--bad:#ec7d73;--fold:#24272c}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1000px;margin:0 auto;padding:32px 16px 64px}
h1{font-size:26px;margin:2px 0 4px;word-break:break-all}
h2{font-size:17px;margin:0 0 4px}
.eyebrow{color:var(--ink3);font-size:13px;text-transform:uppercase;letter-spacing:.06em}
.muted{color:var(--ink2);font-size:13px;margin:0 0 14px}
section{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:18px;
margin-top:18px}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;
margin-top:18px}
.stat{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:10px 12px}
.stat .k{color:var(--ink3);font-size:12px}.stat .v{font-size:18px;font-weight:600}
.pill{display:inline-block;padding:1px 8px;border-radius:99px;font-size:12px;font-weight:600;
background:var(--sunk);color:var(--ink2)}
.pill.st-completed,.pill.st-done{color:var(--ok)}.pill.st-failed{color:var(--bad)}.pill.st-cancelled{color:var(--ink3)}.node.st-cancelled{opacity:.6}
.pill.st-waiting,.pill.st-running,.pill.st-pending{color:var(--warn)}
.notice{margin-top:18px;padding:12px 14px;border-radius:10px;background:var(--sunk);
border-left:4px solid var(--ink3)}
.notice.warn{border-color:var(--warn)}.notice.bad{border-color:var(--bad)}
.notice code{display:block;margin-top:8px}
code,pre{font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace}
code{background:var(--surface);padding:4px 8px;border-radius:6px;word-break:break-all}
pre{background:var(--sunk);padding:12px;border-radius:8px;overflow:auto;margin:0}
.flow{display:flex;flex-direction:column;align-items:center}
.stage{width:100%;display:flex;flex-direction:column;align-items:center}
.stage.par{border:1px dashed var(--line);border-radius:12px;padding:8px}
.par-label{font-size:12px;color:var(--ink3);margin-bottom:6px}
.stage-nodes{display:flex;gap:10px;flex-wrap:wrap;justify-content:center;width:100%}
.arrow{width:2px;height:18px;background:var(--line);position:relative}
.arrow:after{content:"";position:absolute;left:-4px;bottom:-2px;border:5px solid transparent;
border-top-color:var(--line)}
.node{min-width:200px;max-width:300px;flex:1 1 200px;background:var(--surface);
border:1px solid var(--line);border-left:4px solid var(--step);border-radius:8px;padding:8px 10px}
.node.agent{border-left-color:var(--agent)}.node.human{border-left-color:var(--human)}
.node.sleep{border-left-color:var(--sleep)}.node.event{border-left-color:var(--event)}
.node.st-failed{border-color:var(--bad)}
.node-top{display:flex;justify-content:space-between;align-items:center}
.tag{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--ink3)}
.dot{width:9px;height:9px;border-radius:50%;background:var(--ink3)}
.st-done .dot{background:var(--ok)}.st-failed .dot{background:var(--bad)}
.st-waiting .dot,.st-running .dot{background:var(--warn)}
.node-name{font-weight:600;word-break:break-word}
.node-meta{font-size:12px;color:var(--ink2)}
.node-err{font-size:12px;color:var(--bad);margin-top:4px;word-break:break-word}
.scroll{overflow-x:auto}
.timeline{display:block;max-width:none}
.timeline text{font-size:12px;fill:var(--ink2)}
.timeline .fold{fill:var(--fold)}.timeline .fold-label{font-size:10px;fill:var(--ink3)}
.timeline .busy{fill:var(--step)}.timeline .busy.agent{fill:var(--agent)}
.timeline .busy.human{fill:var(--human)}.timeline .busy.sleep{fill:var(--sleep)}
.timeline .busy.event{fill:var(--event)}.timeline .busy.st-failed{fill:var(--bad)}
.timeline .wait{fill:var(--ink3);opacity:.22}
.legend{display:flex;flex-wrap:wrap;gap:14px;margin-top:10px;font-size:12px;color:var(--ink2)}
.sw{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px;
vertical-align:-1px;background:var(--step)}
.sw.agent,.cost-bar.agent{background:var(--agent)}.sw.human,.cost-bar.human{background:var(--human)}
.sw.sleep,.cost-bar.sleep{background:var(--sleep)}.sw.event,.cost-bar.event{background:var(--event)}
.cost-row{display:grid;grid-template-columns:minmax(70px,30%) 1fr 60px;gap:10px;
align-items:center;margin:5px 0;font-size:13px}
.cost-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.cost-track{background:var(--sunk);border-radius:4px;height:12px;overflow:hidden}
.cost-bar{display:block;height:100%;background:var(--step);border-radius:4px}
.cost-val{text-align:right;font-variant-numeric:tabular-nums}
.meter{height:10px;background:var(--sunk);border-radius:5px;overflow:hidden;margin-bottom:4px}
.meter span{display:block;height:100%;background:var(--ok)}.meter.over span{background:var(--bad)}
table{border-collapse:collapse;width:100%;font-size:13px;font-variant-numeric:tabular-nums}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--ink3);font-weight:500}
"""
