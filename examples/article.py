"""A real flow: Claude proposes topics, a person picks one, Claude writes, two reviews run at
once, a person approves, and the article is published once.

It spends tokens through your `claude` login. Everything happens in ./article-work: nothing is
pushed or sent anywhere.

    ratchet --app article.py:engine start article '"SQLite in production"' --id a1 --budget 3
    ratchet --app article.py:engine inbox           # pick a topic
    ratchet --app article.py:engine signal a1 topic.ok '{"pick": 2}'
    ratchet --app article.py:engine tick            # writes and reviews, then waits again
    ratchet --app article.py:engine signal a1 publish.ok '{"decision": "publish"}'
    ratchet --app article.py:engine tick

What it shows, step by step:

- every agent answer goes through a validator, and a bad answer is retried in the same session;
- the person gets the question with its details, and the open points apart (`questions`);
- `ctx.parallel` runs the two reviews at once, each with its own session and cost;
- `ctx.now` gives the date once, so a replay the next day writes the same file name;
- the publish step checks first whether it already published: a crash there costs nothing.

ARTICLE_MODEL and REVIEW_MODEL pick the models (a cheap one is enough to try the flow).
"""

from __future__ import annotations

import json
import os
import re
import shutil
from pathlib import Path

from ratchetflow import Engine, NeedsHuman
from ratchetflow.agents import claude

WORK = Path(os.environ.get("ARTICLE_WORK", "article-work")).resolve()
WRITER = os.environ.get("ARTICLE_MODEL", "claude-opus-5-5")
REVIEWER = os.environ.get("REVIEW_MODEL", "claude-sonnet-5-5")
READ = ["Read", "Grep", "Glob"]

WORK.mkdir(parents=True, exist_ok=True)
engine = Engine(WORK / "runs.db")


def tools(*names: str) -> list[str]:
    return ["--allowedTools", ",".join(names)]


def ask(ctx, name: str, event: str, question: str, *, details=None, questions=()):
    """A step that waits for `ratchet signal <run> <event>` and returns its payload."""
    def wait():
        answer = ctx.scratch.get("signal")
        if answer is None:
            raise NeedsHuman(event, question, details=details, questions=list(questions))
        return answer
    return ctx.step(name, wait)


# --- validators: they raise to make the agent try again, in the same session ---------------

def check_topics(data):
    topics = data.get("topics") if isinstance(data, dict) else None
    if not isinstance(topics, list) or len(topics) != 3:
        raise ValueError('answer {"topics": [...]} with exactly 3 topics')
    for t in topics:
        if not t.get("title") or not t.get("angle"):
            raise ValueError("each topic needs a title and an angle")
    return topics


def check_draft(path: Path):
    def check(_text):
        if not path.exists():
            raise ValueError(f"write the article to {path}")
        body = path.read_text()
        words = len(body.split())
        if not 300 <= words <= 1200:
            raise ValueError(f"the article has {words} words; it must have 300 to 1200")
        if not body.lstrip().startswith("# "):
            raise ValueError("the article starts with a '# ' title")
        return {"words": words, "sections": len(re.findall(r"^## ", body, re.M))}
    return check


def check_review(data):
    if not isinstance(data, dict) or data.get("verdict") not in ("ok", "fix"):
        raise ValueError('answer {"verdict": "ok" | "fix", "notes": [...]}')
    if not isinstance(data.get("notes", []), list):
        raise ValueError("notes is a list of strings")
    return data


# --- a step that touches the outside world: safe to run twice ------------------------------

def publish(draft: Path, target: Path) -> dict:
    if target.exists() and target.read_bytes() == draft.read_bytes():
        return {"published": str(target), "already": True}
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    shutil.copyfile(draft, tmp)
    tmp.replace(target)          # atomic: the file is either the old one or the new one
    return {"published": str(target), "already": False}


# --- the workflow --------------------------------------------------------------------------

@engine.workflow("article")
def article(ctx, subject):
    topics = ctx.step(
        "topics", claude, ctx,
        f"Propose 3 article topics about: {subject}. Each one with a title and a one-sentence "
        'angle. Answer only with a ```json block: {"topics": [{"title": "...", '
        '"angle": "..."}]}',
        model=REVIEWER, cwd=WORK, timeout_s=600, validate=check_topics,
        extra_args=tools(*READ))["data"]

    menu = "\n".join(f"{i}. {t['title']}: {t['angle']}" for i, t in enumerate(topics, 1))
    pick = ask(ctx, "topic-ok", "topic.ok",
               f"Which topic should the article be about?\n{menu}\n"
               "Answer '{\"pick\": N}', or '{\"pick\": 0}' to stop here.",
               details={"topics": topics})
    if not pick.get("pick"):
        return {"stopped": "no topic picked"}
    topic = topics[int(pick["pick"]) - 1]

    day = ctx.now("started").date().isoformat()   # not date.today(): replays get the same day
    draft = WORK / ctx.run_id / "draft.md"
    draft.parent.mkdir(parents=True, exist_ok=True)
    written = ctx.step(
        "write", claude, ctx,
        f"Write an article titled \"{topic['title']}\" ({topic['angle']}). Markdown, 400 to 900 "
        f"words, starting with a '# ' title, with '## ' sections. Write it to {draft} and "
        "nothing else. At the end, list any claim you were unsure of.",
        model=WRITER, cwd=WORK, timeout_s=1800, expect_json=False, validate=check_draft(draft),
        extra_args=tools(*READ, "Write", "Edit"))

    review = ('Answer only with a ```json block: {{"verdict": "ok" or "fix", "notes": '
              '["one short note per problem"]}}. Read {path}; do not change it. {what}')
    style, facts = ctx.parallel(
        ctx.call("review-style", claude, ctx,
                 review.format(path=draft, what="Review the style: clarity, structure, length."),
                 model=REVIEWER, cwd=WORK, timeout_s=900, validate=check_review,
                 extra_args=tools(*READ)),
        ctx.call("review-facts", claude, ctx,
                 review.format(path=draft, what="Check every technical claim; flag the wrong "
                                                "or doubtful ones."),
                 model=REVIEWER, cwd=WORK, timeout_s=900, validate=check_review,
                 extra_args=tools(*READ)),
    )
    notes = [f"style: {n}" for n in style["data"]["notes"]]
    notes += [f"facts: {n}" for n in facts["data"]["notes"]]

    decision = ask(ctx, "publish-ok", "publish.ok",
                   f"\"{topic['title']}\": {written['data']['words']} words. Reviews: style "
                   f"{style['data']['verdict']}, facts {facts['data']['verdict']}. Read {draft}. "
                   "Answer '{\"decision\": \"publish\"}' or '{\"decision\": \"stop\"}'.",
                   details={"draft": str(draft), "checks": written["data"],
                            "write_and_review_usd": round(sum(s["cost_usd"] or 0 for s in
                                                              (written, style, facts)), 4)},
                   questions=notes)
    if decision.get("decision") != "publish":
        return {"stopped": "not published", "draft": str(draft)}

    slug = re.sub(r"[^a-z0-9]+", "-", topic["title"].lower()).strip("-")[:60]
    out = ctx.step("publish", publish, draft, WORK / "published" / f"{day}-{slug}.md")
    return {**out, "words": written["data"]["words"], "notes": notes}


if __name__ == "__main__":
    print(json.dumps(sorted(engine.workflows)))
