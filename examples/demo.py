"""Token-free demo: a flow with steps, a question for a human and a wait.

    ratchet --app examples/demo.py:engine start demo '"Durable execution"' --id a1
    ratchet --app examples/demo.py:engine show a1
    ratchet --app examples/demo.py:engine signal a1 outline.ok '{"decision": "go on"}'
    ratchet --app examples/demo.py:engine tick
"""

import os

from ratchet import Engine, NeedsHuman

engine = Engine(os.environ.get("RATCHET_DB", "demo.db"))


@engine.workflow("demo")
def demo(ctx, topic):
    outline = ctx.step("outline", lambda: {"topic": topic, "sections": ["intro", "body"]})

    def approval():
        answer = ctx.scratch.get("signal")
        if answer is None:
            raise NeedsHuman("outline.ok", "go on with this outline?")
        return answer

    decision = ctx.step("approval", approval)
    for name in outline["sections"]:
        ctx.step(name, lambda n=name: f"{n} written")
    return {"topic": topic, "decision": decision["decision"]}
