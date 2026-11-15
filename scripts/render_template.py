"""Embed infra/statemachine.asl.json into the CloudFormation template (stdlib only).

    python scripts/render_template.py       -> build/template.rendered.yaml

The ASL file stays the single source of truth (the local orchestrator executes the same
file). JSON is valid YAML flow syntax, so the definition is inlined on one line as the
StateMachine `Definition` property; `${StepHandlerArn}` is resolved by CloudFormation
DefinitionSubstitutions. No S3 artifact bucket is needed.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "infra" / "template.yaml"
ASL = ROOT / "infra" / "statemachine.asl.json"
OUTPUT = ROOT / "build" / "template.rendered.yaml"
MARKER = "__STATE_MACHINE_DEFINITION__"


def render():
    template = TEMPLATE.read_text(encoding="utf-8")
    definition = json.dumps(json.loads(ASL.read_text(encoding="utf-8")), separators=(",", ":"))
    body_lines = [l for l in template.splitlines() if not l.lstrip().startswith("#") and MARKER in l]
    if len(body_lines) != 1:
        raise SystemExit("expected exactly one Definition placeholder line in template.yaml")
    return template.replace(f"Definition: {MARKER}", f"Definition: {definition}")


if __name__ == "__main__":
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(render(), encoding="utf-8")
    print(f"wrote {OUTPUT}")
