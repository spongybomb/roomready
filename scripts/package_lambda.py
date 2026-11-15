"""Zip the roomready package for Lambda (stdlib only).

    python scripts/package_lambda.py        -> build/roomready_lambda.zip

The local web UI and server are excluded; boto3 is provided by the Lambda runtime.
"""
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGE = ROOT / "roomready"
OUTPUT = ROOT / "build" / "roomready_lambda.zip"
EXCLUDE_DIRS = {"__pycache__", "web"}
EXCLUDE_FILES = {"server.py", "demo.py"}


def build(output=OUTPUT):
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(PACKAGE.rglob("*.py")):
            rel = path.relative_to(ROOT)
            if EXCLUDE_DIRS.intersection(rel.parts) or path.name in EXCLUDE_FILES:
                continue
            zf.write(path, rel.as_posix())
            count += 1
    return output, count


if __name__ == "__main__":
    out, n = build()
    print(f"wrote {out} ({n} files, {out.stat().st_size} bytes)")
    sys.exit(0)
