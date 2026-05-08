#!/usr/bin/env python3
"""Remove .py source files for moat modules compiled to .so extensions.

Run from /app in the Docker runtime stage after .so files have been
overlaid from the compiler stage.  Any .py file whose module has a
matching .so is deleted.  Also cleans up any residual .c files.
"""
from __future__ import annotations

from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"

removed = 0
for so_path in sorted(SRC.rglob("*.so")):
    # so name: anomaly.cpython-312-x86_64-linux-gnu.so  →  module = anomaly
    module_name = so_path.name.split(".")[0]
    py_path = so_path.parent / f"{module_name}.py"
    if py_path.exists():
        py_path.unlink()
        removed += 1

for c_path in SRC.rglob("*.c"):
    c_path.unlink()

print(
    f"strip_sources: removed {removed} .py files "
    "(moat modules now .so only).",
    flush=True,
)
