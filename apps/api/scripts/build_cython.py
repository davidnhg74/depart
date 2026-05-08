#!/usr/bin/env python3
"""Compile Hafen moat modules to Cython .so extensions.

Invokes the `cython` CLI to produce .c files, then compiles each one
to a .so with gcc/cc directly — no setuptools path-mangling.  Run from
anywhere; uses absolute paths throughout.

Usage:
    python3 scripts/build_cython.py [--dry-run]
"""
from __future__ import annotations

import multiprocessing
import os
import platform
import subprocess
import sys
import sysconfig
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
SRC = BASE / "src"

# Directories (relative to SRC) whose *.py files are compiled recursively.
COMPILE_DIRS = [
    "analyze",
    "analyzers",
    "connectors",
    "core",
    "license",
    "migrate",
    "services",
    "target",
    "transforms",
    "validators",
]

# Individual files outside those dirs.
EXTRA_FILES = [
    "source/oracle/_lexer.py",
    "source/oracle/_visitor.py",
    "source/oracle/parser.py",
    "source/oracle/grammar/PlSqlLexerBase.py",
    "source/oracle/grammar/PlSqlParserBase.py",
]

DRY_RUN = "--dry-run" in sys.argv


def collect() -> list[Path]:
    files: list[Path] = []
    for d in COMPILE_DIRS:
        dp = SRC / d
        if not dp.exists():
            print(f"  [skip] {d}/ — not found", flush=True)
            continue
        for f in sorted(dp.rglob("*.py")):
            if f.name == "__init__.py":
                continue
            files.append(f)
    for rel in EXTRA_FILES:
        f = SRC / rel
        if f.exists():
            files.append(f)
        else:
            print(f"  [skip] {rel} — not found", flush=True)
    return files


def _ext_suffix() -> str:
    return sysconfig.get_config_var("EXT_SUFFIX") or ".so"


def _so_path(py_file: Path) -> Path:
    stem = py_file.name.split(".")[0]
    return py_file.parent / f"{stem}{_ext_suffix()}"


def _compile_one(py_file: Path, include_dir: str, cc: str) -> None:
    c_file = py_file.with_suffix(".c")
    so_file = _so_path(py_file)

    # Step 1: Python → C (via Cython)
    subprocess.run(
        ["cython", "--3", str(py_file), "-o", str(c_file)],
        check=True,
        capture_output=True,
    )

    # Step 2: C → .so
    link_flags: list[str]
    if platform.system() == "Darwin":
        link_flags = ["-bundle", "-undefined", "dynamic_lookup"]
    else:
        link_flags = ["-shared", "-fPIC"]

    subprocess.run(
        [cc, "-O2", f"-I{include_dir}", str(c_file), "-o", str(so_file)]
        + link_flags,
        check=True,
        capture_output=True,
    )

    # Step 3: remove intermediate C file
    c_file.unlink()


def _compile_worker(args: tuple) -> tuple[str, str | None]:
    py_file, include_dir, cc = args
    rel = str(Path(py_file).relative_to(SRC))
    try:
        _compile_one(Path(py_file), include_dir, cc)
        return rel, None
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or b"").decode(errors="replace")
        return rel, stderr


def main() -> None:
    files = collect()
    print(f"Cython: {len(files)} modules queued", flush=True)

    if DRY_RUN:
        for f in files:
            print(f"  {f.relative_to(SRC)}")
        return

    include_dir = sysconfig.get_path("include")
    cc = os.environ.get("CC", "gcc")
    ncpu = max(1, multiprocessing.cpu_count())

    print(f"Compiling with {cc}, {ncpu} workers, include={include_dir}", flush=True)

    args = [(str(f), include_dir, cc) for f in files]

    with multiprocessing.Pool(ncpu) as pool:
        results = pool.map(_compile_worker, args)

    failed = [(rel, err) for rel, err in results if err is not None]
    ok = len(results) - len(failed)

    print(f"Compiled {ok}/{len(results)} modules.", flush=True)
    if failed:
        for rel, err in failed:
            print(f"  FAIL: {rel}\n{err}", flush=True)
        sys.exit(1)

    print("Cython compilation complete.", flush=True)


if __name__ == "__main__":
    main()
