#!/usr/bin/env python3
"""Extract the OSS subset of depart into a standalone public repo.

Writes a clean copy of the parser + runner + analyzer + CLI (everything
MIT-licensable) to a destination directory. Everything AI/billing/auth/
UI/license-specific stays behind in the private repo.

The goal is "one command away from a public launch" — this script
doesn't push, doesn't publish to PyPI, and doesn't touch the working
tree. It just materializes a directory you can `cd` into and `git init`.

Usage:
    scripts/extract-oss.py [--out /tmp/depart-public]

The extracted layout matches what the GitHub repo will look like:

    depart/
      README.md                 ← written here
      LICENSE                   ← MIT, written here
      CONTRIBUTING.md           ← written here
      ARCHITECTURE.md           ← written here
      pyproject.toml            ← derived from the monorepo pyproject
      crates/
        depart-parser/          ← src/core + src/source
        depart-migrate/         ← src/migrate + src/target
        depart-analyze/         ← src/analyze (complexity scorer + extractor)
        depart-cli/             ← src/migrate/__main__.py wrapper
      docker/
        runner/                 ← minimal Dockerfile for the CLI
        oracle-init/            ← HR fixture
      examples/
        hr-schema.sql           ← the HR fixture DDL, stand-alone
      tests/
        test_*.py               ← selected tests that don't touch DB auth
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from textwrap import dedent


REPO_ROOT = Path(__file__).resolve().parent.parent
API_SRC = REPO_ROOT / "apps" / "api" / "src"
API_TESTS = REPO_ROOT / "apps" / "api" / "tests"


# ─── Classification manifest ─────────────────────────────────────────────────
#
# Maps source-tree subtrees to destinations in the extracted repo.
# KEEP here → belongs in the public OSS repo. Anything not listed is
# kept exclusively in the private repo.

PACKAGES = {
    "depart-parser": [
        (API_SRC / "core", "src/parser/core"),
        (API_SRC / "source", "src/parser/source"),
        (API_SRC / "validators", "src/parser/validators"),
    ],
    "depart-migrate": [
        (API_SRC / "migrate", "src/migrate"),
        (API_SRC / "target", "src/target"),
        (API_SRC / "infra", "src/infra"),
    ],
    "depart-analyze": [
        (API_SRC / "analyze", "src/analyze"),
    ],
}

# Tests that exercise only the OSS subset (no DB auth, no AI, no license).
OSS_TESTS = [
    "test_ir.py",
    "test_lexer.py",
    "test_parser_oracle.py",
    "test_parser_equivalence.py",
    "test_complexity.py",
    "test_migrate_keyset.py",
    "test_migrate_planner.py",
    "test_migrate_copy.py",
    "test_migrate_sequences.py",
    "test_migrate_verify.py",
    "test_migrate_runner.py",
    "test_migrate_introspect.py",
    "test_migrate_introspect_oracle_live.py",
    "test_migrate_checkpoint_adapter.py",
    "test_migrate_ddl.py",
    "test_migrate_cli.py",
    "test_sql_extractor.py",
    # Note: test_assess_endpoint.py and test_convert_endpoint.py depend
    # on the routers tree which stays in the private repo. Move them
    # over when we also extract a thin FastAPI surface for the OSS CLI.
]


# ─── Static file contents ────────────────────────────────────────────────────

LICENSE = dedent(
    """\
    MIT License

    Copyright (c) 2026 depart contributors

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    THE SOFTWARE.
    """
)

README = dedent(
    """\
    # depart

    Oracle → PostgreSQL migration, built for teams that want to own their tools.

    - **ANTLR-backed PL/SQL parser** with a dialect-agnostic IR
    - **Data-movement runner** with keyset pagination, binary COPY, and Merkle row verification
    - **Complexity analyzer** that scores schemas and flags risky constructs
    - **Zero phone-home** — everything runs inside your infrastructure

    The enterprise edition (AI conversion, runbook PDF generator, hybrid control plane, SSO)
    lives at <https://depart.cloud>. This repository is the MIT-licensed core.

    ## Install

    ```bash
    pip install depart-parser depart-migrate depart-analyze depart-cli
    ```

    Or run the Docker image:

    ```bash
    docker run --rm ghcr.io/davidnhg74/depart:latest --help
    ```

    ## Quickstart

    ```bash
    depart migrate \\
        --source 'oracle+oracledb://hr:hr@oracle:1521/?service_name=FREEPDB1' \\
        --target 'postgresql+psycopg://user:pass@localhost:5432/depart' \\
        --source-schema HR --target-schema public \\
        --create-tables
    ```

    ## Architecture

    See [ARCHITECTURE.md](./ARCHITECTURE.md) for the IR design and the
    parser / runner split.

    ## Contributing

    Grammar gaps and construct-tagging PRs are especially welcome —
    see [CONTRIBUTING.md](./CONTRIBUTING.md).

    ## License

    MIT. See [LICENSE](./LICENSE).
    """
)

CONTRIBUTING = dedent(
    """\
    # Contributing to depart

    Welcome! The two highest-leverage contribution areas:

    1. **ANTLR grammar coverage.** The PL/SQL grammar covers production
       constructs but has gaps. If you hit a construct we don't recognize,
       open an issue with a minimal repro and tag it `grammar`. PRs that
       add grammar rules + a matching IR visitor are the fastest path to
       a release.

    2. **Type mapping.** `src/migrate/ddl.py` maps Oracle types to
       Postgres. Real-world schemas have edge cases (`NUMBER(38)`,
       `TIMESTAMP WITH LOCAL TIME ZONE`, user-defined types). PRs that
       add cases + a unit test land quickly.

    ## Dev setup

    ```bash
    git clone https://github.com/davidnhg74/depart.git
    cd depart
    pip install -e '.[dev]'
    python scripts/generate_grammar.py   # builds the ANTLR parser
    pytest tests/ --no-cov
    ```

    ## Commit style

    We write commit messages as sentences, not bullets. The first line is
    the "what" (< 70 chars); the body is the "why". Reference issues in
    the footer.
    """
)

ARCHITECTURE = dedent(
    """\
    # Architecture

    depart is split along two axes: **dialect-agnostic IR** (so we can
    handle Oracle PL/SQL today and T-SQL tomorrow) and **introspect-plan-
    run** (so each stage is testable on its own).

    ## Parser → IR

    The ANTLR grammar at `src/parser/source/oracle/grammar/` produces a
    parse tree; a visitor at `src/parser/source/oracle/visitor.py` folds
    it into the IR types at `src/parser/core/ir/nodes.py`. An interim
    parser (hand-rolled regex) runs alongside as a long-tail construct
    scanner and a fallback when the grammar hits something unexpected.

    ## Runner

    `src/migrate/runner.py` walks a `LoadPlan` (produced by
    `planner.py`) and for each table:

    1. **Introspect** — pull column metadata + PK + FK via the
       introspector at `introspect.py`.
    2. **Generate DDL** — if `--create-tables` is set, emit
       `CREATE TABLE IF NOT EXISTS ...` via `ddl.py`.
    3. **Copy rows** — keyset-paginate the source, stream through the
       binary COPY protocol to the target (`copy.py`).
    4. **Verify** — hash every batch on both sides and build a Merkle
       tree per table (`verify.py`). Row-count or hash mismatches are
       surfaced in the `RunResult`.
    5. **Catch up sequences** — after all tables are loaded, advance
       any SERIAL / IDENTITY columns to match the highest loaded value
       (`sequences.py`).

    Checkpoints land after every batch via the `checkpoint` callback;
    `checkpoint_adapter.py` wires this into a persistent record so a
    crashed run resumes from the last completed batch.

    ## What's not in this repo

    AI-powered conversion (live Claude calls), the runbook PDF
    generator, the license verifier, the SaaS frontend, and the hybrid
    control plane all live in the private `depart-cloud` repo under a
    commercial license. See <https://depart.cloud/pricing> for the
    tiering.
    """
)


# ─── Main ────────────────────────────────────────────────────────────────────


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument(
        "--out",
        type=Path,
        default=Path("/tmp/depart-public"),
        help="Destination directory (will be wiped and recreated)",
    )
    args = p.parse_args()

    out: Path = args.out
    if out.exists():
        print(f"→ removing existing {out}")
        shutil.rmtree(out)
    out.mkdir(parents=True)
    (out / "src").mkdir()
    (out / "tests").mkdir()
    (out / "docker").mkdir()
    (out / "examples").mkdir()

    print(f"→ extracting into {out}")

    # Copy source packages.
    for pkg_name, mappings in PACKAGES.items():
        print(f"  · {pkg_name}")
        for src_path, rel_dest in mappings:
            if not src_path.exists():
                print(f"    (skipping missing: {src_path})")
                continue
            dest = out / rel_dest
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(src_path, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "_generated"))

    # Copy selected tests.
    print("  · tests")
    for test_name in OSS_TESTS:
        src = API_TESTS / test_name
        if src.exists():
            shutil.copy2(src, out / "tests" / test_name)
        else:
            print(f"    (missing: {test_name})")

    # Docker + fixtures.
    print("  · docker + fixtures")
    dockerfile = REPO_ROOT / "apps" / "api" / "Dockerfile"
    if dockerfile.exists():
        shutil.copy2(dockerfile, out / "docker" / "Dockerfile")
    oracle_init = REPO_ROOT / "docker" / "oracle-init"
    if oracle_init.exists():
        shutil.copytree(oracle_init, out / "docker" / "oracle-init")

    # Static files.
    print("  · LICENSE / README / CONTRIBUTING / ARCHITECTURE")
    (out / "LICENSE").write_text(LICENSE)
    (out / "README.md").write_text(README)
    (out / "CONTRIBUTING.md").write_text(CONTRIBUTING)
    (out / "ARCHITECTURE.md").write_text(ARCHITECTURE)

    # Stub pyproject.toml with just what the OSS packages need.
    print("  · pyproject.toml (stub)")
    (out / "pyproject.toml").write_text(
        dedent(
            """\
            [project]
            name = "depart"
            version = "0.1.0"
            description = "Oracle to PostgreSQL migration — parser, runner, analyzer."
            readme = "README.md"
            license = { file = "LICENSE" }
            requires-python = ">=3.12"
            dependencies = [
                "antlr4-python3-runtime>=4.13",
                "sqlalchemy>=2",
                "psycopg[binary]>=3",
                "pydantic>=2",
            ]

            [project.optional-dependencies]
            oracle = ["oracledb>=2"]
            dev = ["pytest>=9", "pytest-asyncio>=1"]

            [project.scripts]
            depart = "depart.migrate.__main__:main"

            [build-system]
            requires = ["setuptools>=68"]
            build-backend = "setuptools.build_meta"

            [tool.setuptools.packages.find]
            where = ["src"]
            """
        )
    )

    # Summary.
    file_count = sum(1 for _ in out.rglob("*") if _.is_file())
    print()
    print(f"✓ extracted {file_count} files into {out}")
    print("  next steps:")
    print(f"    cd {out}")
    print("    git init && git add . && git commit -m 'initial public release'")
    print("    git remote add origin git@github.com:davidnhg74/depart.git")
    print("    git push -u origin main")

    return 0


if __name__ == "__main__":
    sys.exit(main())
