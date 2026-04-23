"""PL/SQL conversion corpus matrix runner.

Each fixture in `tests/fixtures/oracle_plsql/` is a real, parseable
PL/SQL object exercising one entry in `routers/convert.py:_EXAMPLES`.
Together they form the regression net for the parser + tag detector
+ canonical-example registry — the three pieces that must stay in
sync as the conversion catalog grows.

Per-fixture, the matrix:

  1. Parse the file with `src.source.oracle.parser.parse`. Assert
     no fatal diagnostics — the input must be syntactically valid
     Oracle that we can ingest.
  2. Verify the expected `ConstructTag` shows up in any object's
     `referenced_constructs`. This is what tells the converter
     "this snippet contains a MERGE / DECODE / NVL / …" — drift
     here means the parser stopped recognizing a construct it used
     to.
  3. Confirm `_EXAMPLES` has an entry for that tag — i.e., the
     converter ships a canonical translation for it.
  4. Spot-check the canonical PG output contains the expected
     idiom (e.g., CONNECT_BY → "WITH RECURSIVE", DECODE → "CASE",
     NVL → "COALESCE"). Catches accidental edits that swap the
     translation for something less idiomatic.

Adding a new fixture = adding a new `_FIXTURES` entry, no test
boilerplate. Pure-Python — no DB, no Oracle, no Anthropic — so
this runs in any environment.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.core.ir.nodes import ConstructTag
from src.routers.convert import _EXAMPLES
from src.source.oracle.parser import parse


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "oracle_plsql"


# Each entry: (relative path, expected tag, expected idiom in the
# canonical PG output). The idiom is a substring match — robust to
# whitespace edits in the canonical example, brittle only when the
# translation strategy itself changes (which is exactly what should
# fail this test).
_FIXTURES = [
    ("procedures/merge_upsert.sql",         ConstructTag.MERGE,            "ON CONFLICT"),
    ("procedures/connect_by_hierarchy.sql", ConstructTag.CONNECT_BY,       "WITH RECURSIVE"),
    ("procedures/autonomous_audit.sql",     ConstructTag.AUTONOMOUS_TXN,   "dblink"),
    ("procedures/bulk_collect_loop.sql",    ConstructTag.BULK_COLLECT,     "FOREACH"),
    ("procedures/dbms_scheduler_job.sql",   ConstructTag.DBMS_SCHEDULER,   "cron.schedule"),
    ("procedures/utl_file_export.sql",      ConstructTag.UTL_FILE,         "COPY"),
    ("functions/nvl_heavy.sql",             ConstructTag.NVL,              "COALESCE"),
    ("functions/decode_reporting.sql",      ConstructTag.DECODE,           "CASE"),
    ("functions/interval_arith.sql",        ConstructTag.INTERVAL,         "INTERVAL"),
    ("functions/legacy_outer_join.sql",     ConstructTag.OUTER_JOIN_PLUS,  "LEFT JOIN"),
    ("functions/rownum_pagination.sql",     ConstructTag.ROWNUM,           "LIMIT"),
]


def _all_construct_tags(module) -> set[ConstructTag]:
    """Flatten every construct tag mentioned by any object in the
    parsed module. The interim parser attaches tags via
    `referenced_constructs`; the ANTLR path adds them through a
    sentinel `<module-constructs>` object — both end up in
    `module.objects[*].referenced_constructs`."""
    tags: set[ConstructTag] = set()
    for obj in module.objects:
        for ref in getattr(obj, "referenced_constructs", []) or []:
            tags.add(ref.tag)
    return tags


@pytest.mark.parametrize(
    "rel_path, expected_tag, expected_idiom",
    _FIXTURES,
    ids=[f[0] for f in _FIXTURES],
)
def test_fixture_parses_tags_and_has_canonical_example(
    rel_path: str, expected_tag: ConstructTag, expected_idiom: str
):
    """The full conversion-corpus contract for one fixture file."""
    source_path = FIXTURES_DIR / rel_path
    source = source_path.read_text()

    # 1. Parse — no fatal diagnostics on any object.
    module = parse(source, name=str(rel_path))
    fatal = []
    for obj in module.objects:
        for diag in getattr(obj, "diagnostics", []) or []:
            severity_name = getattr(getattr(diag, "severity", None), "name", "")
            if severity_name == "ERROR":
                fatal.append(diag)
    assert not fatal, f"{rel_path} produced fatal parse diagnostics: {fatal}"

    # 2. Expected tag fires.
    tags = _all_construct_tags(module)
    assert expected_tag in tags, (
        f"{rel_path} did not produce {expected_tag.name}; "
        f"detected tags: {sorted(t.name for t in tags)}"
    )

    # 3. Canonical example registered for this tag.
    assert expected_tag in _EXAMPLES, (
        f"_EXAMPLES has no entry for {expected_tag.name} — fixture "
        f"covers a construct the converter doesn't ship a canonical "
        f"translation for"
    )

    # 4. Spot-check the PG idiom.
    canonical = _EXAMPLES[expected_tag]
    assert expected_idiom.upper() in canonical.postgres.upper(), (
        f"_EXAMPLES[{expected_tag.name}].postgres no longer contains "
        f"the expected idiom {expected_idiom!r} — translation strategy "
        f"may have drifted from what this fixture documents.\n"
        f"--- canonical output ---\n{canonical.postgres}"
    )


def test_every_fixture_file_is_in_the_matrix():
    """Locks the inverse: a new .sql file dropped into
    `oracle_plsql/` without a matching `_FIXTURES` entry would
    otherwise sit unverified. Better to fail loudly so the matrix
    stays comprehensive."""
    on_disk = {
        str(p.relative_to(FIXTURES_DIR))
        for p in FIXTURES_DIR.rglob("*.sql")
    }
    in_matrix = {rel for rel, *_ in _FIXTURES}
    missing = on_disk - in_matrix
    assert not missing, (
        f"PL/SQL fixtures present on disk but not in _FIXTURES: {missing}. "
        f"Add an entry so the matrix exercises them."
    )
